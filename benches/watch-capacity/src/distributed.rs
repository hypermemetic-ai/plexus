//! Private stdio control and authenticated WSS data plane. Each generator
//! retains its keys and owns a disjoint contiguous range of global users.
use super::{clock_us, histogram, open, stats, user_org, verify};
use anyhow::{Context, Result, ensure};
use futures::{StreamExt, TryStreamExt, stream};
use plexus::tls::Identity;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, Write},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep, sleep_until, timeout},
};

pub struct Access {
    pub bind: IpAddr,
    pub sources: HashSet<IpAddr>,
}

fn hosts(input: &str) -> Result<HashSet<IpAddr>> {
    input
        .split(',')
        .map(|s| {
            let ip: IpAddr = s.parse().context("expected a literal IP address")?;
            ensure!(
                !ip.is_unspecified() && !ip.is_multicast(),
                "expected a unicast IP address"
            );
            Ok(ip)
        })
        .collect()
}

impl Access {
    pub fn new(bind: &str, sources: &str) -> Result<Self> {
        let ip: IpAddr = bind.parse()?;
        ensure!(
            !ip.is_unspecified() && !ip.is_multicast(),
            "bind to an explicit unicast interface IP"
        );
        Ok(Self {
            bind: ip,
            sources: hosts(sources)?,
        })
    }

    pub fn loopback() -> Self {
        Self::new("127.0.0.1", "127.0.0.1").unwrap()
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Control {
    Connect {
        urls: Vec<String>,
        pin: String,
        profile: String,
        rate: u64,
        read_interval_ms: u64,
    },
    Clock,
    Start {
        unix_us: u64,
    },
    Drop,
}

fn command() -> Result<Control> {
    let mut line = String::new();
    ensure!(
        io::stdin().lock().read_line(&mut line)? > 0,
        "controller disconnected"
    );
    Ok(serde_json::from_str(&line)?)
}

fn emit(v: Value) -> Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &v)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn allowed_urls(urls: &[String]) -> Result<()> {
    let allowed = hosts(
        &std::env::var("WATCH_GENERATOR_TARGETS").unwrap_or_else(|_| "127.0.0.1,::1".into()),
    )?;
    ensure!(!urls.is_empty(), "no fixture listeners");
    for url in urls {
        let addr: SocketAddr = url
            .strip_prefix("wss://")
            .and_then(|s| s.strip_suffix('/'))
            .context("expected wss://IP:PORT/")?
            .parse()?;
        ensure!(
            addr.port() != 0 && allowed.contains(&addr.ip()),
            "fixture IP is outside WATCH_GENERATOR_TARGETS"
        );
    }
    Ok(())
}

// Midpoint assumes stable clocks; the interval retains network asymmetry.
async fn calibrate(conn: &plexus::Connection) -> Result<(i128, i128)> {
    let mut best = None;
    for _ in 0..5 {
        let start = i128::from(clock_us());
        let reply = timeout(Duration::from_secs(5), conn.call("bench.clock", ())).await??;
        let end = i128::from(clock_us());
        ensure!(
            end >= start,
            "generator wall clock stepped during calibration"
        );
        let stamp = i128::from(reply.get().parse::<u64>()?);
        let bounds = (stamp - end, stamp - start);
        if best.is_none_or(|(lo, hi)| hi - lo > bounds.1 - bounds.0) {
            best = Some(bounds);
        }
    }
    Ok(best.unwrap())
}

fn distribution(h: &hdrhistogram::Histogram<u64>) -> Value {
    let mut result = stats(h);
    result["buckets_us"] = json!(
        h.iter_recorded()
            .map(|v| [v.value_iterated_to(), v.count_at_value()])
            .collect::<Vec<_>>()
    );
    result
}

pub async fn generator(
    first: usize,
    count: usize,
    total: usize,
    orgs: usize,
    seconds: u64,
) -> Result<()> {
    let end = first.checked_add(count).context("user range overflow")?;
    ensure!(
        count > 0 && end <= total && (1..=total).contains(&orgs) && seconds > 0,
        "invalid generator range"
    );
    let mut keys = Vec::with_capacity(count);
    {
        let mut out = io::BufWriter::new(io::stdout().lock());
        for index in first..end {
            let key = Identity::generate()?;
            serde_json::to_writer(
                &mut out,
                &json!({"event":"principal","index":index,"principal":key.principal()}),
            )?;
            writeln!(out)?;
            keys.push(key);
        }
        out.flush()?;
    }
    emit(json!({"event":"keys_ready"}))?;
    let Control::Connect {
        urls,
        pin,
        profile,
        rate,
        read_interval_ms,
    } = command()?
    else {
        anyhow::bail!("expected connect")
    };
    allowed_urls(&urls)?;
    ensure!(
        matches!(profile.as_str(), "uniform" | "hot") && rate > 0 && read_interval_ms > 0,
        "invalid load profile"
    );
    let hot = profile == "hot";
    let writes = usize::try_from(
        seconds
            .checked_mul(rate)
            .context("write schedule overflow")?,
    )?;
    let target = |n: usize| {
        if hot && n % 5 != 4 {
            0
        } else if hot {
            (n / 5) % orgs
        } else {
            n % orgs
        }
    };
    let mut wanted = vec![0u64; orgs];
    for n in 0..writes {
        wanted[target(n)] += 1;
    }
    let connect = |(i, key): (usize, Identity)| {
        let url = &urls[i % urls.len()];
        let pin = &pin;
        async move {
            let s = open(key, url, pin, user_org(i, orgs, hot)).await
                .with_context(|| format!("enrolling user {i}"))?;
            Ok::<_, anyhow::Error>((i, s))
        }
    };
    let mut pending = (first..end).zip(keys);
    let mut sessions = Vec::with_capacity(count);
    for assignment in pending.by_ref().take(urls.len()) {
        let (i, s) = connect(assignment).await?;
        let probe = timeout(Duration::from_secs(10), s.conn.call("tickets.list", ())).await??;
        ensure!(
            verify(probe.get(), s.org)?.0 == 0,
            "fixture probe saw a changed page"
        );
        sessions.push((i, s));
    }
    let probe_reads = sessions.len();
    let calibration_conn = sessions[0].1.conn.clone();
    let others: Vec<_> = stream::iter(pending)
        .map(connect)
        .buffer_unordered(32)
        .try_collect()
        .await?;
    sessions.extend(others);
    let before = calibrate(&calibration_conn).await?;
    let offset = (before.0 + before.1) / 2;
    let latency = Arc::new(Mutex::new(histogram()));
    let invalid_clock = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let delivery = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (done, closed) = tokio::sync::watch::channel(false);
    let mut consumers = JoinSet::new();
    let mut held = Vec::with_capacity(count);
    let mut writers = HashMap::new();
    let mut expected = 0u64;
    let mut drivers = Vec::with_capacity(count);
    for (index, session) in sessions {
        let org = session.org;
        if index < orgs {
            writers.insert(index, session.conn.clone());
        }
        let goal = wanted[org];
        expected += goal;
        let received = delivery.clone();
        let (latency, invalid_clock) = (latency.clone(), invalid_clock.clone());
        let mut stop = closed.clone();
        let mut items = session.stream;
        consumers.spawn(async move {
            let mut seq=0;
            loop {
                tokio::select!{ biased;
                    item=items.next()=>{
                        let item=item.context("watch closed")??;
                        let arrival=i128::from(clock_us());
                        let (next,stamp)=verify(item.get(),org)?;
                        let delta=arrival+offset-i128::from(stamp);
                        if arrival+before.1<i128::from(stamp) {invalid_clock.store(true,std::sync::atomic::Ordering::Relaxed);}
                        latency.lock().unwrap().record(u64::try_from(delta.max(0))?)?;
                        ensure!(next==seq+1 && next<=goal,"unexpected sequence for user {index}: {next} after {seq}, goal {goal}");
                        seq=next;
                        received.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                    }
                    _=stop.changed()=>break
                }
            }
            ensure!(seq==goal,"incomplete watcher {index}");
            Ok::<_,anyhow::Error>(())
        });
        held.push((index, org, session.conn));
        drivers.push(session.driver);
    }
    emit(json!({"event":"ready","sessions":count}))?;
    let epoch = loop {
        match command()? {
            Control::Clock => emit(json!({"event":"clock","unix_us":clock_us()}))?,
            Control::Start { unix_us } => break unix_us,
            _ => anyhow::bail!("expected clock or start"),
        }
    };
    let now = Instant::now();
    let delay = epoch
        .checked_sub(clock_us())
        .context("start deadline already passed")?;
    let start = now + Duration::from_micros(delay);
    let finish = start + Duration::from_secs(seconds);
    let reads = Arc::new(Mutex::new(histogram()));
    let mut readers = JoinSet::new();
    for (index, org, conn) in &held {
        let (conn, org, index, reads) = (conn.clone(), *org, *index, reads.clone());
        readers.spawn(async move {
            let mut due = start
                + Duration::from_secs_f64(
                    read_interval_ms as f64 / 1000. * index as f64 / total as f64,
                );
            while due < finish {
                sleep_until(due).await;
                let value = timeout(Duration::from_secs(10), conn.call("tickets.list", ()))
                    .await
                    .with_context(|| format!("user {index}, org {org}: read exceeded 10 seconds"))?
                    .with_context(|| format!("user {index}, org {org}: read failed"))?;
                verify(value.get(), org)?;
                reads
                    .lock()
                    .unwrap()
                    .record(due.elapsed().as_micros() as u64)?;
                due += Duration::from_millis(read_interval_ms);
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    let schedule = (0..writes)
        .map(|n| (n, target(n)))
        .filter(|(_, org)| writers.contains_key(org));
    let acks = scheduled_writes(start, rate, schedule, &wanted, |org| {
        let response = writers[&org].call("tickets.advance", ());
        async move { Ok(response.await?.get().parse::<u64>()?) }
    })
    .await?;
    sleep_until(finish).await;
    while let Some(joined) = readers.join_next().await {
        joined??;
    }
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(joined) = consumers.try_join_next() {
                joined??;
                anyhow::bail!("consumer exited early");
            }
            if delivery.load(std::sync::atomic::Ordering::Relaxed) == expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out draining authenticated watch deliveries")??;
    let after = calibrate(&calibration_conn).await?;
    let valid_clock = before.1 >= after.0
        && after.1 >= before.0
        && !invalid_clock.load(std::sync::atomic::Ordering::Relaxed);
    let timing = valid_clock.then(|| distribution(&latency.lock().unwrap()));
    let metrics = if let Some(observer) = writers.get(&0) {
        Some(serde_json::from_str::<Value>(
            timeout(Duration::from_secs(10), observer.call("bench.metrics", ()))
                .await??
                .get(),
        )?)
    } else {
        None
    };
    emit(
        json!({"event":"result","first":first,"sessions":count,"expected_deliveries":expected,"deliveries":delivery.load(std::sync::atomic::Ordering::Relaxed),
        "reads":distribution(&reads.lock().unwrap()),"writes":distribution(&acks),"server_counters":metrics,
        "port_probe_reads":probe_reads,"commit_to_receive":timing,"clock":{"before_offset_range_us":[before.0 as i64,before.1 as i64],"after_offset_range_us":[after.0 as i64,after.1 as i64],"valid":valid_clock}}),
    )?;
    ensure!(matches!(command()?, Control::Drop), "expected drop barrier");
    done.send_replace(true);
    while let Some(joined) = consumers.join_next().await {
        joined??;
    }
    drop(held);
    drop(writers);
    for driver in drivers {
        driver.abort();
        let _ = driver.await;
    }
    emit(json!({"event":"closed"}))
}

/// Open each write at its scheduled time. The pinned worker admits at most four
/// writes per second and an acknowledgement expires ten seconds after opening.
pub(super) async fn scheduled_writes<F, A>(
    start: Instant,
    rate: u64,
    schedule: impl Iterator<Item = (usize, usize)>,
    wanted: &[u64],
    mut open: F,
) -> Result<hdrhistogram::Histogram<u64>>
where
    F: FnMut(usize) -> A,
    A: Future<Output = Result<u64>> + Send + 'static,
{
    const MAX_IN_FLIGHT: usize = 64;

    let mut pending = JoinSet::new();
    let mut revisions: HashMap<usize, HashSet<u64>> = HashMap::new();
    let mut acks = histogram();
    let mut record = |(org, revision, elapsed): (usize, u64, Duration)| -> Result<()> {
        ensure!(
            (1..=wanted[org]).contains(&revision),
            "out-of-range revision {revision} for org {org}"
        );
        ensure!(
            revisions.entry(org).or_default().insert(revision),
            "duplicate revision {revision} for org {org}"
        );
        acks.record(elapsed.as_micros() as u64)?;
        Ok(())
    };
    for (index, org) in schedule {
        let due = start + Duration::from_secs_f64(index as f64 / rate as f64);
        sleep_until(due).await;
        while let Some(joined) = pending.try_join_next() {
            record(joined??)?;
        }
        ensure!(
            pending.len() < MAX_IN_FLIGHT,
            "generator write backlog reached {MAX_IN_FLIGHT}"
        );
        // Connection::call opens immediately, before its response future is polled.
        let deadline = Instant::now() + Duration::from_secs(10);
        let response = open(org);
        pending.spawn(async move {
            let revision = tokio::time::timeout_at(deadline, response)
                .await
                .with_context(|| {
                    format!(
                        "scheduled write {index}, org {org}: acknowledgement exceeded 10 seconds"
                    )
                })?
                .with_context(|| format!("scheduled write {index}, org {org}: call failed"))?;
            Ok::<_, anyhow::Error>((org, revision, due.elapsed()))
        });
    }
    while let Some(joined) = pending.join_next().await {
        record(joined??)?;
    }
    for (org, seen) in revisions {
        ensure!(
            seen.len() as u64 == wanted[org],
            "missing revisions for org {org}"
        );
    }
    Ok(acks)
}

#[cfg(test)]
mod write_schedule_tests {
    use super::*;

    #[tokio::test]
    async fn opens_second_write_before_first_ack_and_validates_reordered_revisions() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut sequence = 0;
        let result = timeout(
            Duration::from_secs(2),
            scheduled_writes(
                Instant::now(),
                100,
                [(0, 0), (1, 0)].into_iter(),
                &[2],
                |_| {
                    sequence += 1;
                    let number = sequence;
                    let gate = Arc::clone(&gate);
                    async move {
                        if number == 1 {
                            gate.notified().await;
                            Ok(2)
                        } else {
                            gate.notify_one();
                            Ok(1)
                        }
                    }
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn rejects_duplicate_revision() {
        let error = scheduled_writes(
            Instant::now(),
            100,
            [(0, 0), (1, 0)].into_iter(),
            &[2],
            |_| async { Ok(1) },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("duplicate revision"));
    }
}
