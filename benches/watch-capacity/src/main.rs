#[cfg(target_os = "linux")]
mod cpu;
mod distributed;
mod service;
#[cfg(target_os = "linux")]
#[path = "../cpu-scaling/projection.rs"]
mod projection;

use anyhow::{Context, Result, bail, ensure};
use futures::StreamExt;
#[cfg(target_os = "linux")]
use futures::{TryStreamExt, stream};
use hdrhistogram::Histogram;
use plexus::{
    Connection, Items,
    tls::{Dial, Identity},
};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(target_os = "linux")]
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
#[cfg(target_os = "linux")]
use tokio::{
    task::JoinSet,
    time::{Instant, sleep, sleep_until, timeout},
};

// First give each organization a member. In the skew scenario one fifth of
// subsequent users also belong to the target of four out of five writes.
fn user_org(i: usize, orgs: usize, hot: bool) -> usize {
    if hot && i >= orgs && i % 5 == 0 {
        0
    } else {
        i % orgs
    }
}

fn clock_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(target_os = "linux")]
struct Fixture(Child);
#[cfg(target_os = "linux")]
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(target_os = "linux")]
fn fixture(keys: &[Identity], orgs: usize, rows: usize, timing_dir: Option<&tempfile::TempDir>) -> Result<(Fixture, String, String)> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.args(["serve", &orgs.to_string(), &rows.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    if let Some(dir) = timing_dir {
        cmd.env("WATCH_TIMING_WINDOW_FILE", dir.path().join("window.json"))
            .env("WATCH_TIMING_SKIP_READS", "0");
    }
    let mut child = Fixture(cmd.spawn()?);
    let mut input = child.0.stdin.take().unwrap();
    for key in keys {
        writeln!(input, "{}", key.principal())?;
    }
    drop(input);
    let mut line = String::new();
    BufReader::new(child.0.stdout.take().unwrap()).read_line(&mut line)?;
    let (url, pin) = line
        .trim()
        .split_once(' ')
        .context("fixture exited before listening")?;
    Ok((child, url.into(), pin.into()))
}

#[cfg(target_os = "linux")]
fn process(pid: u32) -> Result<Value> {
    let dir = format!("/proc/{pid}");
    let status = fs::read_to_string(format!("{dir}/status"))?;
    let kib = |name: &str| -> Result<u64> {
        Ok(status
            .lines()
            .find(|l| l.starts_with(name))
            .context("missing /proc field")?
            .split_whitespace()
            .nth(1)
            .context("missing value")?
            .parse()?)
    };
    let stat = fs::read_to_string(format!("{dir}/stat"))?;
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .context("stat")?
        .1
        .split_whitespace()
        .collect();
    Ok(
        json!({"rss_kib":kib("VmRSS:")?, "peak_rss_kib":kib("VmHWM:")?,
       "cpu_ticks":fields[11].parse::<u64>()?+fields[12].parse::<u64>()?,
       "fds":fs::read_dir(format!("{dir}/fd"))?.count()}),
    )
}

fn histogram() -> Histogram<u64> {
    Histogram::new(3).unwrap()
}
fn stats(h: &Histogram<u64>) -> Value {
    if h.is_empty() {
        return json!({"n":0});
    }
    json!({"n":h.len(),"p50_us":h.value_at_quantile(0.50),"p95_us":h.value_at_quantile(0.95),
        "p99_us":h.value_at_quantile(0.99),"max_us":h.max()})
}

struct Session {
    conn: Connection,
    driver: tokio::task::JoinHandle<std::io::Result<()>>,
    stream: Items,
    org: usize,
}

#[derive(serde::Deserialize)]
struct ReceivedRow<'a> {
    id: usize,
    #[serde(borrow)]
    title: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    status: std::borrow::Cow<'a, str>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ReceivedPage<'a> {
    org: usize,
    sequence: u64,
    committed_us: u64,
    #[serde(borrow)]
    rows: Vec<ReceivedRow<'a>>,
}

fn verify(raw: &str, org: usize) -> Result<(u64, u64)> {
    let page: ReceivedPage<'_> = serde_json::from_str(raw)?;
    ensure!(page.org == org, "cross-org page");
    let prefix = format!("Organization {org} ticket ");
    ensure!(page.rows.len() == 50, "wrong page length");
    for (i, row) in page.rows.iter().enumerate() {
        let suffix = row.title.strip_prefix(&prefix);
        ensure!(
            row.id == i && suffix.and_then(|id| id.parse().ok()) == Some(i),
            "wrong page rows"
        );
        if i != 0 || page.sequence == 0 {
            ensure!(row.status == "Ready", "stale ticket status");
        } else {
            ensure!(row.status == format!("Revision {}", page.sequence), "stale ticket status");
        }
    }
    Ok((page.sequence, page.committed_us))
}

async fn open(key: Identity, url: &str, pin: &str, org: usize) -> Result<Session> {
    let (channel, _) = Dial::new(&key, pin)?.connect(url).await?;
    let (conn, drive) = Connection::new(channel, None);
    let driver = tokio::spawn(drive);
    let mut stream = conn.stream("tickets.watch", ())?;
    let first = stream.next().await.context("watch closed during open")??;
    ensure!(verify(first.get(), org)?.0 == 0, "unexpected initial state");
    Ok(Session {
        conn,
        driver,
        stream,
        org,
    })
}

#[cfg(target_os = "linux")]
async fn run(users: usize, orgs: usize, seconds: u64, rows: usize) -> Result<()> {
    ensure!(
        users > 0 && (1..=users).contains(&orgs) && rows >= 50 && seconds > 0,
        "invalid rung"
    );
    let profile = std::env::var("WATCH_PROFILE").unwrap_or_else(|_| "uniform".into());
    let workload = std::env::var("WATCH_WORKLOAD").unwrap_or_else(|_| "mixed".into());
    ensure!(
        matches!(workload.as_str(), "mixed" | "reads" | "updates"),
        "unknown WATCH_WORKLOAD"
    );
    let hot = profile == "hot";
    let mut population = vec![0; orgs];
    for i in 0..users {
        population[user_org(i, orgs, hot)] += 1;
    }
    let keys = (0..users)
        .map(|_| Identity::generate())
        .collect::<std::io::Result<Vec<_>>>()?;
    let timing_dir = (std::env::var("WATCH_SERVER_TIMING").as_deref() == Ok("1"))
        .then(tempfile::tempdir)
        .transpose()?;
    let (server, urls, pin) = fixture(&keys, orgs, rows, timing_dir.as_ref())?;
    let urls: Vec<_> = urls.split(',').collect();
    let pid = server.0.id();
    let baseline = process(pid)?;
    eprintln!("opening {users} WSS sessions on {} listener(s)", urls.len());
    let mut meter = match std::env::var("WATCH_CPU").as_deref() {
        Ok("1") => Some(cpu::Meter::attach(pid)?),
        Err(std::env::VarError::NotPresent) | Ok("0") => None,
        _ => bail!("WATCH_CPU must be 0 or 1"),
    };
    let mut sample = || meter.as_mut().map(cpu::Meter::sample).transpose();
    let cpu_start = sample()?;
    let open_start = Instant::now();
    let mut sessions: Vec<_> = stream::iter(keys.into_iter().enumerate())
        .map(|(i, key)| {
            let (url, pin) = (urls[i % urls.len()], &pin);
            async move {
                let session = timeout(
                    Duration::from_secs(20),
                    open(key, url, pin, user_org(i, orgs, hot)),
                )
                .await
                .with_context(|| format!("initial watch timed out for user {i}"))??;
                Ok::<_, anyhow::Error>((i, session))
            }
        })
        .buffer_unordered(32)
        .try_collect()
        .await?;
    sessions.sort_by_key(|s| s.0);
    let open_ms = open_start.elapsed().as_millis();
    let cpu_open = sample()?;
    let idle_start = process(pid)?;
    eprintln!("idle: {users} authenticated sockets and first pages, server={idle_start}");
    sleep(Duration::from_secs(2)).await;
    let cpu_idle = sample()?;
    let idle_end = process(pid)?;
    let delivery = Arc::new(Mutex::new(histogram()));
    let read_latency = Arc::new(Mutex::new(histogram()));
    let mut consumers = JoinSet::new();
    let mut readers = JoinSet::new();
    let mut drivers = Vec::new();
    let held: Vec<_> = sessions.iter().map(|(_, s)| s.conn.clone()).collect();
    let writers: Vec<_> = sessions
        .iter()
        .take(orgs)
        .map(|(_, s)| s.conn.clone())
        .collect();
    let lead = Duration::from_millis(100);
    let start = Instant::now() + lead;
    let length = Duration::from_secs(seconds);
    if let Some(dir) = timing_dir.as_ref() {
        let window = json!({
            "start_unix_us": clock_us() + lead.as_micros() as u64,
            "duration_us": seconds.checked_mul(1_000_000).context("duration overflow")?,
        });
        let pending = dir.path().join("window.tmp");
        fs::write(&pending, serde_json::to_vec(&window)?)?;
        fs::rename(pending, dir.path().join("window.json"))?;
    }
    let rate: u64 = std::env::var("WATCH_WRITES_PER_SECOND")
        .unwrap_or_else(|_| "4".into())
        .parse()?;
    let interval_ms: u64 = std::env::var("WATCH_READ_INTERVAL_MS")
        .unwrap_or_else(|_| "10000".into())
        .parse()?;
    ensure!(
        rate > 0 && interval_ms > 0 && matches!(profile.as_str(), "uniform" | "hot"),
        "invalid workload"
    );
    let writes = if workload == "reads" {
        0
    } else {
        usize::try_from(seconds.checked_mul(rate).context("duration overflow")?)?
    };
    let target_org = |w: usize| {
        if profile == "hot" && w % 5 != 4 {
            0
        } else if profile == "hot" {
            (w / 5) % orgs
        } else {
            w % orgs
        }
    };
    let mut target_counts = vec![0; orgs];
    for w in 0..writes {
        target_counts[target_org(w)] += 1;
    }
    // Keep consuming even after a watch's expected last change. Close it only
    // when the harness finishes, so all connections stay open throughout load.
    let (done, closed) = tokio::sync::watch::channel(false);
    let cpu_load_start = sample()?;
    for (index, session) in sessions {
        drivers.push(session.driver);
        let mut updates = session.stream;
        let org = session.org;
        let delivered = delivery.clone();
        let mut stop = closed.clone();
        let expected_for_watch = target_counts[org];
        consumers.spawn(async move {
            let mut previous = 0;
            loop {
                tokio::select! {
                    biased;
                    item=updates.next()=> {
                        let item = item.context("watch closed during load")??;
                        let arrived = clock_us();
                        let (seq, stamp) = verify(item.get(), org)?;
                        ensure!(seq == previous + 1, "missing or duplicate update for {index}: {previous} -> {seq}");
                        previous = seq;
                        delivered.lock().unwrap().record(arrived.checked_sub(stamp).context("system clock stepped")?)?;
                    },
                    _ = stop.changed() => break,
                }
            }
            let expected = expected_for_watch;
            ensure!(previous == expected as u64, "watch {index} got {previous}, expected {expected}");
            Ok::<_, anyhow::Error>(())
        });
        if workload == "updates" {
            continue;
        }
        let measured = read_latency.clone();
        readers.spawn(async move {
            let mut scheduled = start
                + Duration::from_secs_f64(
                    interval_ms as f64 / 1000.0 * index as f64 / users as f64,
                );
            while scheduled < start + length {
                sleep_until(scheduled).await;
                let result = timeout(
                    Duration::from_secs(10),
                    session.conn.call("tickets.list", ()),
                )
                .await
                .with_context(|| format!("ticket read timed out for user {index}"))??;
                let elapsed = scheduled.elapsed();
                verify(result.get(), org)?;
                measured
                    .lock()
                    .unwrap()
                    .record(elapsed.as_micros() as u64)?;
                scheduled += Duration::from_millis(interval_ms);
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    eprintln!("load: {users} watches; starting scheduled reads and writes");
    let mut expected = 0;
    let mut written = vec![0; orgs];
    let mut write_latency = histogram();
    for w in 0..writes {
        let due = start + Duration::from_secs_f64(w as f64 / rate as f64);
        sleep_until(due).await;
        let org = target_org(w);
        written[org] += 1;
        let reply = timeout(
            Duration::from_secs(10),
            writers[org].call("tickets.advance", ()),
        )
        .await.with_context(|| format!("write {w} to organization {org} exceeded 10s; completed reads={}, observed deliveries={}, server={:?}", read_latency.lock().unwrap().len(), delivery.lock().unwrap().len(), process(pid)))??;
        ensure!(
            reply.get().parse::<usize>()? == written[org],
            "write failed"
        );
        write_latency.record(due.elapsed().as_micros() as u64)?;
        expected += population[org];
    }
    sleep_until(start + length).await;
    while let Some(result) = readers.join_next().await {
        result??;
    }
    let grace = Instant::now() + Duration::from_secs(5);
    while delivery.lock().unwrap().len() < expected as u64 && Instant::now() < grace {
        sleep(Duration::from_millis(10)).await;
    }
    let elapsed_s = start.elapsed().as_secs_f64();
    let cpu_load_end = sample()?;
    let cpu_phases = if let (Some(m), Some(seed), Some(open), Some(idle), Some(load), Some(end)) = (
        &meter,
        cpu_start,
        cpu_open,
        cpu_idle,
        cpu_load_start,
        cpu_load_end,
    ) {
        let mut units = vec![];
        if workload == "reads" {
            units.push(("read", read_latency.lock().unwrap().len()));
        } else if workload == "updates" {
            units.push(("committed_update_including_fanout", writes as u64));
            units.push(("delivered_page_including_commit", expected as u64));
        }
        Some(json!({
            "scope":"fixture process; retired user instructions, process CPU includes user and kernel",
            "open":m.report(&seed,&open,&[("authenticated_watch_with_initial_page", users as u64)])?,
            "idle":m.report(&open,&idle,&[])?,
            "load":m.report(&load,&end,&units)?
        }))
    } else {
        None
    };
    let load_end = process(pid)?;
    let client_load_end = process(std::process::id())?;
    let baseline_fds = baseline["fds"].as_u64().context("baseline fd count")?;
    ensure!(
        load_end["fds"].as_u64().context("load fd count")? >= baseline_fds + users as u64,
        "sockets closed during load"
    );
    let metrics: Value = serde_json::from_str(held[0].call("bench.metrics", ()).await?.get())?;
    done.send_replace(true);
    while let Some(result) = consumers.join_next().await {
        result??;
    }
    ensure!(
        delivery.lock().unwrap().len() == expected as u64,
        "incomplete fan-out"
    );
    drop(held);
    drop(writers);
    for task in drivers {
        task.abort();
        let _ = task.await;
    }
    sleep(Duration::from_secs(2)).await;
    println!(
        "{}",
        json!({"users":users,"authenticated_wss":users,"initial_watches":users,"loopback_listeners":urls.len(),"largest_org_users":population.iter().max(),"server_counters":metrics,"organizations":orgs,
        "tickets_per_org":rows,"total_tickets":orgs*rows,"workload":workload,"cpu_phases":cpu_phases,"scheduled_load_s":seconds,"load_and_drain_s":elapsed_s, "profile":profile,"evaluation":std::env::var("WATCH_EVALUATION").unwrap_or_else(|_|"per-watch".into()), "offered_reads_per_s":if workload == "updates" {0.0} else {users as f64*1000.0/interval_ms as f64},"offered_writes_per_s":if workload=="reads" {0} else {rate},"scheduled_write_to_ack":stats(&write_latency),
        "writes":writes,"open_ms":open_ms,"expected_deliveries":expected,
        "read_scheduled_to_receive":stats(&read_latency.lock().unwrap()),
        "commit_to_receive":stats(&delivery.lock().unwrap()),
        "server":{"seeded":baseline,"idle_start":idle_start,"idle_end":idle_end,"load_end":load_end,"after_close":process(pid)?},
        "generator_load_end":client_load_end})
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let variable = if args.first().is_some_and(|mode| mode.starts_with("serve")) {
        "WATCH_SERVER_THREADS"
    } else {
        "WATCH_GENERATOR_THREADS"
    };
    let threads: usize = std::env::var(variable).unwrap_or_else(|_| "4".into()).parse()?;
    ensure!((1..=256).contains(&threads), "{variable} must be in 1..=256");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    match args.as_slice() {
        #[cfg(target_os = "linux")]
        [mode, lanes, seconds] if mode == "projection-ceiling" => {
            projection::run(lanes.parse()?, seconds.parse()?)
        }
        [mode, orgs, rows] if mode == "serve" => runtime.block_on(service::run(
            orgs.parse()?,
            rows.parse()?,
            distributed::Access::loopback(),
        )),
        [mode, orgs, rows, bind, sources] if mode == "serve-external" => {
            let access = distributed::Access::new(bind, sources)?;
            runtime.block_on(service::run(orgs.parse()?, rows.parse()?, access))
        }
        [mode, first, count, total, orgs, seconds] if mode == "generator" => {
            let outcome = runtime.block_on(distributed::generator(
                first.parse()?,
                count.parse()?,
                total.parse()?,
                orgs.parse()?,
                seconds.parse()?,
            ));
            if let Err(error) = &outcome {
                let reason: String = format!("{error:#}").chars().take(1024).collect();
                println!("{}", serde_json::json!({"event":"error","message":reason}));
            }
            outcome
        }
        #[cfg(target_os = "linux")]
        [users, orgs, seconds, rows] => runtime.block_on(run(
            users.parse()?,
            orgs.parse()?,
            seconds.parse()?,
            rows.parse()?,
        )),
        _ => bail!(
            "usage: watch-capacity USERS ORGS SECONDS ROWS (Linux) | projection-ceiling LANES SECONDS (Linux) | serve-external ORGS ROWS BIND_IP SOURCE_IPS | generator FIRST COUNT TOTAL ORGS SECONDS"
        ),
    }
}
