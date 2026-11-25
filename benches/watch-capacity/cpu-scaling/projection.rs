//! Server-only projection and response encoding ceiling. No WSS, driver,
//! wire dispatch, or client response parsing in the measured interval.
use crate::{
    cpu::Meter,
    histogram,
    service::{Store, tickets::Tickets},
    verify,
};
use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use plexus::{Caller, tls::Identity};
use serde_json::json;
use std::{
    hint::black_box,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

pub fn run(lanes: usize, seconds: u64) -> Result<()> {
    ensure!(
        (1..=64).contains(&lanes) && (1..=600).contains(&seconds),
        "invalid projection rung"
    );
    let evaluation = std::env::var("WATCH_EVALUATION").unwrap_or_else(|_| "per-watch".into());
    let keys = (0..lanes)
        .map(|_| Identity::generate().map(|key| key.principal().to_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let store = Arc::new(Store::new(&keys, lanes, 100)?);
    let (ready, readiness) = mpsc::channel();
    let (finished, results) = mpsc::channel();
    let mut gates = Vec::new();
    let mut workers = Vec::new();
    for (org, principal) in keys.into_iter().enumerate() {
        let (begin, start) = mpsc::channel::<(Instant, Instant)>();
        let (release, hold) = mpsc::channel();
        gates.push((begin, release));
        let (store, ready, finished) = (store.clone(), ready.clone(), finished.clone());
        workers.push(
            thread::Builder::new()
                .name(format!("projection-{org}"))
                .spawn(move || -> Result<()> {
                    let activation = Tickets { store };
                    let caller = Caller::new(principal);
                    let mut subscription = Box::pin(activation.watch(&caller));
                    let first =
                        futures::executor::block_on(subscription.next()).context("watch closed")?;
                    let first = first.into_raw();
                    let check = activation.list(&caller, None).into_raw();
                    ensure!(
                        verify(first.get(), org)?.0 == 0 && verify(check.get(), org)?.0 == 0,
                        "wrong initial projection"
                    );
                    let size = check.get().len();
                    ready.send(())?;
                    drop(ready);
                    let (begin, end) = start.recv()?;
                    thread::sleep(begin.saturating_duration_since(Instant::now()));
                    let mut operations = 0u64;
                    let mut latency = histogram();
                    while Instant::now() < end {
                        let sample = operations.is_multiple_of(128).then(Instant::now);
                        let result = activation.list(&caller, None).into_raw();
                        ensure!(
                            black_box(result.get()).len() == size,
                            "stable page length changed"
                        );
                        if let Some(sample) = sample {
                            latency.record(sample.elapsed().as_nanos() as u64)?;
                        }
                        operations += 1;
                    }
                    finished.send((org, operations, size, latency))?;
                    drop(finished);
                    hold.recv()?;
                    Ok(())
                })?,
        );
    }
    drop(ready);
    drop(finished);
    for _ in 0..lanes {
        readiness
            .recv()
            .context("projection worker failed before its preflight")?;
    }
    let mut meter = Meter::attach(std::process::id())?;
    let start = Instant::now() + Duration::from_millis(20);
    let end = start + Duration::from_secs(seconds);
    for (begin, _) in &gates {
        begin.send((start, end))?;
    }
    thread::sleep(start.saturating_duration_since(Instant::now()));
    let before = meter.sample()?;
    let mut ops = 0;
    let mut by_lane = vec![0; lanes];
    let mut bytes = 0;
    let mut h = histogram();
    for _ in 0..lanes {
        let (org, n, size, latency) = results.recv()?;
        ops += n;
        bytes += n * size as u64;
        by_lane[org] = n;
        h.add(&latency)?;
    }
    let after = meter.sample()?;
    for (_, release) in gates {
        release.send(())?;
    }
    for w in workers {
        w.join()
            .map_err(|_| anyhow::anyhow!("projection worker panicked"))??;
    }
    let counters = meter.report(
        &before,
        &after,
        &[("authorized_projection_and_encode", ops)],
    )?;
    let wall = counters["wall_s"].as_f64().context("phase duration")?;
    let latency = json!({"n":h.len(),"p50_ns":h.value_at_quantile(0.5),"p95_ns":h.value_at_quantile(0.95),"p99_ns":h.value_at_quantile(0.99)});
    let encoding = std::env::var("WATCH_ENCODING").unwrap_or_else(|_| "shared".into());
    println!(
        "{}",
        json!({"type":"projection","lanes":lanes,"evaluation":evaluation,"encoding":encoding,"operations":ops,
        "operations_per_lane":by_lane,"operations_per_s":ops as f64/wall,"bytes":bytes,
        "process_cpu_cores":counters["process_cpu_ns"].as_u64().unwrap() as f64/1e9/wall,
        "sampled_projection_and_encode":latency,"counters":counters})
    );
    Ok(())
}
