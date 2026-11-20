//! WSS unary benchmark. Each direction carries frames for one call at a
//! time, so TLS records can be assigned by the socket's write/read batch.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use plexus::tls::{Dial, Identity, Listen};
use plexus::trace::{self, END, Record, Stage};
use plexus::{Connection, Server};
use serde_json::json;

use super::{CALLS, WARM, by_hand, calls, connect, mean, pct, served, signed_us};

const ROWS: [&str; 11] = [
    "caller encode, queue and driver wake",
    "request TLS seal",
    "request TLS open",
    "request TCP I/O",
    "request WebSocket, TLS plumbing and loopback handoff",
    "server dispatch, method, response queue and pump wake",
    "response TLS seal",
    "response TLS open",
    "response TCP I/O",
    "response WebSocket, TLS plumbing and loopback handoff",
    "response delivery and caller return",
];

pub(super) struct Measured {
    scenario: String,
    each: [Vec<f64>; ROWS.len()],
    traced: Vec<f64>,
    untraced_p50: f64,
    accounted: Vec<f64>,
    within: usize,
    max_error: f64,
}

async fn endpoint(server: Arc<Server>, client_id: &Identity, server_id: &Identity, traced: bool) -> Connection {
    let (listen, dial) = if traced {
        (Listen::new(server_id, [client_id.principal()]), Dial::new(client_id, server_id.principal()))
    } else {
        (Listen::new_untraced(server_id, [client_id.principal()]), Dial::new_untraced(client_id, server_id.principal()))
    };
    let bound = listen.unwrap().spawn("127.0.0.1:0".parse().unwrap(), server).await.unwrap();
    let (channel, peer) = dial.unwrap().connect(&format!("wss://{bound}/")).await.unwrap();
    assert_eq!(peer, server_id.principal());
    connect(channel)
}

pub(super) async fn run(name: &str) -> Vec<Measured> {
    let client_id = Identity::from_seed([1; 32]).unwrap();
    let server_id = Identity::from_seed([2; 32]).unwrap();
    let mut result = Vec::new();
    for (kind, is_served) in [("hand", false), ("served", true)] {
        let server = Arc::new(if is_served { served() } else { by_hand() });
        let untraced = endpoint(server.clone(), &client_id, &server_id, false).await;
        calls(&untraced, WARM).await;
        let off = calls(&untraced, CALLS).await.0;
        drop(untraced);

        let traced = endpoint(server, &client_id, &server_id, true).await;
        calls(&traced, WARM).await;
        let mut outside = Vec::with_capacity(CALLS);
        trace::start();
        for _ in 0..CALLS {
            let start = Instant::now();
            traced.call("bench.once", ()).await.unwrap();
            outside.push(signed_us(Instant::now(), start));
        }
        let records = trace::stop();
        result.push(measure(format!("call.{kind}.wss.{name}"), records, outside, off));
    }
    result
}

type Slice = (Instant, Instant);

/// For these serial calls the request and response write/read batches each
/// have a single owner. Match every individual operation in timestamp order.
fn intervals(records: &[Record], begins: Stage, ends: Stage, side: &str) -> Vec<Slice> {
    let mut a: Vec<_> = records.iter().filter(|r| r.stage == begins).map(|r| r.at).collect();
    let mut b: Vec<_> = records.iter().filter(|r| r.stage == ends).map(|r| r.at).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert!(!a.is_empty() && a.len() == b.len(), "{side}: missing or unpaired {begins:?}/{ends:?}");
    a.into_iter().zip(b).collect()
}

/// One call's wire work and the uncovered part of its transport window.
fn wire(records: &[Record], start: Instant, end: Instant, side: &str) -> [f64; 4] {
    let seal = intervals(records, Stage::TlsSealEntry, Stage::TlsSealReturn, side);
    let open = intervals(records, Stage::TlsOpenEntry, Stage::TlsOpenReturn, side);
    let mut tcp = intervals(records, Stage::TlsTcpWriteEntry, Stage::TlsTcpWriteReturn, side);
    tcp.extend(intervals(records, Stage::TlsTcpReadEntry, Stage::TlsTcpReadReturn, side));

    fn covered(slices: &mut [Slice], start: Instant, end: Instant) -> f64 {
        slices.sort_unstable();
        let mut total = 0.;
        let mut last = start;
        for &(a, b) in slices.iter() {
            // A receiving worker can progress before the sender's syscall
            // returns. Count only the portion on this call's path.
            let (lo, hi) = (a.max(last), b.min(end));
            if hi > lo {
                total += signed_us(hi, lo);
                last = hi;
            }
        }
        total
    }
    assert!(seal.iter().chain(&open).all(|&(a, b)| start <= a && a <= b && b <= end), "{side}: crypto operation outside transport window");
    let mut slices = seal;
    let sealing = covered(&mut slices, start, end);
    slices.extend(open);
    let crypto = covered(&mut slices, start, end);
    slices.extend(tcp);
    let active = covered(&mut slices, start, end);
    [sealing, crypto - sealing, active - crypto, signed_us(end, start) - active]
}

fn measure(scenario: String, records: Vec<Record>, mut traced: Vec<f64>, untraced_p50: f64) -> Measured {
    let mut calls = HashMap::<u32, Vec<_>>::new();
    for record in records {
        calls.entry(record.key.0).or_default().push(record);
    }
    let mut ids: Vec<_> = calls.iter().filter(|(_, r)| r.iter().any(|r| r.stage == Stage::Called)).map(|(&id, _)| id).collect();
    ids.sort_unstable();
    assert_eq!(ids.len(), traced.len(), "{scenario}: incomplete call tracing");

    let mut each: [Vec<f64>; ROWS.len()] = std::array::from_fn(|_| Vec::with_capacity(ids.len()));
    let mut accounted = Vec::with_capacity(ids.len());
    let mut within = 0;
    let mut max_error: f64 = 0.;
    for (id, &outside) in ids.into_iter().zip(&traced) {
        let call = &calls[&id];
        let at = |slot, stage| call.iter().find(|r| r.key.1 == slot && r.stage == stage).unwrap_or_else(|| panic!("{scenario}: call {id} missing {stage:?} at slot {slot}")).at;
        let checkpoints = [
            at(0, Stage::Called),
            at(0, Stage::Taken),
            at(0, Stage::Read),
            at(1, Stage::Taken),
            at(END, Stage::Received),
            at(0, Stage::Returned),
        ];
        assert!(checkpoints.is_sorted(), "{scenario}: call {id} checkpoints out of order");
        let (request, response): (Vec<_>, Vec<_>) = call.iter().copied().partition(|r| r.key.1 == 0);
        let [start, taken, read, reply, received, end] = checkpoints;
        let [rs, ro, rt, rw] = wire(&request, taken, read, "request");
        let [ss, so, st, sw] = wire(&response, reply, received, "response");
        let stage_times = [
            signed_us(taken, start),
            rs, ro, rt, rw,
            signed_us(reply, read),
            ss, so, st, sw,
            signed_us(end, received),
        ];
        let total: f64 = stage_times.iter().sum();
        let error = (total - outside).abs() / outside;
        within += usize::from(error <= 0.10);
        max_error = max_error.max(error);
        accounted.push(total);
        for (values, duration) in each.iter_mut().zip(stage_times) {
            values.push(duration);
        }
    }
    for values in &mut each {
        values.sort_by(f64::total_cmp);
    }
    traced.sort_by(f64::total_cmp);
    accounted.sort_by(f64::total_cmp);
    Measured { scenario, each, traced, untraced_p50, accounted, within, max_error }
}

impl Measured {
    pub(super) fn print(&self) {
        let traced = pct(&self.traced, 50);
        let accounted = pct(&self.accounted, 50);
        let explained = mean(&self.accounted) / mean(&self.traced);
        let medians: f64 = self.each.iter().map(|v| pct(v, 50)).sum();
        let n = self.traced.len();
        println!("\n### {}\n", self.scenario);
        println!("| Stage | p50 µs | p99 µs | Mean µs |");
        println!("| --- | ---: | ---: | ---: |");
        for (stage, values) in ROWS.iter().zip(&self.each) {
            println!("| {stage} | {:.3} | {:.3} | {:.3} |", pct(values, 50), pct(values, 99), mean(values));
        }
        println!("\nOutside call p50 {traced:.3} µs traced, {:.3} µs untraced ({:+.1}% measured change).", self.untraced_p50, 100. * (traced / self.untraced_p50 - 1.));
        println!("Stage totals p50 {accounted:.3} µs, sum of stage medians {medians:.3} µs. Mean fit {:.2}%. {}/{n} same-call totals within 10%, maximum error {:.2}%.", explained * 100., self.within, self.max_error * 100.);
        println!("Totals use disjoint wall time from each call. Overlapping wall time is credited once, in order of TLS seal, TLS open, then successful TCP polls. Each transport remainder includes WebSocket framing and parsing, rustls buffering, loopback and task scheduling. Independent stage medians need not sum to the median of the call totals.");
        println!("The paired samples use separate warmed connections with identical methods and keys. The untraced connection uses the original ring provider and has recording disabled. The timed calls exclude setup and handshakes.");
        assert!(self.within * 10 >= n * 9 && (0.9..=1.1).contains(&explained) && (0.9..=1.1).contains(&(accounted / traced)), "{}: WSS call totals fell outside the 10% accounting tolerance", self.scenario);
    }

    pub(super) fn json(&self, out: &mut impl Write, commit: &str, load: f64) {
        let mut line = |stage: &str, metric: &str, value: f64, unit: &str| {
            let v = json!({ "app": "plexus", "scenario": self.scenario, "stage": stage, "metric": metric, "value": value, "unit": unit, "commit": commit, "load": load });
            writeln!(out, "{v}").unwrap();
        };
        for (stage, values) in ROWS.iter().zip(&self.each) {
            line(stage, "p50_us", pct(values, 50), "us");
            line(stage, "p99_us", pct(values, 99), "us");
            line(stage, "mean_us", mean(values), "us");
        }
        line("sum.same_call", "p50_us", pct(&self.accounted, 50), "us");
        line("sum.same_call", "mean_us", mean(&self.accounted), "us");
        line("sum.same_call", "fraction_within_10_percent", self.within as f64 / self.traced.len() as f64, "ratio");
        line("outside", "p50_us", pct(&self.traced, 50), "us");
        line("outside", "mean_us", mean(&self.traced), "us");
        line("outside.untraced", "p50_us", self.untraced_p50, "us");
    }
}
