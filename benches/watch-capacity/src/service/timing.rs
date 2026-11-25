//! Opt-in fixture measurements at the native channel boundary.
use anyhow::{Context as _, Result, ensure};
use futures::{Sink, TryStreamExt};
use hdrhistogram::Histogram;
use plexus::{Channel, bytes::Bytes};
use plexus_proto::{Head, Kind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

const BUCKET_US: u64 = 100_000;
const SHARDS: usize = 32;
const MAX_BUCKETS: u64 = 512;
const PENDING_LIMIT: usize = 4096;

#[derive(Clone, Copy)]
enum Operation {
    Read,
    Write,
    Update,
}
use Operation::{Read, Update, Write};
const OPS: [Operation; 3] = [Read, Write, Update];

#[derive(Default, Clone, Copy, Serialize)]
struct Counts {
    ingress: u64,
    flushed: u64,
}
#[derive(Clone, Copy)]
struct Event {
    at: u64,
    op: Operation,
    flush: bool,
    n: u64,
}
type Totals = [Counts; 3];
fn count(totals: &mut Totals, e: Event) {
    let c = &mut totals[e.op as usize];
    if e.flush {
        c.flushed += e.n
    } else {
        c.ingress += e.n
    }
}
struct Shard {
    totals: Totals,
    scheduled: Totals,
    pending: Vec<Event>,
    overflow: u64,
    latency: [Histogram<u64>; 3],
    buckets: BTreeMap<u64, Totals>,
    bucket_us: u64,
    first: Option<u64>,
    last: Option<u64>,
}
impl Default for Shard {
    fn default() -> Self {
        Self {
            totals: Default::default(),
            scheduled: Default::default(),
            pending: Vec::new(),
            overflow: 0,
            latency: std::array::from_fn(|_| Histogram::new(3).unwrap()),
            buckets: BTreeMap::new(),
            bucket_us: BUCKET_US,
            first: None,
            last: None,
        }
    }
}
#[derive(Clone, Copy)]
struct Window {
    start: u64,
    end: u64,
}
impl Window {
    fn includes(self, at: u64) -> bool {
        (self.start..self.end).contains(&at)
    }
}
impl Shard {
    fn span(&mut self, first: u64, last: u64) {
        let first = self.first.map_or(first, |v| v.min(first));
        let last = self.last.map_or(last, |v| v.max(last));
        self.first = Some(first);
        self.last = Some(last);
        let mut width = self.bucket_us;
        while last / width - first / width >= MAX_BUCKETS {
            width *= 2;
        }
        if width > self.bucket_us {
            self.bucket_us = width;
            for (at, counts) in std::mem::take(&mut self.buckets) {
                self.merge_bucket(at, &counts);
            }
        }
    }
    fn merge_bucket(&mut self, at: u64, counts: &Totals) {
        let sum = self
            .buckets
            .entry(at / self.bucket_us * self.bucket_us)
            .or_default();
        for (a, b) in sum.iter_mut().zip(counts) {
            a.ingress += b.ingress;
            a.flushed += b.flushed;
        }
    }
    fn classify(&mut self, window: Window) {
        for e in self.pending.drain(..) {
            if window.includes(e.at) {
                count(&mut self.scheduled, e);
            }
        }
    }
    fn event(&mut self, e: Event, latency: Option<u64>, window: Option<Window>, awaiting: bool) {
        if e.n == 0 {
            return;
        }
        self.span(e.at, e.at);
        count(&mut self.totals, e);
        count(
            self.buckets
                .entry(e.at / self.bucket_us * self.bucket_us)
                .or_default(),
            e,
        );
        if let Some(window) = window {
            self.classify(window);
            if window.includes(e.at) {
                count(&mut self.scheduled, e);
            }
        } else if awaiting {
            if self.pending.len() < PENDING_LIMIT {
                self.pending.push(e);
            } else {
                self.overflow += e.n;
            }
        }
        if let Some(us) = latency {
            self.latency[e.op as usize].record(us).unwrap();
        }
    }
    fn merge(&mut self, from: &Self) {
        for op in OPS {
            let i = op as usize;
            for (a, b) in [
                (&mut self.totals[i], from.totals[i]),
                (&mut self.scheduled[i], from.scheduled[i]),
            ] {
                a.ingress += b.ingress;
                a.flushed += b.flushed;
            }
            self.latency[i].add(&from.latency[i]).unwrap();
        }
        if let (Some(first), Some(last)) = (from.first, from.last) {
            self.span(first, last);
            for (at, counts) in &from.buckets {
                self.merge_bucket(*at, counts);
            }
        }
        self.overflow += from.overflow;
    }
}

pub(super) struct Timing {
    origin: Instant,
    unix: u64,
    uncertainty: u64,
    path: Option<PathBuf>,
    window: OnceLock<Window>,
    next_window_check_us: AtomicU64,
    shards: [Mutex<Shard>; SHARDS],
    next: AtomicUsize,
    // One monotonic stamp per committed revision, indexed by tenant and sequence.
    commits: Vec<Mutex<Vec<u64>>>,
    subscribers: Vec<AtomicU64>,
    skip: u64,
    read_index: AtomicU64,
    initial: AtomicU64,
    failures: AtomicU64,
    missing: AtomicU64,
}
impl Timing {
    pub(super) fn configured(orgs: usize) -> Result<Option<Arc<Self>>> {
        let enabled = std::env::var("WATCH_SERVER_TIMING").unwrap_or_else(|_| "0".into());
        ensure!(
            matches!(enabled.as_str(), "0" | "1"),
            "WATCH_SERVER_TIMING expects 0 or 1"
        );
        if enabled == "0" {
            return Ok(None);
        }
        let skip = std::env::var("WATCH_TIMING_SKIP_READS")
            .unwrap_or_else(|_| "0".into())
            .parse()?;
        let path = std::env::var_os("WATCH_TIMING_WINDOW_FILE").map(PathBuf::from);
        Ok(Some(Self::new(orgs, skip, path)))
    }
    fn new(orgs: usize, skip: u64, path: Option<PathBuf>) -> Arc<Self> {
        let a = Instant::now();
        let unix = crate::clock_us();
        let separation = a.elapsed();
        Arc::new(Self {
            origin: a + separation / 2,
            unix,
            uncertainty: 1 + separation.as_micros() as u64 / 2,
            path,
            window: OnceLock::new(),
            next_window_check_us: 0.into(),
            shards: std::array::from_fn(|_| Mutex::new(Shard::default())),
            next: AtomicUsize::new(0),
            commits: (0..orgs).map(|_| Mutex::new(Vec::new())).collect(),
            subscribers: (0..orgs).map(|_| AtomicU64::new(0)).collect(),
            skip,
            read_index: 0.into(),
            initial: 0.into(),
            failures: 0.into(),
            missing: 0.into(),
        })
    }
    fn load_window(&self) -> Result<()> {
        if self.window.get().is_some() {
            return Ok(());
        }
        #[derive(Deserialize)]
        struct Schedule {
            start_unix_us: u64,
            duration_us: u64,
        }
        let schedule: Schedule = serde_json::from_slice(&std::fs::read(
            self.path
                .as_ref()
                .context("no scheduled window configured")?,
        )?)?;
        ensure!(schedule.duration_us > 0, "empty scheduled window");
        let start = schedule
            .start_unix_us
            .checked_sub(self.unix)
            .context("window predates fixture")?;
        let end = start
            .checked_add(schedule.duration_us)
            .context("window overflows")?;
        let _ = self.window.set(Window { start, end });
        Ok(())
    }
    fn now(&self) -> u64 {
        self.origin.elapsed().as_micros() as u64
    }
    fn event(
        &self,
        shard: usize,
        at: u64,
        op: Operation,
        flush: bool,
        n: u64,
        latency: Option<u64>,
    ) {
        if self.path.is_some() && self.window.get().is_none() {
            let next = self.next_window_check_us.load(Ordering::Relaxed);
            if at >= next
                && self
                    .next_window_check_us
                    .compare_exchange(
                        next,
                        at.saturating_add(50_000),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                let _ = self.load_window();
            }
        }
        self.shards[shard % SHARDS].lock().unwrap().event(
            Event { at, op, flush, n },
            latency,
            self.window.get().copied(),
            self.path.is_some(),
        );
    }
    pub(super) fn watch(self: &Arc<Self>, org: usize) -> Watch {
        self.subscribers[org].fetch_add(1, Ordering::Relaxed);
        Watch(self.clone(), org)
    }
    pub(super) fn committed(&self, org: usize, seq: u64) {
        let at = self.now();
        let mut commits = self.commits[org].lock().unwrap();
        debug_assert_eq!(commits.len() as u64 + 1, seq);
        commits.push(at);
        drop(commits);
        self.event(
            org,
            at,
            Update,
            false,
            self.subscribers[org].load(Ordering::Relaxed),
            None,
        );
    }
    pub(super) fn snapshot(&self) -> Value {
        let err = self
            .path
            .as_ref()
            .and_then(|_| self.load_window().err())
            .map(|e| e.to_string());
        let window = self.window.get().copied();
        let mut sum = Shard::default();
        for lock in &self.shards {
            let mut s = lock.lock().unwrap();
            if let Some(w) = window {
                s.classify(w);
            }
            sum.merge(&s);
        }
        let results = |i: usize| {
            let h = &sum.latency[i];
            let c = sum.totals[i];
            json!({"ingress":c.ingress,"flushed":c.flushed,"latency_us":{"n":h.len(),"p50":h.value_at_quantile(0.5),"p95":h.value_at_quantile(0.95),"p99":h.value_at_quantile(0.99),"max":h.max()}})
        };
        let elapsed = sum.first.zip(sum.last).map(|(a, b)| b - a);
        let rate = |i: usize| {
            elapsed
                .filter(|t| *t > 0)
                .map(|t| sum.totals[i].flushed as f64 * 1_000_000.0 / t as f64)
        };
        let buckets: Vec<_> = sum
            .buckets
            .iter()
            .map(
                |(start, c)| json!({"start_us":start,"read":c[0],"write":c[1],"watch_update":c[2]}),
            )
            .collect();
        json!({"version":1,"clock":"fixture monotonic microseconds","origin_unix_us":self.unix,"origin_uncertainty_us":self.uncertainty,
            "wall_mapping_drift_us":i128::from(crate::clock_us())-i128::from(self.unix)-i128::from(self.now()),
            "boundary":"complete Open yielded by native receive stream through successful native send sink poll_flush of End; watch updates from SQLite autocommit through successful sink poll_flush of Item",
            "read":results(0),"write":results(1),"watch_update":results(2),"watch_initial_flushed":self.initial.load(Ordering::Relaxed),
            "skipped_probe_reads":self.read_index.load(Ordering::Relaxed).min(self.skip),"failures":self.failures.load(Ordering::Relaxed),
            "missing_commit_stamps":self.missing.load(Ordering::Relaxed),"window_error":err,"unclassified_early_events":sum.overflow,
            "observed":{"first_us":sum.first,"last_us":sum.last,"duration_us":elapsed},
            "rates":{"reads_per_s":rate(0),"writes_per_s":rate(1),"watch_updates_per_s":rate(2)},
            "scheduled":window.map(|w|json!({"start_us":w.start,"end_us":w.end,"duration_us":w.end-w.start,
                "read":sum.scheduled[0],"write":sum.scheduled[1],"watch_update":sum.scheduled[2]})),
            "bucket_us":sum.bucket_us,"buckets":buckets})
    }
    pub(super) fn wrap(self: &Arc<Self>, channel: Channel, org: usize) -> Channel {
        let shard = self.next.fetch_add(1, Ordering::Relaxed) % SHARDS;
        let calls = Arc::new(Mutex::new(HashMap::new()));
        let incoming = calls.clone();
        let meter = self.clone();
        let recv = channel.receive.inspect_ok(move |bytes| {
            let at = meter.now();
            let Ok(head) = Head::read(bytes) else { return };
            if head.kind == Kind::Cancel {
                incoming.lock().unwrap().remove(&head.call.0);
                return;
            }
            if head.kind != Kind::Open {
                return;
            }
            let body = &bytes[head.body];
            // Only the fixture's three generated RPC method names are tracked.
            let call = if body.starts_with(br#"["tickets.list""#) {
                if meter.read_index.fetch_add(1, Ordering::Relaxed) < meter.skip {
                    return;
                }
                Call::Unary(Read, at)
            } else if body.starts_with(br#"["tickets.advance""#) {
                Call::Unary(Write, at)
            } else if body.starts_with(br#"["tickets.list-watch""#) || body.starts_with(br#"["tickets.watch""#) {
                Call::Watch(true)
            } else {
                return;
            };
            if let Call::Unary(op, _) = call {
                meter.event(shard, at, op, false, 1, None);
            }
            incoming.lock().unwrap().insert(head.call.0, call);
        });
        let sink = Output {
            sink: channel.send,
            meter: self.clone(),
            org,
            shard,
            calls,
            pending: Vec::new(),
        };
        let mut wrapped = Channel::new(sink, recv);
        wrapped.binary_input = channel.binary_input;
        wrapped
    }
}
pub(super) struct Watch(Arc<Timing>, usize);
impl Drop for Watch {
    fn drop(&mut self) {
        self.0.subscribers[self.1].fetch_sub(1, Ordering::Relaxed);
    }
}
#[derive(Clone, Copy)]
enum Call {
    Unary(Operation, u64),
    Watch(bool),
}
enum Sample {
    Done(Operation, u64),
    Initial,
    Failed,
    Missing,
}
struct Output {
    sink: Pin<Box<dyn Sink<Bytes, Error = io::Error> + Send>>,
    meter: Arc<Timing>,
    org: usize,
    shard: usize,
    calls: Arc<Mutex<HashMap<u32, Call>>>,
    pending: Vec<Sample>,
}
/// Counted native replies use short Item and successful End forms. Reading
/// only their prefix keeps the 50-row page out of the telemetry JSON parser.
/// The protocol reader handles the remaining forms, including numbered frames.
fn outgoing(bytes: &[u8]) -> Option<(u32, Kind, &[u8])> {
    let (id, rest) = decimal(bytes.strip_prefix(b"[")?)?;
    let id = u32::try_from(id).ok()?;
    if rest == b"]" {
        return Some((id, Kind::End, &[]));
    }
    if rest.starts_with(b",{") {
        return Some((id, Kind::Item, rest[1..].strip_suffix(b"]")?));
    }
    let head = Head::read(bytes).ok()?;
    Some((head.call.0, head.kind, &bytes[head.body]))
}

fn decimal(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let n = bytes.iter().take_while(|b| b.is_ascii_digit()).count();
    if n == 0 {
        return None;
    }
    let number = std::str::from_utf8(&bytes[..n]).ok()?.parse().ok()?;
    Some((number, &bytes[n..]))
}

#[derive(Deserialize)]
struct PageRevision {
    org: usize,
    sequence: usize,
}
fn revision(body: &[u8]) -> Option<PageRevision> {
    // Field order of the generated native page. A changed layout still works.
    let prefix = || {
        let (org, rest) = decimal(body.strip_prefix(br#"{"org":"#)?)?;
        let (seq, rest) = decimal(rest.strip_prefix(br#","sequence":"#)?)?;
        if !matches!(rest.first(), Some(b',' | b'}')) {
            return None;
        }
        Some(PageRevision {
            org: org.try_into().ok()?,
            sequence: seq.try_into().ok()?,
        })
    };
    prefix().or_else(|| serde_json::from_slice(body).ok())
}

impl Output {
    fn sample(&self, bytes: &[u8]) -> Option<Sample> {
        let (id, kind, body) = outgoing(bytes)?;
        let mut calls = self.calls.lock().unwrap();
        let call = calls.get_mut(&id)?;
        match (kind, *call) {
            (Kind::Item, Call::Watch(initial)) => {
                *call = Call::Watch(false);
                if initial {
                    return Some(Sample::Initial);
                }
                drop(calls);
                let Some(rev) = revision(body) else {
                    return Some(Sample::Missing);
                };
                if rev.org != self.org {
                    return Some(Sample::Missing);
                }
                let stamp = rev
                    .sequence
                    .checked_sub(1)
                    .and_then(|i| self.meter.commits[self.org].lock().unwrap().get(i).copied());
                Some(stamp.map_or(Sample::Missing, |at| Sample::Done(Update, at)))
            }
            (Kind::End, call) => {
                calls.remove(&id);
                if !(body.is_empty() || body == b"null") {
                    return Some(Sample::Failed);
                }
                if let Call::Unary(op, at) = call {
                    Some(Sample::Done(op, at))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    fn flushed(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let at = self.meter.now();
        for s in self.pending.drain(..) {
            match s {
                Sample::Done(op, start) => {
                    self.meter
                        .event(self.shard, at, op, true, 1, Some(at - start))
                }
                Sample::Initial => {
                    self.meter.initial.fetch_add(1, Ordering::Relaxed);
                }
                Sample::Failed => {
                    self.meter.failures.fetch_add(1, Ordering::Relaxed);
                }
                Sample::Missing => {
                    self.meter.missing.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
impl Sink<Bytes> for Output {
    type Error = io::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.sink.as_mut().poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, frame: Bytes) -> io::Result<()> {
        let sample = self.sample(&frame);
        self.sink.as_mut().start_send(frame)?;
        if let Some(s) = sample {
            self.pending.push(s);
        }
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = self.sink.as_mut().poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.flushed();
        }
        result
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = self.sink.as_mut().poll_close(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.flushed();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, SinkExt, StreamExt, stream};
    use std::sync::atomic::AtomicBool;

    struct Gate(Arc<AtomicBool>);
    impl Sink<Bytes> for Gate {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, _: Bytes) -> io::Result<()> {
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.0.load(Ordering::Relaxed) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    #[tokio::test]
    async fn only_successful_flushes_complete_samples_and_batched_frames_share_one_boundary() {
        let meter = Timing::new(1, 1, None);
        let gate = Arc::new(AtomicBool::new(false));
        let frames = [
            br#"[1,1,["tickets.list"]]"#.as_slice(),
            br#"[2,1,["tickets.list"]]"#,
            br#"[3,1,["tickets.advance"]]"#,
            br#"[4,1,["tickets.list-watch"]]"#,
        ];
        let chan = Channel::new(
            Gate(gate.clone()),
            stream::iter(frames.map(|f| Ok(Bytes::from_static(f)))),
        );
        let mut channel = meter.wrap(chan, 0);
        while channel.receive.next().await.is_some() {}
        let _subscription = meter.watch(0);
        meter.committed(0, 1);
        for f in [
            b"[1]".as_slice(),
            b"[2]",
            b"[3]",
            br#"[4,{"org":0,"sequence":0}]"#,
            br#"[4,{"org":0,"sequence":1}]"#,
        ] {
            channel.send.feed(Bytes::from_static(f)).await.unwrap();
        }
        assert!(channel.send.flush().now_or_never().is_none());
        let before = meter.snapshot();
        assert_eq!(before["read"]["ingress"], 1);
        assert_eq!(before["read"]["flushed"], 0);
        assert_eq!(before["watch_update"]["ingress"], 1);
        assert_eq!(before["watch_initial_flushed"], 0);
        gate.store(true, Ordering::Relaxed);
        channel.send.flush().await.unwrap();
        channel.send.flush().await.unwrap();
        let after = meter.snapshot();
        for op in ["read", "write", "watch_update"] {
            assert_eq!(after[op]["flushed"], 1);
            assert_eq!(after[op]["latency_us"]["n"], 1);
        }
        assert_eq!(after["watch_initial_flushed"], 1);
        assert_eq!(after["skipped_probe_reads"], 1);
        assert_eq!(after["missing_commit_stamps"], 0);
    }

    #[test]
    fn hour_of_samples_rolls_up_exact_counts_in_at_most_512_bins() {
        let m = Timing::new(1, 0, None);
        for i in 0..36_000u64 {
            m.event(i as usize % SHARDS, i * BUCKET_US, Write, true, 1, Some(1));
        }
        let result = m.snapshot();
        let bins = result["buckets"].as_array().unwrap();
        assert!(bins.len() <= 512);
        assert!(result["bucket_us"].as_u64().unwrap() > BUCKET_US);
        assert_eq!(
            bins.iter()
                .map(|b| b["write"]["flushed"].as_u64().unwrap())
                .sum::<u64>(),
            36_000
        );
        assert!(
            m.shards
                .iter()
                .all(|s| s.lock().unwrap().buckets.len() <= 512)
        );
    }

    #[test]
    fn late_window_metadata_counts_half_open_bounds_and_drained_totals_separately() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("window.json");
        let m = Timing::new(1, 0, Some(path.clone()));
        m.event(0, 99, Read, false, 1, None);
        m.event(0, 100, Read, true, 1, Some(1));
        m.event(1, 199, Write, false, 1, None);
        m.event(1, 200, Write, true, 1, Some(1));
        std::fs::write(
            path,
            json!({"start_unix_us":m.unix+100,"duration_us":100}).to_string(),
        )
        .unwrap();
        let snapshot = m.snapshot();
        let w = &snapshot["scheduled"];
        assert_eq!(w["read"], json!({"ingress":0,"flushed":1}));
        assert_eq!(w["write"], json!({"ingress":1,"flushed":0}));
        assert_eq!(w["watch_update"], json!({"ingress":0,"flushed":0}));
        assert_eq!(snapshot["read"]["ingress"], 1);
        assert_eq!(snapshot["write"]["flushed"], 1);
        assert_eq!(snapshot["unclassified_early_events"], 0);
        assert_eq!(m.snapshot()["scheduled"], *w);
    }
}
