//! trace › A call's time stage by stage. `cargo bench --features tokio,trace --bench trace`
//!
//! A call and a stream of 64-byte items, to a method registered by hand and
//! to one served by `#[plexus::activation]`, in memory (a pair) and over a
//! loopback WebSocket; server and caller in one process, so one clock times
//! both ends. Each way runs on a tokio runtime of 4 workers, the caller in a
//! task of its own as `pace` has it, then again on a one-thread runtime.
//!
//! For each it prints every stage's median and p99 (the time since the
//! stage before it, of the same call or item), how often a stage ran on
//! another thread than the one before, their sum against the call's p50
//! measured from outside with tracing on and off, and the sequence as a
//! mermaid `sequenceDiagram`. With `PLEXUS_TRACE_JSON=FILE` every figure is
//! appended to FILE as a JSON line.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use plexus::trace::{self, END, Record, Stage};
use plexus::{Channel, Connection, Server, raw};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_json::json;

const WARM: usize = 2_000;
const CALLS: usize = 20_000;
const ITEMS: u64 = 100_000;
const SIZE: u64 = 64;

/// What the benchmark serves through `#[plexus::activation]`: the same
/// answer and the same items as the methods registered by hand.
#[plexus::activation]
pub mod bench {
    pub struct Health {
        pub kind: String,
        pub version: String,
        pub unix_now: u64,
        pub ticket_count: u32,
    }

    pub struct Item {
        pub n: u64,
        pub pad: String,
    }

    #[derive(Default)]
    pub struct Bench;

    impl Bench {
        pub fn once(&self) -> Health {
            Health { kind: "health".into(), version: "0.6.2".into(), unix_now: 1_793_258_669, ticket_count: 1518 }
        }

        pub fn count(&self, to: u64, size: u64) -> impl Iterator<Item = Item> {
            let pad = "x".repeat(size as usize);
            (0..to).map(move |n| Item { n, pad: pad.clone() })
        }
    }
}

struct Item<'a> {
    n: u64,
    pad: &'a str,
}

impl Serialize for Item<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut item = s.serialize_struct("Item", 2)?;
        item.serialize_field("n", &self.n)?;
        item.serialize_field("pad", self.pad)?;
        item.end()
    }
}

fn by_hand() -> Server {
    let mut server = Server::new();
    server.method("bench.once", |_| async { raw(&json!({ "kind": "health", "version": "0.6.2", "unix_now": 1_793_258_669u64, "ticket_count": 1518 })) });
    server.stream("bench.count", |params| {
        let p: serde_json::Value = serde_json::from_str(params.get()).unwrap();
        let (to, pad) = (p["to"].as_u64().unwrap(), Arc::<str>::from("x".repeat(p["size"].as_u64().unwrap() as usize)));
        futures::stream::iter(0..to).map(move |n| raw(&Item { n, pad: &pad }))
    });
    server
}

fn served() -> Server {
    let mut server = Server::new();
    bench::serve(bench::Bench, &mut server, "bench");
    server
}

/// Which of a call's frames a stage is stamped on.
#[derive(Clone, Copy, PartialEq)]
enum Slot {
    /// The call itself and its open.
    Call,
    /// Its first item, or for a stream each item.
    Item,
    End,
}

/// One stage of a sequence: where it ends, and what happened since the
/// stage before it. A stage aside is timed from the stage before it too,
/// but is not one the sequence passes through: what a task spends after
/// it has woken the next, which may already be running.
struct Step {
    stage: Stage,
    slot: Slot,
    at: char,
    what: &'static str,
    aside: bool,
}

fn step(stage: Stage, slot: Slot, at: char, what: &'static str) -> Step {
    Step { stage, slot, at, what, aside: false }
}

fn aside(stage: Stage, slot: Slot, at: char, what: &'static str) -> Step {
    Step { stage, slot, at, what, aside: true }
}

#[derive(Clone, Copy, PartialEq)]
enum Way {
    Memory,
    Socket,
}

/// The stages of one call, in the order they happen.
fn call_steps(served: bool, way: Way) -> Vec<Step> {
    use Slot::*;
    use Stage::*;
    let mut s = vec![
        step(Called, Call, 'C', "call entered"),
        aside(OpenQueued, Call, 'C', "the caller serializes the parameters, writes the open, queues it and wakes the driver"),
    ];
    match way {
        Way::Memory => s.extend([
            step(Taken, Call, 'D', "write the open, driver task wakes, its pump takes the open"),
            aside(Flushed, Call, 'D', "the pump flushes, waking the server task"),
            step(Read, Call, 'S', "the frame crosses, server task wakes and has it"),
        ]),
        Way::Socket => s.extend([
            step(Taken, Call, 'D', "write the open, driver task wakes, its pump takes the open"),
            step(WsWrite, Call, 'D', "make the WebSocket frame, masked"),
            step(WsWritten, Call, 'K', "write syscall"),
            step(WsReadEntry, Call, 'S', "loopback, the reactor wakes the server task, it reads"),
            step(WsReadReturn, Call, 'S', "read syscall"),
            step(WsMessage, Call, 'S', "parse the WebSocket message"),
            step(Read, Call, 'S', "hand the frame to the server loop"),
        ]),
    }
    s.extend([step(Started, Call, 'S', "decode the open, register the call, hand it to run"), step(Running, Call, 'S', "the call future is first polled")]);
    if served {
        s.extend([
            step(JobBoxed, Call, 'S', "find the method, read the parameters into Rust values, box the job"),
            aside(JobSent, Call, 'S', "the server task sends the job and wakes the activation thread"),
            step(JobTaken, Call, 'A', "send the job, activation thread wakes, takes it"),
            step(JobPolled, Call, 'A', "spawn the job, first poll"),
            step(MethodDone, Call, 'A', "the method runs, its answer is serialized"),
            aside(AnswerSent, Call, 'A', "the activation thread sends the answer and wakes the server task"),
            step(Answered, Item, 'S', "send the answer, server task wakes, has it"),
        ]);
    } else {
        s.push(step(Answered, Item, 'S', "find the method, it runs, its answer is serialized"));
    }
    s.extend([step(Queued, Item, 'S', "write the item frame, queue it to the pump"), step(Queued, End, 'S', "write the end frame, queue it"), step(Taken, Item, 'S', "the pump takes the item"), step(Taken, End, 'S', "the pump takes the end")]);
    match way {
        Way::Memory => s.extend([aside(Flushed, Item, 'S', "the pump flushes both, waking the driver"), step(Received, Item, 'D', "the frames cross, driver task wakes, reads the item head")]),
        Way::Socket => s.extend([
            step(WsWrite, Item, 'S', "make both WebSocket frames"),
            step(WsWritten, Item, 'K', "write syscall"),
            step(WsReadEntry, Item, 'D', "loopback, the reactor wakes the driver task, it reads"),
            step(WsReadReturn, Item, 'D', "read syscall"),
            step(WsMessage, Item, 'D', "parse the item message"),
            step(Received, Item, 'D', "read the item head"),
        ]),
    }
    s.push(step(Delivered, Item, 'D', "add the item to the call run"));
    if way == Way::Socket {
        s.push(step(WsMessage, End, 'D', "parse the end message"));
    }
    s.extend([
        step(Received, End, 'D', "read the end frame"),
        step(InInbox, Item, 'D', "hand item and end into the call inbox"),
        aside(Woke, Item, 'D', "the driver wakes the caller task"),
        step(Took, Item, 'C', "wake the caller, caller task takes the item"),
        step(Returned, Call, 'C', "take the end, return the answer"),
    ]);
    s
}

/// The stages of one item of a stream, from the server asking for it.
fn item_steps(served: bool, way: Way) -> Vec<Step> {
    use Slot::*;
    use Stage::*;
    let mut s = Vec::new();
    if served {
        s.extend([
            step(Asked, Item, 'A', "credit in hand, the method is asked for its next item"),
            step(ItemMade, Item, 'A', "the method makes the item"),
            step(ItemSerialized, Item, 'A', "serialize it"),
            step(ItemPushed, Item, 'A', "put it in the run shared with the server"),
            aside(ItemSent, Item, 'A', "the activation thread wakes the server task, if it waits"),
            step(Answered, Item, 'S', "the server task takes it from the run"),
        ]);
    } else {
        s.extend([step(Asked, Item, 'S', "credit taken, the next item asked for"), step(Answered, Item, 'S', "make the item, serialize it")]);
    }
    s.extend([step(Queued, Item, 'S', "write the item frame, queue it to the pump"), step(Taken, Item, 'S', "the pump takes it, once the batch is made")]);
    match way {
        Way::Memory => s.extend([aside(Flushed, Item, 'S', "the pump flushes the batch, waking the driver"), step(Received, Item, 'D', "the batch crosses, driver task wakes, reads the item head")]),
        Way::Socket => s.extend([
            step(WsWrite, Item, 'S', "make the WebSocket frames of the batch"),
            step(WsWritten, Item, 'K', "write syscall"),
            step(WsReadEntry, Item, 'D', "loopback, the reactor wakes the driver task, it reads"),
            step(WsReadReturn, Item, 'D', "read syscall"),
            step(WsMessage, Item, 'D', "parse the message"),
            step(Received, Item, 'D', "read the item head"),
        ]),
    }
    s.extend([
        step(Delivered, Item, 'D', "add the item to the call run"),
        step(InInbox, Item, 'D', "hand the run into the call inbox"),
        aside(Woke, Item, 'D', "the driver wakes the caller task"),
        step(Took, Item, 'C', "wake the caller, caller task takes the item"),
    ]);
    s
}

/// Every stamp by its call, its slot and its stage: when and on which thread.
type Stamps = HashMap<(u32, u32, Stage), (Instant, u16)>;

fn index(records: &[Record]) -> Stamps {
    let mut stamps = Stamps::with_capacity(records.len());
    for r in records {
        stamps.entry((r.key.0, r.key.1, r.stage)).or_insert((r.at, r.thread));
    }
    stamps
}

/// What one way measured.
struct Measured {
    scenario: String,
    steps: Vec<Step>,
    /// Each step's durations, µs, sorted; the first step's are empty. A
    /// duration is signed: a stage stamped on one thread may be stamped
    /// before the one it follows on another, and is kept as it was.
    each: Vec<Vec<f64>>,
    /// How often each step ran on another thread than the step before.
    crossed: Vec<usize>,
    /// How many calls or items each step was not stamped for.
    missing: Vec<usize>,
    /// How many were traced, how many had every stage of the sequence, how
    /// many had a stage before the one it follows.
    traced: usize,
    whole: usize,
    backwards: usize,
    /// First stage to last, from the stamps, µs, sorted.
    spans: Vec<f64>,
    stamps_each: f64,
    /// From outside: a call's p50 µs, or a stream's items a second; traced and not.
    outside_on: f64,
    outside_off: f64,
    /// A call's mean µs from outside, traced.
    outside_mean: f64,
    call: bool,
    /// A stream's work per item, paid once an item however it is batched.
    work: Vec<Work>,
}

fn measure(scenario: String, steps: Vec<Step>, records: &[Record], keys: impl Iterator<Item = (u32, u32)>, (outside_on, outside_mean): (f64, f64), outside_off: f64, call: bool) -> Measured {
    let stamps = index(records);
    let n = steps.len();
    let (mut each, mut crossed, mut missing) = (vec![Vec::new(); n], vec![0; n], vec![0; n]);
    let (mut traced, mut whole, mut backwards, mut spans) = (0, 0, 0, Vec::new());
    for (id, item) in keys {
        traced += 1;
        let slot = |s: Slot| match s {
            Slot::Call => 0,
            Slot::Item => item,
            Slot::End => END,
        };
        let found: Vec<Option<(Instant, u16)>> = steps.iter().map(|s| stamps.get(&(id, slot(s.slot), s.stage)).copied()).collect();
        for (i, f) in found.iter().enumerate() {
            missing[i] += f.is_none() as usize;
        }
        if steps.iter().zip(&found).any(|(s, f)| !s.aside && f.is_none()) {
            continue;
        }
        whole += 1;
        let (mut last, mut first, mut back) = (None::<(Instant, u16)>, None, false);
        for (i, (s, f)) in steps.iter().zip(&found).enumerate() {
            let Some((at, thread)) = *f else { continue };
            if let Some((was, on)) = last {
                each[i].push(signed_us(at, was));
                crossed[i] += (thread != on) as usize;
                back |= !s.aside && at < was;
            }
            if !s.aside {
                first.get_or_insert(at);
                last = Some((at, thread));
            }
        }
        backwards += back as usize;
        spans.push(signed_us(last.unwrap().0, first.unwrap()));
    }
    each.iter_mut().for_each(|v| v.sort_by(f64::total_cmp));
    spans.sort_by(f64::total_cmp);
    Measured { scenario, steps, each, crossed, missing, traced, whole, backwards, spans, stamps_each: records.len() as f64 / traced.max(1) as f64, outside_on, outside_off, outside_mean, call, work: Vec::new() }
}

fn signed_us(at: Instant, was: Instant) -> f64 {
    if at >= was { (at - was).as_secs_f64() * 1e6 } else { -(was - at).as_secs_f64() * 1e6 }
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

fn pct(v: &[f64], p: usize) -> f64 {
    if v.is_empty() { f64::NAN } else { v[(v.len() * p / 100).min(v.len() - 1)] }
}

fn participant(at: char) -> &'static str {
    match at {
        'C' => "caller task",
        'D' => "caller driver",
        'K' => "kernel loopback",
        'S' => "server task",
        'A' => "activation thread",
        _ => unreachable!(),
    }
}

impl Measured {
    /// The sum of the medians of the stages the sequence passes through.
    fn sum(&self) -> f64 {
        self.steps.iter().zip(&self.each).skip(1).filter(|(s, _)| !s.aside).map(|(_, v)| pct(v, 50)).sum()
    }

    /// The sum of their means: the mean of first stage to last, exactly.
    fn sum_of_means(&self) -> f64 {
        self.steps.iter().zip(&self.each).skip(1).filter(|(s, _)| !s.aside).map(|(_, v)| mean(v)).sum()
    }

    fn print(&self, stamp_ns: f64) {
        println!("\n### {}\n", self.scenario);
        println!(
            "{} {} traced, {} with every stage, {} with a stage stamped before the one it follows; {:.1} stamps each",
            self.traced,
            if self.call { "calls" } else { "items" },
            self.whole,
            self.backwards,
            self.stamps_each
        );
        println!("\n| # | at | stage | p50 µs | p99 µs | mean µs | on another thread | stamped before the stage it follows |");
        println!("| --: | -- | -- | --: | --: | --: | --: | --: |");
        let mut k = 0;
        for (i, (s, v)) in self.steps.iter().zip(&self.each).enumerate() {
            let number = if s.aside { "aside".to_owned() } else { k.to_string() };
            k += !s.aside as usize;
            if i == 0 {
                println!("| {number} | {} | {} | | | | | |", s.at, s.what);
            } else {
                let crossed = 100.0 * self.crossed[i] as f64 / v.len().max(1) as f64;
                let before = 100.0 * v.iter().filter(|d| **d < 0.0).count() as f64 / v.len().max(1) as f64;
                println!("| {number} | {} | {} | {:.2} | {:.2} | {:.2} | {crossed:.0}% | {before:.1}% |", s.at, s.what, pct(v, 50), pct(v, 99), mean(v));
            }
        }
        let (sum, span) = (self.sum(), pct(&self.spans, 50));
        let stamps_us = self.stamps_each * stamp_ns / 1000.0;
        let missing: usize = self.missing.iter().sum();
        if missing > 0 {
            println!("\n{missing} stamps missing; a call or item missing one of the sequence is left out");
        }
        println!("\nsum of the medians {sum:.2} µs; first stage to last from the stamps, p50 {span:.2} µs, mean {:.2} µs (the sum of the means)", self.sum_of_means());
        if self.call {
            println!(
                "a call from outside: p50 {:.2} µs traced, {:.2} µs untraced; mean {:.2} µs traced; sum of medians / traced p50 = {:.3}; the stamps of one call cost about {:.2} µs",
                self.outside_on,
                self.outside_off,
                self.outside_mean,
                sum / self.outside_on,
                stamps_us
            );
        } else {
            println!(
                "the stream from outside: {:.0} items a second traced, {:.0} untraced, so {:.3} µs an item untraced; the stamps of one item cost about {:.2} µs",
                self.outside_on,
                self.outside_off,
                1e6 / self.outside_off,
                stamps_us
            );
        }
    }

    fn mermaid(&self) {
        let at: Vec<char> = ['C', 'D', 'K', 'S', 'A'].into_iter().filter(|c| self.steps.iter().any(|s| s.at == *c)).collect();
        let last = *at.last().unwrap();
        println!("\n```mermaid\nsequenceDiagram\n  autonumber");
        for c in &at {
            println!("  participant {c} as {}", participant(*c));
        }
        let outside = if self.call {
            format!("a call, sum of the stage medians {:.2} µs, measured from outside p50 {:.2} µs traced, {:.2} µs untraced", self.sum(), self.outside_on, self.outside_off)
        } else {
            format!("an item, sum of the stage medians {:.2} µs, the stream {:.0} items a second traced, {:.0} untraced", self.sum(), self.outside_on, self.outside_off)
        };
        println!("  Note over C,{last}: {} - {outside}", self.scenario);
        let mut from = self.steps[0].at;
        for (s, v) in self.steps.iter().zip(&self.each).skip(1) {
            if s.aside {
                println!("  Note over {}: {} - {:.2} µs", s.at, s.what, pct(v, 50));
            } else {
                println!("  {from}->>{}: {} - {:.2} µs", s.at, s.what, pct(v, 50));
                from = s.at;
            }
        }
        println!("```");
    }

    /// The work-per-item table: each row's median and p99, and each
    /// participant's sum, against 1/rate.
    fn print_work(&self, stamp_ns: f64) {
        if self.work.is_empty() {
            return;
        }
        // A row timed from one stamp to the next holds the second stamp's
        // cost; one shared by a batch holds a share of it.
        let stamp = stamp_ns / 1000.0;
        let less = |w: &Work| (pct(&w.v, 50) - if w.how.contains("shared") { 0.0 } else { stamp }).max(0.0);
        println!("\nwork per item, paid once an item however items are batched:\n");
        println!("| at | work | p50 µs | p50 less one stamp µs | p99 µs | how it is read from the stamps |");
        println!("| -- | -- | --: | --: | --: | -- |");
        for w in &self.work {
            println!("| {} | {} | {:.3} | {:.3} | {:.3} | {} |", w.at, w.what, pct(&w.v, 50), less(w), pct(&w.v, 99), w.how);
        }
        let mut at: Vec<char> = Vec::new();
        for w in &self.work {
            if !at.contains(&w.at) {
                at.push(w.at);
            }
        }
        let sums: Vec<String> = at
            .iter()
            .map(|c| {
                let mine = || self.work.iter().filter(|w| w.at == *c);
                format!("{} {:.3} ({:.3} less the stamps)", participant(*c), mine().map(|w| pct(&w.v, 50)).sum::<f64>(), mine().map(less).sum::<f64>())
            })
            .collect();
        let total: f64 = self.work.iter().map(|w| pct(&w.v, 50)).sum();
        let total_less: f64 = self.work.iter().map(less).sum();
        println!(
            "\nmedians summed, µs an item: {}; all of it {total:.3} ({total_less:.3} less the stamps). 1/rate untraced {:.3} µs, traced {:.3} µs",
            sums.join(", "),
            1e6 / self.outside_off,
            1e6 / self.outside_on
        );
    }

    fn json(&self, out: &mut impl Write, commit: &str, load: f64) {
        let mut line = |stage: &str, metric: &str, value: f64, unit: &str| {
            let v = json!({ "app": "plexus", "scenario": self.scenario, "stage": stage, "metric": metric, "value": value, "unit": unit, "commit": commit, "load": load });
            writeln!(out, "{v}").unwrap();
        };
        for (s, v) in self.steps.iter().zip(&self.each).skip(1) {
            let slot = match s.slot {
                Slot::Call => "",
                Slot::Item => ".item",
                Slot::End => ".end",
            };
            let name = format!("{}{:?}{slot}", if s.aside { "aside." } else { "" }, s.stage);
            line(&name, "p50_us", pct(v, 50), "us");
            line(&name, "p99_us", pct(v, 99), "us");
            line(&name, "mean_us", mean(v), "us");
        }
        for w in &self.work {
            line(&format!("work.{}.{}", w.at, w.what), "p50_us", pct(&w.v, 50), "us");
        }
        line("sum", "p50_us", self.sum(), "us");
        line("sum", "mean_us", self.sum_of_means(), "us");
        line("span", "p50_us", pct(&self.spans, 50), "us");
        if self.call {
            line("outside", "p50_us", self.outside_on, "us");
            line("outside", "mean_us", self.outside_mean, "us");
            line("outside.untraced", "p50_us", self.outside_off, "us");
        } else {
            line("outside", "rate", self.outside_on, "items/s");
            line("outside.untraced", "rate", self.outside_off, "items/s");
        }
    }
}

/// `n` calls: their p50 and mean, µs.
async fn calls(conn: &Connection, n: usize) -> (f64, f64) {
    let mut us = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        conn.call("bench.once", ()).await.unwrap();
        us.push(t.elapsed().as_secs_f64() * 1e6);
    }
    us.sort_by(f64::total_cmp);
    (us[n / 2], mean(&us))
}

async fn stream(conn: &Connection) -> f64 {
    let t = Instant::now();
    let mut items = conn.stream("bench.count", json!({ "to": ITEMS, "size": SIZE })).unwrap();
    let mut n = 0;
    while let Some(item) = items.next().await {
        item.unwrap();
        n += 1;
    }
    assert_eq!(n, ITEMS);
    ITEMS as f64 / t.elapsed().as_secs_f64()
}

/// Run every way on `runtime`, the caller in a task of its own.
fn run(runtime: tokio::runtime::Runtime, name: &'static str) -> Vec<Measured> {
    let measured = runtime.spawn(async move {
        let mut out = Vec::new();
        for (kind, is_served) in [("hand", false), ("served", true)] {
            for way in [Way::Memory, Way::Socket] {
                let server = Arc::new(if is_served { served() } else { by_hand() });
                let channel = match way {
                    Way::Memory => {
                        let (near, far) = plexus::pair();
                        tokio::spawn(server.serve(far));
                        near
                    }
                    Way::Socket => {
                        let bound = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), server).await.unwrap();
                        plexus::ws::connect(&format!("ws://{bound}/")).await.unwrap()
                    }
                };
                let conn = connect(channel);
                let place = match way {
                    Way::Memory => "memory",
                    Way::Socket => "websocket",
                };
                let suffix = if name == "multi" { String::new() } else { format!(".{name}") };

                calls(&conn, WARM).await;
                let off = calls(&conn, CALLS).await.0;
                trace::start();
                let on = calls(&conn, CALLS).await;
                let records = trace::stop();
                let ids: Vec<(u32, u32)> = called(&records);
                out.push(measure(format!("call.{kind}.{place}{suffix}"), call_steps(is_served, way), &records, ids.into_iter(), on, off, true));

                let off = stream(&conn).await;
                trace::start();
                let on = stream(&conn).await;
                let records = trace::stop();
                let call = records.iter().find(|r| r.stage == Stage::Asked).map(|r| r.key.0).expect("the stream was stamped");
                let mut m = measure(format!("stream.{kind}.{place}{suffix}"), item_steps(is_served, way), &records, (1..=ITEMS as u32).map(|n| (call, n)), (on, f64::NAN), off, false);
                m.work = work(&index(&records), call, is_served, way);
                out.push(m);
            }
        }
        out
    });
    runtime.block_on(measured).unwrap()
}

/// The calls that were stamped as called, each with its one item.
fn called(records: &[Record]) -> Vec<(u32, u32)> {
    let mut ids: Vec<(u32, u32)> = records.iter().filter(|r| r.stage == Stage::Called).map(|r| (r.key.0, 1)).collect();
    ids.sort();
    ids
}

fn connect(channel: Channel) -> Connection {
    let (conn, driving) = Connection::new(channel, None);
    tokio::spawn(driving);
    conn
}

/// What a stamp costs, ns: a million of them on this thread.
fn stamp_cost() -> (f64, f64) {
    const N: u32 = 1_000_000;
    let t = Instant::now();
    for i in 0..N {
        std::hint::black_box(Instant::now());
        std::hint::black_box(i);
    }
    let clock = t.elapsed().as_secs_f64() * 1e9 / N as f64;
    trace::start();
    let t = Instant::now();
    for i in 0..N {
        trace::stamp((1, i), Stage::Called);
    }
    let stamp = t.elapsed().as_secs_f64() * 1e9 / N as f64;
    trace::stop();
    (clock, stamp)
}

fn main() {
    let commit = {
        let dir = env!("CARGO_MANIFEST_DIR");
        let git = |args: &[&str]| std::process::Command::new("git").args(args).current_dir(dir).output().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned()).unwrap_or_default();
        let dirty = if git(&["status", "--porcelain", "--untracked-files=no"]).is_empty() { "" } else { "-dirty" };
        format!("plexus@{}{dirty}", git(&["rev-parse", "--short", "HEAD"]))
    };
    let load = || std::fs::read_to_string("/proc/loadavg").ok().and_then(|l| l.split_whitespace().next()?.parse::<f64>().ok()).unwrap_or(f64::NAN);
    let (clock, stamp) = stamp_cost();
    println!("# plexus trace, {commit}, load {:.2} on {} cores", load(), std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));
    println!("\nA stamp costs {stamp:.1} ns, of which reading the clock is {clock:.1} ns. Stage times below include the stamps made inside them; none is subtracted.");

    let mut all = Vec::new();
    for (name, runtime) in [
        ("multi", tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()),
        ("one-thread", tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()),
    ] {
        println!("\n## {}", if name == "multi" { "a runtime of 4 workers" } else { "a one-thread runtime" });
        let load_now = load();
        for m in run(runtime, name) {
            m.print(stamp);
            m.print_work(stamp);
            m.mermaid();
            all.push((m, load_now));
        }
    }
    println!("\nthreads stamped on: {}", trace::threads().join(", "));

    if let Ok(path) = std::env::var("PLEXUS_TRACE_JSON") {
        let mut out = std::fs::OpenOptions::new().create(true).append(true).open(&path).unwrap();
        for (m, load) in &all {
            m.json(&mut out, &commit, *load);
        }
        println!("figures appended to {path}");
    }
}

/// One row of a stream's work per item.
struct Work {
    at: char,
    what: &'static str,
    how: &'static str,
    /// µs an item, sorted.
    v: Vec<f64>,
}

/// What each item costs where it is paid, from the stamps of the stream
/// `call`: a stage of one item alone where the stage is the item's own
/// work; a batch's time shared by the items in it where the work is done
/// once a batch (a flush, a write, a read, a hand-in); and the gap between
/// two items of one batch where a task works through a batch item by item.
fn work(stamps: &Stamps, call: u32, served: bool, way: Way) -> Vec<Work> {
    use Stage::*;
    let at = |n: u32, stage: Stage| stamps.get(&(call, n, stage)).map(|s| s.0);
    let items = 1..=ITEMS as u32;
    let mut rows = Vec::new();
    let mut row = |at: char, what: &'static str, how: &'static str, mut v: Vec<f64>| {
        v.sort_by(f64::total_cmp);
        rows.push(Work { at, what, how, v });
    };
    // A stage of one item.
    let one = |from: Stage, to: Stage| -> Vec<f64> { items.clone().filter_map(|n| Some(signed_us(at(n, to)?, at(n, from)?))).collect() };
    // From one item's stage to the next item's, when nothing marks a batch between them.
    let next = |from: Stage, to: Stage| -> Vec<f64> { items.clone().filter_map(|n| Some(signed_us(at(n + 1, to)?, at(n, from)?))).collect() };
    // From one item's stage to the next item's, both in one batch: `batch` stamps them at one instant.
    let within = |from: Stage, to: Stage, batch: Stage| -> Vec<f64> {
        items.clone().filter(|&n| at(n, batch).is_some() && at(n, batch) == at(n + 1, batch)).filter_map(|n| Some(signed_us(at(n + 1, to)?, at(n, from)?))).collect()
    };
    // A batch's time from `from` (its first item's) to `to`, shared by its items; a batch is the items `batch` stamps at one instant.
    let shared = |from: Stage, first: bool, to: Stage, batch: Stage| -> Vec<f64> {
        let mut groups: Vec<(Instant, Vec<u32>)> = Vec::new();
        for n in items.clone() {
            let Some(b) = at(n, batch) else { continue };
            match groups.last_mut() {
                Some((t, ns)) if *t == b => ns.push(n),
                _ => groups.push((b, vec![n])),
            }
        }
        let mut v = Vec::new();
        for (_, ns) in groups {
            let n0 = if first { ns[0] } else { ns[ns.len() - 1] };
            if let (Some(a), Some(b)) = (at(n0, from), at(ns[0], to)) {
                let each = signed_us(b, a) / ns.len() as f64;
                v.extend(std::iter::repeat_n(each, ns.len()));
            }
        }
        v
    };
    if served {
        row('A', "the method makes the item", "asked to made, each item", one(Asked, ItemMade));
        row('A', "serialize it", "made to serialized, each item", one(ItemMade, ItemSerialized));
        row('A', "put it in the run", "serialized to put, each item", one(ItemSerialized, ItemPushed));
        row('A', "wake the server task, when it waits", "put to after the wake, each item", one(ItemPushed, ItemSent));
        row('A', "check the credit, ask for the next", "after the wake to the next item asked", next(ItemSent, Asked));
        row('S', "take the next item from the run, the server loop", "one item queued to the next taken, both in one pump batch", within(Queued, Answered, pump_batch(way)));
    } else {
        row('S', "make the item, serialize it", "asked to answered, each item", one(Asked, Answered));
        row('S', "take credit, ask for the next, the server loop", "one item queued to the next asked, both in one pump batch", within(Queued, Asked, pump_batch(way)));
    }
    row('S', "write the item frame, queue it to the pump", "answered to queued, each item", one(Answered, Queued));
    match way {
        Way::Memory => {
            row('S', "the pump takes it and holds it for the flush", "one item taken to the next, both in one flush", within(Taken, Taken, Flushed));
            row('S', "flush, waking the driver, per item of the batch", "last item taken to flushed, shared by the batch", shared(Taken, false, Flushed, Flushed));
            row('D', "take the frame from the batch, read its head", "one item added to the run to the next read, both in one flush", within(Delivered, Received, Flushed));
        }
        Way::Socket => {
            row('S', "the pump takes it and makes its WebSocket frame", "one item taken to the next, both in one write", within(Taken, Taken, WsWritten));
            row('S', "the last frame and the flush up to the write, per item", "last item taken to write entered, shared by the batch", shared(Taken, false, WsWrite, WsWritten));
            row('S', "the write syscall, per item", "write entered to returned, shared by the items it wrote", shared(WsWrite, true, WsWritten, WsWritten));
            row('D', "the read syscall, per item", "read entered to returned, shared by the items it read", shared(WsReadEntry, true, WsReadReturn, WsReadReturn));
            row('D', "parse the message", "one item added to the run to the next message, both from one read", within(Delivered, WsMessage, WsReadReturn));
            row('D', "read the item head", "message to head read, each item", one(WsMessage, Received));
        }
    }
    row('D', "add the item to the call run", "head read to added, each item", one(Received, Delivered));
    row('D', "hand the run into the inbox, per item", "last item added to handed in, shared by the run, with the poll that found nothing more", shared(Delivered, false, InInbox, InInbox));
    row('D', "wake the caller, per item", "handed in to woken, shared by the run", shared(InInbox, true, Woke, InInbox));
    row('C', "the caller takes the item", "one item taken to the next, both in one run", within(Took, Took, InInbox));
    rows
}

/// The stage that marks one batch of the pump's: its flush in memory, its
/// write on a socket.
fn pump_batch(way: Way) -> Stage {
    match way {
        Way::Memory => Stage::Flushed,
        Way::Socket => Stage::WsWritten,
    }
}
