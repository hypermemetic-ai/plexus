//! pace › Its speed is known against today's. `cargo bench --features tokio --bench pace`
//!
//! The shapes of plexus-bench's `call.one_item` (bench.once, tickets.health's
//! answer) and `stream.10k.64B` (bench.count: 10,000 items `{"n","pad"}`,
//! the pad 64 `x`), each in memory (a pair) and over a loopback WebSocket,
//! and a long stream, `stream.1M.64B` (a million such items, a few runs), so
//! the steady rate is seen and not only the first windows. Server and caller
//! share one process and one tokio runtime of 4 workers; the caller runs in
//! a task, as a program's would, not on the thread that called `block_on`.
//!
//! Each is measured twice: methods registered by hand on a `Server`
//! (`memory`, `websocket`), and the same shapes written as an activation,
//! `#[plexus::activation]`, served as `plexus::main` serves one, on a thread
//! of its own (`served`, `served ws`). With `--features tls` the same workload also
//! runs over WSS, with separate cold connections and wire/record measurements.

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use plexus::{Channel, Connection, Server, raw};
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde_json::json;

const WARM: usize = 2_000;
const CALLS: usize = 20_000;
const ITEMS: u64 = 10_000;
const STREAMS: usize = 21;
const LONG: u64 = 1_000_000;
const LONGS: usize = 5;

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

/// The same two methods, as an author writes them.
#[plexus::activation]
pub mod served {
    use futures::{Stream, StreamExt};

    /// What `tickets.health` answers.
    pub struct Health {
        pub r#type: String,
        pub version: String,
        pub unix_now: u64,
        pub ticket_count: u32,
    }

    /// One item of `count`.
    pub struct Counted {
        pub n: u64,
        pub pad: String,
    }

    #[derive(Default)]
    pub struct Bench;

    impl Bench {
        pub fn once(&self) -> Health {
            Health { r#type: "health".into(), version: "0.6.2".into(), unix_now: 1_793_258_669, ticket_count: 1518 }
        }

        /// `to` items, each `size` pad.
        pub fn count(&self, to: u64, size: u32) -> impl Stream<Item = Counted> {
            let pad = "x".repeat(size as usize);
            futures::stream::iter(0..to).map(move |n| Counted { n, pad: pad.clone() })
        }
    }
}

fn served() -> Server {
    let mut server = Server::new();
    served::serve(served::Bench, &mut server, "bench");
    server
}

fn bench() -> Server {
    let mut server = Server::new();
    server.method("bench.once", |_| async { raw(&json!({ "type": "health", "version": "0.6.2", "unix_now": 1_793_258_669u64, "ticket_count": 1518 })) });
    server.stream("bench.count", |params| {
        let p: serde_json::Value = serde_json::from_str(params.get()).unwrap();
        let (to, pad) = (p["to"].as_u64().unwrap(), Arc::<str>::from("x".repeat(p["size"].as_u64().unwrap() as usize)));
        futures::stream::iter(0..to).map(move |n| raw(&Item { n, pad: &pad }))
    });
    server
}

async fn measure(name: &str, conn: &Connection) {
    for _ in 0..WARM {
        conn.call("bench.once", ()).await.unwrap();
    }
    let mut us = Vec::with_capacity(CALLS);
    for _ in 0..CALLS {
        let t = Instant::now();
        conn.call("bench.once", ()).await.unwrap();
        us.push(t.elapsed().as_secs_f64() * 1e6);
    }
    us.sort_by(f64::total_cmp);
    let (p50, p99) = (us[CALLS / 2], us[CALLS * 99 / 100]);

    let rates = streams(conn, ITEMS, STREAMS).await;
    let (top, median) = (rates[18], rates[STREAMS / 2]);
    let long = streams(conn, LONG, LONGS).await;
    let (long_top, long_median) = (long[LONGS - 1], long[LONGS / 2]);
    println!(
        "{name:<10} call.one_item p50 {p50:>6.1} µs  p99 {p99:>6.1} µs   stream.10k.64B {top:>9.0} items/s (19th of 21; median {median:.0})   stream.1M.64B {long_top:>9.0} items/s (best of {LONGS}; median {long_median:.0})"
    );
}

/// The rate of `runs` streams of `items` items each, sorted.
async fn streams(conn: &Connection, items: u64, runs: usize) -> Vec<f64> {
    let mut rates = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        let mut stream = conn.stream("bench.count", json!({ "to": items, "size": 64 })).unwrap();
        let mut n = 0;
        while let Some(item) = stream.next().await {
            item.unwrap();
            n += 1;
        }
        assert_eq!(n, items);
        rates.push(items as f64 / t.elapsed().as_secs_f64());
    }
    rates.sort_by(f64::total_cmp);
    rates
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let measured = runtime.spawn(async {
        let server = Arc::new(bench());

        let (near, far) = plexus::pair();
        tokio::spawn(server.serve(far));
        measure("memory", &connect(near)).await;

        let bound = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), server).await.unwrap();
        measure("websocket", &connect(plexus::ws::connect(&format!("ws://{bound}/")).await.unwrap())).await;

        let server = Arc::new(served());
        let (near, far) = plexus::pair();
        tokio::spawn(server.serve(far));
        measure("served", &connect(near)).await;

        let bound = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), server.clone()).await.unwrap();
        measure("served ws", &connect(plexus::ws::connect(&format!("ws://{bound}/")).await.unwrap())).await;

        #[cfg(feature = "tls")]
        tls::measure(Arc::new(bench()), server).await;
    });
    runtime.block_on(measured).unwrap();
}

fn connect(channel: Channel) -> Connection {
    let (conn, driving) = Connection::new(channel, None);
    tokio::spawn(driving);
    conn
}

/// The same over `wss://`: TLS 1.3, each end proving an Ed25519 key. Then
/// what connecting costs, what an item weighs on the wire, and what TLS
/// alone costs, sealing and opening records in memory.
#[cfg(feature = "tls")]
mod tls {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Instant;

    use futures::StreamExt;
    use plexus::Server;
    use plexus::tls::{Dial, Identity, Listen};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const CONNECTS: usize = 200;

    pub async fn measure(hand: Arc<Server>, served: Arc<Server>) {
        let (me, them) = (Identity::generate().unwrap(), Identity::generate().unwrap());
        let dial = Dial::new(&me, them.principal()).unwrap();
        let hand_at = Listen::new(&them, [me.principal()]).unwrap().spawn("127.0.0.1:0".parse().unwrap(), hand.clone()).await.unwrap();
        let (channel, _) = dial.connect(&format!("wss://{hand_at}/")).await.unwrap();
        super::measure("wss", &super::connect(channel)).await;
        let served_at = Listen::new(&them, [me.principal()]).unwrap().spawn("127.0.0.1:0".parse().unwrap(), served).await.unwrap();
        let (channel, _) = dial.connect(&format!("wss://{served_at}/")).await.unwrap();
        super::measure("served wss", &super::connect(channel)).await;

        let ws_at = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), hand.clone()).await.unwrap();
        let ws = timed(|| async { drop(plexus::ws::connect(&format!("ws://{ws_at}/")).await.unwrap()) }).await;
        let wss = timed(|| async { drop(dial.connect(&format!("wss://{hand_at}/")).await.unwrap()) }).await;
        let first_ws = timed(|| async {
            let conn = super::connect(plexus::ws::connect(&format!("ws://{ws_at}/")).await.unwrap());
            conn.call("bench.once", ()).await.unwrap();
        })
        .await;
        let first_wss = timed(|| async {
            let conn = super::connect(dial.connect(&format!("wss://{hand_at}/")).await.unwrap().0);
            conn.call("bench.once", ()).await.unwrap();
        })
        .await;
        println!("connect    ws  p50 {:>6.1} µs  p99 {:>6.1} µs   wss  p50 {:>6.1} µs  p99 {:>6.1} µs   ({CONNECTS} each, one after another)", ws.0, ws.1, wss.0, wss.1);
        println!("connect and one call   ws  p50 {:>6.1} µs  p99 {:>6.1} µs   wss  p50 {:>6.1} µs  p99 {:>6.1} µs", first_ws.0, first_ws.1, first_wss.0, first_wss.1);

        let plain_proxy = counting_proxy(ws_at).await;
        let plain = plexus::ws::connect(&format!("ws://{}/", plain_proxy.0)).await.unwrap();
        let weight = weigh(plain, plain_proxy.1).await;
        let tls_proxy = counting_proxy(hand_at).await;
        let (secure, _) = dial.connect(&format!("wss://{}/", tls_proxy.0)).await.unwrap();
        let secure_weight = weigh(secure, tls_proxy.1).await;
        println!("stream.10k.64B downstream wire bytes/item: ws {weight:.2}, wss {secure_weight:.2} (after handshake and warmup)");
        for write in [1 << 10, 16 << 10, 64 << 10] {
            let (rate, suite) = plexus::tls::records(write, 256 << 20).unwrap();
            println!("records in memory, {:>2} KiB a write: {rate:>7.0} MB/s sealed and opened ({suite}); {:.1} ns for an item's {weight:.0} bytes", write >> 10, weight / rate * 1e3);
        }
    }

    /// The p50 and p99 of `CONNECTS` runs of `f`, in µs.
    async fn timed<F: std::future::Future<Output = ()>>(f: impl Fn() -> F) -> (f64, f64) {
        let mut us = Vec::with_capacity(CONNECTS);
        for _ in 0..CONNECTS {
            let t = Instant::now();
            f().await;
            us.push(t.elapsed().as_secs_f64() * 1e6);
        }
        us.sort_by(f64::total_cmp);
        (us[CONNECTS / 2], us[CONNECTS * 99 / 100])
    }

    /// Count bytes below both WebSocket and TLS framing.
    async fn counting_proxy(at: SocketAddr) -> (SocketAddr, Arc<std::sync::atomic::AtomicU64>) {
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let via = proxy.local_addr().unwrap();
        let counted = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let tally = counted.clone();
        tokio::spawn(async move {
            let (mut near, _) = proxy.accept().await.unwrap();
            let mut far = tokio::net::TcpStream::connect(at).await.unwrap();
            near.set_nodelay(true).unwrap();
            far.set_nodelay(true).unwrap();
            let (mut near_r, mut near_w) = near.split();
            let (mut far_r, mut far_w) = far.split();
            let up = tokio::io::copy(&mut near_r, &mut far_w);
            let down = async {
                let mut buf = vec![0u8; 1 << 16];
                loop {
                    let n = far_r.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    tally.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                    near_w.write_all(&buf[..n]).await.unwrap();
                }
            };
            let _ = tokio::join!(up, down);
        });
        (via, counted)
    }

    async fn weigh(channel: super::Channel, counted: Arc<std::sync::atomic::AtomicU64>) -> f64 {
        let conn = super::connect(channel);
        conn.call("bench.once", ()).await.unwrap();
        let before = counted.load(std::sync::atomic::Ordering::Relaxed);
        let mut items = conn.stream("bench.count", json!({ "to": super::ITEMS, "size": 64 })).unwrap();
        let mut received = 0;
        while let Some(item) = items.next().await {
            item.unwrap();
            received += 1;
        }
        assert_eq!(received, super::ITEMS);
        (counted.load(std::sync::atomic::Ordering::Relaxed) - before) as f64 / super::ITEMS as f64
    }
}
