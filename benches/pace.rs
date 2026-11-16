//! pace › Its speed is known against today's. `cargo bench --features tokio --bench pace`
//!
//! The shapes of plexus-bench's `call.one_item` (bench.once, tickets.health's
//! answer) and `stream.10k.64B` (bench.count: 10,000 items `{"n","pad"}`,
//! the pad 64 `x`), each in memory (a pair) and over a loopback WebSocket.
//! Server and caller share one process and one tokio runtime of 4 workers.

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

    let mut rates = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        let t = Instant::now();
        let mut items = conn.stream("bench.count", json!({ "to": ITEMS, "size": 64 })).unwrap();
        let mut n = 0;
        while let Some(item) = items.next().await {
            item.unwrap();
            n += 1;
        }
        assert_eq!(n, ITEMS);
        rates.push(ITEMS as f64 / t.elapsed().as_secs_f64());
    }
    rates.sort_by(f64::total_cmp);
    let (top, median) = (rates[18], rates[STREAMS / 2]);
    println!("{name:<10} call.one_item p50 {p50:>7.1} µs  p99 {p99:>7.1} µs   stream.10k.64B {top:>10.0} items/s (19th of 21; median {median:.0})");
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    runtime.block_on(async {
        let server = Arc::new(bench());

        let (near, far) = plexus::pair();
        tokio::spawn(server.serve(far));
        measure("memory", &connect(near)).await;

        let bound = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), server).await.unwrap();
        measure("websocket", &connect(plexus::ws::connect(&format!("ws://{bound}/")).await.unwrap())).await;
    });
}

fn connect(channel: Channel) -> Connection {
    let (conn, driving) = Connection::new(channel, None);
    tokio::spawn(driving);
    conn
}
