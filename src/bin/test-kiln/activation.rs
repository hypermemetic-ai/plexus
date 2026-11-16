//! A kiln registered by hand: what the runtime's tests, in memory, over
//! stdio and from a page's runtime, all call.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use plexus::{Server, fail, raw};
use serde_json::json;

pub const HASH: &str = "6b696c6e00000001";
pub const IR: &str = r#"{"hash":"6b696c6e00000001","name":"kiln","methods":["get","fire","heat","heated"]}"#;

/// How many readings `heat` has made, and how many of its streams were
/// dropped: what a test in another process asks `kiln.heated` for.
#[derive(Default)]
pub struct Gauge {
    pub made: AtomicU64,
    pub dropped: AtomicU64,
}

/// Counts a `heat` stream as dropped when it is.
struct Watched(Arc<Gauge>);

impl Drop for Watched {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn kiln() -> (Server, Arc<Gauge>) {
    let gauge = Arc::new(Gauge::default());
    let mut server = Server::new();
    server.hello(HASH);
    server.describe("kiln", IR).expect("the IR is JSON with a hash");
    server.method("kiln.get", |_| async { raw(&json!({ "id": 1, "cone": 6, "state": "cold" })) });
    server.method("kiln.fire", |params| async move {
        let params: serde_json::Value = serde_json::from_str(params.get()).map_err(|e| fail("bad-params", &e.to_string(), ()))?;
        let cone = params["cone"].as_u64().ok_or_else(|| fail("bad-params", "fire takes a cone", ()))?;
        if cone > 10 {
            return Err(fail("too-hot", "the kiln tops out at cone 10", cone));
        }
        raw(&json!({ "id": 1, "cone": cone }))
    });
    let g = gauge.clone();
    server.stream("kiln.heat", move |_| {
        // Endless: a reading every five minutes, for as long as it is read.
        futures::stream::unfold((0u64, Watched(g.clone())), |(minute, watched)| async move {
            watched.0.made.fetch_add(1, Ordering::SeqCst);
            Some((raw(&json!({ "minute": minute })), (minute + 5, watched)))
        })
    });
    let g = gauge.clone();
    server.method("kiln.heated", move |_| {
        let (made, dropped) = (g.made.load(Ordering::SeqCst), g.dropped.load(Ordering::SeqCst));
        async move { raw(&json!({ "made": made, "dropped": dropped })) }
    });
    (server, gauge)
}
