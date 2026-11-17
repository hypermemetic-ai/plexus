//! An activation's stream across its thread: items cross in runs, and still
//! credit bounds the method, a cancel drops it, a panic fails its call
//! alone. `#[activation]`, served as `plexus::main` serves one, in memory.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use plexus::{Connection, Error, Server, TOP_UP, WINDOW, pair};
use serde_json::Value;

/// Counts a stream as dropped when it is.
struct Watched<'a>(&'a std::cell::Cell<u64>);

impl Drop for Watched<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[plexus::activation]
pub mod meter {
    use super::Watched;
    use futures::{Stream, StreamExt};
    use std::cell::Cell;

    #[derive(Default)]
    pub struct Meter {
        made: Cell<u64>,
        dropped: Cell<u64>,
    }

    impl Meter {
        /// Readings, for as long as they are read.
        pub fn tick(&self) -> impl Stream<Item = u64> {
            futures::stream::unfold((0, Watched(&self.dropped)), move |(n, w)| async move {
                self.made.set(self.made.get() + 1);
                Some((n, (n + 1, w)))
            })
        }

        /// One reading, then a wait that never ends.
        pub fn hang(&self) -> impl Stream<Item = u64> {
            let w = Watched(&self.dropped);
            futures::stream::once(async { 0 }).chain(futures::stream::pending()).map(move |n| {
                let _ = &w;
                n
            })
        }

        /// 0 to `to`, not counting `to`.
        pub fn count(&self, to: u64) -> impl Stream<Item = u64> {
            futures::stream::iter(0..to)
        }

        /// `after` readings, then a crack.
        pub fn crack(&self, after: u64) -> impl Stream<Item = u64> {
            futures::stream::iter(0..).map(move |n| if n == after { panic!("the shelf cracked") } else { n })
        }

        /// How many readings were made, and how many streams dropped.
        pub fn counts(&self) -> (u64, u64) {
            (self.made.get(), self.dropped.get())
        }
    }
}

fn connect() -> Connection {
    let mut server = Server::new();
    meter::serve(meter::Meter::default(), &mut server, "meter");
    let (near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    conn
}

async fn counts(conn: &Connection) -> (u64, u64) {
    let v: Value = serde_json::from_str(conn.call("meter.counts", ()).await.unwrap().get()).unwrap();
    (v[0].as_u64().unwrap(), v[1].as_u64().unwrap())
}

/// Wait, as long as a busy machine needs, until `done` holds of the counts.
async fn until(conn: &Connection, what: &str, done: impl Fn((u64, u64)) -> bool) -> (u64, u64) {
    for _ in 0..20_000 {
        let c = counts(conn).await;
        if done(c) {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("{what}: the counts are {:?}", counts(conn).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_arrive_whole_and_in_order() {
    let conn = connect();
    let mut streams: Vec<_> = (0..3).map(|_| conn.stream("meter.count", serde_json::json!({ "to": 100_000 })).unwrap()).collect();
    for items in &mut streams {
        let mut want = 0;
        while let Some(item) = items.next().await {
            assert_eq!(item.unwrap().get(), want.to_string());
            want += 1;
        }
        assert_eq!(want, 100_000);
    }
}

/// The method is asked for an item only with credit for it: a reader that
/// takes nothing gets a window made and no more, and each top-up gets as
/// many more.
#[tokio::test(flavor = "multi_thread")]
async fn credit_bounds_the_method_across_the_hop() {
    let conn = connect();
    let mut items = conn.stream("meter.tick", ()).unwrap();
    let window = WINDOW as u64;
    until(&conn, "a window made", |(made, _)| made >= window).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(counts(&conn).await.0, window, "the method ran past its credit");
    for n in 0..TOP_UP as u64 {
        assert_eq!(items.next().await.unwrap().unwrap().get(), n.to_string());
    }
    until(&conn, "a top-up made", |(made, _)| made >= window + TOP_UP as u64).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(counts(&conn).await.0, window + TOP_UP as u64, "the method ran past its credit");
}

/// A reader that leaves drops the method's stream on its own thread: one
/// still making items, and one waiting for its next.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_drops_the_method_across_the_hop() {
    let conn = connect();
    let mut items = conn.stream("meter.tick", ()).unwrap();
    for _ in 0..3 {
        items.next().await.unwrap().unwrap();
    }
    drop(items);
    let (made, _) = until(&conn, "the making stream dropped", |(_, dropped)| dropped == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(counts(&conn).await.0, made, "the stream made more after it was dropped");

    let mut items = conn.stream("meter.hang", ()).unwrap();
    items.next().await.unwrap().unwrap();
    drop(items);
    until(&conn, "the waiting stream dropped", |(_, dropped)| dropped == 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_fails_its_stream_alone() {
    let conn = connect();
    let mut items = conn.stream("meter.crack", serde_json::json!({ "after": 5 })).unwrap();
    for n in 0..5 {
        assert_eq!(items.next().await.unwrap().unwrap().get(), n.to_string());
    }
    match items.next().await {
        Some(Err(e @ Error::Failed(_))) => assert_eq!(e.to_string(), "internal: the shelf cracked"),
        other => panic!("{other:?}"),
    }
    assert!(items.next().await.is_none());
    assert_eq!(counts(&conn).await, (0, 0));
    let n = conn.stream("meter.count", serde_json::json!({ "to": 10 })).unwrap().count().await;
    assert_eq!(n, 10);
}

/// A stream with all the credit it wants still steps aside: calls to the
/// same activation answer while it runs.
#[tokio::test(flavor = "multi_thread")]
async fn calls_answer_while_a_stream_runs() {
    let conn = connect();
    let mut items = conn.stream("meter.tick", ()).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reading = tokio::spawn({
        let stop = stop.clone();
        async move {
            let mut read = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                items.next().await.unwrap().unwrap();
                read += 1;
            }
            read
        }
    });
    until(&conn, "the stream running", |(made, _)| made > 10_000).await;
    for _ in 0..50 {
        counts(&conn).await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(reading.await.unwrap() > 0);
}
