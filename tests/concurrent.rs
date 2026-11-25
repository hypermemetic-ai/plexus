//! Concurrent native serving keeps a single activation and the native caller boundary.
use futures::{Stream, TryStreamExt, stream};
use plexus::{Caller, Connection, Server, pair};
use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, watch};

fn thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

struct WatchEnd(Arc<Notify>);
impl Drop for WatchEnd {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[plexus::activation(roles(reader = ["watch"], operator = [reader, "increment"]))]
mod counter {
    use super::*;
    pub struct Counter {
        pub count: AtomicU32,
        pub current: watch::Sender<u32>,
        pub end: Arc<Notify>,
        pub threads: Arc<Mutex<HashSet<String>>>,
    }
    impl Counter {
        pub fn increment(&self, caller: &Caller) -> String {
            self.threads.lock().unwrap().insert(thread_id());
            let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            self.current.send_replace(count);
            caller.principal().to_owned()
        }
        pub fn watch(&self, caller: &Caller) -> impl Stream<Item = String> {
            let user = caller.principal().to_owned();
            let rx = self.current.subscribe();
            // The future is deliberately thread-affine, even for a Sync activation.
            let owner = std::rc::Rc::new(thread_id());
            let end = WatchEnd(self.end.clone());
            stream::unfold(
                (rx, true, owner, end),
                move |(mut rx, initial, owner, end)| {
                    let user = user.clone();
                    async move {
                        if !initial {
                            rx.changed().await.ok()?;
                        }
                        assert_eq!(*owner, thread_id());
                        let count = *rx.borrow_and_update();
                        Some((format!("{user}:{count}"), (rx, false, owner, end)))
                    }
                },
            )
        }
    }
}

fn make() -> (counter::Counter, Arc<Mutex<HashSet<String>>>, Arc<Notify>) {
    let threads = Arc::new(Mutex::new(HashSet::new()));
    let end = Arc::new(Notify::new());
    let activation = counter::Counter {
        count: AtomicU32::new(0),
        current: watch::channel(0).0,
        end: end.clone(),
        threads: threads.clone(),
    };
    (activation, threads, end)
}
fn connect(server: &Arc<Server>, caller: &str) -> Connection {
    let (local, remote) = pair();
    tokio::spawn(server.serve_as(remote, Caller::new(caller)));
    let (c, drive) = Connection::new(local, None);
    tokio::spawn(drive);
    c
}
fn policy(server: &mut Server, allow: Arc<AtomicBool>) {
    server.authorize_grants(move |who, _, _| {
        who.principal() == "alice" && allow.load(Ordering::SeqCst)
    });
}

#[tokio::test]
async fn one_hot_caller_reaches_every_worker_and_retains_watch_affinity() {
    let (activation, threads, end) = make();
    let mut server = Server::new();
    plexus::serve_concurrent(activation, NonZeroUsize::new(3).unwrap(), &mut server);
    let allow = Arc::new(AtomicBool::new(true));
    policy(&mut server, allow.clone());
    let server = Arc::new(server);
    let c = connect(&server, "alice");
    let outsider = connect(&server, "bob");
    assert_eq!(c.ready().await.unwrap(), counter::HASH);
    assert_eq!(
        c.call("counter._ir", ()).await.unwrap().get(),
        counter::IR.trim_end()
    );
    assert_eq!(
        outsider
            .call("counter.increment", ())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    let mut watch = c.stream("counter.watch", ()).unwrap();
    assert_eq!(
        watch.try_next().await.unwrap().unwrap().get(),
        r#""alice:0""#
    );
    for i in 1..=3 {
        assert_eq!(
            c.call("counter.increment", ()).await.unwrap().get(),
            r#""alice""#
        );
        assert_eq!(
            watch.try_next().await.unwrap().unwrap().get(),
            format!(r#""alice:{i}""#)
        );
    }
    assert_eq!(threads.lock().unwrap().len(), 3);
    allow.store(false, Ordering::SeqCst);
    assert_eq!(
        c.call("counter.increment", ()).await.unwrap_err().code(),
        Some("forbidden")
    );
    drop(watch);
    tokio::time::timeout(Duration::from_secs(3), end.notified())
        .await
        .expect("stream dropped on its worker");
}

#[tokio::test]
async fn registry_replacement_preserves_an_open_concurrent_stream() {
    let roots = plexus::Activations::default();
    let (old, _, old_end) = make();
    let old_values = old.current.clone();
    let workers = NonZeroUsize::new(2).unwrap();
    roots.insert(plexus::concurrent(old, workers)).unwrap();
    let mut host = roots.server();
    policy(&mut host, Arc::new(AtomicBool::new(true)));
    let c = connect(&Arc::new(host), "alice");
    let mut held = c.stream("counter.watch", ()).unwrap();
    assert_eq!(
        held.try_next().await.unwrap().unwrap().get(),
        r#""alice:0""#
    );
    let (new, threads, _) = make();
    roots.replace(plexus::concurrent(new, workers)).unwrap();
    for _ in 0..2 {
        c.call("counter.increment", ()).await.unwrap();
    }
    assert_eq!(threads.lock().unwrap().len(), 2);
    old_values.send_replace(42);
    assert_eq!(
        held.try_next().await.unwrap().unwrap().get(),
        r#""alice:42""#
    );
    roots.remove("counter").unwrap();
    assert_eq!(
        c.call("counter.increment", ()).await.unwrap_err().code(),
        Some("not-found")
    );
    drop(held);
    tokio::time::timeout(Duration::from_secs(3), old_end.notified())
        .await
        .unwrap();
}

#[plexus::activation]
mod latch {
    use super::*;
    use std::sync::Condvar;
    pub struct Latch {
        pub entered: Arc<Notify>,
        pub released: Mutex<bool>,
        pub wake: Condvar,
    }
    impl Latch {
        pub fn wait(&self) -> bool {
            let locked = self.released.lock().unwrap();
            self.entered.notify_one();
            let (released, _) = self
                .wake
                .wait_timeout_while(locked, Duration::from_secs(3), |released| !*released)
                .unwrap();
            *released
        }
        pub fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.wake.notify_one();
        }
    }
}

#[tokio::test]
async fn a_blocked_method_does_not_block_the_same_callers_next_open() {
    let entered = Arc::new(Notify::new());
    let activation = latch::Latch {
        entered: entered.clone(),
        released: Mutex::default(),
        wake: Default::default(),
    };
    let mut server = Server::new();
    plexus::serve_concurrent(activation, NonZeroUsize::new(2).unwrap(), &mut server);
    let c = connect(&Arc::new(server), "alice");
    let first = tokio::spawn(c.call("latch.wait", ()));
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    c.call("latch.release", ()).await.unwrap();
    assert_eq!(first.await.unwrap().unwrap().get(), "true");
}
