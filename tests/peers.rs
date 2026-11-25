use futures::{StreamExt, TryStreamExt, stream};
use plexus::{
    Caller, Channel, Connection, Error, Server,
    bytes::Bytes,
    pair,
    peer::{Side, connect},
    raw,
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinHandle;

type Driver = JoinHandle<io::Result<()>>;

fn service(name: &str, opened: Arc<AtomicU32>, watch_closed: Arc<AtomicU32>) -> Arc<Server> {
    let mut s = Server::new();
    s.hello(name);
    s.method_with_caller("who", |_, c| async move {
        raw(&c.require_caller()?.principal())
    });
    s.method_with_caller("bytes", |_, mut c| async move {
        raw(&c.take_byte_input()?.try_collect::<Vec<_>>().await?)
    });
    s.accepts_input("bytes");
    s.stream("watch", move |_| {
        opened.fetch_add(1, Ordering::SeqCst);
        struct Watch(Arc<AtomicU32>);
        impl Drop for Watch {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let guard = Watch(watch_closed.clone());
        stream::iter([raw(&1)])
            .chain(stream::pending())
            .inspect(move |_| {
                let _ = &guard;
            })
    });
    Arc::new(s)
}

async fn session(
    wire: Channel,
    side: Side,
    peer: &str,
    s: Arc<Server>,
    expected: &str,
) -> (Connection, Driver) {
    let (conn, drive) = connect(wire, side, Caller::new(peer), s, expected)
        .await
        .unwrap();
    (conn, tokio::spawn(drive))
}

#[tokio::test]
async fn independent_calls_cancel_disconnect_and_fresh_session() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let opened = Arc::<AtomicU32>::default();
        let closed = Arc::<AtomicU32>::default();
        let controller = service("controller", Default::default(), Default::default());
        let worker = service("worker", opened.clone(), closed.clone());
        for rung in 1..=2 {
            let (a, b) = pair();
            let ((to_worker, driver), (to_controller, worker_driver)) = tokio::join!(
                session(a, Side::Accept, "worker-key", controller.clone(), "worker"),
                session(
                    b,
                    Side::Dial,
                    "controller-key",
                    worker.clone(),
                    "controller"
                )
            );
            let (a, b) = tokio::join!(to_controller.ready(), to_worker.ready());
            assert_eq!(
                (a.unwrap(), b.unwrap()),
                ("controller".into(), "worker".into())
            );
            // Both first opens are input-bearing, with bytes including non-UTF8.
            let upload = |byte| stream::iter([Ok(Bytes::from(vec![byte, 255]))]);
            let (a, b) = tokio::join!(
                to_worker.call_with_byte_input("bytes", (), upload(1)),
                to_controller.call_with_byte_input("bytes", (), upload(2))
            );
            assert_eq!(a.unwrap().get(), "[[1,255]]");
            assert_eq!(b.unwrap().get(), "[[2,255]]");
            let mut watch = to_worker.stream("watch", ()).unwrap();
            assert_eq!(watch.next().await.unwrap().unwrap().get(), "1");
            assert_eq!(
                to_controller.call("who", ()).await.unwrap().get(),
                r#""worker-key""#
            );
            drop(watch);
            while closed.load(Ordering::SeqCst) < 2 * rung - 1 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                to_worker.call("who", ()).await.unwrap().get(),
                r#""controller-key""#
            );
            let mut interrupted = to_worker.stream("watch", ()).unwrap();
            assert!(interrupted.next().await.unwrap().is_ok());
            worker_driver.abort();
            assert!(worker_driver.await.unwrap_err().is_cancelled());
            assert!(matches!(interrupted.next().await, Some(Err(Error::Closed))));
            let _ = driver.await.unwrap();
            assert_eq!(opened.load(Ordering::SeqCst), 2 * rung);
        }
    })
    .await
    .expect("links settle after cancellation and disconnect");
}

#[tokio::test]
async fn both_ends_refuse_a_wrong_interface_or_duplicate_opener() {
    for (side, expected) in [(Side::Dial, "right"), (Side::Accept, "wrong")] {
        let (a, b) = pair();
        let (a, b) = tokio::join!(
            connect(
                a,
                Side::Dial,
                Caller::new("r"),
                service("left", Default::default(), Default::default()),
                expected
            ),
            connect(
                b,
                side,
                Caller::new("l"),
                service("right", Default::default(), Default::default()),
                "left"
            )
        );
        assert!(a.is_err() && b.is_err());
    }
}
