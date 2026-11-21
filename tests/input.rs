//! Input has independent credit and EOF. Exercise the same calls through a
//! mounted remote so an intermediate receive window cannot hide a slow reader.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures::channel::oneshot;
use futures::{StreamExt, TryStreamExt, stream};
use plexus::{Connection, Failure, Raw, Remote, Server, WINDOW, fail, pair, raw};
use std::sync::Mutex;
use tokio::{task::JoinHandle, time::timeout};

#[derive(Default)]
struct Observation {
    consumed: AtomicUsize,
    committed: AtomicUsize,
    finished: AtomicUsize,
    pause: Gate,
}

struct Gate {
    signal: Mutex<Option<oneshot::Sender<()>>>,
    wait: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Default for Gate {
    fn default() -> Self {
        let (signal, wait) = oneshot::channel();
        Self {
            signal: Mutex::new(Some(signal)),
            wait: Mutex::new(Some(wait)),
        }
    }
}

impl Gate {
    fn release(&self) {
        self.signal
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
    }
    async fn wait(&self) {
        let wait = self.wait.lock().unwrap().take();
        if let Some(wait) = wait {
            wait.await.unwrap();
        }
    }
}

struct OnDrop(Arc<dyn Fn() + Send + Sync>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        (self.0)();
    }
}

fn connect(server: Server) -> (Connection, JoinHandle<std::io::Result<()>>) {
    let (client, wire) = pair();
    let serving = tokio::spawn(Arc::new(server).serve(wire));
    let (conn, driver) = Connection::new(client, None);
    tokio::spawn(driver);
    (conn, serving)
}

fn setup(
    remote: bool,
    observe: &Arc<Observation>,
) -> (Connection, JoinHandle<std::io::Result<()>>, &'static str) {
    let mut server = Server::new();
    let state = observe.clone();
    server.method_with_caller("media.put", move |_, mut call| {
        let state = state.clone();
        async move {
            let s = state.clone();
            let _drop = OnDrop(Arc::new(move || {
                s.finished.fetch_add(1, Ordering::SeqCst);
            }));
            let mut input = call.take_input(plexus::__private::decode_input::<u32>)?;
            let mut count = 0;
            while input.try_next().await?.is_some() {
                count += 1;
                state.consumed.fetch_add(1, Ordering::SeqCst);
                if count == 1 {
                    state.pause.wait().await;
                }
            }
            state.committed.fetch_add(1, Ordering::SeqCst);
            raw(&count)
        }
    });
    server.accepts_input("media.put");
    server.method("media.ready", |_| async { raw(&true) });
    if remote {
        let (far, _) = connect(server);
        let mut studio = Server::new();
        studio.mount("studio.held", Box::new(Remote::new(far, "media")));
        let (conn, serving) = connect(studio);
        (conn, serving, "studio.held.put")
    } else {
        let (conn, serving) = connect(server);
        (conn, serving, "media.put")
    }
}

async fn until(f: impl Fn() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !f() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition became observable");
}

#[tokio::test]
async fn input_window_is_end_to_end_and_eof_allows_a_reply() {
    for remote in [false, true] {
        let state = Arc::new(Observation::default());
        let (conn, _, put) = setup(remote, &state);
        let produced = Arc::new(AtomicUsize::new(0));
        let seen = produced.clone();
        let count = WINDOW * 2 + 1;
        let source = stream::iter(0..count).map(move |i| {
            seen.fetch_add(1, Ordering::SeqCst);
            raw(&i)
        });
        let answer = tokio::spawn(conn.call_with_input(put, (), source));
        until(|| {
            produced.load(Ordering::SeqCst) == WINDOW as usize
                && state.consumed.load(Ordering::SeqCst) == 1
        })
        .await;
        assert_eq!(state.consumed.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(produced.load(Ordering::SeqCst), WINDOW as usize);
        assert!(!answer.is_finished());
        state.pause.release();
        let receipt = timeout(Duration::from_secs(5), answer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receipt.get(), count.to_string());
        assert_eq!(state.committed.load(Ordering::SeqCst), 1);
        let empty = conn
            .call_with_input(put, (), stream::empty())
            .await
            .unwrap();
        assert_eq!(empty.get(), "0");
    }
}

#[tokio::test]
async fn cancellation_error_and_disconnect_cannot_publish_partial_input() {
    for remote in [false, true] {
        for outcome in ["cancel", "disconnect", "failed"] {
            let state = Arc::new(Observation::default());
            state.pause.release();
            let (conn, serving, put) = setup(remote, &state);
            let (tx, input) = futures::channel::mpsc::unbounded::<Result<Raw, Failure>>();
            let released = Arc::new(AtomicUsize::new(0));
            let signal = released.clone();
            let dropped = OnDrop(Arc::new(move || {
                signal.fetch_add(1, Ordering::SeqCst);
            }));
            let input = input.map(move |x| {
                let _ = &dropped;
                x
            });
            let done = tokio::spawn(conn.call_with_input(put, (), input));
            tx.unbounded_send(raw(&7)).unwrap();
            until(|| state.consumed.load(Ordering::SeqCst) == 1).await;
            match outcome {
                "cancel" => {
                    done.abort();
                    assert!(done.await.unwrap_err().is_cancelled());
                }
                "disconnect" => {
                    serving.abort();
                    assert!(done.await.unwrap().is_err());
                }
                _ => {
                    tx.unbounded_send(Err(fail("reader-failed", "source failed", 7)))
                        .unwrap();
                    let failure = done.await.unwrap().unwrap_err();
                    assert_eq!(failure.code(), Some("reader-failed"));
                    assert_eq!(failure.data(), Some("7"));
                }
            }
            until(|| {
                state.finished.load(Ordering::SeqCst) == 1 && released.load(Ordering::SeqCst) == 1
            })
            .await;
            assert_eq!(
                state.committed.load(Ordering::SeqCst),
                0,
                "{outcome} on remote={remote}"
            );
        }
    }
}

#[tokio::test]
async fn invalid_and_missing_input_fail_independently_of_other_calls() {
    for remote in [false, true] {
        let state = Arc::new(Observation::default());
        let (conn, _, put) = setup(remote, &state);
        let ready = put.replace(".put", ".ready");
        assert_eq!(
            conn.call(put, ()).await.unwrap_err().code(),
            Some("invalid-params")
        );
        assert_eq!(
            conn.call_with_input(&ready, (), stream::pending())
                .await
                .unwrap_err()
                .code(),
            Some("invalid-params")
        );
        assert_eq!(state.finished.load(Ordering::SeqCst), 0);
        let bad = conn.call_with_input(put, (), stream::iter([raw(&"oops")]));
        assert_eq!(bad.await.unwrap_err().code(), Some("invalid-params"));
        assert_eq!(state.committed.load(Ordering::SeqCst), 0);
        assert_eq!(conn.call(&ready, ()).await.unwrap().get(), "true");
    }
}

#[tokio::test]
async fn input_items_need_credit_even_from_a_wire_client() {
    use futures::SinkExt;
    use plexus_proto::{Body, CallId, Frame, Numbering, Open};

    let mut server = Server::new();
    // The handler never polls its input. No credit can have been issued.
    server.method_with_caller("test.hold", |_, mut call| async move {
        let _input = call.take_input(plexus::__private::decode_input::<u32>)?;
        futures::future::pending().await
    });
    server.accepts_input("test.hold");
    server.method("test.ready", |_| async { raw(&true) });
    let (mut client, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    let open = Frame {
        call: CallId(1),
        seq: 0,
        body: Body::Open(Open {
            method: "test.hold".into(),
            params: None,
            input: true,
        }),
    };
    for f in [open, Frame::item_of(CallId(1), &1u32).unwrap()] {
        client
            .send
            .send(f.encode(Numbering::Counted).into())
            .await
            .unwrap();
    }
    let frame = timeout(Duration::from_secs(2), client.receive.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(Frame::decode(&frame).unwrap().body, Body::End(Err(f)) if f.code == "invalid-params")
    );
    client
        .send
        .send(
            Frame::open(CallId(2), "test.ready", None)
                .encode(Numbering::Counted)
                .into(),
        )
        .await
        .unwrap();
    let frame = client.receive.next().await.unwrap().unwrap();
    assert!(matches!(Frame::decode(&frame).unwrap().body, Body::Item(raw) if raw.get() == "true"));
}

#[tokio::test]
async fn replacing_a_method_replaces_its_input_contract() {
    let mut server = Server::new();
    server.method("demo.read", |_| async { raw(&0) });
    server.accepts_input("demo.read");
    server.method("demo.read", |_| async { raw(&42) });
    let (conn, _) = connect(server);
    assert_eq!(conn.call("demo.read", ()).await.unwrap().get(), "42");
    let offered = conn.call_with_input("demo.read", (), stream::pending());
    assert_eq!(offered.await.unwrap_err().code(), Some("invalid-params"));
}

#[tokio::test]
async fn tenant_denial_precedes_input_credit_and_child_dispatch() {
    for remote in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let register = |server: &mut Server, name: &str| {
            server.method_with_caller(name, move |_, mut call| {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let input = call.take_input(plexus::__private::decode_input::<u32>)?;
                    raw(&input.try_collect::<Vec<_>>().await?)
                }
            });
            server.accepts_input(name);
        };
        let mut studio = Server::new();
        studio.guard_descendants("studio", &["tenant"], |call| async move {
            let tenant: String = call.keys().parse("tenant")?;
            Err(fail("not-a-member", "access refused", tenant))
        });
        if remote {
            let mut far = Server::new();
            register(&mut far, "media.put");
            let (far, _) = connect(far);
            studio.mount("studio.media", Box::new(Remote::new(far, "media")));
        } else {
            register(&mut studio, "studio.media.put");
        }
        let (conn, _) = connect(studio);
        let pulls = Arc::new(AtomicUsize::new(0));
        let count = pulls.clone();
        let input = stream::iter([7]).map(move |i| {
            count.fetch_add(1, Ordering::SeqCst);
            raw(&i)
        });
        let err = conn
            .call_with_input("studio[tenant=other].media.put", (), input)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("not-a-member"));
        assert_eq!(err.data(), Some(r#""other""#));
        assert_eq!(pulls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_failed_source_cannot_receive_a_partial_receipt() {
    use futures::SinkExt;
    use plexus_proto::{Body, CallId, Frame, Numbering};
    let (client, mut peer) = pair();
    let (conn, driving) = Connection::new(client, None);
    tokio::spawn(driving);
    let caller = tokio::spawn(conn.call_with_input(
        "media.put",
        (),
        stream::iter([Err(fail("source-failed", "read error", 9))]),
    ));
    let _open = peer.receive.next().await.unwrap().unwrap();
    let credit = Frame {
        call: CallId(1),
        seq: 0,
        body: Body::Credit(1),
    };
    peer.send
        .send(credit.encode(Numbering::Counted).into())
        .await
        .unwrap();
    let error = peer.receive.next().await.unwrap().unwrap();
    assert!(
        matches!(Frame::decode(&error).unwrap().body, Body::End(Err(f)) if f.code == "source-failed")
    );
    for f in [
        Frame::item_of(CallId(1), &"partial receipt").unwrap(),
        Frame::end(CallId(1)),
    ] {
        peer.send
            .send(f.encode(Numbering::Counted).into())
            .await
            .unwrap();
    }
    assert_eq!(
        caller.await.unwrap().unwrap_err().code(),
        Some("source-failed")
    );
}
