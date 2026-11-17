//! A connection and a server on a pair in memory, and the bytes between them.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::activation::{HASH, IR, kiln};
use common::parse;
use futures::{FutureExt, SinkExt, StreamExt};
use plexus::{Channel, Connection, Error, Server, TOP_UP, WINDOW, pair, raw};
use serde_json::json;

fn connect(server: Server, expects: Option<&str>) -> Connection {
    let (near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    let (conn, driving) = Connection::new(near, expects);
    tokio::spawn(driving);
    conn
}

async fn read(channel: &mut Channel) -> String {
    let bytes = tokio::time::timeout(Duration::from_secs(2), channel.receive.next()).await.expect("a frame").unwrap().unwrap();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn every_scenario() {
    common::all(&connect(kiln().0, Some(HASH))).await;
}

/// calls › one item, on the wire: the item, then the end.
#[tokio::test]
async fn the_server_writes_the_item_then_the_end() {
    let (mut near, far) = pair();
    tokio::spawn(Arc::new(kiln().0).serve(far));
    assert_eq!(read(&mut near).await, format!(r#"[0,0,{{"interface":"{HASH}"}}]"#));
    near.send.send(br#"[1,1,["kiln.get"]]"#.to_vec()).await.unwrap();
    assert_eq!(read(&mut near).await, r#"[1,{"cone":6,"id":1,"state":"cold"}]"#);
    assert_eq!(read(&mut near).await, "[1]");
    near.send.send(br#"[2,1,["kiln.fire",{"cone":11}]]"#.to_vec()).await.unwrap();
    assert_eq!(read(&mut near).await, r#"[2,4,{"code":"too-hot","message":"the kiln tops out at cone 10","data":11}]"#);
    near.send.send(br#"[3,1,["kiln.glaze"]]"#.to_vec()).await.unwrap();
    assert_eq!(read(&mut near).await, r#"[3,4,{"code":"not-found","message":"no method kiln.glaze"}]"#);
}

/// streams › on the wire: the open is credit for one, a stream adds the
/// rest of its window, tops it up as it reads, and a drop is a cancel.
#[tokio::test]
async fn the_caller_grants_credit_and_cancels() {
    let (near, mut far) = pair();
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    assert_eq!(read(&mut far).await, r#"[1,1,["kiln.heat"]]"#);
    assert_eq!(read(&mut far).await, format!("[1,6,{}]", WINDOW - 1));
    for n in 0..TOP_UP {
        far.send.send(format!("[1,{n}]").into_bytes()).await.unwrap();
    }
    for n in 0..TOP_UP {
        assert_eq!(items.next().await.unwrap().unwrap().get(), n.to_string());
    }
    assert_eq!(read(&mut far).await, format!("[1,6,{TOP_UP}]"));
    drop(items);
    assert_eq!(read(&mut far).await, "[1,5,null]");

    // A call dropped before its end cancels too.
    let call = conn.call("kiln.get", ());
    assert_eq!(read(&mut far).await, r#"[2,1,["kiln.get"]]"#);
    drop(call);
    assert_eq!(read(&mut far).await, "[2,5,null]");
}

/// streams › on the wire: the server writes exactly as many items as it
/// was given credit for, the open's one first, then each grant's.
#[tokio::test]
async fn the_server_writes_no_further_than_its_credit() {
    let mut server = Server::new();
    server.stream("kiln.count", |_| futures::stream::iter(0..u32::MAX).map(|n| raw(&n)));
    let (mut near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    near.send.send(br#"[1,1,["kiln.count"]]"#.to_vec()).await.unwrap();
    assert_eq!(read(&mut near).await, "[1,0]");
    let quiet = async |near: &mut Channel| tokio::time::timeout(Duration::from_millis(50), near.receive.next()).await.is_err();
    assert!(quiet(&mut near).await, "an item past the open's credit");
    for grant in [1u32, 3, 2] {
        near.send.send(format!("[1,6,{grant}]").into_bytes()).await.unwrap();
        for _ in 0..grant {
            read(&mut near).await;
        }
        assert!(quiet(&mut near).await, "an item past a grant of {grant}");
    }
    near.send.send(b"[1,6,2]".to_vec()).await.unwrap();
    near.send.send(b"[1,6,2]".to_vec()).await.unwrap();
    for n in 7..11 {
        assert_eq!(read(&mut near).await, format!("[1,{n}]"));
    }
    assert!(quiet(&mut near).await, "an item past two grants at once");
}

/// A stream's items are written as they are made, a run at a time, not a
/// window's worth at once (one thread, the reader a task beside the
/// server's, so the count is what was made before it could read).
#[tokio::test(flavor = "current_thread")]
async fn a_stream_is_written_as_it_is_made() {
    tokio::spawn(written_as_made()).await.unwrap();
}

async fn written_as_made() {
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
    let made = Arc::new(AtomicU32::new(0));
    let mut server = Server::new();
    let counted = made.clone();
    server.stream("kiln.count", move |_| {
        let counted = counted.clone();
        futures::stream::iter(0..u32::MAX).map(move |n| {
            counted.fetch_add(1, Relaxed);
            raw(&n)
        })
    });
    let (mut near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    near.send.send(br#"[1,1,["kiln.count"]]"#.to_vec()).await.unwrap();
    near.send.send(format!("[1,6,{}]", WINDOW - 1).into_bytes()).await.unwrap();
    assert_eq!(read(&mut near).await, "[1,0]");
    let made = made.load(Relaxed);
    assert!(made < WINDOW, "{made} items were made before the first was written");
}

/// A pair holds what is fed until it is flushed, then gives it all, in order.
#[tokio::test]
async fn a_pair_gives_what_was_fed_at_the_flush() {
    let (mut near, mut far) = pair();
    for n in 0..3 {
        near.send.feed(format!("[{n}]").into_bytes()).await.unwrap();
    }
    assert!(tokio::time::timeout(Duration::from_millis(20), far.receive.next()).await.is_err(), "a frame before the flush");
    near.send.flush().await.unwrap();
    for n in 0..3 {
        assert_eq!(read(&mut far).await, format!("[{n}]"));
    }
}

/// What arrives for a call dropped before its end goes nowhere, and the
/// connection's next call is answered as ever.
#[tokio::test]
async fn late_answers_to_a_dropped_call_go_nowhere() {
    let (near, mut far) = pair();
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    assert_eq!(read(&mut far).await, r#"[1,1,["kiln.heat"]]"#);
    assert_eq!(read(&mut far).await, format!("[1,6,{}]", WINDOW - 1));
    far.send.send(b"[1,0]".to_vec()).await.unwrap();
    assert_eq!(items.next().await.unwrap().unwrap().get(), "0");
    drop(items);
    assert_eq!(read(&mut far).await, "[1,5,null]");
    far.send.send(b"[1,1]".to_vec()).await.unwrap();
    far.send.send(b"[1]".to_vec()).await.unwrap();
    let call = conn.call("kiln.get", ());
    let (got, ()) = futures::join!(call, async {
        assert_eq!(read(&mut far).await, r#"[2,1,["kiln.get"]]"#);
        far.send.send(b"[2,6]".to_vec()).await.unwrap();
        far.send.send(b"[2]".to_vec()).await.unwrap();
    });
    assert_eq!(got.unwrap().get(), "6");
}

/// pace › `call` on a stream: the window granted once the first item is
/// in, topped up as items arrive, the last item the answer.
#[tokio::test]
async fn a_call_on_a_stream_grants_credit_and_answers_the_last() {
    let (near, mut far) = pair();
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    let call = tokio::spawn(conn.call("kiln.count", ()));
    assert_eq!(read(&mut far).await, r#"[1,1,["kiln.count"]]"#);
    far.send.send(b"[1,0]".to_vec()).await.unwrap();
    assert_eq!(read(&mut far).await, format!("[1,6,{}]", WINDOW - 1));
    for n in 1..WINDOW {
        far.send.send(format!("[1,{n}]").into_bytes()).await.unwrap();
    }
    assert_eq!(read(&mut far).await, format!("[1,6,{TOP_UP}]"));
    far.send.send(b"[1]".to_vec()).await.unwrap();
    assert_eq!(call.await.unwrap().unwrap().get(), (WINDOW - 1).to_string());

    // A server's own stream, read whole by `call`.
    let mut server = Server::new();
    server.stream("kiln.count", |_| futures::stream::iter(0..1000u32).map(|n| raw(&n)));
    assert_eq!(connect(server, None).call("kiln.count", ()).await.unwrap().get(), "999");
}

/// pace › a call with one answer, its end already in, grants nothing.
#[tokio::test]
async fn a_call_with_one_answer_grants_no_credit() {
    let (near, mut far) = pair();
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    let call = conn.call("kiln.get", ());
    let (got, ()) = futures::join!(call, async {
        assert_eq!(read(&mut far).await, r#"[1,1,["kiln.get"]]"#);
        far.send.send(b"[1,6]".to_vec()).await.unwrap();
        far.send.send(b"[1]".to_vec()).await.unwrap();
    });
    assert_eq!(got.unwrap().get(), "6");
    assert!(tokio::time::timeout(Duration::from_millis(50), far.receive.next()).await.is_err(), "a credit frame was sent");
}

/// streams › leaving early: the cancelled method's future is dropped, not
/// left to finish, and the call is not ended by the server.
#[tokio::test]
async fn a_cancel_drops_what_the_method_holds() {
    let state = Arc::new(Mutex::new("not started"));
    struct Held(Arc<Mutex<&'static str>>);
    impl Drop for Held {
        fn drop(&mut self) {
            *self.0.lock().unwrap() = "dropped";
        }
    }
    let mut server = Server::new();
    let s = state.clone();
    server.method("kiln.wait", move |_| {
        let held = Held(s.clone());
        async move {
            *held.0.lock().unwrap() = "running";
            futures::future::pending::<()>().await;
            raw(&())
        }
    });
    let until = async |want: &str| {
        for _ in 0..2000 {
            if *state.lock().unwrap() == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the method is {}, not {want}", state.lock().unwrap());
    };
    let (mut near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    near.send.send(br#"[1,1,["kiln.wait"]]"#.to_vec()).await.unwrap();
    until("running").await;
    near.send.send(b"[1,5,null]".to_vec()).await.unwrap();
    until("dropped").await;
    assert!(tokio::time::timeout(Duration::from_millis(50), near.receive.next()).await.is_err(), "a cancelled call was answered");
}

#[tokio::test]
async fn a_caller_of_another_interface_is_refused_at_hello() {
    let conn = connect(kiln().0, Some("0000"));
    match conn.ready().await {
        Err(Error::Interface { expected, served }) => assert_eq!((expected.as_str(), served.as_str()), ("0000", HASH)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn describe_answers_ir_hash_and_tree() {
    let conn = connect(kiln().0, None);
    assert_eq!(parse(&conn.call("_tree", ()).await.unwrap()), json!([{ "place": "kiln", "hash": HASH }]));
    assert_eq!(parse(&conn.call("kiln._hash", ()).await.unwrap()), json!(HASH));
    assert_eq!(conn.call("kiln._ir", ()).await.unwrap().get(), IR);
}

/// Many calls at once: one waits on another that comes after it.
#[tokio::test]
async fn calls_run_at_once() {
    let (release, released) = futures::channel::oneshot::channel::<()>();
    let release = Mutex::new(Some(release));
    let released = released.shared();
    let mut server = Server::new();
    server.method("kiln.wait", move |_| {
        let released = released.clone();
        async move {
            released.await.unwrap();
            raw("released")
        }
    });
    server.method("kiln.open", move |_| {
        release.lock().unwrap().take().unwrap().send(()).unwrap();
        async { raw(&()) }
    });
    let conn = connect(server, None);
    let waiting = conn.call("kiln.wait", ());
    conn.call("kiln.open", ()).await.unwrap();
    assert_eq!(waiting.await.unwrap().get(), r#""released""#);
}

#[tokio::test]
async fn a_method_that_panics_fails_its_call_alone() {
    let mut server = Server::new();
    server.method("kiln.crack", |_| async { panic!("the shelf cracked") });
    server.method("kiln.get", |_| async { raw(&6) });
    let conn = connect(server, None);
    let e = conn.call("kiln.crack", ()).await.unwrap_err();
    assert_eq!((e.code(), e.to_string().as_str()), (Some("internal"), "internal: the shelf cracked"));
    assert_eq!(conn.call("kiln.get", ()).await.unwrap().get(), "6");
}

/// The driver steps aside while frames keep coming: the reader's credit
/// goes out though the far end never stops writing (one thread, so a
/// driver that never stepped aside would hold it for good).
#[tokio::test(flavor = "current_thread")]
async fn credit_goes_out_while_frames_keep_coming() {
    let (out, mut sent) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    let out = out.sink_map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe));
    let endless = futures::stream::repeat_with(|| Ok(b"[1,7]".to_vec()));
    let (conn, driving) = Connection::new(Channel::new(out, endless), None);
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    tokio::spawn(driving);
    for _ in 0..TOP_UP {
        assert_eq!(items.next().await.unwrap().unwrap().get(), "7");
    }
    let mut frames = Vec::new();
    for _ in 0..3 {
        frames.push(String::from_utf8(sent.next().await.unwrap()).unwrap());
    }
    assert_eq!(frames, [r#"[1,1,["kiln.heat"]]"#.to_owned(), format!("[1,6,{}]", WINDOW - 1), format!("[1,6,{TOP_UP}]")]);
}

/// Items in before the channel closed are read, in order, then the close.
#[tokio::test]
async fn items_in_before_a_close_are_read_then_closed() {
    let (near, mut far) = pair();
    let (conn, driving) = Connection::new(near, None);
    let driving = tokio::spawn(driving);
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    assert_eq!(read(&mut far).await, r#"[1,1,["kiln.heat"]]"#);
    for n in 0..100 {
        far.send.send(format!("[1,{n}]").into_bytes()).await.unwrap();
    }
    drop(far);
    driving.await.unwrap().unwrap();
    for n in 0..100 {
        assert_eq!(items.next().await.unwrap().unwrap().get(), n.to_string());
    }
    assert!(matches!(items.next().await, Some(Err(Error::Closed))));
    assert!(items.next().await.is_none());
}

#[tokio::test]
async fn a_closed_channel_ends_every_call() {
    let (near, far) = pair();
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    let call = conn.call("kiln.get", ());
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    drop(far);
    assert!(matches!(call.await, Err(Error::Closed)));
    assert!(matches!(items.next().await, Some(Err(Error::Closed))));
    assert!(matches!(conn.call("kiln.get", ()).await, Err(Error::Closed)));
}
