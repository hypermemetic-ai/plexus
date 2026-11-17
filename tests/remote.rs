//! children › A child is reached under its parent, across a channel: the
//! test-kiln binary held as a child at `shop.kiln` of a server in this
//! process, every call under it passed over a second connection. The
//! test-kiln counts what `heat` made and dropped, which pacing and the
//! cancel are read from, through the parent.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::activation::{HASH, IR};
use common::parse;
use futures::StreamExt;
use plexus::{Connection, Remote, Server, TOP_UP, WINDOW, pair, raw};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

const SHOP: &str = r#"{"hash":"73686f7000000001","name":"shop"}"#;

fn test_kiln() -> Command {
    Command::new(env!("CARGO_BIN_EXE_test-kiln"))
}

/// A parent, `shop`, with a method of its own and `kiln` held at `shop.kiln`;
/// a connection to it.
fn shop(kiln: Remote) -> Connection {
    let mut server = Server::new();
    server.hello("73686f7000000001");
    server.describe("shop", SHOP).unwrap();
    server.method("shop.open", |_| async { raw(&true) });
    server.mount("shop.kiln", Box::new(kiln));
    let (near, far) = pair();
    tokio::spawn(Arc::new(server).serve(far));
    let (conn, driving) = Connection::new(near, None);
    tokio::spawn(driving);
    conn
}

async fn heated(conn: &Connection) -> (u64, u64) {
    let v = parse(&conn.call("shop.kiln.heated", ()).await.unwrap());
    (v["made"].as_u64().unwrap(), v["dropped"].as_u64().unwrap())
}

async fn until_dropped(conn: &Connection, n: u64) {
    for _ in 0..2000 {
        if heated(conn).await.1 >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("the far stream was not dropped");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_across_a_channel() {
    let conn = shop(Remote::child(test_kiln(), "kiln").unwrap());

    // across a channel: its answers, and its failures as they came.
    assert_eq!(parse(&conn.call("shop.kiln.get", ()).await.unwrap()), json!({ "id": 1, "cone": 6, "state": "cold" }));
    let e = conn.call("shop.kiln.fire", json!({ "cone": 11 })).await.unwrap_err();
    assert_eq!((e.code(), e.data(), e.to_string().as_str()), (Some("too-hot"), Some("11"), "too-hot: the kiln tops out at cone 10"));
    assert_eq!(conn.call("shop.kiln.glaze", ()).await.unwrap_err().code(), Some("not-found"));
    assert_eq!(parse(&conn.call("shop.open", ()).await.unwrap()), json!(true));

    // learned: `_tree` lists the child's places with their hashes, and its `_ir` and `_hash` answer under the parent.
    let tree = parse(&conn.call("_tree", ()).await.unwrap());
    assert_eq!(tree, json!([{ "place": "shop", "hash": "73686f7000000001" }, { "place": "shop.kiln", "hash": HASH }]));
    assert_eq!(conn.call("shop.kiln._ir", ()).await.unwrap().get(), IR);
    assert_eq!(parse(&conn.call("shop.kiln._hash", ()).await.unwrap()), json!(HASH));

    // a stream through a parent, read slowly: the far kiln runs no further
    // ahead than this reader allowed, the parent passing each credit on.
    let (before, dropped) = heated(&conn).await;
    let mut items = conn.stream("shop.kiln.heat", ()).unwrap();
    let mut furthest = 0;
    for taken in 1..=TOP_UP as u64 + 8 {
        let item = items.next().await.unwrap().unwrap();
        assert_eq!(parse(&item), json!({ "minute": (taken - 1) * 5 }));
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut ahead = heated(&conn).await.0 - before - taken;
        // At the top-up half a window is made again, through the parent: on
        // a busy machine, more than one pause's worth.
        for _ in 0..2000 {
            if taken != TOP_UP as u64 || ahead >= WINDOW as u64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            ahead = heated(&conn).await.0 - before - taken;
        }
        assert!(ahead <= WINDOW as u64, "the far kiln made {ahead} more than the caller took");
        furthest = furthest.max(ahead);
    }
    assert_eq!(furthest, WINDOW as u64, "the far kiln never filled the window");
    // ...and left early: the cancel reaches the far kiln, which stops.
    drop(items);
    until_dropped(&conn, dropped + 1).await;
    let (made, _) = heated(&conn).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(heated(&conn).await.0, made, "the far stream made more after it was dropped");

    // `call` on a stream through the parent reads it as it would directly.
    let mut items = conn.stream("shop.kiln.heat", ()).unwrap();
    items.next().await.unwrap().unwrap();
    let calls = (0..20).map(|_| conn.call("shop.kiln.get", ()));
    for got in futures::future::join_all(calls).await {
        assert_eq!(parse(&got.unwrap())["cone"], 6);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_child_is_gone() {
    let (mut child, channel) = plexus::stdio::child(test_kiln().kill_on_drop(true)).unwrap();
    let (far, driving) = Connection::new(channel, None);
    tokio::spawn(driving);
    let conn = shop(Remote::new(far, "kiln"));
    assert_eq!(parse(&conn.call("shop.kiln.get", ()).await.unwrap())["cone"], 6);
    let mut items = conn.stream("shop.kiln.heat", ()).unwrap();
    items.next().await.unwrap().unwrap();

    child.kill().await.unwrap();
    // A stream open under it ends `unavailable`, and so does every call after.
    let ended = loop {
        match items.next().await {
            Some(Ok(_)) => continue,
            other => break other,
        }
    };
    assert_eq!(ended.unwrap().unwrap_err().code(), Some("unavailable"));
    assert_eq!(conn.call("shop.kiln.get", ()).await.unwrap_err().code(), Some("unavailable"));
    assert_eq!(conn.call("shop.kiln._ir", ()).await.unwrap_err().code(), Some("unavailable"));
    // The rest of the tree is untouched.
    assert_eq!(parse(&conn.call("shop.open", ()).await.unwrap()), json!(true));
    assert_eq!(parse(&conn.call("_tree", ()).await.unwrap()), json!([{ "place": "shop", "hash": "73686f7000000001" }, { "place": "shop.kiln" }]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_child_on_a_websocket() {
    let mut kiln = test_kiln().args(["--ws", "127.0.0.1:0"]).stdout(std::process::Stdio::piped()).kill_on_drop(true).spawn().unwrap();
    let mut url = String::new();
    BufReader::new(kiln.stdout.take().unwrap()).read_line(&mut url).await.unwrap();
    let conn = shop(Remote::ws(url.trim(), "kiln").unwrap());
    assert_eq!(parse(&conn.call("shop.kiln.fire", json!({ "cone": 6 })).await.unwrap()), json!({ "id": 1, "cone": 6 }));
    assert_eq!(parse(&conn.call("shop.kiln._hash", ()).await.unwrap()), json!(HASH));
}
