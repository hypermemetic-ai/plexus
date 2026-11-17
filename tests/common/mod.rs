//! What every channel must answer the same: the kiln's scenarios, asked of
//! a connection to a fresh kiln.

#![allow(dead_code)]

#[path = "../../src/bin/test-kiln/activation.rs"]
pub mod activation;

use std::time::Duration;

use futures::StreamExt;
use plexus::{Connection, TOP_UP, WINDOW};
use serde_json::{Value, json};

pub fn parse(raw: &plexus::Raw) -> Value {
    serde_json::from_str(raw.get()).unwrap()
}

async fn heated(conn: &Connection) -> (u64, u64) {
    let v = parse(&conn.call("kiln.heated", ()).await.unwrap());
    (v["made"].as_u64().unwrap(), v["dropped"].as_u64().unwrap())
}

/// Every scenario, in turn, on one connection.
pub async fn all(conn: &Connection) {
    hello(conn).await;
    one_item(conn).await;
    failure(conn).await;
    not_found(conn).await;
    slow_reader(conn).await;
    leaving_early(conn).await;
    at_once(conn).await;
}

pub async fn hello(conn: &Connection) {
    assert_eq!(conn.ready().await.unwrap(), activation::HASH);
}

/// calls › A call and its answer › one item.
pub async fn one_item(conn: &Connection) {
    let got = conn.call("kiln.get", ()).await.unwrap();
    assert_eq!(parse(&got), json!({ "id": 1, "cone": 6, "state": "cold" }));
    assert_eq!(parse(&conn.call("kiln.fire", json!({ "cone": 6 })).await.unwrap()), json!({ "id": 1, "cone": 6 }));
}

/// calls › A call and its answer › a failure.
pub async fn failure(conn: &Connection) {
    let e = conn.call("kiln.fire", json!({ "cone": 11 })).await.unwrap_err();
    assert_eq!(e.code(), Some("too-hot"), "{e}");
    assert_eq!(e.data(), Some("11"));
    assert_eq!(e.to_string(), "too-hot: the kiln tops out at cone 10");
}

/// calls › A call and its answer › no such method.
pub async fn not_found(conn: &Connection) {
    let e = conn.call("kiln.glaze", ()).await.unwrap_err();
    assert_eq!(e.code(), Some("not-found"), "{e}");
}

/// streams › A stream is paced by its reader › a slow reader.
pub async fn slow_reader(conn: &Connection) {
    let (before, dropped) = heated(conn).await;
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    let mut furthest = 0;
    // Past the first top-up, so the window is full again.
    for taken in 1..=TOP_UP as u64 + 8 {
        let item = items.next().await.unwrap().unwrap();
        assert_eq!(parse(&item), json!({ "minute": (taken - 1) * 5 }));
        // Time enough for the server to run as far ahead as it may.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut ahead = heated(conn).await.0 - before - taken;
        // At the top-up half a window is made again: on a busy machine,
        // more than one pause's worth. The bound below is checked as ever.
        for _ in 0..2000 {
            if taken != TOP_UP as u64 || ahead >= WINDOW as u64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            ahead = heated(conn).await.0 - before - taken;
        }
        assert!(ahead <= WINDOW as u64, "the server made {ahead} more than the caller took");
        furthest = furthest.max(ahead);
    }
    assert_eq!(furthest, WINDOW as u64, "the server never filled the window");
    drop(items);
    until_dropped(conn, dropped + 1).await;
}

/// Wait until the kiln has dropped `n` streams in all.
async fn until_dropped(conn: &Connection, n: u64) {
    for _ in 0..2000 {
        if heated(conn).await.1 >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("the server's stream was not dropped");
}

/// streams › A stream is paced by its reader › leaving early.
pub async fn leaving_early(conn: &Connection) {
    let (made, dropped) = heated(conn).await;
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    for _ in 0..3 {
        items.next().await.unwrap().unwrap();
    }
    drop(items);
    until_dropped(conn, dropped + 1).await;
    let (made_after, _) = heated(conn).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(heated(conn).await.0, made_after, "the stream made more after it was dropped");
    assert!(made_after - made <= 3 + WINDOW as u64);
}

/// Many calls at once on one connection: one answers while a stream is open.
pub async fn at_once(conn: &Connection) {
    let mut items = conn.stream("kiln.heat", ()).unwrap();
    items.next().await.unwrap().unwrap();
    let calls = (0..50).map(|_| conn.call("kiln.get", ()));
    for got in futures::future::join_all(calls).await {
        assert_eq!(parse(&got.unwrap())["cone"], 6);
    }
    items.next().await.unwrap().unwrap();
}
