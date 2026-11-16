//! channels › Any channel: the same over a loopback WebSocket.

mod common;

use std::sync::Arc;

use plexus::Connection;

#[tokio::test]
async fn a_websocket_answers_as_in_memory() {
    let bound = plexus::ws::spawn("127.0.0.1:0".parse().unwrap(), Arc::new(common::activation::kiln().0)).await.unwrap();
    let channel = plexus::ws::connect(&format!("ws://{bound}/")).await.unwrap();
    let (conn, driving) = Connection::new(channel, Some(common::activation::HASH));
    tokio::spawn(driving);
    common::all(&conn).await;
}
