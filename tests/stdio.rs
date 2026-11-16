//! channels › Any channel › two processes: the test-kiln binary as a child,
//! called over its stdin and stdout.

mod common;

use plexus::Connection;
use tokio::process::Command;

#[tokio::test]
async fn a_child_process_answers_as_in_memory() {
    let (mut child, channel) = plexus::stdio::child(Command::new(env!("CARGO_BIN_EXE_test-kiln")).kill_on_drop(true)).unwrap();
    let (conn, driving) = Connection::new(channel, Some(common::activation::HASH));
    tokio::spawn(driving);
    common::all(&conn).await;
    // Closing its stdin ends the child.
    drop(conn);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await.expect("the child exits").unwrap();
    assert!(status.success(), "{status}");
}
