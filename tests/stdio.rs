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

/// A frame split across reads arrives whole, a line at a time, whatever the
/// line ends with; frames sent go out as lines, a newline in one a space.
#[tokio::test]
async fn a_frame_split_across_reads_arrives_whole() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::{SinkExt, StreamExt};
    use plexus::bytes::Bytes;
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

    /// Three bytes a read.
    struct Dribble(&'static [u8]);
    impl AsyncRead for Dribble {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let n = self.0.len().min(3);
            buf.put_slice(&self.0[..n]);
            self.0 = &self.0[n..];
            Poll::Ready(Ok(()))
        }
    }

    let (write, mut written) = tokio::io::duplex(1 << 16);
    let mut channel = plexus::stdio::lines(Dribble(b"[1,{\"pad\":\"xxxxxxxxxxxxxxxx\"}]\r\n\n[2]\n[3,\"last\"]"), write);
    let mut read = Vec::new();
    while let Some(frame) = channel.receive.next().await {
        read.push(String::from_utf8(frame.unwrap().to_vec()).unwrap());
    }
    assert_eq!(read, [r#"[1,{"pad":"xxxxxxxxxxxxxxxx"}]"#, "[2]", r#"[3,"last"]"#]);

    channel.send.feed(Bytes::from_static(b"[4,\n5]")).await.unwrap();
    channel.send.feed(Bytes::from_static(b"[6]")).await.unwrap();
    channel.send.flush().await.unwrap();
    let mut out = vec![0; 11];
    written.read_exact(&mut out).await.unwrap();
    assert_eq!(out, b"[4, 5]\n[6]\n");
}
