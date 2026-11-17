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

/// What tungstenite, as the other end, sends and reads: a frame of every
/// length's form both ways, a ping answered, and a close that ends the
/// reading and is answered.
#[tokio::test]
async fn a_server_and_tungstenite_as_its_caller() {
    use futures::{SinkExt, StreamExt};
    use plexus::bytes::Bytes;
    use tungstenite::Message;

    const LENGTHS: [usize; 6] = [1, 125, 126, 65_535, 65_536, 70_000];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let caller = std::thread::spawn(move || {
        let (mut ws, _) = tungstenite::connect(url).unwrap();
        for len in LENGTHS {
            assert_eq!(ws.read().unwrap(), Message::Text("x".repeat(len).into()));
            ws.send(Message::Text("y".repeat(len).into())).unwrap();
        }
        ws.send(Message::Ping(Bytes::from_static(b"are you there"))).unwrap();
        assert_eq!(ws.read().unwrap(), Message::Pong(Bytes::from_static(b"are you there")));
        ws.close(None).unwrap();
        // The server's answer to the close, then the end.
        loop {
            match ws.read() {
                Ok(Message::Close(_)) => {}
                Err(tungstenite::Error::ConnectionClosed) => break,
                other => panic!("{other:?}"),
            }
        }
    });
    let (stream, _) = listener.accept().await.unwrap();
    let mut channel = plexus::ws::accept(stream).await.unwrap();
    for len in LENGTHS {
        channel.send.send(Bytes::from("x".repeat(len))).await.unwrap();
        assert_eq!(channel.receive.next().await.unwrap().unwrap(), "y".repeat(len).as_bytes());
    }
    assert!(channel.receive.next().await.is_none(), "the close ends the reading");
    // Dropped, the socket closes: the caller's last read.
    drop(channel);
    tokio::task::spawn_blocking(move || caller.join().unwrap()).await.unwrap();
}

/// A raw server: the handshake answered by hand, then `then` writes what
/// it will, the 101's bytes handed to it to write first. Its URL.
fn raw_server(then: impl FnOnce(std::net::TcpStream, Vec<u8>) + Send + 'static) -> String {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
        let request = String::from_utf8(request).unwrap();
        let key = request.lines().find_map(|l| l.split_once(':').filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))).unwrap().1.trim();
        let accept = tungstenite::handshake::derive_accept_key(key.as_bytes());
        let head = format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n");
        then(stream, head.into_bytes());
    });
    url
}

/// A server's unmasked text frame (under 126 bytes), as a server writes it.
fn text(frame: &str) -> Vec<u8> {
    [&[0x81, frame.len() as u8][..], frame.as_bytes()].concat()
}

/// Frames written with the handshake's answer, in the same write, are read.
#[tokio::test]
async fn frames_that_come_with_the_handshake_are_read() {
    use futures::StreamExt;
    let url = raw_server(|mut stream, head| {
        use std::io::Write;
        stream.write_all(&[head, text("[0,0,{\"interface\":\"x\"}]"), text("[1,2]")].concat()).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
    });
    let mut channel = plexus::ws::connect(&url).await.unwrap();
    assert_eq!(channel.receive.next().await.unwrap().unwrap(), &b"[0,0,{\"interface\":\"x\"}]"[..]);
    assert_eq!(channel.receive.next().await.unwrap().unwrap(), &b"[1,2]"[..]);
}

/// A message in fragments with a ping between them arrives whole, the ping
/// answered with a pong, masked as a caller's frames are.
#[tokio::test]
async fn fragments_and_a_ping_between_them() {
    use futures::StreamExt;
    let (tell, told) = std::sync::mpsc::channel();
    let url = raw_server(move |mut stream, head| {
        use std::io::{Read, Write};
        let ping = [0x89, 4, b'p', b'i', b'n', b'g'];
        // "[1," then a ping, then "2]": text, not final; continuation, final.
        stream.write_all(&[head, vec![0x01, 3, b'[', b'1', b','], ping.to_vec(), vec![0x80, 2, b'2', b']']].concat()).unwrap();
        // The pong: final, masked, four bytes, the ping's payload once unmasked.
        let mut pong = [0; 10];
        stream.read_exact(&mut pong).unwrap();
        let mask = [pong[2], pong[3], pong[4], pong[5]];
        let payload: Vec<u8> = pong[6..].iter().zip(mask.iter().cycle()).map(|(b, m)| b ^ m).collect();
        tell.send((pong[0], pong[1], payload)).unwrap();
    });
    let mut channel = plexus::ws::connect(&url).await.unwrap();
    assert_eq!(channel.receive.next().await.unwrap().unwrap(), &b"[1,2]"[..]);
    let (first, second, payload) = tokio::task::spawn_blocking(move || told.recv().unwrap()).await.unwrap();
    assert_eq!((first, second, payload), (0x8a, 0x80 | 4, b"ping".to_vec()));
}

/// A socket that closes in the middle of a frame, with no close: the frames
/// before it are read, then the reading fails.
#[tokio::test]
async fn a_socket_closing_mid_frame() {
    use futures::StreamExt;
    let url = raw_server(|mut stream, head| {
        use std::io::Write;
        let half = &text("[1,\"cut short\"]")[..7];
        stream.write_all(&[head, text("[1,0]"), text("[1,1]"), text("[1,2]"), half.to_vec()].concat()).unwrap();
    });
    let mut channel = plexus::ws::connect(&url).await.unwrap();
    for n in 0..3 {
        assert_eq!(channel.receive.next().await.unwrap().unwrap(), format!("[1,{n}]").as_bytes());
    }
    let failed = channel.receive.next().await.unwrap().unwrap_err();
    assert_eq!(failed.kind(), std::io::ErrorKind::BrokenPipe);
    assert!(channel.receive.next().await.is_none());
}
