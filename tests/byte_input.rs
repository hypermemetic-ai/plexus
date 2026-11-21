//! Byte chunks keep their type through a held call and share the ordinary
//! input stream's credit, end and cancellation.
use futures::{SinkExt, StreamExt, TryStreamExt, stream};
#[cfg(feature = "tokio")]
use plexus::Remote;
use plexus::{Channel, Connection, Server, bytes::Bytes, pair, raw};
use plexus_proto::{Body, CallId, Frame, Numbering, binary_input};
use std::sync::{Arc, Mutex};

type Log = Arc<Mutex<Vec<Bytes>>>;

fn endpoint() -> Server {
    let mut server = Server::new();
    server.hello("media");
    server.method_with_caller("media.bytes", |_, mut call| async move {
        let chunks: Vec<_> = call.take_byte_input()?.try_collect().await?;
        raw(&chunks)
    });
    server.accepts_input("media.bytes");
    server.method_with_caller("media.text", |_, mut call| async move {
        let chunks: Vec<_> = call
            .take_input(plexus::__private::decode_input::<String>)?
            .try_collect()
            .await?;
        raw(&chunks)
    });
    server.accepts_input("media.text");
    server
}

/// Record frames at the server after crossing the transport's framing.
fn connect(
    server: impl Into<Arc<Server>>,
    client: Channel,
    mut serving: Channel,
) -> (Connection, Log) {
    let log = Log::default();
    let record = log.clone();
    serving.receive = Box::pin(serving.receive.inspect(move |frame| {
        if let Ok(frame) = frame {
            record.lock().unwrap().push(frame.clone());
        }
    }));
    tokio::spawn(server.into().serve(serving));
    let (conn, driving) = Connection::new(client, None);
    tokio::spawn(driving);
    (conn, log)
}

const CHUNK: &[u8] = &[0, b'\n', 0x81, 0xff];
fn source() -> impl futures::Stream<Item = Result<Bytes, plexus::Failure>> {
    stream::iter([Ok(Bytes::from_static(CHUNK)), Ok(Bytes::new())])
}

#[tokio::test]
async fn a_byte_method_gets_raw_chunks_and_json_clients_still_work() {
    let (a, b) = pair();
    let (conn, log) = connect(endpoint(), a, b);
    let answer = conn
        .call_with_byte_input("media.bytes", (), source())
        .await
        .unwrap();
    assert_eq!(answer.get(), "[[0,10,129,255],[]]");
    let chunks: Vec<_> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|v| v[0] == binary_input::TAG)
        .map(|frame| binary_input::read(frame.clone()).unwrap())
        .collect();
    assert_eq!(
        chunks,
        [
            (CallId(1), 1, Bytes::from_static(CHUNK)),
            (CallId(1), 2, Bytes::new())
        ]
    );
    let input = stream::iter([raw(&"AAqB/w==")]);
    assert_eq!(
        conn.call_with_input("media.bytes", (), input)
            .await
            .unwrap()
            .get(),
        "[[0,10,129,255]]"
    );
    let answers = conn
        .stream_with_byte_input("media.bytes", (), source())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(answers[0].get(), "[[0,10,129,255],[]]");
}

async fn write(ch: &mut Channel, frame: Frame) {
    ch.send
        .send(frame.encode(Numbering::Counted).into())
        .await
        .unwrap();
}

async fn read(ch: &mut Channel) -> Frame {
    Frame::decode(&ch.receive.next().await.unwrap().unwrap()).unwrap()
}

#[tokio::test]
async fn binary_sequence_counts_json_items_and_excludes_controls() {
    let (mut ch, b) = pair();
    tokio::spawn(Arc::new(endpoint()).serve(b));
    assert!(matches!(read(&mut ch).await.body, Body::Hello(h) if h.binary_input));
    let id = CallId(27);
    for valid in [false, true] {
        let mut open = Frame::open(id, "media.bytes", None);
        if let Body::Open(ref mut o) = open.body {
            o.input = true;
        }
        write(&mut ch, open).await;
        loop {
            if matches!(read(&mut ch).await.body, Body::Credit(_)) {
                break;
            }
        }
        write(
            &mut ch,
            Frame {
                call: id,
                seq: 0,
                body: Body::Item(raw(&"AQ==").unwrap()),
            },
        )
        .await;
        write(
            &mut ch,
            Frame {
                call: id,
                seq: 0,
                body: Body::Credit(1),
            },
        )
        .await;
        let mut binary = Vec::new();
        binary_input::write(&mut binary, id, if valid { 2 } else { 1 }, &[0xff]);
        ch.send.send(binary.into()).await.unwrap();
        write(
            &mut ch,
            Frame {
                call: id,
                seq: 0,
                body: Body::End(Ok(())),
            },
        )
        .await;
        if valid {
            assert!(matches!(read(&mut ch).await.body, Body::Item(r) if r.get() == "[[1],[255]]"));
            assert!(matches!(read(&mut ch).await.body, Body::End(Ok(()))));
        } else {
            assert!(
                matches!(read(&mut ch).await.body, Body::End(Err(f)) if f.code == "invalid-params")
            );
        }
    }
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn held_byte_input_uses_base64_lines_only_on_stdio() {
    let (left, right) = tokio::io::duplex(4096);
    let (lr, lw) = tokio::io::split(left);
    let (rr, rw) = tokio::io::split(right);
    let (child, stdio) = connect(
        endpoint(),
        plexus::stdio::lines(lr, lw),
        plexus::stdio::lines(rr, rw),
    );
    let mut parent = Server::new();
    parent.hello("studio");
    parent.mount("studio.held", Box::new(Remote::new(child, "media")));
    let (a, b) = pair();
    let parent = Arc::new(parent);
    let (conn, outer) = connect(parent.clone(), a, b);
    assert_eq!(
        conn.call_with_byte_input("studio.held.bytes", (), source())
            .await
            .unwrap()
            .get(),
        "[[0,10,129,255],[]]"
    );
    assert_eq!(
        outer
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame[0] == binary_input::TAG)
            .count(),
        2
    );
    let inner: Vec<_> = stdio
        .lock()
        .unwrap()
        .iter()
        .map(|line| Frame::decode(line).unwrap())
        .filter_map(|frame| match frame.body {
            Body::Item(r) => Some(r.get().to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(inner, [r#""AAqB/w==""#, r#""""#]);

    // Without a byte consumer behind the mount, a manually injected raw
    // frame must never become a base64 string accepted by the text method.
    let (mut bad, server) = pair();
    tokio::spawn(parent.serve(server));
    assert!(matches!(read(&mut bad).await.body, Body::Hello(_)));
    let id = CallId(45);
    let mut open = Frame::open(id, "studio.held.text", None);
    if let Body::Open(ref mut o) = open.body {
        o.input = true;
    }
    write(&mut bad, open).await;
    assert!(matches!(read(&mut bad).await.body, Body::Credit(_)));
    let mut chunk = Vec::new();
    binary_input::write(&mut chunk, id, 1, b"text");
    bad.send.send(chunk.into()).await.unwrap();
    assert!(matches!(read(&mut bad).await.body, Body::End(Err(f)) if f.code == "invalid-params"));
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn websocket_opcode_distinguishes_raw_input_from_json() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut message = Vec::new();
    binary_input::write(&mut message, CallId(1), 1, b"");
    let (local, mut peer) = tokio::io::duplex(512);
    let mut writer = plexus::ws::handshaken(local, true);
    writer.send.send(message.clone().into()).await.unwrap();
    let mut sent = vec![0; message.len() + 2];
    peer.read_exact(&mut sent).await.unwrap();
    assert_eq!(sent[0], 0x82);
    assert_eq!(&sent[2..], message);
    for fragmented in [false, true] {
        let (local, mut peer) = tokio::io::duplex(512);
        let mut reader = plexus::ws::handshaken(local, false);
        // JSON arriving in a binary message works as before.
        peer.write_all(b"\x82\x02[]").await.unwrap();
        assert_eq!(
            reader.receive.next().await.unwrap().unwrap().as_ref(),
            b"[]"
        );
        if fragmented {
            peer.write_all(&[0x01, 0, 0x80, message.len() as u8])
                .await
                .unwrap();
        } else {
            peer.write_all(&[0x81, message.len() as u8]).await.unwrap();
        }
        peer.write_all(&message).await.unwrap();
        assert!(
            reader
                .receive
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("binary message")
        );
    }
}

#[tokio::test]
async fn the_sender_needs_both_acknowledgements_and_one_credit_per_chunk() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (hello_support, early_ack) in [(false, true), (true, false), (true, true)] {
        let (client, mut peer) = pair();
        let (conn, driver) = Connection::new(client, None);
        tokio::spawn(driver);
        let hello = |interface: &str, supports_binary| {
            let mut hello = Frame::hello(interface, false);
            if let Body::Hello(h) = &mut hello.body {
                h.binary_input = supports_binary;
            }
            hello
        };
        write(&mut peer, hello("initial", !hello_support)).await;
        assert_eq!(conn.ready().await.unwrap(), "initial");
        write(&mut peer, hello("current", hello_support)).await;
        let count = Arc::new(AtomicUsize::new(0));
        let pulled = count.clone();
        let source = stream::repeat_with(move || {
            pulled.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from_static(CHUNK))
        });
        let call = conn.call_with_byte_input("media.bytes", (), source);
        let open = read(&mut peer).await;
        let id = open.call;
        assert!(matches!(open.body, Body::Open(_)));
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let ack = || Frame {
            call: id,
            seq: 0,
            body: Body::Progress(raw(&serde_json::json!({"$plexus":{"input":"bytes"}})).unwrap()),
        };
        if early_ack {
            write(&mut peer, ack()).await;
        }
        write(
            &mut peer,
            Frame {
                call: id,
                seq: 0,
                body: Body::Credit(1),
            },
        )
        .await;
        if !early_ack {
            write(&mut peer, ack()).await;
        }
        let frame = peer.receive.next().await.unwrap().unwrap();
        if early_ack && hello_support {
            assert_eq!(
                binary_input::read(frame).unwrap(),
                (id, 1, Bytes::from_static(CHUNK))
            );
        } else {
            assert!(
                matches!(Frame::decode(&frame).unwrap().body,Body::Item(r) if r.get() == r#""AAqB/w==""#)
            );
        }
        drop(call);
        assert!(matches!(read(&mut peer).await.body, Body::Cancel));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
