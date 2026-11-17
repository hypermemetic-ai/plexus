//! Frames over a WebSocket, one text message each: what a page's
//! `plexus-rt` speaks.
//!
//! tungstenite makes the handshake and masks what a caller writes. Reading,
//! and a server's writing, are this module's own (RFC 6455, 5): a reader
//! takes every frame waiting in one turn, under one lock, each a slice of
//! the bytes it was read into, with no copy; a server writes every frame of
//! a flush into one buffer, which one write puts on the socket. A peer that
//! breaks the protocol is closed with the reason's code. The Autobahn test
//! suite runs against `examples/autobahn.rs`; `fuzz/` fuzzes the reader.

use std::collections::VecDeque;
use std::io::{self, Cursor, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use futures::task::{ArcWake, AtomicWaker, waker_ref};
use futures::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::{HandshakeError, HandshakeRole};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::protocol::frame::coding::{Control, Data, OpCode};
use tungstenite::protocol::frame::{Frame, FrameHeader};
use tungstenite::{Message, WebSocket};

use crate::{Channel, Server, lock};

// A channel stamped by `trace` as its frames cross the socket, or the
// channel alone.
#[cfg(feature = "trace")]
macro_rules! traced {
    ($channel:expr, $clock:expr) => { traced::channel($channel, $clock) };
}
#[cfg(not(feature = "trace"))]
macro_rules! traced {
    ($channel:expr, $clock:expr) => { $channel };
}

/// A channel to the WebSocket at `url` (`ws://127.0.0.1:4455/`).
pub async fn connect(url: &str) -> io::Result<Channel> {
    let request = url.into_client_request().map_err(broken)?;
    let uri = request.uri();
    if uri.scheme_str() != Some("ws") {
        return Err(io::Error::new(io::ErrorKind::Unsupported, format!("{url}: only ws:// is served")));
    }
    let stream = TcpStream::connect(format!("{}:{}", uri.host().unwrap_or("127.0.0.1"), uri.port_u16().unwrap_or(80))).await?;
    // No Nagle, as `accept`: a credit written while the last write is not
    // yet acknowledged would otherwise wait for the delayed ack (40 ms).
    stream.set_nodelay(true)?;
    #[cfg(feature = "trace")]
    let (stream, clock) = crate::trace::Socket::new(stream);
    let wakers = Arc::new(Wakers::default());
    let started = tungstenite::client::client_with_config(request, Io::new(stream, wakers.clone()), Some(config()));
    let (socket, _) = handshake(&wakers, started).await?;
    Ok(traced!(channel(socket, false), clock))
}

/// A channel over a connection just accepted: the WebSocket handshake, then frames.
pub async fn accept(stream: TcpStream) -> io::Result<Channel> {
    stream.set_nodelay(true)?;
    #[cfg(feature = "trace")]
    let (stream, clock) = crate::trace::Socket::new(stream);
    let wakers = Arc::new(Wakers::default());
    let started = tungstenite::accept_with_config(Io::new(stream, wakers.clone()), Some(config()));
    let socket = handshake(&wakers, started).await?;
    Ok(traced!(channel(socket, true), clock))
}

/// Serve `server` to every connection `listener` accepts, each on a task of its own.
pub async fn listen(listener: TcpListener, server: Arc<Server>) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let server = server.clone();
        tokio::spawn(async move {
            if let Ok(channel) = accept(stream).await {
                let _ = server.serve(channel).await;
            }
        });
    }
}

/// Bind `addr` and serve there in the background; the address bound
/// (port 0 picks one).
pub async fn spawn(addr: SocketAddr, server: Arc<Server>) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(listen(listener, server));
    Ok(bound)
}

/// A channel over `stream` as if its handshake were done: for the fuzzer,
/// which feeds the reader bytes with no socket.
#[doc(hidden)]
pub fn handshaken<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(stream: S, server: bool) -> Channel {
    let role = if server { tungstenite::protocol::Role::Server } else { tungstenite::protocol::Role::Client };
    channel(WebSocket::from_raw_socket(Io::new(stream, Arc::new(Wakers::default())), role, Some(config())), server)
}

/// Run a handshake begun by `started` to its end, waiting whenever the
/// socket has nothing to read or no room to write.
async fn handshake<R: HandshakeRole>(wakers: &Wakers, started: Result<R::FinalResult, HandshakeError<R>>) -> io::Result<R::FinalResult> {
    let mut state = Some(started);
    futures::future::poll_fn(|cx| {
        wakers.read.register(cx.waker());
        wakers.write.register(cx.waker());
        // The first try ran with no waker kept: try again now that one is.
        let mid = match state.take().expect("polled after it ended") {
            Err(HandshakeError::Interrupted(mid)) => mid,
            Err(HandshakeError::Failure(e)) => return Poll::Ready(Err(broken(e))),
            Ok(done) => return Poll::Ready(Ok(done)),
        };
        match mid.handshake() {
            Err(HandshakeError::Interrupted(mid)) => {
                state = Some(Err(HandshakeError::Interrupted(mid)));
                Poll::Pending
            }
            Err(HandshakeError::Failure(e)) => Poll::Ready(Err(broken(e))),
            Ok(done) => Poll::Ready(Ok(done)),
        }
    })
    .await
}

/// Who waits on the socket: the reading half and the writing half. Either
/// may make the socket read or write (a read answers a ping, a write may
/// wait for room), so the socket wakes both.
#[derive(Default)]
struct Wakers {
    read: AtomicWaker,
    write: AtomicWaker,
}

impl ArcWake for Wakers {
    fn wake_by_ref(wakers: &Arc<Self>) {
        wakers.read.wake();
        wakers.write.wake();
    }
}

/// A tokio socket as tungstenite reads and writes it: `WouldBlock` where
/// tokio would wait, with [`Wakers`] kept to be woken. What is written is
/// kept in `out` and goes to the socket at a flush, so every frame, of
/// tungstenite's making or this module's, is written in the order made.
struct Io<S> {
    stream: S,
    wakers: Arc<Wakers>,
    out: Vec<u8>,
    /// How much of `out` the socket has taken.
    sent: usize,
    /// Every byte the handshake read, so that those read past its end are
    /// the first of the frames.
    seen: Option<Vec<u8>>,
}

impl<S: Unpin> Io<S> {
    fn new(stream: S, wakers: Arc<Wakers>) -> Io<S> {
        Io { stream, wakers, out: Vec::new(), sent: 0, seen: Some(Vec::new()) }
    }

    fn poll<T>(&mut self, f: impl FnOnce(Pin<&mut S>, &mut Context<'_>) -> Poll<io::Result<T>>) -> io::Result<T> {
        let waker = waker_ref(&self.wakers);
        match f(Pin::new(&mut self.stream), &mut Context::from_waker(&waker)) {
            Poll::Ready(done) => done,
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }

    /// What the handshake read past its HTTP head, which ends at the first
    /// empty line; nothing is kept from here on.
    fn handshaken(&mut self) -> BytesMut {
        let seen = self.seen.take().unwrap_or_default();
        let past = seen.windows(4).position(|w| w == b"\r\n\r\n").map_or(seen.len(), |at| at + 4);
        BytesMut::from(&seen[past..])
    }
}

impl<S: AsyncRead + Unpin> Read for Io<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read = ReadBuf::new(buf);
        self.poll(|stream, cx| stream.poll_read(cx, &mut read))?;
        let n = read.filled().len();
        if let Some(seen) = &mut self.seen {
            seen.extend_from_slice(&buf[..n]);
        }
        Ok(n)
    }
}

impl<S: AsyncWrite + Unpin> Write for Io<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        while self.sent < self.out.len() {
            let (stream, wakers, out) = (&mut self.stream, &self.wakers, &self.out[self.sent..]);
            let waker = waker_ref(wakers);
            match Pin::new(stream).poll_write(&mut Context::from_waker(&waker), out) {
                Poll::Ready(Ok(0)) => return Err(io::ErrorKind::WriteZero.into()),
                Poll::Ready(Ok(n)) => self.sent += n,
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => return Err(io::ErrorKind::WouldBlock.into()),
            }
        }
        self.out.clear();
        self.sent = 0;
        self.poll(|stream, cx| stream.poll_flush(cx))
    }
}

/// tungstenite hands every frame it makes to [`Io`] at once, which keeps it.
fn config() -> WebSocketConfig {
    WebSocketConfig::default().write_buffer_size(0)
}

/// A frame larger than this is refused, as tungstenite refuses one.
const MOST_FRAME: usize = 16 << 20;
/// A message in fragments larger than this is refused.
const MOST_MESSAGE: usize = 64 << 20;
/// How much a read asks the socket for.
const READ: usize = 64 << 10;
/// How many messages, and how many bytes of them, a reader takes in one
/// turn at most: the socket is not read further ahead than that of what
/// has been handed on.
const TURN: usize = 256;
const TURN_BYTES: usize = 1 << 20;

/// One socket, both halves'.
struct Socket<S> {
    /// Used for its socket, and for masking what a caller writes.
    ws: WebSocket<Io<S>>,
    /// The end that accepted: its frames are not masked, and it is sent
    /// masked ones.
    server: bool,
    /// A close was sent: nothing more is.
    closed: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Socket<S> {
    fn io(&mut self) -> &mut Io<S> {
        self.ws.get_mut()
    }

    /// Queue one message, final, to go at the next flush.
    fn queue(&mut self, opcode: OpCode, payload: Bytes) -> io::Result<()> {
        if !self.server {
            // Masked by tungstenite as it writes the frame.
            let frame = Frame::from_payload(FrameHeader { opcode, ..FrameHeader::default() }, payload);
            return self.ws.write(Message::Frame(frame)).map_err(broken);
        }
        let (out, len) = (&mut self.io().out, payload.len());
        out.reserve(10 + len);
        out.push(0x80 | u8::from(opcode));
        match len {
            0..=125 => out.push(len as u8),
            126..=0xffff => {
                out.push(126);
                out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                out.push(127);
                out.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(&payload);
        Ok(())
    }

    /// Put what is queued on the socket: `Pending` while it has no room.
    fn poll_flush(&mut self) -> Poll<io::Result<()>> {
        match self.io().flush() {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            done => Poll::Ready(done),
        }
    }
}

fn channel<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(mut ws: WebSocket<Io<S>>, server: bool) -> Channel {
    let (read, wakers) = (ws.get_mut().handshaken(), ws.get_ref().wakers.clone());
    let socket = Arc::new(Mutex::new(Socket { ws, server, closed: false }));
    let send = Writer { socket: socket.clone(), wakers: wakers.clone(), held: Vec::new(), closing: false };
    Channel::new(send, Reader { socket, wakers, server, buf: read, fragments: None, read: VecDeque::new(), held: 0, failed: None, close: None, ended: false })
}

/// The reading half: every frame waiting is read in one turn, under one
/// lock, and handed on one by one.
struct Reader<S> {
    socket: Arc<Mutex<Socket<S>>>,
    wakers: Arc<Wakers>,
    server: bool,
    /// Bytes read and not yet taken.
    buf: BytesMut,
    fragments: Option<Fragments>,
    read: VecDeque<Bytes>,
    /// The bytes of the messages in `read`.
    held: usize,
    /// What ended the reading, once the frames read before it are handed on.
    failed: Option<io::Error>,
    /// The close to send then, if one is.
    close: Option<Bytes>,
    ended: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Stream for Reader<S> {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        let this = &mut *self;
        if this.read.is_empty() && !this.ended {
            this.wakers.read.register(cx.waker());
            let socket = this.socket.clone();
            let mut socket = lock(&socket);
            if let Err(e) = this.turn(&mut socket) {
                // A peer that broke the protocol is told why, with the
                // close's code (RFC 6455, 7.1.7).
                if let Some(&Refused(code, _)) = e.get_ref().and_then(|e| e.downcast_ref()) {
                    this.close = Some(Bytes::copy_from_slice(&code.to_be_bytes()));
                }
                (this.ended, this.failed) = (true, Some(e));
            }
            this.flush(&mut socket);
        }
        match this.read.pop_front() {
            Some(frame) => {
                this.held -= frame.len();
                Poll::Ready(Some(Ok(frame)))
            }
            None if this.ended => {
                // The close goes once every message before it is handed on,
                // so that what is sent back to those may still go.
                if let Some(close) = this.close.take() {
                    let socket = this.socket.clone();
                    let mut socket = lock(&socket);
                    if !socket.closed {
                        socket.closed = true;
                        let _ = socket.queue(OpCode::Control(Control::Close), close);
                    }
                    this.flush(&mut socket);
                }
                Poll::Ready(this.failed.take().map(Err))
            }
            None => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Reader<S> {
    /// An answer to a ping or a close goes now if the socket takes it, else
    /// with the next flush or turn.
    fn flush(&mut self, socket: &mut Socket<S>) {
        if !socket.io().out.is_empty()
            && let Poll::Ready(Err(e)) = socket.poll_flush()
            && self.failed.is_none()
        {
            (self.ended, self.failed) = (true, Some(e));
        }
    }

    /// Take the frames read, reading more while the socket has some, until
    /// a turn's worth are taken or the reading ends.
    fn turn(&mut self, socket: &mut Socket<S>) -> io::Result<()> {
        while self.read.len() < TURN && self.held < TURN_BYTES && !self.ended {
            if !self.frame(socket)? && !self.fill(socket)? {
                break;
            }
        }
        Ok(())
    }

    /// Read more from the socket: `false` if it has nothing now.
    fn fill(&mut self, socket: &mut Socket<S>) -> io::Result<bool> {
        if self.buf.capacity() - self.buf.len() < READ / 16 {
            self.buf.reserve(READ);
        }
        let io = socket.io();
        let waker = waker_ref(&io.wakers);
        match tokio_util::io::poll_read_buf(Pin::new(&mut io.stream), &mut Context::from_waker(&waker), &mut self.buf) {
            Poll::Pending => Ok(false),
            Poll::Ready(Ok(0)) if socket.closed => {
                self.ended = true;
                Ok(false)
            }
            Poll::Ready(Ok(0)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "the connection closed with no close frame")),
            Poll::Ready(Ok(_)) => Ok(true),
            Poll::Ready(Err(e)) => Err(e),
        }
    }

    /// Take one frame from what was read: `false` if it is not all there.
    fn frame(&mut self, socket: &mut Socket<S>) -> io::Result<bool> {
        let mut cursor = Cursor::new(&self.buf[..]);
        // tungstenite refuses only an opcode of no known kind.
        let Some((header, len)) = FrameHeader::parse(&mut cursor).map_err(|_| refused(PROTOCOL, "a frame of no known kind"))? else { return Ok(false) };
        let at = cursor.position() as usize;
        // The header is checked before its payload is waited for.
        if header.rsv1 || header.rsv2 || header.rsv3 {
            return Err(refused(PROTOCOL, "a frame with a reserved bit set"));
        }
        // A caller masks every frame it sends, a server none (RFC 6455, 5.1).
        if header.mask.is_some() != self.server {
            return Err(refused(PROTOCOL, if self.server { "a caller's frame not masked" } else { "a server's frame masked" }));
        }
        if matches!(header.opcode, OpCode::Control(_)) && (!header.is_final || len > 125) {
            return Err(refused(PROTOCOL, "a control frame fragmented or longer than 125 bytes"));
        }
        let len = usize::try_from(len).ok().filter(|&len| len <= MOST_FRAME).ok_or_else(|| refused(TOO_BIG, "a frame larger than 16 MiB"))?;
        if self.buf.len() < at + len {
            self.buf.reserve(at + len - self.buf.len());
            return Ok(false);
        }
        self.buf.advance(at);
        let mut payload = self.buf.split_to(len);
        if let Some(mask) = header.mask {
            unmask(&mut payload, mask);
        }
        let payload = payload.freeze();
        match header.opcode {
            OpCode::Data(data) => self.data(data, header.is_final, payload)?,
            OpCode::Control(Control::Ping) if !socket.closed => socket.queue(OpCode::Control(Control::Pong), payload)?,
            OpCode::Control(Control::Ping | Control::Pong) => {}
            OpCode::Control(Control::Close) => {
                close(&payload)?;
                // Answered with its code; what came before it is read.
                self.close = Some(payload.slice(..len.min(2)));
                self.ended = true;
            }
            OpCode::Control(Control::Reserved(_)) => return Err(refused(PROTOCOL, "a control frame of no known kind")),
        }
        Ok(true)
    }

    /// A data frame: a whole message, or a fragment of one.
    fn data(&mut self, data: Data, last: bool, payload: Bytes) -> io::Result<()> {
        match (data, &mut self.fragments) {
            (Data::Text | Data::Binary, None) => {
                let (text, mut checked) = (data == Data::Text, 0);
                if text {
                    utf8(&payload, &mut checked, last)?;
                }
                if last {
                    self.hand_on(payload);
                } else {
                    self.fragments = Some(Fragments { bytes: BytesMut::from(&payload[..]), text, checked });
                }
            }
            (Data::Continue, Some(fragments)) => {
                if fragments.bytes.len() + payload.len() > MOST_MESSAGE {
                    return Err(refused(TOO_BIG, "a message larger than 64 MiB"));
                }
                fragments.bytes.extend_from_slice(&payload);
                if fragments.text {
                    utf8(&fragments.bytes, &mut fragments.checked, last)?;
                }
                if last {
                    let message = self.fragments.take().expect("fragments").bytes.freeze();
                    self.hand_on(message);
                }
            }
            (Data::Reserved(_), _) => return Err(refused(PROTOCOL, "a data frame of no known kind")),
            _ => return Err(refused(PROTOCOL, "a fragment out of place")),
        }
        Ok(())
    }

    fn hand_on(&mut self, message: Bytes) {
        self.held += message.len();
        self.read.push_back(message);
    }
}

/// The fragments of a message so far.
struct Fragments {
    bytes: BytesMut,
    text: bool,
    /// How much of `bytes` is known to be UTF-8.
    checked: usize,
}

/// A text message is UTF-8: what is past `checked` of it is checked as its
/// fragments come, so that one broken is refused at once (RFC 6455, 8.1).
/// Only the last fragment may not end a character.
fn utf8(text: &[u8], checked: &mut usize, last: bool) -> io::Result<()> {
    match std::str::from_utf8(&text[*checked..]) {
        Ok(_) => *checked = text.len(),
        Err(e) if e.error_len().is_none() && !last => *checked += e.valid_up_to(),
        Err(_) => return Err(refused(NOT_UTF8, "text that is not UTF-8")),
    }
    Ok(())
}

/// A close frame's payload: none, or a code an endpoint may send and a
/// reason in UTF-8 (RFC 6455, 5.5.1 and 7.4).
fn close(payload: &[u8]) -> io::Result<()> {
    let [a, b, reason @ ..] = payload else {
        return if payload.is_empty() { Ok(()) } else { Err(refused(PROTOCOL, "a close frame of one byte")) };
    };
    if !matches!(u16::from_be_bytes([*a, *b]), 1000..=1003 | 1007..=1014 | 3000..=4999) {
        return Err(refused(PROTOCOL, "a close code no endpoint may send"));
    }
    std::str::from_utf8(reason).map_err(|_| refused(NOT_UTF8, "a close reason that is not UTF-8"))?;
    Ok(())
}

/// A frame from a caller, as it was before its mask (RFC 6455, 5.3).
fn unmask(payload: &mut [u8], mask: [u8; 4]) {
    let word = u32::from_ne_bytes(mask);
    let mut words = payload.chunks_exact_mut(4);
    for w in &mut words {
        let x = u32::from_ne_bytes(w[..].try_into().expect("four bytes")) ^ word;
        w.copy_from_slice(&x.to_ne_bytes());
    }
    for (b, m) in words.into_remainder().iter_mut().zip(mask) {
        *b ^= m;
    }
}

/// The close codes a refusal sends (RFC 6455, 7.4.1).
const PROTOCOL: u16 = 1002;
const NOT_UTF8: u16 = 1007;
const TOO_BIG: u16 = 1009;

/// What the peer sent that breaks the protocol, and the close code it is
/// answered with.
#[derive(Debug)]
struct Refused(u16, &'static str);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebSocket: {}", self.1)
    }
}

impl std::error::Error for Refused {}

fn refused(code: u16, what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Refused(code, what))
}

/// The writing half: frames are held until a flush, then queued in one
/// turn, under one lock, one text message each, and written together.
struct Writer<S> {
    socket: Arc<Mutex<Socket<S>>>,
    wakers: Arc<Wakers>,
    held: Vec<Bytes>,
    closing: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Writer<S> {
    /// Queue what is held, and a close if `close`; ready once all of it is
    /// on the socket.
    fn poll_write(&mut self, cx: &mut Context<'_>, close: bool) -> Poll<io::Result<()>> {
        self.wakers.write.register(cx.waker());
        let mut socket = lock(&self.socket);
        if !self.held.is_empty() {
            if socket.closed {
                self.held.clear();
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "the WebSocket is closed")));
            }
            for frame in self.held.drain(..) {
                // Written as it is, with no check: the runtime wrote it as
                // UTF-8, and the reader checks it.
                socket.queue(OpCode::Data(Data::Text), frame)?;
            }
        }
        if close && !socket.closed {
            socket.closed = true;
            socket.queue(OpCode::Control(Control::Close), Bytes::new())?;
        }
        socket.poll_flush()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sink<Bytes> for Writer<S> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Bytes) -> io::Result<()> {
        self.held.push(frame);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_write(cx, false)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The close is queued once; after that, only flushed.
        let close = !std::mem::replace(&mut self.closing, true);
        self.poll_write(cx, close)
    }
}

fn broken(e: tungstenite::Error) -> io::Error {
    match e {
        tungstenite::Error::Io(e) => e,
        e => io::Error::new(io::ErrorKind::BrokenPipe, e),
    }
}

/// A WebSocket's frames stamped as they are written to its socket and read
/// from it.
#[cfg(feature = "trace")]
mod traced {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use futures::{Sink, StreamExt};

    use crate::Channel;
    use crate::trace::{self, Batch, Clock, Frames, Stage};

    pub fn channel(channel: Channel, clock: Arc<Clock>) -> Channel {
        let Channel { send, receive } = channel;
        let read = clock.clone();
        let mut frames = Frames::default();
        let receive = receive.inspect(move |frame| {
            if let Ok(frame) = frame
                && trace::on()
                && let Some(key) = frames.key(frame)
            {
                if let Some((entered, returned)) = read.read() {
                    trace::stamp_at(key, Stage::WsReadEntry, entered);
                    trace::stamp_at(key, Stage::WsReadReturn, returned);
                }
                trace::stamp(key, Stage::WsMessage);
            }
        });
        Channel::new(Written { send, clock, batch: Batch::default() }, receive)
    }

    /// The sending half: a frame's write to the socket is stamped once the
    /// flush that writes it is done.
    struct Written {
        send: Pin<Box<dyn Sink<Bytes, Error = io::Error> + Send>>,
        clock: Arc<Clock>,
        batch: Batch,
    }

    impl Sink<Bytes> for Written {
        type Error = io::Error;

        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.send.as_mut().poll_ready(cx)
        }

        fn start_send(mut self: Pin<&mut Self>, frame: Bytes) -> io::Result<()> {
            // Keyed again here, not stamped: the pump stamped it taken.
            self.batch.keep(&frame);
            self.send.as_mut().start_send(frame)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let flushed = self.send.as_mut().poll_flush(cx);
            if flushed.is_ready() {
                let (entered, returned) = self.clock.written();
                self.batch.flushed(|stage| if stage == Stage::WsWrite { entered } else { returned }, &[Stage::WsWrite, Stage::WsWritten]);
            }
            flushed
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.send.as_mut().poll_close(cx)
        }
    }
}
