//! The caller's end: opens calls, hands their answers back.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use futures::channel::{mpsc, oneshot};
use futures::future::{self, Either, FutureExt};
use futures::{Stream, StreamExt};
use plexus_proto::{Body, CallId, Failure, Frame, Numbering, Raw};
use serde::Serialize;

use crate::channel::{Channel, pump};
use crate::lock;

/// How many items a stream's caller says it will take before it has taken
/// any (the open carries one of them).
pub const WINDOW: u32 = 8;
/// How many items it takes before saying so again.
pub const TOP_UP: u32 = 4;

/// Why a call gave no answer.
#[derive(Debug)]
pub enum Error {
    /// The method failed: its code, its message and its data.
    Failed(Failure),
    /// The connection closed before the call ended.
    Closed,
    /// The other end serves another interface than the one expected.
    Interface { expected: String, served: String },
    /// The parameters are not JSON.
    Params(serde_json::Error),
}

impl Error {
    /// The failure's code: `too-hot`, `not-found`. `None` unless the method failed.
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Failed(f) => Some(&f.code),
            _ => None,
        }
    }

    /// The failure's data, as JSON text.
    pub fn data(&self) -> Option<&str> {
        match self {
            Error::Failed(f) => f.data.as_deref().map(|d| d.get()),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Failed(x) if x.message.is_empty() => write!(f, "{}", x.code),
            Error::Failed(x) => write!(f, "{}: {}", x.code, x.message),
            Error::Closed => write!(f, "the connection closed"),
            Error::Interface { expected, served } => {
                write!(f, "the other end serves interface {served}; this caller expects {expected}")
            }
            Error::Params(e) => write!(f, "the parameters: {e}"),
        }
    }
}

impl std::error::Error for Error {}

enum Event {
    Item(Raw),
    End(Result<(), Failure>),
}

/// The caller's end of a channel. Clones share it.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
}

struct Shared {
    out: mpsc::UnboundedSender<Vec<u8>>,
    /// Each open call's inbox; `None` once the channel has closed.
    calls: Mutex<Option<HashMap<u32, mpsc::UnboundedSender<Event>>>>,
    next: AtomicU32,
    expects: Option<String>,
    hello: future::Shared<oneshot::Receiver<String>>,
}

impl Connection {
    /// A connection over `channel`, and the future that runs it: spawn it
    /// on any executor. It ends when the channel closes, or when every
    /// clone of the connection and every call it opened is gone.
    ///
    /// `expects` is the interface hash the caller was made for; [`ready`]
    /// refuses any other.
    ///
    /// [`ready`]: Connection::ready
    pub fn new(channel: Channel, expects: Option<&str>) -> (Connection, impl Future<Output = io::Result<()>> + Send + 'static) {
        let (out, frames) = mpsc::unbounded();
        let (said, hello) = oneshot::channel();
        let shared = Arc::new(Shared {
            out,
            calls: Mutex::new(Some(HashMap::new())),
            next: AtomicU32::new(1),
            expects: expects.map(str::to_owned),
            hello: hello.shared(),
        });
        let Channel { send, receive } = channel;
        let drive = drive(receive, pump(send, frames), Arc::downgrade(&shared), said);
        (Connection { shared }, drive)
    }

    /// The interface the other end said at hello, once it has; refused if
    /// it is not the one expected.
    pub async fn ready(&self) -> Result<String, Error> {
        let served = self.shared.hello.clone().await.map_err(|_| Error::Closed)?;
        match &self.shared.expects {
            Some(expected) if *expected != served => Err(Error::Interface { expected: expected.clone(), served }),
            _ => Ok(served),
        }
    }

    /// One answer: the last item before the end (`null` if there was
    /// none). The call is opened now; dropping the future cancels it.
    pub fn call(&self, method: &str, params: impl Serialize) -> impl Future<Output = Result<Raw, Error>> + Send + 'static {
        let opened = encode(&params).and_then(|params| self.open(method, params));
        async move {
            let mut call = opened?;
            let mut got = None;
            loop {
                match call.inbox.next().await {
                    Some(Event::Item(raw)) => got = Some(raw),
                    Some(Event::End(end)) => {
                        call.done = true;
                        return end.map(|()| got.unwrap_or_else(null)).map_err(Error::Failed);
                    }
                    None => {
                        call.done = true;
                        return Err(Error::Closed);
                    }
                }
            }
        }
    }

    /// Many answers, paced by how fast they are read: the server may run
    /// [`WINDOW`] items ahead of the reader and no further. Dropping the
    /// stream before its end cancels the call.
    pub fn stream(&self, method: &str, params: impl Serialize) -> Result<Items, Error> {
        let call = self.open(method, encode(&params)?)?;
        // The open itself carries credit for one.
        self.shared.send(Frame { call: CallId(call.id), seq: 0, body: Body::Credit(WINDOW - 1) });
        Ok(Items { call, taken: 0 })
    }

    fn open(&self, method: &str, params: Option<Raw>) -> Result<Open, Error> {
        let (tx, inbox) = mpsc::unbounded();
        let id = self.shared.next.fetch_add(1, Ordering::Relaxed);
        lock(&self.shared.calls).as_mut().ok_or(Error::Closed)?.insert(id, tx);
        let call = Open { id, inbox, shared: self.shared.clone(), done: false };
        if !self.shared.send(Frame::open(CallId(id), method, params)) {
            return Err(Error::Closed);
        }
        Ok(call)
    }
}

impl Shared {
    fn send(&self, frame: Frame) -> bool {
        self.out.unbounded_send(frame.encode(Numbering::Counted)).is_ok()
    }

    fn deliver(&self, call: CallId, event: Event) {
        let mut calls = lock(&self.calls);
        let Some(calls) = calls.as_mut() else { return };
        let inbox = if matches!(event, Event::End(_)) { calls.remove(&call.0) } else { calls.get(&call.0).cloned() };
        if let Some(inbox) = inbox {
            let _ = inbox.unbounded_send(event);
        }
    }
}

async fn drive(
    mut receive: Pin<Box<dyn Stream<Item = io::Result<Vec<u8>>> + Send>>,
    write: impl Future<Output = io::Result<()>>,
    shared: Weak<Shared>,
    said: oneshot::Sender<String>,
) -> io::Result<()> {
    let mut said = Some(said);
    let held = shared.clone();
    let read = async move {
        while let Some(bytes) = receive.next().await {
            let frame = Frame::decode(&bytes?).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let Some(shared) = shared.upgrade() else { break };
            match frame.body {
                Body::Hello(hello) => {
                    if let Some(said) = said.take() {
                        let _ = said.send(hello.interface);
                    }
                }
                Body::Item(raw) => shared.deliver(frame.call, Event::Item(raw)),
                Body::End(end) => shared.deliver(frame.call, Event::End(end)),
                _ => {}
            }
        }
        Ok(())
    };
    futures::pin_mut!(read, write);
    let ended = match future::select(read, write).await {
        Either::Left((r, _)) | Either::Right((r, _)) => r,
    };
    // Every call still open learns the channel closed.
    if let Some(shared) = held.upgrade() {
        lock(&shared.calls).take();
    }
    ended
}

/// A call that is open. Dropped before its end, it cancels.
struct Open {
    id: u32,
    inbox: mpsc::UnboundedReceiver<Event>,
    shared: Arc<Shared>,
    done: bool,
}

impl Drop for Open {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(calls) = lock(&self.shared.calls).as_mut() {
            calls.remove(&self.id);
        }
        self.shared.send(Frame { call: CallId(self.id), seq: 0, body: Body::Cancel });
    }
}

/// The items of a stream, as they arrive. Reading one may say the server
/// can write more; dropping it early sends `cancel`.
pub struct Items {
    call: Open,
    taken: u32,
}

impl Stream for Items {
    type Item = Result<Raw, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.call.done {
            return Poll::Ready(None);
        }
        let event = match self.call.inbox.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(event) => event,
        };
        Poll::Ready(match event {
            Some(Event::Item(raw)) => {
                self.taken += 1;
                if self.taken.is_multiple_of(TOP_UP) {
                    self.call.shared.send(Frame { call: CallId(self.call.id), seq: 0, body: Body::Credit(TOP_UP) });
                }
                Some(Ok(raw))
            }
            Some(Event::End(end)) => {
                self.call.done = true;
                end.err().map(|f| Err(Error::Failed(f)))
            }
            None => {
                self.call.done = true;
                Some(Err(Error::Closed))
            }
        })
    }
}

fn encode(params: &impl Serialize) -> Result<Option<Raw>, Error> {
    let raw = serde_json::value::to_raw_value(params).map_err(Error::Params)?;
    Ok(match raw.get() {
        "{}" | "null" => None,
        _ => Some(raw),
    })
}

fn null() -> Raw {
    serde_json::value::to_raw_value(&()).expect("null is JSON")
}
