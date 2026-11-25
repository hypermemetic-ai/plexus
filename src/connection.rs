//! The caller's end: opens calls, hands their answers back.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use futures::channel::{mpsc, oneshot};
use futures::future::{self, AbortHandle, Abortable, BoxFuture, Either, FutureExt};
use futures::{Stream, StreamExt};
use plexus_proto::{Body, CallId, Failure, Frame, Kind, Numbering, Raw, Received};
use serde::Serialize;

use crate::channel::{Channel, Out, pump};
use crate::lock;
use crate::input::{InputItem, Source, Upload};
use crate::server::Credit;

/// How many items a stream's caller says it will take before it has taken
/// any (the open carries one of them).
pub const WINDOW: u32 = 4096;
/// How many items it takes before saying so again.
pub const TOP_UP: u32 = 2048;

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
            Error::Failed(f) => f.data.as_ref().map(Raw::get),
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
    /// An item as it came, read as far as its head. The driver only reads
    /// a frame's head; the reader takes the item from it, with no second
    /// read and no copy.
    Item(Received),
    End(Result<(), Failure>),
}

/// The caller's end of a channel. Clones share it.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
}

/// What the driver is told of calls, in the order it happened: it keeps the
/// map of inboxes itself, so an item finds its call with no lock.
enum Told {
    Opened(u32, Slot),
    Gone(u32),
}

/// A call's inbox: what the driver has handed in and the reader has not
/// yet taken. The reader takes all of it at once, so a run of items costs
/// it one lock.
#[derive(Default)]
struct Inbox(Mutex<Queue>);

#[derive(Default)]
struct Queue {
    events: VecDeque<Event>,
    /// Nothing more will come: the call ended, or the channel closed.
    closed: bool,
    wake: Option<Waker>,
}

/// The driver's hold on an inbox, and the run of events it has read for
/// the call and not yet handed in: handed in whole, so the reader is woken
/// once a run, not once an item. Dropped — the call ended, was dropped, or
/// the channel closed — it hands in what it holds and closes the inbox.
struct Slot {
    upload: Option<Sending>,
    inbox: Arc<Inbox>,
    run: Vec<Event>,
    /// A `call`: its first item is credit for one, as the open was, and
    /// it may be a stream or one answer. Once that item is in, the driver
    /// grants the rest of the window, but only where it has read all there
    /// was to read with no end for the call: an end right behind the item
    /// ends the call with no credit sent for nothing. `false` once granted.
    window: bool,
    /// Its call, and how many items it has handed in.
    #[cfg(feature = "trace")]
    traced: (u32, u32),
}

/// Only a byte acknowledgement received before the first positive credit
/// describes this call. A later progress frame cannot change its type.
struct Sending {
    credit: Arc<Credit>,
    byte_ack: Arc<AtomicBool>,
    granted: bool,
    abort: AbortHandle,
}

impl Slot {
    fn new(inbox: Arc<Inbox>, window: bool) -> Slot {
        Slot {
            inbox,
            run: Vec::new(),
            upload: None,
            window,
            #[cfg(feature = "trace")]
            traced: (0, 0),
        }
    }

    fn hand_in(&mut self, close: bool) {
        #[cfg(feature = "trace")]
        let keys = self.keys();
        let mut queue = lock(&self.inbox.0);
        queue.events.extend(self.run.drain(..));
        queue.closed |= close;
        let wake = queue.wake.take();
        drop(queue);
        #[cfg(feature = "trace")]
        let handed = crate::trace::now();
        if let Some(wake) = wake {
            wake.wake();
        }
        #[cfg(feature = "trace")]
        for key in keys {
            crate::trace::stamp_at(key, crate::trace::Stage::InInbox, handed);
            crate::trace::stamp(key, crate::trace::Stage::Woke);
        }
    }

    /// The key of each event in the run.
    #[cfg(feature = "trace")]
    fn keys(&mut self) -> Vec<crate::trace::Key> {
        if !crate::trace::on() {
            return Vec::new();
        }
        let (call, n) = &mut self.traced;
        let keys = self.run.iter().map(|event| match event {
            Event::Item(_) => {
                *n += 1;
                (*call, *n)
            }
            Event::End(_) => (*call, crate::trace::END),
        });
        keys.collect()
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(upload) = &self.upload { upload.abort.abort(); }
        self.hand_in(true);
    }
}

struct Shared {
    /// Latest peer capability on a binary-preserving channel.
    binary_input: Arc<AtomicBool>,
    uploads: mpsc::UnboundedSender<BoxFuture<'static, ()>>,
    out: Out,
    /// Closed once the channel has.
    tell: mpsc::UnboundedSender<Told>,
    next: AtomicU32,
    stride: u32,
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
        Self::numbered(channel, expects, 1, 1)
    }

    pub(crate) fn numbered(channel: Channel, expects: Option<&str>, first: u32, stride: u32)
        -> (Connection, impl Future<Output = io::Result<()>> + Send + 'static + use<>)
    {
        let (out, frames) = crate::channel::out();
        let (said, hello) = oneshot::channel();
        let (tell, told) = mpsc::unbounded();
        let (uploads, uploading) = mpsc::unbounded();
        let shared = Arc::new(Shared {
            binary_input: Arc::new(AtomicBool::new(false)),
            out,
            tell,
            uploads,
            next: AtomicU32::new(first),
            stride,
            expects: expects.map(str::to_owned),
            hello: hello.shared(),
        });
        let Channel { send, receive, binary_input } = channel;
        let drive = drive(receive, pump(send, frames), Arc::downgrade(&shared), told, said, binary_input);
        let drive = async move {
            let sending_input = async {
                uploading.for_each_concurrent(None, |job| job).await;
                future::pending::<io::Result<()>>().await
            };
            futures::pin_mut!(drive, sending_input);
            match future::select(drive, sending_input).await {
                Either::Left((r, _)) | Either::Right((r, _)) => r,
            }
        };
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
    /// none). The call is opened now; dropping the future cancels it. Called
    /// on a stream, it reads every item, granting credit as
    /// [`stream`](Connection::stream) does once the first is in, and answers
    /// the last. A call whose end comes with its first item grants nothing.
    pub fn call(&self, method: &str, params: impl Serialize) -> impl Future<Output = Result<Raw, Error>> + Send + 'static {
        self.call_from(method, params, None)
    }

    /// One answer to a call with streamed input. A producer is polled
    /// only with input credit; dropping the future cancels both directions.
    pub fn call_with_input(
        &self, method: &str, params: impl Serialize,
        input: impl Stream<Item = Result<Raw, Failure>> + Send + 'static,
    ) -> impl Future<Output = Result<Raw, Error>> + Send + 'static {
        self.call_from(method, params, Some(Source::Local(input.map(|v| v.map(InputItem::Json)).boxed())))
    }

    /// Upload byte chunks in the representation supported by this hop.
    pub fn call_with_byte_input(
        &self, method: &str, params: impl Serialize,
        input: impl Stream<Item = Result<bytes::Bytes, Failure>> + Send + 'static,
    ) -> impl Future<Output = Result<Raw, Error>> + Send + 'static {
        self.call_from(method, params, Some(Source::Local(input.map(|v| v.map(InputItem::Bytes)).boxed())))
    }

    fn call_from(&self, method: &str, params: impl Serialize, input: Option<Source>) -> impl Future<Output = Result<Raw, Error>> + Send + 'static {
        #[cfg(feature = "trace")]
        let called = crate::trace::now();
        let opened = encode(&params).and_then(|params| self.open(method, params, true, input));
        #[cfg(feature = "trace")]
        if let Ok(call) = &opened {
            crate::trace::stamp_at((call.id, 0), crate::trace::Stage::Called, called);
        }
        async move {
            let mut call = opened?;
            let (mut got, mut taken) = (None, 0u32);
            loop {
                match future::poll_fn(|cx| call.poll_event(cx)).await {
                    Some(Event::Item(received)) => {
                        got = Some(item(received));
                        taken += 1;
                        stamp!((call.id, taken), Took);
                        // The rest of the window after the first item is the
                        // driver's to grant (`Slot::window`); the top-ups are
                        // the reader's, as `stream`'s.
                        if taken.is_multiple_of(TOP_UP) {
                            call.grant(TOP_UP);
                        }
                    }
                    Some(Event::End(end)) => {
                        call.done = true;
                        stamp!((call.id, 0), Returned);
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
        self.stream_from(method, params, None)
    }

    /// Stream results while independently streaming the caller's input.
    pub fn stream_with_input(
        &self, method: &str, params: impl Serialize,
        input: impl Stream<Item = Result<Raw, Failure>> + Send + 'static,
    ) -> Result<Items, Error> {
        self.stream_from(method, params, Some(Source::Local(input.map(|v| v.map(InputItem::Json)).boxed())))
    }

    /// Independently stream byte chunks in and result items out.
    pub fn stream_with_byte_input(
        &self, method: &str, params: impl Serialize,
        input: impl Stream<Item = Result<bytes::Bytes, Failure>> + Send + 'static,
    ) -> Result<Items, Error> {
        self.stream_from(method, params, Some(Source::Local(input.map(|v| v.map(InputItem::Bytes)).boxed())))
    }

    fn stream_from(&self, method: &str, params: impl Serialize, input: Option<Source>) -> Result<Items, Error> {
        let call = self.open(method, encode(&params)?, false, input)?;
        // The open itself carries credit for one.
        call.grant(WINDOW - 1);
        Ok(Items { call, taken: 0 })
    }

    /// A call passed on from another caller, as a parent passes a call to a
    /// child it holds: opened with credit for one item, as the open it
    /// passes on carried, and granted more only as that caller grants it
    /// ([`Passed::grant`]), so it runs no further ahead than the first
    /// caller allows. Dropped before its end, it cancels. With the
    /// connection closed it fails `unavailable`.
    pub fn pass(&self, method: &str, params: Option<Raw>) -> Passed {
        self.pass_from(method, params, None)
    }

    pub(crate) fn pass_from(&self, method: &str, params: Option<Raw>, input: Option<Source>) -> Passed {
        match self.open(method, params, false, input) {
            Ok(call) => Passed(Ok(call)),
            Err(_) => Passed::failed(unavailable()),
        }
    }

    /// `window`: the driver grants the rest of the window once the first
    /// item is in, unless the end is right behind it ([`Slot::window`]).
    fn open(&self, method: &str, params: Option<Raw>, window: bool, input: Option<Source>) -> Result<Open, Error> {
        let inbox = Arc::new(Inbox::default());
        // Zero marks exhaustion. Never reuse an ID while a previous call may be live.
        let id = self.shared.next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
            (id != 0).then(|| id.checked_add(self.shared.stride).unwrap_or(0))
        }).map_err(|_| Error::Failed(crate::fail("call-ids-exhausted", "open a new connection", ())))?;
        // Told before the open is sent, so before any answer can arrive.
        let mut slot = Slot::new(inbox.clone(), window);
        let with_input = input.is_some();
        let stop_input = if let Some(source) = input {
            let credit = Arc::new(Credit::new(0));
            let (abort, registration) = AbortHandle::new_pair();
            let byte_ack = Arc::new(AtomicBool::new(false));
            slot.upload = Some(Sending { credit: credit.clone(), byte_ack: byte_ack.clone(), granted: false, abort: abort.clone() });
            let mut upload = Upload::new(source, credit);
            let out = self.shared.out.clone();
            let input_inbox = inbox.clone();
            let tell = self.shared.tell.clone();
            let binary = self.shared.binary_input.clone();
            let mut ordinal = 0u64;
            let pump = future::poll_fn(move |cx| {
                let ended = upload.pump(cx, &byte_ack, |next| {
                    let end = match next {
                        Ok(Some(item)) => {
                            ordinal += 1;
                            out.send(|buf| item.write(buf, CallId(id), ordinal, binary.load(Ordering::Acquire) && byte_ack.load(Ordering::Acquire)));
                            return;
                        }
                        Ok(None) => Ok(()),
                        Err(f) => Err(f),
                    };
                    let error = end.as_ref().err().cloned();
                    out.send(|buf| Frame { call: CallId(id), seq: 0, body: Body::End(end) }.encode_into(Numbering::Counted, buf));
                    if let Some(f) = error {
                        // A local producer failure owns this outcome even if
                        // a peer returns a receipt for the partial upload.
                        let wake = {
                            let mut queue = lock(&input_inbox.0);
                            queue.events.push_back(Event::End(Err(f)));
                            queue.closed = true;
                            queue.wake.take()
                        };
                        let _ = tell.unbounded_send(Told::Gone(id));
                        if let Some(wake) = wake { wake.wake(); }
                    }
                });
                if ended { Poll::Ready(()) } else { Poll::Pending }
            });
            self.shared.uploads.unbounded_send(Abortable::new(pump, registration).map(|_| ()).boxed()).map_err(|_| Error::Closed)?;
            Some(abort)
        } else { None };
        self.shared.tell.unbounded_send(Told::Opened(id, slot)).map_err(|_| Error::Closed)?;
        let call = Open { id, inbox, taken: VecDeque::new(), shared: self.shared.clone(), done: false, stop_input };
        let open = plexus_proto::Open { method: method.to_owned(), params, input: with_input };
        if !self.shared.send(Frame { call: CallId(id), seq: 0, body: Body::Open(open) }) {
            return Err(Error::Closed);
        }
        stamp!((id, 0), OpenQueued);
        Ok(call)
    }
}

impl Shared {
    fn send(&self, frame: Frame) -> bool {
        self.out.send(|buf| frame.encode_into(Numbering::Counted, buf))
    }
}

/// The driver's slots by call. A call's id is a number given in order, so
/// it is hashed by one multiplication, not SipHash, which no caller can
/// steer: the ids are this end's own.
type Calls = HashMap<u32, Slot, BuildHasherDefault<Ids>>;

#[derive(Default)]
struct Ids(u64);

impl Hasher for Ids {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u32(b as u32);
        }
    }

    fn write_u32(&mut self, n: u32) {
        self.0 = (self.0 ^ n as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
}

/// Add `event` to its call's run, if the call is still open; `waiting`
/// lists the calls with a run to hand in, `windows` the calls owed their
/// window ([`Slot::window`]). An end is handed in at once.
fn deliver(calls: &mut Calls, waiting: &mut Vec<u32>, windows: &mut Vec<u32>, call: CallId, event: Event) {
    let end = matches!(event, Event::End(_));
    let Some(slot) = calls.get_mut(&call.0) else { return };
    if !end && std::mem::take(&mut slot.window) {
        windows.push(call.0);
    }
    #[cfg(feature = "trace")]
    {
        slot.traced.0 = call.0;
    }
    if slot.run.is_empty() {
        waiting.push(call.0);
    }
    slot.run.push(event);
    if end {
        calls.remove(&call.0);
    }
}

/// Grant the rest of the window to each call in `windows` still open.
fn grant(calls: &Calls, windows: &mut Vec<u32>, shared: &Weak<Shared>) {
    if windows.is_empty() {
        return;
    }
    let Some(shared) = shared.upgrade() else { return windows.clear() };
    for id in windows.drain(..) {
        if calls.contains_key(&id) {
            shared.send(Frame { call: CallId(id), seq: 0, body: Body::Credit(WINDOW - 1) });
        }
    }
}

/// Hand in every run read so far.
fn hand_in(calls: &mut Calls, waiting: &mut Vec<u32>) {
    for id in waiting.drain(..) {
        if let Some(slot) = calls.get_mut(&id) {
            slot.hand_in(false);
        }
    }
}

async fn drive(
    mut receive: Pin<Box<dyn Stream<Item = io::Result<bytes::Bytes>> + Send>>,
    write: impl Future<Output = io::Result<()>>,
    shared: Weak<Shared>,
    mut told: mpsc::UnboundedReceiver<Told>,
    said: oneshot::Sender<String>,
    binary_channel: bool,
) -> io::Result<()> {
    let mut said = Some(said);
    let mut calls = Calls::default();
    let ended = {
        let read = async {
            let (mut run, mut waiting) = (0, Vec::new());
            // Calls owed their window: `young` since the driver last stepped
            // aside, `old` since the time before. Granted once the driver has
            // read all there is (the end, if it was behind the item, came
            // with it), or once it has stepped aside twice since.
            let (mut young, mut old) = (Vec::new(), Vec::new());
            #[cfg(feature = "trace")]
            let mut frames = crate::trace::Frames::default();
            loop {
                // What was read is handed in before the driver waits, and
                // before it steps aside.
                let next = future::poll_fn(|cx| {
                    let next = receive.poll_next_unpin(cx);
                    if next.is_pending() {
                        hand_in(&mut calls, &mut waiting);
                        grant(&calls, &mut old, &shared);
                        grant(&calls, &mut young, &shared);
                    }
                    next
                })
                .await;
                let Some(bytes) = next else { break };
                // Step aside now and then while frames keep coming: the
                // write half sends the credit the reader has granted, and
                // the reader runs, so the far end is not held up until all
                // of it is read.
                run += 1;
                if run == RUN {
                    run = 0;
                    hand_in(&mut calls, &mut waiting);
                    grant(&calls, &mut old, &shared);
                    std::mem::swap(&mut old, &mut young);
                    crate::yield_now().await;
                }
                let bytes = bytes?;
                let bad = |e| io::Error::new(io::ErrorKind::InvalidData, e);
                let received = Received::read(bytes).map_err(bad)?;
                let (call, kind) = (received.head().call, received.head().kind);
                #[cfg(feature = "trace")]
                let key = if crate::trace::on() { frames.of(call.0, kind) } else { None };
                #[cfg(feature = "trace")]
                if let Some(key) = key {
                    crate::trace::stamp(key, crate::trace::Stage::Received);
                }
                if shared.strong_count() == 0 {
                    break;
                }
                while let Ok(told) = told.try_recv() {
                    match told {
                        Told::Opened(id, inbox) => calls.insert(id, inbox),
                        Told::Gone(id) => calls.remove(&id),
                    };
                }
                if kind == Kind::Item {
                    deliver(&mut calls, &mut waiting, &mut young, call, Event::Item(received));
                    #[cfg(feature = "trace")]
                    if let Some(key) = key {
                        crate::trace::stamp(key, crate::trace::Stage::Delivered);
                    }
                    continue;
                }
                match received.into_frame().map_err(bad)?.body {
                    Body::Hello(hello) => {
                        if let Some(shared) = shared.upgrade() {
                            shared.binary_input.store(binary_channel && hello.binary_input, Ordering::Release);
                        }
                        if let Some(said) = said.take() {
                            let _ = said.send(hello.interface);
                        }
                    }
                    Body::Progress(body) => {
                        if let Some(upload) = calls.get(&call.0).and_then(|slot| slot.upload.as_ref())
                            && !upload.granted && crate::input::is_byte_ack(&body) {
                            upload.byte_ack.store(true, Ordering::Release);
                        }
                    }
                    Body::Credit(n) => {
                        if let Some(upload) = calls.get_mut(&call.0).and_then(|slot| slot.upload.as_mut()) {
                            upload.granted |= n != 0;
                            upload.credit.grant(n);
                        }
                    }
                    Body::End(end) => deliver(&mut calls, &mut waiting, &mut young, call, Event::End(end)),
                    _ => {}
                }
            }
            Ok(())
        };
        futures::pin_mut!(read, write);
        match future::select(read, write).await {
            Either::Left((r, _)) | Either::Right((r, _)) => r,
        }
    };
    // Every call still open learns the channel closed, and none opens after.
    told.close();
    drop(told);
    drop(calls);
    ended
}

/// How many frames the driver reads before it steps aside.
const RUN: u32 = 256;

/// A call that is open. Dropped before its end, it cancels.
struct Open {
    stop_input: Option<AbortHandle>,
    id: u32,
    inbox: Arc<Inbox>,
    /// The run last taken from the inbox, not yet read.
    taken: VecDeque<Event>,
    shared: Arc<Shared>,
    done: bool,
}

impl Open {
    /// The next event, or `None` once the inbox is closed and empty.
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        if let Some(event) = self.taken.pop_front() {
            return Poll::Ready(Some(event));
        }
        let mut queue = lock(&self.inbox.0);
        std::mem::swap(&mut self.taken, &mut queue.events);
        if let Some(event) = self.taken.pop_front() {
            return Poll::Ready(Some(event));
        }
        if queue.closed {
            return Poll::Ready(None);
        }
        queue.wake = Some(cx.waker().clone());
        Poll::Pending
    }

    fn grant(&self, n: u32) {
        self.shared.send(Frame { call: CallId(self.id), seq: 0, body: Body::Credit(n) });
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        if let Some(abort) = &self.stop_input { abort.abort(); }
        if self.done {
            return;
        }
        let _ = self.shared.tell.unbounded_send(Told::Gone(self.id));
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
        let event = match self.call.poll_event(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(event) => event,
        };
        Poll::Ready(match event {
            Some(Event::Item(received)) => {
                let raw = item(received);
                self.taken += 1;
                stamp!((self.call.id, self.taken), Took);
                if self.taken.is_multiple_of(TOP_UP) {
                    self.call.grant(TOP_UP);
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

/// A call passed on ([`Connection::pass`]): its items, then its end.
pub struct Passed(Result<Open, Option<Failure>>);

impl Passed {
    /// A call that fails at once, with `failure`.
    pub fn failed(failure: Failure) -> Passed {
        Passed(Err(Some(failure)))
    }

    /// Say it may write `n` more items.
    pub fn grant(&self, n: u32) {
        if let Ok(call) = &self.0 {
            call.grant(n);
        }
    }
}

impl Stream for Passed {
    type Item = Result<Raw, Failure>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let call = match &mut self.0 {
            Err(failure) => return Poll::Ready(failure.take().map(Err)),
            Ok(call) if call.done => return Poll::Ready(None),
            Ok(call) => call,
        };
        let event = match call.poll_event(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(event) => event,
        };
        Poll::Ready(match event {
            Some(Event::Item(received)) => Some(Ok(item(received))),
            Some(Event::End(end)) => {
                call.done = true;
                end.err().map(Err)
            }
            None => {
                call.done = true;
                Some(Err(unavailable()))
            }
        })
    }
}

/// What a call passed on fails with when the connection it went over is gone.
fn unavailable() -> Failure {
    Failure { code: "unavailable".into(), message: "the connection to it has closed".into(), data: None }
}

fn encode(params: &impl Serialize) -> Result<Option<Raw>, Error> {
    let raw = Raw::of(params).map_err(Error::Params)?;
    Ok(match raw.get() {
        "{}" | "null" => None,
        _ => Some(raw),
    })
}

/// An item's body, as its head's read found it.
fn item(received: Received) -> Raw {
    received.into_body().expect("an item has a body")
}

fn null() -> Raw {
    Raw::of(&()).expect("null is JSON")
}
