//! The serving end: reads `open`, runs the method, writes what it answers.

use std::collections::HashMap;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::future::{self, AbortHandle, Abortable, BoxFuture, Either, FutureExt};
use futures::task::AtomicWaker;
use futures::stream::{BoxStream, FuturesUnordered, StreamExt};
use plexus_proto::{Body, CallId, Failure, Frame, Kind, Numbering, Open, Raw};
use serde::Serialize;

use crate::channel::{Channel, pump};
use crate::{Activation, Place, lock};

type One = Arc<dyn Fn(Raw) -> BoxFuture<'static, Result<Raw, Failure>> + Send + Sync>;
type Many = Arc<dyn Fn(Raw) -> BoxStream<'static, Result<Raw, Failure>> + Send + Sync>;
pub(crate) type Pacing = Arc<dyn Fn(Raw) -> Box<dyn Paced> + Send + Sync>;

/// A stream that makes its items ahead of the server, as far as the credit
/// it is granted: a call passed on, or an activation's stream on its thread.
pub(crate) trait Paced: futures::Stream<Item = Result<Raw, Failure>> + Send + Unpin {
    /// Say it may make `n` more items.
    fn grant(&self, n: u32);
}

impl Paced for Box<dyn Paced> {
    fn grant(&self, n: u32) {
        (**self).grant(n)
    }
}

impl Paced for crate::Passed {
    fn grant(&self, n: u32) {
        crate::Passed::grant(self, n)
    }
}

#[derive(Clone)]
enum Handler {
    One(One),
    Many(Many),
    /// Paced by the credit granted to it: an activation's stream.
    Paced(Pacing),
    /// A call under a child held across a channel: the child, and the
    /// method's name below it.
    Pass(Arc<dyn Activation>, String),
}

/// A place in `_tree`: described here, or a child mounted here, whose
/// places are asked of it.
#[derive(Clone)]
enum Entry {
    Described(Place),
    Mounted(String, Arc<dyn Activation>),
}

/// Methods by name, served on as many channels as are handed to
/// [`serve`](Server::serve).
#[derive(Default)]
pub struct Server {
    methods: HashMap<String, Handler>,
    /// Every place described or mounted, in order, for `_tree`.
    places: Vec<Entry>,
    interface: Option<String>,
}

impl Server {
    pub fn new() -> Server {
        Server::default()
    }

    /// The interface hash said at hello, the first frame of every connection.
    pub fn hello(&mut self, interface: &str) {
        self.interface = Some(interface.to_owned());
    }

    /// A method with one answer. Its parameters arrive as JSON (`{}` when
    /// the caller sent none).
    pub fn method<F, A>(&mut self, name: &str, run: F)
    where
        F: Fn(Raw) -> A + Send + Sync + 'static,
        A: Future<Output = Result<Raw, Failure>> + Send + 'static,
    {
        self.methods.insert(name.to_owned(), Handler::One(Arc::new(move |p| run(p).boxed())));
    }

    /// A method with many answers. An `Err` ends the call with that failure.
    pub fn stream<F, S>(&mut self, name: &str, run: F)
    where
        F: Fn(Raw) -> S + Send + Sync + 'static,
        S: futures::Stream<Item = Result<Raw, Failure>> + Send + 'static,
    {
        self.methods.insert(name.to_owned(), Handler::Many(Arc::new(move |p| run(p).boxed())));
    }

    pub(crate) fn paced(&mut self, name: &str, run: Pacing) {
        self.methods.insert(name.to_owned(), Handler::Paced(run));
    }

    /// Say what is served at `at`: `<at>._ir` answers `ir` and `<at>._hash`
    /// its `hash`, and `_tree` lists every place, with its hash.
    pub fn describe(&mut self, at: &str, ir: &str) -> Result<(), serde_json::Error> {
        #[derive(serde::Deserialize)]
        struct Hashed {
            hash: String,
        }
        let said = serde_json::from_str::<Hashed>(ir)?.hash;
        let hash = raw(&said).expect("a string is JSON");
        let ir = Raw::parse(ir).map_err(serde::de::Error::custom)?;
        self.places.push(Entry::Described(Place { place: at.to_owned(), hash: Some(said) }));
        self.method(&format!("{at}._ir"), move |_| std::future::ready(Ok(ir.clone())));
        self.method(&format!("{at}._hash"), move |_| std::future::ready(Ok(hash.clone())));
        Ok(())
    }

    /// Hold `child` at `at`: every call under `<at>.`, its `_ir` and `_hash`
    /// too, is passed to it, unless a method of that name is registered
    /// here; `_tree` lists its places, asked of it each time. Its credit and
    /// cancel are passed on as the caller sends them.
    pub fn mount(&mut self, at: &str, child: Box<dyn Activation>) {
        self.places.push(Entry::Mounted(at.to_owned(), Arc::from(child)));
    }

    fn find(&self, name: &str) -> Option<Handler> {
        if name == "_tree" {
            let places = self.places.clone();
            return Some(Handler::One(Arc::new(move |_| tree(places.clone()).boxed())));
        }
        if let Some(own) = self.methods.get(name) {
            return Some(own.clone());
        }
        // The child mounted deepest that the name is under.
        self.places
            .iter()
            .filter_map(|e| match e {
                Entry::Mounted(at, child) => Some((at.len(), name.strip_prefix(at.as_str())?.strip_prefix('.')?, child)),
                Entry::Described(_) => None,
            })
            .max_by_key(|(len, ..)| *len)
            .map(|(_, rest, child)| Handler::Pass(child.clone(), rest.to_owned()))
    }

    /// Serve one channel until it closes. Every call runs at once with the
    /// others, inside this future: spawn it on any executor.
    pub fn serve(self: &Arc<Self>, channel: Channel) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let server = self.clone();
        let Channel { send, mut receive } = channel;
        let (out, frames) = mpsc::unbounded::<Vec<u8>>();
        if let Some(interface) = &self.interface {
            let _ = out.unbounded_send(Frame::hello(interface, false).encode(Numbering::Counted));
        }
        let (start, started) = mpsc::unbounded::<BoxFuture<'static, ()>>();
        let calls: Arc<Mutex<HashMap<u32, Running>>> = Arc::default();
        let read = async move {
            while let Some(bytes) = receive.next().await {
                let frame = Frame::decode(&bytes?).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let id = frame.call.0;
                match frame.body {
                    Body::Open(open) => {
                        let credit = Arc::new(Credit { left: AtomicU32::new(1), wake: AtomicWaker::new() });
                        let (abort, registration) = AbortHandle::new_pair();
                        lock(&calls).insert(id, Running { credit: credit.clone(), abort });
                        let ran = run(server.clone(), id, open, out.clone(), credit, calls.clone());
                        let _ = start.unbounded_send(Abortable::new(ran, registration).map(|_| ()).boxed());
                    }
                    Body::Credit(n) => {
                        if let Some(call) = lock(&calls).get(&id) {
                            let _ = call.credit.left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| Some(left.saturating_add(n)));
                            call.credit.wake.wake();
                        }
                    }
                    // Dropped at once: the method's future and stream with it.
                    Body::Cancel => {
                        if let Some(call) = lock(&calls).remove(&id) {
                            call.abort.abort();
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        };
        async move {
            let write = pump(send, frames).fuse();
            let read = read.fuse();
            let running = run_all(started).fuse();
            futures::pin_mut!(read, write, running);
            // The channel closing ends the connection and drops every call.
            futures::select! {
                r = read => r,
                w = write => w,
                () = running => Ok(()),
            }
        }
    }
}

/// How many items a stream writes before it steps aside.
const RUN: u32 = 256;

struct Running {
    credit: Arc<Credit>,
    abort: AbortHandle,
}

/// How many items a stream may still write, and who waits to write one.
/// Atomic, so an item takes its credit with no lock.
struct Credit {
    left: AtomicU32,
    wake: AtomicWaker,
}

impl Credit {
    /// Up to `most` of the credit there is, or `Pending` with the waker kept
    /// if there is none.
    fn poll_take(&self, cx: &mut Context<'_>, most: u32) -> Poll<u32> {
        let take = || self.left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| (left > 0).then(|| left - left.min(most)));
        if let Ok(left) = take() {
            return Poll::Ready(left.min(most));
        }
        self.wake.register(cx.waker());
        // Granted between the first look and the waker kept.
        match take() {
            Ok(left) => Poll::Ready(left.min(most)),
            Err(_) => Poll::Pending,
        }
    }
}

/// Run every call handed in, together, until no more can be.
async fn run_all(mut started: mpsc::UnboundedReceiver<BoxFuture<'static, ()>>) {
    let mut running = FuturesUnordered::new();
    loop {
        futures::select! {
            call = started.next() => match call {
                Some(call) => running.push(call),
                None => break,
            },
            () = running.select_next_some() => {}
        }
    }
}

async fn run(
    server: Arc<Server>,
    id: u32,
    open: Open,
    out: mpsc::UnboundedSender<Vec<u8>>,
    credit: Arc<Credit>,
    calls: Arc<Mutex<HashMap<u32, Running>>>,
) {
    let call = CallId(id);
    let send = |kind: Kind, body: &[u8]| {
        let mut bytes = Vec::with_capacity(body.len() + 16);
        plexus_proto::write(&mut bytes, call, kind, 0, body, Numbering::Counted);
        let _ = out.unbounded_send(bytes);
    };
    let given = open.params;
    let params = given.clone().unwrap_or_else(|| raw(&serde_json::Map::new()).expect("{} is JSON"));
    let ended: Result<(), Failure> = match server.find(&open.method) {
        None => Err(failure("not-found", &format!("no method {}", open.method))),
        Some(Handler::One(f)) => match caught(f(params)).await.and_then(|answer| answer) {
            Ok(answer) => {
                send(Kind::Item, answer.get().as_bytes());
                Ok(())
            }
            Err(f) => Err(f),
        },
        Some(Handler::Many(f)) => {
            let mut items = f(params);
            let mut run = 0;
            loop {
                // Step aside now and then, so what is made is written as it
                // is made, not a window's worth at a time.
                run += 1;
                if run == RUN {
                    run = 0;
                    crate::yield_now().await;
                }
                // Wait for credit before asking for the next item, so the
                // method runs no further ahead than the caller allowed.
                take(&credit).await;
                match caught(items.next()).await {
                    Ok(Some(Ok(item))) => send(Kind::Item, item.get().as_bytes()),
                    Ok(Some(Err(f))) | Err(f) => break Err(f),
                    Ok(None) => break Ok(()),
                }
            }
        }
        Some(Handler::Pass(child, rest)) => {
            // The open carried credit for one item; the open passed on
            // carries it on.
            take(&credit).await;
            paced(&credit, child.pass(&rest, given), send).await
        }
        Some(Handler::Paced(f)) => paced(&credit, f(params), send).await,
    };
    lock(&calls).remove(&id);
    match ended {
        Ok(()) => send(Kind::End, b""),
        Err(f) => send(Kind::End, serde_json::to_string(&f).expect("a Failure is JSON").as_bytes()),
    }
}

/// Every credit is passed on to `items` as it arrives, and its items are
/// written as they come: nothing is made beyond what the caller allowed.
async fn paced(credit: &Credit, mut items: impl Paced, send: impl Fn(Kind, &[u8])) -> Result<(), Failure> {
    let mut run = 0;
    loop {
        run += 1;
        if run == RUN {
            run = 0;
            crate::yield_now().await;
        }
        let next = match future::select(more(credit), items.next()).await {
            Either::Left((n, _)) => Err(n),
            Either::Right((item, _)) => Ok(item),
        };
        match next {
            Err(n) => items.grant(n),
            Ok(Some(Ok(item))) => send(Kind::Item, item.get().as_bytes()),
            Ok(Some(Err(f))) => break Err(f),
            Ok(None) => break Ok(()),
        }
    }
}

/// Take one item's credit, waiting for it if there is none.
fn take(credit: &Credit) -> impl Future<Output = ()> + '_ {
    futures::future::poll_fn(move |cx| credit.poll_take(cx, 1).map(|_| ()))
}

/// All the credit there is, waiting for some if there is none.
fn more(credit: &Credit) -> impl Future<Output = u32> + Unpin + '_ {
    futures::future::poll_fn(move |cx| credit.poll_take(cx, u32::MAX))
}

/// `_tree`: every place, each child's asked of it now. A child that cannot
/// be reached is listed at its own place, its hash unknown; the rest of the
/// tree is as ever.
async fn tree(places: Vec<Entry>) -> Result<Raw, Failure> {
    let mut out = Vec::new();
    for entry in places {
        match entry {
            Entry::Described(place) => out.push(place),
            Entry::Mounted(at, child) => match child.tree(&at).await {
                Ok(places) => out.extend(places),
                Err(_) => out.push(Place { place: at, hash: None }),
            },
        }
    }
    raw(&out)
}

/// A method that panics fails its call, `internal`, and nothing else.
async fn caught<T>(answer: impl Future<Output = T>) -> Result<T, Failure> {
    AssertUnwindSafe(answer).catch_unwind().await.map_err(|panic| {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        failure("internal", &message)
    })
}

pub(crate) fn failure(code: &str, message: &str) -> Failure {
    Failure { code: code.to_owned(), message: message.to_owned(), data: None }
}

/// A value as JSON: what a method answers.
pub fn raw<T: Serialize + ?Sized>(value: &T) -> Result<Raw, Failure> {
    Raw::of(value).map_err(|e| failure("internal", &e.to_string()))
}

/// A method's failure: a code a program matches on, a sentence for a
/// person, and data.
pub fn fail(code: &str, message: &str, data: impl Serialize) -> Failure {
    Failure { data: Raw::of(&data).ok(), ..failure(code, message) }
}
