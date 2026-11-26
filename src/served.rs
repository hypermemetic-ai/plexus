//! What `#[activation]` writes for a native build calls: an activation kept
//! on a thread of its own, and a body read by the value layout.
//!
//! A component runs on one thread, so an activation's methods take `&self`
//! and keep what changes in a `RefCell`: it need not be `Sync`. Natively the
//! same activation is moved, once, to a thread of its own and held there in
//! an `Rc`; every call from every connection is handed to that thread, where
//! the calls run together as they do in a component, and the answers are
//! handed back. The [`Server`](crate::Server) stays `Send`, served on any
//! executor; the activation need only be `Send`.
//!
//! The thread runs one call at a time; another runs when it waits (an
//! `async` method awaiting, a stream with no item ready or no credit), and
//! a stream steps aside every [`RUN`] items. A stream's items are made only
//! as far as the caller's credit allows, and cross to the server in runs:
//! one wake-up for all that were made meanwhile.

use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::channel::{mpsc, oneshot};
use futures::future::{self, BoxFuture, Either, FutureExt, LocalBoxFuture};
use futures::executor::LocalPool;
use futures::stream::{Stream, StreamExt};
use futures::task::LocalSpawnExt;
use plexus_proto::{Failure, Raw};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{CallContext, lock};
use crate::server::{Paced, Server, failure};

type Job<A> = Box<dyn FnOnce(&Rc<A>) -> LocalBoxFuture<'static, ()> + Send>;

/// How many items a stream makes before it steps aside for the other calls
/// on its thread.
const RUN: u32 = 256;

/// One stream's way across: the items made and not yet taken, the credit
/// given and not yet used, and who waits on which. The activation's side
/// puts items as they are made and the server's side takes all there are at
/// once, so a run of items costs one wake-up, not one each.
#[derive(Default)]
struct Hop {
    items: VecDeque<Result<Raw, Failure>>,
    /// No more items will be put.
    ended: bool,
    /// Credit given and not yet taken by the activation's side.
    granted: u32,
    /// The server's side is gone: the method's stream is dropped.
    left: bool,
    reader: Option<Waker>,
    feeder: Option<Waker>,
}

impl Hop {
    /// Put the last item, if there is one, and end; a no-op once ended.
    fn end(hop: &Mutex<Hop>, last: Option<Failure>) {
        let reader = {
            let mut hop = lock(hop);
            if hop.ended {
                return;
            }
            hop.items.extend(last.map(Err));
            hop.ended = true;
            hop.reader.take()
        };
        if let Some(w) = reader {
            w.wake();
        }
    }
}

/// The activation's side of one stream, handed to [`feed`].
pub struct Asks(Arc<Mutex<Hop>>);

/// The server's side of one stream: its items, in runs.
pub struct Served {
    hop: Arc<Mutex<Hop>>,
    taken: VecDeque<Result<Raw, Failure>>,
}

impl Served {
    fn failed(failure: Failure) -> Served {
        let hop = Hop { items: VecDeque::from([Err(failure)]), ended: true, ..Hop::default() };
        Served { hop: Arc::new(Mutex::new(hop)), taken: VecDeque::new() }
    }
}

impl Stream for Served {
    type Item = Result<Raw, Failure>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.taken.pop_front() {
            return Poll::Ready(Some(item));
        }
        let this = &mut *self;
        let mut hop = lock(&this.hop);
        std::mem::swap(&mut this.taken, &mut hop.items);
        if let Some(item) = this.taken.pop_front() {
            // Busy with a run: no wake-up is wanted until it is taken.
            hop.reader = None;
            return Poll::Ready(Some(item));
        }
        if hop.ended {
            return Poll::Ready(None);
        }
        if !hop.reader.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            hop.reader = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Paced for Served {
    fn grant(&self, n: u32) {
        let feeder = {
            let mut hop = lock(&self.hop);
            hop.granted = hop.granted.saturating_add(n);
            hop.feeder.take()
        };
        if let Some(w) = feeder {
            w.wake();
        }
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        let feeder = {
            let mut hop = lock(&self.hop);
            hop.left = true;
            hop.items.clear();
            hop.feeder.take()
        };
        if let Some(w) = feeder {
            w.wake();
        }
    }
}

/// Register a stream an activation serves: `run` makes the server's side.
pub fn stream<F>(server: &mut Server, name: &str, run: F)
where
    F: Fn(Raw) -> Served + Send + Sync + 'static,
{
    stream_with_caller(server, name, move |params, _| run(params));
}

/// Register a local stream supplied with the connection's caller.
pub fn stream_with_caller<F>(server: &mut Server, name: &str, run: F)
where
    F: Fn(Raw, CallContext) -> Served + Send + Sync + 'static,
{
    server.paced(name, Arc::new(move |params, call| Box::new(run(params, call))));
}

/// Register a local stream that takes keys on the activation's segment.
pub fn stream_with_context<F>(server: &mut Server, name: &str, at: &str, keys: &'static [&'static str], run: F)
where
    F: Fn(Raw, CallContext) -> Served + Send + Sync + 'static,
{
    server.paced_with_context(name, at, keys, Arc::new(move |params, call| Box::new(run(params, call))));
}

/// End a stream whose activation refused to start it.
pub fn fail_stream(asks: Asks, failure: Failure) {
    Hop::end(&asks.0, Some(failure));
}

/// An activation on a thread of its own. Clones send to the same one; the
/// thread ends when every clone is gone.
pub struct Local<A> {
    jobs: mpsc::UnboundedSender<Job<A>>,
}

impl<A> Clone for Local<A> {
    fn clone(&self) -> Self {
        Local { jobs: self.jobs.clone() }
    }
}

/// The thread a [`Local`] made by [`Local::later`] sends to, before it is
/// started: what is sent waits until then.
pub struct Later<A> {
    taken: mpsc::UnboundedReceiver<Job<A>>,
}

impl<A: Send + 'static> Later<A> {
    /// Move `activation` to a thread of its own. Started inside a tokio
    /// runtime, the thread enters it, so a method may use tokio's timers and
    /// sockets.
    pub fn start(self, activation: A) {
        let mut taken = self.taken;
        #[cfg(feature = "tokio")]
        let runtime = tokio::runtime::Handle::try_current().ok();
        std::thread::Builder::new()
            .name("plexus-activation".into())
            .spawn(move || {
                #[cfg(feature = "tokio")]
                let _entered = runtime.as_ref().map(|r| r.enter());
                let activation = Rc::new(activation);
                let mut pool = LocalPool::new();
                let spawner = pool.spawner();
                pool.run_until(async move {
                    while let Some(job) = taken.next().await {
                        let _ = spawner.spawn_local(job(&activation));
                    }
                });
            })
            .expect("a thread for the activation");
    }
}

impl<A: Send + 'static> Local<A> {
    /// Move `activation` to a thread of its own ([`Later::start`]).
    pub fn start(activation: A) -> Local<A> {
        let (local, later) = Local::later();
        later.start(activation);
        local
    }

    /// A handle to a thread not started yet, so that methods can be
    /// registered while the activation is still at hand: a parent's
    /// children are found on it before it moves.
    pub fn later() -> (Local<A>, Later<A>) {
        let (jobs, taken) = mpsc::unbounded::<Job<A>>();
        (Local { jobs }, Later { taken })
    }

    /// One answer: `run` is called on the activation's thread with what
    /// `args` read, unless it failed. A panic fails the call `internal`; the
    /// caller leaving drops what `run` made.
    pub fn call<P, F, Fut>(&self, args: Result<P, Failure>, run: F) -> BoxFuture<'static, Result<Raw, Failure>>
    where
        P: Send + 'static,
        F: FnOnce(Rc<A>, P) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Raw, Failure>> + 'static,
    {
        self.submit(args, run)
    }

    /// Check an inbound call on the activation's thread before dispatching it
    /// to a descendant.
    #[doc(hidden)]
    pub fn validate<F>(&self, call: CallContext, run: F) -> BoxFuture<'static, Result<(), Failure>>
    where
        F: FnOnce(Rc<A>, CallContext) -> Result<(), Failure> + Send + 'static,
    {
        self.submit(Ok(call), move |activation, call| async move { run(activation, call) })
    }

    #[doc(hidden)]
    pub fn submit<P, F, Fut, T>(&self, args: Result<P, Failure>, run: F) -> BoxFuture<'static, Result<T, Failure>>
    where
        P: Send + 'static,
        F: FnOnce(Rc<A>, P) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Failure>> + 'static,
        T: Send + 'static,
    {
        let args = match args {
            Ok(args) => args,
            Err(f) => return future::ready(Err(f)).boxed(),
        };
        let (answer, answered) = oneshot::channel();
        let job: Job<A> = Box::new(move |a| {
            if answer.is_canceled() {
                return future::ready(()).boxed_local();
            }
            let made = caught(run(a.clone(), args)).map(|r| r.and_then(|r| r));
            async move {
                let mut answer = answer;
                let got = match future::select(answer.cancellation(), std::pin::pin!(made)).await {
                    Either::Left(_) => return,
                    Either::Right((got, _)) => got,
                };
                stamp!(crate::trace::current(), MethodDone);
                let _ = answer.send(got);
                stamp!(crate::trace::current(), AnswerSent);
            }
            .boxed_local()
        });
        stamp!(within job);
        stamp!(crate::trace::current(), JobBoxed);
        let sent = self.jobs.unbounded_send(job);
        stamp!(crate::trace::current(), JobSent);
        async move {
            sent.map_err(|_| stopped())?;
            answered.await.unwrap_or_else(|_| Err(stopped()))
        }
        .boxed()
    }

    /// Many answers, worked out on the activation's thread only as far as
    /// the server's credit allows: `run` is called there and hands its
    /// stream to [`feed`]. A panic ends the stream with an `internal`
    /// failure; the server's side dropped drops the method's stream.
    pub fn stream<P, F, Fut>(&self, args: Result<P, Failure>, run: F) -> Served
    where
        P: Send + 'static,
        F: FnOnce(Rc<A>, P, Asks) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let args = match args {
            Ok(args) => args,
            Err(f) => return Served::failed(f),
        };
        let hop = Arc::new(Mutex::new(Hop::default()));
        let asks = Asks(hop.clone());
        let job: Job<A> = Box::new(move |a| {
            let ending = Ending(asks.0.clone());
            caught(run(a.clone(), args, asks)).map(move |r| ending.end(r.err())).boxed_local()
        });
        stamp!(within job);
        if self.jobs.unbounded_send(job).is_err() {
            return Served::failed(stopped());
        }
        Served { hop, taken: VecDeque::new() }
    }
}

/// Ends a stream when its job does: with the job's panic if it had one,
/// with "stopped" if the job is dropped unfinished (its thread ending).
struct Ending(Arc<Mutex<Hop>>);

impl Ending {
    fn end(self, panic: Option<Failure>) {
        Hop::end(&self.0, panic);
    }
}

impl Drop for Ending {
    fn drop(&mut self) {
        Hop::end(&self.0, Some(stopped()));
    }
}

/// Make the items of `items` as far as the credit given allows, and hand
/// them across as they are made, until there are none or the server's side
/// has left. Steps aside every [`RUN`] items, so that other calls run.
pub async fn feed<T>(asks: Asks, items: impl Stream<Item = T>, write: impl Fn(T) -> Result<Raw, Failure>) {
    let hop = asks.0;
    let mut items = std::pin::pin!(items);
    let mut credit = 0u32;
    #[cfg(feature = "trace")]
    let (call, mut n) = (crate::trace::current().0, 0);
    future::poll_fn(move |cx| {
        let mut made = 0;
        loop {
            if credit == 0 || made == 0 {
                let mut h = lock(&hop);
                if h.left {
                    return Poll::Ready(());
                }
                credit += std::mem::take(&mut h.granted);
                if credit == 0 {
                    h.feeder = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
            if made == RUN {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            stamp!((call, n + 1), Asked);
            let next = match std::panic::catch_unwind(AssertUnwindSafe(|| items.as_mut().poll_next(cx))) {
                Ok(Poll::Pending) => {
                    // A cancel must reach the method while it waits.
                    let mut h = lock(&hop);
                    if h.left {
                        return Poll::Ready(());
                    }
                    h.feeder = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Ok(Poll::Ready(next)) => next,
                Err(panic) => {
                    Hop::end(&hop, Some(panicked(panic)));
                    return Poll::Ready(());
                }
            };
            let Some(item) = next else {
                Hop::end(&hop, None);
                return Poll::Ready(());
            };
            stamp!({ n += 1; (call, n) }, ItemMade);
            let item = match std::panic::catch_unwind(AssertUnwindSafe(|| write(item))) {
                Ok(item) => item,
                Err(panic) => Err(panicked(panic)),
            };
            stamp!((call, n), ItemSerialized);
            if item.is_err() {
                Hop::end(&hop, item.err());
                return Poll::Ready(());
            }
            credit -= 1;
            made += 1;
            let reader = {
                let mut h = lock(&hop);
                if h.left {
                    return Poll::Ready(());
                }
                h.items.push_back(item);
                h.reader.take()
            };
            stamp!((call, n), ItemPushed);
            if let Some(w) = reader {
                w.wake();
            }
            stamp!((call, n), ItemSent);
        }
    })
    .await
}

fn caught<T>(made: impl Future<Output = T>) -> impl Future<Output = Result<T, Failure>> {
    AssertUnwindSafe(made).catch_unwind().map(|r| r.map_err(panicked))
}

fn panicked(panic: Box<dyn std::any::Any + Send>) -> Failure {
    let message = panic.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| panic.downcast_ref::<String>().cloned()).unwrap_or_default();
    failure("internal", &message)
}

fn stopped() -> Failure {
    failure("internal", "the activation's thread has stopped")
}

/// A call's parameters, by name, as they arrived.
pub struct Params<'a>(HashMap<String, &'a RawValue>);

/// Read a body as an object whose keys are among `names`; anything else
/// fails `invalid-params`.
pub fn params<'a>(body: &'a Raw, names: &[&str]) -> Result<Params<'a>, Failure> {
    let map: HashMap<String, &RawValue> = serde_json::from_str(body.get()).map_err(|e| invalid("", &format!("the parameters are an object: {e}")))?;
    if let Some(name) = map.keys().find(|k| !names.contains(&k.as_str())) {
        return Err(invalid(name, &format!("`{name}` is no parameter of this method")));
    }
    Ok(Params(map))
}

impl Params<'_> {
    /// One parameter, read by the value layout. An absent one is `default`
    /// (its JSON) when there is one, else `null`, which only an option takes.
    pub fn take<T: DeserializeOwned>(&self, name: &str, default: Option<&str>) -> Result<T, Failure> {
        match self.0.get(name) {
            Some(value) => serde_json::from_str(value.get()).map_err(|e| invalid(name, &format!("`{name}`: {e}"))),
            None => serde_json::from_str(default.unwrap_or("null")).map_err(|_| invalid(name, &format!("`{name}` is missing"))),
        }
    }
}

/// `invalid-params`, its data the parameter's name.
fn invalid(name: &str, message: &str) -> Failure {
    let data = (!name.is_empty()).then(|| Raw::of(name).expect("a string is JSON"));
    Failure { data, ..failure("invalid-params", message) }
}

/// A method's typed failure: its code the case's name, its data the case's
/// value, its message the case's doc. `cases` holds each case as written on
/// the wire, its name and its doc; a type with no cases fails `failed`, the
/// whole value its data.
pub fn failed<E: Serialize>(error: &E, cases: &[(&str, &str, &str)]) -> Failure {
    let value = match serde_json::to_value(error) {
        Ok(v) => v,
        Err(e) => return failure("internal", &e.to_string()),
    };
    let (wire, data) = match value {
        serde_json::Value::String(s) if !cases.is_empty() => (s, None),
        serde_json::Value::Object(o) if o.len() == 1 && !cases.is_empty() => o.into_iter().next().map(|(k, v)| (k, Some(v))).expect("one key"),
        other => return Failure { data: Raw::of(&other).ok(), ..failure("failed", "") },
    };
    let (code, message) = cases.iter().find(|(w, ..)| *w == wire).map(|(_, n, d)| (*n, *d)).unwrap_or((&wire, ""));
    Failure { data: data.and_then(|d| Raw::of(&d).ok()), ..failure(code, message) }
}

/// Bytes as a parameter, an answer or an item: base64, as in a record.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct B64(#[serde(with = "plexus_proto::bytes")] pub Vec<u8>);
