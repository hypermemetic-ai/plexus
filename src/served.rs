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

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use futures::channel::{mpsc, oneshot};
use futures::executor::LocalPool;
use futures::future::{self, BoxFuture, Either, FutureExt, LocalBoxFuture};
use futures::stream::{BoxStream, Stream, StreamExt};
use futures::task::LocalSpawnExt;
use plexus_proto::{Failure, Raw};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::server::failure;

type Job<A> = Box<dyn FnOnce(&Rc<A>) -> LocalBoxFuture<'static, ()> + Send>;

/// One item of a stream asked for: `None` when there are no more.
type Ask = oneshot::Sender<Option<Result<Raw, Failure>>>;

/// The asks of one stream, in order, until its reader leaves.
pub type Asks = mpsc::UnboundedReceiver<Ask>;

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
        let args = match args {
            Ok(args) => args,
            Err(f) => return future::ready(Err(f)).boxed(),
        };
        let (answer, answered) = oneshot::channel();
        let job: Job<A> = Box::new(move |a| {
            let made = caught(run(a.clone(), args)).map(|r| r.and_then(|r| r));
            async move {
                let mut answer = answer;
                let got = match future::select(std::pin::pin!(made), answer.cancellation()).await {
                    Either::Left((got, _)) => got,
                    Either::Right(_) => return,
                };
                let _ = answer.send(got);
            }
            .boxed_local()
        });
        let sent = self.jobs.unbounded_send(job);
        async move {
            sent.map_err(|_| stopped())?;
            answered.await.unwrap_or_else(|_| Err(stopped()))
        }
        .boxed()
    }

    /// Many answers, each worked out only when the server asks for it: `run`
    /// is called on the activation's thread and hands its stream to [`feed`].
    pub fn stream<P, F, Fut>(&self, args: Result<P, Failure>, run: F) -> BoxStream<'static, Result<Raw, Failure>>
    where
        P: Send + 'static,
        F: FnOnce(Rc<A>, P, Asks) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let args = match args {
            Ok(args) => args,
            Err(f) => return futures::stream::once(future::ready(Err(f))).boxed(),
        };
        let (ask, asks) = mpsc::unbounded::<Ask>();
        let job: Job<A> = Box::new(move |a| caught(run(a.clone(), args, asks)).map(|_| ()).boxed_local());
        if self.jobs.unbounded_send(job).is_err() {
            return futures::stream::once(future::ready(Err(stopped()))).boxed();
        }
        // Dropping the stream drops `ask`, which ends `feed`.
        futures::stream::unfold(Some(ask), |ask| async move {
            let ask = ask?;
            let (tx, rx) = oneshot::channel();
            ask.unbounded_send(tx).ok()?;
            match rx.await {
                Ok(Some(item)) => Some((item, Some(ask))),
                Ok(None) => None,
                Err(_) => Some((Err(stopped()), None)),
            }
        })
        .boxed()
    }
}

/// Answer each ask with the next of `items`, until there are none or the
/// reader has left.
pub async fn feed<T>(mut asks: Asks, items: impl Stream<Item = T>, write: impl Fn(T) -> Result<Raw, Failure>) {
    let mut items = std::pin::pin!(items);
    while let Some(mut ask) = asks.next().await {
        let next = match future::select(caught(items.next()), ask.cancellation()).await {
            Either::Left((next, _)) => next,
            Either::Right(_) => return,
        };
        let last = !matches!(next, Ok(Some(_)));
        let _ = ask.send(match next {
            Ok(Some(item)) => Some(write(item)),
            Ok(None) => None,
            Err(f) => Some(Err(f)),
        });
        if last {
            return;
        }
    }
}

fn caught<T>(made: impl Future<Output = T>) -> impl Future<Output = Result<T, Failure>> {
    AssertUnwindSafe(made).catch_unwind().map(|r| {
        r.map_err(|panic| {
            let message = panic.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| panic.downcast_ref::<String>().cloned()).unwrap_or_default();
            failure("internal", &message)
        })
    })
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
