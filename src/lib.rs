//! The one crate a Plexus activation depends on.
//!
//! It holds `#[plexus::activation]`, the types a method's signature names,
//! and the runtime: a [`Connection`] that calls and a [`Server`] that
//! answers, over any [`Channel`] — a [`pair`] in memory, and with the
//! `tokio` feature the stdio of a process ([`stdio`]) and a WebSocket
//! ([`ws`]). Frames are plexus-proto's.
//!
//! A [`Server`] serves methods untyped: a name, a JSON body in, items out;
//! it may hold a child across a channel ([`Server::mount`], [`Remote`]).
//! `#[activation]`, in a native build, writes the typed side onto it: each
//! module gets `serve(activation, server, at)`, and its activation type
//! [`Serve`], so that one line of `main` is a process ([`main`]).
//!
//! With the `store` feature an activation keeps its state in its own SQLite
//! database, a `Store`.
//!
//! Native methods can take a [`Caller`] supplied by the connection.
//! `#[activation(describe)]` can also describe [`Input`] and [`Progress`].
//!
//! ```
//! use plexus::{Connection, Server, pair, raw};
//! use std::sync::Arc;
//!
//! # #[tokio::main(flavor = "current_thread")] async fn main() {
//! let mut server = Server::new();
//! server.method("kiln.get", |_| async { raw(&6) });
//! let (near, far) = pair();
//! tokio::spawn(Arc::new(server).serve(far)); // any executor
//! let (conn, driving) = Connection::new(near, None);
//! tokio::spawn(driving);
//! assert_eq!(conn.call("kiln.get", ()).await.unwrap().get(), "6");
//! # }
//! ```

// A stamp of `trace`, or nothing at all without the feature.
#[cfg(feature = "trace")]
macro_rules! stamp {
    (enter $key:expr, $stage:ident) => { crate::trace::enter($key, crate::trace::Stage::$stage) };
    // An activation's job, stamped as it is taken and run.
    (within $job:ident) => { let $job = crate::trace::within($job); };
    ($key:expr, $stage:ident) => { crate::trace::stamp($key, crate::trace::Stage::$stage) };
}
#[cfg(not(feature = "trace"))]
macro_rules! stamp {
    ($($t:tt)*) => {};
}

#[cfg(feature = "tokio")]
mod application;
#[cfg(feature = "tokio")]
pub use application::{Application, MainInput, main};
#[cfg(not(target_arch = "wasm32"))]
mod registry;
#[cfg(not(target_arch = "wasm32"))]
pub use registry::{Activations, Registration, concurrent, namespace};
mod channel;
mod input;
pub use input::{Input, RawInput};
#[cfg(feature = "tokio")]
mod listener;
mod connection;
mod context;
mod grant;
pub use grant::Grant;
mod grants;
mod remote;
#[cfg(not(target_arch = "wasm32"))]
mod served;
mod server;
#[cfg(feature = "tokio")]
pub mod stdio;
#[cfg(feature = "store")]
pub mod store;
#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "trace")]
#[doc(hidden)]
pub mod trace;
#[cfg(feature = "tokio")]
pub mod ws;

pub use channel::{Channel, pair};
/// The `bytes` crate, whose `Bytes` a [`Channel`] carries.
pub use bytes;
pub use connection::{Connection, Error, Items, Passed, TOP_UP, WINDOW};
pub use context::{CallContext, Context, ContextKeys, FromCaller, require_caller};
pub use remote::Remote;
pub use plexus_macros::{activation, Context};
mod encoded;
pub use encoded::Encoded;

/// A stream of notifications that an authorized read may have changed.
/// Register interest before returning. Notifications may coalesce; dropping
/// this stream must release the subscription.
pub type Invalidations = std::pin::Pin<Box<dyn futures::Stream<Item = ()> + Send + 'static>>;

/// Supply the dependency notifications for generated watchable reads.
/// Authenticate and scope subscriptions using the original call and params.
pub trait WatchSource {
    fn subscribe(&self, dependencies: &[&str], call: &CallContext, params: &Raw)
        -> Result<Invalidations, Failure>;
}

#[doc(hidden)]
pub fn watch_digest(value: &Raw) -> u128 {
    xxhash_rust::xxh3::xxh3_128(value.get().as_bytes())
}
pub use plexus_proto::{Failure, Raw};
pub use server::{Server, fail, raw};
#[cfg(feature = "store")]
pub use store::Store;

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Raw octets.
pub type Bytes = Vec<u8>;

/// Any JSON value, kept as its text. In a body it is the value itself.
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub String);

impl serde::Serialize for Json {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serde_json::value::RawValue::from_string(self.0.clone()).map_err(serde::ser::Error::custom)?.serialize(s)
    }
}

impl<'de> serde::Deserialize<'de> for Json {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Json, D::Error> {
        Ok(Json(Box::<serde_json::value::RawValue>::deserialize(d)?.get().to_owned()))
    }
}

/// An activation `#[activation]` wrote the serving side of (native builds).
#[cfg(not(target_arch = "wasm32"))]
pub trait Serve: Send + Sized + 'static {
    /// Where it is served unless told otherwise: its own name.
    const NAME: &'static str;
    /// Register every method on `server` under `at`, with its IR and hash,
    /// and every child under `<at>.<child>`; say its hash at hello.
    fn serve(self, server: &mut Server, at: &str);
    /// The same without the hello, for an activation that lives inside
    /// another `P` on `P`'s thread: `get` finds it there. What `serve` and a
    /// parent's `held` call.
    #[doc(hidden)]
    fn held<P, G>(&self, local: &served::Local<P>, get: G, server: &mut Server, at: &str)
    where
        P: Send + 'static,
        G: for<'a> Fn(&'a P) -> &'a Self + Copy + Send + Sync + 'static;
}

/// Register an activation on an existing server under its declared namespace.
/// To choose a different mount path, use [`Serve::serve`].
#[cfg(not(target_arch = "wasm32"))]
pub fn serve<A: Serve>(activation: A, server: &mut Server) {
    activation.serve(server, A::NAME);
}

/// Serve one shared native activation on a bounded number of executor threads.
/// Jobs are assigned round-robin. Each method future or stream runs to
/// completion or cancellation on one worker. The activation and its children,
/// including their authorization state, are shared across all workers.
/// Separate calls may execute concurrently, including calls from one caller.
/// Choose this when the activation supports concurrent access to its state.
#[cfg(not(target_arch = "wasm32"))]
pub fn serve_concurrent<A: Serve + Sync>(activation: A, workers: std::num::NonZeroUsize, server: &mut Server) {
    serve_concurrent_at(activation, workers, server, A::NAME);
}

#[cfg(not(target_arch = "wasm32"))]
fn serve_concurrent_at<A: Serve + Sync>(activation: A, workers: std::num::NonZeroUsize, server: &mut Server, at: &str) {
    let activation = std::sync::Arc::new(activation);
    let local = served::Local::shared(&activation, workers);
    activation.held(&local, std::sync::Arc::as_ref, server, at);
    server.hello_described(at);
}

/// Serve an activation with the standard listeners and application-specific
/// grant policy installed before the first connection.
#[cfg(feature = "tokio")]
pub fn main_with<A: Serve>(activation: A, configure: impl FnOnce(&mut Server)) -> std::io::Result<()> {
    let listener = listener::Listener::from_args(std::env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let _entered = runtime.enter();
    let mut server = Server::new();
    serve(activation, &mut server);
    configure(&mut server);
    runtime.block_on(listener.serve(std::sync::Arc::new(server)))
}

/// What the code `#[activation]` writes names. Not for hand-written code.
#[doc(hidden)]
#[cfg(not(target_arch = "wasm32"))]
pub mod __private {
    pub use crate::input::decode_input;
    pub use crate::served::{B64, Later, Local, feed, failed, params, stream, fail_stream, stream_with_caller, stream_with_context};
    pub use futures;
    pub use plexus_proto::bytes;
    pub use serde;

    /// What serving an activation calls on each of its fields,
    /// `(&self.field).__plexus_claim(at)`, with both traits in scope: a
    /// `Store` is claimed by the place it is served at (this trait, taken
    /// first by method lookup); anything else is passed by
    /// ([`ClaimNone`], reached only through one more reference).
    pub trait ClaimStore {
        fn __plexus_claim(&self, at: &str);
    }
    #[cfg(feature = "store")]
    impl ClaimStore for crate::Store {
        fn __plexus_claim(&self, at: &str) {
            self.claim(at)
        }
    }
    pub trait ClaimNone {
        fn __plexus_claim(&self, _at: &str) {}
    }
    impl<T: ?Sized> ClaimNone for &T {}
}

/// Who is calling. A method that takes one is refused to a caller who has
/// not signed in; `Option<&Caller>` lets anyone call and says who, if known.
/// It is not a parameter a caller sends.
#[derive(Debug, Clone)]
pub struct Caller {
    principal: std::sync::Arc<str>,
    via: Option<std::sync::Arc<str>>,
}

impl Caller {
    pub fn new(principal: impl Into<String>) -> Caller {
        Caller { principal: std::sync::Arc::from(principal.into()), via: None }
    }
    /// The authenticated connection peer, including when it vouches for a user.
    pub fn peer(&self) -> &str {
        self.via.as_deref().unwrap_or(&self.principal)
    }

    /// The signed-in identity: `key:54a9…`, `user:ada@example.com`.
    pub fn principal(&self) -> &str {
        &self.principal
    }
}

/// Where a method reports how far along it is. Not a parameter, and never
/// part of the answer.
pub struct Progress<T>(Box<dyn FnMut(T)>);

impl<T> Progress<T> {
    pub fn to(sink: impl FnMut(T) + 'static) -> Progress<T> {
        Progress(Box::new(sink))
    }
    pub fn report(&mut self, value: T) {
        (self.0)(value)
    }
}

/// Step aside once, so other calls can run. Works under any executor.
pub fn yield_now() -> impl Future<Output = ()> {
    struct Once(bool);
    impl Future for Once {
        type Output = ();
        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
            if self.0 {
                return std::task::Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
    Once(false)
}

/// Any activation at all: what a parent holds when it does not know, when it
/// is compiled, what it will be given (`Box<dyn Activation>`), or knows only
/// the interface of (`Box<dyn Glazes>`, `trait Glazes: Activation`). Every
/// call under the place it is held at is passed to it by name, with JSON,
/// paced as its caller paces it. A [`Remote`] is one: an activation across
/// a channel, in another process or on another machine.
pub trait Activation: Send + Sync + 'static {
    /// Pass a call on: `method` is named below where it is held (`recipes`,
    /// `_ir`, `shelf.get`).
    fn pass(&self, method: &str, params: Option<Raw>) -> Passed;

    /// Pass a call with a key group on the mounted activation's segment.
    /// `keys` is the parsed wire group's original spelling, including the
    /// brackets. The child decides whether those keys open its context.
    fn pass_keyed(&self, _keys: &str, _method: &str, _params: Option<Raw>) -> Result<Passed, Failure> {
        Err(server::failure("invalid-context", "the mounted activation does not accept context keys at its place"))
    }

    /// Pass a call carrying an input stream. The mounted activation must
    /// forward input credit and cancellation to be able to accept it.
    fn pass_with_input(&self, _keys: Option<&str>, _method: &str, _params: Option<Raw>, _input: RawInput) -> Result<Passed, Failure> {
        Err(server::failure("invalid-params", "the mounted activation cannot forward input"))
    }

    /// Dispatch a mounted call with its opening caller and optional input.
    /// Existing mounts retain their service-identity behavior.
    fn pass_call(&self, keys: Option<&str>, method: &str, params: Option<Raw>, _caller: Option<&Caller>, input: Option<RawInput>) -> Result<Passed, Failure> {
        match (input, keys) {
            (Some(input), keys) => self.pass_with_input(keys, method, params, input),
            (None, Some(keys)) => self.pass_keyed(keys, method, params),
            (None, None) => Ok(self.pass(method, params)),
        }
    }

    /// Every place it serves, with its hash, named under `at`, where it is
    /// held: what `_tree` lists for it.
    fn tree(&self, at: &str) -> futures::future::BoxFuture<'static, Result<Vec<Place>, Failure>>;
}

/// A place something is served at, and its interface hash: one entry of
/// `_tree`. The hash is `None` when it is not known (a child that cannot
/// be reached now); a bare name, as plexus-rt's server answers, reads as a
/// place with none.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Place {
    pub place: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

impl<'de> serde::Deserialize<'de> for Place {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Place, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Either {
            Name(String),
            Place {
                place: String,
                #[serde(default)]
                hash: Option<String>,
            },
        }
        Ok(match Either::deserialize(d)? {
            Either::Name(place) => Place { place, hash: None },
            Either::Place { place, hash } => Place { place, hash },
        })
    }
}
