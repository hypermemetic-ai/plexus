//! The one crate a Plexus activation depends on.
//!
//! It holds `#[plexus::activation]`, the types a method's signature names,
//! and the runtime: a [`Connection`] that calls and a [`Server`] that
//! answers, over any [`Channel`] — a [`pair`] in memory, and with the
//! `tokio` feature the stdio of a process ([`stdio`]) and a WebSocket
//! ([`ws`]). Frames are plexus-proto's.
//!
//! A [`Server`] serves methods untyped: a name, a JSON body in, items out.
//! `#[activation]`, in a native build, writes the typed side onto it: each
//! module gets `serve(activation, server, at)`, and its activation type
//! [`Serve`], so that one line of `main` is a process ([`main`]).
//!
//! With the `store` feature an activation keeps its state in its own SQLite
//! database, a `Store`.
//!
//! Some signature types are shapes only: nothing fills a [`Caller`], feeds
//! an [`Input`] or listens to a [`Progress`] yet. `#[activation(describe)]`
//! reads them out of a signature into the IR.
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

mod channel;
mod connection;
#[cfg(not(target_arch = "wasm32"))]
mod served;
mod server;
#[cfg(feature = "tokio")]
pub mod stdio;
#[cfg(feature = "store")]
mod store;
#[cfg(feature = "tokio")]
pub mod ws;

pub use channel::{Channel, pair};
pub use connection::{Connection, Error, Items, TOP_UP, WINDOW};
pub use plexus_macros::activation;
pub use plexus_proto::{Failure, Raw};
pub use server::{Server, fail, raw};
#[cfg(feature = "store")]
pub use {rusqlite, store::Store};

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
    /// Register every method on `server` under `at`, with its IR and hash.
    fn serve(self, server: &mut Server, at: &str);
}

/// Serve `activation` as this process: over stdin and stdout until stdin
/// closes, or with `--ws ADDR` on a WebSocket, every connection sharing the
/// one activation (the address bound is printed on stdout, one line:
/// `ws://127.0.0.1:PORT/`). With `--data DIR` (feature `store`) every
/// `Store` keeps its file in DIR.
///
/// ```no_run
/// # mod kiln { #[derive(Default)] pub struct Kiln; impl plexus::Serve for Kiln { const NAME: &'static str = "kiln"; fn serve(self, _: &mut plexus::Server, _: &str) {} } }
/// fn main() -> std::io::Result<()> {
///     plexus::main(kiln::Kiln::default())
/// }
/// ```
#[cfg(feature = "tokio")]
pub fn main<A: Serve>(activation: A) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    const USAGE: &str = "usage: [--ws ADDR] [--data DIR]   (no --ws: stdin and stdout)";
    let mut ws = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| Error::new(ErrorKind::InvalidInput, USAGE))?;
        match flag.as_str() {
            "--ws" => ws = Some(value.parse::<std::net::SocketAddr>().map_err(|e| Error::new(ErrorKind::InvalidInput, e))?),
            #[cfg(feature = "store")]
            "--data" => store::set_data(value.into()),
            _ => return Err(Error::new(ErrorKind::InvalidInput, USAGE)),
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let _entered = runtime.enter();
    let mut server = Server::new();
    activation.serve(&mut server, A::NAME);
    let server = std::sync::Arc::new(server);
    runtime.block_on(async move {
        match ws {
            None => server.serve(stdio::stdio()).await,
            Some(addr) => {
                let bound = ws::spawn(addr, server).await?;
                println!("ws://{bound}/");
                std::future::pending().await
            }
        }
    })
}

/// What the code `#[activation]` writes names. Not for hand-written code.
#[doc(hidden)]
#[cfg(not(target_arch = "wasm32"))]
pub mod __private {
    pub use crate::served::{B64, Local, feed, failed, params};
    pub use futures;
    pub use plexus_proto::bytes;
    pub use serde;
}

/// Who is calling. A method that takes one is refused to a caller who has
/// not signed in; `Option<&Caller>` lets anyone call and says who, if known.
/// It is not a parameter a caller sends.
#[derive(Debug, Clone)]
pub struct Caller {
    principal: String,
}

impl Caller {
    pub fn new(principal: impl Into<String>) -> Caller {
        Caller { principal: principal.into() }
    }
    /// The signed-in identity: `key:54a9…`, `user:ada@example.com`.
    pub fn principal(&self) -> &str {
        &self.principal
    }
}

/// Items the caller streams in while the call runs. Not a parameter.
pub struct Input<T>(Box<dyn Iterator<Item = T>>);

impl<T> Input<T> {
    pub fn from_iter(items: impl IntoIterator<Item = T> + 'static) -> Input<T> {
        Input(Box::new(items.into_iter()))
    }
}

impl<T> Iterator for Input<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.0.next()
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
/// is compiled, what it will be given. Called by name, with JSON.
pub trait Activation {
    /// What it is: its IR, as JSON.
    fn ir(&self) -> Json;
    /// A method by name.
    fn call(&self, method: &str, params: Json) -> Result<Json, Json>;
}
