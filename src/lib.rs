//! The one crate a Plexus activation depends on.
//!
//! It holds `#[plexus::activation]`, the types a method's signature names,
//! and the runtime: a [`Connection`] that calls and a [`Server`] that
//! answers, over any [`Channel`] — a [`pair`] in memory, and with the
//! `tokio` feature the stdio of a process ([`stdio`]) and a WebSocket
//! ([`ws`]). Frames are plexus-proto's.
//!
//! Methods are served untyped here: a name, a JSON body in, items out.
//! The signature types are shapes only: nothing fills a [`Caller`], feeds an
//! [`Input`] or listens to a [`Progress`] yet. `#[activation(describe)]` reads
//! them out of a signature into the IR.
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
mod server;
#[cfg(feature = "tokio")]
pub mod stdio;
#[cfg(feature = "tokio")]
pub mod ws;

pub use channel::{Channel, pair};
pub use connection::{Connection, Error, Items, TOP_UP, WINDOW};
pub use plexus_macros::activation;
pub use plexus_proto::{Failure, Raw};
pub use server::{Server, fail, raw};

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Raw octets.
pub type Bytes = Vec<u8>;

/// Any JSON value, kept as its text.
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub String);

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
