//! The serving end: reads `open`, runs the method, writes what it answers.

use std::collections::HashMap;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use futures::channel::mpsc;
use futures::future::{AbortHandle, Abortable, BoxFuture, FutureExt};
use futures::stream::{BoxStream, FuturesUnordered, StreamExt};
use plexus_proto::{Body, CallId, Failure, Frame, Kind, Numbering, Open, Raw};
use serde::Serialize;

use crate::channel::{Channel, pump};
use crate::lock;

type One = Arc<dyn Fn(Raw) -> BoxFuture<'static, Result<Raw, Failure>> + Send + Sync>;
type Many = Arc<dyn Fn(Raw) -> BoxStream<'static, Result<Raw, Failure>> + Send + Sync>;

#[derive(Clone)]
enum Handler {
    One(One),
    Many(Many),
}

/// Methods by name, served on as many channels as are handed to
/// [`serve`](Server::serve).
#[derive(Default)]
pub struct Server {
    methods: HashMap<String, Handler>,
    /// Every place `describe` named, for `_tree`.
    mounts: Vec<String>,
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

    /// Say what is served at `at`: `<at>._ir` answers `ir` and `<at>._hash`
    /// its `hash`, and `_tree` lists every place described.
    pub fn describe(&mut self, at: &str, ir: &str) -> Result<(), serde_json::Error> {
        #[derive(serde::Deserialize)]
        struct Hashed {
            hash: String,
        }
        let hash = raw(&serde_json::from_str::<Hashed>(ir)?.hash).expect("a string is JSON");
        let ir = serde_json::value::RawValue::from_string(ir.to_owned())?;
        self.mounts.push(at.to_owned());
        self.method(&format!("{at}._ir"), move |_| std::future::ready(Ok(ir.clone())));
        self.method(&format!("{at}._hash"), move |_| std::future::ready(Ok(hash.clone())));
        Ok(())
    }

    fn find(&self, name: &str) -> Option<Handler> {
        if name == "_tree" {
            let tree = raw(&self.mounts).expect("names are JSON");
            return Some(Handler::One(Arc::new(move |_| std::future::ready(Ok(tree.clone())).boxed())));
        }
        self.methods.get(name).cloned()
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
                        let credit = Arc::new(Mutex::new(Credit { left: 1, wake: None }));
                        let (abort, registration) = AbortHandle::new_pair();
                        lock(&calls).insert(id, Running { credit: credit.clone(), abort });
                        let ran = run(server.clone(), id, open, out.clone(), credit, calls.clone());
                        let _ = start.unbounded_send(Abortable::new(ran, registration).map(|_| ()).boxed());
                    }
                    Body::Credit(n) => {
                        if let Some(call) = lock(&calls).get(&id) {
                            let mut credit = lock(&call.credit);
                            credit.left = credit.left.saturating_add(n);
                            if let Some(wake) = credit.wake.take() {
                                wake.wake();
                            }
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

struct Running {
    credit: Arc<Mutex<Credit>>,
    abort: AbortHandle,
}

/// How many items a stream may still write, and who waits to write one.
struct Credit {
    left: u32,
    wake: Option<Waker>,
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
    credit: Arc<Mutex<Credit>>,
    calls: Arc<Mutex<HashMap<u32, Running>>>,
) {
    let call = CallId(id);
    let send = |kind: Kind, body: &[u8]| {
        let mut bytes = Vec::with_capacity(body.len() + 16);
        plexus_proto::write(&mut bytes, call, kind, 0, body, Numbering::Counted);
        let _ = out.unbounded_send(bytes);
    };
    let params = open.params.unwrap_or_else(|| raw(&serde_json::Map::new()).expect("{} is JSON"));
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
            loop {
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
    };
    lock(&calls).remove(&id);
    match ended {
        Ok(()) => send(Kind::End, b""),
        Err(f) => send(Kind::End, serde_json::to_string(&f).expect("a Failure is JSON").as_bytes()),
    }
}

/// Take one item's credit, waiting for it if there is none.
fn take(credit: &Mutex<Credit>) -> impl Future<Output = ()> + '_ {
    futures::future::poll_fn(move |cx| {
        let mut credit = lock(credit);
        if credit.left > 0 {
            credit.left -= 1;
            return Poll::Ready(());
        }
        credit.wake = Some(cx.waker().clone());
        Poll::Pending
    })
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
    serde_json::value::to_raw_value(value).map_err(|e| failure("internal", &e.to_string()))
}

/// A method's failure: a code a program matches on, a sentence for a
/// person, and data.
pub fn fail(code: &str, message: &str, data: impl Serialize) -> Failure {
    Failure { data: serde_json::value::to_raw_value(&data).ok(), ..failure(code, message) }
}
