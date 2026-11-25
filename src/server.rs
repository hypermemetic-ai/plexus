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

use crate::channel::{Channel, Out, pump};
use crate::input::InputItem;
use crate::context::{keyed_path, valid_name};
use crate::{Activation, CallContext, Caller, ContextKeys, Place, lock};

type One = Arc<dyn Fn(Raw, CallContext) -> BoxFuture<'static, Result<Raw, Failure>> + Send + Sync>;
type Many = Arc<dyn Fn(Raw, CallContext) -> BoxStream<'static, Result<Raw, Failure>> + Send + Sync>;
type Validate = Arc<dyn Fn(CallContext) -> BoxFuture<'static, Result<(), Failure>> + Send + Sync>;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Pacing = Arc<dyn Fn(Raw, CallContext) -> Box<dyn Paced> + Send + Sync>;

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
    Reflect(One),
    Many(Many),
    /// Paced by the credit granted to it: an activation's stream.
    #[cfg(not(target_arch = "wasm32"))]
    Paced(Pacing),
    /// A call under a child held across a channel: the child, and the
    /// method's name below it, with the key group at its place if present.
    Pass(Arc<dyn Activation>, String, Option<String>),
}

/// Which segment of the canonical route accepts keys and what it expects.
#[derive(Clone, Copy)]
struct Gate {
    segment: usize,
    keys: &'static [&'static str],
}

#[derive(Clone)]
struct Method {
    handler: Handler,
    gate: Option<Gate>,
    input: bool,
    grant: Option<crate::grants::MethodGrant>,
}

#[derive(Clone)]
struct DescendantGate {
    keys: &'static [&'static str],
    validate: Validate,
    outer: bool,
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
    methods: HashMap<String, Method>,
    descendant_gates: HashMap<String, Vec<DescendantGate>>,
    grants: crate::grants::Grants,
    /// Every place described or mounted, in order, for `_tree`.
    places: Vec<Entry>,
    interface: Option<String>,
    #[cfg(not(target_arch = "wasm32"))]
    registry: Option<crate::Activations>,
}

impl Server {
    pub fn new() -> Server {
        Server::default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn live(registry: crate::Activations) -> Self {
        let mut server = Self::new();
        server.registry = Some(registry);
        // The catalog identity is stable; individual roots expose their current hashes.
        server.hello("plexus-live");
        server
    }

    // A handwritten Serve must not be able to publish another root's routes,
    // or quietly install a policy that would be lost in the combined server.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn finish_registration(&self, name: &str) -> Result<(), String> {
        let owned = |path: &str| path.split('.').next() == Some(name);
        if self.grants.is_configured() {
            return Err("configure shared grant policy on the registry server".to_owned());
        }
        if self.methods.keys().any(|path| !owned(path)) ||
            self.descendant_gates.keys().any(|path| !owned(path)) ||
            self.places.iter().any(|entry| !owned(match entry {
                Entry::Described(place) => &place.place,
                Entry::Mounted(place, _) => place,
            })) {
            return Err(format!("activation registered a route outside {name}"));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn extend_routes(&mut self, source: &Server) {
        self.methods.extend(source.methods.clone());
        self.places.extend(source.places.iter().cloned());
        for (path, guards) in &source.descendant_gates {
            self.descendant_gates.entry(path.clone()).or_default().extend(guards.iter().cloned());
        }
    }

    fn open_routes(self: &Arc<Self>) -> io::Result<Arc<Self>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(registry) = &self.registry {
            let dynamic = registry.routes()?;
            if self.methods.is_empty() && self.places.is_empty() && self.descendant_gates.is_empty() {
                return Ok(dynamic);
            }
            // Static configuration owns its existing methods, and its admission guards
            // also run for registered routes under those paths.
            let mut routes = Self::new();
            routes.extend_routes(self);
            for guards in routes.descendant_gates.values_mut() {
                for guard in guards { guard.outer = true; }
            }
            routes.extend_routes(&dynamic);
            routes.methods.extend(self.methods.clone());
            return Ok(Arc::new(routes));
        }
        Ok(self.clone())
    }

    /// Look up current role membership when a declared method grant is checked.
    /// Receives the effective caller, the IR grant name and eligible role names.
    /// Without a policy, every method declaring a grant is denied.
    pub fn authorize_grants(&mut self, check: impl Fn(&Caller, &str, &[&str]) -> bool + Send + Sync + 'static) {
        self.grants.authorize(check);
    }

    /// Permit this authenticated service to vouch for a user on one exact
    /// local, grant-protected method. The user's grant is checked on every open.
    pub fn trust_forwarder(&mut self, peer: &str, address: &str) {
        self.grants.trust(peer, address);
    }

    #[doc(hidden)]
    pub fn require_grant(&mut self, address: String, grant: &'static str, roles: &'static [&'static str]) {
        self.methods.get_mut(&address).expect("register the method before its grant").grant = Some((grant, roles));
    }

    /// The interface hash said at hello, the first frame of every connection.
    pub fn hello(&mut self, interface: &str) {
        self.interface = Some(interface.to_owned());
    }

    /// A concurrent registration uses the same root hash as its generated
    /// serving function. The held activation has already registered its IR.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn hello_described(&mut self, at: &str) {
        if let Some(hash) = self.places.iter().rev().find_map(|entry| match entry {
            Entry::Described(place) if place.place == at => place.hash.as_ref(),
            _ => None,
        }) {
            self.interface = Some(hash.clone());
        }
    }

    /// Declare that a registered method requires a caller input stream.
    #[doc(hidden)]
    pub fn accepts_input(&mut self, name: &str) {
        self.methods.get_mut(name).expect("register the method before marking its input").input = true;
    }

    fn register(&mut self, name: &str, handler: Handler, gate: Option<Gate>) {
        self.methods.insert(name.to_owned(), Method { handler, gate, input: false, grant: None });
    }

    /// A method with one answer. Its parameters arrive as JSON (`{}` when
    /// the caller sent none).
    pub fn method<F, A>(&mut self, name: &str, run: F)
    where
        F: Fn(Raw) -> A + Send + Sync + 'static,
        A: Future<Output = Result<Raw, Failure>> + Send + 'static,
    {
        self.method_with_caller(name, move |params, _| run(params));
    }

    /// A local unary method with the connection's caller available.
    #[doc(hidden)]
    pub fn method_with_caller<F, A>(&mut self, name: &str, run: F)
    where
        F: Fn(Raw, CallContext) -> A + Send + Sync + 'static,
        A: Future<Output = Result<Raw, Failure>> + Send + 'static,
    {
        self.register(name, Handler::One(Arc::new(move |p, call| run(p, call).boxed())), None);
    }

    /// A local unary method that takes keys on the activation's segment.
    #[doc(hidden)]
    pub fn method_with_context<F, A>(&mut self, name: &str, at: &str, keys: &'static [&'static str], run: F)
    where
        F: Fn(Raw, CallContext) -> A + Send + Sync + 'static,
        A: Future<Output = Result<Raw, Failure>> + Send + 'static,
    {
        self.gated(name, at, keys, Handler::One(Arc::new(move |p, call| run(p, call).boxed())));
    }

    /// A method with many answers. An `Err` ends the call with that failure.
    pub fn stream<F, S>(&mut self, name: &str, run: F)
    where
        F: Fn(Raw) -> S + Send + Sync + 'static,
        S: futures::Stream<Item = Result<Raw, Failure>> + Send + 'static,
    {
        self.register(name, Handler::Many(Arc::new(move |p, _| run(p).boxed())), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn paced(&mut self, name: &str, run: Pacing) {
        self.register(name, Handler::Paced(run), None);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn paced_with_context(&mut self, name: &str, at: &str, keys: &'static [&'static str], run: Pacing) {
        self.gated(name, at, keys, Handler::Paced(run));
    }

    fn gated(&mut self, name: &str, at: &str, keys: &'static [&'static str], handler: Handler) {
        assert!(name.strip_prefix(at).is_some_and(|rest| rest.starts_with('.')));
        self.register(name, handler, Some(Gate { segment: segment(at), keys }));
    }

    /// Require the activation to admit calls under its place before dispatch.
    /// Its own contextual methods open it as part of their invocation.
    #[doc(hidden)]
    pub fn guard_descendants<F, A>(&mut self, at: &str, keys: &'static [&'static str], run: F)
    where
        F: Fn(CallContext) -> A + Send + Sync + 'static,
        A: Future<Output = Result<(), Failure>> + Send + 'static,
    {
        let previous = self.descendant_gates.insert(at.to_owned(), vec![DescendantGate {
            keys,
            validate: Arc::new(move |call| run(call).boxed()),
            outer: false,
        }]);
        assert!(previous.is_none(), "a descendant guard is already registered at {at}");
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
        for (suffix, value) in [("_ir", ir), ("_hash", hash)] {
            let reflect = Handler::Reflect(Arc::new(move |_, _| future::ready(Ok(value.clone())).boxed()));
            self.register(&format!("{at}.{suffix}"), reflect, None);
        }
        Ok(())
    }

    /// Hold `child` at `at`: every call under `<at>.`, its `_ir` and `_hash`
    /// too, is passed to it, unless a method of that name is registered
    /// here; `_tree` lists its places, asked of it each time. Its credit and
    /// cancel are passed on as the caller sends them.
    pub fn mount(&mut self, at: &str, child: Box<dyn Activation>) {
        self.places.push(Entry::Mounted(at.to_owned(), Arc::from(child)));
    }

    async fn find(&self, host: &Server, address: &str, caller: &Option<Caller>, input: bool) -> Result<(Handler, ContextKeys), Failure> {
        let not_found = || failure("not-found", &format!("no method {address}"));
        if address == "_tree" {
            if input { return Err(failure("invalid-params", "_tree takes no input stream")); }
            let places = self.places.clone();
            return Ok((Handler::One(Arc::new(move |_, _| tree(places.clone()).boxed())), ContextKeys::default()));
        }
        let mut keyed = address.contains(['[', ']']).then(|| keyed_path(address)).transpose()?;
        let mut groups = keyed.as_mut().map(|path| std::mem::take(&mut path.groups)).unwrap_or_default();
        let name = keyed.as_ref().map_or(address, |path| path.name.as_str());
        let (mut handler, leaf, mounted, grant) = if let Some(method) = self.methods.get(name) {
            if input != method.input {
                let why = if input { "this method takes no input stream" } else { "this method requires an input stream" };
                return Err(failure("invalid-params", why));
            }
            (method.handler.clone(), method.gate, None, method.grant)
        } else {
            let (at, rest, child) = self.mounted(name).ok_or_else(not_found)?;
            (Handler::Pass(child.clone(), rest.to_owned(), None), None, Some(at), None)
        };
        let reflection = matches!(handler, Handler::Reflect(_))
            || mounted.is_some() && name.rsplit_once('.').is_some_and(|(_, suffix)| {
                matches!(suffix, "_ir" | "_hash") && name.split('.').all(valid_name)
            });
        if reflection && keyed.is_none() {
            return Ok((handler, ContextKeys::default()));
        }
        let mut checks = Vec::new();
        if !self.descendant_gates.is_empty() {
            for (segment, (end, _)) in name.match_indices('.').enumerate() {
                if let Some(guards) = self.descendant_gates.get(&name[..end]) {
                    let own = leaf.is_some_and(|leaf| leaf.segment == segment);
                    for guard in guards {
                        // Contextual methods open their own activation; outer guards still apply.
                        if own && (!guard.outer || host.methods.contains_key(name)) { continue; }
                        let keys = groups.get(&segment).cloned().unwrap_or_default();
                        keys.check(guard.keys)?;
                        checks.push((&guard.validate, keys));
                    }
                    if !own { groups.remove(&segment); }
                }
            }
        }
        let keys = if let Some(leaf) = leaf {
            let keys = groups.remove(&leaf.segment).unwrap_or_default();
            keys.check(leaf.keys)?;
            keys
        } else {
            ContextKeys::default()
        };
        if !groups.is_empty() {
            let mount = mounted.map(segment);
            if !mount.is_some_and(|mount| groups.keys().all(|&index| index >= mount)) {
                return Err(failure("invalid-context", "context keys belong on the activation's segment"));
            }
            if let (Handler::Pass(_, method, keys), Some(path), Some(mount)) = (&mut handler, keyed.as_ref(), mount) {
                *method = path.segments[mount + 1..].join(".");
                if groups.contains_key(&mount) {
                    let mounted = path.segments[mount];
                    let start = mounted.find('[').expect("a parsed key group starts with '['");
                    *keys = Some(mounted[start..].to_owned());
                }
            }
        }
        for (validate, keys) in checks {
            caught(async { validate(CallContext::new(caller.clone(), keys)).await }).await??;
        }
        if let Some(method) = grant {
            host.grants.check(method, caller.as_ref())?;
        }
        Ok((handler, keys))
    }

    fn mounted<'a, 'b>(&'a self, name: &'b str) -> Option<(&'a str, &'b str, &'a Arc<dyn Activation>)> {
        // The child mounted deepest that the name is under.
        self.places
            .iter()
            .filter_map(|e| match e {
                Entry::Mounted(at, child) => Some((at.as_str(), name.strip_prefix(at.as_str())?.strip_prefix('.')?, child)),
                Entry::Described(_) => None,
            })
            .max_by_key(|(at, ..)| at.len())
    }

    /// Serve one channel until it closes. Every call runs at once with the
    /// others, inside this future: spawn it on any executor.
    pub fn serve(self: &Arc<Self>, channel: Channel) -> impl Future<Output = io::Result<()>> + Send + 'static {
        self.serve_bound(channel, None)
    }

    /// Serve a channel whose caller has already been authenticated.
    /// The caller is trusted for the lifetime of the channel.
    pub fn serve_as(self: &Arc<Self>, channel: Channel, caller: Caller) -> impl Future<Output = io::Result<()>> + Send + 'static {
        self.serve_bound(channel, Some(caller))
    }

    fn serve_bound(self: &Arc<Self>, channel: Channel, caller: Option<Caller>) -> impl Future<Output = io::Result<()>> + Send + 'static {
        let server = self.clone();
        let Channel { send, mut receive, binary_input } = channel;
        // Keep an output-only leaf's hello byte-for-byte stable. A mount
        // learns its child's method capability when that call grants credit.
        #[cfg(not(target_arch = "wasm32"))]
        let channel_supports_binary = binary_input;
        let binary_input = binary_input && self.interface.is_some() && (
            self.methods.values().any(|method| method.input)
            || self.places.iter().any(|entry| matches!(entry, Entry::Mounted(..)))
        );
        #[cfg(not(target_arch = "wasm32"))]
        let binary_input = binary_input || (channel_supports_binary && self.registry.is_some());
        let (out, frames) = crate::channel::out();
        if let Some(interface) = &self.interface {
            let mut hello = Frame::hello(interface, false);
            if let Body::Hello(hello) = &mut hello.body {
                hello.binary_input = binary_input;
            }
            out.send(|buf| hello.encode_into(Numbering::Counted, buf));
        }
        let (start, started) = mpsc::unbounded::<BoxFuture<'static, ()>>();
        let calls: Arc<Mutex<HashMap<u32, Running>>> = Arc::default();
        let read = async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(registry) = &server.registry { registry.bind_runtime()?; }
            while let Some(bytes) = receive.next().await {
                #[cfg(feature = "trace")]
                let read = crate::trace::now();
                let bytes = bytes?;
                if bytes.first() == Some(&plexus_proto::binary_input::TAG) {
                    if !binary_input {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "binary input was not advertised"));
                    }
                    let (id, seq, payload) = plexus_proto::binary_input::read(bytes)?;
                    receive_input(&calls, &out, id, |input| input.item(InputItem::Bytes(payload), Some(seq)));
                    continue;
                }
                let frame = Frame::decode(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let id = frame.call.0;
                match frame.body {
                    Body::Open(open) => {
                        #[cfg(feature = "trace")]
                        crate::trace::stamp_at((id, 0), crate::trace::Stage::Read, read);
                        let credit = Arc::new(Credit::new(1));
                        let (ingress, input) = if open.input {
                            let (ingress, input) = crate::input::incoming(out.clone(), frame.call);
                            (Some(ingress), Some(input))
                        } else { (None, None) };
                        let (abort, registration) = AbortHandle::new_pair();
                        if lock(&calls).insert(id, Running { credit: credit.clone(), abort, input: ingress }).is_some() {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "duplicate open call id"));
                        }
                        let invocation = Invocation { id, open, caller: caller.clone(), input };
                        let ran = run(server.clone(), server.open_routes()?, invocation, out.clone(), credit, calls.clone());
                        let _ = start.unbounded_send(Abortable::new(ran, registration).map(|_| ()).boxed());
                        stamp!((id, 0), Started);
                    }
                    Body::Credit(n) => {
                        if let Some(call) = lock(&calls).get(&id) {
                            call.credit.grant(n);
                        }
                    }
                    Body::Item(raw) => receive_input(&calls, &out, frame.call, |input| input.item(InputItem::Json(raw), None)),
                    Body::End(end) => receive_input(&calls, &out, frame.call, |input| input.end(end)),
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

fn segment(at: &str) -> usize {
    at.bytes().filter(|&b| b == b'.').count()
}

/// How many items a stream writes before it steps aside.
const RUN: u32 = 256;

fn receive_input(
    calls: &Mutex<HashMap<u32, Running>>, out: &Out, id: CallId,
    receive: impl FnOnce(&mut crate::input::Ingress),
) {
    let mut calls = lock(calls);
    if let Some(running) = calls.get_mut(&id.0) {
        if let Some(input) = &mut running.input {
            receive(input);
        } else {
            running.abort.abort();
            calls.remove(&id.0);
            out.send(|buf| Frame::failed(id, "invalid-params", "call was opened without input")
                .encode_into(Numbering::Counted, buf));
        }
    }
}

struct Running {
    input: Option<crate::input::Ingress>,
    credit: Arc<Credit>,
    abort: AbortHandle,
}

/// How many items a stream may still write, and who waits to write one.
/// Atomic, so an item takes its credit with no lock.
pub(crate) struct Credit {
    left: AtomicU32,
    wake: AtomicWaker,
}

impl Credit {
    pub(crate) fn new(left: u32) -> Self {
        Self { left: AtomicU32::new(left), wake: AtomicWaker::new() }
    }

    pub(crate) fn grant(&self, n: u32) {
        let _ = self.left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| Some(left.saturating_add(n)));
        self.wake.wake();
    }

    /// Up to `most` of the credit there is, or `Pending` with the waker kept
    /// if there is none.
    pub(crate) fn poll_take(&self, cx: &mut Context<'_>, most: u32) -> Poll<u32> {
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

struct Invocation {
    id: u32,
    open: Open,
    caller: Option<Caller>,
    input: Option<crate::RawInput>,
}

async fn run(
    server: Arc<Server>,
    routes: Arc<Server>,
    invocation: Invocation,
    out: Out,
    credit: Arc<Credit>,
    calls: Arc<Mutex<HashMap<u32, Running>>>,
) {
    let Invocation { id, open, caller, input } = invocation;
    stamp!(enter (id, 0), Running);
    let call = CallId(id);
    let input_state = input.as_ref().map(|input| input.state());
    let send = |kind: Kind, body: &[u8]| {
        if kind == Kind::Item && input_state.as_ref().is_some_and(|s| s.error().is_some()) { return; }
        out.send(|buf| plexus_proto::write(buf, call, kind, 0, body, Numbering::Counted));
    };
    // Each frame of the call stamped as the server has it and queues it.
    #[cfg(feature = "trace")]
    let sent = crate::trace::Sent::new();
    #[cfg(feature = "trace")]
    let send = |kind: Kind, body: &[u8]| sent.send(&send, kind, body);
    let dispatch = async {
        let (open, caller) = server.grants.resolve(open, caller, |name, trusted, metadata| {
            match routes.methods.get(name) {
                Some(method) if trusted && method.grant.is_some() => Ok(()),
                Some(method) if metadata && matches!(method.handler, Handler::Reflect(_)) => Ok(()),
                None if metadata => Err(failure("not-found", &format!("no method {name}"))),
                _ => Err(failure("forbidden", "this peer may not forward the caller on this method")),
            }
        })?;
        let given = open.params;
        let params = given.clone().unwrap_or_else(|| raw(&serde_json::Map::new()).expect("{} is JSON"));
        let (handler, keys) = routes.find(&server, &open.method, &caller, open.input).await?;
        let mut context = CallContext::new(caller, keys);
        context.input = input;
        match handler {
            Handler::One(f) | Handler::Reflect(f) => match caught(f(params, context)).await.and_then(|answer| answer) {
                Ok(answer) => {
                    send(Kind::Item, answer.get().as_bytes());
                    Ok(())
                }
                Err(f) => Err(f),
            },
            Handler::Many(f) => {
                let mut items = f(params, context);
                let mut run = 0;
                #[cfg(feature = "trace")]
                let mut n = 0;
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
                    stamp!(enter { n += 1; (id, n) }, Asked);
                    match caught(items.next()).await {
                        Ok(Some(Ok(item))) => send(Kind::Item, item.get().as_bytes()),
                        Ok(Some(Err(f))) | Err(f) => break Err(f),
                        Ok(None) => break Ok(()),
                    }
                }
            }
            Handler::Pass(child, rest, mount_keys) => {
                // The open carried credit for one item; the open passed on
                // carries it on.
                take(&credit).await;
                let input = context.input.take();
                let passed = child.pass_call(mount_keys.as_deref(), &rest, given, context.caller(), input);
                match passed {
                    Ok(items) => paced(&credit, items, send).await,
                    Err(f) => Err(f),
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Handler::Paced(f) => paced(&credit, f(params, context), send).await,
        }
    };
    let ended = if let Some(state) = &input_state {
        futures::pin_mut!(dispatch);
        let failed = state.failure();
        futures::pin_mut!(failed);
        match future::select(failed, dispatch).await {
            Either::Left((f, _)) => Err(f),
            Either::Right((result, _)) => state.error().map_or(result, Err),
        }
    } else {
        dispatch.await
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
