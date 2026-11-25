//! Compose a native activation and configure its authenticated peer dependencies.
use crate::{
    Caller, Channel, Connection, Failure, Peer, PeerInterface, Serve, Server,
    bindings::{Bindings, Slot},
    lock,
    peer::{self, Side},
    tls::{Dial, Identity, Listen},
};
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{sleep, timeout},
};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// An operator-controlled directory. A keystore://name identity reads name.key;
/// a keystore://name peer pin reads name.pub, containing the public key: principal.
pub struct PeerKeys(PathBuf);
impl PeerKeys {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self(directory.into())
    }
    fn path(&self, uri: &str, ext: &str) -> io::Result<PathBuf> {
        let name = uri
            .strip_prefix("keystore://")
            .ok_or_else(|| invalid("expected keystore://name"))?;
        if !crate::context::valid_name(name) {
            return Err(invalid("invalid keystore name"));
        }
        Ok(self.0.join(format!("{name}.{ext}")))
    }
    pub fn identity(&self, reference: &str) -> io::Result<Identity> {
        if reference.starts_with("keystore://") {
            Identity::from_key_file(self.path(reference, "key")?)
        } else {
            Identity::from_key_file(Path::new(reference))
        }
    }
    pub fn pin(&self, reference: &str) -> io::Result<String> {
        let pin = if reference.starts_with("keystore://") {
            std::fs::read_to_string(self.path(reference, "pub")?)?
                .trim()
                .to_owned()
        } else {
            reference.to_owned()
        };
        crate::tls::validate_pin(&pin)?;
        Ok(pin)
    }
}

type Live = Arc<Mutex<Option<Connection>>>;
#[derive(Clone)]
struct Target {
    hash: &'static str,
    live: Live,
}
enum Endpoint {
    Accept(String, String, Target),
    Dial(String, Dial, Target),
}

/// A host serves one activation. Peers use its local identity for every outbound call.
pub struct PeerHost {
    server: Server,
    identity: Identity,
    bindings: Bindings,
    endpoints: Vec<Endpoint>,
}
impl PeerHost {
    pub fn new<A: Serve>(activation: A, identity: Identity) -> Self {
        let mut server = Server::new();
        crate::serve(activation, &mut server);
        Self {
            server,
            identity,
            bindings: Bindings {
                root: A::NAME.to_owned(),
                ..Default::default()
            },
            endpoints: Vec::new(),
        }
    }
    /// Configure method grants, explicit child mounts or additional local methods before start.
    pub fn server_mut(&mut self) -> &mut Server {
        &mut self.server
    }
    pub fn peer<'a, T: PeerInterface>(&'a mut self, name: &'a str) -> PeerBuilder<'a, T> {
        PeerBuilder {
            host: self,
            name,
            marker: std::marker::PhantomData,
        }
    }
    /// Publish an existing binding as a child, forwarding the inbound caller.
    /// The child name is relative to the activation's canonical namespace.
    pub fn mount_peer(&mut self, child: &str, slot: &str) -> io::Result<()> {
        if !crate::context::valid_name(child) {
            return Err(invalid("invalid peer child name"));
        }
        let binding = self
            .bindings
            .slots
            .get(slot)
            .ok_or_else(|| invalid("unknown peer to mount"))?;
        let place = format!("{}.{child}", self.bindings.root);
        self.server.mount_peer_at(
            &place,
            Box::new(Proxy {
                namespace: binding.namespace,
                live: binding.live.clone(),
            }),
        )
    }
    pub fn requires_peer(&mut self, name: &str) -> io::Result<()> {
        if !self.bindings.slots.contains_key(name) {
            return Err(invalid("unknown required peer"));
        }
        if !self.bindings.required.iter().any(|n| n == name) {
            self.bindings.required.push(name.into());
        }
        Ok(())
    }
    /// Bind listeners and start dialing before applying activation readiness.
    pub async fn start(mut self) -> io::Result<RunningPeers> {
        let bindings = Arc::new(self.bindings);
        self.server.peers = Some(bindings.clone());
        let server = Arc::new(self.server);
        let (changed, _) = watch::channel(());
        let mut tasks = JoinSet::new();
        let mut groups = BTreeMap::<String, BTreeMap<String, Target>>::new();
        let mut dials = Vec::new();
        for endpoint in self.endpoints {
            match endpoint {
                Endpoint::Accept(addr, pin, target) => {
                    if groups
                        .entry(addr)
                        .or_default()
                        .insert(pin, target)
                        .is_some()
                    {
                        return Err(invalid("one listener cannot bind the same peer key twice"));
                    }
                }
                Endpoint::Dial(url, dial, target) => dials.push((url, dial, target)),
            }
        }
        let mut addresses = BTreeMap::new();
        let mut listeners = Vec::new();
        for (authority, targets) in groups {
            let listen = Arc::new(Listen::new(
                &self.identity,
                targets.keys().map(String::as_str),
            )?);
            let targets = Arc::new(targets);
            let tcp = TcpListener::bind(&authority).await?;
            addresses.insert(authority, tcp.local_addr()?);
            listeners.push((tcp, listen, targets));
        }
        for (tcp, listen, targets) in listeners {
            let (server, changed) = (server.clone(), changed.clone());
            tasks.spawn(async move {
                let mut sessions = JoinSet::new();
                let pending = Arc::new(Semaphore::new(32));
                loop {
                    while sessions.try_join_next().is_some() {}
                    let (stream, _) = match tcp.accept().await {
                        Ok(v) => v,
                        Err(_) => {
                            sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                    };
                    let Ok(permit) = pending.clone().try_acquire_owned() else {
                        continue;
                    };
                    let (listen, targets, server, changed) = (
                        listen.clone(),
                        targets.clone(),
                        server.clone(),
                        changed.clone(),
                    );
                    sessions.spawn(async move {
                        let handshake = async {
                            let (wire, pin) = listen.accept(stream).await?;
                            let target = targets
                                .get(&pin)
                                .ok_or_else(|| invalid("unexpected peer principal"))?;
                            bind(wire, pin, Side::Accept, server, target, &changed).await
                        };
                        let result = timeout(Duration::from_secs(10), handshake).await;
                        drop(permit);
                        if let Ok(Ok((lease, driver))) = result {
                            let _lease = lease;
                            let _ = driver.await;
                        }
                    });
                }
            });
        }
        for (url, dial, target) in dials {
            let (server, changed) = (server.clone(), changed.clone());
            tasks.spawn(async move {
                let mut retry = Duration::from_millis(150);
                loop {
                    let handshake = async {
                        let (wire, pin) = dial.connect(&url).await?;
                        bind(wire, pin, Side::Dial, server.clone(), &target, &changed).await
                    };
                    if let Ok(Ok((lease, driver))) =
                        timeout(Duration::from_secs(10), handshake).await
                    {
                        retry = Duration::from_millis(150);
                        let _ = driver.await;
                        drop(lease);
                    }
                    sleep(retry).await;
                    retry = (retry * 2).min(Duration::from_secs(5));
                }
            });
        }
        Ok(RunningPeers {
            server,
            bindings,
            changed,
            addresses,
            _tasks: tasks,
        })
    }
}

pub struct PeerBuilder<'a, T> {
    host: &'a mut PeerHost,
    name: &'a str,
    marker: std::marker::PhantomData<T>,
}
impl<T: PeerInterface> PeerBuilder<'_, T> {
    fn install(self, make: impl FnOnce(Target) -> Endpoint) -> io::Result<()> {
        if !crate::context::valid_name(self.name)
            || self.host.bindings.slots.contains_key(self.name)
        {
            return Err(invalid("invalid or duplicate peer name"));
        }
        let live = Live::default();
        self.host.endpoints.push(make(Target {
            hash: T::HASH,
            live: live.clone(),
        }));
        self.host.bindings.slots.insert(
            self.name.into(),
            Slot {
                namespace: T::NAMESPACE,
                hash: T::HASH,
                live,
            },
        );
        Ok(())
    }
    pub fn accept(self, url: &str, pin: impl AsRef<str>) -> io::Result<()> {
        let uri = uri(url)?;
        if uri.path_and_query().is_some_and(|p| p.as_str() != "/") {
            return Err(invalid("accept URL must use the root path"));
        }
        let pin = pin.as_ref();
        crate::tls::validate_pin(pin)?;
        let addr = format!("{}:{}", uri.host().unwrap(), uri.port_u16().unwrap_or(443));
        self.install(|target| Endpoint::Accept(addr, pin.into(), target))
    }
    pub fn dial(self, url: &str, pin: impl AsRef<str>) -> io::Result<()> {
        uri(url)?;
        let dial = Dial::new(&self.host.identity, pin.as_ref())?;
        self.install(|target| Endpoint::Dial(url.into(), dial, target))
    }
}
fn uri(url: &str) -> io::Result<tungstenite::http::Uri> {
    let uri: tungstenite::http::Uri = url.parse().map_err(|_| invalid("invalid WSS URL"))?;
    if uri.scheme_str() != Some("wss")
        || uri.host().is_none()
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
    {
        return Err(invalid("expected wss://host[:port]"));
    }
    if uri.port().is_none()
        && uri.authority().is_some_and(|a| {
            // A colon after a bracketed IPv6 literal (or after a hostname) introduces a port.
            a.as_str()
                .trim_start_matches(uri.host().unwrap_or_default())
                .starts_with(':')
        })
    {
        return Err(invalid("invalid WSS port"));
    }
    Ok(uri)
}

struct Proxy {
    namespace: &'static str,
    live: Live,
}
impl Proxy {
    fn remote(&self) -> Result<crate::Remote, Failure> {
        lock(&self.live)
            .clone()
            .map(|conn| crate::Remote::new(conn, self.namespace).for_caller())
            .ok_or_else(|| crate::fail("unavailable", "mounted peer is offline", self.namespace))
    }
}
impl crate::Activation for Proxy {
    fn pass(&self, method: &str, params: Option<crate::Raw>) -> crate::Passed {
        self.pass_call(None, method, params, None, None)
            .unwrap_or_else(crate::Passed::failed)
    }
    fn pass_call(
        &self,
        keys: Option<&str>,
        method: &str,
        params: Option<crate::Raw>,
        caller: Option<&Caller>,
        input: Option<crate::RawInput>,
    ) -> Result<crate::Passed, Failure> {
        self.remote()?
            .pass_call(keys, method, params, caller, input)
    }
    fn tree(
        &self,
        at: &str,
    ) -> futures::future::BoxFuture<'static, Result<Vec<crate::Place>, Failure>> {
        match self.remote() {
            Ok(remote) => remote.tree(at),
            Err(error) => Box::pin(async { Err(error) }),
        }
    }
}

struct Lease {
    live: Live,
    changed: watch::Sender<()>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        *lock(&self.live) = None;
        self.changed.send_replace(());
    }
}
async fn bind(
    wire: Channel,
    pin: String,
    side: Side,
    server: Arc<Server>,
    target: &Target,
    changed: &watch::Sender<()>,
) -> io::Result<(
    Lease,
    impl std::future::Future<Output = io::Result<()>> + Send + 'static + use<>,
)> {
    let (connection, driver) =
        peer::connect(wire, side, Caller::new(pin), server, target.hash).await?;
    let mut live = lock(&target.live);
    if live.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "peer already connected",
        ));
    }
    *live = Some(connection);
    changed.send_replace(());
    Ok((
        Lease {
            live: target.live.clone(),
            changed: changed.clone(),
        },
        driver,
    ))
}

/// Owns the listeners and session tasks. Dropping the host stops them.
pub struct RunningPeers {
    server: Arc<Server>,
    bindings: Arc<Bindings>,
    changed: watch::Sender<()>,
    addresses: BTreeMap<String, SocketAddr>,
    _tasks: JoinSet<()>,
}
impl RunningPeers {
    pub fn server(&self) -> Arc<Server> {
        self.server.clone()
    }
    pub fn listen_addr(&self, authority: &str) -> Option<SocketAddr> {
        self.addresses.get(authority).copied()
    }
    pub fn peer<T: PeerInterface>(&self, name: &str) -> Result<Peer<T>, Failure> {
        self.bindings.resolve(Some(name))
    }
    pub fn is_ready(&self) -> bool {
        self.bindings.check().is_ok()
    }
    pub async fn ready(&self) {
        let mut changes = self.changed.subscribe();
        while !self.is_ready() {
            changes.changed().await.expect("host owns sender");
        }
    }
}
