//! A child held across a channel: every call under its place passed over a
//! [`Connection`] to another server, paced end to end.

use futures::future::{BoxFuture, FutureExt};
use plexus_proto::{Failure, Raw};

use crate::connection::{Connection, Passed};
use crate::{Activation, Place};

/// An activation served at `at` on the far end of a connection. Held as a
/// child (`server.mount("studio.glaze", Box::new(remote))`), a call to
/// `studio.glaze.recipes` is passed on as `glaze.recipes`: its open with
/// credit for one item, then each credit and the cancel as the caller sends
/// them, so nothing is held here beyond what the caller allowed. When the
/// connection closes, every call under it fails `unavailable`.
#[derive(Clone)]
pub struct Remote {
    conn: Connection,
    at: String,
    forward_caller: bool,
}

impl Remote {
    /// The activation at `at` on the far end of `conn`.
    pub fn new(conn: Connection, at: &str) -> Remote {
        Remote { conn, at: at.to_owned(), forward_caller: false }
    }

    /// Forward the inbound caller on a mounted route. Each destination method
    /// must explicitly trust this service and authorize that caller.
    pub fn for_caller(mut self) -> Self {
        self.forward_caller = true;
        self
    }

    /// The connection it is reached over.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl Activation for Remote {
    fn pass(&self, method: &str, params: Option<Raw>) -> Passed {
        self.pass_call(None, method, params, None, None).unwrap_or_else(Passed::failed)
    }

    fn pass_keyed(&self, keys: &str, method: &str, params: Option<Raw>) -> Result<Passed, Failure> {
        self.pass_call(Some(keys), method, params, None, None)
    }

    fn pass_with_input(&self, keys: Option<&str>, method: &str, params: Option<Raw>, input: crate::RawInput) -> Result<Passed, Failure> {
        self.pass_call(keys, method, params, None, Some(input))
    }

    fn pass_call(&self, keys: Option<&str>, method: &str, params: Option<Raw>, caller: Option<&crate::Caller>, input: Option<crate::RawInput>) -> Result<Passed, Failure> {
        let address = format!("{}{}.{method}", self.at, keys.unwrap_or_default());
        let input = input.map(crate::input::Source::Forward);
        if !self.forward_caller {
            return Ok(self.conn.pass_from(&address, params, input));
        }
        let caller = crate::require_caller(caller)?;
        let body = crate::grants::Forwarded { method: address, principal: caller.principal().into(), params };
        let envelope = Raw::of(&body).map_err(|_| crate::server::failure("internal", "cannot encode the forwarded call"))?;
        Ok(self.conn.pass_from(crate::grants::FORWARD, Some(envelope), input))
    }

    fn tree(&self, at: &str) -> BoxFuture<'static, Result<Vec<Place>, Failure>> {
        let (call, far, at) = (self.conn.call("_tree", ()), self.at.clone(), at.to_owned());
        async move {
            let raw = call.await.map_err(|e| match e {
                crate::Error::Failed(f) => f,
                _ => crate::server::failure("unavailable", "the connection to it has closed"),
            })?;
            let places: Vec<Place> = serde_json::from_str(raw.get()).map_err(|e| crate::server::failure("internal", &format!("its _tree: {e}")))?;
            // Its places under where it is served, renamed under where it is held.
            Ok(places
                .into_iter()
                .filter_map(|p| {
                    let rest = p.place.strip_prefix(far.as_str())?;
                    (rest.is_empty() || rest.starts_with('.')).then(|| Place { place: format!("{at}{rest}"), hash: p.hash })
                })
                .collect())
        }
        .boxed()
    }
}

#[cfg(feature = "tokio")]
mod spawned {
    use std::io;

    use tokio::process::Command;

    use super::Remote;
    use crate::Connection;

    impl Remote {
        /// Start `command` and hold the activation it serves at `at` over its
        /// stdin and stdout (`plexus::main` with no flag). The connection
        /// runs on a thread of its own, so this works with or without a
        /// runtime; the child is killed when the connection ends.
        pub fn child(mut command: Command, at: &str) -> io::Result<Remote> {
            let runtime = runtime()?;
            let (mut child, channel) = {
                let _entered = runtime.enter();
                crate::stdio::child(command.kill_on_drop(true))?
            };
            let (conn, driving) = Connection::new(channel, None);
            std::thread::Builder::new().name("plexus-remote".into()).spawn(move || {
                runtime.block_on(async move {
                    let _ = driving.await;
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                })
            })?;
            Ok(Remote::new(conn, at))
        }

        /// Hold the activation served at `at` on the WebSocket at `url`. The
        /// connection runs on a thread of its own, as [`child`](Remote::child)'s.
        pub fn ws(url: &str, at: &str) -> io::Result<Remote> {
            let url = url.to_owned();
            Self::socket(at, async move { crate::ws::connect(&url).await })
        }

        /// Hold a remote over one mutually authenticated TLS connection. The dialer
        /// supplies this service's identity and the pin the remote must prove.
        /// Calls through this handle use that service identity for the whole hop.
        #[cfg(feature = "tls")]
        pub fn wss(url: &str, at: &str, dial: crate::tls::Dial) -> io::Result<Remote> {
            let url = url.to_owned();
            Self::socket(at, async move { dial.connect(&url).await.map(|(channel, _)| channel) })
        }

        /// As wss, with the incoming authenticated caller asserted on each
        /// invocation. The receiving server explicitly trusts this service
        /// for each eligible method. Unsupported peers refuse the call.
        #[cfg(feature = "tls")]
        pub fn wss_for_caller(url: &str, at: &str, dial: crate::tls::Dial) -> io::Result<Remote> {
            Ok(Self::wss(url, at, dial)?.for_caller())
        }

        fn socket(at: &str, connect: impl Future<Output = io::Result<crate::Channel>> + Send + 'static) -> io::Result<Remote> {
            let runtime = runtime()?;
            let (tell, told) = std::sync::mpsc::channel();
            std::thread::Builder::new().name("plexus-remote".into()).spawn(move || {
                runtime.block_on(async move {
                    let channel = match connect.await {
                        Ok(channel) => channel,
                        Err(e) => return drop(tell.send(Err(e))),
                    };
                    let (conn, driving) = Connection::new(channel, None);
                    let _ = tell.send(Ok(conn));
                    let _ = driving.await;
                })
            })?;
            let conn = told.recv().map_err(|_| io::Error::other("the connection's thread stopped"))??;
            Ok(Remote::new(conn, at))
        }
    }

    fn runtime() -> io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread().enable_all().build()
    }
}
