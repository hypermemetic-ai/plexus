//! Atomically replaceable native activation roots, shared by every transport.
use crate::context::valid_name;
use crate::{Serve, Server, lock};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};

type Install = Box<dyn FnOnce(&mut Server) + Send>;

#[derive(Clone, Default)]
struct Runtime {
    #[cfg(feature = "tokio")]
    handle: Option<tokio::runtime::Handle>,
}

impl Runtime {
    fn here() -> Self {
        Self {
            #[cfg(feature = "tokio")]
            handle: tokio::runtime::Handle::try_current().ok(),
        }
    }
}

// A root is initialized once across all catalog generations that contain it.
struct Root {
    name: &'static str,
    install: Mutex<Option<Install>>,
    server: OnceLock<Result<Arc<Server>, String>>,
}

impl Root {
    fn get(&self, runtime: &Runtime) -> io::Result<&Arc<Server>> {
        let result = self.server.get_or_init(|| {
            #[cfg(feature = "tokio")]
            let _enter = runtime.handle.as_ref().map(|h| h.enter());
            #[cfg(not(feature = "tokio"))]
            let _ = runtime;
            let install = lock(&self.install).take().expect("root initialized once");
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut server = Server::new();
                install(&mut server);
                server.finish_registration(self.name)?;
                Ok(Arc::new(server))
            }))
            .unwrap_or_else(|_| Err("activation registration panicked".to_owned()))
        });
        result
            .as_ref()
            .map_err(|message| io::Error::other(message.clone()))
    }
}

#[derive(Default)]
struct Generation {
    roots: BTreeMap<String, Arc<Root>>,
    routes: OnceLock<Arc<Server>>,
}

#[derive(Default)]
struct State {
    current: Arc<Generation>,
    running: Option<Runtime>,
}

/// A shared catalog of live native activation roots. Registration before
/// serving is staged, so activation threads inherit the listener's runtime.
/// Insert, replace and remove affect future opens on existing connections.
#[derive(Clone, Default)]
pub struct Activations {
    state: Arc<Mutex<State>>,
}

impl Activations {
    /// Register a new root under its canonical declared namespace.
    pub fn insert<A: Serve>(&self, activation: A) -> io::Result<()> {
        self.install(activation, false)
    }

    /// Change the implementation of an existing root, keeping current calls alive.
    pub fn replace<A: Serve>(&self, activation: A) -> io::Result<()> {
        self.install(activation, true)
    }

    fn install<A: Serve>(&self, activation: A, replace: bool) -> io::Result<()> {
        let name = A::NAME;
        check_name(name)?;
        let root = Arc::new(Root {
            name,
            install: Mutex::new(Some(Box::new(move |s| activation.serve(s, name)))),
            server: OnceLock::new(),
        });
        // Stage atomically if the listener hasn't started. Otherwise prepare
        // the replacement outside the lock before publishing it.
        let runtime = {
            let state = lock(&self.state);
            check_exists(&state.current, name, replace)?;
            match &state.running {
                Some(runtime) => runtime.clone(),
                None => {
                    change(state, name, Some(root));
                    return Ok(());
                }
            }
        };
        root.get(&runtime)?;
        let state = lock(&self.state);
        check_exists(&state.current, name, replace)?;
        change(state, name, Some(root));
        Ok(())
    }

    /// Withdraw a root and all its descendants. Existing calls retain their handlers.
    pub fn remove(&self, name: &str) -> io::Result<()> {
        check_name(name)?;
        let state = lock(&self.state);
        check_exists(&state.current, name, true)?;
        change(state, name, None);
        Ok(())
    }

    /// Build a server that follows this handle. Configure transport-wide
    /// grant authorization and exact trusted forwarders on the returned server.
    pub fn server(&self) -> Server {
        Server::live(self.clone())
    }

    pub(crate) fn bind_runtime(&self) -> io::Result<()> {
        let (generation, runtime) = {
            let mut state = lock(&self.state);
            let runtime = state.running.get_or_insert_with(Runtime::here).clone();
            (state.current.clone(), runtime)
        };
        for root in generation.roots.values() {
            root.get(&runtime)?;
        }
        Ok(())
    }

    pub(crate) fn routes(&self) -> io::Result<Arc<Server>> {
        let (generation, runtime) = {
            let state = lock(&self.state);
            (
                state.current.clone(),
                state.running.clone().unwrap_or_default(),
            )
        };
        if let Some(routes) = generation.routes.get() {
            return Ok(routes.clone());
        }
        let mut routes = Server::new();
        for root in generation.roots.values() {
            routes.extend_routes(root.get(&runtime)?);
        }
        Ok(generation.routes.get_or_init(|| Arc::new(routes)).clone())
    }
}

fn check_name(name: &str) -> io::Result<()> {
    if valid_name(name) && !name.starts_with('_') {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "expected one public activation namespace",
    ))
}

fn check_exists(generation: &Generation, name: &str, expected: bool) -> io::Result<()> {
    if generation.roots.contains_key(name) == expected {
        return Ok(());
    }
    let kind = if expected {
        io::ErrorKind::NotFound
    } else {
        io::ErrorKind::AlreadyExists
    };
    Err(io::Error::new(kind, format!("activation {name}")))
}

fn change(mut state: std::sync::MutexGuard<'_, State>, name: &str, root: Option<Arc<Root>>) {
    let mut roots = state.current.roots.clone();
    if let Some(root) = root {
        roots.insert(name.to_owned(), root);
    } else {
        roots.remove(name);
    }
    let next = Arc::new(Generation {
        roots,
        routes: OnceLock::new(),
    });
    let old = std::mem::replace(&mut state.current, next);
    drop(state);
    // A staged activation may run arbitrary Drop code, including registry access.
    drop(old);
}
