//! Configure a process serving several independently named activation roots.
use std::future::Future;
use std::io;
use std::sync::Arc;

use futures::future::{BoxFuture, FutureExt, select_all};

use crate::{Activations, Serve, Server, listener::Listener};

/// Start a single native activation, or build an application from a collection.
///
/// Passing one activation runs the standard native listener (stdio, `--ws`
/// or pinned `--wss`). Passing an array or vector returns an [`Application`].
/// Use [`crate::activations!`] to mix different activation types in an array.
///
/// ```no_run
/// # #[plexus::activation] mod kiln { #[derive(Default)] pub struct Kiln; impl Kiln { pub fn get(&self) -> u32 { 6 } } }
/// use kiln::Kiln;
/// fn main() -> std::io::Result<()> {
///     plexus::main([Kiln::default()]).run()
/// }
/// ```
pub fn main<I: MainInput>(input: I) -> I::Output {
    input.start()
}

/// The input determines whether [`main`] runs a service or returns an application.
pub trait MainInput {
    type Output;
    #[doc(hidden)]
    fn start(self) -> Self::Output;
}

impl<A: Serve> MainInput for A {
    type Output = io::Result<()>;
    fn start(self) -> Self::Output {
        crate::main_with(self, |_| {})
    }
}

type Install = Box<dyn FnOnce(&Activations) -> io::Result<()> + Send>;

/// An owned activation in a mixed-type array. [`crate::activations!`] constructs these.
pub struct Registration(Install);

impl<A: Serve> From<A> for Registration {
    fn from(activation: A) -> Self {
        Self(Box::new(move |roots| roots.insert(activation)))
    }
}

/// Build an array from differently typed activations. Each expression is evaluated once.
#[macro_export]
macro_rules! activations {
    ($($activation:expr),* $(,)?) => {{
        let entries: [$crate::Registration; _] = [$( $crate::Registration::from($activation) ),*];
        entries
    }};
}

impl<T: Into<Registration>, const N: usize> MainInput for [T; N] {
    type Output = Application;
    fn start(self) -> Application {
        Application::from_list(self)
    }
}

impl<T: Into<Registration>> MainInput for Vec<T> {
    type Output = Application;
    fn start(self) -> Application {
        Application::from_list(self)
    }
}

impl MainInput for Activations {
    type Output = Application;
    fn start(self) -> Application {
        Application {
            roots: self,
            listeners: Vec::new(),
            configure: Vec::new(),
            startup_error: None,
        }
    }
}

type Setup = Box<dyn FnOnce(&mut Server) + Send>;
type Listen = Box<dyn FnOnce(Arc<Server>) -> BoxFuture<'static, io::Result<()>> + Send>;

/// The native listeners, policy and live activation registry for one application.
#[must_use = "call .run() or .run_async() to serve the application"]
pub struct Application {
    roots: Activations,
    configure: Vec<Setup>,
    listeners: Vec<Listen>,
    startup_error: Option<io::Error>,
}

impl Application {
    fn from_list<T: Into<Registration>>(entries: impl IntoIterator<Item = T>) -> Self {
        let mut app = Activations::default().start();
        for entry in entries {
            if let Err(err) = entry.into().0(&app.roots) {
                app.startup_error = Some(err);
                break;
            }
        }
        app
    }

    /// Keep this handle to insert, replace and remove roots while the service runs.
    pub fn activations(&self) -> Activations {
        self.roots.clone()
    }

    /// Install shared server policy, such as the grant checker used by every root.
    pub fn configure(mut self, configure: impl FnOnce(&mut Server) + Send + 'static) -> Self {
        self.configure.push(Box::new(configure));
        self
    }

    /// Attach a listener to the same live server. The application stops if any listener exits.
    pub fn with_listener<F>(
        mut self,
        listen: impl FnOnce(Arc<Server>) -> F + Send + 'static,
    ) -> Self
    where
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.listeners
            .push(Box::new(|server| listen(server).boxed()));
        self
    }

    /// Report an adapter configuration error before opening a listener.
    #[doc(hidden)]
    pub fn with_startup_error(mut self, error: io::Error) -> Self {
        self.startup_error.get_or_insert(error);
        self
    }

    /// Run a standalone process. Native `--ws`, `--wss`, and `--data` options
    /// apply as on a single activation. An added listener replaces default stdio.
    pub fn run(self) -> io::Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(io::Error::other(
                "use run_async().await within a Tokio runtime",
            ));
        }
        let listener = Listener::from_args(std::env::args().skip(1))?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(self.serve(listener))
    }

    /// Run in the caller's Tokio runtime, using configured listeners or default stdio.
    /// This embedding entry point does not consume the host process's CLI arguments.
    pub async fn run_async(self) -> io::Result<()> {
        self.serve(Listener::Stdio).await
    }

    async fn serve(self, native: Listener) -> io::Result<()> {
        if let Some(error) = self.startup_error {
            return Err(error);
        }
        self.roots.bind_runtime()?;
        let mut server = self.roots.server();
        for configure in self.configure {
            configure(&mut server);
        }
        let server = Arc::new(server);
        let mut listeners: Vec<_> = self
            .listeners
            .into_iter()
            .map(|f| f(server.clone()))
            .collect();
        if listeners.is_empty() || !matches!(native, Listener::Stdio) {
            listeners.push(native.serve(server).boxed());
        }
        let (result, _, _) = select_all(listeners).await;
        result
    }
}
