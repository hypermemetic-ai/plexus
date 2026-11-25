//! Generated native watch companions use the original typed read and admission.
use futures::{StreamExt, channel::mpsc};
use plexus::{Caller, Connection, Encoded, Failure, Invalidations, Raw, Server, WatchSource};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct State {
    value: AtomicU32,
    registered: AtomicBool,
    reads: AtomicU32,
    change_while_reading: AtomicBool,
    wake: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

#[plexus::activation(roles(reader = ["list", "plain"]))]
mod board {
    use super::*;
    pub struct Board(pub(super) Arc<State>);
    impl Board {
        #[watch(on = ["rows","membership"])]
        pub async fn list(&self, caller: &Caller, offset: u32) -> Encoded<u32> {
            assert_eq!(caller.principal(), "alice");
            assert!(self.0.registered.load(Ordering::SeqCst));
            self.0.reads.fetch_add(1, Ordering::SeqCst);
            let value = self.0.value.load(Ordering::SeqCst);
            Encoded::new(&(value + offset))
                .unwrap()
                .with_watch_digest(u128::from(value / 2))
        }
        #[watch(on = ["membership"])]
        pub fn plain(&self) -> u32 {
            let visible = self.0.value.load(Ordering::SeqCst);
            if self.0.change_while_reading.swap(false, Ordering::SeqCst) {
                self.0.value.store(visible + 1, Ordering::SeqCst);
                self.0
                    .wake
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .unbounded_send(())
                    .unwrap();
            }
            visible
        }
    }
}
impl WatchSource for board::Board {
    fn subscribe(
        &self,
        deps: &[&str],
        cx: &plexus::CallContext,
        _: &Raw,
    ) -> Result<Invalidations, Failure> {
        assert_eq!(cx.require_caller()?.principal(), "alice");
        assert!(deps.contains(&"membership"));
        let (tx, rx) = mpsc::unbounded();
        self.0.registered.store(true, Ordering::SeqCst);
        *self.0.wake.lock().unwrap() = Some(tx.clone());
        // A notification during creation survives until the first re-read.
        tx.unbounded_send(()).unwrap();
        Ok(Box::pin(rx))
    }
}

fn client(server: &Arc<Server>, caller: &str) -> Connection {
    let (a, b) = plexus::pair();
    tokio::spawn(server.serve_as(a, Caller::new(caller)));
    let (conn, run) = Connection::new(b, None);
    tokio::spawn(run);
    conn
}

#[tokio::test]
async fn generated_watch_rechecks_grant_and_suppresses_equal_visible_digest() {
    let state = Arc::new(State::default());
    let grant = Arc::new(AtomicBool::new(true));
    let allowed = grant.clone();
    let mut server = Server::new();
    board::serve(board::Board(state.clone()), &mut server, "board");
    server.authorize_grants(move |caller, grant, roles| {
        caller.principal() == "alice"
            && matches!(grant, "board.list" | "board.plain")
            && roles.contains(&"reader")
            && allowed.load(Ordering::SeqCst)
    });
    let server = Arc::new(server);
    let alice = client(&server, "alice");
    let bob = client(&server, "bob");
    let args = serde_json::json!({"offset":7});
    let mut denied = bob.stream("board.list-watch", args.clone()).unwrap();
    assert_eq!(
        denied.next().await.unwrap().unwrap_err().code(),
        Some("forbidden")
    );
    assert!(!state.registered.load(Ordering::SeqCst));
    let mut items = alice.stream("board.list-watch", args.clone()).unwrap();
    assert_eq!(items.next().await.unwrap().unwrap().get(), "7");
    let wake = state.wake.lock().unwrap().clone().unwrap();
    state.value.store(1, Ordering::SeqCst);
    wake.unbounded_send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), items.next())
            .await
            .is_err()
    );
    assert!(state.reads.load(Ordering::SeqCst) >= 2);
    state.value.store(2, Ordering::SeqCst);
    wake.unbounded_send(()).unwrap();
    assert_eq!(items.next().await.unwrap().unwrap().get(), "9");
    grant.store(false, Ordering::SeqCst);
    wake.unbounded_send(()).unwrap();
    assert_eq!(
        items.next().await.unwrap().unwrap_err().code(),
        Some("forbidden")
    );
    assert!(wake.is_closed());
    grant.store(true, Ordering::SeqCst);
    state.change_while_reading.store(true, Ordering::SeqCst);
    let mut plain = alice.stream("board.plain-watch", ()).unwrap();
    assert_eq!(plain.next().await.unwrap().unwrap().get(), "2");
    // No further write is needed: the source was live when the first
    // authorized projection raced a change from 2 to 3.
    assert_eq!(plain.next().await.unwrap().unwrap().get(), "3");
    let wake = state.wake.lock().unwrap().clone().unwrap();
    wake.unbounded_send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), plain.next())
            .await
            .is_err()
    );
    drop(plain);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !wake.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[plexus::activation(context = Scope)]
mod scoped {
    use super::*;
    #[derive(plexus::Context)]
    pub struct Scope {
        tenant: String,
    }
    pub enum Denied {
        MembershipChanged,
    }
    pub struct Scoped(pub(super) Arc<State>);
    impl Scoped {
        #[open]
        pub fn open(&self, cx: &Scope) -> Result<(), Denied> {
            if cx.tenant == "acme" && self.0.registered.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Denied::MembershipChanged)
            }
        }
        #[watch(on = ["membership"])]
        pub fn name(&self, cx: &Scope, label: String) -> Result<String, Denied> {
            Ok(format!("{} {label}", cx.tenant))
        }
    }
}

impl WatchSource for scoped::Scoped {
    fn subscribe(
        &self,
        _: &[&str],
        _: &plexus::CallContext,
        params: &Raw,
    ) -> Result<Invalidations, Failure> {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(params.get()).unwrap()["label"],
            "board"
        );
        let (tx, rx) = mpsc::unbounded();
        *self.0.wake.lock().unwrap() = Some(tx);
        Ok(Box::pin(rx))
    }
}

#[tokio::test]
async fn keyed_watch_reopens_the_typed_scope_on_refresh() {
    let state = Arc::new(State::default());
    state.registered.store(true, Ordering::SeqCst);
    let mut server = Server::new();
    scoped::serve(scoped::Scoped(state.clone()), &mut server, "scoped");
    let user = client(&Arc::new(server), "alice");
    let args = serde_json::json!({"label":"board"});
    let path = "scoped[tenant=acme].name-watch";
    let mut wrong = user
        .stream("scoped[tenant=other].name-watch", args.clone())
        .unwrap();
    assert!(wrong.next().await.unwrap().is_err());
    assert!(state.wake.lock().unwrap().is_none());
    let mut good = user.stream(path, args.clone()).unwrap();
    assert_eq!(good.next().await.unwrap().unwrap().get(), r#""acme board""#);
    state.registered.store(false, Ordering::SeqCst);
    state
        .wake
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .unbounded_send(())
        .unwrap();
    let revoked = good.next().await.unwrap().unwrap_err();
    let unary = user
        .call("scoped[tenant=acme].name", args)
        .await
        .unwrap_err();
    assert_eq!(revoked.code(), unary.code());
    assert!(good.next().await.is_none());
}
