//! Typed change sources use the caller and the same decoded arguments as the read.
use futures::{StreamExt, channel::mpsc};
use plexus::{Caller, Connection, Failure, Server};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

#[derive(Default)]
struct Source(Mutex<Option<mpsc::UnboundedSender<()>>>);
impl Source {
    fn changed(&self, caller: &Caller, id: &str) -> mpsc::UnboundedReceiver<()> {
        assert_eq!((caller.principal(), id), ("alice", "ticket-7"));
        let (tx, rx) = mpsc::unbounded();
        *self.0.lock().unwrap() = Some(tx);
        rx
    }

    fn policy(&self, caller: &Caller) -> Result<mpsc::UnboundedReceiver<()>, Failure> {
        Ok(self.changed(caller, "ticket-7"))
    }

    fn notify(&self) {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .unbounded_send(())
            .unwrap();
    }
    fn closed(&self) -> bool {
        self.0.lock().unwrap().as_ref().unwrap().is_closed()
    }
}

#[plexus::activation(roles(reader = ["get"]))]
mod board {
    use super::*;
    pub struct Board {
        pub(super) store: Arc<Source>,
        pub(super) access: Arc<Source>,
        pub(super) reads: Arc<AtomicU32>,
    }
    impl Board {
        #[watch(on = [store.changed(caller, &id), self.access.policy(caller)?])]
        pub fn get(&self, caller: &Caller, id: String) -> u32 {
            assert_eq!((caller.principal(), id.as_str()), ("alice", "ticket-7"));
            let n = self.reads.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // Both subscriptions must already exist, and together wake one read.
                self.store.notify();
                self.access.notify();
            }
            n
        }
    }
}

#[tokio::test]
async fn typed_sources_register_before_read_coalesce_and_release_on_revocation() {
    use std::time::Duration;
    tokio::time::timeout(Duration::from_secs(5), async {
        let store = Arc::<Source>::default();
        let access = Arc::<Source>::default();
        let reads = Arc::<AtomicU32>::default();
        let allowed = Arc::new(AtomicBool::new(true));
        let mut server = Server::new();
        board::serve(
            board::Board {
                store: store.clone(),
                access: access.clone(),
                reads: reads.clone(),
            },
            &mut server,
            "board",
        );
        let grant = allowed.clone();
        server.authorize_grants(move |_, name, _| {
            name == "board.get" && grant.load(Ordering::SeqCst)
        });
        let (a, b) = plexus::pair();
        tokio::spawn(Arc::new(server).serve_as(a, Caller::new("alice")));
        let (client, drive) = Connection::new(b, None);
        tokio::spawn(drive);
        let mut updates = client
            .stream("board.get-watch", serde_json::json!({"id":"ticket-7"}))
            .unwrap();
        assert_eq!(updates.next().await.unwrap().unwrap().get(), "0");
        assert_eq!(updates.next().await.unwrap().unwrap().get(), "1");
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), updates.next())
                .await
                .is_err()
        );
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        allowed.store(false, Ordering::SeqCst);
        access.notify();
        assert_eq!(
            updates.next().await.unwrap().unwrap_err().code(),
            Some("forbidden")
        );
        assert!(store.closed() && access.closed());
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        let schema: serde_json::Value = serde_json::from_str(board::IR).unwrap();
        assert_eq!(
            schema["methods"][0]["watch"]["on"],
            serde_json::json!(["store.changed", "access.policy"])
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_closed_policy_source_ends_the_watch_even_if_data_is_ready() {
    let (data, data_stream) = mpsc::unbounded();
    let (policy, policy_stream) = mpsc::unbounded();
    let mut changes =
        plexus::merge_invalidations(vec![Box::pin(data_stream), Box::pin(policy_stream)]);
    data.unbounded_send(()).unwrap();
    drop(policy);
    assert_eq!(changes.next().await, None);
    drop(changes);
    assert!(data.is_closed());
}
