//! Native protocol calls remain on the same connection across catalog changes.
#![cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
use futures::{StreamExt, TryStreamExt, stream};
use plexus::{Activations, Caller, Connection, Serve, Server, pair};
use serde_json::json;
use std::{
    io::ErrorKind,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct OnCancel(Arc<AtomicBool>);
impl Drop for OnCancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[plexus::activation]
mod meter {
    use super::*;
    pub struct Meter {
        pub version: u32,
        pub cancelled: Arc<AtomicBool>,
    }

    impl Meter {
        pub async fn version(&self) -> u32 {
            // The activation thread must enter the serving runtime even if it
            // was added by an ordinary thread.
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.version
        }
        pub fn readings(&self) -> impl futures::Stream<Item = u32> {
            let guard = OnCancel(self.cancelled.clone());
            stream::repeat(self.version).map(move |v| {
                let _keep = &guard;
                v
            })
        }
    }
}

mod next {
    #[plexus::activation]
    pub mod meter {
        pub struct Meter;
        impl Meter {
            pub fn version(&self) -> u32 {
                2
            }
        }
    }
}

// Use the public native registration API for the grant-bearing duplex route.
struct Upload(u32);
impl Serve for Upload {
    const NAME: &'static str = "meter";
    fn serve(self, server: &mut Server, at: &str) {
        let path = format!("{at}.sum");
        server.method_with_caller(&path, move |_, mut context| async move {
            let mut input = context.take_input(plexus::__private::decode_input::<u32>)?;
            let mut sum = self.0;
            while let Some(n) = input.try_next().await? {
                sum += n;
            }
            plexus::raw(&sum)
        });
        server.accepts_input(&path);
        server.require_grant(path, "meter.sum", &["operator"]);
    }

    fn held<P, G>(&self, _: &plexus::__private::Local<P>, _: G, _: &mut Server, _: &str)
    where
        P: Send + 'static,
        G: for<'a> Fn(&'a P) -> &'a Self + Copy + Send + Sync + 'static,
    {
        unreachable!("test fixture is registered as a root")
    }
}

#[plexus::activation]
mod other {
    pub struct Other {
        pub count: std::cell::Cell<u32>,
    }
    impl Other {
        pub fn bump(&self) -> u32 {
            self.count.set(self.count.get() + 1);
            self.count.get()
        }
    }
}

fn connect(server: &Arc<Server>, principal: &str) -> Connection {
    let (a, b) = pair();
    tokio::spawn(server.serve_as(b, Caller::new(principal)));
    let (connection, driver) = Connection::new(a, None);
    tokio::spawn(driver);
    connection
}

#[tokio::test]
async fn one_connection_sees_insert_replace_remove_and_keeps_its_stream() {
    let registry = Activations::default();
    registry.insert(other::Other { count: 0.into() }).unwrap();
    let server = Arc::new(registry.server());
    let connection = connect(&server, "reader");
    connection.ready().await.unwrap();
    assert_eq!(connection.call("other.bump", ()).await.unwrap().get(), "1");
    assert_eq!(
        connection
            .call("meter.version", ())
            .await
            .unwrap_err()
            .code(),
        Some("not-found")
    );
    let manager = registry.clone();
    let cancelled = Arc::new(AtomicBool::new(false));
    let watch = cancelled.clone();
    std::thread::spawn(move || {
        manager.insert(meter::Meter {
            version: 1,
            cancelled: watch,
        })
    })
    .join()
    .unwrap()
    .unwrap();
    assert_eq!(
        connection.call("meter.version", ()).await.unwrap().get(),
        "1"
    );
    let places: Vec<plexus::Place> =
        serde_json::from_str(connection.call("_tree", ()).await.unwrap().get()).unwrap();
    assert!(
        places
            .iter()
            .any(|p| p.place == "meter" && p.hash.as_deref() == Some(meter::HASH))
    );
    assert_eq!(
        connection.call("meter._hash", ()).await.unwrap().get(),
        json!(meter::HASH).to_string()
    );
    let mut old = connection.stream("meter.readings", ()).unwrap();
    assert_eq!(old.next().await.unwrap().unwrap().get(), "1");
    registry.replace(next::meter::Meter).unwrap();
    let ir: serde_json::Value =
        serde_json::from_str(connection.call("meter._ir", ()).await.unwrap().get()).unwrap();
    assert_eq!(ir["hash"], next::meter::HASH);
    assert_ne!(next::meter::HASH, meter::HASH);
    assert_eq!(
        connection
            .call("meter.readings", ())
            .await
            .unwrap_err()
            .code(),
        Some("not-found")
    );
    assert_eq!(
        connection.call("meter.version", ()).await.unwrap().get(),
        "2"
    );
    assert_eq!(old.next().await.unwrap().unwrap().get(), "1");
    registry.remove("meter").unwrap();
    assert_eq!(
        connection.call("meter._ir", ()).await.unwrap_err().code(),
        Some("not-found")
    );
    assert_eq!(old.next().await.unwrap().unwrap().get(), "1");
    drop(old);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !cancelled.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancel must drop the retired activation’s original stream");
    assert_eq!(connection.call("other.bump", ()).await.unwrap().get(), "2");
}

#[tokio::test]
async fn forwarded_duplex_call_uses_shared_policy_on_every_open() {
    let registry = Activations::default();
    registry.insert(Upload(10)).unwrap();
    let mut server = registry.server();
    server.authorize_grants(|caller, grant, roles| {
        caller.principal() == "alice" && grant == "meter.sum" && roles.contains(&"operator")
    });
    server.trust_forwarder("bridge", "meter.sum");
    let server = Arc::new(server);
    let connection = connect(&server, "bridge");
    connection.ready().await.unwrap();
    let forwarded = |who| json!({"method":"meter.sum","principal":who});
    let input = || stream::iter([plexus::raw(&2), plexus::raw(&3)]);
    assert_eq!(
        connection
            .call_with_input("_as", forwarded("alice"), input())
            .await
            .unwrap()
            .get(),
        "15"
    );
    assert_eq!(
        connection
            .call_with_input("_as", forwarded("other"), input())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    assert_eq!(
        connection
            .call_with_input("meter.sum", (), input())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    registry.replace(Upload(20)).unwrap();
    assert_eq!(
        connection
            .call_with_input("_as", forwarded("alice"), input())
            .await
            .unwrap()
            .get(),
        "25"
    );
}

#[test]
fn duplicate_missing_and_reserved_roots_fail_before_binding() {
    let registry = Activations::default();
    let meter = || meter::Meter {
        version: 1,
        cancelled: Arc::default(),
    };
    assert_eq!(
        registry.replace(meter()).unwrap_err().kind(),
        ErrorKind::NotFound
    );
    registry.insert(meter()).unwrap();
    assert_eq!(
        registry.insert(meter()).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    registry.replace(meter()).unwrap();
    registry.remove("meter").unwrap();
    assert_eq!(
        registry.remove("meter").unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        registry.remove("meter.child").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        registry.remove("_tree").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[plexus::activation(context = ShelfCx)]
mod shelf {
    #[derive(plexus::Context)]
    pub struct ShelfCx {
        aisle: u16,
    }
    pub struct Shelf;
    impl Shelf {
        pub fn location(&self, cx: &ShelfCx) -> u16 {
            cx.aisle
        }
    }
}

#[tokio::test]
async fn outer_admission_and_registered_activation_receive_the_same_context() {
    let registry = Activations::default();
    registry.insert(shelf::Shelf).unwrap();
    let mut server = registry.server();
    server.guard_descendants("shelf", &["aisle"], |cx| async move {
        if cx.keys().parse::<u16>("aisle")? > 5 {
            return Err(plexus::fail("forbidden", "restricted aisle", ()));
        }
        Ok(())
    });
    let connection = connect(&Arc::new(server), "visitor");
    assert_eq!(
        connection
            .call("shelf[aisle=3].location", ())
            .await
            .unwrap()
            .get(),
        "3"
    );
    assert_eq!(
        connection
            .call("shelf[aisle=7].location", ())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    registry.replace(shelf::Shelf).unwrap();
    assert_eq!(
        connection
            .call("shelf[aisle=7].location", ())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    assert_eq!(
        connection
            .call("shelf.location", ())
            .await
            .unwrap_err()
            .code(),
        Some("invalid-context")
    );
}
