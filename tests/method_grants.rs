//! Grants apply to each opening, even on a connection that stays up.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures::{TryStreamExt, stream};
use plexus::{Caller, Connection, Server, pair};
use serde_json::json;

#[plexus::activation(roles(reader = ["level"], operator = [reader, "fill"]))]
mod tank {
    use super::*;
    use plexus::Input;

    #[derive(Default)]
    pub struct Tank {
        started: AtomicUsize,
    }

    impl Tank {
        pub fn level(&self) -> u32 {
            self.started.load(Ordering::SeqCst) as u32
        }

        pub async fn fill(&self, mut water: Input<u32>) -> u32 {
            self.started.fetch_add(1, Ordering::SeqCst);
            let mut sum = 0;
            while let Some(next) = water.try_next().await.unwrap() {
                sum += next;
            }
            sum
        }
    }
}

fn connect(server: &Arc<Server>, user: Option<&str>) -> Connection {
    let (near, far) = pair();
    if let Some(name) = user {
        tokio::spawn(server.serve_as(far, Caller::new(name)));
    } else {
        tokio::spawn(server.serve(far));
    }
    let (conn, driver) = Connection::new(near, None);
    tokio::spawn(driver);
    conn
}

#[tokio::test]
async fn role_membership_is_checked_again_before_pulling_input() {
    let mut server = Server::new();
    tank::serve(tank::Tank::default(), &mut server, "site.tank");
    server.trust_forwarder("service", "site.tank.fill");
    let operator = Arc::new(AtomicBool::new(true));
    let current = operator.clone();
    server.authorize_grants(move |who, grant, roles| {
        assert!(grant.starts_with("tank."));
        who.principal() == "alice" && roles.contains(&"operator") && current.load(Ordering::SeqCst)
    });
    let server = Arc::new(server);
    let service = connect(&server, Some("service"));
    let alice = connect(&server, Some("alice"));
    let anon = connect(&server, None);
    let call = json!({"method":"site.tank.fill","principal":"alice"});
    assert_eq!(
        anon.call("_as", &call).await.unwrap_err().code(),
        Some("unauthenticated")
    );
    assert_eq!(alice.call("site.tank.level", ()).await.unwrap().get(), "0");
    let outside = json!({"method":"site.tank.level","principal":"alice"});
    assert_eq!(
        service.call("_as", outside).await.unwrap_err().code(),
        Some("forbidden")
    );
    let filled = service
        .call_with_input("_as", &call, stream::iter([plexus::raw(&3u32)]))
        .await
        .unwrap();
    assert_eq!(filled.get(), "3");
    assert_eq!(alice.call("site.tank.level", ()).await.unwrap().get(), "1");

    operator.store(false, Ordering::SeqCst);
    let never = stream::poll_fn(|_| panic!("refused upload requested input"));
    let e = service
        .call_with_input("_as", &call, never)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some("forbidden"));
    assert_eq!(
        alice.call("site.tank.level", ()).await.unwrap_err().code(),
        Some("forbidden")
    );
    operator.store(true, Ordering::SeqCst);
    assert_eq!(alice.call("site.tank.level", ()).await.unwrap().get(), "1");
}

#[tokio::test]
async fn declaring_roles_without_an_assignment_policy_denies_invocation() {
    let mut server = Server::new();
    tank::serve(tank::Tank::default(), &mut server, "tank");
    let server = Arc::new(server);
    assert_eq!(
        connect(&server, Some("alice"))
            .call("tank.level", ())
            .await
            .unwrap_err()
            .code(),
        Some("forbidden")
    );
    assert_eq!(
        connect(&server, None)
            .call("tank.level", ())
            .await
            .unwrap_err()
            .code(),
        Some("unauthenticated")
    );
}
