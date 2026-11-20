//! A declared local child and a boxed service mount beneath two typed openings.
#![cfg(feature = "tokio")]

use plexus::{Caller, Connection, Remote, Server, pair};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[plexus::activation]
mod probe {
    use super::*;

    pub struct Probe {
        pub calls: Arc<AtomicUsize>,
    }

    impl Probe {
        pub fn principal(&self, caller: &Caller) -> String {
            self.calls.fetch_add(1, Ordering::Relaxed);
            caller.principal().into()
        }
    }
}

#[plexus::activation(context = WingCx)]
mod wing {
    use super::probe::Probe;
    use super::*;

    #[derive(plexus::Context)]
    pub struct WingCx {
        room: u32,
    }

    pub enum Denied {
        WrongRoom,
    }

    pub struct Wing {
        pub checks: Arc<AtomicUsize>,
        pub probe: Probe,
        pub service: Remote,
    }

    impl Wing {
        #[open]
        fn open(&self, cx: &WingCx) -> Result<(), Denied> {
            self.checks.fetch_add(1, Ordering::Relaxed);
            if cx.room == 7 {
                Ok(())
            } else {
                Err(Denied::WrongRoom)
            }
        }

        pub fn label(&self, cx: &WingCx) -> u32 {
            cx.room
        }

        #[child]
        pub fn local(&self) -> &Probe {
            &self.probe
        }

        #[child]
        pub fn remote(&self) -> Box<dyn plexus::Activation> {
            Box::new(self.service.clone())
        }
    }
}

#[plexus::activation(context = YardCx)]
mod yard {
    use super::wing::Wing;
    use super::*;

    #[derive(plexus::Context)]
    pub struct YardCx {
        #[from(caller)]
        who: Caller,
        tenant: String,
    }

    pub enum Denied {
        NotAMember,
    }

    pub struct Yard {
        pub checks: Arc<AtomicUsize>,
        pub wing: Wing,
    }

    impl Yard {
        #[open]
        fn open(&self, cx: &YardCx) -> Result<(), Denied> {
            self.checks.fetch_add(1, Ordering::Relaxed);
            if cx.tenant == "acme" && cx.who.principal() == "alice" {
                Ok(())
            } else {
                Err(Denied::NotAMember)
            }
        }

        #[child]
        pub fn wing(&self) -> &Wing {
            &self.wing
        }
    }
}

fn connection(server: Arc<Server>, principal: &str) -> Connection {
    let (near, far) = pair();
    tokio::spawn(server.serve_as(far, Caller::new(principal)));
    let (conn, drive) = Connection::new(near, None);
    tokio::spawn(drive);
    conn
}

#[tokio::test]
async fn openings_follow_the_path_once_and_keep_the_two_hop_identities_distinct() {
    let yard_checks = Arc::new(AtomicUsize::new(0));
    let wing_checks = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let mut kiln = Server::new();
    probe::serve(
        probe::Probe {
            calls: hits.clone(),
        },
        &mut kiln,
        "probe",
    );
    let service = Remote::new(connection(Arc::new(kiln), "studio-service"), "probe");
    let mut root = Server::new();
    yard::serve(
        yard::Yard {
            checks: yard_checks.clone(),
            wing: wing::Wing {
                checks: wing_checks.clone(),
                probe: probe::Probe {
                    calls: hits.clone(),
                },
                service,
            },
        },
        &mut root,
        "yard",
    );
    let root = Arc::new(root);
    let (alice, bob) = (connection(root.clone(), "alice"), connection(root, "bob"));

    for child in ["local", "remote"] {
        let path = format!("yard[tenant=acme].wing[room=7].{child}.principal");
        assert_eq!(
            bob.call(&path, ()).await.unwrap_err().code(),
            Some("not-a-member")
        );
        let expected = if child == "local" {
            "alice"
        } else {
            "studio-service"
        };
        assert_eq!(
            alice.call(&path, ()).await.unwrap().get(),
            serde_json::to_string(expected).unwrap()
        );
    }
    assert_eq!(hits.load(Ordering::Relaxed), 2);
    assert_eq!(yard_checks.load(Ordering::Relaxed), 4);
    assert_eq!(wing_checks.load(Ordering::Relaxed), 2);

    assert_eq!(
        alice
            .call("yard[tenant=acme].wing[room=7].label", ())
            .await
            .unwrap()
            .get(),
        "7"
    );
    assert_eq!(wing_checks.load(Ordering::Relaxed), 3);
    assert_eq!(
        alice
            .call("yard[tenant=acme].wing[room=9].remote.principal", ())
            .await
            .unwrap_err()
            .code(),
        Some("wrong-room")
    );
    assert_eq!(hits.load(Ordering::Relaxed), 2);
    for child in ["local", "remote"] {
        let unkeyed = format!("yard.wing.{child}._hash");
        let bound = format!("yard[tenant=acme].wing[room=7].{child}._hash");
        assert_eq!(
            alice.call(&bound, ()).await.unwrap(),
            bob.call(&unkeyed, ()).await.unwrap()
        );
        assert_eq!(
            bob.call(&bound, ()).await.unwrap_err().code(),
            Some("not-a-member")
        );
    }
}
