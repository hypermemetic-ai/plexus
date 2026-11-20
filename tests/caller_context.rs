//! A direct user's key reaches a native activation before it opens the tenant.
#![cfg(feature = "tls")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use plexus::tls::{Dial, Identity, Listen};
use plexus::{Caller, Channel, Connection, Error, Failure, FromCaller, Server};
use serde_json::{Value, json};

#[plexus::activation]
mod guestbook {
    use futures::{Stream, stream};
    use plexus::Caller;
    use std::cell::RefCell;

    #[derive(Default)]
    pub struct Guestbook {
        entries: RefCell<Vec<(String, String)>>,
    }

    impl Guestbook {
        pub async fn sign(&self, text: String, caller: &Caller) -> u32 {
            self.entries
                .borrow_mut()
                .push((caller.principal().into(), text));
            self.entries.borrow().len() as u32
        }

        pub fn mine(&self, caller: &Caller) -> impl Stream<Item = String> {
            let entries = self.entries.borrow().clone();
            stream::iter(
                entries
                    .into_iter()
                    .filter(move |(who, _)| who == caller.principal())
                    .map(|(_, body)| body),
            )
        }

        pub fn count(&self) -> u32 {
            self.entries.borrow().len() as u32
        }

        pub fn visitor(&self, caller: Option<&Caller>) -> Option<String> {
            caller.map(|who| who.principal().to_owned())
        }
    }
}

#[plexus::activation(context = ShelfCx)]
mod shelf {
    #[derive(plexus::Context)]
    pub struct ShelfCx {
        aisle: u16,
        label: String,
    }

    #[derive(Default)]
    pub struct Shelf;

    impl Shelf {
        pub fn location(&self, cx: &ShelfCx) -> String {
            format!("{}:{}", cx.aisle, cx.label)
        }
    }
}

#[tokio::test]
async fn public_context_reads_typed_keys_without_a_user() {
    let mut server = Server::new();
    shelf::serve(shelf::Shelf, &mut server, "shelf");
    let (near, far) = plexus::pair();
    tokio::spawn(Arc::new(server).serve(far));
    let conn = connect(near);
    assert_eq!(
        read(&conn, "shelf[label=a.b,aisle=3].location").await,
        "3:a.b"
    );
    assert_eq!(
        conn.call("shelf[label=a.b,aisle=lots].location", ())
            .await
            .unwrap_err()
            .code(),
        Some("invalid-context")
    );
    let ir = read(&conn, "shelf._ir").await;
    assert!(ir["methods"][0].get("caller").is_none());
}

struct User(String);

#[plexus::activation(context = StudioCx)]
mod studio {
    use super::{HashMap, Mutex, User};
    use futures::{Stream, stream};
    use std::sync::Arc;

    #[derive(plexus::Context)]
    pub struct StudioCx {
        #[from(caller)]
        user: User,
        tenant: String,
    }

    pub enum Denied {
        NotAMember,
    }

    pub struct Studio {
        pub users: HashMap<String, String>,
        pub members: Arc<Mutex<HashMap<String, String>>>,
    }

    impl Studio {
        #[open]
        pub fn open(&self, cx: &StudioCx) -> Result<(), Denied> {
            if self.members.lock().unwrap().get(&cx.tenant) == Some(&cx.user.0) {
                Ok(())
            } else {
                Err(Denied::NotAMember)
            }
        }

        pub fn status(&self, cx: &StudioCx) -> String {
            format!("{} in {}", cx.user.0, cx.tenant)
        }

        pub fn readings(&self) -> impl Stream<Item = u32> {
            stream::iter([1, 2])
        }
    }
}

impl FromCaller<studio::Studio> for User {
    fn from_caller(activation: &studio::Studio, caller: &Caller) -> Result<Self, Failure> {
        activation
            .users
            .get(caller.principal())
            .cloned()
            .map(User)
            .ok_or_else(|| plexus::fail("unknown-user", "no account for this key", ()))
    }
}

fn connect(channel: Channel) -> Connection {
    let (connection, driver) = Connection::new(channel, None);
    tokio::spawn(driver);
    connection
}

async fn read(conn: &Connection, method: &str) -> Value {
    serde_json::from_str(conn.call(method, ()).await.unwrap().get()).unwrap()
}

async fn stream(conn: &Connection, method: &str) -> Result<Vec<Value>, Error> {
    let mut items = conn.stream(method, ())?;
    let mut rows = Vec::new();
    while let Some(item) = items.next().await {
        rows.push(serde_json::from_str(item?.get()).unwrap());
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_authenticated_users_and_an_anonymous_connection_share_one_activation() {
    let service = Identity::generate().unwrap();
    let alice_key = Identity::generate().unwrap();
    let bob_key = Identity::generate().unwrap();
    let unregistered = Identity::generate().unwrap();
    let members = Arc::new(Mutex::new(HashMap::from([("acme".into(), "Alice".into())])));
    let mut server = Server::new();
    guestbook::serve(guestbook::Guestbook::default(), &mut server, "guestbook");
    studio::serve(
        studio::Studio {
            users: HashMap::from([
                (alice_key.principal().into(), "Alice".into()),
                (bob_key.principal().into(), "Bob".into()),
            ]),
            members: members.clone(),
        },
        &mut server,
        "work.studio",
    );
    let server = Arc::new(server);
    let listener = Listen::new(
        &service,
        [
            alice_key.principal(),
            bob_key.principal(),
            unregistered.principal(),
        ],
    )
    .unwrap();
    let addr = listener
        .spawn("127.0.0.1:0".parse().unwrap(), server.clone())
        .await
        .unwrap();
    let url = format!("wss://{addr}/");
    let dial = |id: &Identity| Dial::new(id, service.principal()).unwrap();
    let (alice, bob, guest) = (dial(&alice_key), dial(&bob_key), dial(&unregistered));
    let (alice, bob) = tokio::join!(alice.connect(&url), bob.connect(&url));
    let (alice, bob) = (connect(alice.unwrap().0), connect(bob.unwrap().0));
    let guest = connect(guest.connect(&url).await.unwrap().0);
    let (near, far) = plexus::pair();
    tokio::spawn(server.serve(far));
    let anonymous = connect(near);

    assert_eq!(read(&anonymous, "guestbook.visitor").await, Value::Null);
    assert_eq!(
        read(&alice, "guestbook.visitor").await,
        alice_key.principal()
    );
    let (a, b) = tokio::join!(
        alice.call("guestbook.sign", json!({"text":"hello"})),
        bob.call("guestbook.sign", json!({"text":"hi"}))
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        anonymous
            .call("guestbook.sign", json!({"text":"ignored"}))
            .await
            .unwrap_err()
            .code(),
        Some("unauthenticated")
    );
    assert_eq!(
        stream(&anonymous, "guestbook.mine")
            .await
            .unwrap_err()
            .code(),
        Some("unauthenticated")
    );
    assert_eq!(
        bob.call(
            "guestbook.sign",
            json!({"caller":alice_key.principal(),"text":"forged"})
        )
        .await
        .unwrap_err()
        .code(),
        Some("invalid-params")
    );
    assert_eq!(
        stream(&alice, "guestbook.mine").await.unwrap(),
        [json!("hello")]
    );
    assert_eq!(stream(&bob, "guestbook.mine").await.unwrap(), [json!("hi")]);

    assert_eq!(read(&anonymous, "guestbook.count").await, 2);
    let acme = "work.studio[tenant=acme]";
    assert_eq!(
        read(&alice, &format!("{acme}.status")).await,
        "Alice in acme"
    );
    assert_eq!(
        stream(&alice, &format!("{acme}.readings")).await.unwrap(),
        [json!(1), json!(2)]
    );
    assert_eq!(
        stream(&bob, &format!("{acme}.readings"))
            .await
            .unwrap_err()
            .code(),
        Some("not-a-member")
    );
    assert_eq!(
        anonymous
            .call(&format!("{acme}.status"), ())
            .await
            .unwrap_err()
            .code(),
        Some("unauthenticated")
    );
    assert_eq!(
        guest
            .call(&format!("{acme}.status"), ())
            .await
            .unwrap_err()
            .code(),
        Some("unknown-user")
    );
    assert_eq!(
        alice
            .call("work.studio[tenant=other].status", ())
            .await
            .unwrap_err()
            .code(),
        Some("not-a-member")
    );
    assert!(
        bob.call(&format!("{acme}.status"), json!({"user":"Alice"}))
            .await
            .is_err()
    );
    for address in [
        "work.studio.status",
        "work.studio[other=acme].status",
        "work.studio[tenant=acme,tenant=other].status",
        "work.studio[tenant=acme,user=Alice].status",
        "work[tenant=acme].studio.status",
        "work.studio.status[tenant=acme]",
        "work.studio[tenant=acme.status",
    ] {
        assert!(
            alice.call(address, ()).await.is_err(),
            "unexpected access at {address}"
        );
    }
    members.lock().unwrap().remove("acme");
    assert_eq!(
        stream(&alice, &format!("{acme}.readings"))
            .await
            .unwrap_err()
            .code(),
        Some("not-a-member")
    );

    let ir = read(&anonymous, "work.studio._ir").await;
    assert_eq!(
        ir["context"]["keys"],
        json!([{"name":"tenant","type":"string"}])
    );
    assert_eq!(ir["context"]["fails"], "denied");
    assert!(
        ir["methods"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["params"] == json!([]) && m["caller"] == "required")
    );
    assert!(
        !ir["methods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["name"] == "open")
    );
    let ir = read(&alice, "guestbook._ir").await;
    let mine = ir["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "mine")
        .unwrap();
    assert_eq!(
        (&mine["params"], &mine["caller"]),
        (&json!([]), &json!("required"))
    );
}
