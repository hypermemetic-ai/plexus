use futures::StreamExt;
use plexus::{Caller, Connection, PeerHost, PeerKeys, RunningPeers, tls::Identity};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

#[plexus::activation(roles(operator=["who","sequence","echo"]))]
mod kiln {
    use futures::{Stream, StreamExt};
    use plexus::{Caller, Input};
    pub struct Kiln(pub u32);
    impl Kiln {
        pub fn who(&self, caller: &Caller) -> (u32, String) {
            (self.0, caller.principal().to_owned())
        }
        pub fn sequence(&self, n: u32) -> impl Iterator<Item = u32> {
            0..n
        }
        pub fn echo(&self, chunks: Input<plexus::Bytes>) -> impl Stream<Item = plexus::Bytes> {
            chunks.map(|v| v.expect("test input"))
        }
    }
}
#[plexus::activation(injections(Peer<Kiln>))]
mod studio {
    use super::kiln::Kiln;
    use super::{Arc, AtomicU32, Ordering};
    use plexus::Peer;
    pub struct Studio(pub Arc<AtomicU32>);
    impl Studio {
        pub fn status(&self) -> u32 {
            self.0.load(Ordering::SeqCst)
        }
        pub async fn inspect(&self, k: Peer<Kiln>) -> Result<(u32, String), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            k.who().await.map_err(|e| e.to_string())
        }
        pub async fn verify(&self, k: plexus::Peer<Kiln>) -> Result<(u32, String), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            k.who().await.map_err(|e| e.to_string())
        }
        pub async fn identities(
            &self,
            user: plexus::User,
            kiln: Peer<Kiln>,
        ) -> Result<(String, String), String> {
            let (_, service) = kiln.who().await.map_err(|error| error.to_string())?;
            Ok((user.principal().into(), service))
        }
        pub fn streamed(&self, _k: Peer<Kiln>) -> impl Iterator<Item = u32> {
            0..2
        }
    }
}

fn rpc(host: &RunningPeers) -> Connection {
    let (a, b) = plexus::pair();
    tokio::spawn(host.server().serve_as(b, Caller::new("user:ada")));
    let (conn, driver) = Connection::new(a, None);
    tokio::spawn(driver);
    conn
}
fn key(seed: u8) -> Identity {
    Identity::from_seed([seed; 32]).unwrap()
}
async fn worker(seed: u8, url: &str, controller: &str) -> RunningPeers {
    let mut host = PeerHost::new(kiln::Kiln(seed.into()), key(seed));
    host.server_mut().authorize_grants(|caller, _, _| {
        caller.principal() == "user:ada" || caller.principal().starts_with("key:")
    });
    host.server_mut().trust_forwarder(controller, "kiln.who");
    host.peer::<studio::Studio>("studio")
        .dial(url, controller)
        .unwrap();
    host.requires_peer("studio").unwrap();
    host.start().await.unwrap()
}

#[tokio::test]
async fn application_selects_the_injected_peer_independently_of_the_proxy() {
    tokio::time::timeout(Duration::from_secs(6), async {
        let local = key(1);
        let pin = local.principal().to_owned();
        let count = Arc::new(AtomicU32::new(0));
        let mut studio = PeerHost::new(studio::Studio(count.clone()), local);
        studio
            .peer::<kiln::Kiln>("north")
            .accept("wss://127.0.0.1:0", key(2).principal())
            .unwrap();
        studio
            .inject_peer::<kiln::Kiln>()
            .accept("wss://127.0.0.1:0", key(3).principal())
            .unwrap();
        studio.mount_peer("kiln", "north").unwrap();
        let host = studio.start().await.unwrap();
        let client = rpc(&host);
        let url = format!("wss://{}/", host.listen_addr("127.0.0.1:0").unwrap());
        assert!(host.is_ready());
        assert_eq!(client.call("studio.status", ()).await.unwrap().get(), "0");
        assert_eq!(
            client.call("studio.verify", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
        assert_eq!(
            client.call("studio.streamed", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
        assert_eq!(
            client.call("studio.inspect", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        let north = worker(2, &url, &pin).await;
        let south = worker(3, &url, &pin).await;
        north.ready().await;
        south.ready().await;
        for name in ["north", "kiln"] {
            while host.peer::<kiln::Kiln>(name).is_err() {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(
            serde_json::from_str::<(u32, String)>(
                client.call("studio.verify", ()).await.unwrap().get()
            )
            .unwrap(),
            (3, pin.clone())
        );
        assert_eq!(
            client.call("studio.kiln.who", ()).await.unwrap().get(),
            r#"[2,"user:ada"]"#
        );
        assert_eq!(
            serde_json::from_str::<(u32, String)>(
                client.call("studio.inspect", ()).await.unwrap().get()
            )
            .unwrap(),
            (3, pin.clone())
        );
        assert_eq!(
            serde_json::from_str::<(String, String)>(
                client.call("studio.identities", ()).await.unwrap().get()
            )
            .unwrap(),
            ("user:ada".into(), pin.clone())
        );
        let k = host.peer::<kiln::Kiln>("kiln").unwrap();
        assert_eq!(k.who().await.unwrap(), (3, pin.clone()));
        assert_eq!(
            k.sequence(3)
                .unwrap()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [0, 1, 2]
        );
        assert_eq!(
            k.echo(futures::stream::iter([Ok(vec![0, 255])]))
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap(),
            [0, 255]
        );
        assert_eq!(
            client
                .call("studio.verify", serde_json::json!({"k":"south"}))
                .await
                .unwrap_err()
                .code(),
            Some("invalid-params")
        );
        let tree = client.call("_tree", ()).await.unwrap();
        assert!(!tree.get().contains("south") && !tree.get().contains("north"));
        assert!(tree.get().contains("studio.kiln"));
        drop(south);
        while host.peer::<kiln::Kiln>("kiln").is_ok() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            client.call("studio.verify", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
        assert!(host.peer::<kiln::Kiln>("north").is_ok());
        let south = worker(3, &url, &pin).await;
        south.ready().await;
        while host.peer::<kiln::Kiln>("kiln").is_err() {
            tokio::task::yield_now().await;
        }
        assert!(client.call("studio.verify", ()).await.is_ok());
        assert_eq!(count.load(Ordering::SeqCst), 3);
    })
    .await
    .expect("peer sessions became ready");
}

#[tokio::test]
async fn required_binding_opens_only_after_pin_and_interface_verify() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let count = Arc::new(AtomicU32::new(0));
        let mut s = PeerHost::new(studio::Studio(count.clone()), key(1));
        s.inject_peer::<kiln::Kiln>()
            .required()
            .accept("wss://127.0.0.1:0", key(2).principal())
            .unwrap();
        let host = s.start().await.unwrap();
        let client = rpc(&host);
        let addr = host.listen_addr("127.0.0.1:0").unwrap();
        let url = format!("wss://{addr}/");
        use plexus::{
            peer::{Side, connect},
            tls::Dial,
        };
        let dial = Dial::new(&key(9), key(1).principal()).unwrap();
        assert!(dial.connect(&url).await.is_err());
        let (wire, pin) = Dial::new(&key(2), key(1).principal())
            .unwrap()
            .connect(&url)
            .await
            .unwrap();
        let mut wrong = plexus::Server::new();
        wrong.hello("other");
        assert!(
            connect(
                wire,
                Side::Dial,
                Caller::new(pin),
                Arc::new(wrong),
                studio::HASH
            )
            .await
            .is_err()
        );
        assert!(!host.is_ready());
        assert_eq!(
            client.call("studio.status", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
        let worker = worker(2, &url, key(1).principal()).await;
        host.ready().await;
        assert_eq!(
            serde_json::from_str::<(u32, String)>(
                client.call("studio.inspect", ()).await.unwrap().get()
            )
            .unwrap(),
            (2, key(1).principal().to_owned())
        );
        drop(worker);
        while host.is_ready() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            client.call("studio.status", ()).await.unwrap_err().code(),
            Some("unavailable")
        );
    })
    .await
    .unwrap();
}

#[test]
fn keystore_references_and_generated_contract() {
    let tmp = std::env::temp_dir().join(format!("llink8-{}", std::process::id()));
    let dir = tmp.as_path();
    std::fs::create_dir(dir).unwrap();
    std::fs::write(dir.join("studio.key"), [1; 32]).unwrap();
    std::fs::write(dir.join("north.pub"), format!("{}\n", key(2).principal())).unwrap();
    let keys = PeerKeys::new(dir);
    assert_eq!(
        keys.identity("keystore://studio").unwrap().principal(),
        key(1).principal()
    );
    assert_eq!(keys.pin("keystore://north").unwrap(), key(2).principal());
    assert!(keys.identity("keystore://../studio").is_err());
    assert!(keys.pin("keystore://studio").is_err());
    assert!(!studio::CLIENT_TS.contains("Peer<"));
    assert_eq!(
        <studio::Studio as plexus::Serve>::PEER_INJECTIONS,
        &[plexus::PeerDependency::of::<kiln::Kiln>()]
    );
    let ir: serde_json::Value = serde_json::from_str(studio::IR).unwrap();
    let method = ir["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == "verify")
        .unwrap();
    assert!(method["params"].as_array().is_none_or(Vec::is_empty));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn two_candidates_require_an_application_choice_before_listening() {
    let mut studio = PeerHost::new(studio::Studio(Default::default()), key(1));
    for (name, seed) in [("first", 2), ("second", 3)] {
        studio
            .peer::<kiln::Kiln>(name)
            .accept("wss://127.0.0.1:0", key(seed).principal())
            .unwrap();
    }
    let err = studio.start().await.err().expect("ambiguous wiring");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("kiln"));
}

#[tokio::test]
async fn missing_provider_only_blocks_methods_that_use_it() {
    let studio = PeerHost::new(studio::Studio(Default::default()), key(1))
        .start()
        .await
        .unwrap();
    let client = rpc(&studio);
    assert_eq!(client.call("studio.status", ()).await.unwrap().get(), "0");
    assert_eq!(
        client.call("studio.inspect", ()).await.unwrap_err().code(),
        Some("unavailable")
    );
}
