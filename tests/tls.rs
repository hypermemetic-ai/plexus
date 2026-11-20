//! Mutually pinned TLS 1.3 under the same WebSocket channel as loopback WS.

mod common;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use plexus::tls::{Dial, Identity, Listen};
use plexus::{Channel, Connection, Server};
use tokio::net::TcpListener;
use tokio::time::timeout;

// Public output from Synapse's `_key show` for these disposable seeds.
const SERVER: &str = "key:ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c";
const CALLER: &str = "key:fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618";
const CALLER_FILE: &str = "PLEXUS_TLS_TEST_CALLER_FILE";
const URL: &str = "PLEXUS_TLS_TEST_URL";

fn seed_file(folder: &Path, name: &str, seed: [u8; 32]) -> PathBuf {
    let path = folder.join(name);
    std::fs::write(&path, format!("{}\n", STANDARD.encode(seed))).unwrap();
    path
}

struct TestFiles(PathBuf);

impl TestFiles {
    fn new(name: &str) -> Self {
        let folder = std::env::temp_dir().join(format!("plexus-tls-{name}-{}", std::process::id()));
        std::fs::create_dir(&folder).unwrap();
        Self(folder)
    }
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn the_kiln_over_wss_answers_as_over_ws() {
    let (server, caller) = (Identity::generate().unwrap(), Identity::generate().unwrap());
    let listen = Listen::new(&server, [caller.principal()]).unwrap();
    let bound = listen
        .spawn(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(common::activation::kiln().0),
        )
        .await
        .unwrap();
    let (channel, peer) = Dial::new(&caller, server.principal())
        .unwrap()
        .connect(&format!("wss://{bound}/"))
        .await
        .unwrap();
    assert_eq!(peer, server.principal());
    let (conn, driving) = Connection::new(channel, Some(common::activation::HASH));
    tokio::spawn(driving);
    common::all(&conn).await;
}

#[test]
fn from_seeds_and_synapse_key_files() {
    assert_eq!(Identity::from_seed([7; 32]).unwrap().principal(), SERVER);
    assert_eq!(Identity::from_seed([9; 32]).unwrap().principal(), CALLER);
    let folder = TestFiles::new("formats");
    let path = seed_file(&folder.0, "key", [7; 32]);
    assert_eq!(Identity::from_key_file(&path).unwrap().principal(), SERVER);

    let seed = [0xff; 32];
    let expected = Identity::from_seed(seed).unwrap();
    for bytes in [
        seed.to_vec(),
        format!(" {}\r\n", URL_SAFE_NO_PAD.encode(seed)).into_bytes(),
    ] {
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            Identity::from_key_file(&path).unwrap().principal(),
            expected.principal()
        );
    }

    let invalid = "bad and private file contents go here";
    std::fs::write(&path, invalid).unwrap();
    let error = Identity::from_key_file(&path)
        .err()
        .expect("file is refused");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!format!("{error:?} {error}").contains(invalid));
}

#[tokio::test]
async fn key_file_peers_read_each_others_principal_across_processes() {
    let folder = TestFiles::new("process");
    let server_path = seed_file(&folder.0, "server.key", [7; 32]);
    let caller_path = seed_file(&folder.0, "caller.key", [9; 32]);
    let listen = Listen::new(&Identity::from_key_file(server_path).unwrap(), [CALLER]).unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", "key_file_child", "--nocapture", "--ignored"])
        .env(CALLER_FILE, caller_path)
        .env(
            URL,
            format!("wss://{}/plexus?probe=1", tcp.local_addr().unwrap()),
        )
        .kill_on_drop(true);

    timeout(Duration::from_secs(10), async {
        let (result, accepted) = tokio::join!(child.output(), async {
            listen.accept(tcp.accept().await.unwrap().0).await
        });
        let output = result.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let (_, peer) = accepted.unwrap();
        assert_eq!(peer, CALLER);
    })
    .await
    .expect("child connected and reported the server principal");
}

#[tokio::test]
#[ignore = "run in its own process by the key-file interoperability test"]
async fn key_file_child() {
    let Some(file) = std::env::var_os(CALLER_FILE) else {
        return;
    };
    let dial = Dial::new(&Identity::from_key_file(file).unwrap(), SERVER).unwrap();
    let (_, peer) = dial.connect(&std::env::var(URL).unwrap()).await.unwrap();
    assert_eq!(peer, SERVER);
}

#[tokio::test]
async fn a_server_with_another_key_is_refused_before_a_frame() {
    let (server, caller, other) = (
        Identity::generate().unwrap(),
        Identity::generate().unwrap(),
        Identity::generate().unwrap(),
    );
    let listen = Listen::new(&server, [caller.principal()]).unwrap();
    let dial = Dial::new(&caller, other.principal()).unwrap();
    let (dialed, accepted) = attempt(&dial, &listen).await;
    let err = dialed.err().expect("the caller refuses").to_string();
    assert!(
        err.contains(server.principal()),
        "names the key offered: {err}"
    );
    assert!(
        err.contains(other.principal()),
        "names the key expected: {err}"
    );
    assert!(accepted.is_err(), "the server gets no channel");
}

#[tokio::test]
async fn a_caller_the_server_does_not_accept_is_refused_before_dispatch() {
    let (server, caller, stranger) = (
        Identity::generate().unwrap(),
        Identity::generate().unwrap(),
        Identity::generate().unwrap(),
    );
    let listen = Listen::new(&server, [caller.principal()]).unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://{}/", tcp.local_addr().unwrap());
    let dial = Dial::new(&stranger, server.principal()).unwrap();
    let mut rpc = Server::new();
    rpc.hello("authenticated");
    rpc.method("forbidden", |_| async {
        panic!("an unauthenticated call ran")
    });
    let rpc = Arc::new(rpc);

    let (dialed, accepted) = timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                let (channel, _) = dial.connect(&url).await?;
                let (conn, driving) = Connection::new(channel, None);
                tokio::spawn(driving);
                let _ = conn.call("forbidden", ()).await;
                Ok::<_, io::Error>(())
            },
            async {
                let (channel, _) = listen.accept(tcp.accept().await.unwrap().0).await?;
                rpc.serve(channel).await
            }
        )
    })
    .await
    .expect("the handshake completes");

    let refused = accepted.expect_err("the server refuses");
    assert!(
        refused.to_string().contains(stranger.principal()),
        "names the key offered: {refused}"
    );
    assert!(dialed.is_err(), "the caller gets no channel");
}

async fn attempt(
    dial: &Dial,
    listen: &Listen,
) -> (io::Result<(Channel, String)>, io::Result<(Channel, String)>) {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("wss://{}/", tcp.local_addr().unwrap());
    timeout(Duration::from_secs(10), async {
        tokio::join!(dial.connect(&url), async {
            listen.accept(tcp.accept().await.unwrap().0).await
        })
    })
    .await
    .expect("the handshake completes")
}

#[tokio::test]
async fn a_plain_address_is_not_dialed() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let me = Identity::generate().unwrap();
    let err = Dial::new(&me, me.principal())
        .unwrap()
        .connect(&format!("ws://{}/", tcp.local_addr().unwrap()))
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    assert!(
        timeout(Duration::from_millis(50), tcp.accept())
            .await
            .is_err()
    );
}
