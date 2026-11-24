//! Smoke the mounted HTTP endpoint across a real listener with a native grant.
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode, header};
use plexus::{Caller, Server};
use plexus_layers_mcp::http::{HttpOrigins, router};
use serde_json::{Value, json};

use tokio::sync::Notify;
struct OnDrop(Arc<Notify>);
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.notify_one()
    }
}

#[plexus::activation]
mod studio {
    use super::OnDrop;
    use kiln::kiln::Kiln;
    use std::sync::Arc;
    use tokio::sync::Notify;
    #[derive(Default)]
    pub struct Studio {
        kiln: Kiln,
        pub started: Arc<Notify>,
        pub stopped: Arc<Notify>,
    }
    impl Studio {
        pub async fn wait(&self) -> u32 {
            let _guard = OnDrop(self.stopped.clone());
            self.started.notify_one();
            std::future::pending().await
        }
        #[child]
        pub fn kiln(&self) -> &Kiln {
            &self.kiln
        }
    }
}

struct Peer {
    client: reqwest::Client,
    endpoint: String,
}

impl Peer {
    fn request(&self, token: &str, method: &str, params: Value) -> reqwest::RequestBuilder {
        self.client
            .post(&self.endpoint)
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-11-25")
            .bearer_auth(token)
            .json(&json!({"jsonrpc":"2.0", "id":1,"method":method,"params":params}))
    }

    async fn rpc(&self, token: &str, method: &str, params: Value) -> Value {
        let response = self.request(token, method, params).send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(!headers.contains_key("mcp-session-id"));
        serde_json::from_str(&body).expect("stateless JSON response")
    }
}

fn identity(headers: &HeaderMap) -> Option<Caller> {
    match headers.get(header::AUTHORIZATION)?.to_str().ok()? {
        "Bearer access-alice" => Some(Caller::new("alice")),
        "Bearer access-bob" => Some(Caller::new("bob")),
        _ => None,
    }
}

#[tokio::test]
async fn authenticated_stateless_requests_retain_their_own_native_caller() {
    let mut server = Server::new();
    let studio = studio::Studio::default();
    let started = studio.started.clone();
    let stopped = studio.stopped.clone();
    studio::serve(studio, &mut server, "studio");
    server.require_grant("studio.kiln.log".into(), "read_log", &["operator"]);
    server
        .authorize_grants(|caller, grant, _| caller.principal() == "alice" && grant == "read_log");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let origins = HttpOrigins::default()
        .allow_origin(origin.parse().unwrap())
        .unwrap();
    let app = router(Arc::new(server), origins, |parts| async move {
        identity(&parts.headers)
    });
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let peer = Peer {
        client: reqwest::Client::new(),
        endpoint: format!("{origin}/mcp"),
    };
    let init = json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"native-client","version":"1"}});
    let reply = peer.rpc("access-alice", "initialize", init).await;
    assert!(
        reply["result"]["capabilities"]["tools"].is_object(),
        "{reply}"
    );
    let reply = peer
        .request("access-bob", "tools/list", json!({}))
        .header(header::ORIGIN, &origin)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let tools = reply["result"]["tools"]
        .as_array()
        .expect("list of discovered tools");
    let name = tools
        .iter()
        .filter_map(|t| t["name"].as_str())
        .find(|name| name.ends_with("log"))
        .expect("kiln log tool");
    let call = json!({"name":name,"arguments":{}});
    let (yes, no) = tokio::join!(
        peer.rpc("access-alice", "tools/call", call.clone()),
        peer.rpc("access-bob", "tools/call", call)
    );
    assert!(
        yes["result"].is_object() && yes["result"]["isError"] != true,
        "{yes}"
    );
    assert!(no.to_string().contains("forbidden"), "{no}");
    let spoof = peer
        .rpc(
            "access-bob",
            "tools/call",
            json!({"name":name,"arguments":{"principal":"alice"}}),
        )
        .await;
    assert!(
        spoof["result"]["isError"] == true || spoof["error"].is_object(),
        "{spoof}"
    );

    let unknown = peer
        .request("not-a-credential", "tools/list", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::UNAUTHORIZED);
    let absent = peer.client.post(&peer.endpoint).send().await.unwrap();
    assert_eq!(absent.status(), StatusCode::UNAUTHORIZED);
    let foreign = peer
        .request("access-alice", "tools/list", json!({}))
        .header(header::ORIGIN, "https://other.test")
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::FORBIDDEN);
    let host = peer
        .request("access-alice", "tools/list", json!({}))
        .header(header::HOST, "other.test")
        .send()
        .await
        .unwrap();
    assert_eq!(host.status(), StatusCode::FORBIDDEN);
    // Dropping an outstanding HTTP request must drop the native method future.
    let request = peer.request(
        "access-alice",
        "tools/call",
        json!({"name":"studio.wait","arguments":{}}),
    );
    let pending = tokio::spawn(async move { request.send().await });
    let limit = std::time::Duration::from_secs(5);
    tokio::time::timeout(limit, started.notified())
        .await
        .expect("native method started");
    pending.abort();
    let _ = pending.await;
    tokio::time::timeout(limit, stopped.notified())
        .await
        .expect("HTTP disconnect cancelled the native method");
    task.abort();
}
