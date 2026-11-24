//! The fluent listener serves the registry owned by the application.
use std::io;
use std::time::Duration;

use axum::http::{StatusCode, header::AUTHORIZATION};
use kiln::Kiln;
use plexus::Caller;
use plexus_layers_mcp::{Mcp, http::HttpOrigins};
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[plexus::activation(roles(operator = ["read"]))]
mod meter {
    pub struct Meter {
        pub value: u32,
    }
    impl Meter {
        pub fn read(&self) -> u32 {
            self.value
        }
    }
}

fn post(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: Value,
) -> reqwest::RequestBuilder {
    client
        .post(url)
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
}

async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: Value) -> Value {
    let response = post(client, url, method, params)
        .bearer_auth("app-operator")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

async fn tools(client: &reqwest::Client, url: &str) -> Vec<String> {
    let response = rpc(client, url, "tools/list", json!({})).await;
    response["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect()
}

async fn read(client: &reqwest::Client, url: &str) -> Value {
    rpc(
        client,
        url,
        "tools/call",
        json!({"name":"meter.read","arguments":{}}),
    )
    .await
}

#[tokio::test]
async fn fluent_application_serves_live_registry_and_caller() {
    let socket = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let url = format!("http://{}/mcp", socket.local_addr().unwrap());
    let app = plexus::main([Kiln::default()])
        .configure(|server| {
            server.authorize_grants(|caller, grant, roles| {
                caller.principal() == "meter-reader"
                    && grant == "meter.read"
                    && roles.contains(&"operator")
            });
        })
        .mcp_with(socket, HttpOrigins::default(), |parts| async move {
            let valid = parts
                .headers
                .get(AUTHORIZATION)
                .is_some_and(|v| v == "Bearer app-operator");
            valid.then(|| Caller::new("meter-reader"))
        });
    let registry = app.activations();
    let running = tokio::spawn(app.run_async());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let anonymous = post(&client, &url, "tools/list", json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let initial = tools(&client, &url).await;
    assert!(initial.iter().any(|n| n == "kiln.log"));
    assert!(!initial.iter().any(|n| n == "meter.read"));
    registry.insert(meter::Meter { value: 3 }).unwrap();
    assert!(tools(&client, &url).await.iter().any(|n| n == "meter.read"));
    assert_eq!(
        read(&client, &url).await["result"]["structuredContent"]["value"],
        3
    );
    registry.replace(meter::Meter { value: 9 }).unwrap();
    assert_eq!(
        read(&client, &url).await["result"]["structuredContent"]["value"],
        9
    );
    registry.remove("meter").unwrap();
    assert!(!tools(&client, &url).await.iter().any(|n| n == "meter.read"));
    assert!(read(&client, &url).await["error"].is_object());
    running.abort();
    let _ = running.await;
}

#[test]
fn missing_default_bearer_is_reported_before_any_listener_starts() {
    const CHILD: &str = "PLEXUS_MCP_MISSING_TOKEN_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "missing_default_bearer_is_reported_before_any_listener_starts",
            ])
            .env(CHILD, "1")
            .env_remove("PLEXUS_MCP_TOKEN")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stdout)
        );
        return;
    }
    let app = plexus::main([Kiln::default()])
        .with_listener(|_| async {
            panic!("other listener was polled before MCP configuration was checked")
        })
        .mcp();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = runtime.block_on(app.run_async()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("PLEXUS_MCP_TOKEN"));
}
