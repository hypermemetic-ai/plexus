//! An MCP client starts the installed-shaped bridge, which starts the Layers child.
//! This harness also serves the test studio so neither process depends on PATH.
use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::{error::Error, sync::Arc, time::Duration};

// The pending stream owns this guard until native dispatch releases it.
struct Holding(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for Holding {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[plexus::activation]
mod studio {
    use futures::Stream;
    use kiln::kiln::Kiln;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    pub struct Studio {
        kiln: Kiln,
        holding: Arc<AtomicBool>,
    }

    impl Studio {
        pub fn name(&self) -> String {
            "studio".into()
        }

        pub fn holding(&self) -> bool {
            self.holding.load(Ordering::SeqCst)
        }

        pub fn hold(&self) -> impl Stream<Item = u32> {
            self.holding.store(true, Ordering::SeqCst);
            let guard = super::Holding(self.holding.clone());
            futures::stream::once(async move {
                let _guard = guard;
                futures::future::pending::<u32>().await
            })
        }

        #[child]
        pub fn kiln(&self) -> &Kiln {
            &self.kiln
        }
    }
}

fn call(name: &'static str, args: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name).with_arguments(args.as_object().unwrap().clone())
}

async fn round_trip() -> Result<(), Box<dyn Error>> {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_plexus-layers-mcp"));
    command
        .arg("--")
        .arg(std::env::current_exe()?)
        .arg("--fixture");
    // A fresh in-memory kiln lets the assertions run against the same starting state.
    command.env_remove("PLEXUS_DATA");
    let client = ().serve(TokioChildProcess::new(command)?).await?;
    let tools = client.list_all_tools().await?;
    assert!(tools.iter().any(|t| t.name == "studio.name"));
    let heat = tools
        .iter()
        .find(|t| t.name == "studio.kiln.heat")
        .expect("nested heat tool");
    let schema = heat.input_schema.as_ref();
    assert_eq!(schema["properties"]["cone"]["type"], "integer");
    assert_eq!(schema["properties"]["hours"]["type"], "number");

    let counted = client
        .call_tool(call("upload.count", json!({"_input": [1, 3, 5]})))
        .await?;
    assert_eq!(counted.structured_content, Some(json!({"value":3})));
    let empty = client.call_tool(call("upload.count", json!({}))).await?;
    assert_eq!(empty.structured_content, Some(json!({"value":0})));

    let fired = client
        .call_tool(call(
            "studio.kiln.fire",
            json!({"cone":6,"hours":1,"loaded-by":"Mira"}),
        ))
        .await?;
    assert_ne!(fired.is_error, Some(true));
    let row = fired.structured_content.expect("structured firing")["value"].clone();
    assert_eq!(row["cone"], 6);
    assert_eq!(row["loaded-by"], "Mira");
    let read = client
        .call_tool(call("studio.kiln.get", json!({"id":row["id"]})))
        .await?;
    assert_eq!(read.structured_content, Some(json!({"value":row})));

    let refused = client
        .call_tool(call("studio.kiln.get", json!({"id":999})))
        .await?;
    assert_eq!(refused.is_error, Some(true));
    let failure = refused.structured_content.expect("structured failure");
    assert_eq!(failure["code"], "not-found");
    assert_eq!(failure["data"], 999);
    assert!(!failure["message"].as_str().unwrap().is_empty());

    let stream = client
        .call_tool(call("studio.kiln.heat", json!({"cone":6,"hours":1})))
        .await?;
    assert_ne!(stream.is_error, Some(true));
    let body = stream.structured_content.expect("structured stream");
    let readings = body["items"].as_array().expect("ordered readings");
    assert_eq!(readings.len(), 13);
    for (i, reading) in readings.iter().enumerate() {
        assert_eq!(reading["minute"], (i * 5) as u64);
    }
    assert_eq!(readings[0]["phase"], "climbing");
    assert_eq!(readings[12]["phase"], "cooling");
    use rmcp::model::{ClientRequest, Request};
    use rmcp::service::PeerRequestOptions;
    let pending = client
        .send_cancellable_request(
            ClientRequest::CallToolRequest(Request::new(call("studio.hold", json!({})))),
            PeerRequestOptions::no_options(),
        )
        .await?;
    for expected in [true, false] {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let observed = client.call_tool(call("studio.holding", json!({}))).await?;
                if observed.structured_content == Some(json!({"value":expected})) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<(), rmcp::ServiceError>(())
        })
        .await??;
        if expected {
            client
                .notify_cancelled(rmcp::model::CancelledNotificationParam::new(
                    Some(pending.id.clone()),
                    None,
                ))
                .await?;
        }
    }
    drop(pending);
    client.cancel().await?;
    eprintln!(
        "stdio bridge passed: nested schema, kiln firing and readback, typed not-found, 13 ordered readings, native stream cancellation"
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args().nth(1).as_deref() == Some("--fixture") {
        let mut server = plexus::Server::new();
        plexus::serve(studio::Studio::default(), &mut server);
        // Register a native input method alongside the macro-defined Studio.
        // Canonical IR has the same advertised hash as the server's description.
        let upload = plexus_ir::Ir::read(
            r#"{"ir":"plexus/0","activation":"upload","types":[],"methods":[{"name":"count","params":[],"input":"u32","returns":"u32"}]}"#,
        )?;
        server.describe("upload", &upload.json())?;
        server.method_with_caller("upload.count", |_, mut cx| async move {
            use futures::TryStreamExt;
            let mut source = cx.take_input(plexus::__private::decode_input::<u32>)?;
            let mut count = 0u32;
            while source.try_next().await?.is_some() {
                count += 1;
            }
            plexus::raw(&count)
        });
        server.accepts_input("upload.count");
        Arc::new(server).serve(plexus::stdio::stdio()).await?;
    } else {
        tokio::time::timeout(Duration::from_secs(30), round_trip()).await??;
    }
    Ok(())
}
