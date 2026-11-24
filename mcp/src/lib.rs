//! Native Layers tools over Model Context Protocol. One handler speaks through
//! exactly one upstream connection, with the caller that connection established.
mod app;
pub use app::{Mcp, McpListener};
mod catalog;
pub mod http;

use catalog::Entry;
use futures::{StreamExt, stream};
use plexus::{Connection, Place, Raw};
use plexus_ir::Ir;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_ITEMS: usize = 4096;
const MAX_BYTES: usize = 4 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct LayersMcp {
    connection: Connection,
    // A host-configured path, with its context-free name for matching _tree.
    root: Option<(String, String)>,
}

impl LayersMcp {
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            root: None,
        }
    }

    /// Expose one mount and its descendants, optionally binding Layers context
    /// keys (for example studio[tenant=acme]). The native server validates keys.
    pub fn at(connection: Connection, place: &str) -> Result<Self, String> {
        let canonical = catalog::public_mount(place)?.ok_or("reserved Layers mount")?;
        Ok(Self {
            connection,
            root: Some((place.into(), canonical)),
        })
    }

    fn address(&self, place: &str) -> Option<String> {
        let Some((bound, canonical)) = &self.root else {
            return Some(place.into());
        };
        let tail = place.strip_prefix(canonical)?;
        (tail.is_empty() || tail.starts_with('.')).then(|| format!("{bound}{tail}"))
    }

    async fn discover(&self) -> Result<BTreeMap<String, Entry>, ErrorData> {
        self.connection.ready().await.map_err(internal)?;
        let tree = self.connection.call("_tree", ()).await.map_err(internal)?;
        let places: Vec<Place> = serde_json::from_str(tree.get()).map_err(internal)?;
        let mut all = BTreeMap::new();
        for place in places {
            if catalog::public_mount(&place.place)
                .map_err(internal)?
                .as_deref()
                != Some(&place.place)
            {
                continue;
            }
            let Some(address) = self.address(&place.place) else {
                continue;
            };
            let source = match self.connection.call(&format!("{address}._ir"), ()).await {
                Ok(source) => source,
                Err(e)
                    if matches!(
                        e.code(),
                        Some(
                            "forbidden"
                                | "unauthenticated"
                                | "unavailable"
                                | "not-found"
                                | "invalid-context"
                        )
                    ) =>
                {
                    continue;
                }
                Err(e) => return Err(internal(e)),
            };
            let ir = Ir::read(source.get()).map_err(internal)?;
            if place.hash.is_some_and(|hash| hash != ir.hash()) {
                return Err(internal("Layers interface changed during discovery"));
            }
            for entry in catalog::tools(&address, &ir).map_err(internal)? {
                if all.insert(entry.tool.name.to_string(), entry).is_some() {
                    return Err(internal("duplicate MCP tool name"));
                }
            }
        }
        Ok(all)
    }

    async fn invoke(&self, request: CallToolRequestParams) -> Result<CallToolResult, ErrorData> {
        let entry = self
            .discover()
            .await?
            .remove(request.name.as_ref())
            .ok_or_else(|| ErrorData::invalid_params("unknown Layers tool", None))?;
        let mut args = request.arguments.unwrap_or_default();
        let properties = entry
            .tool
            .input_schema
            .get("properties")
            .and_then(Value::as_object);
        if args
            .keys()
            .any(|key| !properties.is_some_and(|p| p.contains_key(key)))
        {
            return Err(ErrorData::invalid_params("unexpected tool argument", None));
        }
        let input = if entry.input {
            match args.remove("_input") {
                None => Some(Vec::new()),
                Some(Value::Array(values)) if values.len() <= MAX_ITEMS => Some(values),
                _ => {
                    return Err(ErrorData::invalid_params(
                        "_input must be a bounded array",
                        None,
                    ));
                }
            }
        } else {
            None
        };
        let address = &entry.address;
        let input = input.map(|items| stream::iter(items.into_iter().map(|v| plexus::raw(&v))));
        let opened = match input {
            Some(input) => self.connection.stream_with_input(address, args, input),
            None => self.connection.stream(address, args),
        };
        let mut stream = match opened {
            Ok(stream) => stream,
            Err(error) => return Ok(native_error(error)),
        };
        let mut values = Vec::new();
        let mut bytes = 0;
        while let Some(item) = stream.next().await {
            let raw = match item {
                Ok(raw) => raw,
                Err(error) => return Ok(native_error(error)),
            };
            bytes += raw.get().len();
            if bytes > MAX_BYTES || values.len() >= MAX_ITEMS {
                return Ok(failure(
                    "result-limit",
                    "Layers result exceeded the MCP collection limit",
                    Value::Null,
                ));
            }
            values.push(serde_json::from_str::<Value>(raw.get()).map_err(internal)?);
        }
        let response = if entry.streaming {
            json!({"items": values})
        } else {
            json!({"value": values.pop().unwrap_or(Value::Null)})
        };
        Ok(CallToolResult::structured(response))
    }
}

fn internal(e: impl std::fmt::Display) -> ErrorData {
    ErrorData::internal_error(e.to_string(), None)
}

fn failure(code: &str, message: &str, data: Value) -> CallToolResult {
    CallToolResult::structured_error(json!({"code":code, "message":message, "data":data}))
}

fn native_error(error: plexus::Error) -> CallToolResult {
    match error {
        plexus::Error::Failed(f) => failure(
            &f.code,
            &f.message,
            f.data
                .as_ref()
                .map(Raw::get)
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null),
        ),
        other => failure("upstream-error", &other.to_string(), Value::Null),
    }
}

impl ServerHandler for LayersMcp {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info = Implementation::new("plexus-layers-mcp", env!("CARGO_PKG_VERSION"));
        info
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.is_some_and(|r| r.cursor.is_some()) {
            return Err(ErrorData::invalid_params(
                "unexpected pagination cursor",
                None,
            ));
        }
        let tools = tokio::select! {
            _ = ctx.ct.cancelled() => return Err(internal("discovery cancelled")),
            result = tokio::time::timeout(DEADLINE, self.discover()) =>
                result.map_err(|_| internal("Layers discovery timed out"))??,
        };
        Ok(ListToolsResult::with_all_items(
            tools.into_values().map(|e| e.tool).collect(),
        ))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        // Dropping the invocation drops the native Items and cancels both directions.
        let result = tokio::select! {
            biased;
            _ = ctx.ct.cancelled() => failure("cancelled", "MCP caller cancelled the call", Value::Null),
            result = tokio::time::timeout(DEADLINE, self.invoke(request)) =>
                result.unwrap_or_else(|_| Ok(failure("timeout", "Layers call timed out", Value::Null)))?,
        };
        Ok(result.into())
    }
}
