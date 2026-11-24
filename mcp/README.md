# Native Layers MCP bridge

`plexus-layers-mcp` exposes the methods of a native Layers-1 activation as MCP tools. It lives in the Layers repository beside the runtime and uses its native connection, interface description and caller checks. MCP clients speak stdio to the executable. A Layers application can also mount the library's Streamable HTTP endpoint.

## Start an application with MCP

```rust
use kiln::Kiln;
use plexus_layers_mcp::Mcp;

fn main() -> std::io::Result<()> {
    plexus::main([Kiln::default()]).mcp().run()
}
```

The application registers each activation under its declared namespace. Set `PLEXUS_MCP_TOKEN` to a secret bearer token, then run `cargo run --locked --example http` in this directory. A client connects to `http://127.0.0.1:3000/mcp` and sends `Authorization: Bearer <token>`. Calls run as `local:mcp-operator` under the service's native grants. Add `-- --ws 127.0.0.1:0` to serve the same activations over a native WebSocket as well. [Runnable main](examples/http.rs)

For several Rust types, use `plexus::activations![kiln, glaze]`. Keep the registry handle when another part of your application controls which services are available.

```rust
let app = plexus::main(plexus::activations![kiln, glaze]).mcp();
let roots = app.activations();
let service = tokio::spawn(app.run_async());

roots.insert(audit)?;
roots.replace(updated_kiln)?;
roots.remove("glaze")?;
service.await??;
```

Each edit applies to subsequent discovery requests and calls across listeners, including open native connections. The connection identifies the catalog. Native clients can bind to an individual activation by checking its current interface hash, for example `kiln._hash`. A call already running keeps its activation until it finishes. Inserting a duplicate namespace returns `AlreadyExists`. Replacing or removing an absent namespace returns `NotFound`. Use `.configure(...)` to set shared grants and `.mcp_with(...)` for your own listener and caller verifier. [Live registry and HTTP tests](tests/app.rs)

## Run a service as an MCP child

From this directory in the native sibling checkouts:

```sh
cargo build --locked
cargo build --manifest-path ../../plexus-examples/Cargo.toml -p studio -p glaze
./target/debug/plexus-layers-mcp -- ../../plexus-examples/target/debug/studio
```

Configure an MCP client to launch that final command, using absolute paths in its configuration. The bridge starts the child directly and reserves stdout for MCP. A plain child stdio connection has no authenticated user. For a service that requires a verified principal, use a pinned native WSS connection:

```sh
plexus-layers-mcp --place 'studio[tenant=acme]' \
  --wss "$STUDIO_WSS_URL" --key-file "$CLIENT_KEY_FILE" --server-key "$STUDIO_PRINCIPAL"
```

Each stdio client owns its process and upstream identity. The optional `--place` selects a mount and its descendants. Context keys stay on the native dispatch address, so a client sees the stable tool name `studio.kiln.heat` and the service receives `studio[tenant=acme].kiln.heat`.

## Mount HTTP

Mount `plexus_layers_mcp::http::router(server, origins, verify_request)` in the application's Axum router. It serves `/mcp` using stateless Streamable HTTP. The host's async verifier receives the original HTTP request parts and returns the authenticated `Option<plexus::Caller>`. Returning `None` produces HTTP 401. Each request uses its own caller for native discovery and dispatch. Configure accepted browser origins with `HttpOrigins::allow_origin`. With `HttpOrigins::default()`, a native client can reach a loopback listener.

The application supplies its session or bearer verifier, TLS termination and any OAuth discovery routes. The executable's fixed WSS key is for one MCP client. The HTTP adapter uses the host-verified caller on each request. [Runnable HTTP integration](tests/http.rs)

## Tool contract

- `tools/list` reads native `_tree` and the reflected activations' `_ir`. Nested tools have qualified names. Parameter types, defaults, records and wire enum names come from the native interface. Discovery describes an interface. Native grants decide whether the caller can execute each method.
- `tools/call` accepts the declared arguments. The runtime supplies the caller. A method taking native input also accepts an optional `_input` array, consumed under native credit. Omitting it ends the input stream immediately.
- A unary answer uses `structuredContent.value`. A finite stream returns an ordered `structuredContent.items` array. Call failures set `isError` and carry the native `code`, `message` and `data`. The MCP SDK provides a text representation alongside structured content.
- A call has a 60-second deadline. Stream collection is bounded to 4,096 items and 4 MiB of native JSON item bytes. Reaching a limit returns a tool error and drops the native call. MCP cancellation on stdio or disconnecting a pending HTTP request releases the native stream. Live notification feeds require a different result contract.

## Verify

```sh
cargo test --locked
```

The tests run a real MCP client, child process, kiln activation and HTTP listener. They exercise input, nested discovery, application failures, concurrent callers with different grants and cancellation.

For the separate authenticated Studio and remote kiln walkthrough, build the sibling Studio's `studio-kiln` binary with its `tls` feature and run:

```sh
cargo build --manifest-path ../../plexus-examples/Cargo.toml -p studio --bin studio-kiln --features tls
python3 tests/studio_wss.py ./target/debug/plexus-layers-mcp ../../plexus-examples/target/debug/studio-kiln
```

The walkthrough generates temporary software keys, starts both services on loopback, and checks the remote kiln's audit after the operator and viewer call the same tool.
