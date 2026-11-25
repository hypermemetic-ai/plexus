# Generated-client watch ladder

Run one Node process against a fresh native Rust fixture on loopback. Every session creates a distinct Ed25519 identity, establishes a separate TLS 1.3 WebSocket, and verifies the fixture public key before sending the HTTP upgrade. The runner enrolls ephemeral principals on the fixture's stdin and generates the TypeScript client from the running fixture's IR.

With `WATCH_NODE_LIVE=1`, the runner compiles the production `plexus-ui/host` and a renderless Svelte component. Each session has its own component context, scoped generated service and `live(() => Tickets.list())` effect. It verifies every initial and changed 50-row authorized page, including ordering, organization, sequence and row status. Disposing every effect must return the native fixture's subscription count to zero. The direct mode exercises the generated client's `.watch()` iterator and its `return()` method.

From the benchmark directory, with sibling `plexus-{ui,rt,gen}` checkouts, Node 24+, and OpenSSL installed:

```sh
cargo build --release --locked --bin watch-capacity
cargo build --locked --manifest-path ../../../plexus-gen/Cargo.toml
(cd ../../../plexus-rt && npm ci && npm run build)
(cd ../../../plexus-ui && npm ci)
WATCH_NODE_LIVE=1 node tests/generated-clients/ladder.mjs 1 10 100 1000
```

The prepared runtime can also be supplied with `WATCH_NODE_CODEC` (codec WASM), `WATCH_NODE_WS` (Node ws package), and `WATCH_NODE_UI_MODULES` (the plexus-ui node_modules directory). `WATCH_NODE_FIXTURE` and `WATCH_NODE_GEN` accept built executable paths. Leave `WATCH_NODE_LIVE` unset for the direct iterator path. Each rung uses up to ten organizations and five writes per organization, with a verified delivery barrier after each wave. Set `WATCH_NODE_ROUNDS` for 1–100 waves.

The JSONL reports application-frame bytes in both directions during the update window. The throughput denominator includes issuing the writes, receiving acknowledgements, decoding page values, and validating each result. TLS/WebSocket headers, initial pages and cancellation traffic fall outside that byte count. The renderless client creates no browser tabs and measures no DOM layout or painting. These short barrier-driven runs measure the whole client/fixture path. Use the native server ladder for steady-state capacity.

The recorded renderless run used Node v24.20.0 on the Ryzen host. Rungs 1 and 10 used CPU 23, and rungs 100 and 1,000 shared CPUs 22–23 between Node and the fixture. Other experiments ran on the host. See [renderless-results.jsonl](renderless-results.jsonl).
