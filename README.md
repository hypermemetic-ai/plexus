# plexus

The one crate a Plexus activation depends on: the attribute, the types a method's signature names, and the runtime that serves and calls over any channel.

- `Server`: methods registered by name, untyped (a JSON body in; one value, a stream of values, or a failure out); `hello` says the interface hash, `describe(at, ir)` answers `<at>._ir`, `<at>._hash` and `_tree`. A stream writes only as far as its caller's credit; a cancel drops it. Many calls run at once on one connection.
- `Connection`: `call` (one answer), `stream` (items, credit granted as they are read, `cancel` when dropped), `ready` (the hello, checked against the hash expected). A failure is `Error::Failed` with its code, message and data.
- `Channel`: whole frames as bytes. `pair()` in memory; with the `tokio` feature, `stdio` (a frame a line: this process, or a child) and `ws` (a text message a frame: `connect`, `accept`, `listen`, `spawn`). Without it the crate builds for a component (`wasm32-unknown-unknown`).
- Frames are plexus-proto's; the behaviour matches `../plexus-rt`, the page's runtime, which calls this server in `tests/page.rs`.

```sh
cargo test --features tokio                     # in memory, a child over stdio, a WebSocket, and plexus-rt in Node (ignored, with why, without Node 22+)
cargo bench --features tokio --bench pace       # call.one_item and stream.10k.64B, in memory and over a loopback WebSocket
cargo build --target wasm32-unknown-unknown     # the core alone
cargo run --features tokio --bin kiln -- --ws 127.0.0.1:0   # the hand-registered kiln the tests call
```

Part of Plexus, built from scratch (tracker: LAYERS-9). Its sibling crates are expected beside it: `../<crate>`.

MIT.
