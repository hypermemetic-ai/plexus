# plexus

The one crate a Plexus activation depends on: the attribute, the types a method's signature names, and the runtime that serves and calls over any channel.

- `Server`: methods registered by name, untyped (a JSON body in; one value, a stream of values, or a failure out); `hello` says the interface hash, `describe(at, ir)` answers `<at>._ir`, `<at>._hash` and `_tree`. A stream writes only as far as its caller's credit; a cancel drops it. Many calls run at once on one connection.
- `Connection`: `call` (one answer; on a stream, every item read with credit granted as `stream` does, the last answered), `stream` (items, credit granted as they are read: a window of 256, topped up by 128; `cancel` when dropped), `ready` (the hello, checked against the hash expected). A failure is `Error::Failed` with its code, message and data.
- An activation served natively: `#[activation]` writes `serve(activation, server, at)` and `impl Serve`; `plexus::main(activation)` (feature `tokio`) is a process serving it over stdio, or with `--ws ADDR` on a WebSocket. The activation is moved to a thread of its own and held in an `Rc` (`served.rs`): its methods take `&self` with `RefCell` state, as in a component, so it need be `Send` but not `Sync`, while the `Server` stays `Send` on any executor.
- `Channel`: whole frames as bytes. `pair()` in memory; with the `tokio` feature, `stdio` (a frame a line: this process, or a child) and `ws` (a text message a frame: `connect`, `accept`, `listen`, `spawn`). Without it the crate builds for a component (`wasm32-unknown-unknown`).
- Frames are plexus-proto's; the behaviour matches `../plexus-rt`, the page's runtime, which calls this server in `tests/page.rs`.

```sh
cargo test --features tokio                     # in memory, a child over stdio, a WebSocket, and plexus-rt in Node (ignored, with why, without Node 22+)
cargo bench --features tokio --bench pace       # call.one_item and stream.10k.64B, in memory and over a loopback WebSocket
cargo build --target wasm32-unknown-unknown     # the core alone
cargo run --features tokio --bin kiln -- --ws 127.0.0.1:0   # the hand-registered kiln the tests call (it counts what `heat` made, which pacing is checked by)
```

Part of Plexus, built from scratch (tracker: LAYERS-9). Its sibling crates are expected beside it: `../<crate>`.

MIT.
