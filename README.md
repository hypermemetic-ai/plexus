# plexus

The one crate a Plexus activation depends on: the attribute, the types a method's signature names, and the runtime that serves and calls over any channel.

- `Server`: methods registered by name, untyped (a JSON body in; one value, a stream of values, or a failure out); `hello` says the interface hash, `describe(at, ir)` answers `<at>._ir` and `<at>._hash`, and `_tree` lists every place with its hash (`[{"place": "studio.kiln", "hash": "…"}]`; a child that cannot be reached is listed with none). A stream writes only as far as its caller's credit; a cancel drops it. Many calls run at once on one connection.
- A child across a channel: `mount(at, Box<dyn Activation>)` passes every call under `<at>.` on to it, its `_ir` and `_hash` too, and asks it for its places at each `_tree`. A `Remote` is one: an activation at a place on the far end of a `Connection` (`Remote::new`), a child process over its stdio (`Remote::child`) or a WebSocket (`Remote::ws`); the last two run their connection on a thread of their own, so they need no runtime. A call passed on (`Connection::pass`) opens with credit for one item and is granted more only as its caller grants it, so nothing is held between beyond what the caller allowed; a cancel is passed on; when the connection closes, every call under it fails `unavailable`, and the rest of the server is untouched.
- `Connection`: `call` (one answer; on a stream, every item read with credit granted as `stream` does, the last answered), `stream` (items, credit granted as they are read: a window of 256, topped up by 128; `cancel` when dropped), `ready` (the hello, checked against the hash expected). A failure is `Error::Failed` with its code, message and data.
- An activation served natively: `#[activation]` writes `serve(activation, server, at)` and `impl Serve`, its `#[child]`ren served under `<at>.<child>`: one held by reference on its parent's thread, one that is anything (`Box<dyn Activation>`) or fits an interface (`Box<dyn Glazes>`, `trait Glazes: Activation`) mounted; `plexus::main(activation)` (feature `tokio`) is a process serving it over stdio, or with `--ws ADDR` on a WebSocket. The activation is moved to a thread of its own and held in an `Rc` (`served.rs`): its methods take `&self` with `RefCell` state, as in a component, so it need be `Send` but not `Sync`, while the `Server` stays `Send` on any executor.
- `Store` (feature `store`): an activation's own SQLite database, through an API of plexus's own (`plexus::store`): `execute`, `query` and `query_one` with a row mapper, `transaction`; a parameter is a `Value` (null, integer, real, text, blob), a column a `ValueRef` borrowed from where it lies, read with `Row::get::<T>(column)`; an error is a value, `store::Error`. `Store::open("kiln", SCHEMA)` in the activation's `Default`; it is opened at its first use, its schema run then. The same source is rusqlite in a process (`src/store/native.rs`) and, in a component, the import `plexus:store/sql` (`wit/store.wit`, `src/store/wasm.rs`), a statement's parameters and a query's rows crossing as one block of bytes each (`src/store/packed.rs`, which the engine includes too): SQLite is not in the component at all (the kiln: 26 KB → 33 KB gzipped, against 434 KB with SQLite inside), and the component builds with plain cargo, no clang. The runtime decides where a name's bytes go, by the activation it serves, so an activation that names another's database gets one of its own: natively a store that is a field of an activation is claimed by the place it is served at when it is served (`#[activation]` offers each field), and its file is `<DIR>/<place>/<name>.db` (`<DIR>/kiln/kiln.db`; the kiln held by the studio: `<DIR>/studio.kiln/kiln.db`), DIR being `plexus::main`'s `--data DIR` or `PLEXUS_DATA`; with neither it is in memory, said once on stderr. A store held anywhere else than a field of the activation itself is claimed by none and is `<DIR>/<name>.db`. A name is letters, digits, `_`, `-` and `.`, not first. In a page the host scopes it the same way (`../plexus-rt`'s `store`: `<activation>/<name>.db` in the browser's file store).
- `engine/` (its own crate, `plexus-store-engine`): the one SQLite a page holds, exporting `plexus:store/sql` — rusqlite with SQLite compiled to wasm (clang, so built in a container: `../plexus-rt/scripts/engine.sh`), statements kept prepared by their text, over a VFS that imports `plexus:store/host`: a clock, randomness, and the host's files by name — a page's worker hands it the browser's file store (OPFS), so a database outlives a reload; a name the host does not keep is kept in memory. 350 KB brotli, fetched once by a page for every activation in it.
- `Channel`: whole frames as bytes. `pair()` in memory; with the `tokio` feature, `stdio` (a frame a line: this process, or a child) and `ws` (a text message a frame: `connect`, `accept`, `listen`, `spawn`). Without it the crate builds for a component (`wasm32-unknown-unknown`).
- With the opt-in `tls` feature, `tls::Dial` pins the server’s Ed25519 principal and `tls::Listen` accepts the listed client principals. Each returns a `(Channel, String)` containing the peer’s verified `key:<hex>` before Plexus exchanges its hello. Feed the channel to the existing `Connection` or `Server`. Both ends need TLS 1.3 and a key.
- Frames are plexus-proto's; the behaviour matches `../plexus-rt`, the page's runtime, which calls this server in `tests/page.rs`.

```sh
cargo test --features tokio                     # in memory, a child over stdio, a WebSocket, a child held across a channel, and plexus-rt in Node (ignored, with why, without Node 22+)
cargo bench --features tokio --bench pace       # call.one_item and stream.10k.64B, in memory and over a loopback WebSocket
cargo test --locked --features tls --test tls   # mutual pins, key files and the shared kiln over WSS
cargo bench --locked --features tls --bench pace       # compare WS and WSS calls, streams and new connections
cargo bench --locked --features tls,trace --bench trace # trace a warm call through the record layer
cargo build --target wasm32-unknown-unknown     # the core alone; with `store` too (the import; no clang)
cargo test --features store --test store        # Store natively: statements, rows, errors as values, a transaction, a name refused
cargo test --features store --test store_names  # a parent and the child it holds open the same name: each its own database
cargo run --features tokio --bin test-kiln -- --ws 127.0.0.1:0   # the hand-registered kiln the tests call (it counts what `heat` made, which pacing is checked by)
```

To connect a caller that already has a Synapse file key, use the server’s public principal as the pin. The same `Connection::stream` call works on the returned channel:

```rust
let me = plexus::tls::Identity::from_key_file(key_path)?;
let dial = plexus::tls::Dial::new(&me, &server_principal)?;
let (channel, peer) = dial.connect("wss://studio.example:4455/").await?;
let (connection, driving) = plexus::Connection::new(channel, None);
tokio::spawn(driving);
let readings = connection.stream("studio.kiln.heat", params)?;
```

On the server, `Listen::new(&server_identity, allowed_callers)?.accept(tcp).await?` returns the channel and the verified caller principal. The application can inspect the principal before passing the channel to `Server::serve`.

## Callers and context in native activations

`Listen::spawn` supplies the verified peer to each call on the connection. An application accepting its own connections can use `server.serve_as(channel, verified_caller)` after authentication. `server.serve(channel)` serves anonymous calls. A method can take `caller: &Caller` to require a caller, or `Option<&Caller>` to accept either. The client sends only the ordinary method arguments.

An activation declares its opening contract with `#[plexus::activation(context = StudioCx)]`. Define the context inside the module:

```rust
#[derive(plexus::Context)]
pub struct StudioCx {
    #[from(caller)]
    user: User,
    tenant: String,
}
```

Implement `FromCaller<Studio>` on the application's `User` to look up the verified principal in the studio's own account records. A call to `studio[tenant=acme].status` supplies the tenant. Before invoking the method, Plexus fills the context and runs the activation's optional `#[open] fn open(&self, cx: &StudioCx) -> Result<(), Denied>`. This check also covers methods that omit the context parameter. Each new call rechecks membership. A running stream keeps its opening context through completion or cancellation.

The two-client example in `tests/caller_context.rs` exercises a streaming guestbook, app-owned user lookup, typed path keys and membership changes on a live connection. Run `cargo test --locked --features tls --test caller_context`. Caller propagation across remote hops and browser sign-in require their own authentication contracts.

Part of Plexus, built from scratch (tracker: LAYERS-9). Its sibling crates are expected beside it: `../<crate>`.

MIT.
