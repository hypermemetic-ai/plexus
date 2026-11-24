---
name: plexus-v2-activations
description: Build and test native Layers-1 (Plexus RPC v2) Rust activations using plexus::activation. Use for streaming methods, typed inputs, children, generated interfaces, and calling the result with Axon.
---

# Build a Plexus v2 activation

Start with the crate that owns the interface. A Layers-1 activation imports the `plexus` facade and puts `#[plexus::activation]` on a Rust module. For an existing service that registers a `DynamicHub`, check which protocol the caller expects before porting it.

When a method depends on identity, roles, a tenant or an exact resource, read the [v2 policy subskill](../plexus-v2-policies/SKILL.md) before choosing its signature.

## A stream you can call

For a new binary crate named `counter` next to the native `plexus/` checkout, use Rust edition 2024 and these dependencies. Adjust the path to the checkout used by the application.

```toml
[dependencies]
plexus = { path = "../plexus", features = ["tokio"] }
futures = "0.3"
```

Put this in `src/main.rs`. The module supplies the wire namespace. The method takes a named argument and yields one item for each tick.

```rust
#[plexus::activation]
pub mod counter {
    use futures::{stream, Stream};

    #[derive(Default)]
    pub struct Counter;

    impl Counter {
        pub fn ticks(&self, count: u32) -> impl Stream<Item = u32> {
            stream::iter(1..=count)
        }
    }
}

fn main() -> std::io::Result<()> {
    plexus::main(counter::Counter)
}
```

With native Axon on PATH, run these in the crate directory. Each invocation starts its own stdio server. The first lists the interface and the second should yield 1, 2, and 3.

```sh
cargo build
axon --run ./target/debug/counter counter
axon --run ./target/debug/counter counter ticks --count 3
```

## Choose the signature

- Return `T` for one answer or `impl Stream<Item = T>` for incremental output. A method that can refuse an opening can return `Result<impl Stream<Item = T>, E>` with a public error enum declared in the activation module. This preserves the stream's item type.
- For duplex RPC, take **one** owned `plexus::Input<T>` plus any ordinary parameters. Polling the input grants credit. Each item is a `Result<T, plexus::Failure>`. For uploads, `Input<plexus::Bytes>` also supports negotiated binary frames. Check the macro revision used by the consumer. Revisions with an injection registry enable `injections(input)` on the activation; earlier v2 revisions infer input from the type.
- Use `#[child] fn kiln(&self) -> &Kiln` for a held local child. A child returning `Box<dyn plexus::Activation>` can hold a `Remote`. The caller uses the same qualified path in both cases. The studio in `plexus-examples/studio/src/lib.rs` serves both kinds.
- Use the module's generated `IR`, `HASH`, `CLIENT_TS`, and `WIT` to inspect the public contract. `plexus-gen ts interface.json` also generates a TS client from saved IR. Check that parameter names, item types and typed failures match what clients consume.

For a regression test, mount the activation on a `plexus::Server`, connect through `plexus::pair()`, and call through `Connection`. Exercise the streaming path with more than one item. For duplex calls, also exercise cancellation or rejected input as applicable. Examples in the native checkout include `tests/input.rs` and `tests/method_grants.rs`.
