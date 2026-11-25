# LWATCH single-server capacity ladder

Run from this directory on Linux with the Layers sibling repositories checked out. Build a release binary. Each rung launches a separate server process on ephemeral loopback WSS listeners. The load generator creates one random signing key and one mutually authenticated connection per user. No existing database, hostname or credential is used.

```sh
cargo build --release --locked
python3 ladder.py --bin target/release/watch-capacity --out uniform.jsonl
python3 ladder.py --bin target/release/watch-capacity --out hot.jsonl --profile hot
```

The default ladder tries 1, 10, 100, 1k, 10k, 100k, 250k, 500k, 750k and 1M. Use `--max-users 100 --seconds 10` for a quick run. The target is 1M users, 10k organizations and 2,000 real SQLite rows per organization (20M at the largest rung). Lower rungs use fewer organizations. A steady-state result includes simultaneous open subscriptions, scheduled reads and writes. Each user holds one live 50-row board, issues one read every ten seconds, and sees only their organization's tickets. The writer commits four changes per second. Override these rates with `WATCH_READ_INTERVAL_MS` and `WATCH_WRITES_PER_SECOND`. The hot profile places extra readers in the first organization and sends four out of five writes there.

The native generator opens the companion of the declared watchable read, `tickets.list-watch`. The same activation serves `Tickets.list()`, `Tickets.list({ status: "Ready" })` and `Tickets.get(id)` to generated Plexus UI clients. A filtered list selects the first 50 matching tickets by ID within the authenticated organization. Both watchable reads declare `#[watch(on = [store.ticket_change(caller)?])]`. This subscribes to committed ticket changes in the caller’s organization. The list is read again with its original filter, and the digest of its visible rows determines whether to emit a page. The original hand-authored `tickets.watch` stream is still available to older clients. All new native rungs record their chosen method and interface hash. In shared mode one organization retains one evaluated page for its identical authorized projection. Each subscription retains a 128-bit digest for visible change detection, and the last subscription releases the shared page. The fixture uses immutable organization membership and a warm 50-row projection.

Shared evaluation uses `WATCH_ENCODING=shared` by default. It serializes each published page once and hands a reference to its JSON bytes to each generated read or watch response. `WATCH_ENCODING=per-call` retains the typed page and serializes it on each response. The fixture's immutable membership and common query allow organization-level sharing. Filtered reads use the SQLite index on organization, status and ID and evaluate separately for each caller. The capacity ladder exercises the unfiltered shared projection. `server_counters.page_encodes` counts actual response encodings and `cached_page_json_bytes` counts retained encoded pages. Every delivery still incurs its own protocol framing, TLS and socket work.

Build these sibling repositories on `testing/lwatch-ui`: `plexus`, `plexus-macros`, `plexus-gen`, `plexus-ir`, `plexus-rt`, and `plexus-ui`. The `plexus-proto` base is `4ee4128`. The binary's `client-ts` and `interface` commands print the TypeScript client and IR emitted by the Rust activation. The component and headless-browser integration live in `plexus-ui/tests/fixtures/native-watch` and `plexus-ui/tests/native_watch_browser.mjs`. [Generated-client ladder](tests/generated-clients/README.md).

Each rung opens connections, receives and validates every first page, holds them idle for two seconds, runs the scheduled load, drains outstanding work and samples again two seconds after teardown. A fresh fixture per rung accommodates changing membership and corpus size. This measures the post-close floor within a rung; a long-lived process soak is separate. Read and write latencies include generator scheduling delay. Update latency runs from database commit to receipt of the decoded native item, using the shared host clock. An omitted, duplicate, stale or cross-organization result fails the rung. The load scenario expects every update to arrive, including on a hot organization. A replay/reset-aware slow-reader profile belongs with the production contract.

JSONL records the workload, effective population, real sockets, watches, read/write/delivery latency samples, expected deliveries, query and byte counts, server memory, CPU ticks and file descriptors by phase, and separate generator resources. The capacity verdict uses the server's admitted and successfully flushed operations during the scheduled window. The default p99 budgets are 100 ms for a read, 250 ms for a write acknowledgement and 250 ms for commit to watch-update flush. The default minimum load fraction is 95%. Client round-trip and one-way histograms describe the path seen by the generator. An underfed rung is inconclusive about server capacity. A partial ladder establishes only the passing populations for each profile.

Loopback listeners share one server and TLS acceptor; each destination serves at most 8,000 test connections to avoid exhausting the generator's ephemeral ports. The driver checks its per-process descriptor limit and projects the combined server and generator peak RSS before each larger rung, retaining host headroom. Kernel networking buffers cost additional memory. The reference server has 12 cores/24 threads and about 31 GiB of RAM. External generators are needed to isolate its ultimate capacity.

## Measure the Layers-1 server

Build with `cargo build --release --locked`. The Linux connection-stage tool samples separate server and generator processes using proportional set size (PSS). Run in a fresh cgroup to collect kernel charges across both ends. Stage choices are `tcp`, `tls`, `ws`, `wss` and `idle`.

```sh
systemd-run --user --scope target/release/connection-cost idle 10000
WATCH_CPU=1 WATCH_WORKLOAD=reads WATCH_EVALUATION=shared target/release/watch-capacity 1000 10 12 2000
```

`WATCH_CPU=1` opens Linux hardware counters for each fixture thread and separates opening, idle and load. Inspect `cpu_phases` for retired user instructions and process CPU time. Use `WATCH_WORKLOAD=reads` or `updates` to attribute load to one operation. Update costs include the stated fan-out. The mixed run reports aggregate work. Set `WATCH_ACTIVATION_LANES` to the number of native activation workers. The default `WATCH_DISPATCH=tenant` pins each verified caller's organization to a replica. With `WATCH_DISPATCH=concurrent`, one generated activation shares the workers, allowing calls for a single hot organization to use multiple cores. `WATCH_SERVER_THREADS` and `WATCH_GENERATOR_THREADS` set transport worker counts independently. Defaults are one activation lane and four Tokio workers in each process.

For an instruction-throughput reference on x86-64 Linux, build `cc -O2 -pthread core-rate.c -o /tmp/core-rate` and pass the CPU IDs to exercise, for example `/tmp/core-rate 0` for one logical CPU. Use the host's `thread_siblings_list` to choose one logical CPU per physical core. The program reports measured IPC and aggregate instructions/s for independent integer adds. Use measured RPC CPU time and observed saturation to size application workloads.

[Distributed workload and clocks](DISTRIBUTED.md) · [Mac control activation](MAC-WORKER.md)


## Mac-generated capacity ladder

Start the [pinned Mac worker](MAC-WORKER.md), then run the ladder on the Ryzen
with a fresh output directory. Set the lane count and dispatch mode explicitly
when comparing implementations. This example runs uniform and hot profiles at
each requested population. With these settings the final population runs for
one hour if the preceding rungs and local resource checks pass.

```sh
WATCH_ACTIVATION_LANES=4 WATCH_DISPATCH=concurrent WATCH_ENCODING=shared WATCH_SERVER_THREADS=4 \
python3 mac-ladder.py --out-dir results/mac-01 --bin-dir target/release \
  --key-file ./ryzen-controller.key --mac "$MAC_IP" --mac-pin "$MAC_WORKER_PRINCIPAL" \
  --ryzen "$RYZEN_IP" --profiles uniform hot --seconds 30 --final-seconds 3600
```

For an outbound-only worker, add `--control-host 127.0.0.1 --control-port 24762`
after setting up the rendezvous. Use `--rungs 32,100,1000` for a short ladder.
The independent profile histories feed a memory projection that separates
dataset growth from socket growth, then applies a 25% margin to the estimate
from measured rungs. The first estimate uses 128 MiB plus 256 bytes per row and
32 KiB per user. Before each rung the driver checks the Ryzen process FD limit
and host/cgroup memory headroom. It samples headroom during the child process
and interrupts it if the reserve is breached. `--reserve-gib` defaults to 4.
Provision Mac memory and FDs separately.

`ladder.jsonl` records the invocation, binary hashes, server budgets, per-rung
evidence paths, headroom checks, each profile's stopping reason and its highest
passing population. Each rung preserves its original JSONL and stderr. The
runner classifies observed correctness failures, insufficient generator
load, incomplete telemetry, native server latency and backlog. A stopped
profile leaves the other profile eligible for later rungs. Exit 0 means
all requested rungs passed. Exit 1 means a correctness or runner failure,
2 means a capacity stop, inconclusive run or resource guard, and 130 means
operator interruption. Run `python3 tests/mac-ladder.py` for the offline
ladder tests.


## Observe a native rung in Chromium

Create a fresh private directory for one rung, build `watch-capacity` in release mode and start `plexus-ui/tests/native_watch_browser.mjs` with the binary, the TypeScript runtime, the codec Wasm and that directory. Use the same directory in `WATCH_BROWSER_RUN_DIR` when launching the native benchmark. The page enrolls its own ephemeral public pin and mounts a compiled component scoped to organization zero. The generator waits for its first rendered row before scheduling writes. On completion the page must see the final expected revision and cancel the watch. The JSON result includes the rendered revisions and the browser and Svelte versions under `browser_observer`.

The native population uses one independent authenticated socket and one full-page watch per user. The browser adds one authenticated connection watching ticket zero. Its reads and row updates are counted separately from the native full-page workload. The server’s CPU and memory measurements include both.

[Measured native and browser results on the Ryzen](cpu-scaling/evidence/ryzen-generated-ui.json)
