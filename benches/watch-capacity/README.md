# LWATCH single-server capacity ladder

Run from this directory on Linux with the Layers sibling repositories checked out. Build a release binary. Each rung launches a separate server process on ephemeral loopback WSS listeners. The load generator creates one random signing key and one mutually authenticated connection per user. No existing database, hostname or credential is used.

```sh
cargo build --release --locked
python3 ladder.py --bin target/release/watch-capacity --out uniform.jsonl
python3 ladder.py --bin target/release/watch-capacity --out hot.jsonl --profile hot
```

The default ladder tries 1, 10, 100, 1k, 10k, 100k, 250k, 500k, 750k and 1M. Use `--max-users 100 --seconds 10` for a quick run. The target is 1M users, 10k organizations and 2,000 real SQLite rows per organization (20M at the largest rung). Lower rungs use fewer organizations. A steady-state result includes simultaneous open subscriptions, scheduled reads and writes. Each user holds one live 50-row board, issues one read every ten seconds, and sees only their organization's tickets. The writer commits four changes per second. Override these rates with `WATCH_READ_INTERVAL_MS` and `WATCH_WRITES_PER_SECOND`. The hot profile places extra readers in the first organization and sends four out of five writes there.

The fixture runs an explicit native watch method. It filters invalidations by organization, reruns the SQLite read, and retains a 128-bit digest per watch to suppress equal results. The default `WATCH_EVALUATION=per-watch` evaluates each affected subscription separately. With `WATCH_EVALUATION=shared`, each active organization retains one evaluated page for its identical authorized projection. An explicit list read can reuse that snapshot. A visible write refreshes the page once. Each subscriber retains its digest and sequence. A slow subscriber can skip intermediate snapshots, and the last subscriber leaving releases the shared page. The reference mode executes a query for each ordinary read. Test data and queries are deterministic. The database is in memory, the first page is warm, and every write updates a projected row. Real application grants, revocation, unchanged-result traffic, the generated watchable-read contract, browser rendering and reconnect storms need separate runs. No benchmark result here proves those scenarios.

Each rung opens connections, receives and validates every first page, holds them idle for two seconds, runs the scheduled load, drains outstanding work and samples again two seconds after teardown. A fresh fixture per rung accommodates changing membership and corpus size. This measures the post-close floor within a rung; a long-lived process soak is separate. Read and write latencies include generator scheduling delay. Update latency runs from database commit to receipt of the decoded native item, using the shared host clock. An omitted, duplicate, stale or cross-organization result fails the rung. The load scenario expects every update to arrive, including on a hot organization. A replay/reset-aware slow-reader profile belongs with the production contract.

JSONL records the workload, effective population, real sockets, watches, read/write/delivery latency samples, expected deliveries, query and byte counts, server memory, CPU ticks and file descriptors by phase, and separate generator resources. The provisional stop margins are p99 read ≤100 ms and p99 update ≤250 ms, adjustable on the command line. Exit 2 records a resource or latency stop. Exit 1 records a failed rung. A partial run establishes its completed rungs only.

Loopback listeners share one server and TLS acceptor; each destination serves at most 8,000 test connections to avoid exhausting the generator's ephemeral ports. The driver checks its per-process descriptor limit and projects the combined server and generator peak RSS before each larger rung, retaining host headroom. Kernel networking buffers cost additional memory. The reference server has 12 cores/24 threads and about 31 GiB of RAM. External generators are needed to isolate its ultimate capacity.

## Measure the Layers-1 server

Build with `cargo build --release --locked`. The Linux connection-stage tool samples separate server and generator processes using proportional set size (PSS). Run in a fresh cgroup to collect kernel charges across both ends. Stage choices are `tcp`, `tls`, `ws`, `wss` and `idle`.

```sh
systemd-run --user --scope target/release/connection-cost idle 10000
WATCH_CPU=1 WATCH_WORKLOAD=reads WATCH_EVALUATION=shared target/release/watch-capacity 1000 10 12 2000
```

`WATCH_CPU=1` opens Linux hardware counters for each fixture thread and separates opening, idle and load. Inspect `cpu_phases` for retired user instructions and process CPU time. Use `WATCH_WORKLOAD=reads` or `updates` to attribute load to one operation. Update costs include the stated fan-out. The mixed run reports aggregate work. Set `WATCH_ACTIVATION_LANES` to replicate the fixture's generated activation over the same underlying store. The verified caller's organization determines its lane. `WATCH_SERVER_THREADS` and `WATCH_GENERATOR_THREADS` set transport worker counts independently. Defaults are one activation lane and four Tokio workers in each process.

For an instruction-throughput reference on x86-64 Linux, build `cc -O2 -pthread core-rate.c -o /tmp/core-rate` and pass the CPU IDs to exercise, for example `/tmp/core-rate 0` for one logical CPU. Use the host's `thread_siblings_list` to choose one logical CPU per physical core. The program reports measured IPC and aggregate instructions/s for independent integer adds. Use measured RPC CPU time and observed saturation to size application workloads.

[Distributed workload and clocks](DISTRIBUTED.md) · [Mac control activation](MAC-WORKER.md)
