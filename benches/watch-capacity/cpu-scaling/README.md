# Watch CPU scaling

This Linux benchmark measures the existing native WSS fixture and its load generator on disjoint physical CPUs. Each user opens an authenticated session, subscribes to its authorized 50-row page and validates each scheduled read. The mixed profile also checks every scheduled write acknowledgement and watch delivery. The fixture shares the database and authorized views across activation lanes.

The November 25 server-capacity measurements cover a 480-session rate sweep, uniform and hot populations up to 250,000 actual watches, and Mac-generated traffic. The receipt records exact in-window counts alongside full-run server percentiles, client diagnostics and failed experiments. [Open the measured results](evidence/ryzen-server-capacity.json).

## Run the WSS sweep

Build the benchmark from its directory with `cargo build --release --locked --bin watch-capacity`. Inspect `lscpu -e=CPU,CORE,SOCKET` before selecting role masks. The example masks suit a 12-core machine on which CPUs 12–23 are the SMT siblings of CPUs 0–11.

```sh
python3 cpu-scaling/run.py \
  --server-cpus 0-3 --generator-cpus 4-11,16-23 \
  --users 480 --orgs 48 --rows 100 --seconds 15 \
  --intervals-ms 4 --writes-per-second 4 \
  --lanes 1,2,4,8,12 --wss-threads 2,4 --generator-threads 12 \
  --profile hot --workloads reads,mixed \
  --evaluation shared --encoding shared --dispatch concurrent > result.jsonl
```

The interval controls total offered reads/s as `1000 × users / interval_ms`. Sessions remain open through startup, a two-second idle window, the scheduled load and its drain. A short compiled Linux preload sets the fixture mask before its Rust runtime starts. The Python driver independently checks every thread's affinity for both processes. The requested server and generator masks must have disjoint physical cores, including SMT siblings. Affinity constrains where each benchmark process runs. Other host processes may still run on these cores.

Each new WSS result includes the fixture's monotonic timing, client diagnostics, and a machine-readable `verdict`. The same `--server-read-p99-ms`, `--server-write-p99-ms`, `--server-fanout-p99-ms` and `--min-load-fraction` options apply to the local ladder, Mac runner and CPU sweep. The sweep continues through overload and underfeed so the remaining CPU and latency measurements are retained. Its exit code reflects whether all trials completed. Inspect each cell's verdict to determine which loads passed. The historical receipts in this directory predate server-window timing and establish only the measurements they contain.

Every successful row contains these measurements.

- `read_achieved_s`, write and delivery rates divide the verified totals by scheduled load plus drain. `result.read_scheduled_to_receive`, `scheduled_write_to_ack` and `commit_to_receive` retain the original user-visible latency histograms in microseconds. Scheduled latency includes queueing behind earlier calls.
- `server_cpu_cores` divides fixture process CPU by the server hardware-counter phase's wall time. `server_threads` uses each Linux task's scheduler runtime; raw counters, retired user instructions, multiplex scaling and the native thread names are under `result.cpu_phases`. The load phase includes the roughly 100 ms scheduling lead-in.
- `generator_cpu_cores` subtracts local generator process CPU ticks observed at the load marker from its own end-of-load sample. It uses the same counter phase duration. Start and end snapshots for the two roles are asynchronous. The generator's sampling resolution is the recorded `ticks_per_second`. A value of 1.0 CPU means one second of process CPU per second of elapsed time. SMT threads share execution resources.
- `result.open_ms`, `result.cpu_phases.open` and `generator_open_cpu_s` cover connection setup and the first authorized page. Idle CPU appears separately. Each process records its own memory footprint.
- Failed or timed-out rungs emit a failure record and make the sweep exit unsuccessfully. A delayed read may still pass verification and miss its latency goal. Check both throughput and latency before accepting a rung.

## Isolate the server projection

```sh
for encoding in per-call shared; do
  for lanes in 1 2 4 8 12; do
    WATCH_EVALUATION=shared WATCH_ENCODING=$encoding \
      taskset -c 0-11 target/release/watch-capacity projection-ceiling "$lanes" 5
  done
done > projection.jsonl
```

Each lane creates a real caller, uses the native tickets activation to open its authorized 50-row watch and validates the initial watch and list result. After a common start, one OS thread per lane repeatedly calls that same activation's list method, consumes the complete response's byte length, and holds the subscription through the final sample. This directly measures membership lookup, shared-view access and native response preparation. Per-call encoding serializes the result on each read. Shared encoding clones the existing immutable authorized JSON. Each thread samples operation latency once per 128 calls. User authentication at the socket, native wire dispatch, TLS, network, client parsing and mixed updates are measured by the WSS sweep above.

The direct load uses the shared fixture's existing counters, so contention there is included. Linux process CPU, per-thread scheduler time and actual retired user instructions cover all lanes. Initialization, opening the first watches, and validating their contents precede the timed interval.

## Recorded runs

The JSONL files in `evidence/` retain all measured threads and user-facing tails. All trials used a Ryzen 9 5900X (12 physical cores, two SMT threads/core), Linux 7.2.9-ogc3.1.fc44.x86_64, the powersave CPU governor, loopback WSS, 480 users, 48 organizations, and 100 stored rows per organization. The projection-only trials used one organization and one caller per lane.

| File | Workload and CPU assignment |
| --- | --- |
| `ryzen-wss.jsonl` | Prior generator and per-call response encoding at Plexus `5c99731`. Uniform reads and mixed, 120k offered reads/s, 2-second scheduled load. Server and generator each had six physical cores with SMT. All 1/2/4/8/12 lane × 2/6 WSS-thread combinations were measured. |
| `ryzen-wss-saturation.jsonl` | Same prior binary, 240k offered, uniform reads and mixed. Four server cores without SMT, eight separate generator cores with SMT, 4 WSS threads, 12 generator threads. |
| `ryzen-hot-shared.jsonl`, `ryzen-hot-per-call.jsonl` | Borrowed generator validation and concurrent native dispatch, hot mixed load. Held the CPU masks, operations and four WSS threads constant while comparing 1/4/8 activation workers and the two response encodings at 120k and 240k offered, 3 seconds each. |
| `ryzen-single-core.jsonl` | Hot mixed load, shared encoding, one runnable fixture CPU, 2 WSS threads, 1 or 4 activation workers, 5-second offers. |
| `ryzen-hot-confirm.jsonl` | Two independent 15-second hot mixed runs at 120k offered, four server cores, four WSS threads and four concurrent activation workers, shared encoding. |
| `ryzen-projection.jsonl` | Server projection only, 1/2/4/8/12 lanes, both encodings, 2 seconds each, process mask 0–11 (one CPU per physical core). |

The new encoding fixture corresponds to Plexus `75551da`, with the in-tree CPU driver and sibling macros `6650533`, protocol `4ee4128`. The environment row identifies the exact release binary by SHA-256. Tests were run serially on a shared host. Use repeated longer runs and independent generator hosts for a production capacity estimate.

## Measured operating point

With a four-core fixture, four WSS threads and four concurrent activation workers, each 15-second hot mixed run completed **1,800,000 authorized reads, 60 commits and all 4,745 deliveries**. Throughput was 119,991 and 119,983 reads/s. Scheduled read p99 was 4.275 and 4.155 ms, write-to-ack p99 was 6.455 and 2.887 ms, and commit-to-receive p99 was 2.231 and 2.537 ms. Fixture CPU was 2.17 cores in both runs, generator CPU was 4.24 and 4.10 cores, and opening all 480 secured watches took 212 and 203 ms.

The 3-second hot mixed sweep showed how the queue behaved near overload. Four shared-encoding workers at 240k offered reads/s achieved 163,248 reads/s including drain, with 1.41 s scheduled read p99. Four per-call encoding workers achieved 156,472/s with 1.61 s p99. In the shared case the reactor threads each used 0.62–0.64 CPU, the four activation workers about 0.13 CPU each, and the generator 5.42 cores. These short observations capture the loopback benchmark's queueing limit. They leave CPU time available on both roles.

With only one fixture CPU, the hot mixed shared-encoding path with one activation worker delivered 49.2k reads/s at 60k offered and 48.1k/s at 120k offered. Fixture CPU stayed near 0.77 cores. The separate uncached per-watch reference in `ryzen-uncached.jsonl` reached 24.3k reads/s read-only and 24.0k/s mixed with 30k offered, one physical fixture CPU, and 0.85 fixture cores in either case. These plateaus include WSS, scheduler waits and loopback traffic. Attributing all remaining capacity to idle or saturated hardware would require independent host and kernel measurements.

| Server-only projection lanes | Per-call results/s | Per-call CPU cores | Shared authorized lookups/s | Shared CPU cores |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 438,460 | 1.00 | 13,046,883 | 1.00 |
| 2 | 854,778 | 1.99 | 21,790,325 | 1.99 |
| 4 | 1,675,274 | 3.98 | 25,359,020 | 3.98 |
| 8 | 3,123,953 | 7.97 | 28,576,554 | 7.97 |
| 12 | 4,228,196 | 11.93 | 32,056,900 | 8.96 |

Per-call work used around 46,800 retired user instructions per response. Shared immutable results used around 691 per lookup. The projection numbers quantify local serving work for an already-authorized caller. The WSS results above quantify verified client operations across the transport.
