# Run a watch-capacity generator on a Mac

Build the two native binaries in the same directory on the Mac. The worker runs
in the foreground and launches only its sibling `watch-capacity generator`.
The Ryzen host runs the SQLite fixture. Both data and control connections use
mutually pinned TLS.

```sh
cargo build --release --locked --bin load-agent --bin watch-capacity
target/release/load-agent keygen ./mac-worker.key
```

Generate a separate controller key on the Ryzen using the same `keygen` command.
Each command prints a public `key:…` principal. Exchange those public principals
over the operator's trusted channel. Key files are created exclusively with
mode 0600. To display an existing file's public principal, use
`load-agent principal FILE`.

Start the worker with a specific local interface and the Ryzen's literal
target IP. Set the open-file limit to accommodate at least the requested
number of sockets plus process overhead.

```sh
target/release/load-agent serve ./mac-worker.key "$MAC_IP:4700" \
  "$RYZEN_CONTROLLER_PRINCIPAL" "$RYZEN_IP" "$MAX_USERS"
```

The first stdout line contains the bound WSS URL, worker principal, and local
limits. Keep this process running for the test. Ctrl-C or SIGTERM stops and
reaps its generator. Temporary control connections have independent lifetimes.

## Control RPC

Use the Ryzen controller key and the worker's public pin. For example, prepare
ten users for one second against one organization.

```sh
load-agent call ./ryzen-controller.key "$MAC_URL" "$MAC_WORKER_PRINCIPAL" worker.prepare \
  '{"first":0,"count":10,"total":10,"orgs":1,"seconds":1}'
```

Each call prints its JSON value to stdout. Refusals and connection failures
exit nonzero. Only one run is active at a time. Use the returned run ID for
the following calls. The controller can retrieve public keys in batches
while the generator creates them.

| Method | Arguments | Result |
| --- | --- | --- |
| `worker.prepare` | `first,count,total,orgs,seconds` | Fresh numeric run ID. The local generator owns the contiguous range starting at `first`. |
| `worker.keys` | `run,offset,limit` | Public principals from this shard, in index order. The relative offset starts at zero, the maximum limit is 1024. |
| `worker.status` | `run` | `phase,keys,sessions,result,error`. Result is nullable JSON text containing the generator's full result event. |
| `worker.connect` | `run,ports,pin,profile,rate,read-interval-ms` | Opens the authorized users against the local operator's fixed target IP. `pin` is the ephemeral fixture's public principal. |
| `worker.clock` | `run` | Current worker Unix time in microseconds. |
| `worker.start` | `run,unix-us` | Starts at the given worker-local timestamp in the next two minutes. Repeating the same start is idempotent. |
| `worker.drop` | `run` | After result, release connections and await `closed` via status. |
| `worker.stop` | `run` | Stop and reap the child immediately. |

Wait for `keys_ready` before connecting. Assemble *all* generator shards' keys
in global-index order and start the fixture using that exact ordered input.
Use its advertised ports and principal. The fixture source allowlist must admit
the Mac's source IP. A generator authenticates the fixture and reads its native
board before opening the rest of its shard. Wait for `ready` before scheduling
the start. After `result`, fetch the metrics and send `drop`. A new prepare
replaces a closed, stopped, or failed run and invalidates its ID. Local stderr
contains the generator's diagnostic if it exits early.

The range cannot exceed the operator's MAX_USERS. The fixture supports at most
1M users globally, 10k organizations, one hour, four writes per second, and
read intervals of at least one second through this worker. Provision up to one
fixture port per 8000 global users, rounded up. Waiting for key collection,
connection, start or result collection is bounded, including when the controller
disconnects. The private user keys stay in the generator process.

Run the two-user smoke test on either Unix platform after building the binaries.
It uses loopback and generated temporary keys, then stops both services.

```sh
python3 tests/load-agent.py target/release
```

## A Mac with an outbound-only tailnet path

On the Ryzen, run `load-agent rendezvous "$RYZEN_IP:47674" 127.0.0.1:24762 "$MAC_IP"`. On the Mac, run `load-agent dial-back ./mac-worker.key "$RYZEN_IP:47674" "$RYZEN_CONTROLLER_PRINCIPAL" "$RYZEN_IP" "$MAX_USERS"`. The worker opens two outgoing TCP connections and serves the same pinned control activation on them. Its TLS role stays the server. The Ryzen coordinator connects to `wss://127.0.0.1:24762/` using the Mac pin. Its benchmark data sockets still run directly from the Mac to the Ryzen. The rendezvous takes incoming worker sockets only from the fixed Mac IP and takes controller sockets only on loopback.
