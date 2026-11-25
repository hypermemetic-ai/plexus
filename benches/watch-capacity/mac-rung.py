#!/usr/bin/env python3
"""Drive one pinned Mac generator and one local Layers-1 fixture. JSONL on stdout."""
import argparse
import contextlib
import ipaddress
import json
import os
from pathlib import Path
import resource
import select
import subprocess
import sys
import time
from urllib.parse import urlsplit


def record(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def process_memory(pid):
    entries = Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()
    fields = {entry.split()[0][:-1]: int(entry.split()[1]) for entry in entries[1:]
              if entry.startswith(("Pss:", "Rss:", "SwapPss:"))}
    return {**fields, "units": "KiB", "fds": len(list(Path(f"/proc/{pid}/fd").iterdir()))}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bin-dir", type=Path, required=True)
    p.add_argument("--key-file", type=Path, required=True)
    p.add_argument("--mac", type=ipaddress.IPv4Address, required=True)
    p.add_argument("--control-port", type=int, default=47671)
    p.add_argument("--mac-pin", required=True)
    p.add_argument("--ryzen", type=ipaddress.IPv4Address, required=True)
    p.add_argument("--users", type=int, required=True)
    p.add_argument("--orgs", type=int, required=True)
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--rows", type=int, default=2000)
    p.add_argument("--profile", choices=("uniform", "hot"), default="uniform")
    p.add_argument("--writes-per-second", type=int, default=4)
    p.add_argument("--read-interval-ms", type=int, default=10000)
    args = p.parse_args()
    if not 1 <= args.orgs <= args.users <= 1_000_000 or args.rows < 50 or min(args.seconds,args.read_interval_ms,args.writes_per_second) <= 0:
        p.error("invalid workload dimensions")
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < args.users + 256:
        p.error("insufficient local descriptor budget")
    url = f"wss://{args.mac}:{args.control_port}/"

    def call(method, **params):
        raw = subprocess.run([str(args.bin_dir / "load-agent"), "call", str(args.key_file), url,
                              args.mac_pin, "worker." + method, json.dumps(params)],
                             check=True, capture_output=True, text=True, timeout=45)
        return json.loads(raw.stdout)

    def wait_phase(phase, timeout):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            s = call("status", run=run)
            if s["phase"] == phase:
                return s
            if s["phase"] in ("stopped", "failed", "closed"):
                raise RuntimeError(s.get("error") or f"worker reached {s['phase']}")
            if s["phase"] != last:
                record("worker_phase", phase=s["phase"], keys=s["keys"], sessions=s["sessions"])
                last = s["phase"]
            time.sleep(.25 if args.users < 5000 else 1)
        raise TimeoutError(f"worker did not reach {phase}")

    def clock_sample():
        # NTP-style midpoint. Includes control CLI overhead in the error bound.
        before = time.time_ns()//1000
        peer = call("clock", run=run)
        after = time.time_ns()//1000
        if after < before:
            raise RuntimeError("coordinator wall clock stepped")
        return {"offset_us": peer - (before + after)//2, "uncertainty_us": (after-before+1)//2}

    run = call("prepare", first=0, count=args.users, total=args.users, orgs=args.orgs, seconds=args.seconds)
    record("run", run=run, users=args.users, orgs=args.orgs, rows=args.rows, seconds=args.seconds,
           profile=args.profile, offered_reads_s=args.users*1000/args.read_interval_ms, offered_writes_s=args.writes_per_second)
    with contextlib.ExitStack() as cleanup:
        def stop():
            try:
                call("stop", run=run)
            except Exception as error:
                print(f"worker cleanup: {error}", file=sys.stderr)
        cleanup.callback(stop)
        wait_phase("keys_ready", 60 + args.users/250)
        fixture = cleanup.enter_context(subprocess.Popen(
            [str(args.bin_dir/"watch-capacity"), "serve-external", str(args.orgs), str(args.rows),
             str(args.ryzen), str(args.mac)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            env={**os.environ, "WATCH_PROFILE":args.profile, "WATCH_EVALUATION":"shared"}))
        cleanup.callback(fixture.kill)
        with fixture.stdin as keys:
            for offset in range(0, args.users, 1024):
                n = min(1024, args.users-offset)
                batch = call("keys", run=run, offset=offset, limit=n)
                if len(batch) != n or not all(isinstance(key,str) and len(key)==68 and key.startswith("key:") for key in batch):
                    raise ValueError("worker returned invalid principal batch")
                keys.writelines(key+"\n" for key in batch)
        if not select.select([fixture.stdout], [], [], max(120, args.orgs/3))[0]:
            raise TimeoutError("fixture did not become ready")
        addresses, pin = fixture.stdout.readline().strip().split()
        sockets = [urlsplit(addr) for addr in addresses.split(",")]
        if any(addr.scheme!="wss" or addr.hostname!=str(args.ryzen) or not addr.port for addr in sockets):
            raise ValueError("unexpected fixture listener")
        record("seeded", server=process_memory(fixture.pid))
        call("connect", run=run, ports=[s.port for s in sockets], pin=pin, profile=args.profile,
             rate=args.writes_per_second, **{"read-interval-ms":args.read_interval_ms})
        ready = wait_phase("ready", 120 + args.users/30)
        if ready["sessions"] != args.users:
            raise ValueError("worker did not hold every requested session")
        record("connected", sessions=ready["sessions"], server=process_memory(fixture.pid))
        clock = min((clock_sample() for _ in range(5)), key=lambda m:m["uncertainty_us"])
        record("clock_before", **clock)
        start = time.time_ns()//1000 + clock["offset_us"] + max(2_000_000, clock["uncertainty_us"]*10)
        call("start", run=run, **{"unix-us":start})
        end = wait_phase("result", args.seconds+180)
        record("result", worker=json.loads(end["result"]), server=process_memory(fixture.pid))
        call("drop", run=run)
        wait_phase("closed", 60 + args.users/4000)
        record("closed", server=process_memory(fixture.pid))


if __name__ == "__main__":
    main()
