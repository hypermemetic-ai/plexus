#!/usr/bin/env python3
"""Drive one pinned Mac generator and one local Layers-1 fixture. JSONL on stdout."""
import argparse
import contextlib
import ipaddress
import json
import os
from pathlib import Path
import re
import resource
import select
import subprocess
import sys
import time
import tempfile
from capacity_verdict import Schedule, add_arguments, budgets, classify, exit_code
from server_resources import CpuWindow
from urllib.parse import urlsplit


def record(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def process_memory(pid):
    entries = Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines()
    fields = {entry.split()[0][:-1]: int(entry.split()[1]) for entry in entries[1:]
              if entry.startswith(("Pss:", "Rss:", "SwapPss:"))}
    return {**fields, "units": "KiB", "fds": len(list(Path(f"/proc/{pid}/fd").iterdir()))}


def tcp_sample(port, expected):
    # A listener serves at most 8,000 users. TCP_INFO counts include TLS and WS
    # but exclude TCP/IP headers and the outer tailnet tunnel.
    command = ["ss", "-Htin", "state", "established", f"sport = :{port}"]
    output = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    sockets = [line for line in output.splitlines() if line and not line[0].isspace()]
    if len(sockets) != expected:
        raise ValueError(f"sample listener has {len(sockets)} connections, expected {expected}")
    counters = {}
    for field in ("bytes_sent", "bytes_acked", "bytes_received", "bytes_retrans"):
        counters[field] = sum(map(int, re.findall(rf"\b{field}:(\d+)", output)))
    if not counters["bytes_acked"]:
        raise ValueError("kernel did not report acknowledged TCP bytes")
    return {"port": port, "sessions": len(sockets), **counters}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bin-dir", type=Path, required=True)
    p.add_argument("--key-file", type=Path, required=True)
    p.add_argument("--mac", type=ipaddress.IPv4Address, required=True)
    p.add_argument("--control-host", type=ipaddress.IPv4Address, help="local endpoint of an operator-provided control tunnel")
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
    add_arguments(p)
    args = p.parse_args()
    if not 1 <= args.orgs <= args.users <= 1_000_000 or args.rows < 50 or min(args.seconds,args.read_interval_ms,args.writes_per_second) <= 0:
        p.error("invalid workload dimensions")
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < args.users + 256:
        p.error("insufficient local descriptor budget")
    url = f"wss://{args.control_host or args.mac}:{args.control_port}/"

    def call(method, **params):
        raw = subprocess.run([str(args.bin_dir / "load-agent"), "call", str(args.key_file), url,
                              args.mac_pin, "worker." + method, json.dumps(params)],
                             capture_output=True, text=True, timeout=45)
        if raw.returncode:
            raise RuntimeError(f"worker.{method} failed: {raw.stderr.strip()}")
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
           profile=args.profile, read_interval_ms=args.read_interval_ms, server_budgets=budgets(args), offered_reads_s=args.users*1000/args.read_interval_ms, offered_writes_s=args.writes_per_second)
    with contextlib.ExitStack() as cleanup:
        window_path = Path(cleanup.enter_context(tempfile.TemporaryDirectory(prefix="watch-fixture-"))) / "window.json"
        expected_ports = (args.users + 7999)//8000
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
            env={**os.environ, "WATCH_PROFILE":args.profile, "WATCH_EVALUATION":"shared",
                 "WATCH_SERVER_TIMING":"1", "WATCH_TIMING_WINDOW_FILE":str(window_path),
                 "WATCH_TIMING_SKIP_READS":str(expected_ports)}))
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
        if len(sockets) != expected_ports:
            raise ValueError("fixture listener count differs from expected enrollment probes")
        if any(addr.scheme!="wss" or addr.hostname!=str(args.ryzen) or not addr.port for addr in sockets):
            raise ValueError("unexpected fixture listener")
        record("seeded", server=process_memory(fixture.pid))
        call("connect", run=run, ports=[s.port for s in sockets], pin=pin, profile=args.profile,
             rate=args.writes_per_second, **{"read-interval-ms":args.read_interval_ms})
        ready = wait_phase("ready", 120 + args.users/30)
        if ready["sessions"] != args.users:
            raise ValueError("worker did not hold every requested session")
        sample_count = (args.users + len(sockets) - 1)//len(sockets)
        record("connected", sessions=ready["sessions"], server=process_memory(fixture.pid),
               tcp=tcp_sample(sockets[0].port, sample_count))
        clock = min((clock_sample() for _ in range(5)), key=lambda m:m["uncertainty_us"])
        record("clock_before", **clock)
        local_start_unix_us = time.time_ns()//1000 + max(2_000_000, clock["uncertainty_us"]*10)
        window = {"start_unix_us":local_start_unix_us, "duration_us":args.seconds*1_000_000}
        staged = window_path.with_suffix(".tmp")
        staged.write_text(json.dumps(window))
        staged.replace(window_path)
        record("schedule", **window, expected_port_probes=expected_ports)
        with CpuWindow(fixture.pid, local_start_unix_us, args.seconds) as cpu:
            call("start", run=run, **{"unix-us":local_start_unix_us + clock["offset_us"]})
            end = wait_phase("result", args.seconds+180)
        worker = json.loads(end["result"])
        schedule = Schedule(args.users, args.orgs, args.seconds, args.read_interval_ms,
                            args.writes_per_second, args.profile, port_probes=expected_ports)
        verdict = classify(worker, schedule, budgets(args), remote=True, start_unix_us=local_start_unix_us,
                           clock_uncertainty_us=clock["uncertainty_us"])
        record("result", worker=worker, server=process_memory(fixture.pid), server_cpu=cpu.report(),
               tcp=tcp_sample(sockets[0].port, sample_count))
        call("drop", run=run)
        wait_phase("closed", 60 + args.users/4000)
        record("closed", server=process_memory(fixture.pid))
        record("verdict", verdict=verdict)
        return exit_code(verdict)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        record("verdict", verdict={"status":"fail","code":"runner_error","reasons":[str(exc)]})
        raise SystemExit(1)
