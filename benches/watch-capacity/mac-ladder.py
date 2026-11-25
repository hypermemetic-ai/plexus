#!/usr/bin/env python3
"""Measure successive single-server rungs with the pinned Mac generator."""
import argparse
import hashlib
import ipaddress
import json
import math
import os
import platform
import resource
import signal
import subprocess
import sys
import time
from pathlib import Path

import capacity_verdict

HERE = Path(__file__).resolve().parent
MIB = 1024**2
GIB = 1024**3
STATUS = {
    "pass": ("pass", 0), "correctness": ("fail", 1),
    "server_latency": ("fail", 2), "server_backlog": ("fail", 2),
    "underfed": ("inconclusive", 2), "missing_telemetry": ("inconclusive", 2),
    "runner_error": ("fail", 1),
}


def memory_available():
    """Available bytes at the host and any enclosing cgroup-v2 limits."""
    host = next(int(line.split()[1]) * 1024 for line in
                Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemAvailable:"))
    limits = [host]
    cg = Path("/sys/fs/cgroup")
    for line in Path("/proc/self/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            path = cg / line[3:].lstrip("/")
            for parent in (path, *path.parents):
                if parent != cg and cg not in parent.parents:
                    break
                maximum = parent / "memory.max"
                if maximum.exists():
                    value = maximum.read_text().strip()
                    if value != "max":
                        limits.append(max(0, int(value) - int((parent / "memory.current").read_text())))
    return min(limits)


def org_count(users):
    return min(users, 10000, max(2, (users + 99) // 100))


def projected_bytes(history, users, rows):
    """Project a fresh fixture, separating dataset growth from session growth."""
    if not history:
        return 128 * MIB + 256 * rows + 32 * 1024 * users
    previous = history[-1]
    per_row = previous["seeded"] / previous["rows"]
    per_user = (previous["peak_pss"] - previous["seeded"]) / previous["users"]
    # Apply the marginal slope of the last two completed rungs when available.
    if len(history) > 1:
        older = history[-2]
        if previous["rows"] > older["rows"]:
            per_row = max(0, (previous["seeded"] - older["seeded"]) /
                          (previous["rows"] - older["rows"]))
        active = previous["peak_pss"] - previous["seeded"]
        old_active = older["peak_pss"] - older["seeded"]
        per_user = max(per_user, (active - old_active) / (previous["users"] - older["users"]))
    process = (previous["peak_pss"] + per_row * (rows - previous["rows"])
               + per_user * (users - previous["users"]))
    # PSS excludes socket memory; extrapolate sampled whole-host high-water
    # consumption as a second estimate when it exceeded the process measurement.
    extra_per_user = max(0, previous["host_delta"] - previous["peak_pss"]) / previous["users"]
    return math.ceil(1.25 * (process + extra_per_user * users))


def read_evidence(path):
    verdicts, memory, phases = [], {}, []
    run_id = None
    with path.open() as source:
        for line in source:
            record = json.loads(line)
            event = record["event"]
            phases.append(event)
            if event == "run":
                run_id = record["run"]
            if event in ("seeded", "connected", "result"):
                sample = record.get("server", {})
                if sample.get("units") == "KiB" and isinstance(sample.get("Pss"), int):
                    memory[event] = sample["Pss"] * 1024
            if event == "verdict":
                verdicts.append(record["verdict"])
    return verdicts, memory, phases, run_id


def judge(path, status):
    verdicts, memory, phases, _ = read_evidence(path)
    if len(verdicts) != 1:
        raise ValueError(f"expected one verdict, received {len(verdicts)}; runner exit {status}")
    verdict = verdicts[0]
    code = verdict["code"]
    if code not in STATUS or status != STATUS[code][1] or verdict["status"] != STATUS[code][0]:
        raise ValueError(f"inconsistent verdict {code!r}/{verdict.get('status')!r} and exit {status}")
    if phases[-1] != "verdict":
        raise ValueError("rung output continued after its verdict")
    if code == "pass":
        required = ["run", "seeded", "connected", "result", "closed", "verdict"]
        if [p for p in phases if p in required] != required or set(memory) != {"seeded", "connected", "result"}:
            raise ValueError("passing rung lacks ordered completion evidence or measured server memory")
    return verdict, memory


def error_tail(path, limit=2000):
    with path.open("rb") as source:
        source.seek(0, os.SEEK_END)
        source.seek(max(0, source.tell() - limit))
        return source.read().decode("utf-8", errors="replace").strip()


def stop_child(proc):
    if proc.poll() is not None:
        return
    # Let mac-rung unwind its ExitStack, kill the fixture and stop the Mac run.
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=50)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


def cleanup_worker(raw, args):
    # On an interrupted or malformed run, use the run ID already written by
    # this controller. Only this worker and this run can receive the stop.
    run = None
    with raw.open() as source:
        for line in source:
            try:
                rec = json.loads(line)
                if rec.get("event") == "run":
                    run = rec.get("run")
                    break
            except ValueError:
                continue
    if not isinstance(run, int):
        return "no run ID received"
    command = [str(args.bin_dir / "load-agent"), "call", str(args.key_file),
               f"wss://{args.control_host or args.mac}:{args.control_port}/",
               args.mac_pin, "worker.stop", json.dumps({"run": run})]
    try:
        reply = subprocess.run(command, capture_output=True, text=True, timeout=40)
        return "worker stopped" if reply.returncode == 0 else reply.stderr[-1000:]
    except (OSError, subprocess.TimeoutExpired) as error:
        return str(error)


def run_one(args, profile, users, seconds, history, record):
    orgs = org_count(users)
    rows = orgs * args.rows
    before = memory_available()
    reserve = int(args.reserve_gib * GIB)
    projected = projected_bytes(history, users, rows)
    fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    metadata = dict(profile=profile, users=users, orgs=orgs, seconds=seconds)
    record("preflight", **metadata, available_bytes=before, reserve_bytes=reserve,
           projected_bytes=projected, fd_limit=fd_limit)
    if fd_limit != resource.RLIM_INFINITY and users + 256 > fd_limit:
        return "resource_guard", ["local per-process descriptor limit"], 2
    if before - projected < reserve:
        return "resource_guard", ["projected fixture would cross the local memory reserve"], 2
    name = f"{users}-{profile}"
    raw = args.out_dir / f"{name}.jsonl"
    log = args.out_dir / f"{name}.stderr"
    command = [sys.executable, str(HERE / "mac-rung.py"), "--bin-dir", str(args.bin_dir),
               "--key-file", str(args.key_file), "--mac", str(args.mac), "--ryzen", str(args.ryzen),
               "--mac-pin", args.mac_pin, "--control-port", str(args.control_port),
               "--users", str(users), "--orgs", str(orgs), "--seconds", str(seconds),
               "--rows", str(args.rows), "--profile", profile,
               "--read-interval-ms", str(args.read_interval_ms), "--writes-per-second", str(args.writes_per_second)]
    command.extend(["--server-read-p99-ms", str(args.server_read_p99_ms),
                    "--server-write-p99-ms", str(args.server_write_p99_ms),
                    "--server-fanout-p99-ms", str(args.server_fanout_p99_ms),
                    "--min-load-fraction", str(args.min_load_fraction)])
    if args.control_host:
        command.extend(["--control-host", str(args.control_host)])
    minimum = before
    deadline = time.monotonic() + seconds + 600 + users / 20 + orgs / 2
    interrupted = None
    record("launch", **metadata, argv=command, raw=raw.name, stderr=log.name)
    with raw.open("x") as output, log.open("x") as errors:
        proc = subprocess.Popen(command, stdout=output, stderr=errors, start_new_session=True)
        try:
            while proc.poll() is None:
                minimum = min(minimum, memory_available())
                if minimum < reserve:
                    interrupted = ("resource_guard", "live local memory below reserve", 2)
                    break
                if time.monotonic() > deadline:
                    interrupted = ("runner_error", "rung exceeded total deadline", 1)
                    break
                time.sleep(.5)
        except (OSError, ValueError, RuntimeError) as error:
            interrupted = ("runner_error", str(error), 1)
        except KeyboardInterrupt:
            interrupted = ("interrupted", "operator interrupted the ladder", 130)
        finally:
            stop_child(proc)
    if interrupted:
        code, reason, status = interrupted
        record("interrupted_rung", **metadata, reason=reason, worker_cleanup=cleanup_worker(raw, args),
               lowest_available_bytes=minimum, runner_exit=proc.returncode)
        return code, [reason], status
    try:
        verdict, memory = judge(raw, proc.returncode)
    except (KeyError, ValueError, TypeError) as error:
        detail = error_tail(log)
        record("invalid_evidence", **metadata, reason=str(error), stderr_tail=detail, runner_exit=proc.returncode,
               worker_cleanup=cleanup_worker(raw, args))
        return "runner_error", [x for x in (str(error), detail) if x], 1
    record("rung", **metadata, verdict=verdict, lowest_available_bytes=minimum, runner_exit=proc.returncode)
    code = verdict["code"]
    if code == "pass":
        history.append(dict(users=users, rows=rows, seeded=memory["seeded"],
                            peak_pss=max(memory.values()), host_delta=max(0, before - minimum)))
    return code, verdict.get("reasons", []), STATUS[code][1]


def ladder(profiles, rungs, run, record):
    history = {p: [] for p in profiles}
    stops = {}
    highest = {p: None for p in profiles}
    for users in rungs:
        for profile in profiles:
            if profile in stops:
                continue
            try:
                code, reasons, status = run(profile, users, history[profile])
            except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                code, reasons, status = "runner_error", [str(error)], 1
            if code == "pass":
                highest[profile] = users
            else:
                stops[profile] = dict(users=users, code=code, reasons=reasons, exit=status)
                record("stop", profile=profile, **stops[profile])
            if status == 130:
                record("finished", highest_passed=highest, stops=stops, interrupted=True)
                return 130
    record("finished", highest_passed=highest, stops=stops, interrupted=False)
    if any(s["exit"] == 1 for s in stops.values()):
        return 1
    return 2 if stops else 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--bin-dir", required=True, type=Path)
    p.add_argument("--key-file", required=True, type=Path)
    p.add_argument("--mac", required=True, type=ipaddress.IPv4Address)
    p.add_argument("--ryzen", required=True, type=ipaddress.IPv4Address)
    p.add_argument("--mac-pin", required=True)
    p.add_argument("--control-host", type=ipaddress.IPv4Address)
    p.add_argument("--control-port", type=int, default=47671)
    p.add_argument("--profiles", nargs="+", choices=("uniform", "hot"), default=["uniform", "hot"])
    p.add_argument("--rungs", default="32,100,1000,10000,100000,250000,500000,750000,1000000")
    p.add_argument("--seconds", default=30, type=int)
    p.add_argument("--final-seconds", type=int, help="duration for the last requested population")
    p.add_argument("--rows", default=2000, type=int)
    p.add_argument("--reserve-gib", default=4.0, type=float)
    p.add_argument("--read-interval-ms", default=10000, type=int)
    p.add_argument("--writes-per-second", default=4, type=int)
    capacity_verdict.add_arguments(p)
    args = p.parse_args()
    try:
        rungs = [int(n) for n in args.rungs.split(",")]
    except ValueError:
        p.error("--rungs expects comma-separated integer populations")
    if (rungs != sorted(set(rungs)) or not 1 <= min(rungs) <= max(rungs) <= 1_000_000
        or len(set(args.profiles)) != len(args.profiles) or args.rows < 50
        or not 1 <= args.seconds <= 3600 or (args.final_seconds is not None and not 1 <= args.final_seconds <= 3600)
        or not math.isfinite(args.reserve_gib) or args.reserve_gib <= 0 or not 1 <= args.control_port <= 65535
        or not 1 <= args.writes_per_second <= 4 or args.read_interval_ms < 1000):
        p.error("invalid workload, profile, port or resource guard")
    args.bin_dir = args.bin_dir.resolve(strict=True)
    args.key_file = args.key_file.resolve(strict=True)
    args.out_dir.mkdir(parents=True, exist_ok=False)
    with (args.out_dir / "ladder.jsonl").open("x", buffering=1) as summary:
        def record(event, **data):
            line = json.dumps(dict(event=event, unix_s=time.time(), **data))
            print(line, file=summary)
            print(line, flush=True)
        try:
            version = subprocess.run(["git", "-C", HERE, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
            binaries = {}
            for name in ("load-agent", "watch-capacity"):
                with (args.bin_dir / name).open("rb") as binary:
                    binaries[name] = hashlib.file_digest(binary, "sha256").hexdigest()
            env = {key: os.environ.get(key) for key in ("WATCH_DISPATCH", "WATCH_ACTIVATION_LANES", "WATCH_ENCODING", "WATCH_SERVER_THREADS", "WATCH_CPU")}
            record("configuration", argv=sys.argv[1:], revision=version, binary_sha256=binaries, kernel=platform.release(),
                   environment=env, rungs=rungs, profiles=args.profiles, budgets=capacity_verdict.budgets(args), clock="Unix seconds")
            def run(profile, users, history):
                seconds = args.final_seconds if args.final_seconds and users == rungs[-1] else args.seconds
                return run_one(args, profile, users, seconds, history, record)
            return ladder(args.profiles, rungs, run, record)
        except (KeyboardInterrupt, OSError, ValueError) as error:
            record("aborted", reason=type(error).__name__ + ": " + str(error))
            return 130 if isinstance(error, KeyboardInterrupt) else 1


if __name__ == "__main__":
    def interrupted(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
