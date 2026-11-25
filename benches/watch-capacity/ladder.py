#!/usr/bin/env python3
"""PERF-12-shaped independent native WSS rungs. Writes JSONL and a stop reason."""
import argparse
import json
import os
from pathlib import Path
import platform
import resource
import signal
import subprocess
from capacity_verdict import Schedule, add_arguments, budgets, classify, exit_code

RUNGS = (1, 10, 100, 1_000, 10_000, 100_000, 250_000, 500_000, 750_000, 1_000_000)

def memory_available():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is missing")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-users", type=int, choices=RUNGS, default=1_000_000)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--tickets-per-org", type=int, default=2000)
    parser.add_argument("--profile", choices=["uniform","hot"], default="uniform")
    add_arguments(parser)
    args=parser.parse_args()
    if args.seconds < 1 or args.tickets_per_org < 50:
        parser.error("positive duration and at least 50 tickets per organization required")
    binary = args.bin.resolve(strict=True)
    port_range = list(map(int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split()))
    fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    env = {**os.environ, "WATCH_PROFILE":args.profile, "WATCH_SERVER_TIMING":"1", "WATCH_TIMING_SKIP_READS":"0"}
    workload = env.get("WATCH_WORKLOAD", "mixed")
    interval_ms = int(env.get("WATCH_READ_INTERVAL_MS", "10000"))
    writes_per_second = int(env.get("WATCH_WRITES_PER_SECOND", "4"))
    with args.out.open("x", buffering=1) as out:
        def record(kind, **values):
            row = json.dumps(dict(kind=kind, **values))
            print(row, flush=True)
            print(row, file=out)
        record("configuration", machine=platform.machine(), kernel=platform.release(),
               cpu=next(l.split(":",1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines() if l.startswith("model name")),
               ticks_per_sec=os.sysconf("SC_CLK_TCK"), mem_available=memory_available(),
               source_ports=port_range, fd_limit=fd_limit, load_average=os.getloadavg(),
               profile=args.profile, server_budgets=budgets(args), workload=workload,
               read_interval_ms=interval_ms, writes_per_second=writes_per_second)
        previous = None
        for users in RUNGS:
            if users>args.max_users:
                break
            orgs=min(users, max(2,(users+99)//100),10000)
            reason=None
            if users+100>=fd_limit:
                reason="per-process FD budget"
            elif min(users,8000)+100>=port_range[1]-port_range[0]+1:
                reason="ephemeral port budget for a loopback destination"
            elif previous:
                peak_kib=previous["server"]["load_end"]["peak_rss_kib"]+previous["generator_load_end"]["peak_rss_kib"]
                projected = int(peak_kib*1024*(users/previous["users"])*1.35)
                available = memory_available()
                if projected > available*0.60:
                    reason=f"projected combined peak {projected} bytes exceeds 60% of available {available} bytes; use external generators or reduce retained memory"
            if reason:
                record("stop",users=users,reason=reason)
                return 2
            with subprocess.Popen([str(binary),str(users),str(orgs),str(args.seconds),str(args.tickets_per_org)],
                    stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env,start_new_session=True) as proc:
                try:
                    stdout,stderr=proc.communicate(timeout=args.seconds+max(50,users//30))
                except BaseException:
                    os.killpg(proc.pid,signal.SIGKILL)
                    proc.wait()
                    record("stop",users=users,reason="rung interrupted or exceeded deadline")
                    raise
            if proc.returncode:
                record("stop",users=users,reason=stderr[-2000:] or f"exit {proc.returncode}")
                return 1
            result=json.loads(stdout)
            schedule = Schedule(users, orgs, args.seconds, interval_ms, writes_per_second, args.profile, workload)
            verdict = classify(result, schedule, budgets(args))
            record("rung", **result, verdict=verdict)
            if verdict["status"] != "pass":
                record("stop", users=users, reason=verdict["code"], verdict=verdict)
                return exit_code(verdict)
            previous=result
        record("finished",highest_measured=previous["users"])
    return 0

if __name__=="__main__":
    raise SystemExit(main())
