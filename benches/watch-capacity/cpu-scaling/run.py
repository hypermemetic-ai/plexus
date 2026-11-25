#!/usr/bin/env python3
"""Measure one WSS fixture and its generator on distinct physical cores."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import select
import signal
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
TICKS = os.sysconf("SC_CLK_TCK")


def cpus(spec):
    result = set()
    for item in spec.split(","):
        ends = list(map(int, item.split("-")))
        result.update(range(ends[0], ends[-1] + 1))
    if not result or not result <= os.sched_getaffinity(0):
        raise ValueError(f"CPU list outside current affinity: {spec}")
    return result


def core(cpu):
    topo = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
    return tuple(int((topo / k).read_text()) for k in ("physical_package_id", "core_id"))


def ticks(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def affinity(pid, expected):
    tasks = list(Path(f"/proc/{pid}/task").iterdir())
    if any(os.sched_getaffinity(int(t.name)) != expected for t in tasks):
        raise RuntimeError(f"unexpected thread affinity for pid {pid}")


def trial(a, shim, lanes, reactor, workload, interval, server, generator):
    env = dict(os.environ, WATCH_CPU="1", WATCH_EVALUATION=a.evaluation,
               WATCH_PROFILE=a.profile, WATCH_ENCODING=a.encoding, WATCH_DISPATCH=a.dispatch,
               WATCH_ACTIVATION_LANES=str(lanes),
               WATCH_SERVER_THREADS=str(reactor), WATCH_GENERATOR_THREADS=str(a.generator_threads),
               WATCH_WORKLOAD=workload, WATCH_READ_INTERVAL_MS=str(interval),
               WATCH_WRITES_PER_SECOND=str(a.writes_per_second),
               WATCH_FIXTURE_CPUS=",".join(map(str, sorted(server))), LD_PRELOAD=str(shim))
    with tempfile.TemporaryFile(mode="w+") as output:
        p = subprocess.Popen([str(a.bin.resolve()), *map(str, (a.users, a.orgs, a.seconds, a.rows))],
                             env=env, stdout=output, stderr=subprocess.PIPE,
                             start_new_session=True, preexec_fn=lambda: os.sched_setaffinity(0, generator))
        deadline = time.monotonic() + a.timeout
        logs = []
        pending = b""
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([p.stderr], [], [], remaining)[0]:
                    raise subprocess.TimeoutExpired(p.args, a.timeout)
                chunk = os.read(p.stderr.fileno(), 65536)
                if not chunk:
                    break
                lines = (pending + chunk).split(b"\n")
                pending = lines.pop()
                for line in lines:
                    logs.append(line.decode(errors="replace"))
                    if line.startswith(b"opening "):
                        opening = ticks(p.pid)
                    elif line.startswith(b"idle:"):
                        opened = ticks(p.pid)
                        child = Path(f"/proc/{p.pid}/task/{p.pid}/children").read_text().split()
                        if len(child) != 1:
                            raise RuntimeError(f"expected one fixture, found {child}")
                        affinity(int(child[0]), server)
                        affinity(p.pid, generator)
                    elif line.startswith(b"load:"):
                        load_start = ticks(p.pid)
            if p.wait(timeout=max(0, deadline-time.monotonic())):
                raise RuntimeError("\n".join(logs + [pending.decode(errors="replace")])[-2500:])
            output.seek(0)
            result = json.load(output)
        finally:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            p.wait()
            p.stderr.close()
    phase = result["cpu_phases"]["load"]
    seconds = result["load_and_drain_s"]
    threads = [{"tid":t["tid"], "name":t["name"],
                "cpu_cores":t["scheduler_cpu_ns"] / 1e9 / phase["wall_s"]}
               for t in phase["threads"]]
    return dict(activation_lanes=lanes, wss_threads=reactor, workload=workload,
                interval_ms=interval, server_cpus=sorted(server), generator_cpus=sorted(generator),
                generator_threads=a.generator_threads, encoding=a.encoding, dispatch=a.dispatch, profile=a.profile, users=a.users, organizations=a.orgs,
                scheduled_seconds=a.seconds, load_and_drain_s=seconds,
                read_offered_s=result["offered_reads_per_s"],
                read_achieved_s=result["read_scheduled_to_receive"]["n"] / seconds,
                writes_achieved_s=result["writes"] / seconds,
                deliveries_achieved_s=result["commit_to_receive"]["n"] / seconds,
                server_cpu_cores=phase["process_cpu_ns"] / 1e9 / phase["wall_s"],
                generator_cpu_cores=(result["generator_load_end"]["cpu_ticks"]-load_start) / TICKS / phase["wall_s"],
                generator_open_cpu_s=(opened-opening)/TICKS, server_threads=threads, result=result)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bin", type=Path, default=HERE.parent/"target/release/watch-capacity")
    p.add_argument("--server-cpus", required=True)
    p.add_argument("--generator-cpus", required=True)
    p.add_argument("--lanes", default="1,2,4,8,12")
    p.add_argument("--wss-threads", default="2,8")
    p.add_argument("--workloads", default="reads,mixed")
    p.add_argument("--intervals-ms", default="20")
    p.add_argument("--profile", choices=["uniform","hot"], default="uniform")
    p.add_argument("--encoding", choices=["shared","per-call"], default="shared")
    p.add_argument("--dispatch", choices=["tenant","concurrent"], default="tenant")
    p.add_argument("--evaluation", choices=["shared","per-watch"], default="shared")
    for k,v in (("users",240),("orgs",24),("rows",100),("seconds",3),
                ("writes-per-second",4),("generator-threads",8),("repeats",1),("timeout",180)):
        p.add_argument("--"+k, type=int, default=v)
    a=p.parse_args()
    server,generator=cpus(a.server_cpus),cpus(a.generator_cpus)
    if {core(c) for c in server} & {core(c) for c in generator}:
        p.error("server and generator must use disjoint physical cores, including SMT siblings")
    print(json.dumps(dict(type="environment",cpu=next(line.partition(":")[2].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),kernel=platform.release(),
              binary_sha256=hashlib.sha256(a.bin.read_bytes()).hexdigest(),ticks_per_second=TICKS,
              server_physical_cores=len({core(c) for c in server}),
              generator_physical_cores=len({core(c) for c in generator}))),flush=True)
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        shim=Path(tmp)/"fixture-affinity.so"
        subprocess.run(["cc","-O2","-Wall","-Werror","-fPIC","-shared",str(HERE/"fixture-affinity.c"),"-o",str(shim)],check=True)
        for repeat in range(a.repeats):
            for interval in map(int,a.intervals_ms.split(",")):
                for workload in a.workloads.split(","):
                    for reactor in map(int,a.wss_threads.split(",")):
                        for lane in map(int,a.lanes.split(",")):
                            settings=dict(repeat=repeat,interval_ms=interval,workload=workload,wss_threads=reactor,activation_lanes=lane)
                            try:
                                value=trial(a,shim,lane,reactor,workload,interval,server,generator)
                            except (RuntimeError,subprocess.TimeoutExpired) as e:
                                failures += 1
                                print(json.dumps(dict(type="failure",**settings,error=str(e))),flush=True)
                            else:
                                print(json.dumps(dict(type="result",repeat=repeat,**value)),flush=True)

    return int(failures > 0)


if __name__=="__main__":
    raise SystemExit(main())
