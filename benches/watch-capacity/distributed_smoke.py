#!/usr/bin/env python3
"""Run independent generator processes against a single local fixture."""

import argparse
import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import time


async def stop(process):
    if process.returncode is None:
        process.kill()
    await process.wait()


async def launch(stack, binary, *args, env=None):
    process = await asyncio.create_subprocess_exec(
        str(binary.resolve()), *map(str, args),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, limit=8 << 20, env=env,
    )
    stack.push_async_callback(stop, process)
    return process


async def event(worker, kind):
    line = await worker.stdout.readline()
    if not line:
        raise RuntimeError(f"generator exited before {kind}")
    message = json.loads(line)
    if message.get("event") != kind:
        raise ValueError(f"expected {kind}, received {message.get('event')}")
    return message


async def send(worker, **fields):
    worker.stdin.write(json.dumps(fields).encode() + b"\n")
    await worker.stdin.drain()


async def run(a):
    if not 1 <= a.workers <= a.users or not 1 <= a.orgs <= a.users:
        raise ValueError("invalid worker or organization count")
    async with AsyncExitStack() as stack:
        fixture = await launch(stack, a.bin, "serve-external", a.orgs, a.rows, "127.0.0.1", "127.0.0.1",
                               env={**os.environ, "WATCH_PROFILE": a.profile})
        workers = []
        first = 0
        for i in range(a.workers):
            size = a.users // a.workers + (i < a.users % a.workers)
            worker = await launch(stack, a.bin, "generator", first, size, a.users, a.orgs, a.seconds)
            workers.append((worker, first, size))
            first += size

        for worker, first, size in workers:
            for index in range(first, first + size):
                item = await event(worker, "principal")
                if item["index"] != index:
                    raise ValueError("invalid enrollment order")
                fixture.stdin.write(item["principal"].encode() + b"\n")
                await fixture.stdin.drain()
            await event(worker, "keys_ready")
        fixture.stdin.close()
        listener, pin = (await fixture.stdout.readline()).decode().strip().split()
        connect = dict(op="connect", pin=pin, urls=listener.split(","), profile=a.profile,
                       rate=a.rate, read_interval_ms=a.read_interval_ms)
        for worker, _, _ in workers:
            await send(worker, **connect)
        for worker, _, size in workers:
            if (await event(worker, "ready"))["sessions"] != size:
                raise ValueError("generator failed to open its range")

        offsets = []
        for worker, _, _ in workers:
            bounds = []
            for _ in range(5):
                before = time.time_ns() // 1000
                await send(worker, op="clock")
                remote = (await event(worker, "clock"))["unix_us"]
                after = time.time_ns() // 1000
                bounds.append((remote - after, remote - before))
            offsets.append(min(bounds, key=lambda pair: pair[1] - pair[0]))
        start = time.time_ns() // 1000 + 1_000_000
        for (worker, _, _), (low, high) in zip(workers, offsets):
            await send(worker, op="start", unix_us=start + (low + high) // 2)

        results = await asyncio.gather(*(event(w, "result") for w, _, _ in workers))
        expected_writes = a.seconds * a.rate
        expected_deliveries = 0
        def user_org(index):
            return 0 if a.profile == "hot" and index >= a.orgs and index % 5 == 0 else index % a.orgs
        population = [0] * a.orgs
        for index in range(a.users):
            population[user_org(index)] += 1
        for n in range(expected_writes):
            org = (0 if n % 5 != 4 else n // 5 % a.orgs) if a.profile == "hot" else n % a.orgs
            expected_deliveries += population[org]
        expected_reads = -(-(a.seconds * 1000 * a.users) // a.read_interval_ms)
        for field, count in (("sessions", a.users), ("deliveries", expected_deliveries),
                             ("expected_deliveries", expected_deliveries)):
            if sum(r[field] for r in results) != count:
                raise ValueError(f"aggregate {field} mismatch")
        for field, count in (("writes", expected_writes), ("reads", expected_reads)):
            if sum(r[field]["n"] for r in results) != count:
                raise ValueError(f"aggregate {field} mismatch")
        for worker, _, _ in workers:
            await send(worker, op="drop")
        for worker, _, _ in workers:
            await event(worker, "closed")
            if await worker.wait():
                raise ValueError("generator failed after drop")
        print(json.dumps(dict(workers=results, controller_to_worker_offset_bounds_us=offsets,
                              users=a.users, reads=expected_reads, writes=expected_writes,
                              deliveries=expected_deliveries)))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bin", type=Path, default=Path(__file__).parent / "target/release/watch-capacity")
    for name, default in [("users", 8), ("workers", 2), ("orgs", 3), ("rows", 50),
                          ("seconds", 1), ("rate", 8), ("read-interval-ms", 500),
                          ("deadline", 120)]:
        p.add_argument("--" + name, type=int, default=default)
    p.add_argument("--profile", choices=["hot", "uniform"], default="uniform")
    args = p.parse_args()
    asyncio.run(asyncio.wait_for(run(args), args.deadline))
