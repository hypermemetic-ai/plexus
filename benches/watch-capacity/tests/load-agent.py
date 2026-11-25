"""Small end-to-end test with real generator, control TLS and fixture TLS."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

bin_dir = Path(sys.argv[1]).resolve()
agent = bin_dir / "load-agent"
fixture = bin_dir / "watch-capacity"


def run(*args, ok=True):
    p = subprocess.run([agent, *args], capture_output=True, text=True, timeout=10)
    if ok:
        assert p.returncode == 0, p.stderr
        return p.stdout.strip()
    assert p.returncode != 0


with tempfile.TemporaryDirectory() as directory:
    controller = Path(directory) / "controller.key"
    worker = Path(directory) / "worker.key"
    foreign = Path(directory) / "foreign.key"
    controller_pin = run("keygen", controller)
    worker_pin = run("keygen", worker)
    run("keygen", foreign)
    assert controller.stat().st_mode & 0o777 == 0o600
    assert run("principal", worker) == worker_pin
    run("keygen", worker, ok=False)
    service = subprocess.Popen(
        [agent, "serve", worker, "127.0.0.1:0", controller_pin, "127.0.0.1", "2"],
        stdout=subprocess.PIPE, text=True,
    )
    host = None
    try:
        url = json.loads(service.stdout.readline())["url"]

        def rpc(method, args, *, ok=True, key=controller):
            value = run("call", key, url, worker_pin, method, json.dumps(args), ok=ok)
            return json.loads(value) if ok else None

        def wait(r, phase):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = rpc("status", {"run": r})
                if state["phase"] == phase:
                    return state
                assert state["phase"] != "failed", state
                time.sleep(.01)
            raise AssertionError((phase, state))

        plan = {"first": 0, "count": 2, "total": 2, "orgs": 1, "seconds": 1}
        rpc("prepare", plan, ok=False, key=foreign)
        rpc("prepare", {**plan, "count": 3, "total": 3}, ok=False)
        r = rpc("prepare", plan)
        rpc("prepare", plan, ok=False)
        wait(r, "keys_ready")
        args = {"run": r}
        keys = rpc("keys", {**args, "offset": 0, "limit": 2})
        assert len(set(keys)) == 2
        assert rpc("keys", {**args, "offset": 1, "limit": 1}) == keys[1:]
        rpc("keys", {**args, "offset": 0, "limit": 1025}, ok=False)
        rpc("start", {**args, "unix-us": 0}, ok=False)
        host = subprocess.Popen(
            [fixture, "serve-external", "1", "50", "127.0.0.1", "127.0.0.1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        host.stdin.write("\n".join(keys) + "\n")
        host.stdin.close()
        urls, pin = host.stdout.readline().strip().split()
        ports = [int(u.rstrip("/").rsplit(":", 1)[-1]) for u in urls.split(",")]
        connect = {**args, "ports": ports, "pin": pin, "profile": "uniform", "rate": 1, "read-interval-ms": 1000}
        rpc("connect", {**connect, "ports": [0]}, ok=False)
        rpc("connect", connect)
        wait(r, "ready")
        start = {**args, "unix-us": rpc("clock", args) + 300_000}
        rpc("start", start)
        rpc("start", start)
        rpc("start", {**start, "unix-us": start["unix-us"] + 1}, ok=False)
        result = json.loads(wait(r, "result")["result"])
        assert result["sessions"] == 2
        assert result["expected_deliveries"] == result["deliveries"] == 2
        rpc("drop", args)
        wait(r, "closed")
        rpc("drop", args)
        r2 = rpc("prepare", plan)
        rpc("status", args, ok=False)
        rpc("stop", {"run": r2})
        wait(r2, "stopped")
        rpc("stop", {"run": r2})
        # Wrong fixture pin is rejected by the signed data path and terminates the generator.
        r3 = rpc("prepare", plan)
        wait(r3, "keys_ready")
        rpc("connect", {**connect, "run": r3, "pin": worker_pin})
        assert wait(r3, "failed")["error"]
        r4 = rpc("prepare", plan)
        wait(r4, "keys_ready")
        child_pid = int(subprocess.check_output(["pgrep", "-P", str(service.pid)], text=True).strip())
        service.terminate()
        service.wait(timeout=5)
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise AssertionError("generator survived worker termination")
        print("pinned worker and fixture, bounds, run barriers, repeat start, result, close, stop and wrong-pin failure passed")
    finally:
        for p in [host, service]:
            if p is not None and p.poll() is None:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=5)
