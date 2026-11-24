"""Run against built plexus-layers-mcp and studio-kiln binaries. Uses loopback and temporary keys."""
import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import sys
import tempfile


async def launch(stack, *argv):
    proc = await asyncio.create_subprocess_exec(
        *argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        env={k: v for k, v in os.environ.items() if k != "PLEXUS_DATA"},
    )
    async def stop():
        if proc.returncode is None:
            proc.terminate()
        await proc.wait()
    stack.push_async_callback(stop)
    return proc


class Client:
    def __init__(self, proc):
        self.proc = proc
        self.seq = 0

    async def send(self, body):
        body["jsonrpc"] = "2.0"
        self.proc.stdin.write(json.dumps(body).encode() + b"\n")
        await self.proc.stdin.drain()

    async def rpc(self, method, params):
        self.seq += 1
        await self.send({"id": self.seq, "method": method, "params": params})
        reply = json.loads(await self.proc.stdout.readline())
        assert reply.get("id") == self.seq and "result" in reply, reply
        return reply["result"]

    async def call(self, method, **args):
        return await self.rpc("tools/call", {"name": "studio.kiln." + method, "arguments": args})


async def walkthrough(bridge, studio):
    async with AsyncExitStack() as stack:
        directory = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        keys, pins = {}, {}
        for name in ("studio", "kiln", "operator", "viewer"):
            key = directory / name
            with key.open("xb") as f:
                os.fchmod(f.fileno(), 0o600)
                f.write(os.urandom(32))
            proc = await launch(stack, studio, "principal", key)
            stdout, _ = await proc.communicate()
            assert proc.returncode == 0
            keys[name], pins[name] = key, stdout.decode().strip()

        async def listen(*args):
            proc = await launch(stack, studio, *args)
            url = (await proc.stdout.readline()).decode().strip()
            assert url.startswith("wss://127.0.0.1:"), url
            return url

        remote = await listen("kiln-grants", keys["kiln"], pins["studio"], pins["operator"], pins["viewer"])
        front = await listen("studio-grants", keys["studio"], pins["operator"], pins["viewer"], remote, pins["kiln"])

        async def connect(name):
            proc = await launch(stack, bridge, "--place", "studio[tenant=acme]",
                                "--wss", front, "--key-file", keys[name], "--server-key", pins["studio"])
            client = Client(proc)
            info = await client.rpc("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                    "clientInfo": {"name": "studio-test", "version": "1"}})
            assert "tools" in info["capabilities"]
            await client.send({"method": "notifications/initialized"})
            while True:
                tools = (await client.rpc("tools/list", {}))["tools"]
                if any(t["name"] == "studio.kiln.heat" for t in tools):
                    return client
                await asyncio.sleep(0.05)

        operator, viewer = await asyncio.gather(connect("operator"), connect("viewer"))
        denied = await viewer.call("heat", cone=6, hours=1)
        assert denied["isError"] and denied["structuredContent"]["code"] == "forbidden", denied
        result = await operator.call("heat", cone=6, hours=1)
        readings = result["structuredContent"]["items"]
        assert [r["minute"] for r in readings] == list(range(0, 61, 5))
        receipt = (await viewer.call("audit"))["structuredContent"]["value"]
        assert receipt["caller"] == pins["operator"] and receipt["via"] == pins["studio"], receipt
        assert receipt["started"] == receipt["completed"] == 1
        print("MCP → pinned WSS Studio → pinned WSS kiln: viewer denied, operator returned "
              "13 readings, kiln independently recorded operator via Studio, one dispatch.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: python3 tests/studio_wss.py /path/to/plexus-layers-mcp /path/to/studio-kiln")
    asyncio.run(asyncio.wait_for(walkthrough(*sys.argv[1:]), timeout=30))
