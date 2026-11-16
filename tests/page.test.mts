// plexus-rt in Node, the page's runtime, against the Rust kiln over a WebSocket.
// Run by tests/page.rs: node tests/page.test.mts WS_URL CODEC_WASM INTERFACE_HASH

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Codec, Connection, PlexusFailure, type Channel } from "../../plexus-rt/plexus-rt.ts";

const [url, wasm, hash] = process.argv.slice(2);
const codec = await Codec.load(readFileSync(wasm));
const enc = new TextEncoder(), dec = new TextDecoder();
const WINDOW = 256, TOP_UP = 128;

/// A connection over a WebSocket of Node's own, as a page makes one.
async function connect(expects = hash): Promise<{ conn: Connection; close(): void }> {
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";
  let on: (f: Uint8Array) => void = () => {};
  ws.onmessage = (e) => on(typeof e.data === "string" ? enc.encode(e.data) : new Uint8Array(e.data));
  const channel: Channel = { send: (f) => ws.send(dec.decode(f)), receive: (cb) => { on = cb; } };
  const conn = new Connection(channel, codec, expects);
  await new Promise((ok, no) => { ws.onopen = ok; ws.onerror = no; });
  return { conn, close: () => ws.close() };
}

const heated = async (conn: Connection): Promise<{ made: number; dropped: number }> => conn.call("kiln.heated", {});
const sleep = (ms: number) => new Promise((ok) => setTimeout(ok, ms));

test("the hello's interface hash matches", async () => {
  const { conn, close } = await connect();
  await conn.ready;
  assert.equal(conn.interface, hash);
  close();
});

test("a client of another interface is refused at hello", async () => {
  const { conn, close } = await connect("0000");
  await assert.rejects(conn.ready, new RegExp(`interface is ${hash}`));
  close();
});

test("a call", async () => {
  const { conn, close } = await connect();
  assert.deepEqual(await conn.call("kiln.get", {}), { id: 1, cone: 6, state: "cold" });
  assert.deepEqual(await conn.call("kiln.fire", { cone: 6 }), { id: 1, cone: 6 });
  close();
});

test("a failure", async () => {
  const { conn, close } = await connect();
  await assert.rejects(conn.call("kiln.fire", { cone: 11 }), (e: unknown) => {
    assert.ok(e instanceof PlexusFailure);
    assert.equal(e.code, "too-hot");
    assert.equal(e.payload, 11);
    return true;
  });
  await assert.rejects(conn.call("kiln.glaze", {}), { code: "not-found" });
  close();
});

test("a stream is paced by its reader, and leaving it cancels", async () => {
  const { conn, close } = await connect();
  const before = await heated(conn);
  let taken = 0, furthest = 0;
  for await (const r of conn.stream("kiln.heat", {})) {
    assert.deepEqual(r, { minute: taken * 5 });
    taken++;
    await sleep(5);
    const ahead = (await heated(conn)).made - before.made - taken;
    assert.ok(ahead <= WINDOW, `the server made ${ahead} more than the reader took`);
    furthest = Math.max(furthest, ahead);
    if (taken === TOP_UP + 8) break;   // past the first top-up, so the window is full again
  }
  assert.equal(furthest, WINDOW, "the server never filled the window");
  // The break sent cancel: the Rust stream is dropped, and makes no more.
  let after = await heated(conn);
  for (let i = 0; after.dropped === before.dropped && i < 2000; i++) {
    await sleep(1);
    after = await heated(conn);
  }
  assert.equal(after.dropped, before.dropped + 1, "the server's stream was not dropped");
  await sleep(20);
  assert.equal((await heated(conn)).made, after.made);
  close();
});
