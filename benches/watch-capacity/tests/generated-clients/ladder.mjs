// Node 24+, OpenSSL, and the sibling plexus-rt checkout's ws package/wasm.
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { X509Certificate } from "node:crypto";
import { once } from "node:events";
import { copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { Agent } from "node:https";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { connect } from "node:tls";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

import { compileReactiveSession } from "./reactive.mjs";

const bench = fileURLToPath(new URL("../../", import.meta.url));
const stack = resolve(bench, "../../..");
const runtime = join(stack, "plexus-rt");
const require = createRequire(join(runtime, "package.json"));
const WebSocket = require(process.env.WATCH_NODE_WS || "ws");
const fixture = process.env.WATCH_NODE_FIXTURE || join(bench, "target/release/watch-capacity");
const gen = process.env.WATCH_NODE_GEN || join(stack, "plexus-gen/target/debug/plexus-gen");
const wasm = process.env.WATCH_NODE_CODEC || join(runtime, "dist/plexus_proto_web.wasm");
const rungs = process.argv.length > 2 ? process.argv.slice(2).map(Number) : [1, 10, 100, 1000];
assert(rungs.every(n => Number.isInteger(n) && n >= 1 && n <= 1000));
const rounds = Number(process.env.WATCH_NODE_ROUNDS || 5);
assert(Number.isInteger(rounds) && rounds > 0 && rounds <= 100);

function principal(pemOrDer) {
  const key = new X509Certificate(pemOrDer).publicKey.export({format:"jwk"});
  assert.equal(key.crv, "Ed25519");
  return "key:" + Buffer.from(key.x, "base64url").toString("hex");
}

function identity(dir) {
  const keyPath = join(dir,"current.key"), certPath = join(dir,"current.crt");
  execFileSync("openssl", ["req","-x509","-newkey","ed25519","-nodes","-subj","/CN=fixture-user",
    "-days","1","-keyout",keyPath,"-out",certPath], {stdio:"ignore"});
  const cert = readFileSync(certPath);
  return {key:readFileSync(keyPath), cert, principal:principal(cert)};
}

function pinnedAgent(identity, pin) {
  const agent = new Agent({keepAlive:false});
  agent.createConnection = (options, callback) => {
    // The TLS handshake proves possession. Check the exact fixture key before
    // passing the connection to HTTPS for the WebSocket upgrade.
    const socket = connect({...options, key:identity.key, cert:identity.cert,
      rejectUnauthorized:false, minVersion:"TLSv1.3", maxVersion:"TLSv1.3"});
    const fail = error => callback(error);
    socket.once("error", fail);
    socket.once("secureConnect", () => {
      try { assert.equal(principal(socket.getPeerCertificate().raw), pin); }
      catch (error) { socket.destroy(error); return; }
      socket.removeListener("error", fail);
      callback(null, socket);
    });
  };
  return agent;
}

function checkPage(page, org, revision) {
  assert.equal(page.org, org);
  assert.equal(page.sequence, BigInt(revision));
  assert.equal(page.rows.length, 50);
  for (const [id, row] of page.rows.entries()) {
    assert.equal(row.id, id);
    assert.equal(row.title, "Organization " + org + " ticket " + id);
    assert.equal(row.status, id === 0 && revision ? "Revision " + revision : "Ready");
  }
}

async function run(count) {
  const dir = mkdtempSync(join(tmpdir(),"plexus-generated-watch-"));
  const orgs = Math.min(count, 10);
  const sockets = [], agents = [];
  let child, childExit, timer;
  async function exercise() {
    copyFileSync(join(runtime,"plexus-rt.ts"),join(dir,"plexus-rt.ts"));
    const {Codec, Connection, socketChannel} = await import(pathToFileURL(join(dir,"plexus-rt.ts")));
    const codec = await Codec.load(readFileSync(wasm));
    const identities = Array.from({length:count}, () => identity(dir));
    assert.equal(new Set(identities.map(i=>i.principal)).size, count);
    child = spawn(fixture, ["serve",String(orgs),"50"], {stdio:["pipe","pipe","inherit"],
      env:{...process.env, WATCH_EVALUATION:"shared", WATCH_PROFILE:"uniform"}});
    childExit = once(child, "exit");
    child.stdin.end(identities.map(i=>i.principal).join("\n")+"\n");
    const lines = createInterface({input:child.stdout});
    const announcement = await lines[Symbol.asyncIterator]().next();
    assert.equal(announcement.done,false,"fixture exited before readiness");
    const [url,pin] = announcement.value.split(" ");
    assert.equal(new URL(url).hostname,"127.0.0.1");
    const bytes = {caller:0, activation:0};
    const connections = [];
    const started = performance.now();
    for (let start=0;start<count;start+=16) {
      const batch = await Promise.all(identities.slice(start,start+16).map(async id => {
        const agent = pinnedAgent(id,pin);
        agents.push(agent);
        const ws = new WebSocket(url, {agent, perMessageDeflate:false, handshakeTimeout:10_000});
        sockets.push(ws);
        const channel = socketChannel(ws,(side,frame)=>{bytes[side]+=frame.byteLength;});
        const conn = new Connection(channel,codec);
        await Promise.race([conn.ready, once(ws,"error").then(([err])=>Promise.reject(err))]);
        return conn;
      }));
      connections.push(...batch);
    }
    const connectMs = performance.now()-started;
    const ir = await connections[0].call("tickets._ir",{});
    const source = execFileSync(gen, ["ts","-"], {input:JSON.stringify(ir)});
    writeFileSync(join(dir,"tickets.ts"), source);
    const {TicketsClient} = await import(pathToFileURL(join(dir,"tickets.ts")));
    assert.equal(TicketsClient.interface,await connections[0].call("tickets._hash",{}));
    const clients = connections.map(conn=>new TicketsClient(conn));
    const useLive = process.env.WATCH_NODE_LIVE === "1";
    const mount = useLive ? await compileReactiveSession(stack,dir) : undefined;
    const handles = mount ? connections.map(conn=>mount(conn,TicketsClient)) :
      clients.map(client=>client.list().watch()[Symbol.asyncIterator]());
    const initialStart = performance.now();
    async function receive(revision) {
      await Promise.all(handles.map(async (handle,i)=>{
        const item = await handle.next();
        assert.equal(item.done,false);
        checkPage(item.value,i%orgs,revision);
      }));
    }
    await receive(0);
    const initMs = performance.now()-initialStart;
    const metrics = ()=>connections[0].call("bench.metrics",{});
    assert.equal((await metrics()).shared_subscriptions,count);
    const baseline = {...bytes};
    const updateStart = performance.now();
    for(let revision=1;revision<=rounds;revision++) {
      const replies = await Promise.all(clients.slice(0,orgs).map(client=>client.advance()));
      assert(replies.every(reply=>reply===BigInt(revision)));
      await receive(revision);
    }
    const changedMs = performance.now()-updateStart;
    const changedBytes = {caller:bytes.caller-baseline.caller,activation:bytes.activation-baseline.activation};
    await Promise.all(handles.map(h=>h.return()));
    const deadline = performance.now()+10_000;
    while((await metrics()).shared_subscriptions !== 0) {
      assert(performance.now()<deadline,"fixture subscriptions survived cancellation");
      await delay(10);
    }
    console.log(JSON.stringify({runtime:"Node "+process.version,transport:"loopback mTLS WSS",
      client_binding:useLive ? "Svelte plexus-ui/host live (renderless)" : "generated .watch()",
      interface:TicketsClient.interface,authenticated_sockets:count,distinct_principals:count,orgs,
      rows_per_page:50,initial_pages:count,changed_pages:rounds*count,writes:rounds*orgs,
      subscriptions_after_cancel:0,connect_ms:connectMs,initial_ms:initMs,changed_ms:changedMs,
      changed_pages_per_second:1000*rounds*count/changedMs,update_window_plexus_frame_bytes:changedBytes}));
  }
  try {
    await Promise.race([exercise(), new Promise((_,reject)=>{
      timer=setTimeout(()=>reject(Error("rung timed out: "+count)),120_000);
    })]);
  } finally {
    clearTimeout(timer);
    for(const socket of sockets) socket.terminate();
    for(const agent of agents) agent.destroy();
    if(child) {child.kill(); await childExit;}
    rmSync(dir,{recursive:true,force:true});
  }
}

for(const n of rungs) await run(n);
