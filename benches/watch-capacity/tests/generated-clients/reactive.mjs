// Compile a renderless test component against the production plexus-ui/host
// runes. Each instance has its own component context and destroyable effect root.
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

export async function compileReactiveSession(stack, dir) {
  const dependencies = process.env.WATCH_NODE_UI_MODULES || join(stack,"plexus-ui/node_modules");
  const require = createRequire(join(dependencies,".resolve.cjs"));
  const {compile, compileModule} = require("svelte/compiler");
  const {build} = require("esbuild");
  const face = readFileSync(join(stack,"plexus-ui/cli/src/svelte/face.js"),"utf8");
  writeFileSync(join(dir,"face.js"), compileModule(face,{filename:"face.svelte.js",generate:"client"}).js.code);
  const component = `<script>
    import {setContext} from "svelte";
    import {BUS,service,live} from "./face.js";
    let {conn, Client, state} = $props();
    setContext(BUS, {service:conn});
    const Tickets = service(c => new Client(c));
    const page = live(() => Tickets.list());
    $effect.pre(() => {
      if (page.status === "ready") state.push(page.value);
      if (page.status === "error") state.fail(page.error);
    });
  </script>`;
  writeFileSync(join(dir,"Session.js"),compile(component,{filename:"Session.svelte",generate:"client",runes:true}).js.code);
  const entry = `import Session from "./Session.js";
    export function start(conn, Client, state) {
      return $effect.root(() => { Session(null, {conn, Client, state}); });
    }`;
  writeFileSync(join(dir,"entry.js"),compileModule(entry,{filename:"entry.svelte.js",generate:"client"}).js.code);
  const module = join(dir,"session.mjs");
  await build({entryPoints:[join(dir,"entry.js")],outfile:module,bundle:true,platform:"browser",
    format:"esm",conditions:["browser"],nodePaths:[dependencies]});
  const {start} = await import(pathToFileURL(module));
  return (conn,Client) => {
    let waiting, pending, failure;
    const stop = start(conn,Client,{
      push(page) {
        if (waiting) { waiting.resolve({value:page,done:false}); waiting=undefined; }
        else { if(pending) throw Error("unobserved intermediate page"); pending=page; }
      },
      fail(error) { failure=error; waiting?.reject(error); },
    });
    return {
      async next() {
        if(failure) throw failure;
        if(pending) { const value=pending; pending=undefined; return {value,done:false}; }
        waiting=Promise.withResolvers();
        return waiting.promise;
      },
      return() { stop(); return {done:true}; },
    };
  };
}
