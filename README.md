# plexus

Plexus RPC, from scratch, in one repository.

This is the monorepo of the new Plexus RPC (tracker: `PLEXUS/KAYOKEN-1`). It
starts empty on purpose: nothing is copied from the repositories that came
before it. Those stay as they are and are moved aside, unchanged, on the
owner's word.

The first thing built here is a 2D display, drawn with WebGPU (WebGL2 where
WebGPU is missing), that shows the Plexus Rust services that are running.
For now a service only has to exist: start, say its name, and be seen.

## Layout

| path | holds |
|---|---|
| `crates/` | the libraries a service is built from, one directory per crate |
| `services/` | Rust services, one binary crate per directory |
| `display/` | the 2D display of the running services |
| `scripts/` | the repository's checks, `scripts/naming-gate.sh` among them |

`Cargo.toml` is a virtual workspace over `crates/*` and `services/*`;
`display` joins it when its crate is written. The crate structure and the git
procedures are menger docs under the monorepo's ticket on the tracker, not files here.

## Rules

- A Plexus backend is a **service**. Every name in this tree says so;
  `scripts/naming-gate.sh` fails on any other.
- One project is pushed at a time, and pushing is the owner's word. This
  repository has no remote until the owner makes one.
- Every commit names its ticket.
