---
name: plexus-v2-policies
description: Implement authorization for native Layers-1 activations. Use for authenticated callers, method roles, context and resource admission, startup grant files, or Plexus admin review and revocation of runtime permissions.
---

# Author a native Plexus policy

Use this with the [v2 activation skill](../plexus-v2-activations/SKILL.md). Decide which question each layer answers.

| Question | Where the decision lives |
| --- | --- |
| Who is calling? | The authenticated transport supplies `Caller`. A context field with `#[from(caller)]` can resolve an application identity through `FromCaller`. Use the verified principal for authorization. |
| Who may invoke this method? | Declare `roles(reader = ["level"], operator = [reader, "fill"])` on the activation. Use `server.authorize_grants` to check the effective caller, grant name and eligible roles on each opening. A declared grant with no policy denies the call. |
| May the caller enter this tenant or child? | Put the typed keys and verified identity in a `#[derive(plexus::Context)]` and declare `context = StudioCx`. Its `#[open]` check applies to the activation and its descendants. |
| May this user operate on this exact object? | In the application, validate the method and target, check membership in the target org within the caller's session ceiling when applicable, then consult the resource grant. Do this before pairing, polling input, or starting work. |

## Load and enforce the same grant

In the native service, load entries before starting the listener:

```rust
let entries: Vec<plexus_permissions::Entry> =
    plexus::Grant::load_file_from_env("PLEXUS_GRANTS", None)?;
```

An unset variable selects the supplied defaults. An empty file selects zero entries. An unreadable or malformed file returns an error. This example file gives a delegate access to one owner's agent. Replace both placeholders with 64 lowercase hex digits from the respective public keys.

```text
principal(builder = "key:<builder-public-key-hex>")
principal(owner = "key:<owner-public-key-hex>")
grant(principal = builder, method = agent.connect,
      target = { org: "hypermemetic:", owner: owner, agent: "notes" })
```

Construct one `plexus_permissions::Book::new(roots, entries, validate)` and share it between the protected activation and `Permissions::new(book.clone())`. Supply a validator for each supported method and its exact target fields. The native agent's validator requires `agent.connect`, the configured org, a specific owner, and a specific agent name. Resolve the target used for pairing from the same checked values. An attachment takes its owner from the verified user.

For a single operation use `book.allows(principal, method, target)`. For a delegated stream acquire `book.lease(principal, method, target)` before entering the rendezvous. Stop **both directions** on `lease.revoked()`, including a controller still waiting for a peer. A regrant permits a fresh connection.

## Request, review, revoke

Mount the management activation with `plexus_permissions::permissions::serve(Permissions::new(book.clone()), server, "permissions")`. These are native RPC calls:

```text
builder → permissions.request(method="agent.connect", target={org, owner, agent})
builder → permissions.requests()                     // own history
root    → permissions.requests(pending=true)         // all pending requests
root    → permissions.approve(id) or permissions.deny(id)
root    → permissions.grant(principal, method, target) or permissions.revoke(principal, method, target)
```

The server derives the requester from the connection. Configure Plexus admin roots out of band. Ordinary app admins can request access; review and mutation require a directly authenticated Plexus admin. The standalone `plexus-agent` reads roots from `PLEXUS_ADMINS` and its org from `PLEXUS_AGENT_ORG`. TLS admission uses `--allow-key`. Supply `--data DIR` or `PLEXUS_DATA` to retain decisions and revocations across restarts.

Test with distinct owner, requester, unrelated member and reviewer identities. Verify requester-only visibility, a denied sibling target, approval on the next call, and revocation during a stream and after restart. The native source of record is `plexus/permissions/`; `axon/agent/src/policy.rs` applies it and `axon/agent/tests/permissions.rs` runs the workflow over TLS. Menger's published-RPC adapter uses the separate `MENGER_GRANTS` setting.
