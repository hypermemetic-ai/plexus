# Native permission requests

Mount `permissions::serve(Permissions::new(book.clone()), server, "permissions")`
beside an activation that consults the same `Book`. The host supplies the
Plexus admin principal IDs, optional startup grants from
`plexus::Grant::load_file_from_env`, and a validator for supported methods
and exact target fields. A short-lived method can call `book.allows`. For a
stream, acquire `book.lease` before opening the resource and stop input and
output when `lease.revoked()` resolves.

A signed-in caller can use `request(method, target)`. The server takes the
requester's identity from the connection. Repeating a pending request for
the same grant returns its existing ID. `requests(pending?)` shows the
caller's own requests. A directly authenticated Plexus admin sees everyone's
requests and can call `approve(id)`, `deny(id)`, `grant(principal, method,
target)` and `revoke(principal, method, target)`. Applications that assign
an ordinary admin role still provision Plexus admins separately. A permission
request cannot add a Plexus admin.

Share one book per running service. The Plexus Store keeps request history
and grant overrides in `permissions.db` under `--data DIR` or `PLEXUS_DATA`.
Without a data directory the store is in memory. A durable revoke overrides
a startup file grant across restarts. Regranting permits a new connection.
Previously revoked leases stay closed.
