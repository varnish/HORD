# Vendored sideway 0.4.3 + `migrate_id` / `peer_addr` / `reject` patch

This is an unmodified vendored copy of [`sideway`](https://crates.io/crates/sideway)
**0.4.3** with three local additions, applied to the workspace via
`[patch.crates-io]` in the repo-root `Cargo.toml`.

## The patch

**1. `Identifier::migrate(&self, &Arc<EventChannel>)`** wrapping `rdma_migrate_id`,
so an accepted connection can be moved to its own event channel (the listener's
channel only carries connect requests). Without it, a threaded/looping acceptor
races each worker's establish-wait on the shared channel. See `SIDEWAY-PORT.md`
and the upstream issue.

**2. `Identifier::peer_addr(&self) -> Option<SocketAddr>`** wrapping
`rdma_get_peer_addr`, so the HORD listener (`hord-async::HordListener`) can label
each accepted connection with its peer's address — the `SocketAddr` it hands to
the per-connection service closure. Read-only (no syscall; reads the id's resolved
destination sockaddr) and additive, so it needs no error type.

**3. `Identifier::reject(&self) -> Result<(), RejectError>`** wrapping
`rdma_reject` (no private data), so a server that has accepted a `ConnectRequest`
id but then fails to set the connection up can refuse it explicitly. Without it,
`hord-core` dropped the id on a per-connection setup failure, leaving the peer to
wait out a connect timeout (and a half-open id to linger until timewait). Used by
`Listener::process_event`'s `ConnectRequest` arm on any post-ack setup failure.

Changed files: `src/rdmacm/communication_manager.rs` only —
- import `rdma_migrate_id`, `rdma_get_peer_addr`, and `rdma_reject`;
- `Identifier._event_channel: Arc<EventChannel>` → `Mutex<Arc<EventChannel>>`
  (so `migrate` can swap it through `&self`, since `Identifier` is shared as `Arc`);
- new `MigrateError` / `MigrateErrorKind` and `RejectError` / `RejectErrorKind`;
- the `migrate` method;
- the `peer_addr` method;
- the `reject` method.

`examples/`, `tests/`, and their `[dev-dependencies]` were removed from the
vendored copy (we build only the library); everything else is verbatim 0.4.3.

## Removing this patch

When the upstream `migrate_id` issue lands in a released sideway:

1. Delete `vendor/sideway/`.
2. Remove the `[patch.crates-io]` block from the root `Cargo.toml`.
3. Bump `sideway` in `hord-core/Cargo.toml` to the release that has it.

The `hord-core` call sites (`Listener::accept` → `migrate`, `Connection::peer_addr`
→ `peer_addr`, `Listener::process_event` → `reject`) are unchanged by removal as
long as the upstream methods keep the `migrate(&self, &Arc<EventChannel>)`,
`peer_addr(&self) -> Option<SocketAddr>`, and `reject(&self) -> Result<…>` shapes.
`peer_addr` and `reject` are plain `rdma_get_peer_addr` / `rdma_reject` wrappers and
may not land upstream on the same timeline as `migrate`; if not, keep just those
methods (and the import) as the residual patch rather than dropping the vendor tree.
