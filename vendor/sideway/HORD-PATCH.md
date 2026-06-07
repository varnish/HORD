# Vendored sideway 0.4.3 + `migrate_id` / `peer_addr` patch

This is an unmodified vendored copy of [`sideway`](https://crates.io/crates/sideway)
**0.4.3** with two local additions, applied to the workspace via
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

Changed files: `src/rdmacm/communication_manager.rs` only —
- import `rdma_migrate_id` and `rdma_get_peer_addr`;
- `Identifier._event_channel: Arc<EventChannel>` → `Mutex<Arc<EventChannel>>`
  (so `migrate` can swap it through `&self`, since `Identifier` is shared as `Arc`);
- new `MigrateError` / `MigrateErrorKind`;
- the `migrate` method;
- the `peer_addr` method.

`examples/`, `tests/`, and their `[dev-dependencies]` were removed from the
vendored copy (we build only the library); everything else is verbatim 0.4.3.

## Removing this patch

When the upstream `migrate_id` issue lands in a released sideway:

1. Delete `vendor/sideway/`.
2. Remove the `[patch.crates-io]` block from the root `Cargo.toml`.
3. Bump `sideway` in `hord-core/Cargo.toml` to the release that has it.

The `hord-core` call sites (`Listener::accept` → `migrate`, `Connection::peer_addr`
→ `peer_addr`) are unchanged by removal as long as the upstream methods keep the
`migrate(&self, &Arc<EventChannel>)` and `peer_addr(&self) -> Option<SocketAddr>`
shapes. `peer_addr` is a plain `rdma_get_peer_addr` wrapper and may not land
upstream on the same timeline as `migrate`; if not, keep just that method (and the
two-line import) as the residual patch rather than dropping the vendor tree.
