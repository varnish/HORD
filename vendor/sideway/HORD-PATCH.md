# Vendored sideway 0.4.3 + `migrate_id` patch

This is an unmodified vendored copy of [`sideway`](https://crates.io/crates/sideway)
**0.4.3** with one local addition, applied to the workspace via
`[patch.crates-io]` in the repo-root `Cargo.toml`.

## The patch

Adds `rdmacm::communication_manager::Identifier::migrate(&self, &Arc<EventChannel>)`
wrapping `rdma_migrate_id`, so an accepted connection can be moved to its own
event channel (the listener's channel only carries connect requests). Without it,
a threaded/looping acceptor races each worker's establish-wait on the shared
channel. See `SIDEWAY-PORT.md` and the upstream issue.

Changed files: `src/rdmacm/communication_manager.rs` only —
- `import rdma_migrate_id`;
- `Identifier._event_channel: Arc<EventChannel>` → `Mutex<Arc<EventChannel>>`
  (so `migrate` can swap it through `&self`, since `Identifier` is shared as `Arc`);
- new `MigrateError` / `MigrateErrorKind`;
- the `migrate` method.

`examples/`, `tests/`, and their `[dev-dependencies]` were removed from the
vendored copy (we build only the library); everything else is verbatim 0.4.3.

## Removing this patch

When the upstream `migrate_id` issue lands in a released sideway:

1. Delete `vendor/sideway/`.
2. Remove the `[patch.crates-io]` block from the root `Cargo.toml`.
3. Bump `sideway` in `hord-core/Cargo.toml` to the release that has it.

The `hord-core` call site (`Listener::accept`) is unchanged by removal as long as
the upstream method keeps the `migrate(&self, &Arc<EventChannel>)` shape.
