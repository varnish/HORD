# HORD prototype — TODO

## Remaining / future

- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.
- [ ] **§7.6 range requests** — small add-on on top of the zero-copy path.
- [ ] A zero-copy *source* buffer pool on the server (amortize registration —
      §8.3) instead of registering per response.
- [ ] Concurrent independent read+write on one async stream (two tasks over
      `tokio::io::split`) needs a multi-waiter scheme on the completion fd.
- [ ] **§5.3 Version-mismatch reject** — a peer that doesn't recognise the handshake
      version MUST reject with its own handshake (highest supported version) as
      reject private data. Currently the handshake is parsed but mismatch is not
      handled.
- [ ] **§11.2 RDMA write bounds validation** — verify that the server's RDMA
      write stays within the `addr`/`len` bounds the client advertised. The
      client trusts the server today; a malicious or buggy server could
      overwrite unrelated memory.
- [ ] **§11.3 DoS protections** — max connections per client IP/GID, idle
      connection timeouts, limits on total registered memory. None implemented.
- [ ] **§4.1.1 HTTP pipelining** — multiple in-flight GETs on one connection,
      responses returned in request order. The spec says this is "supported and
      expected" for prefetch controllers. The stream layer should support it but
      it has not been tested end-to-end.
- [ ] **§4.1.3 Upstream content normalization** — a HORD edge-cache server
      fetching from origins over standard HTTP must decompress
      (`Content-Encoding`), dechunk (`Transfer-Encoding`), and materialize a
      known `Content-Length` before serving over HORD. Not implemented.
- [ ] **§13.3 Python bindings** (`pyhord` crate) — PyO3 wrappers for the client,
      zero-copy GPU buffers, and a PyTorch `DataLoader` integration. Not started.

## Deferred from the Pass 7 (§7.7) code review

Surfaced by the high-effort review of the protocol-splitting work and consciously
deferred — none is a live bug on the supported single-task path. Context kept so a
future pass has what it needs.

- [ ] **Multi-WR split write (> `WRITE_WR_MAX` = 1 GiB) can skip the immediate.** The
      immediate rides only the final WR (`begin_rdma_write_inner`); if a *non-final*
      chunk's `ibv_post_send` fails mid-batch, the call returns `Err` having never
      posted the imm-bearing WR, so the peer's recv WR is never consumed and no
      data-plane completion is delivered. Both paths now recover via connection
      teardown — the async client via `peer_closed` → close, and the sync path too now
      that it has half-close detection (the sync half-close item above is done), so a
      sync data-plane consumer's `poll_completed_transfer` returns `None` instead of
      hanging. **Remaining (optional):** the narrower fix of surfacing an *error-bearing*
      completion on a partial-post failure rather than relying on teardown. Narrow
      (needs a > 1 GiB object *and* a mid-batch post failure).

- [ ] **Data-plane completion queue has no backpressure.** A `RecvRdmaWithImm`
      reposts its recv WR immediately (spec §7.7.5 *requires* this) and pushes the id
      onto an unbounded `completed_transfers` `VecDeque`, so a client that pipelines
      many split requests and drains slowly accumulates queued ids without the
      transport throttling it (unlike stream data, which holds its recv buffer until
      `read()`). Bounded by the client's own outstanding requests; entries are 4
      bytes. **Note:** cannot be fixed at the transport layer without violating the
      §7.7.5 "repost immediately" rule — any cap belongs to the data-plane consumer
      API, not the stream. Captured for awareness, likely WONTFIX at this layer.

- [ ] **Minor cleanup (low priority).** The `pattern()` LCG test helper still lives in
      three crates — `hord-core/tests/rdma_write_smoke.rs`,
      `hord-zerocopy/tests/split_http_smoke.rs`, and two `#[cfg(test)]` unit-test modules
      in `hord-stream/src/stream.rs`. The `hord-async` copies are now consolidated in
      `hord-async/tests/common` (a shared integration-test module). DRYing the rest is
      genuinely cross-crate: integration tests can't share code across crates and the
      `hord-stream` copies are in-`src` unit tests no `tests/common` can reach, so the
      only fix is a shared dev-only crate — over-engineering for a 7-line fn, deferred.
      (Left as-is by design: the `begin_rdma_write`/`rdma_write_all`
      `_with_imm`/`_inner` wrapper trios and the C `hord_post_write`/`_with_imm` pair —
      named methods read better at call sites than threading an `Option<u32>`.)
