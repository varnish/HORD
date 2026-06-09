# HORD prototype — TODO

- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.

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

## Carapace integration — requirements on HORD

Carapace's direct listener (`carapace-direct`) is the intended host for HORD. Its
per-connection function `serve_conn` (`carapace-direct/src/handler.rs:57`) is
already generic over `tokio::io::AsyncRead + AsyncWrite + Unpin + Send + 'static`,
and `AsyncHordStream` implements `AsyncRead + AsyncWrite`, so at the HTTP layer the
two halves fit. **They do not fit at the runtime layer, and they do not yet deliver
the zero-copy story HORD exists for.** This section is the demand list: what HORD
must provide so the match is exact, not approximate. Versions are *not* the problem
— both sides are on tokio 1.x / hyper 1.x; the gaps below are structural.

Sequenced as three milestones (byte-stream parity → one-sided zero-copy →
zero-copy from MSE4 pages). Blocker 0 (the server runtime model) and Milestones 2–3
are complete; the open work is the residual review follow-ups below plus the
Carapace-side verification on Milestone 1.

### Deferred from the HordListener (Blocker 0) code review

The following remains consciously deferred — not a correctness bug on the supported
single-host path.

- [ ] **Synchronous handshake still pins the worker (now bounded, not unbounded).**
      With `ESTABLISH_TIMEOUT` + the existing `HANDSHAKE_TIMEOUT`, a stalled peer can no
      longer wedge a worker *forever*, but it can still stall that worker's other
      connections for up to those bounds (head-of-line). The deeper fix is the
      documented handshake **stage** — run establishment/handshake off the worker (or
      async) so a slow peer never blocks a worker's reactor. Deferred (bigger; the
      bound makes it non-urgent).

### Milestone 1 — HTTP/1.1 over RDMA (byte-stream parity)

This is the honest first integration: hyper runs unmodified over the stream, body
frames go out as RDMA SEND/RECV. **No zero-copy** — MSE4 still copies into `Bytes`
(`carapace-mse4/src/storage.rs`, `read_body` → `Bytes::copy_from_slice`) and HORD
copies that into a send buffer. The point is a working seam and a throughput number.

- [ ] **Long-lived QP, many requests.** The M2M API workload is many small requests;
      per-request QP setup (CM handshake + MR registration) would dominate. The
      byte-stream must carry HTTP/1.1 keep-alive transparently — N requests over one
      QP, with `AsyncRead` returning `Ok(0)` only on a real peer half-close, so
      hyper's keep-alive loop and Carapace's promotion-on-clean-EOF logic
      (`carapace-direct/src/body.rs` only promotes to the memory tier when the body
      drained to EOF) both behave exactly as on TCP.

      **HORD side confirmed (tested).** The EOF contract — `AsyncRead` returns `Ok(0)`
      *only* on a real half-close, never between pipelined requests — is documented on
      `AsyncHordStream::poll_read` and locked in by `keep_alive_many_requests_one_qp`
      in `hord-async/tests/listener.rs` (3 requests over one QP, then a clean
      half-close → EOF). The demo server (hyper, unmodified) serves N keep-alive GETs
      over one connection. **Remaining (Carapace side):** verify Carapace's own
      promotion-on-clean-EOF against the HORD stream once it integrates.

### Deferred from the M2/M3 zero-copy code review

A max-effort review of the M2 serve methods + M3 scatter-gather work fixed the
findings worth fixing. The following were consciously **deferred** — each touches
proven RDMA-semantic code or is a pure optimization, and none is a live bug on the
supported path:

- [x] **Unify the *sync* single-buffer transport path into the gather core.** Done.
      `begin_rdma_write_inner` is now a 1-segment shim (`WriteSegment::from_registered`
      → `begin_rdma_write_gather_inner`); the duplicate `WRITE_WR_MAX` chunking loop and
      the zero-length-imm special case are gone, leaving one WR-posting core in
      `hord-stream`. Verified over `rxe0` (single-buffer + split-mode wire tests pass).

- [x] **Reroute `post_write` / `post_write_with_imm` through `post_write_gather`.** Done.
      Both single-SGE primitives are now 1-SGE shims over `post_write_gather`; the
      `imm.to_be()` endianness encoding lives in exactly one place (the cross-referencing
      comments are gone). Verified by `hord-core/tests/rdma_write_smoke.rs` over `rxe0`.

- [x] **Batch a scatter-gather write that exceeds the send-pool cap.** Done. The
      blocking `rdma_write_gather_all` and the async `rdma_write_gather` now split a
      source whose WR count exceeds the send pool (more than `send_pool * max_send_sge`
      fragments) into consecutive send-pool-sized batches, draining each before posting
      the next, with the immediate riding the final WR of the *final* batch. A new
      `next_batch_len` planner (device-free unit tests in `plan_gather_tests`) cuts each
      batch at a segment boundary ≤ the pool's WR budget. The *non-blocking*
      `begin_rdma_write_gather` still returns `InvalidInput` over-cap (it can't drain
      without blocking), and a *single* segment over `send_pool * WRITE_WR_MAX` bytes
      (~16 GiB) still can't be split across batches — both documented on the entry
      points. Verified on rxe0: `over_cap_gather_batches_and_lands_contiguously`
      (hord-stream, split-mode imm) and `over_cap_async_gather_batches_and_lands_contiguously`
      (hord-async), both with `send_pool = 2`.

- [x] **An imm-only (zero-SGE) write-with-immediate primitive.** Done. `post_write_gather`
      now accepts an empty `sg_list` when `imm` is `Some` (verbs permits `num_sge == 0`
      for a write-with-imm), so the all-empty gather branch posts a true imm-only WR
      instead of borrowing `segments.first()`'s `(addr, lkey)` for a fake 0-length SGE.
      A literally empty `segments` slice with an immediate now delivers the bare transfer
      ID (was `InvalidInput`). Verified on rxe0: `rdma_write_imm_only_zero_sge`
      (hord-core, confirms rxe honours `num_sge == 0`, byte_len 0, imm delivered) and
      `split_mode_zero_length_body_delivers_imm` (hord-stream, empty-slice gather +
      `len == 0` single-buffer, both deliver the imm end to end).
