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

- [ ] **Split recv headroom is pinned before negotiation.** `recv_wr_count` sizes and
      `post_all_recvs` posts `split_credits` extra recv WRs from the *local* config at
      construction (before the handshake), so a connection that advertises split mode
      pins `split_credits * max_message_size` (default 512 KiB) per connection even
      against peers that never negotiate it. Can't be revised post-handshake because
      receives must be pre-posted before the QP goes live (the two-phase RNR-avoidance
      design). **Fix:** register the split headroom into a *separate* MR and post it
      lazily in `apply_peer` only when split mode survives negotiation.

- [ ] **Minor cleanup (low priority).** The `pattern()` LCG test helper is copy-pasted
      across 5 test modules in 3 crates — a shared test-support location would prevent
      cross-crate drift, but a dedicated crate for a 7-line fn is over-engineering for
      now. (Left as-is by design: the `begin_rdma_write`/`rdma_write_all`
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

### Deferred from the second HordListener code review (recall pass, 2026-06-08)

The following remain consciously deferred.

- [ ] **`hord-async` carries server-only deps for embedders that want only the
      adapter.** `HordListener` pulled `log` + tokio `sync`/`macros` into what was a
      stream-adapter crate; a host that brings its own accept loop and wants only the
      `AsyncRead`/`AsyncWrite` adapter still compiles the listener and its deps. **Fix:**
      put the listener behind a `listener = ["dep:log", "tokio/sync", "tokio/macros"]`
      feature so the adapter dependency surface stays minimal. (Low priority — `log` is
      near-zero-cost; Carapace gates the whole `hord-async` dep behind its own feature.)

- [ ] **Test-support duplication.** `tests/listener.rs` adds a 6th copy of the
      `pattern_byte`/`pattern_vec` helpers (the `pattern()` LCG dup already tracked
      above) and of `current_thread_rt` (identical in 5 other `hord-async` test
      modules), and the acceptor/worker/demo each re-build a current-thread runtime
      with the same boilerplate — all candidates for a shared `tests/common` module + a
      `build_current_thread_rt()` helper.

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

- [ ] **Unify the *sync* single-buffer transport path into the gather core.** The
      async path already routes single-buffer writes through the gather core (a
      1-segment list), but `hord-stream` still keeps `begin_rdma_write_inner` (with its
      own `WRITE_WR_MAX` chunking and zero-length-imm WR) alongside
      `begin_rdma_write_gather_inner`. Making the single-buffer one a 1-segment shim and
      deleting the duplicate would leave one WR-posting core. Deferred: it touches the
      proven single-buffer path; the shared `check_write_capacity` already removes the
      *accounting* drift, leaving only the posting loop duplicated.

- [ ] **Reroute `post_write` / `post_write_with_imm` through `post_write_gather`.**
      The single-SGE primitives are now the 1-SGE special case of the gather primitive,
      and each re-encodes the `imm.to_be()` byte order (the cross-referencing comments
      admit the drift risk). Collapsing them onto `post_write_gather([one_sge])` would
      put the WR-construction + endianness in one place. Deferred (touches hord-core's
      verb-posting primitives).

- [ ] **Batch a scatter-gather write that exceeds the send-pool cap.** A source
      fragmented into more than `send_pool * max_send_sge` segments (defaults: 16 × ≤16
      = 256) currently fails with a non-retryable `InvalidInput` rather than being
      delivered in several drained batches. Documented on the gather entry points for
      now; batching is the real fix when an MSE4 workload proves it necessary.

- [ ] **An imm-only (zero-SGE) write-with-immediate primitive.** An all-empty gather
      with an immediate currently borrows `segments.first()`'s `(addr, lkey)` for a
      0-length SGE, and an *empty* segment list with an immediate is an `InvalidInput`
      rather than delivering the bare transfer ID. Verbs permits `num_sge == 0` for a
      write-with-imm; a dedicated imm-only WR would make the immediate-as-signal case
      stop masquerading as a degenerate data write. Niche; the current behavior is safe
      (a returned error, no UB).
