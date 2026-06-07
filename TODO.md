# HORD prototype — TODO

- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.
- [x] **Zero-copy *source* buffer pool on the server (§8.3) — done
      (`hord_zerocopy::SourcePool` + `serve_rdma_write_pooled`).** A per-connection,
      lazily-grown pool of reusable registered source buffers, so a connection that
      serves several zero-copy responses (split mode, or any HTTP keep-alive) reuses
      MR registrations instead of `ibv_reg_mr` per response. Lazy growth (register on
      first need, up to a cap) means a single-response connection costs exactly one
      registration — no worse than per-response — while a reused one pays only `cap`.
      `acquire` lends a `SourceLease` (owns its buffer + an `Rc`, so it is safe across
      an `.await`); an object past the slab or a momentarily-exhausted pool falls back
      to a one-off (§8.4), so the pool only tunes efficiency, never correctness. Both
      demo servers (sync via `serve_rdma_write_pooled`, async via `pool.acquire` in
      `serve_zero_copy`) are wired to it, covering the split-mode source too. Tested
      on rxe0 (`source_pool.rs`): 5 sequential responses reuse **one** registered
      buffer (0 fallbacks), and oversized objects fall back while staying correct.
- [x] **Multi-waiter completion-fd scheme — done (`AsyncHordStream::into_split`).**
      Concurrent independent read+write on one async stream (two tasks, replacing
      `tokio::io::split`) and a true HTTP-unaware split *data-plane* consumer on
      its own task both needed a multi-waiter scheme on the one completion fd.
      Done as a reactor split: one **pump** task owns the fd / drains the CQ /
      wakes all parked handles (wake-all), and `into_split` hands back
      `ReadHalf` + `WriteHalf` + `DataPlane` that re-park on a shared waker list
      instead of touching the fd. No transport/hord-core/wire change — pure
      `hord-async` (the `HordStream` state machine was already full-duplex-correct,
      it just lacked a second driving task). Tests (rxe0): `duplex.rs`
      (`async_full_duplex_split`, 16 MiB each way, reader+writer tasks) and
      `split_consumer.rs` (`split_data_plane_separate_task`, data plane on its own
      task concurrent with the HTTP control plane).
- [x] **Half-close detection on the *synchronous* stream — done.** The async path
      watched the CM channel; the sync path only noticed a peer via a flushed
      completion, so a blocked `read()` / `poll_completed_transfer()` could spin
      forever on a peer's *graceful* `rdma_disconnect` (which leaves our recv WRs
      un-flushed). `apply_peer` now flips the CM channel non-blocking and the blocking
      busy-poll (`pump`) consults it on a rate-limited cadence (`CM_DISCONNECT_POLL_SPINS`),
      marking the stream closed so `read` sees EOF / `poll_completed_transfer` returns
      `None`. The non-blocking path (async reactor) is untouched. Tested on rxe0
      (`sync_half_close_unblocks_read`, which hangs → fails without the fix).
- [x] **`--range` (single-range §7.6) wired into the async bins — done.** Mirrors the
      sync `server.rs` / `client.rs` using the existing demo-lib codec + base-offset
      fill/verify: `server_async` resolves the `Range` header (206 + `Content-Range`,
      416, or whole-object 200), composed with zero-copy via the offset-agnostic
      write (`PatternBody` and `serve_zero_copy` now carry an absolute base/len/total);
      `client_async` gains `--range`, sizes its buffer to the range, verifies at the
      absolute offset, and reports 416. Validated end-to-end on rxe0 (stream + zero-copy
      ranges, suffix ranges, 416).
- [x] **True thread-per-core server (worker pool + `spawn_local`) — done.** `server_async`
      now spawns a fixed pool of worker threads (one per core, `--workers N` to override),
      each a current-thread runtime + `LocalSet`; a blocking acceptor round-robins each
      accepted (`Send`) `Connection` to a worker over a channel, and the worker
      `spawn_local`s a `run_connection` task — so one worker drives many connections
      concurrently on one core (the 1:1 per-connection completion-channel + `AsyncFd`
      model; the N:1 demux in `113.md` remains a later fd-economy optimization, not a
      prerequisite). Replaces one-OS-thread-per-connection. Validated on rxe0: 1 worker
      multiplexes 6 concurrent connections, 4 workers serve 16, all integrity-verified.
      Caveat documented in `run_connection`: the synchronous handshake briefly pins a
      worker; a production version would handshake asynchronously.

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
      lazily in `apply_peer` only when split mode survives negotiation. (The
      source-buffer-pool and multi-waiter items it related to are now both done; this
      recv-side headroom item is still open.)

- [x] **`serve_zero_copy` (async demo) forked `hord_zerocopy::serve_rdma_write`** —
      done. The §7.7/§7.3 *policy* (too-large gate, split-vs-plain selection, the
      zero-length 1-byte-source workaround, status mapping) is now a single pure
      function `hord_zerocopy::RdmaWriteAction::decide`, called by both the sync
      `serve_rdma_write` and the async `serve_zero_copy`; each then just runs the
      returned plan with its own register/fill/write calls. The *mechanism* stays
      split (irreducible — one drives the blocking `rdma_write_all`, the other an
      `rdma_write` future), but the drift-prone policy is single-sourced, so a
      spec/policy change now reaches both paths. `decide` is device-free and
      unit-tested on every branch; the async path was re-verified end-to-end on rxe0
      (plain write, too-large 413, plain + split zero-length, 4-transfer split).
      Factored the *decision* rather than adding async library orchestration —
      keeps the altitude the original deferral flagged.

- [ ] **Minor cleanup (low priority).** The `pattern()` LCG test helper is copy-pasted
      across 5 test modules in 3 crates — a shared test-support location would prevent
      cross-crate drift, but a dedicated crate for a 7-line fn is over-engineering for
      now. (Left as-is by design: the `begin_rdma_write`/`rdma_write_all`
      `_with_imm`/`_inner` wrapper trios and the C `hord_post_write`/`_with_imm` pair —
      named methods read better at call sites than threading an `Option<u32>`.)
