# HORD prototype — TODO

- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.
- [ ] A zero-copy *source* buffer pool on the server (amortize registration —
      §8.3) instead of registering per response. Also covers the split-mode
      source registered per response.
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
- [ ] Half-close detection on the *synchronous* stream (the async path has it).
- [ ] Wire `--range` (single-range §7.6, done in the sync demo) into the async bins
      (`*_async`). The `Range`/`Content-Range` codec + base-offset fill/verify already
      exist in the demo lib; apply them in the async `serve_zero_copy` (now a thin
      executor over `RdmaWriteAction::decide` — see the resolved fork item below) and
      in `client_async`, mirroring the sync `server.rs` / `client.rs`.
- [ ] True thread-per-core server (worker pool + `spawn_local`) instead of one OS
      thread per connection.

## Deferred from the Pass 7 (§7.7) code review

Surfaced by the high-effort review of the protocol-splitting work and consciously
deferred — none is a live bug on the supported single-task path. Context kept so a
future pass has what it needs.

- [ ] **Multi-WR split write (> `WRITE_WR_MAX` = 1 GiB) can skip the immediate.** The
      immediate rides only the final WR (`begin_rdma_write_inner`); if a *non-final*
      chunk's `ibv_post_send` fails mid-batch, the call returns `Err` having never
      posted the imm-bearing WR, so the peer's recv WR is never consumed and no
      data-plane completion is delivered. The async client recovers via connection
      teardown (`peer_closed` → close); the residual is the *sync* path, which has no
      half-close detection (see the sync half-close item above). **Fix:** on a
      partial-post failure in split mode, guarantee the connection is observably
      closed on both paths (or surface an error-bearing completion). Narrow (needs a
      > 1 GiB object *and* a mid-batch post failure).

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
      lazily in `apply_peer` only when split mode survives negotiation. Interacts with
      the source-buffer-pool item above (the multi-waiter item is now done).

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
