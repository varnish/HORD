# HORD prototype — TODO

- [x] **§7.7 protocol splitting** — done (Pass 7). `IBV_WR_RDMA_WRITE_WITH_IMM`
      in the shim/core, `SPLIT_MODE_CAPABLE` negotiation, the opcode-demux
      dispatcher + transfer-credit recv headroom in the stream, `with_id` / split
      dispatch in `hord-zerocopy`, `SplitReceiver`, async `rdma_write_with_imm` /
      `next_split_completion`, and `--split` on the async demo. Verified
      end-to-end on Soft-RoCE (incl. the §7.7.7 mid-write failure path). The data
      plane still shares the one driver task (see the multi-waiter item below); a
      true separate data-plane consumer thread is the remaining follow-up.
- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.
- [ ] **§7.6 range requests** — small add-on on top of the zero-copy path.
- [ ] A zero-copy *source* buffer pool on the server (amortize registration —
      §8.3) instead of registering per response. Also covers the split-mode
      source registered per response.
- [ ] Concurrent independent read+write on one async stream (two tasks over
      `tokio::io::split`) needs a multi-waiter scheme on the completion fd. The
      same gap blocks a true HTTP-unaware split *data-plane* consumer running on
      its own thread; for now it shares the control plane's driver task.
- [ ] Half-close detection on the *synchronous* stream (the async path has it).
- [ ] True thread-per-core server (worker pool + `spawn_local`) instead of one OS
      thread per connection.

## Deferred from the Pass 7 (§7.7) code review

Surfaced by the high-effort review of the protocol-splitting work and consciously
deferred — none is a live bug on the supported single-task path. Context kept so a
future pass has what it needs.

- [ ] **Transfer credits are neither enforced nor advertised (spec §7.7.6).** The
      receiver pre-posts a fixed `split_credits` (default 8) recv-WR headroom, but
      (a) the count never travels on the wire — the 16-byte handshake carries only
      the `SPLIT_MODE_CAPABLE` bit — and (b) the sender's `begin_rdma_write_with_imm`
      bounds a write only against local free send slots, never against how many recv
      WRs the peer has posted. So `split_credits` is a *local sizing heuristic*, not
      flow control. **Failure:** a concurrent split sender (not the demo, which
      serialises over HTTP/1.1 keep-alive) with more in-flight write-with-immediates
      than the peer's posted recv WRs — reachable when split traffic runs alongside
      unread stream data holding recv slots, or when `send_pool_size` is configured
      above the recv headroom — hits RNR. Under the default `rnr_retry=7` (infinite)
      this *stalls* the transfer indefinitely; if `rnr_retry` is lowered the QP errors
      and the connection dies, with no diagnostic. **Fix:** carry a credit count in
      the handshake and add sender-side in-flight-transfer accounting (a real window)
      so an overrun back-pressures instead of RNR-ing. This is a protocol feature
      (wire-format change), not a local patch — hence deferred.

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
      the source-buffer-pool and multi-waiter items above.

- [ ] **`serve_zero_copy` (async demo) forks `hord_zerocopy::serve_rdma_write`.** The
      async server can't `.await` the sync library orchestration, so the §7.7 policy
      (too_large gate, id/negotiation gate, the zero-length 1-byte-source workaround,
      status mapping) is duplicated — and the zero-length workaround now lives in
      three layers (stream `begin_rdma_write_inner`, `serve_rdma_write`, demo
      `serve_zero_copy`). **Failure (maintenance):** a spec/policy change applied to
      the library silently won't reach the deployed async path. **Fix:** provide an
      async-capable orchestration in the library (generic over a sync/async write
      strategy, or an async variant in `hord-async`) and have both servers call it.
      Deferred because adding library API solely for a demo is the wrong altitude; a
      proper async orchestration is a feature.

- [ ] **Minor cleanup (low priority).** The `pattern()` LCG test helper is copy-pasted
      across 5 test modules in 3 crates — a shared test-support location would prevent
      cross-crate drift, but a dedicated crate for a 7-line fn is over-engineering for
      now. (Left as-is by design: the `begin_rdma_write`/`rdma_write_all`
      `_with_imm`/`_inner` wrapper trios and the C `hord_post_write`/`_with_imm` pair —
      named methods read better at call sites than threading an `Option<u32>`.)
