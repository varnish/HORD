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
zero-copy from MSE4 pages), with a hard blocker in front of all of them.

> **Status (2026-06-07): Blocker 0 is done.** `hord_async::HordListener` ships the
> thread-per-core topology + the `FnMut(AsyncHordStream, SocketAddr) -> impl Future`
> serve closure + a `watch`-driven graceful shutdown (`hord-async/src/listener.rs`).
> The two Milestone-1 *properties* that ride the same seam — keep-alive EOF and
> `poll_write` backpressure — are confirmed, tested (`hord-async/tests/listener.rs`)
> and documented; the rest of Milestone 1 (Carapace-side promotion, feature-gated
> CI) and Milestones 2–3 are untouched. The demo server now runs on `HordListener`
> (`hord-demo/src/bin/server_async.rs`), so it doubles as the integration example.

### Blocker 0 — server runtime model (`!Send` is a wall, not a wrinkle)

- [x] **Provide a server listener that works under a work-stealing runtime.**
      `AsyncHordStream` is `!Send` by construction — its registered buffers hold raw
      pointers and the stream is pinned to the thread that built it
      (`hord-async/src/lib.rs:19-26`). Carapace's direct service accepts on pingora's
      *multi-threaded* runtime and drives each connection with `tokio::spawn`
      (`carapace-direct/src/service.rs:117`), which **requires `Send`**. A
      `TcpStream` → `AsyncHordStream` swap therefore does not compile — this is the
      single biggest piece of work and it is HORD's to solve, because the
      thread-affinity is intrinsic to how the CQ is polled, not an accident of the
      current code. The SPEC §13.2 sketch (`tokio::spawn(serve_connection(stream,…))`)
      is aspirational and would not compile against today's `!Send` stream; the
      `HordListener`/`HordConnector` types it shows do not exist yet.

      **Demand:** ship a real `HordListener` that *owns the runtime topology* —
      a pool of acceptor threads, one current-thread runtime / `LocalSet` per core,
      each its own completion domain — and lets the host run a per-connection service
      via `spawn_local` without ever requiring the stream to cross a thread. The
      cleanest shape for Carapace: HORD owns the threads and accept loop, and Carapace
      hands in a `FnMut(AsyncHordStream, SocketAddr) -> impl Future` closure (a
      `!Send`-friendly `serve_conn` variant). Carapace will add the `!Send` variant on
      its side; HORD must make the topology a supported, documented mode rather than
      something each embedder reinvents.

      **Done.** `hord_async::HordListener` (`hord-async/src/listener.rs`):
      `bind(ip, port, config)` → `.workers(n)` / `.grace_timeout(d)` →
      `serve(shutdown, serve_fn).await`, where `serve_fn` is exactly
      `FnMut(AsyncHordStream, SocketAddr) -> impl Future` (`+ Clone + Send + 'static`;
      the returned future is `!Send` and `spawn_local`d on its worker). One acceptor
      thread (own current-thread runtime, parks on the listener's CM fd) round-robins
      the still-`Send` `Connection` to a thread-per-core worker pool; each worker
      builds the `!Send` stream on its own thread and drives many connections via its
      own `LocalSet` + completion domain (the 1:1 CQ-fd model). The stream never
      crosses a thread. Carapace still owns the matching `!Send` `serve_conn` variant
      on its side. (`HordConnector` — a client-side analogue — remains unbuilt; not
      needed for the server seam.)

- [x] **Cooperate with an externally-driven graceful shutdown.** The direct service
      drains in-flight connections on a shutdown watch, bounded by a 30 s timeout
      (`carapace-direct/src/service.rs:137-159`, via
      `hyper_util::server::graceful::GracefulShutdown`). `HordListener` must accept a
      shutdown signal (a `tokio::sync::watch` receiver is enough — do not couple to
      pingora types), stop accepting, and let in-flight connections finish their
      current response before the QPs are torn down. A QP ripped mid-`RDMA_WRITE` is a
      torn delivery; draining is not optional.

      **Done.** `serve` takes a `tokio::sync::watch::Receiver<bool>` (pingora's
      `ShutdownWatch` *is* one — pass it straight through; no pingora coupling). On a
      flip to `true` (or a dropped sender) the acceptor stops accepting via a
      non-blocking `Listener::try_accept` parked on the CM fd (new in `hord-core`, so
      the signal interrupts accepting instead of blocking in the CM channel), the
      worker channels close, and each worker waits for its in-flight connection tasks
      to finish — bounded by `grace_timeout` (default 30 s, matching the direct
      service). `serve` resolves once every worker has drained, so a host `async fn`
      `.await`s it as the tail of its own shutdown. The **per-connection** "finish the
      current response" drain stays the host's job (it captures its own shutdown handle
      in `serve_fn` and drives its HTTP layer's graceful shutdown); HORD's timeout is
      the backstop — documented on `HordListener`. Verified end-to-end (keep-alive +
      Ctrl-C drain) by `hord-async/tests/listener.rs` and the demo server.

### Deferred from the HordListener (Blocker 0) code review

A max-effort review of the Blocker 0 work **fixed six findings**: the `serve()`
cancellation thread-leak (a `StopOnCancel` drop guard now winds the threads down if
the future is dropped); the unbounded `accept_finish` establishment wait (now bounded
by `ESTABLISH_TIMEOUT`, so a half-open peer can't wedge a worker forever); the
acceptor error-spin on a fatal CM event (now only `clear_ready`s on a clean drain,
backs off, and terminates after `MAX_CONSECUTIVE_ACCEPT_ERRORS` — though the recall
pass below found this fix was incomplete and device removal still hung; now fixed);
the `peer_addr`
AF_INET6 out-of-provenance read (now copies through a `cm_id`-wide pointer); and two
flaky/lossy tests (listener binds before the serving thread spawns; the keep-alive
test cleans up before asserting).

A **follow-up cleanup pass** then cleared the cheap items: the demo installs an
`env_logger` backend (so `HordListener` diagnostics surface again — the pre-refactor
demo printed them via `eprintln!`); the acceptor caps its per-wakeup drain
(`MAX_DRAIN_PER_WAKEUP`) so a connection flood can't starve the shutdown signal;
`ListenerFd` was folded into the crate's `ReactorFd`; `HordStream::connect` shares the
`qp_sizing` helper with the accept paths; and the worker uses a `tokio::task::JoinSet`
(O(1) reaping instead of an O(n²) `retain`, and it surfaces handler panics instead of
swallowing them).

The following remain consciously deferred — none is a correctness bug on the
supported single-host path.

- [x] **Idle keep-alive connections pay the full `grace_timeout` at shutdown.**
      `HordListener` stops accepting and bounds the drain, but did not signal the
      per-connection service to wind down; an idle keep-alive connection parked in
      `hyper` awaiting the next request only ended when the client closed or `grace`
      (30 s) elapsed. The contract puts per-connection drain on the host (capture your
      own shutdown handle in the serve closure and drive hyper's `GracefulShutdown`),
      but the demo didn't wire it, so Ctrl-C with an idle client appeared to hang for
      `grace`. **Fix:** wire the demo's per-connection graceful shutdown; consider
      passing the serve closure a cancel token so the common case isn't a footgun.

      **Done (both halves).** The serve closure now *receives* the cancel signal so
      embedders don't reinvent (and forget) the clone-and-capture: `serve_fn`'s
      signature gained a third argument — a clone of the same shutdown
      `watch::Receiver<bool>` — handed to every connection (`worker_loop` clones it
      per `spawn_local`). A closure that ignores it still works; HORD's `grace` stays
      the backstop. The demo's `serve_connection` now drives hyper's per-connection
      graceful shutdown off it: it `select!`s the pinned `http1` connection against
      the receiver and, on a flip to `true` (or a dropped sender), calls
      `Connection::graceful_shutdown()` then drives the connection to completion — so
      the in-flight response finishes and the keep-alive loop ends at once instead of
      parking for `grace`. An already-set signal is honoured without waiting for a
      `changed()` edge. Module + `serve` docs updated (the old "capture your own
      clone" advice replaced by the third-arg contract). Regressed by
      `idle_keep_alive_drains_promptly_on_shutdown` in `hord-async/tests/listener.rs`
      (client makes one request then sits idle on the keep-alive; shutdown fires while
      the server is parked; asserts `serve()` returns far inside the 10 s grace —
      proving the signal, not the timeout, drove the drain). The deeper
      handshake-stage item below stays open; this is the per-connection drain only.

- [ ] **`peer_addr` unknown-peer is a lossy sentinel.** The acceptor folds a
      missing/unmappable peer address into `UNKNOWN_PEER` (`0.0.0.0:0`), so a host
      can't distinguish "unknown" from a peer that is literally `0.0.0.0:0`, and
      anonymous peers all alias to one identity — the seam the cross-cutting
      multi-tenant trust model (below) rides on. **Fix when that lands:** surface
      `Option<SocketAddr>` (ideally plus the GID) to the service rather than a default.

- [x] **Unbounded acceptor→worker channel + blind round-robin.** `dispatch` round-robins
      over an unbounded `mpsc`, skipping only *closed* channels, not a merely-wedged
      worker (e.g. one in its now-bounded ~10 s handshake/establish wait, or monopolized
      by a long zero-copy write). A wedged worker silently accumulates a backlog of
      accepted connections, each already holding a live QP/CM id. **Fix:** bounded
      channel with `try_send` + failover to the next live worker, or least-loaded
      selection. Pairs with the handshake-stage item below.

      **Done.** Each worker channel is now bounded (`WORKER_CHANNEL_CAP = 32`) and
      `dispatch` uses `try_send`: a worker whose queue is *full* is skipped exactly like
      a *closed* (dead-thread) one, so a connection fails over to the next live, non-full
      worker instead of piling unboundedly behind a wedged one. If every worker is full
      or gone the connection is shed (its `Drop` issues a graceful disconnect) rather
      than blocking the single accept loop — back-pressure on accept under sustained
      overload; the bounded queues keep the half-open-QP backlog finite. The deeper
      **handshake-stage** item below (run establishment off the worker so a slow peer
      never wedges it in the first place) remains open — this bounds and routes around a
      wedged worker; it doesn't stop a worker from wedging.

- [ ] **Synchronous handshake still pins the worker (now bounded, not unbounded).**
      With `ESTABLISH_TIMEOUT` + the existing `HANDSHAKE_TIMEOUT`, a stalled peer can no
      longer wedge a worker *forever*, but it can still stall that worker's other
      connections for up to those bounds (head-of-line). The deeper fix is the
      documented handshake **stage** — run establishment/handshake off the worker (or
      async) so a slow peer never blocks a worker's reactor. Deferred (bigger; the
      bound makes it non-urgent).

### Deferred from the second HordListener code review (recall pass, 2026-06-08)

A second, recall-oriented review of the Blocker 0 work **fixed three acceptor
control-flow bugs the first review's fix had left incomplete** (commit alongside
this note):

- **`DeviceRemoval` is now terminal in the async acceptor.** The previous pass
  claimed device removal "terminates after `MAX_CONSECUTIVE_ACCEPT_ERRORS`", but
  it was folded into the *transient* error counter as a generic `io::Error::other`
  string, so after the one removal event was acked the next poll read empty
  (`Ok(None)`), reset `consecutive_errors` to 0, and the acceptor re-parked on a
  dead fd **forever** — the cap never tripped. Fixed by carrying a typed
  `hord_core::DeviceRemoved` (recognised via `is_device_removed`); the acceptor now
  stops immediately, matching the blocking `Listener::accept` contract.
- **The error cap can no longer be evaded by an interleaved empty poll.** `Ok(None)`
  (drained-empty) no longer resets `consecutive_errors`; only a real accept
  (`Ok(Some)`) does. A persistent-but-punctuated CM error now climbs to the cap
  instead of looping at ~10 attempts/sec forever.
- **An already-set shutdown is honoured at `serve()` entry.** The acceptor now checks
  `*shutdown.borrow()` before the accept loop; `watch::changed()` does not fire for
  the value present at receiver creation, so a receiver that is already `true` (e.g.
  `watch::channel(true)`) would otherwise have been ignored until a later toggle.
  Regression-tested by `shutdown_already_set_returns_promptly` in
  `hord-async/tests/listener.rs`.

The following remain consciously deferred.

- [x] **🔴 Use-after-free / torn delivery when a connection task is aborted
      mid-`RDMA_WRITE`.** `worker_loop`'s grace-timeout drain aborts in-flight
      connection tasks by dropping the `JoinSet` (`hord-async/src/listener.rs`, the
      `timeout(grace, drain)`). If a task is parked inside `poll_rdma_write` with a
      posted-but-unreaped write WR (the documented backpressure case: a slow /
      non-reading RDMA peer), the abort drops the future and, because the nested
      hyper/service future holding the **source** `RegisteredBuffer` drops before the
      outer `SharedAsyncStream` locals, the source MR is deregistered and its storage
      freed **while the QP still has the outstanding write** — the NIC can DMA-read
      freed memory and the peer gets a torn write. The careful drain in
      `poll_rdma_write` / `rdma_write_all` (which exists *precisely* to prevent this on
      every poll-return path) is bypassed by task abort, and `HordStream::Drop`'s
      "destroy the QP before deregistering buffers" ordering only covers the stream's
      own send/recv pools, not a separately-owned source buffer.

      **Done (candidate b — force synchronous QP teardown before the abort).** This
      is the option that generalises: it makes the NIC quiescent regardless of who
      owns the source storage (so it also covers the caller-owned external MRs of
      Milestone 3, which option (a) — "HORD pins the storage" — cannot, and without
      (c)'s per-buffer in-flight tracking or a `Drop` that has to drive the CQ).
      `HordStream::teardown_handle()` hands back a `ConnTeardown` (an `Arc<Connection>`
      whose `force_teardown()` calls the idempotent `Connection::shutdown` →
      `ibv_destroy_qp`), surfaced through `AsyncHordStream::teardown_handle()`.
      `worker_loop` keeps one `ConnTeardown` per live task, keyed by `tokio::task::Id`
      (pruned as tasks finish, so it never pins a dead connection's resources). When
      the grace drain times out, it force-tears-down every still-in-flight
      connection's QP **before** dropping the `JoinSet` — so by the time an aborted
      future frees its source buffer, the QP is gone and no DMA can race the free.
      Mirrors `HordStream::Drop`'s "stop the NIC before touching buffers" invariant,
      extended to externally-owned buffers; no hot-path or steady-state memory cost.
      Regressed by `shutdown_mid_backpressured_rdma_write_is_safe` in
      `hord-async/tests/listener.rs` (split-write into a non-draining peer → server
      wedges mid-`RDMA_WRITE` → listener shutdown → asserts the abort path ran and
      `serve()` returned bounded by the grace window, no torn-teardown panic/hang).
      Hosts should still drive the per-connection graceful drain (the idle-keep-alive
      item above) so the abort stays a true last resort, but the UAF is now closed
      even when it is reached.

- [x] **`try_accept` abandons sibling connect-requests and leaks a cm_id on a
      per-connection setup failure.** A `process_event` error for one `ConnectRequest`
      (e.g. `Endpoint::build` / `migrate` failing) makes `try_accept` return `Err`
      before draining other requests queued in the same fd wakeup (they are not lost —
      the fd stays readable — but are delayed behind the 100 ms backoff), and the
      already-acked+migrated cm_id is dropped without an explicit `rdma_reject`, so that
      peer waits out a connect timeout and a half-open id lingers until timewait.
      **Fix:** isolate per-connection setup failures from the listener error cap — log
      + reject that one connection and keep draining the rest — rather than treating a
      single bad peer as a listener-level error. Pairs with the unbounded-channel item.

      **Done.** `process_event`'s `ConnectRequest` arm now runs all post-ack setup (its
      own CM channel, the migrate, the `Endpoint::build`, the INIT transition) as one
      fallible block; on any failure it best-effort `rdma_reject`s the peer (so the
      client fails fast instead of timing out, no half-open id lingers) and returns the
      error tagged `hord_core::ConnectionSetupFailed` (recognised by
      `is_connection_setup_failure`, re-exported through `hord-stream`). The
      `HordListener` acceptor gained a match arm for it: log + skip that one peer and
      keep draining the wakeup's queue **without** touching `consecutive_errors` or the
      back-off, so a single bad peer can't climb toward `MAX_CONSECUTIVE_ACCEPT_ERRORS`;
      the skip still counts toward the per-wakeup drain cap so a flood can't starve the
      biased shutdown branch. `sideway` 0.4.3 exposes no reject, so the vendored copy
      gained `Identifier::reject` (`rdma_reject`, no private data) alongside
      `migrate`/`peer_addr` (HORD-PATCH.md updated). Regressed by device-free marker
      unit tests (the two listener markers must not cross-trigger) and the `#[ignore]`d
      `hord-core/tests/reject_on_setup_failure.rs` (induce the failure with an
      impossible QP sizing → assert the peer's `connect_finish` fails fast, not a
      watchdog timeout, and the server's error is `is_connection_setup_failure`).

- [ ] **`expect_event_timed` is a server-only twin of `expect_event` with a 1 ms poll
      and a divergent channel-mode side effect.** Three issues, all low-severity but
      worth unifying: (1) it busy-polls with a fixed `std::thread::sleep(1ms)` (the old
      `expect_event` blocked on the fd and woke immediately), so each synchronous
      handshake on a worker now adds up to ~1 ms latency and that sleep blocks the
      worker's whole current-thread runtime; (2) it leaves the *server* CM channel
      non-blocking on every return path, while the client's `connect_finish` still uses
      the unbounded blocking `expect_event` and leaves it blocking — so the "no peer
      pins a thread forever" guarantee holds only server-side and the two ends diverge
      in channel mode; (3) it duplicates the ack/match/error-format body of
      `expect_event`. **Fix:** one timeout-bearing, fd-driven (not sleep-polled) CM
      wait shared by both ends. Pairs with the handshake-stage item above.

- [ ] **`hord-zerocopy` gates the `rdma` layer with 17 scattered
      `#[cfg(feature = "rdma")]` attributes.** The orchestration items form one
      contiguous region, so a single `#[cfg(feature = "rdma")] mod rdma { … }`
      (re-exported) would collapse them to one and make the device-free boundary
      structurally unleakable rather than a matter of per-item discipline (forgetting
      one either breaks the default build or pulls a `hord-stream` type into the codec
      layer — the very regression the `codec` CI job guards). Mechanical; deferred to
      avoid a large move in the same change as the correctness fixes.

- [ ] **`hord-async` carries server-only deps for embedders that want only the
      adapter.** `HordListener` pulled `log` + tokio `sync`/`macros` into what was a
      stream-adapter crate; a host that brings its own accept loop and wants only the
      `AsyncRead`/`AsyncWrite` adapter still compiles the listener and its deps. **Fix:**
      put the listener behind a `listener = ["dep:log", "tokio/sync", "tokio/macros"]`
      feature so the adapter dependency surface stays minimal. (Low priority — `log` is
      near-zero-cost; Carapace gates the whole `hord-async` dep behind its own feature.)

- [ ] **Test-support duplication and a weak backpressure assertion.**
      `tests/listener.rs` adds a 6th copy of the `pattern_byte`/`pattern_vec` helpers
      (the `pattern()` LCG dup already tracked below) and of `current_thread_rt`
      (identical in 5 other `hord-async` test modules), and the
      acceptor/worker/demo each re-build a current-thread runtime with the same
      boilerplate — all candidates for a shared `tests/common` module + a
      `build_current_thread_rt()` helper. Separately,
      `poll_write_backpressures_slow_reader` proves backpressure via
      `blocked_observed = !write_done` after a fixed 500 ms sleep, which cannot
      distinguish "`poll_write` returned `Pending`" from "the write hadn't started
      yet" — a regression that merely delays the write start would pass. **Fix:** make
      the assertion observe the write genuinely stalling mid-flight (e.g. bytes
      received so far `<` payload while `write_done` is false) rather than a timer.

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

- [x] **`poll_write` must exert real backpressure.** Carapace throttles the MSE4 read
      loop with a 4-frame mpsc channel (`CHUNK_CHANNEL_DEPTH` in `body.rs`); that only
      works if a slow consumer makes `poll_write` return `Poll::Pending` when send
      credits are exhausted, rather than buffering unbounded and returning `Ready`.
      Confirm and document that credit exhaustion surfaces as `Pending` — otherwise a
      slow RDMA reader lets Carapace pull an arbitrarily large object fully into RAM.

      **Done.** `AsyncHordStream::poll_write` parks on the completion fd and returns
      `Poll::Pending` when the credit window is exhausted (it never buffers unbounded
      to return `Ready`) — documented on the impl and verified by
      `poll_write_backpressures_slow_reader` in `hord-async/tests/listener.rs`: a
      server writing 16 MiB to a non-reading client stays blocked mid-write (proving
      `Pending`), and the payload still arrives intact once the client drains.

- [x] **Feature-isolated build, device-free CI.** Carapace will gate HORD behind a
      cargo feature so the default build and CI need no NIC. HORD already isolates the
      RDMA libs below `hord-async`/`hord-stream`/`hord-core`; keep it that way and
      guarantee the crates Carapace links pull `sideway`/`librdmacm`/`libibverbs`
      *only* when the feature is on. The pure codec types (`hord-zerocopy`'s
      `RdmaWriteReq`/`RdmaWriteStatus`/`RdmaWriteAction`) must stay linkable without
      any RDMA library so Carapace can unit-test header handling on a laptop.

      **Done.** `hord-zerocopy` is now two layers behind one switch. The **default**
      build is the pure header codec (`RdmaWriteReq`/`RdmaWriteStatus`/`RdmaWriteAction`)
      with **zero dependencies**: `hord-stream` is an `optional` dep that only the new
      `rdma` feature enables (`rdma = ["dep:hord-stream"]`), so the default build pulls
      in *none* of `hord-stream`/`hord-core`/`sideway`/`librdmacm`/`libibverbs`. The
      write orchestration (`ZeroCopyRequest`, `serve_rdma_write{,_pooled}`, `SourcePool`,
      `SplitReceiver`/`SplitCompletion`) and the three device loopback tests are gated
      behind `rdma` (each item `#[cfg(feature = "rdma")]`; the test crates
      `#![cfg(feature = "rdma")]`). `hord-demo` enables the feature, so the full
      workspace build/suite is unchanged, while `cargo test -p hord-zerocopy` builds and
      runs the 14 codec unit tests with no NIC and no rdma-core. A new device-free CI
      job (`codec`) runs that on a stock runner (no Soft-RoCE) and asserts via
      `cargo tree` that the default build pulls no RDMA crate — so the isolation can't
      silently regress. `hord-core`/`hord-stream`/`hord-async` stay RDMA-only by
      construction (Carapace gates the whole `hord-async` dependency behind its own
      feature; there is no device-free subset of those to expose). **Remaining (Carapace
      side):** add the cargo feature that turns on `hord-async` + `hord-zerocopy/rdma`,
      and keep the default Carapace build/CI codec-only.

### Milestone 2 — one-sided zero-copy into client buffers (`X-HORD-RDMA-Write`)

The out-of-band path: the body is delivered by a one-sided `RDMA_WRITE` into the
client's registered (GPU) buffer, *not* through hyper's body stream. The server
still does **one copy** (MSE4 page → registered source buffer → DMA) — Milestone 3
removes that. This is where the workload that justifies HORD (GPU consumers pulling
segments) actually pays off.

- [ ] **Make `SharedAsyncStream` ergonomic from inside a hyper service handler.**
      Today the one-sided write is driven via `SharedAsyncStream::rdma_write`
      (`hord-async/src/lib.rs:49-57`), reachable from the handler because it shares the
      one CQ the driving task drains. Carapace's handler (`handle` in `handler.rs`)
      needs that handle threaded into request context so a route can parse
      `X-HORD-RDMA-Write`, call `RdmaWriteAction::decide(...)`, and either respond or
      write. Provide a documented, borrow-sound pattern for "reach the connection from
      the handler" that does not depend on the embedder understanding the
      `Rc<RefCell<…>>` aliasing rules — a misuse here is a soundness bug, not a perf
      bug.

- [ ] **Surface the DMA'd byte count for logging.** When the body goes out via
      `RDMA_WRITE`, it bypasses hyper entirely — Carapace's `FinalizingBody`
      (`carapace-direct/src/vsl.rs:281`) counts hyper *frame* bytes and would record
      ~0 body bytes for a zero-copy delivery, corrupting VSL `ReqAcct`. HORD must
      report, per transfer, the number of bytes actually written to the peer (and the
      `RdmaWriteStatus` outcome) so Carapace can finalize accurate transaction logs.

### Milestone 3 — true zero-copy from MSE4 pages (the actual prize)

This is the reason the seam exists (CLAUDE.md: "the response body can become an
RDMA scatter-gather payload referencing MSE4-resident pages"). It needs work on
*both* sides: MSE4 must register its resident pages as MRs and expose a zero-copy
read API; HORD must accept caller-owned MRs and gather from them. Carapace's
"response bodies are immutable end-to-end" principle is exactly what makes stable
page references safe to hand to the NIC — but the plumbing is absent today.

- [ ] **Register caller-provided memory as an MR (`register_external`).** Today
      `register_source(len)` hands back a *HORD-owned* `RegisteredBuffer`, forcing
      Carapace to copy each MSE4 page into it — that is not zero-copy. HORD must expose
      `register_external(ptr, len) -> Mr` (returns an `lkey`) so Carapace/MSE4 can
      register the mmap'd store (or the AIO buffers) **once** and DMA straight out of
      resident pages.

- [ ] **Scatter-gather source for a single logical transfer.** An MSE4 object is
      stored across multiple non-contiguous allocations (the `Mse4HitHandler.allocs:
      Vec<(disk_off, alloc_size, buf_off)>` list in `storage.rs`). The client's
      destination buffer is contiguous; the source is fragmented. `rdma_write_all`
      currently takes a single `(src, src_off, len)`. HORD must accept a **gather
      list** of `(local_addr, lkey, len)` segments (all within registered MRs) and
      lay them down contiguously at the remote offset — either as one SGE-list WR or a
      chained sequence at increasing remote offsets — so a fragmented cached object
      becomes one logical zero-copy write.

- [ ] **Pin/lifetime contract for registered pages.** A page that is RDMA-registered
      and in flight must not be evicted or rewritten underneath the NIC — that is a
      DMA-into-freed-memory hazard. This collides directly with MSE4 eviction
      (TinyUFO is shared and eviction is the documented multi-tenant failure mode) and
      is *guarded but not guaranteed* by the immutability principle. HORD must
      document the registration lifetime contract explicitly (how long an `Mr` and an
      in-flight transfer require the backing pages to stay resident and unmodified),
      support `deregister`, and ideally cache MRs so Carapace isn't registering on the
      hot path. Carapace owns the "keep the page resident until the transfer
      completes" half; HORD owns stating the contract and exposing completion so
      Carapace knows when it is safe to release.

### Cross-cutting

- [ ] **Peer identity + trust model for multi-tenancy.** Carapace is multi-tenant by
      design; RDMA QPs carry no TLS and HORD currently has no authentication. HORD must
      expose the peer's GID / connection identity per connection so Carapace can attach
      a `tenant` dimension (cache key namespace, VSL/Prometheus `tenant` label, PURGE
      authority) to HORD-served traffic, and must document the trust model plainly
      (last-hop trusted-fabric assumption) so the multi-tenant security posture is a
      stated decision rather than a silent gap.

- [ ] **Connection metadata for VSL parity.** The direct path already logs two
      documented divergences from the proxy path (no `Connected` timestamp;
      `ttl/grace/age` logged as zero). HORD should expose handshake-completion timing
      and negotiated capabilities (`zero_copy_negotiated` / `split_mode_negotiated` are
      already there) so the HORD listener can record a transport-accurate transaction
      tree rather than inheriting the TCP path's gaps.
