# HORD prototype — TODO

Follow-ups from the code review (details in PROTOTYPE.md → "Open issues from
code review"). Tackled as focused passes, not one big change: the items are
different kinds of work with different risk, and two of them belong with the
async milestone rather than now.

## Pass 1 — Flow-control correctness  (DONE)
- [x] **#3 Full-duplex credit deadlock** — credit-returns now travel a separate,
      self-clocked control lane: `CTRL_RECV_SLACK` extra receive buffers kept
      permanently posted, and a reserved control send slot bounded by one
      in-flight message (`ctrl_send_busy`) rather than by a data credit. No
      wire-format change.
- [x] **#8 Unbounded reassembly** — a received data buffer is held in place
      (`ReadyMsg`) and only re-posted / credited on application *consumption* in
      `read()`, so backpressure reaches the sender and the reassembly footprint
      is bounded to the receive pool.
  - [x] Full-duplex bulk test (`fullduplex_tests::full_duplex_bulk`, `#[ignore]`d):
        forces the simultaneous-zero-credit standoff via a barrier and verifies
        16 MiB each way; confirmed to deadlock if the control lane is removed.

## Pass 2 — Soundness & ownership  (hord-core refactor · independent of async)  (DONE)
- [x] **#9 Aliasing UB** — registered storage is now `Box<[UnsafeCell<u8>]>`
      reached only through raw pointers (`UnsafeCell::raw_get`); no `&`/`&mut [u8]`
      is ever formed over an allocation the NIC may be DMA-ing into. Envelope
      encode/decode and payload copies go via stack buffers and
      `RegisteredBuffer::copy_in`/`copy_out` (raw `copy_nonoverlapping`).
- [x] **#6 MR↔PD lifetime** — `Connection::register_buffer` now returns a
      `RegisteredBuffer` that owns its storage and holds an `Arc<Connection>`, so
      the PD provably outlives every MR regardless of drop order. `HordStream`'s
      `Drop` is down to a single `shutdown()` (the one inherent runtime step:
      quiesce DMA before the MRs deregister); the `Option`/field-order dance is
      gone.
  - Done together (same buffer/MR ownership model). Verified: workspace builds
    clean, clippy clean, demo integrity OK over `rxe0`, and the `#[ignore]`d
    `full_duplex_bulk` test passes.

## Pass 3 — Async + hyper milestone  (the big one · absorbed #15, #11, #14)  (DONE)

`tokio::AsyncRead`/`AsyncWrite` feeding `hyper`, with the server handling many
connections at once. `tokio` + `hyper` are the first third-party crates in the
workspace; they are confined to a **new `hord-async` crate** (and the demo's
hyper bins), so `hord-core`/`hord-stream` stay dep-free and air-gapped-buildable.
Verified over `rxe0` at every step; the sync path is unchanged throughout.

### 3a — Shim: pollable fds + tunable CM params  (DONE)
- [x] **CQ completion channel.** CQ now built against an `ibv_comp_channel`
      (`hord_conn_cq_fd`), with `hord_cq_arm` (`ibv_req_notify_cq`) and
      `hord_cq_consume` (`ibv_get_cq_event` + batched `ibv_ack_cq_events`); fd set
      `O_NONBLOCK`. Transparent to the sync path — it never arms, so an un-armed
      channel delivers no notifications and `ibv_poll_cq` works as before.
- [x] **CM fd + half-close hooks.** `hord_conn_cm_fd` / `hord_conn_cm_set_nonblock`
      / `hord_conn_check_disconnect` (the CM channel stays blocking for the
      handshake, flipped non-blocking only after). The *listener* fd was **not**
      needed: the acceptor stays a blocking loop (see 3d), so no `expect_event`
      split was required.
- [x] **#11 (params).** `rnr_retry_count` / `retry_count` / `resolve_timeout_ms`
      threaded out as `hord_core::CmParams` (Default = the old `7`/`7`/`2000`),
      wired through `HordConfig::cm`.

### 3b — Non-blocking stream core  (DONE, no behavior change)
- [x] `send_message` split into `can_send_data()` + the non-blocking
      `post_data_message()`; new public `try_write` / `try_flush_stage` /
      `try_read` (`None` = would-block, `Some(0)` = EOF) / `drain_completions` /
      `sends_outstanding` / `return_owed_credits` / `is_closed` / `mark_closed`.
      `Read`/`Write`/`flush` are now thin busy-poll facades over them — sync demo
      still ~698 MiB/s integrity-OK, `full_duplex_bulk` still passes. `accept`
      split into `accept_begin` (returns the `Send` `Connection`) + `from_accepted`.

### 3c — Async stream wrapper  (`hord-async` · DONE — closes #15)
- [x] **`AsyncFd` over the CQ fd**, with the arm→drain-again race guard.
      `AsyncRead`/`AsyncWrite`/`poll_flush`/`poll_shutdown` over the 3b core; no
      flow-control logic duplicated. **#15 closed.**
- [x] **#11 (deadlines)** via caller-side `tokio::time::timeout` (a cancelled
      read/write future drops cleanly); CM params from 3a are the transport half.
- [x] **Half-close.** CM fd registered; a peer `DISCONNECTED` marks the stream
      closed → clean EOF. Retires the "no graceful half-close" limitation **on
      the async path** (the sync path still has none — by design).
- *Deviation from plan:* the handshake runs **synchronously on the connection's
  own thread** (each connection gets a thread; see 3d), so `spawn_blocking` was
  unnecessary. **Driver model:** the stream is driven by one task (as hyper
  does); two tasks over `tokio::io::split` are *not* supported (both would wait
  on the single completion fd — documented in the crate).

### 3d — Multi-connection server + hyper  (DONE — includes #14)
- [x] **Thread-per-connection** (not the planned `spawn_local` fan-out): a
      blocking accept loop hands each `Connection` to a fresh OS thread running a
      current-thread runtime + `hyper` `serve_connection`. Many connections at
      once, the `!Send` stream stays on its thread, no unsound `Send`. The
      *client* uses `spawn_local` under a `LocalSet` to drive the hyper connection
      task. Verified: 6 concurrent clients × 32 MiB, all integrity-OK.
- [x] hyper over the async stream (`hord-server-async` / `hord-client-async`);
      cross-compatible with the sync bins (identical wire protocol, both ways).
      Sync bins kept as the busy-polled reference.
- [x] **#14.** `/size/<n>` is a custom `http_body::Body` streaming 256 KiB frames
      with an exact size_hint (Content-Length, no up-front allocation).

### 3e — Tests & verification  (DONE)
- [x] `async_request_response` (4 MiB, integrity + half-close EOF + timeout).
- [x] **#15 evidence** (`idle_cpu`): async blocked read = **~90 µs CPU over 1 s**
      idle vs sync busy-poll = **~800 ms CPU over 800 ms** (~9000×).
- [x] Throughput parity: hyper bodies ~650–675 MiB/s vs sync ~700 MiB/s.
- [x] Timeout: an idle-peer read deadlines out instead of hanging.
- *Not done:* an async `full_duplex_bulk` analogue — it needs two tasks over
  `split`, the unsupported model above; the sync `full_duplex_bulk` already
  covers the control-lane / #3 standoff, and the shared 3b state machine means
  the async path exercises the same logic.

---
Fixed in the review pass (reference): #1 #2 #4 #5 #7 #10 #12 #13.
Fixed in Pass 1 (flow-control credit redesign): #3 #8.
Fixed in Pass 2 (soundness & ownership): #6 #9.
Fixed in Pass 3 (async + hyper): #15 #11 #14 + async half-close.

## Remaining / future
- [ ] Concurrent independent read+write on one async stream (two tasks over
      `tokio::io::split`) needs a multi-waiter scheme on the completion fd.
- [ ] Half-close detection on the *synchronous* stream (the async path has it).
- [ ] True thread-per-core server (worker pool + `spawn_local`) instead of one
      OS thread per connection.
