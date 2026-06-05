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

## Pass 3 — Async + hyper milestone  (the big one · absorbs #15, #11, #14)

The end state from the spec/README: `tokio::AsyncRead`/`AsyncWrite` feeding
`hyper`, with the server multiplexing many connections instead of accepting one
at a time. Today the only things standing in the way are (a) `pump(true)`'s
`spin_loop` busy-wait and (b) the shim's *synchronous* CM-event waits
(`expect_event`); the CQ is created with no completion channel
(`ibv_create_cq(..., NULL, NULL, 0)`), so there is nothing to poll on yet.

Two cross-cutting decisions that shape the whole pass (flagged in **Decisions**
below — worth settling before 3c):

- **Dependency inflection.** `tokio` + `hyper` are the first third-party crates
  in the workspace; that breaks the deliberate zero-dep property `hord-core` and
  `hord-stream` advertise. Plan keeps both of those crates dep-free and confines
  the runtime to a **new `hord-async` crate**, so the synchronous stream, the
  demo-less core, and the air-gapped build story all survive intact.
- **Thread affinity.** A connection's verbs/CM objects are not safe to drive
  concurrently, and `HordStream` is `!Send` (the buffers hold raw pointers; it
  holds `Arc<Connection>` where `Connection: Send + !Sync`). RDMA connections are
  naturally thread-pinned, so the server model is **thread-per-core + a
  current-thread runtime + `spawn_local`** rather than `tokio::spawn` over the
  multi-thread runtime. This avoids inventing an unsound `Send`/`Sync` claim.

Sequenced as five sub-passes, smallest blast radius first; each lands building +
clippy-clean with the sync demo still passing over `rxe0`.

### 3a — Shim: pollable fds + tunable CM params  (C, foundational)
- [ ] **CQ completion channel.** Create an `ibv_comp_channel`, build the CQ
      against it (`ibv_create_cq(ctx, cqe, NULL, channel, 0)`), and expose
      `hord_conn_cq_fd()`. Add `hord_cq_arm()` (`ibv_req_notify_cq`) and
      `hord_cq_ack_events()` (`ibv_get_cq_event` + batched `ibv_ack_cq_events`).
      Set the channel fd `O_NONBLOCK` so a spurious reactor wakeup can't block.
- [ ] **CM event-channel fds.** Expose `hord_listener_cm_fd()` /
      `hord_conn_cm_fd()` and set both event channels `O_NONBLOCK`. Split the
      blocking `expect_event` waits so the Rust side can drive CM transitions on
      fd-readiness (or keep them blocking behind `spawn_blocking` — see
      Decisions).
- [ ] **#11 (params).** Thread `rnr_retry_count` / `retry_count` and the
      `resolve_addr`/`resolve_route` timeouts (today hardcoded `7` / `2000`) out
      to the Rust API as a config struct, instead of baking in infinite RNR
      retry.

### 3b — Non-blocking stream core  (hord-stream refactor · no behavior change)
- [ ] Factor `HordStream`'s blocking loops out from its state machine. `pump`,
      `send_message`, `read`, `flush` today interleave the credit/slot logic with
      a `spin_loop`. Extract a core exposing non-blocking primitives
      (`drain_cq()`, `try_send_message() -> WouldBlock`, non-blocking read/flush
      progress) and re-express the existing `std::io::Read`/`Write`/`flush` as
      thin busy-poll facades over it. Net effect zero on the sync path — the
      `#[ignore]`d `full_duplex_bulk` test and the demo keep passing unchanged —
      but the async wrapper in 3c then shares the *same* state machine instead of
      duplicating the credit/control-lane logic.

### 3c — Async stream wrapper  (new `hord-async` crate · the core of #15)
- [ ] **`AsyncFd` over the CQ fd.** Arm → `await` readable → `ack` → re-arm →
      drain, with the standard re-arm/poll-again guard against the completion
      that lands between the last drain and the re-arm. Replaces the `spin_loop`;
      **closes #15** (a blocked connection now parks instead of pinning a core).
- [ ] **`AsyncRead` + `AsyncWrite`** over the 3b core: on `WouldBlock`, register
      CQ-fd readiness and yield; a send completion (frees a slot / carries
      credits) or a recv completion wakes it. `poll_flush` waits on outstanding
      send completions the same way.
- [ ] **Async connect/accept** driving the CM fd via `AsyncFd` (handshake leg may
      stay on `spawn_blocking` to start — see Decisions).
- [ ] **#11 (deadlines).** Wrap the wait points in `tokio::time::timeout` so a
      stalled-but-alive peer surfaces an error instead of hanging `read`/`flush`
      forever.
- [ ] **Half-close.** With the CM fd now pollable, process `DISCONNECTED` on the
      data path and map it to clean EOF — retires the "no graceful half-close
      detection" limitation rather than relying on `Content-Length` +
      flush-before-disconnect.

### 3d — Multi-connection server + hyper
- [ ] **Async accept loop, one task per connection.** Thread-per-core: each
      worker thread runs a current-thread runtime and `spawn_local`s a task per
      accepted `HordStream` (keeps each connection thread-affine; no `Send`
      requirement on the stream).
- [ ] **Validate async transport first** by porting the hand-rolled demo
      client/server to the async stream, *then* swap in `hyper` over it — hyper
      changes nothing below the socket, so proving the async byte stream in
      isolation de-risks the integration.
- [ ] **#14.** Once handlers are async, stream `/size/<n>` in fixed-size chunks
      instead of materialising up to 1 GiB up front (the natural shape for an
      `AsyncWrite` body). Folded here rather than left as a standalone chore.

### 3e — Tests & verification
- [ ] Async analogue of `full_duplex_bulk` (same standoff, driven by the
      reactor) + an end-to-end async HTTP round trip.
- [ ] **#15 evidence:** confirm ~0% CPU while a connection is blocked on I/O
      (contrast the current 100%-while-blocked busy-poll).
- [ ] Throughput parity with the sync prototype's ~0.7 GiB/s over `rxe0`.
- [ ] Timeout behaviour: a peer that stalls mid-transfer now errors on the
      deadline instead of hanging.

### Decisions to settle before 3c
- **Crate layout:** new `hord-async` crate (keeps `hord-core`/`hord-stream`
  dep-free) vs. an `async` feature on `hord-stream`. Plan assumes the former.
- **Server task model:** thread-per-core + `spawn_local` (no new `unsafe`) vs.
  an `unsafe impl Send` to allow `tokio::spawn` on the multi-thread runtime. Plan
  assumes the former.
- **CM handshake drive:** fully non-blocking via the CM fd vs. `spawn_blocking`
  for the one-time begin→finish handshake (the per-core busy-poll problem is the
  *data* path, so blocking the rare handshake is the low-risk fallback). Plan
  starts with `spawn_blocking` and can tighten later; the CM fd must still be
  pollable for the live-connection `DISCONNECTED` event regardless.

---
Fixed in the review pass (reference): #1 #2 #4 #5 #7 #10 #12 #13.
Fixed in Pass 1 (flow-control credit redesign): #3 #8.
Fixed in Pass 2 (soundness & ownership): #6 #9.
Pass 3 absorbs the remaining open issues: #15 #11 #14 + the half-close limitation.
