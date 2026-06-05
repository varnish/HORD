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

## Pass 3 — Async + hyper milestone  (the big one · absorbs two review items)
- [ ] Async `HordStream`: drive the CQ + CM event-channel fds via
      `tokio::io::unix::AsyncFd`; implement `AsyncRead`/`AsyncWrite`.
      → subsumes **#15** (busy-poll burns a core).
- [ ] Deadlines on the wait loops + tunable CM retry params. → **#11**.
- [ ] Multi-connection server (per-connection task), then `hyper` over the
      async stream.

## Minor / anytime
- [ ] **#14** Demo server: stream `/size/<n>` in fixed-size chunks instead of one
      up-front allocation.

---
Fixed in the review pass (reference): #1 #2 #4 #5 #7 #10 #12 #13.
Fixed in Pass 1 (flow-control credit redesign): #3 #8.
Fixed in Pass 2 (soundness & ownership): #6 #9.
