# HORD prototype — TODO

Follow-ups from the code review (details in PROTOTYPE.md → "Open issues from
code review"). Tackled as focused passes, not one big change: the items are
different kinds of work with different risk, and two of them belong with the
async milestone rather than now.

## Pass 1 — Flow-control correctness  (independent · highest stakes · testable now)
- [ ] **#3 Full-duplex credit deadlock** — add a credit-return path that does not
      consume a data credit (a small reserved pool of receive buffers for control
      messages, accounted separately from data credits).
- [ ] **#8 Unbounded reassembly** — return credits / re-post receive buffers on
      application *consumption* (once `read()` drains the bytes), not on receipt,
      so backpressure actually reaches the sender.
  - Design these two together — both change *when/how* credits are returned.
  - Add a full-duplex bulk test (both ends write large bodies at once) to
    exercise the deadlock path the half-duplex HTTP demo can't reach.

## Pass 2 — Soundness & ownership  (hord-core refactor · independent of async)
- [ ] **#9 Aliasing UB** — keep registered buffers behind `UnsafeCell` /
      raw-pointer access; never form `&`/`&mut` over an allocation the NIC is
      concurrently DMA-ing into.
- [ ] **#6 MR↔PD lifetime** — tie `MemoryRegion` lifetime to the `Connection`/PD
      (e.g. `Arc<Connection>`) so correct teardown isn't only enforced by
      `HordStream`'s hand-rolled `Drop`.
  - These touch the same buffer/MR ownership model; do them together.

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
