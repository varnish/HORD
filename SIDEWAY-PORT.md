# sideway port (spike)

This branch (`sideway-port`) replaces `hord-core`'s hand-written C shim over
`librdmacm` + `libibverbs` with the [`sideway`](https://crates.io/crates/sideway)
crate (a safe rdma-core wrapper), plus `rdma-mummy-sys` for the few calls sideway
doesn't expose. It also moves the HORD handshake out of RDMA-CM private data into
a first message over the QP, and carries one small local sideway patch
(`Identifier::migrate`, under `vendor/sideway/`) while that lands upstream.
`main` keeps the C shim; switch between the two branches to compare.

## The trade

| | `main` (C shim) | `sideway-port` |
| --- | --- | --- |
| C code | 895 lines (`shim.c` 642 + `shim.h` 199 + `build.rs` 54) | **0** |
| `unsafe` in `hord-core/src/lib.rs` | 27 | **13** (−52%) |
| `unsafe` in `hord-stream/src/stream.rs` | 8 | 11 (+3: the first-message handshake) |
| `unsafe`, core + stream | 35 | **24** (−31%) |
| `hord-core` third-party deps | **0** | 2 direct (`sideway`, `rdma-mummy-sys`), ~46 in tree |
| First build | C compiler + system rdma-core-dev | also cmake + libclang; compiles a vendored rdma-core (~once) |

Net: **no C, ~⅓ less `unsafe`, an ergonomic + maintained API** — at the cost of
the previous dep-free / air-gapped-buildable property and a heavier first build.
The hot-path `unsafe` (`post_*`, `reg_mr`) stays `unsafe` by design on both sides
— that's inherent to handing the NIC a buffer that aliases live memory. What went
away is the connection-setup / MR / teardown / error-handling `unsafe` and all of
the C.

## What changed

1. **`hord-core` → sideway.** RC QP lifecycle, MR registration, post send/recv/
   write/write-with-imm, CQ polling, and the RDMA-CM dance (resolve/listen/connect/
   accept/establish/disconnect, INIT→RTR→RTS via the CM-computed attributes) are
   now safe sideway calls. The QP lives behind a `RefCell<Option<_>>` because
   sideway's post/modify take `&mut` while HORD drives a connection through `&self`.
2. **Handshake → first message.** `hord-core` no longer knows about the handshake
   at all; it's a pure transport. `hord-stream` exchanges the 16-byte handshake as
   the first send/recv over the established QP (recv pre-posted before RTS, so it's
   RNR-safe). This was forced by a sideway gap (below) but is independently better:
   the spec's 60-byte handshake never fit RDMA-CM's ~56-byte private-data area
   (the prototype had already truncated it), and a first message lifts that ceiling.
3. **C shim deleted**; `build.rs` gone.

## sideway gaps found (all upstream-able)

1. **CM private data** — `ConnectionParameter` exposes only `qp_number`; `Event`
   exposes no private-data accessor; the raw `cm_id`/`conn_param` are private.
   So sideway's safe `rdmacm` cannot carry the HORD handshake. **Avoided** by
   moving the handshake to a first message (which is a net win anyway).
2. **`rdma_migrate_id`** — not exposed (and the raw `cm_id` is private, so it can't
   be bridged externally). The C shim gave every accepted connection its own CM
   event channel by migrating its `cm_id`; without it, accepted connections share
   the listener's channel, so a *looping* acceptor that hands connections to other
   threads has a worker's `accept_finish` race the next `accept_begin`.
   **Resolved on this branch** with a small carried patch: `vendor/sideway/` adds
   `Identifier::migrate` (~30 lines wrapping `rdma_migrate_id`), wired via
   `[patch.crates-io]`, and `Listener::accept` migrates each accepted connection to
   its own channel. Exercised by `hord-core`'s `concurrent_accept` test (a looping
   acceptor + concurrent workers — deadlocks on the shared-channel design, passes
   here). Filed upstream; drop the patch when it ships (see
   `vendor/sideway/HORD-PATCH.md`).
3. **`ConnectionParameter` retry/rnr setters** — only `qp_number` is settable, so
   `CmParams.retry_count`/`rnr_retry_count` aren't plumbed through; sideway's
   defaults (7/7, same as the old shim) are used. Minor; loopback is reliable.
4. **CQ arm / comp-channel consume** — sideway wraps neither `ibv_req_notify_cq`
   nor `ibv_get_cq_event`/`ibv_ack_cq_events`. **Bridged** via `rdma-mummy-sys`
   using sideway's documented raw-handle escape hatches (`CompletionQueue::cq()`,
   `CompletionChannel::comp_channel()`) — no fork needed.

Gap 2 is carried as a local patch here (`vendor/sideway/`); closing gaps 1 & 3
upstream too would let this run on 100% stock sideway with the handshake either
way.

## Test status

`cargo test --workspace -- --include-ignored --test-threads=1` is **fully green
on the host's Soft-RoCE `rxe0`** — including the ignored RDMA tests: handshake +
CM setup, send/recv/write/write-with-imm, the async reactor (CQ-fd parking +
peer-disconnect detection), zero-copy, range requests, protocol splitting, and
`concurrent_accept` (the looping-acceptor/per-connection-channel test for the
migrate patch).
