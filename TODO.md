# HORD prototype — TODO

Work is tackled as focused passes (different kinds of work, different risk),
each verified on the host's Soft-RoCE device (`rxe0`, see CLAUDE.md) and
recorded in PROTOTYPE.md. Earlier passes are summarised here; the current pass
is detailed.

## Earlier passes (DONE — see PROTOTYPE.md for details)

- **Code review pass** — fixed clear-cut correctness/soundness bugs (#1 #2 #4 #5
  #7 #10 #12 #13).
- **Pass 1 — Flow-control correctness** — full-duplex credit deadlock via a
  self-clocked control lane (#3); receipt → consumption credit return, bounding
  reassembly (#8).
- **Pass 2 — Soundness & ownership** — aliasing UB closed with
  `Box<[UnsafeCell<u8>]>` reached only by raw pointer (#9); MR↔PD lifetime made a
  type guarantee via `RegisteredBuffer` (#6).
- **Pass 3 — Async + hyper** — tokio `AsyncRead`/`AsyncWrite` parking on the CQ
  completion fd (#15), `hyper` over the async stream, multi-connection server,
  tunable CM params (#11), streamed bodies (#14), async half-close.

## Pass 4 — Zero-copy RDMA-write (spec §7.1–7.4)  (DONE · branch `pass-4-zero-copy-rdma-write`)

The first one-sided RDMA in the codebase: the server places a response body
directly into the client's registered buffer via `IBV_WR_RDMA_WRITE`; the HTTP
response carries `Content-Length: 0` + `X-HORD-RDMA-Write:
status=complete;bytes_written=<n>`. No staging/reassembly copy on the data path.
A new **`hord-zerocopy`** crate holds the HTTP semantics; the transport half
lives in `hord-stream`/`hord-core`. Both demo paths (sync + async/hyper) gained
zero-copy.

### 4a — Transport verb (`hord-core` + C shim)  (DONE)
- [x] `hord_post_write` (`IBV_WR_RDMA_WRITE`, `wr.rdma.remote_addr`/`rkey`) +
      `HORD_WC_OPCODE_RDMA_WRITE`; `ACCESS_REMOTE_WRITE`; `Opcode::RdmaWrite`;
      `Connection::post_write` (`unsafe`).
- [x] Smoke test (`hord-core/tests/rdma_write_smoke.rs`, `#[ignore]`d): a 16 MiB
      single-WR write over `rxe0` lands intact — de-risks the "one large WR,
      NIC-segmented" assumption before the driver is built.

### 4b — Negotiation + write driver (`hord-stream`)  (DONE)
- [x] Handshake `ZERO_COPY_CAPABLE` (set via `Handshake::with_zero_copy`),
      `HordConfig::zero_copy` (default true), `HordStream::zero_copy_negotiated`.
      (Also fixed the flag layout to match spec §5.3: dropped the non-spec
      `HTTP2_CAPABLE`, `SPLIT_MODE_CAPABLE` is now bit 1.)
- [x] `register_remote_writable` (client dest, `LOCAL_WRITE|REMOTE_WRITE`) /
      `register_source` (server src, `LOCAL_WRITE`) pass-throughs.
- [x] Non-blocking write driver: a `WRITE_FLAG` `wr_id` routed **before** the
      `is_send` branch in `handle_completion` (writes belong to neither pool — a
      `writes_outstanding` counter), `begin_rdma_write` (chunks ≤ `WRITE_WR_MAX`
      = 1 GiB, bounded by the send queue), `writes_pending`, and the blocking
      facade `rdma_write_all`. Writes cost no credit and no send-pool slot.

### 4c — `hord-zerocopy` crate  (DONE)
- [x] Pure `X-HORD-RDMA-Write` codec: `RdmaWriteReq` (§12.3) and
      `RdmaWriteStatus::{Complete,TooLarge,Declined}` (§12.4), parse + format,
      7 unit tests (round-trips, the spec examples, malformed input).
- [x] Sync orchestration over `&mut HordStream`: `ZeroCopyRequest` (client) and
      `serve_rdma_write` (server: register source → fill → write → status).

### 4d — Sync demo  (DONE — working milestone)
- [x] `hord-client --zero-copy [--zc-buf <n>]`: advertise a buffer, read the body
      from it on `complete`, fall back to the stream on `declined`/`too_large`.
- [x] `hord-server`: RDMA-write `/size/<n>` into the client's buffer; §7.4
      `status=declined` echoed on any body response to a zero-copy request.
- [x] Verified over `rxe0`: 64 MiB zero-copy (integrity OK), too_large→413,
      declined fallback on `GET /`, stream path regression-free.

### 4e — Async write capability (`hord-async`)  (DONE)
- [x] `SharedAsyncStream(Rc<RefCell<AsyncHordStream>>)`: `AsyncRead`/`AsyncWrite`
      delegated by `borrow_mut` **per poll, never across an await**, so a `hyper`
      server can drive an RDMA write from its handler while hyper owns the stream
      (same CQ, same task). `register_remote_writable` / `register_source` /
      `zero_copy_negotiated` pass-throughs; async `rdma_write` over
      `poll_rdma_write` (reuses `poll_events`, no flow-control logic duplicated).

### 4f — Async/hyper demo  (DONE)
- [x] `hord-client-async --zero-copy`: register dest, add the header, read the
      payload from the buffer after the (empty-body) response.
- [x] `hord-server-async`: clone the shared handle into the `service_fn` closure,
      `await` the RDMA write, respond `Content-Length: 0`. Verified over `rxe0`
      (64 MiB zero-copy ~700 MiB/s, too_large, declined; no double-borrow).

### 4g — Tests & docs  (DONE)
- [x] `#[ignore]`d `rxe0` integration tests: sync `zerocopy_loopback` (complete +
      too_large + integrity) and async `zerocopy` (`SharedAsyncStream` write).
- [x] Codec unit tests; handshake flag round-trip. No regression in
      `full_duplex_bulk` / async `loopback`.
- [x] PROTOTYPE.md + this file + README spec findings updated.

---
Fixed in the review pass: #1 #2 #4 #5 #7 #10 #12 #13.
Pass 1: #3 #8. Pass 2: #6 #9. Pass 3: #15 #11 #14 + async half-close.

## Remaining / future
- [ ] **§7.7 protocol splitting** — `IBV_WR_RDMA_WRITE_WITH_IMM`, control/data
      plane split, transfer credits. The natural next pass (the `id=` request
      param is already parsed and currently ignored).
- [ ] **§7.5 GPUDirect** — untestable on this host (no GPU / real NIC); the
      addr/rkey path is opaque, so it should work unchanged on capable hardware.
- [ ] **§7.6 range requests** — small add-on on top of the zero-copy path.
- [ ] A zero-copy *source* buffer pool on the server (amortize registration —
      §8.3) instead of registering per response.
- [ ] Concurrent independent read+write on one async stream (two tasks over
      `tokio::io::split`) needs a multi-waiter scheme on the completion fd.
- [ ] Half-close detection on the *synchronous* stream (the async path has it).
- [ ] True thread-per-core server (worker pool + `spawn_local`) instead of one OS
      thread per connection.
