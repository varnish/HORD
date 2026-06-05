# HORD prototype

A first working implementation of the HORD stream path: **HTTP/1.1 over RDMA
RC**, demonstrated end-to-end over Soft-RoCE.

This implements the two-sided (send/recv) transport described in spec sections
4–6, 8 and 9. The **zero-copy / RDMA-write extension (spec section 7) is
deliberately out of scope** for this prototype — no `X-HORD-RDMA-Write`, no
one-sided writes, no GPUDirect, no protocol splitting. The point here was to get
real bytes of HTTP moving over real queue pairs.

## What works

- RC connection setup and teardown via the RDMA CM (`rdma_cm`), with the HORD
  handshake carried in the CM private-data field.
- The 8-byte message envelope (spec 12.2) framing every RDMA send.
- Credit-based flow control (spec 9): initial credits from the handshake,
  piggybacked grants on data messages, and `CREDIT_ONLY` top-ups for
  one-directional bulk transfers.
- A `std::io::Read + Write` byte stream (`HordStream`) that segments writes into
  messages and reassembles receives — the stream abstraction of spec 6.
- A minimal but real HTTP/1.1 client and server running unmodified over that
  stream.

Verified on this host's Soft-RoCE device (`rxe0`, loopback over `enp14s0`):
~0.7–0.75 GiB/s for bodies from 1 MiB to 1 GiB, flat with size, byte-pattern
integrity checked end to end.

```
=== GET /size/1048576     (   1 MiB) === 200 OK  703 MiB/s  integrity OK
=== GET /size/16777216    (  16 MiB) === 200 OK  719 MiB/s  integrity OK
=== GET /size/67108864    (  64 MiB) === 200 OK  714 MiB/s  integrity OK
=== GET /size/268435456   ( 256 MiB) === 200 OK  719 MiB/s  integrity OK
=== GET /size/1073741824  (1024 MiB) === 200 OK  753 MiB/s  integrity OK
```

(Soft-RoCE is CPU-bound software RDMA; this is single-threaded, busy-polled, and
copies through a staging buffer. There is plenty of headroom — see
[Limitations](#limitations).)

## Layout

```
hord-core/     RDMA transport. Safe Rust wrappers over a small C shim
               (csrc/shim.c) that drives librdmacm + libibverbs. The shim
               exists because the verbs data-path calls (ibv_post_send,
               ibv_poll_cq, ...) are `static inline` in the rdma-core headers
               and therefore not linkable symbols.
hord-stream/   HORD wire protocol: handshake, envelope, credit flow control,
               and the HordStream byte stream (Read + Write).
hord-demo/     hord-server and hord-client: a tiny HTTP/1.1 origin and client.
```

There are **no third-party crate dependencies** — only `std`, plus the system
`librdmacm`/`libibverbs` linked through the shim. `build.rs` invokes `cc`/`ar`
directly rather than pulling in the `cc` crate.

## Building

Needs a C compiler and the RDMA dev packages:

```sh
sudo apt-get install -y libibverbs-dev librdmacm-dev   # provides headers + .so symlinks
cargo build --release
```

## Running (Soft-RoCE loopback)

The defaults target this host's `rxe0` device (IP `77.40.251.67`, see
`CLAUDE.md`). `rdma_cm` resolves that IP to the RoCEv2 GID and the rxe transport
loops it back internally. (Note: `127.0.0.1` does **not** work — it routes via
`lo`, which has no RDMA device.)

```sh
# Terminal 1
./target/release/hord-server                 # listens on 77.40.251.67:4791

# Terminal 2
./target/release/hord-client --path /                       # small greeting
./target/release/hord-client --path /size/67108864          # 64 MiB, integrity-checked
```

Routes: `GET /` → greeting; `GET /size/<n>` → `<n>` bytes of a verifiable
pattern; anything else → 404.

## Design notes

**Connection setup is two-phase** (`*_begin` → caller posts receives →
`*_finish`). Receives must be pre-posted before the QP can carry traffic, so the
stream layer registers its receive pool and posts all receive WRs *between*
creating the QP and accepting/connecting. This avoids an initial
receiver-not-ready (RNR) storm.

**`flush()` waits for send completions.** For RC, a send completion means the
message has been placed in the peer's receive buffer and acknowledged. So once
`flush()` returns, the data is delivered and it is safe to disconnect — which is
exactly what the server relies on before dropping the connection.

**One CQ per connection**, shared by sends and receives; work requests are
tagged by `wr_id` (top bit = send vs. recv, low bits = buffer slot).

## Limitations (prototype scope)

These are deliberate cuts, not oversights — each is a known next step:

- **Synchronous + busy-polled.** The spec's target API is `tokio::AsyncRead`/
  `AsyncWrite` feeding `hyper`. The natural path is to drive the CQ and CM event
  channels (both have pollable fds) via `tokio::io::unix::AsyncFd`, then wrap
  `HordStream` as async. The current stream is `std::io` and busy-polls the CQ
  (100% CPU while blocked).
- **One connection at a time** on the server (sequential `accept`). Real use
  needs a per-connection task/thread.
- **No graceful half-close detection.** We rely on HTTP `Content-Length` and
  `flush()`-before-disconnect rather than processing CM `DISCONNECTED` events on
  the data path.
- **Two copies on the receive path** (NIC buffer → reassembly `VecDeque` →
  caller). The send path is down to one copy (into the registered staging
  buffer). The reassembly buffer could be bypassed when a read spans a whole
  message.
- **HTTP is hand-rolled** (just enough to prove the transport). Swapping in
  `hyper` over an async `HordStream` is the intended end state and changes
  nothing below the socket.

## Open issues from code review (deferred, by design)

A max-effort review surfaced these. The clear-cut correctness/soundness bugs
were fixed (send/recv error paths now mark the connection closed instead of
leaking a slot; `flush()` returns an error instead of silently truncating when
the peer drops mid-send; `read_head` is size-capped; the handshake length is
range-checked; `Connection::register` is now `unsafe`; the receive drain is a
bulk copy). The following are real but are design-level work beyond a first
prototype:

- **Full-duplex credit deadlock.** Returning credits goes through
  `send_message`, which itself costs a credit. If both peers simultaneously
  reach zero send credits while each still owes grants, neither can send even a
  `CREDIT_ONLY` message — mutual, unrecoverable. The half-duplex HTTP
  request/response demo never hits this. Fix: a credit-return path that does not
  consume a data credit (e.g. a small reserved pool of receive buffers for
  control messages, accounted separately).
- **Reassembly buffer is unbounded.** A consumed receive buffer is re-posted
  (and its credit returned) on *receipt*, not on application *consumption*, so a
  fast sender + slow reader grows the `rx` `VecDeque` without limit. Credit flow
  control bounds the NIC receive pool but not application memory. Fix: defer the
  re-post/credit-return until `read()` has drained the bytes.
- **MR lifetime is not tied to the PD in the type system.** Correct teardown
  ordering lives in `HordStream`'s hand-written `Drop`; a raw
  `Connection` + `MemoryRegion` pair dropped in the wrong order still leaks the
  PD or use-after-frees. `register` being `unsafe` documents the contract; a
  deeper fix has `MemoryRegion` hold a reference (e.g. `Arc<Connection>`).
- **Registered buffers use plain `Box<[u8]>`.** The NIC DMA-writes receive slots
  while Rust forms `&`/`&mut` references over the same allocation — UB under the
  aliasing model even if it works on today's compilers. Sound form: keep
  registered memory behind `UnsafeCell` / raw-pointer access only.
- **No timeouts.** `rnr_retry_count = 7` is infinite RNR retry; combined with
  busy-poll waits, a stalled-but-alive peer hangs `flush()`/`read()` forever.
  Production needs deadlines on the wait loops and tunable CM retry params.
- **Demo server materialises the whole body.** `/size/<n>` allocates and
  pattern-fills up to 1 GiB before sending. Fine for a demo; a real handler
  would stream fixed-size chunks.

## Findings worth folding back into the spec

- **Handshake size (spec 12.1).** The spec's handshake is 60 bytes (14
  meaningful + 46 reserved). The RDMA CM private-data area for an RC connection
  is only ~56 bytes on IB/RoCE, so a 60-byte handshake does not reliably fit.
  This prototype transmits just the 16 meaningful bytes and drops the reserved
  tail. Recommend trimming the reserved field in the spec.
- **Endianness.** Spec 12 doesn't state a byte order for the multi-byte
  envelope/handshake fields. This prototype uses big-endian (network order),
  which has the nice property that the magic `0x484F5244` serialises to the
  ASCII bytes `HORD`. Worth stating explicitly in the spec.
