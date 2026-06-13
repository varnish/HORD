# HORD prototype

A first working implementation of the HORD stream path: **HTTP/1.1 over RDMA
RC**, demonstrated end-to-end over Soft-RoCE.

This implements the two-sided (send/recv) transport described in spec sections
4–6, 8 and 9, **the zero-copy / RDMA-write extension (spec section 7.1–7.4)** —
one-sided `IBV_WR_RDMA_WRITE` straight into a client-registered buffer, advertised
via `X-HORD-RDMA-Write` — **and protocol splitting (spec §7.7)**:
`IBV_WR_RDMA_WRITE_WITH_IMM` delivers the payload plus a transfer-ID immediate to
the client's CQ, so a data-plane consumer collects payloads off the completion
queue without parsing HTTP. Single-range `Range` requests (spec §7.6) compose with
the zero-copy path in the sync demo. The only spec feature still unbuilt is §7.5
GPUDirect (untestable here — no GPU/real NIC — though the addr/rkey path is opaque,
so it should work unchanged on capable hardware).

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
- **Zero-copy responses (spec §7.1–7.4).** When both peers advertise
  `ZERO_COPY_CAPABLE`, a `GET /size/<n>` carrying `X-HORD-RDMA-Write` is served
  by a one-sided RDMA write straight into the client's registered buffer; the
  HTTP response is `Content-Length: 0` + `status=complete;bytes_written=<n>`, and
  the body never touches the stream. The `too_large` (413) and `declined`
  (stream fallback) outcomes are handled too. Works on both the sync and
  async/hyper demos.
- **Range requests (spec §7.6).** A `GET /size/<n>` carrying a single-range
  `Range: bytes=…` (`a-b`, `a-`, or `-n`) is answered with `206 Partial Content`
  and `Content-Range: bytes start-end/total`, composed with the zero-copy path:
  the server RDMA-writes just the sub-range into the client's buffer. This needed
  *no* transport change — the one-sided write is offset-agnostic, so serving a
  range is the whole-object path with the source filled from the range's absolute
  offset. A range past the end of the object returns `416` + `Content-Range: bytes
  */total` with no write (and, being bodiless, no `X-HORD-RDMA-Write` per §7.4); a
  multi-range request is served as a full `200` (no `multipart/byteranges`). The
  sync demo's `--range` exercises it (stream and zero-copy); the async bins are a
  follow-up.
- **Protocol splitting (spec §7.7).** When both peers also advertise
  `SPLIT_MODE_CAPABLE`, a request carrying `;id=<n>` is served with
  `IBV_WR_RDMA_WRITE_WITH_IMM`: the write lands the payload and delivers the
  transfer ID to the client's CQ as a `RECV_RDMA_WITH_IMM` completion. A
  dispatcher in the stream demuxes completions by opcode (stream message vs.
  payload), so the *data plane* (`SplitReceiver` / async `next_split_completion`)
  collects payloads by ID — out of order, with no HTTP parsing — while the
  control plane still gets the `status=complete` HTTP response. Transfer credits
  (§7.7.6) are pre-posted recv headroom, advertised in the handshake and
  enforced sender-side: a sender bounds its in-flight write-with-immediates by
  the peer's advertised window and back-pressures rather than overrunning the
  peer's recv WRs; an immediate returns no stream data credit. The `--split`
  flag on the async/hyper demo exercises it end to end.

Verified on a Soft-RoCE device (`rxe0`, looped back over a local Ethernet NIC):
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
hord-core/     RDMA transport. Safe Rust over librdmacm + libibverbs through
               the `sideway` crate (a safe rdma-core wrapper), plus
               `rdma-mummy-sys` for the few CQ-event-channel calls sideway does
               not yet wrap. (Earlier revisions drove rdma-core through a
               hand-written C shim; the sideway port replaced it — see git
               history.)
hord-stream/   HORD wire protocol: handshake, envelope, credit flow control,
               and the HordStream byte stream (Read + Write), factored over a
               non-blocking core that both the sync facade and the async wrapper
               drive. Also the zero-copy transport half: capability negotiation,
               buffer registration, and the one-sided RDMA-write driver — plus the
               §7.7 split half: write-with-immediate, the completion dispatcher
               (demux by opcode), transfer-credit recv headroom, and the
               data-plane transfer queue.
hord-zerocopy/ Zero-copy HTTP semantics (spec §7). Default build: the pure
               X-HORD-RDMA-Write header codec (RdmaWriteReq / RdmaWriteStatus /
               RdmaWriteAction) — no dependencies, no RDMA libraries. The `rdma`
               feature adds the client/server orchestration over a HordStream,
               split-mode dispatch (§7.7) and the SplitReceiver data-plane handle.
hord-async/    Async wrapper: tokio AsyncRead/AsyncWrite over a HordStream,
               driving the CQ completion-channel fd with AsyncFd (no busy-poll);
               SharedAsyncStream lets a hyper handler drive a zero-copy or
               split-mode (write-with-imm) write, and next_split_completion
               receives data-plane completions.
hord-demo/     hord-server / hord-client (sync) and hord-server-async /
               hord-client-async (hyper over the async stream). --zero-copy opts
               into the RDMA-write path; --split (async) opts into §7.7.
```

Dependency story: `hord-core` and `hord-stream` take only `sideway` (the safe
`librdmacm`/`libibverbs` wrapper) plus its `rdma-mummy-sys` bindings; the async
milestone (`hord-async`, and the demo's hyper bins) adds `tokio` + `hyper`,
confined to those crates. **`hord-zerocopy` is special: its default build has no
dependencies at all** — the pure `X-HORD-RDMA-Write` header codec links with no
RDMA library, so an embedder can unit-test header handling on a machine with no
NIC and no rdma-core (`cargo test -p hord-zerocopy`). Its `rdma` feature pulls in
`hord-stream` — and so the RDMA libraries — to add the write orchestration.

## Building

Needs the RDMA dev packages plus clang/libclang (sideway's `rdma-mummy-sys`
runs bindgen against the rdma-core headers):

```sh
sudo apt-get install -y libibverbs-dev librdmacm-dev clang libclang-dev pkg-config
cargo build --release
```

## Running (Soft-RoCE loopback)

The defaults target an `rxe0` device over the RoCEv2 IP `192.0.2.1` (a reserved
RFC 5737 documentation address). Assign that address to your rxe-backing NIC, or
point the demos elsewhere with `$HORD_TEST_IP` (or `--bind`/`--server`); see
`CLAUDE.md` for the device setup. `rdma_cm` resolves the IP to the RoCEv2 GID and
the rxe transport loops it back internally. (Note: `127.0.0.1` does **not** work
— it routes via `lo`, which has no RDMA device.)

```sh
# Terminal 1
./target/release/hord-server                 # listens on 192.0.2.1:4791

# Terminal 2
./target/release/hord-client --path /                       # small greeting
./target/release/hord-client --path /size/67108864          # 64 MiB, integrity-checked
./target/release/hord-client --path /size/67108864 --zero-copy   # via one-sided RDMA write
```

Routes: `GET /` → greeting; `GET /size/<n>` → `<n>` bytes of a verifiable
pattern; anything else → 404. `--zero-copy` advertises a registered buffer and,
when the server honours it, the body is RDMA-written into that buffer
(`delivery: zero-copy`) instead of streamed. The async bins
(`hord-server-async` / `hord-client-async --zero-copy`) behave identically over
`hyper`.

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

**One CQ per connection**, shared by sends, receives and one-sided RDMA writes;
work requests are tagged by `wr_id` (top bits = send / control-send / RDMA-write
lane, low bits = buffer slot). A write completion is reaped by a dedicated
counter, separate from the send and receive pools.

## Limitations (prototype scope)

The async milestone (Pass 3, in `hord-async` + the demo's `*-async` bins) cleared
most of the original cuts. What it resolved:

- ~~**Synchronous + busy-polled.**~~ `hord-async` drives the CQ
  completion-channel fd with `tokio::io::unix::AsyncFd`, so a blocked connection
  *parks* instead of busy-polling: measured ~90 µs of CPU over a 1 s idle read,
  versus ~800 ms (a full core) for the synchronous busy-poll. The `std::io`
  stream remains as the busy-polled reference path.
- ~~**One connection at a time.**~~ `hord-server-async` accepts in a loop and
  runs each connection on its own thread (current-thread runtime + `hyper`);
  verified with 6 concurrent transfers.
- ~~**No graceful half-close detection.**~~ The async stream registers the CM fd
  and maps a peer `DISCONNECTED` to a clean EOF. (The *synchronous* stream still
  has none — it relies on `Content-Length` + `flush()`-before-disconnect.)
- ~~**HTTP is hand-rolled.**~~ `hyper` runs unmodified over the async stream
  (`http_body::Body` streams `/size/<n>` in fixed-size chunks). The hand-rolled
  codec stays in the sync demo.

And then the **zero-copy pass** (Pass 4, `hord-zerocopy` + `--zero-copy` in the
demos) cleared the last copy on the data path:

- ~~**One copy on each path.**~~ The *stream* path still copies into the
  registered staging buffer on send (the receive-side reassembly copy was already
  gone — payload held in place until `read()`). The *zero-copy* path eliminates
  the staging copy entirely: the server fills a registered source once and
  RDMA-writes it straight into the client's buffer, which the client reads in
  place — no transport copy. On Soft-RoCE the throughput gain is modest (rxe
  copies in software in the kernel); on real RDMA hardware this is the whole
  point.

What remains:

- **Multi-task driving — done (`AsyncHordStream::into_split`).** A single RC
  connection has one CQ and one completion fd carrying interleaved completions
  for every direction, so two tasks each parking on that fd (e.g. via
  `tokio::io::split`) clobber each other's waker and steal each other's
  completions — the async stream's own `poll_*` impls are sound only for one
  driving task (as `hyper` uses it). `into_split` resolves this with a reactor
  split: one **pump** task owns the fd, drains the CQ, and wakes every parked
  handle after each drain; the returned `ReadHalf` / `WriteHalf` / `DataPlane`
  never touch the fd — they run the same non-blocking `HordStream` primitives and
  re-park on a shared waker list. This makes two-task full-duplex work (test
  `async_full_duplex_split`, 16 MiB each way) and lets the §7.7 *data plane* run
  as an independent HTTP-unaware consumer on its own task, concurrent with the
  control plane (test `split_data_plane_separate_task`) — the spec's intended
  split. The `HordStream` state machine was unchanged; it only ever lacked a way
  to be driven from more than one async task. (`SharedAsyncStream` still keeps a
  zero-copy write on the *single* hyper task and is the right tool there.)
- **Thread-per-core via `HordListener`.** The async server topology now lives in a
  reusable library type, `hord_async::HordListener` (`hord-async/src/listener.rs`):
  an accept loop + a thread-per-core worker pool (one current-thread runtime /
  `LocalSet` + completion domain per worker), each connection `spawn_local`d on its
  worker, plus a `watch`-driven graceful shutdown. The host supplies only a
  per-connection service closure `(AsyncHordStream, SocketAddr) -> impl Future` —
  the Carapace "Blocker 0" seam (see TODO.md). The demo server runs on it.
- **Zero-copy source registered per response.** The server registers (and frees)
  a source MR per zero-copy *or split-mode* response; a real server would
  amortize this with a pool (spec §8.3). §7.5 GPUDirect remains unbuilt
  (untestable on this host — see above).
- **Range requests are sync-demo only.** §7.6 is wired into `hord-server` /
  `hord-client`; the async/hyper bins are a fast-follow — the range logic belongs
  in the forked `serve_zero_copy` (see TODO.md).

## Open issues from code review (deferred, by design)

A max-effort review surfaced these. The clear-cut correctness/soundness bugs
were fixed (send/recv error paths now mark the connection closed instead of
leaking a slot; `flush()` returns an error instead of silently truncating when
the peer drops mid-send; `read_head` is size-capped; the handshake length is
range-checked; `Connection::register` was made `unsafe` (later superseded by the
safe `register_buffer` — see the soundness pass below); the receive drain is a
bulk copy).

A later **flow-control pass** then redesigned credit handling and fixed two
more:

- **Full-duplex credit deadlock.** Returning credits used to go through
  `send_message`, which itself cost a credit, so two peers that simultaneously
  hit zero credits while each owed grants could deadlock. Credit-returns now
  travel a separate, self-clocked *control lane* — a small pool of always-posted
  receive buffers (`CTRL_RECV_SLACK`) plus a reserved control send slot bounded
  by one in-flight message rather than by a data credit. No wire-format change.
- **Reassembly buffer was unbounded.** A received data buffer is now held in
  place and only re-posted / credited on application *consumption* in `read()`,
  not on receipt — so backpressure reaches the sender and the reassembly
  footprint is bounded to the receive pool. (`fullduplex_tests::full_duplex_bulk`
  exercises both fixes.)

A **soundness & ownership pass** then closed the two memory-model issues, both
in `hord-core`'s buffer/MR ownership model:

- **Aliasing UB.** Registered storage is now `Box<[UnsafeCell<u8>]>` reached only
  through raw pointers (`UnsafeCell::raw_get`), so the NIC can DMA into some
  slots while we read/write others without ever forming a `&`/`&mut [u8]` over
  the allocation — sound under the aliasing model, not merely on today's
  compilers. Envelope encode/decode and payload copies route through stack
  buffers and `RegisteredBuffer::copy_in`/`copy_out` (raw `copy_nonoverlapping`).
- **MR↔PD lifetime.** `Connection::register_buffer` returns a `RegisteredBuffer`
  that owns its storage and holds an `Arc<Connection>`, so the PD provably
  outlives every MR regardless of drop order — registration is now a *safe* call.
  `HordStream`'s `Drop` shrank to a single `shutdown()`: the one ordering step
  that must stay at runtime is quiescing DMA (destroy the QP) before the MRs
  deregister; the PD/MR lifetime itself is now type-enforced, not hand-rolled.

Both of the remaining design-level items were then closed by the **async pass**:

- ~~**No timeouts.**~~ The CM retry params (`rnr_retry_count` / `retry_count` /
  resolve timeout) are now tunable via `HordConfig::cm`, and the async stream is
  cancellable, so a stalled-but-alive peer is bounded with `tokio::time::timeout`
  instead of hanging forever. (The synchronous busy-poll path is still
  un-deadlined.)
- ~~**Demo server materialises the whole body.**~~ The async server's
  `/size/<n>` streams a verifiable pattern in fixed-size (256 KiB) chunks via a
  custom `http_body::Body`, with no up-front allocation.

## Findings worth folding back into the spec

- **Handshake size (spec 12.1).** The spec's handshake is 60 bytes (14
  meaningful + 46 reserved). The RDMA CM private-data area for an RC connection
  is only ~56 bytes on IB/RoCE, so a 60-byte handshake does not reliably fit.
  This prototype transmits just 16 bytes and drops the rest of the reserved
  tail. Bytes 14..16 — reserved in the spec — now carry the `split_credits`
  transfer-credit window (see below); a peer that leaves them zero (or sends only
  14 bytes) reads as zero credits, so split mode declines gracefully. Recommend
  trimming the reserved field in the spec and defining the credit field.
- **Endianness.** Spec 12 doesn't state a byte order for the multi-byte
  envelope/handshake fields. This prototype uses big-endian (network order),
  which has the nice property that the magic `0x484F5244` serialises to the
  ASCII bytes `HORD`. Worth stating explicitly in the spec.
- **Handshake flag bits (spec 5.3).** The spec defines bit 0 `ZERO_COPY_CAPABLE`
  and bit 1 `SPLIT_MODE_CAPABLE`; there is no HTTP/2 flag (HTTP/2 was dropped
  from the spec). An early prototype had stubbed an `HTTP2_CAPABLE` at bit 1 and
  pushed split mode to bit 2 — now corrected to match §5.3. (No on-wire impact:
  only bit 0 is currently set.)
- **Zero-copy MR access flags (spec 7 / 8.3).** §7 doesn't spell out the access
  flags. The client's destination buffer must be registered
  `IBV_ACCESS_REMOTE_WRITE`, and per IBA that flag is only valid in combination
  with `IBV_ACCESS_LOCAL_WRITE` — so the buffer is registered with *both*. The
  server's source buffer needs only local access (the NIC reads it). Worth a
  one-line note in §8.3.
- **Zero-copy write sizing (spec 7.3 / 8.4).** A single `IBV_WR_RDMA_WRITE` WR
  carries the whole body — the NIC segments it into MTU-sized packets — so §8.4's
  "single large RDMA write" is literally one WR in the common case. This
  prototype caps a WR at 1 GiB and would split a larger object across WRs bounded
  by the send-queue depth; verified up to 64 MiB single-WR on Soft-RoCE. The
  `addr` advertised in `X-HORD-RDMA-Write` is the buffer's absolute virtual
  address and `rkey` is its MR rkey, exactly as §12.3 describes — confirmed
  interoperable end to end.
- **Split-mode completion opcode (spec §7.7.5).** The client recognises a payload
  completion by the verbs opcode `IBV_WC_RECV_RDMA_WITH_IMM` (value **129** =
  `(1<<7)+1`, the entry after `IBV_WC_RECV`), confirmed on Soft-RoCE. Worth
  stating the opcode explicitly in §7.7.5 alongside "demultiplexes by opcode".
- **Immediate byte order (spec §12).** §12 says the immediate is presented "in
  host byte order ... the verbs implementation handles conversion." In practice
  the verbs `imm_data` field is `__be32` and *no* automatic conversion happens —
  the application must `htonl` on send and `ntohl` on receive (this prototype
  does so in `hord-core` — `id.to_be()` on post, `u32::from_be` on the
  completion — so the rest of the code sees a host-order `u32`). Same-endian
  peers round trip either way; the conversion only matters cross-endian.
  Recommend §12 say
  the immediate travels big-endian on the wire and the application is responsible
  for the conversion (rather than implying the library does it).
- **The recv buffer a write-with-imm consumes carries no data (spec §7.7.5).**
  The payload lands in the client's *remote-writable* region (via `remote_addr`),
  not in the consumed receive WR's buffer. §7.7.5 already says "contains no data
  ... reposted immediately"; worth adding that the completion's `byte_len` is
  therefore not a length into that buffer and MUST NOT be used to read it.
- **Transfer-credit flow control (spec §7.7.6).** §7.7.6 frames transfer credits
  as an *implicit per-request grant* (the client posts one extra recv buffer per
  split request; "no explicit replenishment needed") and does not advertise a
  count. This prototype instead uses a **negotiated static window**: each side
  pre-posts a fixed `split_credits` headroom of recv WRs (config knob, default 8,
  reposted on consumption per §7.7.5) *and advertises that count* in the
  handshake (bytes 14..16). The sender then bounds its in-flight
  write-with-immediates against the peer's advertised window
  (`imm_outstanding <= peer_split_credits`), freeing a credit when each
  imm-bearing WR is acked. An overrun **back-pressures** — the blocking path
  reaps an in-flight transfer and retries, the async path returns `WouldBlock`
  and re-polls — instead of silently RNR-stalling (the failure mode before this
  change: with `rnr_retry=7` the transfer hung indefinitely; lower it and the QP
  errored, with no diagnostic either way). Split mode declines (falls back to the
  stream) against a peer that advertises the capability bit but a zero window.
  Recommend §7.7.6 either adopt an advertised window or spell out the recv-queue
  accounting an implicit-grant sender must do to avoid the same overrun. (The
  earlier prototype enforced neither — `split_credits` was a local sizing
  heuristic only.)
- **Range requests (spec §7.6).** §7.6 only says ranges "compose naturally" with
  zero-copy. In practice the composition is *free*: a one-sided RDMA write is
  offset-agnostic, so serving `bytes=a-b` is exactly the whole-object path with
  `object_size = b-a+1` and the source filled from offset `a` — no transport
  change, and `X-HORD-RDMA-Write`'s `len`/`bytes_written` simply describe the
  *range* length rather than the whole object. Worth pinning down in §7.6 the two
  HTTP edges it currently leaves implicit: an unsatisfiable range → `416` +
  `Content-Range: bytes */total` (a bodiless response, so per §7.4 it omits
  `X-HORD-RDMA-Write`), and a multi-range request → served as a full `200` (HORD
  has no `multipart/byteranges`, §4.1.2). This prototype implements the sync demo
  that way; the same offset-agnostic mechanism would cover split mode (§7.7)
  unchanged.
