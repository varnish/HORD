# HORD prototype

A first working implementation of the HORD stream path: **HTTP/1.1 over RDMA
RC**, demonstrated end-to-end over Soft-RoCE.

This implements the two-sided (send/recv) transport described in spec sections
4–6, 8 and 9, **the zero-copy / RDMA-write extension (spec section 7.1–7.4)** —
one-sided `IBV_WR_RDMA_WRITE` straight into a client-registered buffer, advertised
via `X-HORD-RDMA-Write` — **and protocol splitting (spec §7.7)**:
`IBV_WR_RDMA_WRITE_WITH_IMM` delivers the payload plus a transfer-ID immediate to
the client's CQ, so a data-plane consumer collects payloads off the completion
queue without parsing HTTP. The only spec feature still unbuilt is §7.5 GPUDirect
(untestable here — no GPU/real NIC — though the addr/rkey path is opaque, so it
should work unchanged on capable hardware).

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
- **Protocol splitting (spec §7.7).** When both peers also advertise
  `SPLIT_MODE_CAPABLE`, a request carrying `;id=<n>` is served with
  `IBV_WR_RDMA_WRITE_WITH_IMM`: the write lands the payload and delivers the
  transfer ID to the client's CQ as a `RECV_RDMA_WITH_IMM` completion. A
  dispatcher in the stream demuxes completions by opcode (stream message vs.
  payload), so the *data plane* (`SplitReceiver` / async `next_split_completion`)
  collects payloads by ID — out of order, with no HTTP parsing — while the
  control plane still gets the `status=complete` HTTP response. Transfer credits
  (§7.7.6) are pre-posted recv headroom; an immediate returns no stream data
  credit. The `--split` flag on the async/hyper demo exercises it end to end.

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
               and the HordStream byte stream (Read + Write), factored over a
               non-blocking core that both the sync facade and the async wrapper
               drive. Also the zero-copy transport half: capability negotiation,
               buffer registration, and the one-sided RDMA-write driver — plus the
               §7.7 split half: write-with-immediate, the completion dispatcher
               (demux by opcode), transfer-credit recv headroom, and the
               data-plane transfer queue.
hord-zerocopy/ Zero-copy HTTP semantics (spec §7): the X-HORD-RDMA-Write header
               codec and the client/server orchestration over a HordStream;
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

`hord-core`, `hord-stream` and `hord-zerocopy` have **no third-party crate
dependencies** — only `std`, plus the system `librdmacm`/`libibverbs` linked
through the shim (`build.rs` invokes `cc`/`ar` directly, not the `cc` crate). The
async milestone (`hord-async`, and the demo's hyper bins) is the sole exception:
it pulls in `tokio` + `hyper`, confined to those crates so the transport stays
air-gapped-buildable.

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

- **Single-task driver.** The async stream is built for one driving task (as
  `hyper` uses it); two tasks over `tokio::io::split` would both wait on the one
  completion fd and need a multi-waiter scheme. (`SharedAsyncStream` keeps a
  zero-copy write on that *same* one task — it does not add a second waiter.)
  This is also why the §7.7 *data plane* shares the control plane's task rather
  than running as an independent HTTP-unaware consumer thread polling the shared
  CQ directly — the spec's intended split. The mechanism (write-with-immediate,
  opcode demux, demux-by-ID) is all there; only the second waiter is deferred.
- **Thread-per-connection, not thread-per-core.** A real server would use a
  bounded worker pool with `spawn_local`, not one OS thread per connection.
- **Zero-copy source registered per response.** The server registers (and frees)
  a source MR per zero-copy *or split-mode* response; a real server would
  amortize this with a pool (spec §8.3). §7.5 GPUDirect remains unbuilt
  (untestable on this host — see above).

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
  This prototype transmits just the 16 meaningful bytes and drops the reserved
  tail. Recommend trimming the reserved field in the spec.
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
  does so in the C shim, so Rust sees a host-order `u32`). Same-endian peers round
  trip either way; the conversion only matters cross-endian. Recommend §12 say
  the immediate travels big-endian on the wire and the application is responsible
  for the conversion (rather than implying the library does it).
- **The recv buffer a write-with-imm consumes carries no data (spec §7.7.5).**
  The payload lands in the client's *remote-writable* region (via `remote_addr`),
  not in the consumed receive WR's buffer. §7.7.5 already says "contains no data
  ... reposted immediately"; worth adding that the completion's `byte_len` is
  therefore not a length into that buffer and MUST NOT be used to read it.
- **Transfer-credit posting (spec §7.7.6).** §7.7.6 has the client post one extra
  recv buffer *per* split request. This prototype instead pre-posts a fixed
  `split_credits` headroom of recv WRs (a config knob, default 8) on top of the
  data pool and control slack, reposting each on consumption. It satisfies the
  intent (a recv WR is always available for an immediate, without starving the
  data window) without dynamically growing the recv queue per request — simpler,
  at the cost of bounding concurrent in-flight split transfers to that headroom.
  Either model is spec-conformant on the wire; worth noting the headroom approach
  as an option.
