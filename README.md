# HORD: HTTP Over RDMA

**Version 0.1.0 — Draft Specification**

## Abstract

HORD transports HTTP/1.1 over RDMA (InfiniBand and RoCE). It provides a byte-stream abstraction over RDMA's message-oriented queue pairs, allowing unmodified HTTP semantics to operate over RDMA with optional zero-copy data transfer.

HORD targets environments where HTTP clients and servers are connected by RDMA-capable networks — most notably AI training and inference clusters consuming object storage over InfiniBand or RoCE fabrics.

## Status

This document is an early draft intended to seed discussion and guide implementation of a reference library. It is not yet a formal standard.

## Table of Contents

1. [Motivation](#1-motivation)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [Terminology](#3-terminology)
4. [Architecture Overview](#4-architecture-overview)
5. [Connection Lifecycle](#5-connection-lifecycle)
6. [Stream Abstraction Layer](#6-stream-abstraction-layer)
7. [Zero-Copy Extension](#7-zero-copy-extension)
8. [Buffer Management](#8-buffer-management)
9. [Flow Control](#9-flow-control)
10. [Error Handling](#10-error-handling)
11. [Security Considerations](#11-security-considerations)
12. [Wire Format Reference](#12-wire-format-reference)
13. [Implementation Guidance](#13-implementation-guidance)
14. [Relationship to Existing Standards](#14-relationship-to-existing-standards)

---

## 1. Motivation

Hyperscaler object storage (S3, GCS, Azure Blob) is increasingly consumed by GPU compute nodes connected via InfiniBand or RoCE. The kernel TCP/IP stack introduces unnecessary overhead: context switches, buffer copies, and interrupt processing that add latency and consume CPU cycles needed for compute.

RDMA eliminates these costs through kernel bypass and zero-copy transfers, but has historically required application-specific protocols. Rather than replacing HTTP with a custom RDMA protocol, HORD keeps HTTP as the application protocol and replaces only the transport. This preserves the entire HTTP ecosystem — caching semantics, content negotiation, range requests, authentication — while delivering RDMA-class performance.

### 1.1 Target Environments

Any environment with RDMA-capable networking processing data stored in object storage.

### 1.2 Expected Topology

```
Object Storage (S3/GCS/Azure)
        │  HTTP/TCP
   Edge Cache (HORD server)
        │  HTTP/RDMA (HORD)
  Compute Nodes (HORD clients)
```

The edge cache is the RDMA termination point. It speaks standard HTTP upstream and HORD to local compute nodes. HORD adoption requires changes only at the last hop.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- Preserve HTTP semantics exactly. Any valid HTTP/1.1 exchange must work identically over HORD.
- Provide a byte-stream interface over RDMA's message-oriented queue pairs.
- Enable zero-copy data transfer as an optional extension, including direct placement into GPU memory via GPUDirect RDMA.
- Remain transport-agnostic within the RDMA family. Work over InfiniBand and RoCEv2 without protocol changes.
- Support implementation as a library. Primary delivery is a Rust crate with Python bindings.

### 2.2 Non-Goals

- Replacing HTTP. HORD is not a new application protocol.
- HTTP/2 and HTTP/3 framing. HORD targets HTTP/1.1 only. HPACK and binary framing add CPU overhead that defeats the purpose of RDMA, and H/2 stream multiplexing is redundant with cheap per-QP parallelism.
- Kernel-level integration. HORD operates in userspace via `libibverbs`.
- Transport encryption. See [Security Considerations](#11-security-considerations).

---

## 3. Terminology

| Term           | Definition                                                                                                         |
| -------------- | ------------------------------------------------------------------------------------------------------------------ |
| **RC QP**      | Reliable Connected Queue Pair. The RDMA connection primitive used by HORD.                                         |
| **MR**         | Memory Region. A contiguous block registered with the RDMA NIC for direct access.                                  |
| **CQ**         | Completion Queue. Receives notifications when RDMA operations complete.                                            |
| **Send/Recv**  | Two-sided RDMA operations. The sender posts a send; the receiver must have pre-posted a matching receive.          |
| **RDMA Write** | One-sided operation. The initiator writes directly into remote registered memory without involving the remote CPU. |
| **rkey**       | Remote key. Authorizes a remote party to perform RDMA read/write on a memory region.                               |
| **GDR**        | GPUDirect RDMA. Allows RDMA operations to target GPU device memory directly.                                       |
| **ODP**        | On-Demand Paging. Allows RDMA operations on unpinned memory, with page faults handled by the NIC/driver.           |
| **WR / WC**    | Work Request / Work Completion. Instructions posted to a QP and their completion notifications on a CQ.            |

---

## 4. Architecture Overview

```
┌──────────────────────────────────┐
│         HTTP Layer               │
│ (hyper, or other HTTP/1.1 impl)  │
├──────────────────────────────────┤
│      Stream Abstraction Layer    │
│   AsyncRead + AsyncWrite over    │
│   RDMA send/recv operations      │
├──────────────────────────────────┤
│      RDMA Transport Layer        │
│   QP management, MR pools,       │
│   CQ processing, CM events       │
└──────────────────────────────────┘
```

RDMA Transport Layer manages device discovery, protection domain creation, QP lifecycle, memory registration, and completion processing.

Stream Abstraction Layer bridges RDMA's message semantics to a byte-stream interface via `tokio::io::AsyncRead` and `AsyncWrite`. It manages pre-posted receive buffers, segments outgoing data into RDMA sends, and reassembles incoming messages.

HTTP Layer. HORD targets a narrow profile of HTTP/1.1 oriented toward bulk object transfer; see [Section 4.1](#41-http-profile). The request/response model, status codes, and common headers are preserved. Implementations MAY reuse an off-the-shelf HTTP/1.1 parser (e.g., hyper) for header processing, but HORD-specific framing — notably the body-on-the-side semantics of [Section 7](#7-zero-copy-extension) and the data-plane bypass of [Section 7.7](#77-protocol-splitting) — is handled by a HORD-aware adapter sitting between the parser and the stream. The data plane in split mode bypasses the HTTP layer entirely.

### 4.1 HTTP Profile

HORD specifies a narrow subset of HTTP/1.1, sufficient for bulk object reads and explicit about what it omits. The profile is read-oriented in v0.1; write methods may be added in a later revision.

#### 4.1.1 Required

- **Methods:** GET, HEAD.
- **Status codes:** the full 1xx–5xx range as defined by RFC 9110.
- **Framing:** `Content-Length`, `Content-Type`, `Content-Range`. Every HORD response carries a known length; see [Section 4.1.2](#412-out-of-scope) on chunked.
- **Range:** single-range `Range` requests and `Content-Range` in 206 responses. Multipart byteranges are not supported.
- **Conditional requests:** `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, `ETag`, `Last-Modified`.
- **Caching:** `Cache-Control`, `Age`, `Expires`, `Date`.
- **Authorization:** the `Authorization` header is forwarded opaquely (bearer tokens).
- **Connection control:** `Connection: close`. Persistent connections are the default per HTTP/1.1.
- **Identification:** `Server`, `User-Agent` permitted; not interpreted by HORD.
- **HORD extensions:** `X-HORD-*` (see [Section 7](#7-zero-copy-extension) and [Section 12](#12-wire-format-reference)).
- **Pipelining:** Multiple in-flight requests on a single connection are supported and expected — prefetch controllers issue many GETs concurrently. Control-plane responses are returned in request order. Data-plane completions in split mode ([Section 7.7](#77-protocol-splitting)) MAY arrive in any order relative to one another and to the control-plane responses; clients demultiplex by transfer `id`.

#### 4.1.2 Out of scope

Clients SHOULD NOT send and servers MAY reject (with 400 or 501) or ignore the following:

- **Write methods:** PUT, POST, DELETE, PATCH.
- **Other methods:** CONNECT, OPTIONS, TRACE.
- **Content negotiation:** `Accept`, `Accept-Language`, `Accept-Charset`, `Vary`. HORD assumes the client knows the object's representation by key.
- **Compression:** `Content-Encoding`, `Accept-Encoding`. Trading CPU for bandwidth runs counter to HORD's purpose; objects are served as stored.
- **Transfer-Encoding:** `chunked` and other transfer codings. HORD always knows response length, and zero-copy ([Section 7](#7-zero-copy-extension)) requires it.
- **Trailers:** `TE`, `Trailer`.
- **Continuation:** `Expect: 100-continue`.
- **Protocol upgrade:** `Upgrade`, `Connection: upgrade`. There is no upgrade path on a HORD connection.
- **Authentication challenges:** 401/407 with `WWW-Authenticate` / `Proxy-Authenticate` flows. Bearer tokens via `Authorization` only.
- **State:** `Cookie`, `Set-Cookie`.
- **Multipart ranges:** `multipart/byteranges`.

#### 4.1.3 Normalizing upstream content

A HORD server (typically an edge cache) fetching from origins over standard HTTP may receive content that uses out-of-scope features — e.g., `Content-Encoding: gzip` or `Transfer-Encoding: chunked`. The server MUST normalize before serving over HORD: decompress encoded bodies, dechunk transfer-encoded responses, and materialize a known `Content-Length`. The HORD wire MUST NOT carry features the profile excludes.

---

## 5. Connection Lifecycle

HORD connections use the RDMA Connection Manager (CM) for setup and teardown, following the standard RC connection flow.

### 5.1 Server Startup

1. Open the RDMA device and allocate a Protection Domain (PD).
2. Create Completion Queue(s) and allocate/register buffer pools (see [Section 8](#8-buffer-management)).
3. Create an `rdma_cm_id`, bind, and call `rdma_listen()`.

### 5.2 Connection Setup

1. Server receives `RDMA_CM_EVENT_CONNECT_REQUEST`.
2. Server creates a QP, pre-posts receive WRs, and accepts via `rdma_accept()` with the HORD handshake (see [Section 12.1](#121-handshake-cm-private-data)).
3. Client receives `RDMA_CM_EVENT_ESTABLISHED`. QP transitions through INIT -> RTR -> RTS automatically via the CM.
4. Both sides may now post send/recv operations.

#### 5.2.1 RC Parameters

The Reliable Connected QP attributes that govern retry behavior have a material effect on whether transient conditions surface as connection-fatal errors. HORD recommends the following defaults; implementations MAY tune them based on fabric characteristics.

| Attribute        | Recommended | Notes                                                                                            |
| ---------------- | ----------- | ------------------------------------------------------------------------------------------------ |
| `retry_count`    | 7           | Maximum per IBA. Number of times the NIC retries a packet on transport NAK.                      |
| `rnr_retry`      | 7           | Maximum per IBA. Number of RNR (Receiver Not Ready) retries before failing the QP.               |
| `timeout`        | 14          | Local ACK timeout exponent; ~4.1 ms. Reasonable for intra-DC fabrics.                            |
| `min_rnr_timer`  | 12          | NAK delay when no receive buffer is posted; ~640 µs. Pairs with `rnr_retry=7` to absorb bursts.  |

These values are passed via `rdma_conn_param` during `rdma_connect()` / `rdma_accept()` and applied during the CM-driven QP transitions in step 3 above.

### 5.3 Handshake

During `rdma_connect()` and `rdma_accept()`, both sides exchange a handshake in the CM private data field. See [Section 12.1](#121-handshake-cm-private-data) for the wire format.

**Flags:**

| Bit  | Name                 | Description                                                                   |
| ---- | -------------------- | ----------------------------------------------------------------------------- |
| 0    | `ZERO_COPY_CAPABLE`  | Peer supports the zero-copy extension (Section 7)                             |
| 1    | `SPLIT_MODE_CAPABLE` | Peer supports protocol splitting (Section 7.7). Requires `ZERO_COPY_CAPABLE`. |
| 2-15 | Reserved             | Must be zero                                                                  |

Both sides MUST agree on the effective `max_message_size` as `min(client, server)`. The `max_recv_buffers` value informs the peer of the initial receive credit (see [Section 9](#9-flow-control)).

**Version mismatch.** Only version 1 is currently defined. A peer that does not recognize the version in the handshake MUST reject the connection via `rdma_reject()` and SHOULD include its own handshake structure (with its highest supported version) as reject private data. A future revision of HORD MAY define a version-negotiation procedure; v0.1 implementations need only support reject-on-mismatch.

### 5.4 Connection Teardown

1. Complete or abandon outstanding HTTP exchanges.
2. Call `rdma_disconnect()`.
3. Peer receives `RDMA_CM_EVENT_DISCONNECTED`.
4. Both sides drain CQs, destroy QPs, and release resources.

---

## 6. Stream Abstraction Layer

The stream layer presents RDMA as a reliable, ordered byte stream.

### 6.1 Sending

When the HTTP layer writes via `AsyncWrite`:

1. Bytes are appended to a send staging buffer within a registered memory region.
2. On `flush()` or when the staging buffer reaches `max_message_size`, an RDMA send WR is posted.
3. `poll_write()` completes when data is copied to the staging buffer (not on RDMA send completion). Send completion is tracked asynchronously to reclaim the buffer.

A pool of staging buffers allows multiple in-flight sends.

### 6.2 Receiving

1. Pre-posted receive buffers are maintained on the QP.
2. On RDMA recv completion, data is appended to a reassembly buffer.
3. `AsyncRead::poll_read()` drains from the reassembly buffer.
4. Consumed receive buffers are re-posted to maintain credit.

### 6.3 Ordering and Framing

RDMA RC queue pairs deliver messages in order, providing TCP-equivalent ordering without additional sequence numbering.

The stream layer does not impose application-level framing — HTTP's own framing (Content-Length, chunked encoding) delineates messages. Internally, each RDMA send is wrapped in a message envelope (see [Section 12.2](#122-message-envelope)) to support credit management and message boundaries.

---

## 7. Zero-Copy Extension

For large payloads, HORD optionally bypasses the stream layer and places data directly into client memory via RDMA write.

### 7.1 Negotiation

Zero-copy requires both peers to indicate `ZERO_COPY_CAPABLE` in the handshake. It is requested per-HTTP-request via headers and is always optional — the server MAY respond via the stream instead.

### 7.2 Request Headers

The client advertises a registered memory region for the response body:

```http
GET /dataset/shard-00042.tar HTTP/1.1
Host: edge-cache.local
X-HORD-RDMA-Write: addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216
```

See [Section 12.3](#123-x-hord-rdma-write-request-header) for parameter details.

### 7.3 Server Behavior

If the server elects to use zero-copy:

1. **Object fits in the client's buffer.** The server performs an RDMA write into the client's buffer, then sends the HTTP response on the stream with `Content-Length: 0`:

```http
HTTP/1.1 200 OK
Content-Length: 0
Content-Type: application/octet-stream
X-HORD-RDMA-Write: status=complete;bytes_written=14680064
```

The payload is delivered out-of-band by the RDMA write; `bytes_written` carries the authoritative payload size. `Content-Length: 0` preserves the HTTP/1.1 framing rule — `Content-Length` describes bytes that follow on the stream, and zero bytes follow — so off-the-shelf HTTP parsers handle the response correctly without HORD-specific framing logic. Application code retrieves the payload size from `bytes_written`, not from `Content-Length`.

2. **Object exceeds the buffer.** The server returns 413 with no body and reports the actual object size:

```http
HTTP/1.1 413 Content Too Large
Content-Length: 0
X-HORD-RDMA-Write: status=too_large;object_size=1073741824
```

The client may retry with a `Range` header or allocate a larger buffer.

3. **Response has no body.** For HEAD requests and for responses where HTTP semantics define no message body (204 No Content, 304 Not Modified, and others per RFC 9110), the server MUST NOT perform an RDMA write. Any `X-HORD-RDMA-Write` request header is ignored and not echoed in the response.

### 7.4 Response Outcomes

A request bearing `X-HORD-RDMA-Write` produces one of three response statuses:

1. **`status=complete`** — payload was placed in the client's buffer via RDMA write. See [Section 7.3](#73-server-behavior) item 1.
2. **`status=too_large`** — payload exceeds the client's buffer. See [Section 7.3](#73-server-behavior) item 2.
3. **`status=declined`** — the server elected not to use zero-copy for this request (resource pressure, server policy, payload too small to benefit, or pre-flight rejection of the client's rkey/address). The body is sent on the stream as in a non-HORD response:

```http
HTTP/1.1 200 OK
Content-Length: 14680064
Content-Type: application/octet-stream
X-HORD-RDMA-Write: status=declined
```

**Mid-write failure.** Once the server has begun an RDMA write, partial failure leaves the client's buffer in an undefined state. The server MUST NOT signal `complete` unless the entire payload was placed. On mid-write failure (network error, QP transition to error state) the server closes the connection; the client retries on a new connection with a fresh buffer.

**Header presence.** Servers MUST include `X-HORD-RDMA-Write` in any body-bearing response to a request that carried `X-HORD-RDMA-Write`. Bodiless responses ([Section 7.3](#73-server-behavior) item 3) omit it. Absence of the header on a body-bearing response is a protocol violation; clients SHOULD treat it as `declined` and log a warning.

### 7.5 GPUDirect RDMA

When the client registers GPU device memory and provides its address and rkey, the server's RDMA write targets GPU memory directly. This is transparent to HORD — the address and rkey are opaque to the server.

Requirements:

- NVIDIA GPU with GPUDirect RDMA support
- Mellanox ConnectX-5+ (or equivalent with peer memory support)
- `nvidia-peermem` kernel module loaded
- Sufficient GPU BAR1 size

### 7.6 Range Requests

Zero-copy and range requests compose naturally:

```http
GET /dataset/shard-00042.tar HTTP/1.1
Range: bytes=0-16777215
X-HORD-RDMA-Write: addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216
```

### 7.7 Protocol Splitting

Protocol splitting separates the _control plane_ (HTTP exchange) from the _data plane_ (payload delivery), allowing the data consumer to receive payloads without parsing HTTP.

#### 7.7.1 Motivation

In many deployments, the payload consumer (e.g., a GPU training loop) is distinct from the HTTP manager (e.g., a prefetch controller). Protocol splitting uses RDMA write-with-immediate (`IBV_WR_RDMA_WRITE_WITH_IMM`) to deliver both payload and a completion signal directly on the CQ — no HTTP parsing required.

```
┌──────────────────────────────────────────────────────┐
│                   Application                        │
├─────────────────────────┬────────────────────────────┤
│     Control Plane       │        Data Plane          │
│     (HTTP-aware)        │        (HTTP-unaware)      │
│  - Sends requests       │  - Owns receive buffers    │
│  - Parses responses     │  - Polls CQ for            │
│  - Manages rkeys        │    RECV_RDMA_WITH_IMM      │
├─────────────────────────┴────────────────────────────┤
│               HORD Transport Layer                   │
│          (QP, MR, CQ — shared by both)               │
└──────────────────────────────────────────────────────┘
```

A dispatcher demultiplexes completions by opcode: `IBV_WC_RECV` for stream messages (control plane), `IBV_WC_RECV_RDMA_WITH_IMM` for payload completions (data plane).

#### 7.7.2 Mechanism

RDMA write-with-immediate atomically:

1. Writes payload into the client's registered memory (identical to standard zero-copy).
2. Delivers a 32-bit immediate value to the client's CQ, consuming one posted receive buffer.

QP ordering guarantees the payload is fully written before the completion signal arrives.

#### 7.7.3 Request

The client requests split-mode by including the `id` parameter:

```http
GET /dataset/shard-00042.tar HTTP/1.1
Host: edge-cache.local
X-HORD-RDMA-Write: addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216;id=42
```

If `id` is omitted, the server uses plain RDMA write (Section 7.3). If present and the server supports split mode, it SHOULD use write-with-immediate. Otherwise, the server ignores `id`.

#### 7.7.4 Server Behavior

1. Post RDMA write operations for the payload.
2. Post a final write-with-immediate with `imm_data` set to the client's transfer ID (may carry the last payload portion or be zero-length).
3. Send the HTTP response on the stream as in Section 7.3.

**Ordering:** The CQ completion will typically arrive _before_ the HTTP response. Implementations MUST NOT assume a specific ordering between them.

#### 7.7.5 Completion Semantics

When the client's CQ delivers `IBV_WC_RECV_RDMA_WITH_IMM`:

- `wc.imm_data` contains the transfer ID.
- Payload is guaranteed fully written.
- The consumed receive buffer contains no data and SHOULD be reposted immediately.

The data-plane consumer can use the data immediately without waiting for the HTTP response.

#### 7.7.6 Credit Accounting

Write-with-immediate consumes one posted receive buffer. Each request with `X-HORD-RDMA-Write` containing `id` constitutes an implicit grant of one additional receive credit. The client MUST post a receive buffer for this before sending the request.

The server tracks two credit types:

- _Stream credits:_ For stream messages (Section 9).
- _Transfer credits:_ One per in-flight split-mode request, implicitly granted, no explicit replenishment needed.

#### 7.7.7 Failure Handling

- Write-with-immediate not yet posted: fall back to stream response with `status=declined` ([Section 7.4](#74-response-outcomes)).
- Write-with-immediate fails after posting: QP enters error state, connection closes, client retries.
- The server MUST NOT send `status=complete` unless the write-with-immediate succeeded.
- Clients SHOULD implement a timeout for data-plane completions.

---

## 8. Buffer Management

Memory registration (`ibv_reg_mr`) is expensive and should be amortized.

### 8.1 Buffer Pool Architecture

```
Buffer Pool
├── Send Pool: N buffers x max_message_size bytes
├── Recv Pool: M buffers x max_message_size bytes (pre-posted to QP)
└── Cache Pool (server only): Large region for zero-copy RDMA writes
```

### 8.2 Pool Sizing

| Parameter          | Default      | Notes                                                                              |
| ------------------ | ------------ | ---------------------------------------------------------------------------------- |
| `max_message_size` | 64 KiB       | Balances overhead vs. memory usage                                                 |
| `max_recv_buffers` | 32           | Data credits advertised in this side's handshake ([Section 12.1](#121-handshake-cm-private-data)) |
| Send pool          | 16 buffers   | In-flight sends per connection                                                     |
| Recv pool          | 33 buffers   | At least `max_recv_buffers + 1`; the extra is reserved for `CREDIT_ONLY` ([Section 9](#9-flow-control)) |
| Cache pool         | Impl-defined | Depends on memory and workload                                                     |

Each side's recv pool size is governed by the `max_recv_buffers` value *it* advertised in its handshake, not the peer's. If a side advertises N credits, it MUST keep at least N + 1 recv buffers posted at all times.

### 8.3 Memory Registration

Pre-registration (preferred): Register all pools at startup. No registration on the data path.

On-Demand Paging (optional): If the NIC supports ODP, regions can be registered lazily — useful for variable-size cache pools at the cost of slightly higher first-touch latency.

### 8.4 Large Object Handling

- **Stream path:** Segmented across multiple RDMA sends automatically.
- **Zero-copy path:** Single large RDMA write (or multiple if constrained by max WR size). The RDMA layer handles MTU segmentation.

---

## 9. Flow Control

RDMA RC provides reliable delivery but no application-level flow control. HORD uses credit-based flow control at the stream layer.

### 9.1 Credits

- At connection setup, each side has `max_recv_buffers` data credits — the value advertised by the peer in its handshake.
- Each data-bearing send consumes one credit on the sender's tally.
- Credits are replenished by piggybacking a non-zero `credits` value on outgoing messages via the `credits` field in the message envelope ([Section 12.2](#122-message-envelope)).
- Receivers SHOULD send a replenishment proactively — a `CREDIT_ONLY` message ([Section 12.2](#122-message-envelope)) when more than half of `max_recv_buffers` have been consumed without being advertised back to the peer.

### 9.2 Reserved Control Credit

Each side MUST keep at least one recv buffer posted beyond `max_recv_buffers` (see [Section 8.2](#82-pool-sizing)). This buffer is reserved for `CREDIT_ONLY` messages and is replenished immediately upon consumption.

A peer MAY send a single in-flight `CREDIT_ONLY` message regardless of its data-credit tally. This guarantees that credit recovery is always possible: if both peers exhaust their data credits simultaneously, either side can still emit a `CREDIT_ONLY` to break the deadlock. The reserved-credit budget is one per side; a sender MUST NOT have more than one unacknowledged `CREDIT_ONLY` in flight.

### 9.3 Backpressure

When data credits reach zero, `AsyncWrite` blocks (`Poll::Pending`) until replenished. Backpressure propagates through the HTTP layer naturally — pipelined requests stall until the peer drains its receive queue.

---

## 10. Error Handling

### 10.1 Transport Errors

RDMA transport errors (QP errors, protection errors) are fatal to the connection:

1. The stream layer returns an error from `AsyncRead`/`AsyncWrite`.
2. The HTTP layer handles it per its own semantics (e.g., retry on a new connection).
3. The transport layer destroys the QP and releases resources.

### 10.2 Application Errors

HTTP-level errors (4xx, 5xx) are handled entirely at the HTTP layer and are invisible to HORD.

---

## 11. Security Considerations

### 11.1 Transport Security

RDMA does not natively support encryption. In most target environments (InfiniBand or RoCEv2 within a data center), this is acceptable.

Mitigations for shared networks:

- Network isolation: Dedicated RDMA VLANs or InfiniBand partitions.
- IPsec: RoCEv2 traffic can be encrypted at the IP layer (may reduce performance).
- Application-layer encryption: Encrypt objects before storage; decrypt after receipt.

### 11.2 Memory Safety

The zero-copy extension requires sharing memory addresses and rkeys. Mitigations:

- Register dedicated, bounded regions that don't overlap with other application memory.
- Revoke rkeys (`ibv_dereg_mr`) promptly on connection close.
- Validate that RDMA writes stay within communicated bounds.

### 11.3 Denial of Service

Implementations SHOULD enforce:

- Maximum connections per client IP/GID.
- Idle connection timeouts.
- Limits on total registered memory.

---

## 12. Wire Format Reference

All multi-byte integer fields in HORD wire formats are transmitted in network byte order (big-endian). The handshake magic value `0x484F5244` is the ASCII sequence `H`, `O`, `R`, `D` (bytes `0x48 0x4F 0x52 0x44`) and appears verbatim in packet captures. The `imm_data` field used by RDMA write-with-immediate ([Section 7.7](#77-protocol-splitting)) is likewise transmitted in network byte order: the verbs API types it as `__be32` and performs **no** conversion, so the application MUST convert between host and network byte order itself (`htonl` on send, `ntohl` on receive, or `u32::to_be`/`from_be`). A `transfer ID` whose four octets differ will be corrupted on a mixed-endian peer pair if this conversion is omitted.

### 12.1 Handshake (CM Private Data)

```
Offset  Size    Field
0       4       magic (0x484F5244)
4       2       version (1)
6       2       flags
8       4       max_message_size
12      2       max_recv_buffers
14      46      reserved (zero)
                ─────────
                60 bytes total
```

### 12.2 Message Envelope

```
Offset  Size    Field
0       4       length (payload bytes)
4       2       credits
6       2       flags
8       ...     payload (HTTP byte stream)
```

**Flags:**

| Bit  | Name          | Description                                                |
| ---- | ------------- | ---------------------------------------------------------- |
| 0    | `CREDIT_ONLY` | Payload is empty; message exists only to replenish credits |
| 1-15 | Reserved      | Must be zero                                               |

### 12.3 X-HORD-RDMA-Write Request Header

```
X-HORD-RDMA-Write: addr=<hex u64>;rkey=<hex u32>;len=<decimal u64>[;id=<decimal u32>]
```

| Parameter | Type        | Description                                         |
| --------- | ----------- | --------------------------------------------------- |
| `addr`    | hex u64     | Start address of client's registered receive buffer |
| `rkey`    | hex u32     | Remote key authorizing server writes                |
| `len`     | decimal u64 | Buffer capacity in bytes                            |
| `id`      | decimal u32 | Optional. Transfer ID for split-mode (Section 7.7)  |

### 12.4 X-HORD-RDMA-Write Response Header

```
X-HORD-RDMA-Write: status=complete;bytes_written=<decimal u64>
X-HORD-RDMA-Write: status=too_large;object_size=<decimal u64>
X-HORD-RDMA-Write: status=declined
```

---

## 13. Implementation Guidance

### 13.1 Reference Implementation

```
hord/
├── hord-core/         # RDMA transport: device mgmt, QP lifecycle, MR pools, CQ processing
├── hord-stream/       # Stream abstraction: AsyncRead/AsyncWrite over RDMA send/recv
├── hord-client/       # HTTP client using hyper over hord-stream
├── hord-server/       # HTTP server using hyper over hord-stream
├── hord-zerocopy/     # Zero-copy extension: X-HORD-RDMA-Write middleware
└── pyhord/            # Python bindings via PyO3
```

### 13.2 Rust API Surface

**Server:**

```rust
let config = HordConfig {
    listen_addr: "10.0.0.1:4791".parse()?,
    max_message_size: 65536,
    recv_pool_size: 32,
    send_pool_size: 16,
    zero_copy: true,
    ..Default::default()
};

let listener = HordListener::bind(config).await?;
loop {
    let (stream, peer) = listener.accept().await?;
    tokio::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(stream, my_service)
            .await
    });
}
```

**Client:**

```rust
let connector = HordConnector::new(HordClientConfig::default());
let client = Client::builder(TokioExecutor::new()).build(connector);
let resp = client.get("hord://edge-cache:4791/dataset/shard-042.tar".parse()?).await?;
```

**Zero-copy with GPU buffer:**

```rust
let buf = RdmaBuffer::alloc(16 * 1024 * 1024, &connector)?;
let resp = client.request(
    Request::builder()
        .uri("hord://edge-cache:4791/dataset/shard-042.tar")
        .header("X-HORD-RDMA-Write", buf.header_value())
        .body(Empty::<Bytes>::new())?
).await?;
// If response has X-HORD-RDMA-Write: status=complete, data is in buf
```

**Split-mode:**

```rust
let receiver = SplitReceiver::new(&connector)?;
let bufs: Vec<RdmaBuffer> = (0..8)
    .map(|_| RdmaBuffer::alloc(16 * 1024 * 1024, &connector))
    .collect::<Result<_, _>>()?;

for (i, buf) in bufs.iter().enumerate() {
    client.request(
        Request::builder()
            .uri(format!("hord://edge-cache:4791/dataset/shard-{i:05}.tar"))
            .header("X-HORD-RDMA-Write", buf.header_value_with_id(i as u32))
            .body(Empty::<Bytes>::new())?
    );
}

// Data plane: poll completions directly — no HTTP parsing
while let Some(completion) = receiver.poll_completion().await? {
    process_shard(&bufs[completion.transfer_id as usize]);
}
```

### 13.3 Python API

```python
import pyhord

client = pyhord.Client("10.0.0.1:4791")
response = client.get("/dataset/shard-042.tar")

# Zero-copy into GPU memory
import torch
buf = pyhord.GpuBuffer(torch.empty(16 * 1024 * 1024, dtype=torch.uint8, device='cuda:0'))
response = client.get("/dataset/shard-042.tar", rdma_buffer=buf)

# As a PyTorch DataLoader
from pyhord.torch import HordDataset
dataset = HordDataset(
    server="10.0.0.1:4791",
    keys=[f"/dataset/shard-{i:05d}.tar" for i in range(1000)],
    prefetch=8,
)
loader = torch.utils.data.DataLoader(dataset, batch_size=None)
for batch in loader:
    model(batch)
```

### 13.4 URI Scheme and Port

HORD uses the `hord://` URI scheme. Implementations SHOULD also support transparent upgrade from `http://` via a mechanism TBD (DNS SRV, Alt-Svc, or out-of-band configuration).

Default port: **4791** (provisional, subject to change before 1.0).

### 13.5 Testing

Implementations SHOULD support loopback mode using software RDMA (`rxe` or `siw` kernel modules) for development and CI without RDMA hardware.

---

## 14. Relationship to Existing Standards

| Standard                | Relationship                                                                                                                            |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **iSER, SRP, NFS/RDMA** | Existing RDMA protocols for other application layers (SCSI, NFS). HORD is the HTTP analog.                                              |
| **SMB Direct**          | Closest precedent — byte-stream over RDMA with optional direct data placement for SMB. HORD follows a similar pattern adapted for HTTP. |
| **HTTP/3 / QUIC**       | Complementary. HTTP/3 targets internet-scale; HORD targets data center fabrics. An edge cache _could_ speak both.                       |
| **UCX**                 | Could serve as HORD's transport layer instead of raw `libibverbs`. Valid implementation strategy, not required.                         |

---

## License

This specification is released under the Apache License 2.0.

## Authors

Per Buer, Varnish Software

## Changelog

- **v0.1.0** — Initial draft. Stream abstraction, zero-copy extension, buffer management, flow control.
