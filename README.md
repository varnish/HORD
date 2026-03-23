# HORD: HTTP Over RDMA

**Version 0.1.0 — Draft Specification**

## Abstract

HORD defines a method for transporting HTTP/1.1 over RDMA (Remote Direct Memory Access) transports, including InfiniBand and RoCE (RDMA over Converged Ethernet). It provides a byte-stream abstraction over RDMA's message-oriented queue pair interface, allowing unmodified HTTP/1.1 semantics to operate over RDMA with optional extensions for zero-copy data transfer.

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

Hyperscaler object storage (S3, GCS, Azure Blob) is increasingly consumed by GPU compute nodes connected via InfiniBand or RoCE. In these environments, the kernel TCP/IP stack introduces unnecessary overhead: context switches, buffer copies, and interrupt processing that add latency and consume CPU cycles needed for compute.

RDMA eliminates these costs through kernel bypass and zero-copy transfers. However, RDMA has historically required application-specific protocols, fragmenting the ecosystem and making interoperability difficult.

HTTP is the universal protocol of object storage. Rather than replacing HTTP with a custom RDMA protocol, HORD keeps HTTP as the application protocol and replaces only the transport layer. This preserves the entire HTTP ecosystem — caching semantics, content negotiation, range requests, authentication, and existing tooling — while delivering RDMA-class performance.

### 1.1 Target Environments

- AI training clusters reading datasets from object storage through caching proxies
- AI inference systems loading model weights and serving predictions
- High-frequency trading infrastructure accessing market data and analytics
- Any environment with RDMA-capable networking and HTTP-based data access patterns

### 1.2 Expected Topology

```
Object Storage (S3/GCS/Azure)
        │
        │  HTTP/TCP
        ▼
  Mid-Tier Cache
        │
        │  HTTP/TCP
        ▼
   Edge Cache (HORD server)
        │
        │  HTTP/RDMA (HORD)
        ▼
  Compute Nodes (HORD clients)
```

The edge cache is the RDMA termination point. It speaks standard HTTP upstream and HORD to local compute nodes. This means HORD adoption requires changes only at the last hop, not across the entire infrastructure.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- **Preserve HTTP/1.1 semantics exactly.** A HORD connection must be indistinguishable from a TCP connection at the HTTP layer. Any valid HTTP/1.1 exchange must work identically over HORD.

- **Provide a byte-stream interface.** The transport layer presents a reliable, ordered byte stream to the HTTP implementation, abstracting RDMA's message-oriented queue pairs.

- **Enable zero-copy data transfer as an optional extension.** For large payloads, HORD defines an HTTP extension that uses RDMA write operations to place data directly into client-specified memory, including GPU memory via GPUDirect RDMA.

- **Remain transport-agnostic within the RDMA family.** HORD must work over InfiniBand and RoCEv2 without protocol changes.

- **Support implementation as a library.** The primary delivery mechanism is a Rust crate with Python bindings, not a kernel module or OS facility.

### 2.2 Non-Goals

- **Replacing HTTP.** HORD is not a new application protocol.
- **Kernel-level integration.** HORD operates in userspace via `libibverbs`.
- **HTTP/2 or HTTP/3.** HORD transports HTTP/1.1 only. HTTP/2's multiplexing and flow control are redundant over RDMA's native capabilities.
- **Authentication or authorization.** RDMA provides no built-in auth, but HORD is just HTTP — standard HTTP authentication mechanisms (`Authorization` headers, tokens) work unchanged over HORD. Fabric-level access control (InfiniBand P_Keys, VLAN segmentation) provides the network isolation layer.
- **Multicast or unreliable transport.** HORD uses Reliable Connected (RC) queue pairs only.
- **Transport encryption.** See [Security Considerations](#11-security-considerations).

---

## 3. Terminology

| Term | Definition |
|------|-----------|
| **HORD** | HTTP Over RDMA — this specification. |
| **RC QP** | Reliable Connected Queue Pair. The RDMA connection primitive used by HORD. |
| **MR** | Memory Region. A contiguous block of memory registered with the RDMA NIC for direct access. |
| **CQ** | Completion Queue. Receives notifications when RDMA operations complete. |
| **Send/Recv** | Two-sided RDMA operations. The sender posts a send; the receiver must have pre-posted a matching receive. |
| **RDMA Write** | One-sided operation. The initiator writes directly into the remote side's registered memory without involving the remote CPU. |
| **rkey** | Remote key. Authorizes a remote party to perform RDMA read/write on a memory region. |
| **GDR** | GPUDirect RDMA. Allows RDMA operations to target GPU device memory directly. |
| **ODP** | On-Demand Paging. Allows RDMA operations on memory that is not pinned, with page faults handled transparently by the NIC/driver. |
| **WR** | Work Request. An instruction posted to a queue pair. |
| **WC** | Work Completion. A notification on a CQ that a WR has completed. |

---

## 4. Architecture Overview

HORD is structured as three layers:

```
┌──────────────────────────────────┐
│         HTTP Layer               │
│   (hyper or any HTTP/1.1 impl)   │
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

### 4.1 Layer Responsibilities

**RDMA Transport Layer** manages device discovery, protection domain creation, queue pair lifecycle, memory registration, and completion processing. It exposes an internal API for posting send/recv work requests and polling completions.

**Stream Abstraction Layer** bridges RDMA's message semantics to a byte-stream interface. It manages pre-posted receive buffers, segments outgoing byte streams into RDMA send operations, and reassembles incoming messages into a contiguous stream. This layer implements `tokio::io::AsyncRead` and `tokio::io::AsyncWrite`.

**HTTP Layer** is an unmodified HTTP/1.1 implementation (e.g., hyper) operating over the stream. It has no knowledge of RDMA. HORD's zero-copy extension is implemented as HTTP headers interpreted by middleware, not by modifying the HTTP stack itself.

HORD is HTTP/1.1 only. HTTP/2's multiplexing and flow control add complexity that provides no benefit over RDMA — RDMA already delivers reliable, ordered, low-latency transport, and HORD's credit-based flow control (Section 9) handles backpressure at the transport level. Connection multiplexing is inexpensive over RDMA (QP setup is fast, and there is no TCP handshake overhead), making HTTP/2's stream multiplexing unnecessary.

---

## 5. Connection Lifecycle

HORD connections use the RDMA Connection Manager (CM) for setup and teardown, following the standard RC connection flow.

### 5.1 Server Startup

1. Open the RDMA device and allocate a Protection Domain (PD).
2. Create a shared Completion Queue (CQ) or per-connection CQs based on configuration.
3. Allocate and register the buffer pools (see [Buffer Management](#8-buffer-management)).
4. Create an `rdma_cm_id` and bind to the listen address.
5. Call `rdma_listen()`.

### 5.2 Connection Setup

1. Server receives `RDMA_CM_EVENT_CONNECT_REQUEST`.
2. Server creates a QP in INIT state, associated with its PD and CQ.
3. Server pre-posts receive WRs on the new QP (at least `MIN_RECV_POSTED`, see [Flow Control](#9-flow-control)).
4. Server accepts the connection via `rdma_accept()`, including private data with the HORD handshake (see [5.3](#53-handshake)).
5. Client receives `RDMA_CM_EVENT_ESTABLISHED`. QP transitions through INIT → RTR → RTS automatically via the CM.
6. Both sides may now post send/recv operations.

### 5.3 Handshake

During `rdma_connect()` and `rdma_accept()`, both sides exchange a handshake in the CM private data field (up to 196 bytes for RC connections):

```
HORD Handshake (v1):
  magic:            u32  = 0x484F5244 ("HORD")
  version:          u16  = 1
  flags:            u16
  max_message_size: u32  (maximum bytes per RDMA send, excluding framing)
  max_recv_buffers: u16  (number of pre-posted receive buffers)
  reserved:         [u8; 44]
```

**Flags:**

| Bit | Name | Description |
|-----|------|-------------|
| 0 | `ZERO_COPY_CAPABLE` | Peer supports the zero-copy extension (Section 7) |
| 1-15 | Reserved | Must be zero |

Both sides MUST agree on the effective `max_message_size` as `min(client, server)`. The `max_recv_buffers` value informs the peer of the initial receive credit (see [Flow Control](#9-flow-control)).

### 5.4 Connection Teardown

Either side may initiate teardown:

1. Complete all outstanding HTTP exchanges (graceful) or abandon them (abrupt).
2. Call `rdma_disconnect()`.
3. Peer receives `RDMA_CM_EVENT_DISCONNECTED`.
4. Both sides drain CQs, destroy QPs, and release resources.

---

## 6. Stream Abstraction Layer

The stream layer presents RDMA as a reliable, ordered byte stream to the HTTP implementation.

### 6.1 Sending

When the HTTP layer writes bytes via `AsyncWrite`:

1. Bytes are appended to a send staging buffer within a registered memory region.
2. When `flush()` is called or the staging buffer reaches `max_message_size`, the layer posts an RDMA send WR containing the buffered data.
3. The `poll_write()` future completes when the data has been copied to the staging buffer (not when the RDMA send completes). Send completion is tracked asynchronously to reclaim the staging buffer.

A pool of send staging buffers allows multiple sends to be in flight simultaneously.

### 6.2 Receiving

1. The layer maintains a pool of pre-posted receive buffers on the QP.
2. When an RDMA recv completion arrives, the received data is appended to a reassembly buffer.
3. `AsyncRead::poll_read()` drains from the reassembly buffer.
4. Consumed receive buffers are re-posted to the QP to maintain receive credit.

### 6.3 Ordering Guarantees

RDMA RC queue pairs deliver messages in order. Combined with the single-producer staging and reassembly buffers, the stream provides TCP-equivalent ordering guarantees without additional sequence numbering.

### 6.4 HTTP Pipelining

HTTP/1.1 pipelining — sending multiple requests without waiting for each response — is expected to work well over HORD and is RECOMMENDED. The conditions that made pipelining unreliable over TCP/IP do not apply:

- **Head-of-line blocking** is mitigated by RDMA's low latency and the edge cache serving from registered memory, making response times consistently fast.
- **Broken intermediaries** are not a concern — HORD operates as a single hop between the edge cache and compute nodes with no middleboxes.
- **Error recovery ambiguity** is reduced by RDMA RC's reliable delivery — a connection either delivers all messages in order or fails cleanly.

Clients SHOULD pipeline requests when issuing multiple GETs (e.g., prefetching dataset shards). Servers MUST respond to pipelined requests in order per the HTTP/1.1 specification.

### 6.5 Message Framing

The stream layer does not impose its own framing. The byte stream is continuous; HTTP/1.1's own framing (Content-Length, chunked transfer encoding) delineates messages at the application layer.

One exception: each RDMA send message is prefixed with a 4-byte length header to allow the receiver to distinguish message boundaries within the completion:

```
HORD Message Envelope:
  length: u32    (payload bytes following this header)
  payload: [u8]  (HTTP byte stream data)
```

This is an internal transport detail not visible to the HTTP layer.

---

## 7. Zero-Copy Extension

For large response payloads, HORD defines an optional HTTP extension that bypasses the stream layer and places data directly into client-specified memory via RDMA write.

### 7.1 Negotiation

Zero-copy is available only when both peers indicated `ZERO_COPY_CAPABLE` in the handshake. It is requested per-HTTP-request via headers and is always optional — the server MAY ignore the zero-copy request and respond normally via the stream.

### 7.2 Request Headers

The client advertises a registered memory region for receiving the response body:

```http
GET /dataset/shard-00042.tar HTTP/1.1
Host: edge-cache.local
X-HORD-RDMA-Write: addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `addr` | hex u64 | Start address of the client's registered receive buffer |
| `rkey` | hex u32 | Remote key authorizing the server to write to this buffer |
| `len` | decimal u64 | Capacity of the receive buffer in bytes |

### 7.3 Server Behavior

If the server elects to use zero-copy:

1. Resolve the requested object and determine its size.
2. If the object fits in the client's buffer (`Content-Length <= len`): perform an RDMA write of the object data into the client's buffer starting at `addr`.
3. Wait for the RDMA write completion.
4. Send the HTTP response via the normal stream with the body omitted:

```http
HTTP/1.1 200 OK
Content-Length: 14680064
Content-Type: application/octet-stream
X-HORD-RDMA-Write: status=complete;bytes_written=14680064
```

The response body on the stream is empty. The `Content-Length` header reflects the logical size of the object, and `X-HORD-RDMA-Write: status=complete` signals that the data was delivered via RDMA write.

If the object exceeds the buffer (`Content-Length > len`), the server MUST NOT perform a partial RDMA write. Instead, it responds with:

```http
HTTP/1.1 413 Content Too Large
X-HORD-RDMA-Write: status=too_large;object_size=1073741824
```

The client may retry with a standard `Range` header or allocate a larger buffer.

### 7.4 Failure Cases

If the server cannot perform the RDMA write (invalid rkey, network error), it MUST fall back to a normal stream-based response. The client detects this by the absence of the `X-HORD-RDMA-Write` response header and reads the body from the stream as usual.

### 7.5 GPUDirect RDMA

When the client registers GPU device memory (via `nvidia_p2p_get_pages` or equivalent) and provides its address and rkey, the server's RDMA write targets GPU memory directly. This is transparent to the HORD protocol — the address and rkey are opaque to the server. The NIC and GPU handle the peer-to-peer DMA.

Requirements for GPUDirect RDMA:
- NVIDIA GPU with GPUDirect RDMA support
- Mellanox ConnectX-5 or later (or equivalent RDMA NIC with peer memory support)
- `nvidia-peermem` kernel module loaded
- GPU BAR1 size sufficient for the registered region

### 7.6 Interaction with HTTP Range Requests

Zero-copy and range requests compose naturally. A client may issue:

```http
GET /dataset/shard-00042.tar HTTP/1.1
Range: bytes=0-16777215
X-HORD-RDMA-Write: addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216
```

The server performs the RDMA write of the specified range into the client's buffer.

---

## 8. Buffer Management

Efficient buffer management is critical to HORD performance. Memory registration (`ibv_reg_mr`) is expensive and should be amortized.

### 8.1 Buffer Pool Architecture

Both client and server maintain pools of pre-registered memory regions:

```
Buffer Pool
├── Send Pool: N buffers × max_message_size bytes
│   Used for staging outgoing stream data
├── Recv Pool: M buffers × max_message_size bytes
│   Pre-posted to the QP for incoming messages
└── Cache Pool (server only): Large region for cached objects
    Served directly via RDMA write (zero-copy path)
```

### 8.2 Pool Sizing

| Parameter | Recommended Default | Notes |
|-----------|-------------------|-------|
| `max_message_size` | 64 KiB | Balances per-message overhead against memory usage. Larger values reduce send/recv count for big transfers on the stream path. |
| Send pool size | 16 buffers | Allows 16 in-flight sends per connection |
| Recv pool size | 32 buffers | Must be >= `max_recv_buffers` from handshake |
| Cache pool | Implementation-defined | Depends on available memory and workload |

### 8.3 Memory Registration Strategy

**Pre-registration (recommended):** Allocate and register all buffer pools at startup. No registration on the data path.

**On-Demand Paging (optional):** If the NIC supports ODP (implicit or explicit), memory regions can be registered lazily. This is particularly useful for the cache pool where object sizes vary. ODP trades slightly higher per-access latency (on first touch) for simpler memory management.

### 8.4 Large Object Handling

Objects that exceed `max_message_size` are handled differently on the two paths:

- **Stream path:** The stream layer segments the object across multiple RDMA sends automatically. The HTTP layer sees a continuous byte stream.
- **Zero-copy path:** The server performs a single large RDMA write (or multiple writes if the NIC's max WR size is a constraint). The RDMA layer handles segmentation into MTU-sized packets transparently.

---

## 9. Flow Control

RDMA RC transport provides reliable delivery but no built-in application-level flow control analogous to TCP's receive window. HORD implements credit-based flow control at the stream layer.

### 9.1 Receive Credits

Each side has a finite number of pre-posted receive buffers. A send will fail if the remote side has no posted receive. HORD tracks credits explicitly:

- At connection setup, each side has `max_recv_buffers` credits (from handshake).
- Each send consumes one credit.
- Credits are replenished when the receiver re-posts consumed buffers. Replenishment is communicated via a credit field in the message envelope.

### 9.2 Credit Replenishment

The message envelope is extended with a credit field:

```
HORD Message Envelope:
  length:  u32
  credits: u16   (number of receive buffers re-posted since last message)
  flags:   u16
  payload: [u8]
```

When the receiver re-posts receive buffers, it piggybacks the count on the next outgoing message. If no outgoing message is pending, a zero-length credit-only message is sent.

### 9.3 Backpressure

When send credits reach zero, the stream layer's `AsyncWrite` blocks (returns `Poll::Pending`) until credits are replenished. This propagates backpressure through the HTTP layer naturally — a slow consumer stalls the producer without dropping data or requiring retransmission.

---

## 10. Error Handling

### 10.1 Transport Errors

RDMA transport errors (QP errors, protection errors, remote access violations) are fatal to the connection. On any CQ error completion:

1. The stream layer returns an error from the next `AsyncRead` or `AsyncWrite` call.
2. The HTTP layer observes a connection error and handles it per its own semantics (e.g., retry the request on a new connection).
3. The RDMA transport layer destroys the QP and releases associated resources.

### 10.2 Application Errors

HTTP-level errors (4xx, 5xx) are handled entirely at the HTTP layer and are not visible to the HORD transport.

### 10.3 Zero-Copy Errors

If an RDMA write fails during a zero-copy transfer:

- If the write has not started: fall back to stream-based response.
- If the write has partially completed: the server MUST NOT send a success response. Instead, it closes the connection. The client observes a connection error and retries.

This is a deliberate simplification. Partial RDMA writes leave the client's buffer in an undefined state, and there is no HTTP-native mechanism to signal "the body was delivered via a side channel but is corrupt." A clean retry is safer.

---

## 11. Security Considerations

### 11.1 Transport Security

RDMA does not natively support encryption. Data on the wire is unencrypted. HORD inherits this limitation.

In most target environments (InfiniBand fabrics within a data center), this is acceptable — the network is physically isolated and trusted. For RoCE deployments on shared Ethernet, this may be a concern.

Possible mitigations:
- **Network isolation:** Deploy HORD only on dedicated RDMA VLANs or InfiniBand partitions.
- **IPsec:** RoCEv2 traffic can be encrypted at the IP layer via IPsec, though this may negate some performance benefits.
- **Application-layer encryption:** Encrypt objects before storage and serve encrypted bytes via HORD. Decryption happens at the client after receipt.

### 11.2 Memory Safety

The zero-copy extension requires the client to share memory addresses and remote keys with the server. A malicious or buggy server could write to arbitrary client memory.

Mitigations:
- Clients SHOULD register dedicated, bounded memory regions for HORD receive buffers. These regions should not overlap with other application memory.
- Clients SHOULD revoke rkeys (`ibv_dereg_mr`) promptly when a connection closes.
- Implementations MUST validate that RDMA write operations stay within the bounds communicated by the client.

### 11.3 Denial of Service

A malicious client could exhaust server resources by opening many connections (each consuming QPs, CQs, and registered memory) or by not posting receives (stalling the server's sends).

Implementations SHOULD enforce:
- Maximum connections per client IP/GID.
- Timeouts on idle connections.
- Limits on total registered memory.

---

## 12. Wire Format Reference

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

**Envelope flags:**

| Bit | Name | Description |
|-----|------|-------------|
| 0 | `CREDIT_ONLY` | Payload is empty; message exists only to replenish credits |
| 1-15 | Reserved | Must be zero |

### 12.3 X-HORD-RDMA-Write Request Header

```
X-HORD-RDMA-Write: addr=<hex u64>;rkey=<hex u32>;len=<decimal u64>
```

### 12.4 X-HORD-RDMA-Write Response Header

```
X-HORD-RDMA-Write: status=complete;bytes_written=<decimal u64>
X-HORD-RDMA-Write: status=too_large;object_size=<decimal u64>
X-HORD-RDMA-Write: status=declined
```

`status=declined` indicates the server chose not to use zero-copy. The response body is delivered via the stream as usual.

---

## 13. Implementation Guidance

### 13.1 Reference Implementation

The reference implementation is a Rust workspace:

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
use hord_server::{HordListener, HordConfig};

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
use hord_client::{HordConnector, HordClientConfig};
use hyper_util::client::legacy::Client;

let connector = HordConnector::new(HordClientConfig {
    max_message_size: 65536,
    recv_pool_size: 32,
    send_pool_size: 16,
    zero_copy: true,
    ..Default::default()
});

let client = Client::builder(TokioExecutor::new())
    .build(connector);

let resp = client.get("hord://edge-cache:4791/dataset/shard-042.tar".parse()?)
    .await?;
```

**Zero-copy client (explicit buffer):**

```rust
use hord_zerocopy::RdmaBuffer;

// Register a receive buffer (could be GPU memory)
let buf = RdmaBuffer::alloc(16 * 1024 * 1024, &connector)?;

let resp = client.request(
    Request::builder()
        .uri("hord://edge-cache:4791/dataset/shard-042.tar")
        .header("X-HORD-RDMA-Write", buf.header_value())
        .body(Empty::<Bytes>::new())?
).await?;

if resp.headers().get("X-HORD-RDMA-Write")
    .map_or(false, |v| v.to_str().unwrap().contains("complete"))
{
    // Data is in buf, ready for GPU consumption
}
```

### 13.3 Python API

```python
import pyhord

# Simple HTTP client over RDMA
client = pyhord.Client("10.0.0.1:4791")
response = client.get("/dataset/shard-042.tar")
data = response.content

# Zero-copy into GPU memory
import torch
buf = pyhord.GpuBuffer(torch.empty(16 * 1024 * 1024, dtype=torch.uint8, device='cuda:0'))
response = client.get("/dataset/shard-042.tar", rdma_buffer=buf)
# Data is now in the torch tensor, delivered NIC → GPU

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

### 13.4 URI Scheme

HORD uses the `hord://` URI scheme to indicate that the connection should use RDMA transport. Implementations SHOULD also support transparent upgrade from `http://` when both client and server are HORD-capable, via a mechanism TBD (e.g., DNS SRV records, Alt-Svc headers on an initial TCP connection, or out-of-band configuration).

### 13.5 Port

HORD defaults to port **4791**. This is one above the standard RDMA CM port (4791 is unassigned by IANA as of this writing). Implementations MUST allow configuration of an alternative port.

*Note: Port number is provisional and subject to change before 1.0.*

### 13.6 Testing

Implementations SHOULD support a "loopback" mode using software RDMA (`rxe` or `siw` kernel modules) for development and CI environments without RDMA hardware. The `rdma_rxe` module provides a software RoCE implementation over any Ethernet interface.

---

## 14. Relationship to Existing Standards

### 14.1 iSER, SRP, NFS/RDMA

These are existing protocols that run over RDMA, but they serve different application layers (SCSI, NFS). HORD is the HTTP analog.

### 14.2 SMB Direct

SMB Direct (SMB over RDMA) is the closest precedent to HORD. It provides a byte-stream abstraction over RDMA for the SMB protocol, with optional direct data placement for large transfers. HORD follows a similar architectural pattern adapted for HTTP.

### 14.3 HTTPv3 / QUIC

HTTP/3 runs over QUIC (UDP-based). HORD is complementary, not competing — HTTP/3 targets internet-scale deployments, while HORD targets data center fabrics. An edge cache could speak HTTP/3 to external clients and HORD to local compute nodes.

### 14.4 UCX

UCX (Unified Communication X) provides a transport abstraction that includes RDMA. HORD could potentially use UCX as its transport layer instead of raw `libibverbs`. This is a valid implementation strategy but not required by this specification.

---

## Appendix A: Configuration Defaults

| Parameter | Default | Range | Description |
|-----------|---------|-------|-------------|
| `port` | 4791 | 1-65535 | Listen/connect port |
| `max_message_size` | 65,536 | 4,096 — 1,048,576 | Max bytes per RDMA send |
| `recv_pool_size` | 32 | 4 — 256 | Pre-posted receive buffers per connection |
| `send_pool_size` | 16 | 4 — 256 | Send staging buffers per connection |
| `max_connections` | 1024 | 1 — 65535 | Server-side connection limit |
| `idle_timeout` | 60s | 1s — 3600s | Idle connection timeout |
| `zero_copy` | true | bool | Enable zero-copy extension |
| `gdr` | false | bool | Enable GPUDirect RDMA buffer registration |

## Appendix B: Performance Expectations

These are rough targets for the reference implementation, not normative requirements:

| Metric | TCP Baseline | HORD Stream | HORD Zero-Copy |
|--------|-------------|-------------|----------------|
| Latency (small GET) | ~50 μs | ~5 μs | N/A |
| Throughput (large GET) | ~10 GB/s | ~20 GB/s | ~24 GB/s (line rate, HDR IB) |
| CPU usage (per GB) | High | Low | Minimal |
| GPU load latency | ~100 μs | ~30 μs | ~10 μs (NIC → GPU) |

*Values are illustrative. Actual performance depends on hardware, MTU, queue depth, and workload.*

---

## License

This specification is released under the Apache License 2.0.

## Authors

Per Buer, Varnish Software

## Changelog

- **v0.1.0** — Initial draft. Stream abstraction, zero-copy extension, buffer management, flow control.
