# HORD — HTTP Over RDMA

HORD transports HTTP/1.1 over RDMA, giving ideally unmodified HTTP
semantics a byte-stream over RDMA's message-oriented queue pairs, with an optional
zero-copy path that places response bodies straight into client memory.

The target is AI/compute clusters consuming object storage over an RDMA fabric:
an edge cache speaks plain HTTP upstream and HORD on the last hop to compute nodes.

This repository is the reference implementation — a Rust workspace plus the
specification it implements.

## Status

Working prototype, demonstrated end-to-end over Soft-RoCE (`rxe`). Implemented:

- Stream path — RC queue pairs, the message envelope, credit-based flow
  control, and a byte-stream (`Read`/`Write` + async `AsyncRead`/`AsyncWrite`)
  that carries unmodified HTTP/1.1 (incl. `hyper`). Spec §4–§6, §8, §9.
- Zero-copy — one-sided `RDMA_WRITE` into a client-registered buffer,
  advertised via `X-HORD-RDMA-Write`. Spec §7.1–§7.4.
- Protocol splitting — `RDMA_WRITE_WITH_IMM` delivers payload + a transfer-ID
  immediate to the client's CQ, so a data-plane consumer collects bodies off the
  completion queue without parsing HTTP. Spec §7.7.
- Range requests — single-range `Range`/`Content-Range`, composed with the
  zero-copy write. Spec §7.6.

Not yet built: §7.5 GPUDirect RDMA — the `addr`/`rkey` path is opaque to the
server, so it should work unchanged on capable hardware.

## Layout

| Crate            | Responsibility                                                            |
| ---------------- | ------------------------------------------------------------------------- |
| `hord-core`      | RDMA transport: device/PD/QP lifecycle, MR registration, CQ processing. Wraps `libibverbs`/`librdmacm` via the modified `sideway` crate. |
| `hord-stream`    | HORD wire protocol: handshake, envelope, credit flow control, the `HordStream` byte stream, and the zero-copy / split-mode write drivers. |
| `hord-zerocopy`  | Zero-copy HTTP semantics (§7). Default: the pure `X-HORD-RDMA-Write` header codec (`RdmaWriteReq`/`RdmaWriteStatus`/`RdmaWriteAction`) — no dependencies, links with no NIC or RDMA libraries. `rdma` feature: adds the client/server write orchestration, the source-buffer pool, and the `SplitReceiver` data plane (depends on `hord-stream`). |
| `hord-async`     | tokio `AsyncRead`/`AsyncWrite` over a `HordStream`, driving the CQ event fd with `AsyncFd` (no busy-poll); reactor split for multi-task duplex + data plane. |
| `hord-demo`      | `hord-server`/`hord-client` (sync) and `hord-server-async`/`hord-client-async` (`hyper`). |

## Build

Needs the RDMA userspace dev packages, then a normal release build:

```sh
sudo apt-get install -y libibverbs-dev librdmacm-dev
cargo build --release
```

## Run (Soft-RoCE loopback)

Both endpoints run against the local `rxe` device and connect over its RoCEv2 IP.
The default is `192.0.2.1` (a reserved RFC 5737 documentation address, so no real
host IP is baked into the tree) on port 4791 — so out of the box you must either
assign that address to the rxe-backing NIC (`sudo ip addr add 192.0.2.1/32 dev
<netdev>`) or point the binaries at your device IP with `$HORD_TEST_IP` (or
`--bind` on the server / `--server` on the client). `127.0.0.1` will **not** work:
it routes via `lo`, which has no RDMA device. See [CLAUDE.md](CLAUDE.md) for the
device setup.

```sh
export HORD_TEST_IP=<rxe device IP>   # or assign 192.0.2.1 to the rxe-backing NIC

# Terminal 1
./target/release/hord-server                                # listens on $HORD_TEST_IP:4791

# Terminal 2
./target/release/hord-client --path /size/67108864             # 64 MiB, integrity-checked
./target/release/hord-client --path /size/67108864 --zero-copy # via one-sided RDMA write
```

The `*-async` binaries behave identically over `hyper`; `--split` (async) exercises §7.7.

## Test

```sh
# Full suite incl. the RDMA loopback tests (need the rxe device up). Same address
# rule as above: $HORD_TEST_IP, or assign the 192.0.2.1 default to the NIC. The
# ignored data-path tests also need a raised RLIMIT_MEMLOCK — they register 4–16
# MiB buffers, and the usual 8 MiB default fails them; see the "Locked memory"
# caveat in CLAUDE.md.
HORD_TEST_IP=<rxe device IP> cargo test --workspace -- --include-ignored --test-threads=1

# Logic tests only: no device to run, but still
# builds the transport, so it needs the RDMA dev packages installed:
cargo test --workspace

# Pure header codec only: needs neither a NIC nor rdma-core, the hord-zerocopy
# `rdma` feature is off — how an embedder unit-tests X-HORD-RDMA-Write on a laptop.
cargo test -p hord-zerocopy
```

## Documentation

| Document                        | Contents                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| [SPEC.md](SPEC.md)              | The HORD specification (v0.1.0 draft).                         |
| [PROTOTYPE.md](PROTOTYPE.md)    | The long form of this README: feature-by-feature detail, design notes, prototype limitations, the deferred code-review items, and findings to fold back into the spec. |
| [testing.md](testing.md)        | Hardware / GPUDirect (§7.5) test plan.                         |
| [TODO.md](TODO.md)              | Remaining work and deferred review items.                      |
| [CLAUDE.md](CLAUDE.md)          | The Soft-RoCE dev environment: device bring-up, the `RLIMIT_MEMLOCK` caveat, host-specific setup notes. |

## License

Apache-2.0. © Per Buer, Varnish Software.
