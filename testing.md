# HORD — hardware test plan

This is the plan for taking HORD off Soft-RoCE and onto real RDMA hardware, with
the specific goal of exercising **§7.5 GPUDirect RDMA** — the one spec feature
the `rxe` dev box ([CLAUDE.md](CLAUDE.md)) cannot reach.

## TL;DR — how many hosts?

| Goal                                   | Hosts | Transport                                  |
| -------------------------------------- | ----- | ------------------------------------------ |
| Validate HORD logic (all but §7.5)     | 1     | Soft-RoCE **or** real-RNIC loopback        |
| Representative RDMA throughput numbers | 2     | Real RNICs                                 |
| GPUDirect **smoke test** (§7.5)        | 1     | Real RNIC + GPU, NIC self-loopback         |
| GPUDirect **real numbers** (§7.5)      | 2     | Real RNIC + GPU on each host               |

**One host is enough to bring up and functionally validate GPUDirect**, provided
it has a real RDMA NIC and a GPU wired under the same PCIe switch/root complex.
Two hosts only become necessary when you want trustworthy cross-host performance
numbers — not for correctness.

### Why loopback works (and where it doesn't)

- **Plain RDMA loopback** works on one host two ways: Soft-RoCE loops internally
  (what we do today), and real ConnectX-class NICs support *self-loopback* — two
  QPs on the same port over the local RoCEv2 IP. The data still goes down into the
  NIC and back, so MRs, CQs and the verbs path are all real. Good for correctness.
  The caveat: loopback short-circuits parts of the packet path, so it won't expose
  cross-host GID/MTU/routing or real congestion — eventually worth two hosts.
- **GPUDirect cannot run on Soft-RoCE at all.** `rdma_rxe` is a CPU software
  transport; it bounces data through host memory and cannot peer-to-peer DMA into
  a GPU's PCIe BAR. So the moment §7.5 is in play, the `rxe0` environment is out —
  a real RNIC is required regardless of host count.
- **GPUDirect loopback** on one host: register a GPU buffer as the RDMA-write
  destination (and a host or GPU buffer as the source); the NIC P2P-DMAs out of
  the source and into the GPU BAR over its loopback path. This genuinely exercises
  the GDR path and HORD's code — it's the right first target.

## Hardware prerequisites (for §7.5)

- A GPUDirect-RDMA-capable RNIC — NVIDIA/Mellanox ConnectX-5 or newer (the spec
  says ConnectX-5+; ConnectX-3 Pro also has GDR but is EOL).
- An NVIDIA GPU with GPUDirect RDMA support (datacenter parts — Tesla/A/H/L
  series; consumer GeForce is generally *not* supported for GDR).
- The `nvidia-peermem` kernel module loaded (modern path), **or** the dma-buf
  registration path (`ibv_reg_dmabuf_mr`, kernel ≥ 5.12 + recent MLX5 driver),
  which does not need `nvidia-peermem`.
- GPU and NIC on the **same PCIe root complex / switch** — ideally under one
  switch. Crossing the inter-socket link (UPI/QPI) usually disables P2P or makes
  it pathologically slow.
- Sufficient GPU **BAR1** size to map the registered region (large-BAR / ReBAR
  enabled in firmware).

## Phase -1 — pick the hardware (cloud)

No NIC on this dev box, so the realistic path is renting. HORD's transport
choices narrow the field sharply: it needs **standard ibverbs RC queue pairs +
RDMA-CM + `RDMA_WRITE`/`WRITE_WITH_IMM` on a real ConnectX (mlx5)**, InfiniBand or
RoCEv2, with GPUDirect RDMA. That one requirement disqualifies two of the biggest
GPU-cloud offerings:

- **AWS EFA is out.** Elastic Fabric Adapter's ibverbs device offers only **UD and
  SRD** queue pairs — **no RC**, and no RDMA-CM. NCCL gets GDR via EFA's
  libfabric/SRD path, but HORD's RC + `RDMA_WRITE` model can't run on it without a
  transport rewrite. All P4/P5/P6 instances are non-starters as the code stands.
- **GCP GPUDirect-TCPX is out.** The A3 High/Mega instances move GPU data over
  **TCP** (TCPX/TCPXO), not RDMA — no verbs for HORD to use.

What does fit:

| Provider | Instance family | Fabric | Notes |
| --- | --- | --- | --- |
| **Azure** | ND A100 v4, ND H100 v5, ND H200 v5, ND GB200 v6 | NVIDIA Quantum **InfiniBand** (ConnectX-6/7) | Full RC verbs + RDMA-CM + GDR exposed to the VM. Easiest mainstream match. |
| **Oracle OCI** | BM.GPU.A100 / H100 / B200 (**bare metal**) | **RoCEv2** cluster network on ConnectX | Bare metal → root, kernel modules, `nvidia-peermem`, no virtualization caveats. Closest to our current RoCE testing. |
| **GCP** | A3 **Ultra** (H200), A4 (B200) *only* | **RoCE** w/ ConnectX-7 ("Titanium") | The newer RoCE SKUs do real RDMA, unlike the TCPX ones. Verify tenant verbs access. |
| **CoreWeave / Lambda / Crusoe / Nebius** | H100/H200/B200 clusters | NDR **InfiniBand** (ConnectX-7) | Neoclouds, often near-bare-metal; IB with full verbs + GDR. |
| **AWS** | — | EFA | Only if HORD is ported to libfabric/SRD. Not recommended. |

Practical notes:

- **Prefer bare metal** (OCI, or a neocloud bare-metal SKU). §7.5 needs to load
  `nvidia-peermem` (or the dma-buf path) and possibly tweak ACS/IOMMU — all of
  which want root and kernel-module control a locked-down managed VM may deny.
- **One node covers Phase 0–2.** The single-host loopback smoke test needs exactly
  one RDMA+GPU node (RC self-loopback on its local GID). The catch: these fabrics
  are almost always sold as **8-GPU nodes**, so even the one-host test is a
  big-node rental — there's rarely a cheap "1× GPU + 1× ConnectX" SKU, because the
  RNIC *is* the cluster fabric.
- **Two nodes for Phase 3**, in the same placement group / cluster network so the
  fabric links them (Azure proximity placement groups, OCI cluster networks, a
  neocloud "1-click cluster").
- **De-risk for ~an hour's spend first.** Rent the smallest RDMA-capable node and
  run the Phase 0 preflight before writing any code — if `ib_write_bw --use_cuda`
  passes, the hardware is good and any later failure is in HORD.
- **A plain GPU VM + Soft-RoCE buys nothing** — `rxe` still can't do GDR, so you'd
  learn nothing beyond this box.

Starting points: **Azure ND-series** (mainstream, InfiniBand) or **OCI bare-metal
GPU** (most control, RoCEv2); a neocloud (CoreWeave/Lambda) if you want IB with
near-bare-metal access.

## Phase 0 — hardware preflight (no HORD code)

Confirm the box can do GPUDirect *before* touching HORD. If any of these fail,
HORD cannot work either and the fix is environmental.

```sh
# 1. NIC ↔ GPU PCIe topology. Want PIX or PXB between the GPU and the HCA;
#    SYS (crosses the CPU interconnect) is the usual "GDR doesn't work" cause.
nvidia-smi topo -m

# 2. RDMA device is real hardware, port active, RoCEv2/IB.
ibv_devinfo                          # note the device name, e.g. mlx5_0
rdma link show

# 3. peer-memory provider present (one of these).
lsmod | grep -E 'nvidia_peermem|nv_peer_mem'      # nvidia-peermem path
#   or confirm dma-buf MR support in the driver/kernel for the ibv_reg_dmabuf path.

# 4. End-to-end GDR baseline with perftest built against CUDA. This is the
#    canonical "does GPUDirect work on this hardware at all" check — single host,
#    NIC self-loopback, NIC writing into GPU memory.
ib_write_bw -d mlx5_0 --use_cuda=0 &           # server, GPU 0 as the buffer
ib_write_bw -d mlx5_0 --use_cuda=0 <local-roce-ip>   # client
#   Expect line-rate-ish BW. If this is slow or fails, stop and fix the
#   environment (ACS/IOMMU, BAR1, topology) before going further.
```

A passing Phase 0 means the hardware path is good and any later failure is in
HORD's own code.

## Phase 1 — the §7.5 implementation seam

GPUDirect is **transparent to the server**: the server only ever sees `addr`,
`rkey`, `len` in `X-HORD-RDMA-Write` (spec §12.3) and issues a one-sided write
against them — it neither knows nor cares that `addr` is a GPU virtual address.
So **all the new code is on the client/buffer-registration side.** The wire
format, the server, and the split/range paths are unchanged.

What exists today (`hord-core`):

- `Connection::register_buffer(len, access)` allocates **host** memory
  (`vec![0u8; len]` behind a `Box<[UnsafeCell<u8>]>`) and registers it with
  `ibv_reg_mr`, returning a `RegisteredBuffer`.
- `RegisteredBuffer` exposes exactly what the zero-copy path consumes:
  `as_mut_ptr()` (→ `addr`), `rkey()`, `len()`, and `copy_in`/`copy_out`.
- `ZeroCopyRequest::from_buffer(buf)` / `HordStream::register_remote_writable`
  build the request descriptor from that buffer.

The minimal change to land §7.5:

1. **Add a device-memory registration entry point**, e.g.
   `Connection::register_device_buffer(dev_ptr, len, access)` that registers an
   externally-allocated CUDA device pointer (`cudaMalloc`) via `ibv_reg_mr`
   (peer-mem path) or `ibv_reg_dmabuf_mr` (dma-buf path) — instead of allocating
   host storage. The destination must carry `IBV_ACCESS_REMOTE_WRITE`, which per
   IBA is only valid together with `IBV_ACCESS_LOCAL_WRITE` (see PROTOTYPE.md's
   spec findings) — register with **both**, exactly as the host path does.
2. **Generalise `RegisteredBuffer`'s storage** to be host *or* device backed
   (an enum: owned `Box<[UnsafeCell<u8>]>` vs. a `{dev_ptr, len, free-fn}` that
   `cudaFree`s on drop). `as_mut_ptr()`/`rkey()`/`len()` stay identical; only
   `copy_in`/`copy_out` change for device storage (they become `cudaMemcpy`
   H2D/D2H). Once it exposes the same three accessors, `ZeroCopyRequest::from_buffer`
   and the whole §7.3/§7.6/§7.7 client path work **unmodified**.
3. **Gate it behind a Cargo feature** (e.g. `gpudirect`) so the default build
   stays host-only and free of the CUDA link dependency. The lightest dependency
   is raw FFI to three `libcudart` symbols — `cudaMalloc`, `cudaFree`,
   `cudaMemcpy` — keeping the crate's no-third-party-crate property under the
   default feature set.

> Lower-effort alternative for a pure smoke test: skip the typed buffer entirely
> and post a raw `(addr, rkey, len)` for a `cudaMalloc`'d region through the
> low-level `hord-core` API, hand-driving one RDMA write. Proves the hardware +
> opaque-address claim without the client-API generalisation — but doesn't
> integrate with the demo client. Prefer step 1–3 for anything beyond a one-off.

## Phase 2 — single-host GPUDirect loopback test

Goal: prove a HORD server RDMA-writes a response body into **GPU memory** and the
client reads it back correctly. One host, one NIC (self-loopback), one GPU.

Harness (mirrors the existing `hord-zerocopy/tests/zerocopy_loopback.rs`, but the
client's destination is a device buffer):

1. Server: unchanged. Serves `GET /size/<n>` with the verifiable byte pattern,
   honouring `X-HORD-RDMA-Write` (its source buffer may stay host memory — the
   server side needs no GPU).
2. Client: allocate an `n`-byte CUDA buffer, register it
   (`register_device_buffer`, `REMOTE_WRITE|LOCAL_WRITE`), wrap it in a
   `ZeroCopyRequest`, and `GET /size/<n>` with the resulting `X-HORD-RDMA-Write`.
3. Server RDMA-writes the body straight into GPU memory; HTTP response is
   `Content-Length: 0` + `status=complete;bytes_written=<n>` (spec §7.3).
4. Verify: `cudaMemcpy` D2H into a host buffer and check the pattern
   (`pattern()` / base-offset verify already used by the demos). Integrity pass =
   the NIC placed the bytes in GPU memory correctly.

Then layer on the existing matrix, all of which should compose unchanged because
the write is offset-agnostic and the address is opaque:

- **Sizes**: 1 MiB → 1 GiB (watch BAR1 limits), flat-throughput sanity.
- **Range (§7.6)**: `--range` into a device buffer sized to the range; verify at
  the absolute offset; 416 on unsatisfiable.
- **Split (§7.7)**: `;id=<n>` → data-plane completion carries the transfer ID;
  the body is in GPU memory, no HTTP parse on the data plane.
- **too_large (413)** and **declined** (stream fallback) outcomes still hold.

Wire this as `#[ignore]`d tests behind the `gpudirect` feature, the same way the
RDMA loopback tests are gated on the `rxe` device today, so a GPU-less CI run
skips them and a GPU box runs them with `--include-ignored`.

## Phase 3 — two-host run (real numbers)

Only needed once Phase 2 is green and you want performance you can quote:

- Two hosts, each NIC + GPU; replace the blanket RoCE firewall deny on the
  fabric NIC with an `allow … from <peer>` rule (see CLAUDE.md's firewall note).
- Same matrix as Phase 2, GPU-to-GPU across the wire. This is the only
  configuration that measures true cross-host GDR throughput/latency and exercises
  real GID resolution, MTU, and the full packet path that loopback hides.
- A `ib_write_bw --use_cuda` two-host baseline gives the hardware ceiling to
  compare HORD against.

## Gotchas

- **PCIe ACS / IOMMU** silently breaks P2P even with correct topology. If Phase 0
  `ib_write_bw --use_cuda` is slow/fails despite PIX/PXB, disable ACS on the
  switch ports between GPU and NIC (or set the IOMMU to passthrough). This is the
  single most common false failure.
- **BAR1 size**: the registered GPU region must fit in mappable BAR1. Large
  transfers need large-BAR / Resizable BAR enabled in firmware; otherwise cap the
  per-write size and split.
- **GeForce vs datacenter GPUs**: GDR is generally unsupported / unvalidated on
  consumer cards. Use a datacenter part if results look wrong.
- **peer-mem vs dma-buf**: if `nvidia-peermem` is awkward to load, the
  `ibv_reg_dmabuf_mr` path needs no kernel module — pick whichever the driver
  stack supports and key the `register_device_buffer` implementation off it.

## Pass / fail criteria

- **Phase 0**: `ib_write_bw --use_cuda` completes at a sane bandwidth. (Gate.)
- **Phase 1/2**: the GPUDirect loopback test delivers the body into GPU memory
  with a clean integrity check across the size matrix, and the range/split/413/
  declined variants behave exactly as the host-memory zero-copy tests do.
- **Phase 3**: cross-host GPU-to-GPU throughput within a reasonable fraction of
  the `ib_write_bw --use_cuda` hardware baseline.

Reaching Phase 2-green closes the last open spec feature (TODO.md §7.5) and
validates the prototype's standing claim that the zero-copy path is
address-agnostic — host and GPU destinations differ only in how the client's
buffer is allocated and registered.
