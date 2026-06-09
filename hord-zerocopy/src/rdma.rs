//! Write *orchestration* (spec §7.3 / §7.7) — the device-dependent layer.
//!
//! Compiled only under the crate's `rdma` feature and re-exported at the crate
//! root (`pub use rdma::*`), so callers keep using `hord_zerocopy::ZeroCopyRequest`
//! / `serve_rdma_write` / `SourcePool` / `SplitReceiver` directly. Everything here
//! drives the actual one-sided write and so depends on `hord-stream` ->
//! `hord-core` -> sideway / libibverbs / librdmacm. The pure §7.3/§7.7 policy
//! ([`RdmaWriteAction`]) deliberately stays in the codec layer (the crate root),
//! on the device-free side of the module wall.

use std::{cell::RefCell, io, rc::Rc};

use hord_stream::{HordStream, RegisteredBuffer};

use super::{RdmaWriteAction, RdmaWriteReq, RdmaWriteStatus};

// ---- client orchestration ----------------------------------------------------

/// A registered destination buffer for a zero-copy response, together with the
/// request header advertising it. Hold it across the request/response: once the
/// response head reports [`RdmaWriteStatus::Complete`], the payload is already in
/// this buffer (delivered out-of-band by the server's RDMA write — RC ordering
/// guarantees it has landed by the time the response head arrives). Read it with
/// [`copy_out`](Self::copy_out).
pub struct ZeroCopyRequest {
    buf: RegisteredBuffer,
    id: Option<u32>,
}

impl ZeroCopyRequest {
    /// Register a `capacity`-byte destination region the server may RDMA-write
    /// into. Gate on [`HordStream::zero_copy_negotiated`] before offering it.
    pub fn new(stream: &HordStream, capacity: usize) -> io::Result<Self> {
        Ok(Self::from_buffer(stream.register_remote_writable(capacity)?))
    }

    /// Wrap an already-registered remote-writable buffer. Use this when the
    /// buffer was registered elsewhere — e.g. via an async stream's
    /// `register_remote_writable` — so the async client gets the same request
    /// descriptor / verify path as the sync client without re-deriving it.
    pub fn from_buffer(buf: RegisteredBuffer) -> Self {
        ZeroCopyRequest { buf, id: None }
    }

    /// Request split mode (§7.7) for this transfer: the emitted header carries
    /// `id=<transfer_id>`, and a split-capable server delivers the body via RDMA
    /// write-with-immediate, signalling `transfer_id` on the data-plane CQ (see
    /// [`SplitReceiver`]). Gate on [`HordStream::split_mode_negotiated`] before
    /// using it — on a non-split connection the `id` is simply ignored by the
    /// server. Chainable on [`new`](Self::new) / [`from_buffer`](Self::from_buffer).
    pub fn with_id(mut self, transfer_id: u32) -> Self {
        self.id = Some(transfer_id);
        self
    }

    /// The split-mode transfer ID this request advertises, if any.
    pub fn id(&self) -> Option<u32> {
        self.id
    }

    /// The request descriptor (`addr`/`rkey`/`len`, plus `id` in split mode) for
    /// this buffer.
    pub fn request(&self) -> RdmaWriteReq {
        RdmaWriteReq {
            addr: self.buf.as_mut_ptr() as u64,
            rkey: self.buf.rkey(),
            len: self.buf.len() as u64,
            id: self.id,
        }
    }

    /// The `X-HORD-RDMA-Write: addr=..;rkey=..;len=..` line to add to the GET.
    pub fn header_line(&self) -> String {
        self.request().header_line()
    }

    /// Destination capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Copy `dst.len()` delivered bytes out of the buffer, starting at `off`.
    /// (Consuming/verifying the payload reads the application's own buffer — this
    /// is not a transport copy; a real consumer can use it in place instead.)
    pub fn copy_out(&self, off: usize, dst: &mut [u8]) {
        self.buf.copy_out(off, dst);
    }
}

// ---- server orchestration ----------------------------------------------------

/// Perform the server side of a zero-copy response (spec §7.3, and §7.7 in split
/// mode).
///
/// If `object_size` fits the client's advertised buffer (`req.len`), register a
/// source region, let `fill` populate it, RDMA-write it into the client's
/// `[addr, rkey]`, and return [`RdmaWriteStatus::Complete`]. If it does not fit,
/// return [`RdmaWriteStatus::TooLarge`] without writing. The caller turns the
/// returned status into the HTTP response header — always with `Content-Length:
/// 0` for `Complete` (the bytes travel out-of-band).
///
/// **Split mode (§7.7).** When `req.id` is present *and*
/// [`HordStream::split_mode_negotiated`] holds, the body is delivered with RDMA
/// write-with-immediate carrying `req.id`, so the client's data plane is
/// signalled on its CQ (the HTTP response is still returned, as §7.7.4 step 3).
/// Otherwise `req.id` is ignored and a plain write is used. Either way the
/// returned status is the same — split vs. plain is purely a delivery mechanism.
///
/// Gate on [`HordStream::zero_copy_negotiated`] (and your own policy) before
/// calling. On a transport failure mid-write the stream is closed and an `Err`
/// is returned; the caller MUST NOT report `complete` in that case (§7.4/§7.7.7).
///
/// The source region is registered per call and released once the write is
/// acknowledged. A production server would amortize registration with a pool
/// (spec §8.3) rather than register per response.
///
/// The §7.3/§7.7 decision — too-large, split vs. plain, the zero-length handling —
/// is [`RdmaWriteAction::decide`]; this function just executes the resulting plan
/// with the blocking write calls, so the async server can share the same policy.
pub fn serve_rdma_write(
    stream: &mut HordStream,
    req: &RdmaWriteReq,
    object_size: u64,
    fill: impl FnOnce(&RegisteredBuffer),
) -> io::Result<RdmaWriteStatus> {
    match RdmaWriteAction::decide(req, object_size, stream.split_mode_negotiated()) {
        // Object too large, or an empty body in plain mode: nothing to write —
        // return the status the caller puts in the response header.
        RdmaWriteAction::Respond(status) => Ok(status),
        // Deliver the body: register a source, fill it, and one-sided-write it —
        // with a write-with-immediate in split mode, else a plain write.
        RdmaWriteAction::Write {
            payload_len,
            source_len,
            transfer_id,
        } => {
            let src = stream.register_source(source_len)?;
            run_write_plan(stream, &src, req, payload_len, transfer_id, fill)?;
            // `src` drops after the write returns: rdma_write_all{,_with_imm}
            // blocked until the write completed and was acked, so no DMA
            // references the MR — deregistration is sound.
            Ok(RdmaWriteStatus::Complete {
                bytes_written: payload_len as u64,
            })
        }
    }
}

/// Like [`serve_rdma_write`], but draws the source region from a [`SourcePool`]
/// (spec §8.3) instead of registering a fresh MR per response — `ibv_reg_mr` is
/// expensive (§8.1), so a server reusing a connection (HTTP keep-alive, or a split
/// run that serves many transfers on one connection) amortizes it. The HTTP-facing
/// behaviour is identical to [`serve_rdma_write`]: same status, same `Content-Length:
/// 0`, same §7.3/§7.7 policy via [`RdmaWriteAction::decide`].
///
/// The pool lends a pre-registered buffer when the payload fits its slab and one is
/// available, growing lazily to its cap and otherwise falling back to a one-off
/// registration (an oversized object — §8.4 — or a momentarily exhausted pool), so
/// correctness never depends on the pool being large enough. The leased buffer is
/// returned to the pool for reuse only after the write is acknowledged.
pub fn serve_rdma_write_pooled(
    stream: &mut HordStream,
    pool: &SourcePool,
    req: &RdmaWriteReq,
    object_size: u64,
    fill: impl FnOnce(&RegisteredBuffer),
) -> io::Result<RdmaWriteStatus> {
    match RdmaWriteAction::decide(req, object_size, stream.split_mode_negotiated()) {
        RdmaWriteAction::Respond(status) => Ok(status),
        RdmaWriteAction::Write {
            payload_len,
            source_len,
            transfer_id,
        } => {
            // The lease holds its buffer (and only an `Rc` to the pool) until it
            // drops at the end of this block — after `run_write_plan` has blocked
            // for the write's completion, so no DMA references the buffer when it
            // returns to the pool. The fallback registrar borrows `stream` only for
            // the call; the subsequent `&mut stream` write is unaffected.
            let lease = pool.acquire(source_len, |n| stream.register_source(n))?;
            run_write_plan(stream, lease.buffer(), req, payload_len, transfer_id, fill)?;
            Ok(RdmaWriteStatus::Complete {
                bytes_written: payload_len as u64,
            })
        }
    }
}

/// Execute a decided [`RdmaWriteAction::Write`] against an already-acquired source
/// buffer: fill its first `payload_len` bytes, then one-sided-write them into the
/// client's `[addr, rkey]` — with a write-with-immediate carrying `transfer_id`
/// (split mode, §7.7) or a plain write. Blocks until the write is acknowledged.
/// Shared by [`serve_rdma_write`] and [`serve_rdma_write_pooled`] so the only thing
/// that differs between them is where the source comes from.
fn run_write_plan(
    stream: &mut HordStream,
    src: &RegisteredBuffer,
    req: &RdmaWriteReq,
    payload_len: usize,
    transfer_id: Option<u32>,
    fill: impl FnOnce(&RegisteredBuffer),
) -> io::Result<()> {
    if payload_len > 0 {
        fill(src);
    }
    match transfer_id {
        Some(id) => stream.rdma_write_all_with_imm(src, 0, req.addr, req.rkey, payload_len, id),
        None => stream.rdma_write_all(src, 0, req.addr, req.rkey, payload_len),
    }
}

// ---- server source buffer pool (spec §8.1 / §8.3) ----------------------------

/// A pool of registered source buffers for zero-copy responses, so a server
/// amortizes memory registration (`ibv_reg_mr` is expensive — spec §8.1) across
/// responses on a connection instead of registering one MR per response (§8.3).
///
/// MRs are scoped to a protection domain, so a pool belongs to **one connection**
/// (one [`HordStream`] / async stream); pass that stream's `register_source` to
/// [`acquire`](Self::acquire). The pool grows **lazily**: it pre-registers nothing,
/// registers a slab-sized buffer on first need (up to `capacity`), and reuses it
/// thereafter — so a single-response connection costs exactly one registration
/// (no worse than registering per response) while a reused connection pays only
/// `capacity` registrations no matter how many responses it serves.
///
/// [`acquire`](Self::acquire) lends a buffer when the payload fits the slab
/// (`buf_size`); a larger object (§8.4) or a moment the pool is at capacity and
/// fully lent falls back to a one-off registration, so correctness never depends
/// on the pool size — only efficiency. The returned [`SourceLease`] hands a pooled
/// buffer back on drop.
///
/// `Clone` is an `Rc` bump: share one pool across a connection's request handlers
/// (e.g. into a `hyper` `service_fn`). The lease owns its buffer and holds only an
/// `Rc`, so it is safe to keep across an `.await` — no pool borrow is held. `!Send`
/// (its buffers hold raw pointers), like everything on the zero-copy path.
#[derive(Clone)]
pub struct SourcePool(Rc<RefCell<PoolInner>>);

struct PoolInner {
    buf_size: usize,           // slab size; a request larger than this falls back
    capacity: usize,           // max pooled buffers (bounds pinned memory)
    free: Vec<RegisteredBuffer>,
    registered: usize,         // pooled buffers in existence (free + lent out)
    fallbacks: u64,            // one-off registrations (oversized / pool exhausted)
}

impl SourcePool {
    /// A pool of up to `capacity` reusable source buffers, each `buf_size` bytes.
    /// Registers nothing up front (lazy growth — see the type docs); cheap and
    /// infallible. `buf_size` should be the common response size (larger objects
    /// fall back, §8.4) and `capacity` the responses a connection may have in
    /// flight at once (e.g. the split transfer window) so the steady state never
    /// falls back.
    pub fn new(capacity: usize, buf_size: usize) -> SourcePool {
        SourcePool(Rc::new(RefCell::new(PoolInner {
            buf_size: buf_size.max(1), // a zero-length MR is not portable
            capacity,
            free: Vec::new(),
            registered: 0,
            fallbacks: 0,
        })))
    }

    /// The per-buffer slab size; a request larger than this falls back to a one-off.
    pub fn buf_size(&self) -> usize {
        self.0.borrow().buf_size
    }

    /// Pooled buffers currently free (lendable without registering or falling back).
    pub fn available(&self) -> usize {
        self.0.borrow().free.len()
    }

    /// Pooled buffers registered so far (grows lazily up to `capacity`).
    pub fn registered(&self) -> usize {
        self.0.borrow().registered
    }

    /// One-off (fallback) registrations made: an oversized object, or a moment the
    /// pool was at capacity and fully lent out. `0` means every response reused (or
    /// grew) a pooled buffer; a rising count means the slab or capacity is too small
    /// for the workload.
    pub fn fallbacks(&self) -> u64 {
        self.0.borrow().fallbacks
    }

    /// Borrow a source buffer holding at least `len` bytes. Reuses a free pooled
    /// buffer when `len <= buf_size`; otherwise, if the request fits the slab and
    /// the pool is below `capacity`, registers a new slab buffer (lazy growth) that
    /// will return to the pool on drop; otherwise registers a one-off via `register`
    /// (oversized §8.4, or pool exhausted). `register` — the owning stream's
    /// `register_source` — is called only when a registration is actually needed,
    /// and with no pool borrow held (so it may freely touch the stream).
    pub fn acquire(
        &self,
        len: usize,
        register: impl FnOnce(usize) -> io::Result<RegisteredBuffer>,
    ) -> io::Result<SourceLease> {
        // Fast path: hand out a free pooled buffer that fits, under a momentary
        // borrow (never held across the caller's write/await — the lease owns the
        // buffer and only an Rc). Otherwise decide whether to grow the pool.
        let (grow, slab) = {
            let mut inner = self.0.borrow_mut();
            let fits = len <= inner.buf_size;
            if fits {
                if let Some(buf) = inner.free.pop() {
                    return Ok(self.lent(buf));
                }
            }
            (fits && inner.registered < inner.capacity, inner.buf_size)
        };

        if grow {
            // Lazily register a fresh slab buffer that joins the pool for reuse.
            let buf = register(slab)?;
            let mut inner = self.0.borrow_mut();
            inner.registered += 1;
            return Ok(self.lent(buf));
        }

        // Fallback: oversized for the slab, or the pool is at capacity and empty.
        let buf = register(len.max(1))?;
        self.0.borrow_mut().fallbacks += 1;
        Ok(SourceLease { buf: Some(buf), pool: None })
    }

    /// Wrap a pooled buffer in a lease that returns it to this pool on drop.
    fn lent(&self, buf: RegisteredBuffer) -> SourceLease {
        SourceLease {
            buf: Some(buf),
            pool: Some(Rc::clone(&self.0)),
        }
    }
}

/// A source buffer lent by a [`SourcePool`]. Reach it via [`buffer`](Self::buffer)
/// to fill and RDMA-write from; hold the lease until the write is acknowledged
/// (the NIC DMA-reads the buffer until then). On drop a pooled buffer returns to
/// the pool for reuse; a one-off fallback buffer is deregistered. It owns its
/// buffer and holds only an `Rc` to the pool, so it is safe to keep across an
/// `.await`.
pub struct SourceLease {
    buf: Option<RegisteredBuffer>,
    // Some -> pooled (return to the pool on drop); None -> one-off (deregister).
    pool: Option<Rc<RefCell<PoolInner>>>,
}

impl SourceLease {
    /// The leased registered source buffer (capacity >= the requested length).
    pub fn buffer(&self) -> &RegisteredBuffer {
        self.buf
            .as_ref()
            .expect("a SourceLease holds its buffer until it is dropped")
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        if let (Some(buf), Some(pool)) = (self.buf.take(), self.pool.take()) {
            // Return the pooled buffer for reuse. The write drained before the
            // lease dropped, so no DMA references it.
            pool.borrow_mut().free.push(buf);
        }
        // else: a one-off fallback buffer drops here (its MR is deregistered).
    }
}

// ---- client data plane (spec §7.7) -------------------------------------------

/// A completed split-mode transfer, identified by the `id` the client placed in
/// its `X-HORD-RDMA-Write` request ([`ZeroCopyRequest::with_id`]). By the time
/// this is returned the payload is fully in the client's registered buffer (QP
/// ordering guarantees the write landed before the immediate's completion,
/// §7.7.2), so the consumer can use it immediately without the HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitCompletion {
    /// The transfer ID, echoed from the write-with-immediate's `imm_data`.
    pub transfer_id: u32,
}

/// The client's *data plane* for protocol splitting (§7.7): payload completions
/// arrive straight off the CQ — keyed by transfer ID, with no HTTP parsing — for
/// requests issued with [`ZeroCopyRequest::with_id`]. The consumer maps each
/// returned [`SplitCompletion::transfer_id`] back to the buffer it advertised.
///
/// In this prototype the data plane shares the one driver task — and the one
/// [`HordStream`] — with the control plane (see PROTOTYPE.md, "single-task
/// driver"), so a `SplitReceiver` *borrows* the stream for a poll rather than
/// owning an independent CQ waiter. The intended use is: the control plane issues
/// its requests, then the data plane drains completions. A production split would
/// run the data plane on its own thread polling the shared CQ directly; that
/// needs a multi-waiter scheme and is deferred.
pub struct SplitReceiver<'s> {
    stream: &'s mut HordStream,
}

impl<'s> SplitReceiver<'s> {
    /// Borrow `stream` as a data-plane receiver. Errors if protocol splitting was
    /// not negotiated on this connection (gate with
    /// [`HordStream::split_mode_negotiated`]).
    pub fn new(stream: &'s mut HordStream) -> io::Result<Self> {
        if !stream.split_mode_negotiated() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "protocol splitting (§7.7) was not negotiated on this connection",
            ));
        }
        Ok(SplitReceiver { stream })
    }

    /// Block until the next transfer completes, returning its [`SplitCompletion`]
    /// — or `None` if the connection closed first. Mirrors the spec §13.2
    /// `poll_completion()` shape (synchronous here; the async stream parks on the
    /// completion fd instead of busy-polling).
    pub fn poll_completion(&mut self) -> io::Result<Option<SplitCompletion>> {
        Ok(self
            .stream
            .poll_completed_transfer()?
            .map(|transfer_id| SplitCompletion { transfer_id }))
    }

    /// Non-blocking: drain the CQ once and return a transfer if one is already
    /// complete, else `None`. A `None` here means "nothing ready *yet*" — it does
    /// NOT signal end-of-stream; a consumer looping on `try_completion` MUST also
    /// check [`is_closed`](Self::is_closed) to detect a closed connection (unlike
    /// the blocking [`poll_completion`](Self::poll_completion), whose `None` is
    /// EOF). This asymmetry is why `is_closed` exists.
    pub fn try_completion(&mut self) -> io::Result<Option<SplitCompletion>> {
        self.stream.drain_completions()?;
        Ok(self
            .stream
            .next_completed_transfer()
            .map(|transfer_id| SplitCompletion { transfer_id }))
    }

    /// Whether the connection has closed. A non-blocking consumer driving
    /// [`try_completion`](Self::try_completion) checks this to distinguish EOF
    /// from "no transfer ready yet" — once `is_closed()` is true and
    /// `try_completion` returns `None`, no further transfers will arrive.
    pub fn is_closed(&self) -> bool {
        self.stream.is_closed()
    }
}
