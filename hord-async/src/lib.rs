//! Async HORD stream: [`tokio::io::AsyncRead`] + [`tokio::io::AsyncWrite`] over
//! a [`hord_stream::HordStream`], so `hyper` (or any tokio byte-stream consumer)
//! can run over RDMA unmodified.
//!
//! ## How it drives the NIC
//!
//! The synchronous stream busy-polls the completion queue (100% CPU while
//! blocked). Here we instead wait on the CQ's completion-channel fd via
//! [`tokio::io::unix::AsyncFd`]: arm the CQ, park the task on the fd, and only
//! drain completions when the kernel signals one. That is the whole of review
//! item #15 — a blocked connection now costs nothing.
//!
//! The credit / control-lane state machine is *not* reimplemented: this type
//! calls the same non-blocking primitives the synchronous facade does
//! ([`try_read`](hord_stream::HordStream::try_read),
//! [`try_write`](hord_stream::HordStream::try_write), …) and supplies a reactor
//! in place of the busy-poll loop.
//!
//! ## Thread affinity
//!
//! [`HordStream`] is `!Send` (its registered buffers hold raw pointers), so an
//! `AsyncHordStream` is pinned to the thread that built it. A server therefore
//! accepts on one thread (producing the `Send`
//! [`Connection`](hord_stream::Connection)) and builds + runs each stream on the
//! thread that will drive it — see [`AsyncHordStream::from_accepted`].
//!
//! ## Driver model
//!
//! [`AsyncHordStream`] itself is meant to be driven by a **single task** — one
//! state machine that reads and writes in turn. That is exactly how `hyper`
//! drives a connection (read the request, write the response), so it is the case
//! that matters, and it is the only mode in which `AsyncHordStream`'s own
//! `AsyncRead`/`AsyncWrite` impls are sound: every `poll_*` arms and drains the
//! one completion-channel fd itself, so two *independent* tasks each polling it
//! (e.g. via [`tokio::io::split`]) would clobber each other's waker and steal
//! each other's completions.
//!
//! For genuinely concurrent, independently-scheduled halves, use
//! [`AsyncHordStream::into_split`] instead of `tokio::io::split`: it spawns one
//! **pump** task that owns the fd and drains the CQ, and hands back a
//! [`ReadHalf`], [`WriteHalf`], and [`DataPlane`] that each park on a shared
//! waker list rather than on the fd. That is the multi-waiter scheme — it makes
//! two-task duplex and a separate split-mode (§7.7) data-plane consumer work
//! (see [`into_split`](AsyncHordStream::into_split)). The underlying
//! [`HordStream`] was always full-duplex-correct (the sync `full_duplex_bulk`
//! test proves it); it only lacked a way to be driven from more than one async
//! task.
//!
//! ## Zero-copy (spec §7)
//!
//! The one-sided RDMA write is driven through [`SharedAsyncStream`], a clonable
//! handle that lets a `hyper` server reach the connection from inside its request
//! handler — necessary because the write shares the one completion queue `hyper`
//! drains, on the one driving task. A handler's one call is
//! [`SharedAsyncStream::serve_rdma_write_pooled`] (or
//! [`serve_rdma_write`](SharedAsyncStream::serve_rdma_write)): it runs the
//! §7.3/§7.7 server policy, performs the write, and returns the
//! [`RdmaWriteStatus`] — including the `bytes_written` count a host's transaction
//! log needs (the body bypasses `hyper`, so frame-counting sees nothing). The
//! lower-level [`rdma_write`](SharedAsyncStream::rdma_write) primitive is still
//! there for callers that drive the policy themselves. The client needs no handle:
//! it registers its destination buffer up front
//! ([`AsyncHordStream::register_remote_writable`]) and reads it after the
//! response. The `X-HORD-RDMA-Write` HTTP semantics live in `hord-zerocopy`.
//!
//! ## Timeouts
//!
//! Per-operation deadlines are applied the idiomatic tokio way — wrap a call in
//! [`tokio::time::timeout`] — rather than baked into the stream. Combined with
//! the now-tunable CM retry params (`hord_stream::HordConfig::cm`), a
//! stalled-but-alive peer no longer hangs a reader forever (review item #11). A
//! cancelled (timed-out) read/write future drops cleanly: already-reaped
//! completions stay reaped and no slot is leaked.

use std::cell::RefCell;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use hord_stream::{Connection, HordConfig, HordStream, Mr, RegisteredBuffer, WriteSegment};
use hord_zerocopy::{RdmaWriteAction, RdmaWriteReq, RdmaWriteStatus, SourcePool};

mod listener;
pub use listener::HordListener;

/// A raw fd owned elsewhere (by the connection), wrapped only so `AsyncFd` can
/// register it with the reactor. Dropping it does **not** close the fd — it just
/// deregisters; the connection closes the fd at shutdown.
struct ReactorFd(RawFd);

impl AsRawFd for ReactorFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// An async byte stream over a single HORD RC connection.
pub struct AsyncHordStream {
    // Field order is load-bearing for Drop: the `AsyncFd`s must deregister their
    // fds from the reactor *before* `HordStream`'s Drop shuts the connection down
    // and closes those fds. Struct fields drop in declaration order, so the fds
    // come first and `stream` last.
    cq: AsyncFd<ReactorFd>,
    cm: Option<AsyncFd<ReactorFd>>,
    stream: HordStream,
}

impl AsyncHordStream {
    /// Client: connect to `ip:port` and complete the HORD handshake, then wrap
    /// the stream for async I/O.
    ///
    /// The handshake is synchronous (it briefly blocks the calling task — there
    /// is nothing else to do on this connection yet). Must be called from within
    /// a Tokio runtime, since it registers the completion fd with the reactor.
    pub fn connect(ip: &str, port: u16, config: &HordConfig) -> io::Result<Self> {
        Self::wrap(HordStream::connect(ip, port, config)?)
    }

    /// Server: finish a connection returned by
    /// [`HordStream::accept_begin`](hord_stream::HordStream::accept_begin)
    /// (register buffers, post receives, complete the handshake) and wrap it.
    /// Call on the thread that will drive the connection — the resulting stream
    /// is `!Send`.
    pub fn from_accepted(conn: Connection, config: &HordConfig) -> io::Result<Self> {
        Self::wrap(HordStream::from_accepted(conn, config)?)
    }

    /// Register a freshly-handshaked stream's fds with the reactor.
    fn wrap(stream: HordStream) -> io::Result<Self> {
        let cq = AsyncFd::new(ReactorFd(stream.cq_fd()?))?;
        // Half-close detection is best-effort: flip the CM channel non-blocking
        // and register it. If that fails we simply run without it — the data
        // path is unaffected; we just won't notice a peer disconnect as promptly
        // (the next failed completion still closes the stream).
        let cm = stream
            .set_cm_nonblock()
            .and_then(|()| stream.cm_fd())
            .ok()
            .and_then(|fd| AsyncFd::new(ReactorFd(fd)).ok());
        Ok(AsyncHordStream { cq, cm, stream })
    }

    /// Effective max payload bytes per RDMA message after negotiation.
    pub fn payload_capacity(&self) -> usize {
        self.stream.payload_capacity()
    }

    /// Begin a graceful disconnect (best-effort).
    pub fn disconnect(&self) {
        self.stream.disconnect();
    }

    /// A handle that can force this connection's QP down out-of-band, making the
    /// NIC quiescent so source buffers can be freed safely. See
    /// [`HordStream::teardown_handle`]. [`HordListener`] takes one per connection
    /// so that, if it must *abort* a task parked mid-`RDMA_WRITE` at the grace
    /// deadline, it can quiesce the NIC before the aborted future frees a source
    /// buffer the QP still references — closing the use-after-free that task abort
    /// would otherwise open.
    pub fn teardown_handle(&self) -> hord_stream::ConnTeardown {
        self.stream.teardown_handle()
    }

    // ---- zero-copy extension (spec §7) -------------------------------------

    /// Whether the zero-copy extension was negotiated on this connection.
    /// See [`HordStream::zero_copy_negotiated`].
    pub fn zero_copy_negotiated(&self) -> bool {
        self.stream.zero_copy_negotiated()
    }

    /// Whether protocol splitting (spec §7.7) was negotiated on this connection.
    /// See [`HordStream::split_mode_negotiated`].
    pub fn split_mode_negotiated(&self) -> bool {
        self.stream.split_mode_negotiated()
    }

    /// Register a destination buffer the peer may RDMA-write into (client side).
    /// See [`HordStream::register_remote_writable`].
    pub fn register_remote_writable(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.stream.register_remote_writable(len)
    }

    /// Register a source buffer to RDMA-write from (server side).
    /// See [`HordStream::register_source`].
    pub fn register_source(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.stream.register_source(len)
    }

    /// Register caller-owned memory as a zero-copy write *source* (server side),
    /// returning an [`Mr`] — DMA straight out of resident pages instead of copying
    /// into a HORD buffer. Combine its spans with [`WriteSegment::from_mr`] and
    /// deliver them with [`SharedAsyncStream::rdma_write_gather`]. See
    /// [`HordStream::register_external`].
    ///
    /// # Safety
    /// `[ptr, ptr+len)` must stay live, resident, and unmodified until the returned
    /// [`Mr`] is dropped — and across any in-flight write that references it (same
    /// contract as [`HordStream::register_external`]).
    pub unsafe fn register_external(&self, ptr: *mut u8, len: usize) -> io::Result<Mr> {
        // SAFETY: forwarded to the stream's register_external (same contract).
        unsafe { self.stream.register_external(ptr, len) }
    }

    /// Non-blocking driver for a one-sided RDMA write: post the WR(s) on the first
    /// poll (tracked by `w.posted`), then park on the completion fd until every WR
    /// is acknowledged. Mirrors [`poll_write`] / [`poll_flush`] — it reuses
    /// [`poll_events`](Self::poll_events) and the write driver in `hord-stream`,
    /// duplicating no flow-control logic. [`SharedAsyncStream::rdma_write`] wraps
    /// this in a future.
    fn poll_rdma_write(
        &mut self,
        cx: &mut Context<'_>,
        w: &mut PendingWrite<'_>,
    ) -> Poll<io::Result<()>> {
        // Post on the first poll. Split mode (§7.7) rides the immediate on the
        // final WR; plain zero-copy posts an ordinary write. Both reap as one-sided
        // writes. A split post can hit transfer-credit back-pressure (§7.7.6): it
        // returns `WouldBlock` having posted nothing, so leave `post_outcome` unset
        // and pump events to free a credit, retrying on a later poll.
        while w.post_outcome.is_none() {
            // The gather entry points subsume the single-buffer case (a 1-segment
            // list), so one post path drives both `rdma_write` and
            // `rdma_write_gather`; the immediate rides the final WR.
            let r = match w.imm {
                Some(id) => self.stream.begin_rdma_write_gather_with_imm(
                    w.segments, w.peer_addr, w.peer_rkey, id,
                ),
                None => self
                    .stream
                    .begin_rdma_write_gather(w.segments, w.peer_addr, w.peer_rkey),
            };
            match r {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Back-pressure: the transfer-credit window (§7.7.6) or the
                    // send pool is momentarily full and `begin` posted nothing.
                    // An outstanding send/write must drain to free it; if NOTHING
                    // is in flight the block can't clear (e.g. an imm write on a
                    // connection that never negotiated split) — surface it.
                    if !self.stream.sends_outstanding() && !self.stream.writes_pending() {
                        return Poll::Ready(Err(io::Error::other(
                            "RDMA write back-pressured with nothing in flight to drain",
                        )));
                    }
                    match self.poll_events(cx)? {
                        Poll::Ready(()) => continue,
                        Poll::Pending => return Poll::Pending,
                    }
                }
                other => w.post_outcome = Some(other),
            }
        }
        // Drain EVERY write that posted before resolving — even on a post error or
        // a closed stream. Otherwise an error return would let the caller drop a
        // source buffer/`Mr` (deregistering its MR + freeing/unpinning the storage)
        // while the NIC is still DMA-reading it from an outstanding write (UAF).
        while self.stream.writes_pending() {
            match self.poll_events(cx)? {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
        // All posted writes reaped. A post error wins; else a closed stream means
        // the write didn't fully land (§7.4 — never report complete); else Ok.
        if let Some(Err(e)) = w.post_outcome.take() {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(if self.stream.is_closed() {
            Err(write_aborted())
        } else {
            Ok(())
        })
    }

    /// Data-plane driver for protocol splitting (§7.7): drive the completion fd
    /// until the next split-mode transfer ID is available, returning it — or
    /// `None` once the connection has closed with nothing left queued. Already
    /// reaped transfers (e.g. drained while the control plane read an HTTP
    /// response) are returned immediately without parking. Mirrors `poll_read` /
    /// `poll_rdma_write`: same reactor, no flow-control logic duplicated.
    fn poll_split_completion(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<u32>>> {
        loop {
            if let Some(result) = next_transfer(&mut self.stream) {
                return Poll::Ready(Ok(result));
            }
            std::task::ready!(self.poll_events(cx))?;
        }
    }

    /// Drive the completion (and CM) fds for the single-task driver: a thin
    /// wrapper that hands its owned `stream`/`cq`/`cm` to the shared
    /// [`poll_reactor`] (the multi-waiter pump drives the same core through a
    /// `RefCell`). `Ready(())` means the state advanced — completions were drained
    /// or the peer closed — so the caller should retry its `try_*` operation;
    /// `Pending` means the task is parked on the reactor.
    fn poll_events(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        poll_reactor(&mut self.stream, &self.cq, &self.cm, cx)
    }
}

// ---- shared reactor core (single-task driver + multi-waiter pump) -----------
//
// These free functions are the non-blocking heart of both drivers. The
// single-task `AsyncHordStream` owns `stream`/`cq`/`cm` and drives them inline
// (looping on `poll_events`); the multi-waiter pump ([`pump_loop`]) reaches the
// same `stream` through a `RefCell`. Both call these, so the credit logic, the
// EOF/closed semantics, the flush barrier, and the completion-channel race guard
// live in exactly one place rather than drifting between the two paths.

/// One reactor step: drain the CQ, check for a peer half-close, then arm + park
/// on the completion-channel fd. `Ready(())` means the state advanced (drained,
/// or the peer closed) and the caller should retry its `try_*` op; `Pending`
/// means parked on the fd(s).
///
/// The arm/drain ordering closes the classic completion-channel race: we arm the
/// CQ and then drain again, so a completion landing between the first drain and
/// the arm is still seen (it is now in the CQ), and any later one triggers a
/// notification on the armed channel.
fn poll_reactor(
    stream: &mut HordStream,
    cq: &AsyncFd<ReactorFd>,
    cm: &Option<AsyncFd<ReactorFd>>,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    // 1. Anything already sitting in the CQ.
    if stream.drain_completions()? > 0 {
        return Poll::Ready(Ok(()));
    }
    // 2. Peer-initiated half-close.
    if poll_cm_event(stream, cm, cx)? {
        return Poll::Ready(Ok(()));
    }
    // 3. Arm, then drain again to close the arm race.
    stream.arm_cq()?;
    if stream.drain_completions()? > 0 {
        return Poll::Ready(Ok(()));
    }
    // 4. Park on the completion-channel fd.
    loop {
        let mut guard = std::task::ready!(cq.poll_read_ready(cx))?;
        // The fd signalled: consume the notification (drains the fd so it is no
        // longer readable), re-arm for the next one, and drain the CQ.
        stream.consume_cq_events();
        stream.arm_cq()?;
        let drained = stream.drain_completions()?;
        guard.clear_ready();
        if drained > 0 {
            return Poll::Ready(Ok(()));
        }
        // The notification carried no work we hadn't already drained (or was
        // spurious). We re-armed and cleared readiness, so loop to re-park.
    }
}

/// Best-effort, non-blocking peer-half-close check via the CM fd. Returns `true`
/// if it just marked the stream closed.
///
/// On a *non-teardown* CM event it loops: the next `poll_read_ready` re-checks
/// the fd and, once no event remains, returns `Pending` and re-registers the
/// waker — so a *later* disconnect still wakes the driver. Without this re-arm a
/// driver that consumed a non-teardown event would stop watching the CM fd until
/// some unrelated CQ completion happened to re-enter the reactor (harmless for
/// the single-task path, which re-enters on every poll, but a latent miss for the
/// pump, which only re-enters on a wake).
fn poll_cm_event(
    stream: &mut HordStream,
    cm: &Option<AsyncFd<ReactorFd>>,
    cx: &mut Context<'_>,
) -> io::Result<bool> {
    let Some(cm) = cm.as_ref() else {
        return Ok(false);
    };
    loop {
        match cm.poll_read_ready(cx) {
            Poll::Ready(Ok(mut guard)) => {
                let disconnected = stream.check_disconnect()?;
                guard.clear_ready();
                if disconnected {
                    stream.mark_closed();
                    return Ok(true);
                }
                // Non-teardown event (or spurious readiness): loop to re-poll,
                // which re-registers the waker on the next `Pending`.
            }
            Poll::Ready(Err(e)) => return Err(e),
            Poll::Pending => return Ok(false),
        }
    }
}

/// One non-blocking read attempt into `buf`. `Ok(true)` means resolve `poll_read`
/// as `Ready` (bytes were read, or EOF); `Ok(false)` means no data is buffered
/// yet and the caller should park — owed credits have been returned first, so a
/// peer blocked on us (the #3 path) can make progress.
fn try_read_into(stream: &mut HordStream, buf: &mut ReadBuf<'_>) -> io::Result<bool> {
    match stream.try_read(buf.initialize_unfilled())? {
        // EOF (or the caller passed a full buffer): no bytes, Ready.
        Some(0) => Ok(true),
        Some(n) => {
            buf.advance(n);
            Ok(true)
        }
        None => {
            stream.return_owed_credits(true)?;
            Ok(false)
        }
    }
}

/// One non-blocking write attempt. `Some(n)` means `n` bytes were accepted
/// (resolve `poll_write` as `Ready`); `None` means no slot/credit right now and
/// the caller should park. `try_write` errors if the stream is closed.
fn try_write_from(stream: &mut HordStream, buf: &[u8]) -> io::Result<Option<usize>> {
    let n = stream.try_write(buf)?;
    Ok((n > 0).then_some(n))
}

/// The flush delivery barrier. `Ok(true)` means every staged message is sent and
/// every data send *and* one-sided RDMA write is acknowledged (an RC completion
/// == delivered + acked); `Ok(false)` means the caller should park;
/// `Err(BrokenPipe)` if the connection closed before all sends were acked.
fn poll_flush_ready(stream: &mut HordStream) -> io::Result<bool> {
    let stage_clear = stream.try_flush_stage()?;
    if stage_clear && !stream.sends_outstanding() && !stream.writes_pending() {
        return Ok(true);
    }
    if stream.is_closed() {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "connection closed before all sends were acknowledged",
        ));
    }
    Ok(false)
}

/// Next split-mode (§7.7) transfer outcome, or `None` if the caller should park.
/// `Some(Some(id))` is a completed transfer; `Some(None)` is end-of-stream (the
/// connection closed with none queued). Queued completions win even after a
/// half-close — the payload landed before the peer went away.
fn next_transfer(stream: &mut HordStream) -> Option<Option<u32>> {
    if let Some(id) = stream.next_completed_transfer() {
        return Some(Some(id));
    }
    stream.is_closed().then_some(None)
}

impl AsyncRead for AsyncHordStream {
    /// Reads buffered stream data, parking on the completion fd when none is
    /// ready. **Keep-alive EOF:** this resolves to `Ok(())` with zero bytes filled
    /// (EOF) *only* when the peer has actually half-closed (a graceful CM
    /// disconnect or a transport teardown — `try_read` returns `Some(0)` once
    /// `peer_closed`). It never reports EOF merely because no bytes are buffered
    /// between two requests, so `hyper`'s keep-alive loop serves many requests over
    /// one QP and a host's promote-on-clean-EOF logic only triggers on a real
    /// close — the same contract a `TcpStream` gives. (Verified by
    /// `tests/listener.rs`.)
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if try_read_into(&mut this.stream, buf)? {
                return Poll::Ready(Ok(()));
            }
            std::task::ready!(this.poll_events(cx))?;
        }
    }
}

impl AsyncWrite for AsyncHordStream {
    /// Accepts as many bytes as a send slot + credit allow right now.
    /// **Backpressure:** when the credit window is exhausted (`try_write` accepts
    /// nothing), this does *not* buffer unbounded and return `Ready` — it parks on
    /// the completion fd and returns `Poll::Pending` until a send completion frees
    /// a credit. A slow RDMA reader therefore back-pressures the writer, so a host
    /// that streams a body through a bounded channel (throttled by `poll_write`
    /// going `Pending`) cannot be driven to pull an arbitrarily large object fully
    /// into RAM. (Verified by `tests/listener.rs`.)
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if let Some(n) = try_write_from(&mut this.stream, buf)? {
                return Poll::Ready(Ok(n));
            }
            std::task::ready!(this.poll_events(cx))?;
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if poll_flush_ready(&mut this.stream)? {
                return Poll::Ready(Ok(()));
            }
            std::task::ready!(this.poll_events(cx))?;
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // This type owns the whole stream, so a shutdown closes the connection:
        // deliver everything, then begin a graceful disconnect.
        std::task::ready!(self.as_mut().poll_flush(cx))?;
        self.stream.disconnect();
        Poll::Ready(Ok(()))
    }
}

fn write_aborted() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "connection closed before the RDMA write completed",
    )
}

/// In-progress one-sided RDMA write, threaded through
/// [`AsyncHordStream::poll_rdma_write`] across polls. `post_outcome` is `None`
/// until the first poll issues the WR(s); thereafter it holds that `begin`
/// call's result, which is reported only *after* every posted WR has been drained
/// (so a post error can't drop a source region with DMA outstanding). The source
/// is a scatter-gather [`WriteSegment`] list — the single-buffer
/// [`rdma_write`](SharedAsyncStream::rdma_write) is just the 1-segment case — laid
/// down contiguously at `[peer_addr, …]`. The `'a` borrow keeps the source regions
/// alive for the whole write.
struct PendingWrite<'a> {
    segments: &'a [WriteSegment<'a>],
    peer_addr: u64,
    peer_rkey: u32,
    /// `Some(id)` for split mode (§7.7): the final WR is a write-with-immediate
    /// carrying `id`. `None` for a plain zero-copy write.
    imm: Option<u32>,
    post_outcome: Option<io::Result<()>>,
}

/// A clonable handle to an [`AsyncHordStream`], so a `hyper` *server* can drive a
/// zero-copy RDMA write from inside its request handler while `hyper` owns the
/// stream for HTTP. (`hyper`'s `service_fn` never receives the connection, and
/// the RDMA write shares the one completion queue `hyper` drains — so the write
/// must be driven by the same object, on the same task.)
///
/// Both `hyper` (via [`tokio::io::AsyncRead`]/[`AsyncWrite`]) and the handler (via
/// [`serve_rdma_write_pooled`](Self::serve_rdma_write_pooled) — the recommended
/// one-call entry point — or the lower-level [`rdma_write`](Self::rdma_write)) reach
/// the stream through this handle, which `borrow_mut`s the shared cell **only for
/// the duration of each poll — never across an await**. That keeps the two borrows *sequential*, which is the whole
/// safety argument: everything runs on one current-thread task, and within that
/// task no two polls overlap, so the cell is never borrowed twice at once.
///
/// **The borrow discipline is a runtime invariant, not a type-level one.** It
/// holds for the demo (a single body-less GET, driven by `hyper`'s sequential
/// per-connection poll loop) and for any single-task driver that polls read,
/// write, and `rdma_write` in turn. It would be **violated** by driving the
/// stream from two tasks at once — e.g. [`tokio::io::split`] with the halves on
/// independent tasks of a multi-thread runtime — where a concurrent
/// `borrow_mut` panics (`already borrowed: BorrowMutError`), aborting the
/// connection task. That split model is the one the module header documents as
/// unsupported; this handle does not change that. (No present code path — not
/// even a streamed request body, which `hyper` still reads sequentially before
/// polling the handler — triggers it.)
///
/// The *client* does not need this: it registers its destination buffer up front
/// and reads it after the response, so it can hand a plain [`AsyncHordStream`] to
/// `hyper` and keep the [`RegisteredBuffer`] alongside.
#[derive(Clone)]
pub struct SharedAsyncStream(Rc<RefCell<AsyncHordStream>>);

impl SharedAsyncStream {
    /// Wrap an [`AsyncHordStream`] in a shared, clonable handle.
    pub fn new(inner: AsyncHordStream) -> Self {
        SharedAsyncStream(Rc::new(RefCell::new(inner)))
    }

    /// Whether the zero-copy extension was negotiated. See
    /// [`AsyncHordStream::zero_copy_negotiated`].
    pub fn zero_copy_negotiated(&self) -> bool {
        self.0.borrow().zero_copy_negotiated()
    }

    /// Whether protocol splitting (§7.7) was negotiated. See
    /// [`AsyncHordStream::split_mode_negotiated`].
    pub fn split_mode_negotiated(&self) -> bool {
        self.0.borrow().split_mode_negotiated()
    }

    /// Register a source buffer to RDMA-write from (server side).
    pub fn register_source(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.0.borrow().register_source(len)
    }

    /// Register a destination buffer the peer may RDMA-write into (client side).
    pub fn register_remote_writable(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.0.borrow().register_remote_writable(len)
    }

    /// Register caller-owned memory as a zero-copy write *source*, returning an
    /// [`Mr`] to gather from via [`rdma_write_gather`](Self::rdma_write_gather). See
    /// [`AsyncHordStream::register_external`].
    ///
    /// # Safety
    /// Same contract as [`AsyncHordStream::register_external`]: the region must stay
    /// live, resident, and unmodified until the `Mr` is dropped and across any
    /// in-flight write that references it.
    pub unsafe fn register_external(&self, ptr: *mut u8, len: usize) -> io::Result<Mr> {
        // SAFETY: forwarded (same residency/lifetime contract).
        unsafe { self.0.borrow().register_external(ptr, len) }
    }

    /// Best-effort graceful disconnect.
    pub fn disconnect(&self) {
        self.0.borrow().disconnect();
    }

    /// RDMA-write `src[src_off .. src_off+len]` into the peer's `[peer_addr,
    /// peer_rkey]`, awaiting completion. The body is delivered out-of-band; the
    /// caller then sends an HTTP response with `Content-Length: 0`. Borrows the
    /// shared stream afresh on each poll (no borrow held across an await). For a
    /// *fragmented* source use [`rdma_write_gather`](Self::rdma_write_gather); this
    /// is the single-segment case of it.
    pub async fn rdma_write(
        &self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        let segments = [WriteSegment::from_registered(src, src_off, len)];
        let mut w = PendingWrite {
            segments: &segments,
            peer_addr,
            peer_rkey,
            imm: None,
            post_outcome: None,
        };
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_rdma_write(cx, &mut w)).await
    }

    /// Split-mode (§7.7) counterpart of [`rdma_write`](Self::rdma_write): deliver
    /// the body with RDMA write-with-immediate carrying `transfer_id`, so the
    /// peer's data plane is signalled on its CQ. On `Ok(())` the payload landed
    /// and the immediate was delivered; the caller then sends the HTTP response
    /// (still `Content-Length: 0`). `len` may be `0` — an empty WR still carries
    /// the immediate. Borrows the shared stream afresh on each poll.
    pub async fn rdma_write_with_imm(
        &self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
        transfer_id: u32,
    ) -> io::Result<()> {
        let segments = [WriteSegment::from_registered(src, src_off, len)];
        let mut w = PendingWrite {
            segments: &segments,
            peer_addr,
            peer_rkey,
            imm: Some(transfer_id),
            post_outcome: None,
        };
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_rdma_write(cx, &mut w)).await
    }

    /// Scatter-gather counterpart of [`rdma_write`](Self::rdma_write) (spec §7,
    /// Milestone 3): deliver a *fragmented* source — a `&[WriteSegment]`, e.g. an
    /// MSE4 object's non-contiguous allocations registered via
    /// [`register_external`](Self::register_external) — as one logical zero-copy
    /// write, laid down contiguously at the peer's `[peer_addr, peer_rkey]`. The
    /// `segments` borrow keeps every source [`Mr`]/buffer alive until the write
    /// completes (no copy into a HORD buffer first — the actual zero-copy path).
    /// Borrows the shared stream afresh on each poll (no borrow across an await).
    pub async fn rdma_write_gather(
        &self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
    ) -> io::Result<()> {
        let mut w = PendingWrite {
            segments,
            peer_addr,
            peer_rkey,
            imm: None,
            post_outcome: None,
        };
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_rdma_write(cx, &mut w)).await
    }

    /// Split-mode (§7.7) counterpart of [`rdma_write_gather`](Self::rdma_write_gather):
    /// the final WR is a write-with-immediate carrying `transfer_id`, signalling the
    /// peer's data plane on its CQ. An empty gather still delivers the immediate.
    pub async fn rdma_write_gather_with_imm(
        &self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
        transfer_id: u32,
    ) -> io::Result<()> {
        let mut w = PendingWrite {
            segments,
            peer_addr,
            peer_rkey,
            imm: Some(transfer_id),
            post_outcome: None,
        };
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_rdma_write(cx, &mut w)).await
    }

    /// Serve the server side of a zero-copy response (spec §7.3, and §7.7 in split
    /// mode) over this connection, returning the [`RdmaWriteStatus`] to put in the
    /// HTTP response — **the single borrow-sound entry point** a `hyper` handler
    /// should use to deliver a body out-of-band. A fresh source MR is registered per
    /// call; for a connection that serves many responses (HTTP keep-alive, or a
    /// split run) use [`serve_rdma_write_pooled`](Self::serve_rdma_write_pooled) to
    /// amortize registration (spec §8.3).
    ///
    /// This is the async counterpart of [`hord_zerocopy::serve_rdma_write`]: it runs
    /// the same pure §7.3/§7.7 policy ([`RdmaWriteAction::decide`]) and then drives
    /// the *async* one-sided write through this handle, so the policy can never
    /// drift from the synchronous path.
    ///
    /// # What it returns (the §7.4 outcome — DMA byte count + status for logging)
    ///
    /// * [`RdmaWriteStatus::Complete`] `{ bytes_written }` — the body was delivered
    ///   by RDMA write; `bytes_written` is the count actually placed in the peer's
    ///   buffer (`== object_size`; `0` for an empty split body, whose immediate
    ///   still rode). The response carries `Content-Length: 0`. **A host whose
    ///   transaction log counts `hyper` body frames — which see nothing here — must
    ///   record `bytes_written` as the body size**; surfacing it for exactly that is
    ///   why this returns the status rather than `()`.
    /// * [`RdmaWriteStatus::TooLarge`] `{ object_size }` — the object exceeds the
    ///   client's advertised buffer (`req.len`); **nothing was written** and `fill`
    ///   is never called.
    /// * Never [`RdmaWriteStatus::Declined`]: declining is a host policy decision
    ///   made *before* calling this (don't call it; send the body on the stream and
    ///   echo `declined` yourself). [`RdmaWriteAction::decide`] never yields it.
    ///
    /// `fill` populates the source buffer's first `object_size` bytes just before
    /// the write — this prototype's one server-side copy (Milestone 3 removes it by
    /// gathering from caller-owned MRs). On a transport failure mid-write this
    /// returns `Err` and the connection closes; the caller MUST NOT report
    /// `complete` then (§7.4/§7.7.7) — map the `Err` to a 5xx. The too-large and
    /// error cases never deliver a §7.7 immediate, so a split data-plane consumer
    /// must bound its wait rather than assume one completion per request.
    ///
    /// # Borrow soundness
    ///
    /// This is the pattern the module header calls for: an embedder calls one method
    /// and never touches the `Rc<RefCell<…>>` aliasing rules. It takes only
    /// *momentary* borrows of the shared cell — to decide, to register, and once per
    /// write poll — and **never holds one across an `.await`** (the source
    /// [`RegisteredBuffer`] and the policy live in locals), so the single-task borrow
    /// discipline that makes [`rdma_write`](Self::rdma_write) sound covers the whole
    /// serve.
    pub async fn serve_rdma_write(
        &self,
        req: &RdmaWriteReq,
        object_size: u64,
        fill: impl FnOnce(&RegisteredBuffer),
    ) -> io::Result<RdmaWriteStatus> {
        match RdmaWriteAction::decide(req, object_size, self.split_mode_negotiated()) {
            RdmaWriteAction::Respond(status) => Ok(status),
            RdmaWriteAction::Write {
                payload_len,
                source_len,
                transfer_id,
            } => {
                let src = self.register_source(source_len)?;
                self.run_write_plan(&src, req, payload_len, transfer_id, fill).await?;
                // `src` drops after the write returns: `run_write_plan` awaited the
                // write's completion (and ack), so no DMA references the MR —
                // deregistration is sound. Mirrors `hord_zerocopy::serve_rdma_write`.
                Ok(RdmaWriteStatus::Complete {
                    bytes_written: payload_len as u64,
                })
            }
        }
    }

    /// Like [`serve_rdma_write`](Self::serve_rdma_write), but draws the source region
    /// from a [`SourcePool`] (spec §8.3) instead of registering a fresh MR per
    /// response — the async counterpart of [`hord_zerocopy::serve_rdma_write_pooled`].
    /// `ibv_reg_mr` is expensive (§8.1), so a server reusing a connection (HTTP
    /// keep-alive, or a split run serving many transfers) amortizes it. The
    /// HTTP-facing behaviour is identical: same status (carrying `bytes_written`),
    /// same `Content-Length: 0`, same §7.3/§7.7 policy. The pool falls back to a
    /// one-off registration for an oversized object (§8.4) or a momentarily exhausted
    /// pool, so correctness never depends on the pool being large enough.
    pub async fn serve_rdma_write_pooled(
        &self,
        pool: &SourcePool,
        req: &RdmaWriteReq,
        object_size: u64,
        fill: impl FnOnce(&RegisteredBuffer),
    ) -> io::Result<RdmaWriteStatus> {
        match RdmaWriteAction::decide(req, object_size, self.split_mode_negotiated()) {
            RdmaWriteAction::Respond(status) => Ok(status),
            RdmaWriteAction::Write {
                payload_len,
                source_len,
                transfer_id,
            } => {
                // The registrar borrows the stream only for the `register_source`
                // call (the pool calls it with no pool borrow held), so no borrow is
                // outstanding at the write's `.await`. The lease owns its buffer and
                // holds only an `Rc` to the pool, so it is safe across the await; it
                // drops at block end — after the write is acked — returning the
                // buffer to the pool with no DMA still referencing it.
                let lease = pool.acquire(source_len, |n| self.register_source(n))?;
                self.run_write_plan(lease.buffer(), req, payload_len, transfer_id, fill)
                    .await?;
                Ok(RdmaWriteStatus::Complete {
                    bytes_written: payload_len as u64,
                })
            }
        }
    }

    /// Fill `src`'s first `payload_len` bytes (when there are any) and drive the
    /// one-sided write into the client's `[addr, rkey]` — write-with-immediate
    /// carrying `transfer_id` in split mode (§7.7), else a plain write. The async
    /// sibling of `hord_zerocopy`'s private `run_write_plan`; shared by both serve
    /// methods so the only thing differing between them is where the source comes
    /// from. Holds no `RefCell` borrow across the `.await` (it delegates to
    /// [`rdma_write`](Self::rdma_write) / [`rdma_write_with_imm`](Self::rdma_write_with_imm),
    /// which borrow per poll).
    async fn run_write_plan(
        &self,
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
            Some(id) => {
                self.rdma_write_with_imm(src, 0, req.addr, req.rkey, payload_len, id)
                    .await
            }
            None => self.rdma_write(src, 0, req.addr, req.rkey, payload_len).await,
        }
    }

    /// Receive the next split-mode (§7.7) transfer completion off the CQ,
    /// returning its transfer ID — or `None` when the connection has closed with
    /// none left queued. The data-plane primitive: no HTTP parsing, demultiplexed
    /// by the ID the client put in `X-HORD-RDMA-Write`. Borrows the shared stream
    /// afresh on each poll (no borrow held across the await).
    pub async fn next_split_completion(&self) -> io::Result<Option<u32>> {
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_split_completion(cx)).await
    }
}

impl AsyncRead for SharedAsyncStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut inner = self.get_mut().0.borrow_mut();
        Pin::new(&mut *inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for SharedAsyncStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.get_mut().0.borrow_mut();
        Pin::new(&mut *inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.get_mut().0.borrow_mut();
        Pin::new(&mut *inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.get_mut().0.borrow_mut();
        Pin::new(&mut *inner).poll_shutdown(cx)
    }
}

// ======================= Multi-waiter split (pump model) =====================
//
// One RC connection has a single CQ and a single completion-channel fd, and the
// completions on it are interleaved across directions (recv data, send acks,
// one-sided RDMA-write acks, split-mode immediates). `AsyncHordStream` drives all
// of that from one task by having each `poll_*` arm + drain the fd itself — fine
// for a single state machine, but two independent tasks each parking on the one
// fd would clobber each other's waker (Tokio's `AsyncFd` holds ~one waker per
// direction) and steal each other's completions.
//
// `into_split` closes that gap with a classic reactor split: exactly one **pump**
// task owns the fd, drains the CQ, and wakes every parked handle after each
// drain; the handles never touch the fd — they run the same non-blocking
// `HordStream` primitives the single-task path does, and re-park on a shared
// waker list. The state machine is unchanged; it gains a second (and third)
// driving task, nothing more.

/// State shared between the pump task and the split handles for one connection.
struct Shared {
    /// Wakers of handles parked waiting for the CQ to advance. Wake-all: after any
    /// drain every parked handle re-checks its own predicate (cheap, and it keeps
    /// the pump oblivious to which completion belongs to which handle). Drained
    /// when the pump wakes everyone, so it stays bounded by the live parked count.
    wakers: RefCell<Vec<Waker>>,
    // Drop order is load-bearing (as in `AsyncHordStream`): the `AsyncFd`s must
    // deregister from the reactor *before* `HordStream`'s Drop closes their fds.
    // Struct fields drop in declaration order, so the fds precede `stream`.
    cq: AsyncFd<ReactorFd>,
    cm: Option<AsyncFd<ReactorFd>>,
    stream: RefCell<HordStream>,
}

impl Shared {
    /// Park the **pump** on the reactor and drain the CQ, via the shared
    /// [`poll_reactor`] — the borrow is taken only across `poll_reactor`'s
    /// synchronous work and dropped when it returns, so the pump never holds it
    /// across the `.await` in [`pump_loop`] and the handles can borrow freely when
    /// they run (the current-thread executor never interleaves them mid-poll).
    /// `Ready(())` means the state advanced; `Pending` means parked on the fd(s).
    fn drive_once(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        poll_reactor(&mut self.stream.borrow_mut(), &self.cq, &self.cm, cx)
    }

    /// Record `cx`'s waker so the pump wakes this handle on the next drain.
    /// De-duplicated via [`Waker::will_wake`]; because the pump drains the list
    /// when it wakes everyone, it stays bounded by the count of parked handles.
    fn register(&self, cx: &Context<'_>) {
        let waker = cx.waker();
        let mut wakers = self.wakers.borrow_mut();
        if !wakers.iter().any(|w| w.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    /// Wake every parked handle (wake-all). Takes the list first so the wakes
    /// happen with no borrow held and it is left empty for re-registration.
    fn wake_all(&self) {
        let woken = std::mem::take(&mut *self.wakers.borrow_mut());
        for w in woken {
            w.wake();
        }
    }

    fn is_closed(&self) -> bool {
        self.stream.borrow().is_closed()
    }
}

/// The single task that owns the completion fd for a split stream: park, drain,
/// wake every handle, repeat — until the connection closes (then wake once more
/// so parked handles observe EOF/abort, and exit). Spawned by
/// [`AsyncHordStream::into_split`] and aborted by [`PumpGuard`] when the last
/// handle drops, so it never outlives the handles nor spins on a dead connection.
async fn pump_loop(shared: Rc<Shared>) {
    loop {
        match std::future::poll_fn(|cx| shared.drive_once(cx)).await {
            // The state advanced (a drain or a CM close) — wake everyone to
            // re-check their predicate, then stop if the connection has closed.
            Ok(()) => {
                shared.wake_all();
                if shared.is_closed() {
                    break;
                }
            }
            // The reactor fd errored: no further progress is possible, so mark the
            // stream closed and wake every handle to surface it, then stop.
            Err(_) => {
                shared.stream.borrow_mut().mark_closed();
                shared.wake_all();
                break;
            }
        }
    }
}

/// Aborts the pump task when the last split handle drops. Held by every handle
/// behind an `Rc`, so the pump lives exactly as long as some handle does. The
/// pump also holds its own `Rc<Shared>`, so `Shared` (and the fds + stream it
/// owns) is torn down only once the pump has stopped — never out from under it.
struct PumpGuard(tokio::task::JoinHandle<()>);

impl Drop for PumpGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The handles [`AsyncHordStream::into_split`] yields. Move each to its own task;
/// the shared pump keeps driving the connection until all of them are dropped.
/// Take the ones you need and drop the rest (dropping a subset does not stop the
/// pump — only dropping the last handle does).
pub struct SplitParts {
    /// Read half — [`AsyncRead`], drivable from its own task.
    pub read: ReadHalf,
    /// Write half — [`AsyncWrite`], drivable from its own task.
    pub write: WriteHalf,
    /// Split-mode (§7.7) data plane + buffer registration / negotiation accessors.
    pub data: DataPlane,
}

/// Read half of a split [`AsyncHordStream`] — an [`AsyncRead`] that can be driven
/// from a task independent of the [`WriteHalf`].
pub struct ReadHalf {
    shared: Rc<Shared>,
    _pump: Rc<PumpGuard>,
}

/// Write half of a split [`AsyncHordStream`] — an [`AsyncWrite`] that can be
/// driven from a task independent of the [`ReadHalf`].
pub struct WriteHalf {
    shared: Rc<Shared>,
    _pump: Rc<PumpGuard>,
}

/// Data-plane handle of a split [`AsyncHordStream`]: split-mode (§7.7) transfer
/// completions, plus the buffer-registration and negotiation accessors. Drivable
/// from its own task, concurrently with the HTTP control plane on the
/// [`ReadHalf`] / [`WriteHalf`] — the multi-waiter case the single-task driver
/// could not serve.
///
/// In split mode, hold this for as long as the peer may RDMA-write-with-immediate
/// (i.e. while the [`WriteHalf`] issues id-bearing requests) and keep calling
/// [`next_split_completion`](Self::next_split_completion): each arriving transfer
/// pushes its id onto an in-stream queue, which only the `DataPlane` drains.
/// Dropping it while transfers keep arriving lets that queue grow unbounded (the
/// pre-existing §7.7.5 "repost immediately" backpressure gap — entries are 4
/// bytes, bounded by the peer's outstanding requests), so dropping `DataPlane`
/// while still issuing split requests is unsupported.
pub struct DataPlane {
    shared: Rc<Shared>,
    _pump: Rc<PumpGuard>,
}

impl AsyncHordStream {
    /// Split into independently-pollable handles that can be driven from
    /// **separate tasks** — the multi-waiter scheme that the single-task driver
    /// (and [`tokio::io::split`]) cannot provide for a HORD stream.
    ///
    /// Spawns one **pump** task (via [`tokio::task::spawn_local`]) that owns the
    /// completion fd and drains the CQ for all handles, so each handle just runs
    /// its non-blocking `HordStream` primitive and parks on a shared waker list.
    /// The pump is `!Send` (it drives the `!Send` stream), so **this must be
    /// called from within a [`tokio::task::LocalSet`]** — e.g. the current-thread
    /// runtime + `LocalSet` the async server/demo already use — and panics
    /// otherwise, like any `spawn_local`. The pump is aborted automatically once
    /// every returned handle has been dropped.
    ///
    /// Returns a [`SplitParts`]; use the [`ReadHalf`] / [`WriteHalf`] for two-task
    /// duplex and/or the [`DataPlane`] for a separate split-mode consumer.
    pub fn into_split(self) -> SplitParts {
        // `AsyncHordStream` has no `Drop` impl, so destructuring moves the fds and
        // the stream out wholesale (the careful field-drop order now lives on
        // `Shared` instead).
        let AsyncHordStream { cq, cm, stream } = self;
        let shared = Rc::new(Shared {
            wakers: RefCell::new(Vec::new()),
            cq,
            cm,
            stream: RefCell::new(stream),
        });
        let pump = tokio::task::spawn_local(pump_loop(Rc::clone(&shared)));
        let guard = Rc::new(PumpGuard(pump));
        SplitParts {
            read: ReadHalf {
                shared: Rc::clone(&shared),
                _pump: Rc::clone(&guard),
            },
            write: WriteHalf {
                shared: Rc::clone(&shared),
                _pump: Rc::clone(&guard),
            },
            data: DataPlane { shared, _pump: guard },
        }
    }
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if try_read_into(&mut this.shared.stream.borrow_mut(), buf)? {
            return Poll::Ready(Ok(()));
        }
        // Register *before* yielding: the pump runs only once we return Pending
        // (single-threaded executor), so a drain that wakes us can't be lost.
        this.shared.register(cx);
        Poll::Pending
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Some(n) = try_write_from(&mut this.shared.stream.borrow_mut(), buf)? {
            return Poll::Ready(Ok(n));
        }
        this.shared.register(cx);
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if poll_flush_ready(&mut this.shared.stream.borrow_mut())? {
            return Poll::Ready(Ok(()));
        }
        this.shared.register(cx);
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Flush ONLY — do not disconnect. HORD has no wire-level half-close, and
        // the connection is shared with the ReadHalf (and DataPlane), so shutting
        // down the writer must not tear the whole connection down (which would
        // abort an independent reader mid-response). The connection closes when
        // every handle is dropped; for an explicit close use `DataPlane::disconnect`.
        self.poll_flush(cx)
    }
}

impl DataPlane {
    /// Whether the zero-copy extension was negotiated. See
    /// [`AsyncHordStream::zero_copy_negotiated`].
    pub fn zero_copy_negotiated(&self) -> bool {
        self.shared.stream.borrow().zero_copy_negotiated()
    }

    /// Whether protocol splitting (§7.7) was negotiated. See
    /// [`AsyncHordStream::split_mode_negotiated`].
    pub fn split_mode_negotiated(&self) -> bool {
        self.shared.stream.borrow().split_mode_negotiated()
    }

    /// Register a destination buffer the peer may RDMA-write into (consumer side).
    /// See [`AsyncHordStream::register_remote_writable`].
    pub fn register_remote_writable(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.shared.stream.borrow().register_remote_writable(len)
    }

    /// Register a source buffer to RDMA-write from (producer side).
    /// See [`AsyncHordStream::register_source`].
    pub fn register_source(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.shared.stream.borrow().register_source(len)
    }

    /// Best-effort graceful disconnect of the whole connection.
    pub fn disconnect(&self) {
        self.shared.stream.borrow().disconnect();
    }

    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<io::Result<Option<u32>>> {
        if let Some(result) = next_transfer(&mut self.shared.stream.borrow_mut()) {
            return Poll::Ready(Ok(result));
        }
        self.shared.register(cx);
        Poll::Pending
    }

    /// Receive the next split-mode (§7.7) transfer completion off the CQ,
    /// returning its 32-bit transfer ID — or `None` once the connection has closed
    /// with none left queued. Parks on the shared pump, so it can run on its own
    /// task concurrently with the HTTP control plane on the read/write halves.
    pub async fn next_split_completion(&self) -> io::Result<Option<u32>> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }
}
