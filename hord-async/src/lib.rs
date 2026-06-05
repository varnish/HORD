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
//! The stream is meant to be driven by a **single task** — one state machine
//! that reads and writes in turn. That is exactly how `hyper` drives a
//! connection (read the request, write the response), so it is the case that
//! matters. Splitting the stream with [`tokio::io::split`] and driving the read
//! and write halves from two *independent* tasks is **not** supported: both
//! halves would wait on the one completion-channel fd, and a single completion
//! stream carries events for both directions, so waking the correct half would
//! need a multi-waiter scheme this prototype does not implement. Concurrent
//! bidirectional traffic from one task (the busy-poll path's
//! `full_duplex_bulk`) is fine; two tasks over `split` can stall.
//!
//! ## Zero-copy (spec §7)
//!
//! The one-sided RDMA write is driven through [`SharedAsyncStream`], a clonable
//! handle that lets a `hyper` server reach the connection from inside its request
//! handler — necessary because the write shares the one completion queue `hyper`
//! drains, on the one driving task. The client needs no handle: it registers its
//! destination buffer up front ([`AsyncHordStream::register_remote_writable`])
//! and reads it after the response. The `X-HORD-RDMA-Write` HTTP semantics live
//! in `hord-zerocopy`.
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
use std::task::{Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use hord_stream::{Connection, HordConfig, HordStream, RegisteredBuffer};

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
    pub fn from_accepted(
        conn: Connection,
        peer_bytes: Vec<u8>,
        config: &HordConfig,
    ) -> io::Result<Self> {
        Self::wrap(HordStream::from_accepted(conn, peer_bytes, config)?)
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

    // ---- zero-copy extension (spec §7) -------------------------------------

    /// Whether the zero-copy extension was negotiated on this connection.
    /// See [`HordStream::zero_copy_negotiated`].
    pub fn zero_copy_negotiated(&self) -> bool {
        self.stream.zero_copy_negotiated()
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
        if w.post_outcome.is_none() {
            w.post_outcome = Some(self.stream.begin_rdma_write(
                w.src, w.src_off, w.peer_addr, w.peer_rkey, w.len,
            ));
        }
        // Drain EVERY write that posted before resolving — even on a post error or
        // a closed stream. Otherwise an error return would let the caller drop
        // `src` (deregistering its MR + freeing the storage) while the NIC is
        // still DMA-reading it from an outstanding write (use-after-free).
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

    /// Best-effort, non-blocking half-close check via the CM fd. Returns `true`
    /// if it just marked the stream closed.
    fn poll_cm_disconnect(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        let Some(cm) = self.cm.as_ref() else {
            return Ok(false);
        };
        match cm.poll_read_ready(cx) {
            Poll::Ready(Ok(mut guard)) => {
                let disconnected = self.stream.check_disconnect()?;
                guard.clear_ready();
                if disconnected {
                    self.stream.mark_closed();
                }
                Ok(disconnected)
            }
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => Ok(false),
        }
    }

    /// Drive the completion (and CM) fds. `Ready(())` means the state advanced —
    /// completions were drained or the peer closed — so the caller should retry
    /// its `try_*` operation; `Pending` means the task is parked on the reactor.
    ///
    /// The arm/drain ordering closes the classic completion-channel race: we arm
    /// the CQ and then drain again, so a completion that lands between the first
    /// drain and the arm is still seen (it is now in the CQ), and any later one
    /// triggers a notification on the armed channel.
    fn poll_events(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // 1. Anything already sitting in the CQ.
        if self.stream.drain_completions()? > 0 {
            return Poll::Ready(Ok(()));
        }
        // 2. Peer-initiated half-close.
        if self.poll_cm_disconnect(cx)? {
            return Poll::Ready(Ok(()));
        }
        // 3. Arm, then drain again to close the arm race.
        self.stream.arm_cq()?;
        if self.stream.drain_completions()? > 0 {
            return Poll::Ready(Ok(()));
        }
        // 4. Park on the completion-channel fd.
        loop {
            let mut guard = std::task::ready!(self.cq.poll_read_ready(cx))?;
            // The fd signalled: consume the notification (drains the fd so it is
            // no longer readable), re-arm for the next one, and drain the CQ.
            self.stream.consume_cq_events();
            self.stream.arm_cq()?;
            let drained = self.stream.drain_completions()?;
            guard.clear_ready();
            if drained > 0 {
                return Poll::Ready(Ok(()));
            }
            // The notification carried no work we hadn't already drained (or was
            // spurious). We re-armed and cleared readiness, so loop to re-park.
        }
    }
}

impl AsyncRead for AsyncHordStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.stream.try_read(buf.initialize_unfilled())? {
                // EOF (or the caller passed a full buffer): no bytes, Ready.
                Some(0) => return Poll::Ready(Ok(())),
                Some(n) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                None => {} // no data buffered yet — wait for a completion
            }
            // Return owed credits so the peer can keep sending, then park.
            this.stream.return_owed_credits(true)?;
            match this.poll_events(cx)? {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for AsyncHordStream {
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
            // try_write errors if the stream is closed, accepts >0 bytes if it
            // can make progress, or returns 0 when blocked on a slot/credit.
            let n = this.stream.try_write(buf)?;
            if n > 0 {
                return Poll::Ready(Ok(n));
            }
            match this.poll_events(cx)? {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            // Emit any staged partial message, then wait for every data send
            // *and* one-sided RDMA write to be acknowledged (an RC completion ==
            // delivered + acked) — flush is a full delivery barrier.
            let stage_clear = this.stream.try_flush_stage()?;
            if stage_clear && !this.stream.sends_outstanding() && !this.stream.writes_pending() {
                return Poll::Ready(Ok(()));
            }
            if this.stream.is_closed() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "connection closed before all sends were acknowledged",
                )));
            }
            match this.poll_events(cx)? {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Deliver everything, then begin a graceful disconnect.
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
/// call's result, which is reported only *after* every posted WR has been
/// drained (so a post error can't drop the source buffer with DMA outstanding).
struct PendingWrite<'a> {
    src: &'a RegisteredBuffer,
    src_off: usize,
    peer_addr: u64,
    peer_rkey: u32,
    len: usize,
    post_outcome: Option<io::Result<()>>,
}

/// A clonable handle to an [`AsyncHordStream`], so a `hyper` *server* can drive a
/// zero-copy RDMA write from inside its request handler while `hyper` owns the
/// stream for HTTP. (`hyper`'s `service_fn` never receives the connection, and
/// the RDMA write shares the one completion queue `hyper` drains — so the write
/// must be driven by the same object, on the same task.)
///
/// Both `hyper` (via [`tokio::io::AsyncRead`]/[`AsyncWrite`]) and the handler (via
/// [`rdma_write`](Self::rdma_write)) reach the stream through this handle, which
/// `borrow_mut`s the shared cell **only for the duration of each poll — never
/// across an await**. That keeps the two borrows *sequential*, which is the whole
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

    /// Register a source buffer to RDMA-write from (server side).
    pub fn register_source(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.0.borrow().register_source(len)
    }

    /// Register a destination buffer the peer may RDMA-write into (client side).
    pub fn register_remote_writable(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.0.borrow().register_remote_writable(len)
    }

    /// Best-effort graceful disconnect.
    pub fn disconnect(&self) {
        self.0.borrow().disconnect();
    }

    /// RDMA-write `src[src_off .. src_off+len]` into the peer's `[peer_addr,
    /// peer_rkey]`, awaiting completion. The body is delivered out-of-band; the
    /// caller then sends an HTTP response with `Content-Length: 0`. Borrows the
    /// shared stream afresh on each poll (no borrow held across an await).
    pub async fn rdma_write(
        &self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        let mut w = PendingWrite {
            src,
            src_off,
            peer_addr,
            peer_rkey,
            len,
            post_outcome: None,
        };
        std::future::poll_fn(|cx| self.0.borrow_mut().poll_rdma_write(cx, &mut w)).await
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
