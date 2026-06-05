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
//! ## Timeouts
//!
//! Per-operation deadlines are applied the idiomatic tokio way — wrap a call in
//! [`tokio::time::timeout`] — rather than baked into the stream. Combined with
//! the now-tunable CM retry params (`hord_stream::HordConfig::cm`), a
//! stalled-but-alive peer no longer hangs a reader forever (review item #11). A
//! cancelled (timed-out) read/write future drops cleanly: already-reaped
//! completions stay reaped and no slot is leaked.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use hord_stream::{Connection, HordConfig, HordStream};

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
            // Emit any staged partial message, then wait for every data send to
            // be acknowledged (an RC send completion == delivered + acked).
            let stage_clear = this.stream.try_flush_stage()?;
            if stage_clear && !this.stream.sends_outstanding() {
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
