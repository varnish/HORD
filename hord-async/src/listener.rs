//! [`HordListener`]: the server runtime topology for embedding HORD under a
//! work-stealing host runtime — the Carapace integration's "Blocker 0".
//!
//! ## The problem it solves
//!
//! [`AsyncHordStream`] is `!Send` by construction: its registered buffers hold
//! raw pointers and the completion queue is polled from the thread that built it
//! (see the crate-root docs). A host that accepts on a multi-threaded runtime and
//! drives each connection with `tokio::spawn` therefore **cannot** hand a HORD
//! stream to its per-connection service — `tokio::spawn` requires `Send`. The
//! thread-affinity is intrinsic (it is how the CQ is driven), not an accident, so
//! the fix cannot live in the host: HORD must own the runtime topology.
//!
//! ## The topology
//!
//! `HordListener` owns it end to end — **thread-per-core**:
//!
//! - One **acceptor** thread runs a current-thread runtime, parks on the
//!   listener's CM-channel fd, and round-robins each accepted (still-`Send`)
//!   [`Connection`](hord_stream::Connection) to a worker over a channel.
//! - **N worker** threads (one per core by default), each a current-thread
//!   runtime + [`tokio::task::LocalSet`] — its own completion domain. A worker
//!   builds the `!Send` [`AsyncHordStream`] on its own thread (so the stream never
//!   crosses a thread) and `spawn_local`s the host's service future for it. One
//!   worker thus drives *many* connections concurrently on one core, each parked
//!   on its own CQ fd via that runtime's reactor (the 1:1 model; the N:1
//!   completion-channel demux is a later fd-economy optimization, not needed here).
//!
//! The host supplies a per-connection service as a closure
//! `FnMut(`[`AsyncHordStream`]`, SocketAddr) -> impl Future` — a `!Send`-friendly
//! `serve_conn`. The closure (and the futures it returns) never leave the worker
//! thread, so nothing it touches need be `Send`. The closure *itself* must be
//! `Send + Clone` only so a copy can be handed to each worker thread.
//!
//! ```no_run
//! # async fn doc() {
//! use hord_async::HordListener;
//! use hord_stream::HordConfig;
//!
//! let (_tx, shutdown) = tokio::sync::watch::channel(false);
//! let listener = HordListener::bind("0.0.0.0", 4791, HordConfig::default())
//!     .expect("bind");
//! listener
//!     .serve(shutdown, |stream, peer| async move {
//!         // Drive `stream` (an `AsyncHordStream`: AsyncRead + AsyncWrite) here —
//!         // e.g. `hyper::server::conn::http1::Builder::new().serve_connection(...)`.
//!         // Wrap it in a `SharedAsyncStream` first if the handler needs to reach
//!         // the connection for a zero-copy RDMA write.
//!         let _ = (stream, peer);
//!     })
//!     .await;
//! # }
//! ```
//!
//! ## Graceful shutdown
//!
//! [`serve`](HordListener::serve) takes a [`tokio::sync::watch::Receiver<bool>`]
//! (pingora's `ShutdownWatch` *is* one, so it can be passed straight through; no
//! coupling to pingora types). When the value flips to `true` — or the sender is
//! dropped — the acceptor stops accepting, the worker channels close, and each
//! worker lets its in-flight connection tasks finish before exiting, bounded by
//! [`grace_timeout`](HordListener::grace_timeout). `serve` resolves once every
//! worker has drained, so a host `async fn` can `.await` it as the end of its
//! own shutdown sequence.
//!
//! HORD owns *stopping the accept loop and bounding the drain*. The host owns its
//! own **per-connection** graceful drain: a keep-alive connection sitting idle
//! between requests is blocked inside the service future (waiting for the next
//! request) and will not return until the client closes or the grace timeout
//! elapses. To wind such connections down promptly, capture your own clone of the
//! shutdown receiver in the service closure and drive your HTTP layer's graceful
//! shutdown with it (e.g. `hyper_util`'s `GracefulShutdown`) — exactly as the
//! host would on a `TcpStream`. HORD's grace timeout is the backstop, not the
//! mechanism.
//!
//! ## Properties the byte-stream parity relies on (Milestone 1)
//!
//! Two properties of [`AsyncHordStream`] make `hyper` keep-alive and the host's
//! body backpressure behave exactly as on TCP; both are exercised by
//! `tests/listener.rs`:
//!
//! - **Keep-alive EOF.** [`AsyncRead`](tokio::io::AsyncRead) returns `Ok(0)` only
//!   on a *real* peer half-close — never between pipelined requests on one QP — so
//!   `hyper`'s keep-alive loop serves N requests over one connection and a host's
//!   promote-on-clean-EOF logic sees EOF only when the peer actually closed.
//! - **`poll_write` backpressure.** When the credit window is exhausted,
//!   [`poll_write`](tokio::io::AsyncWrite::poll_write) returns `Poll::Pending`
//!   (it parks on the completion fd) rather than buffering unbounded and returning
//!   `Ready`. A slow RDMA reader therefore back-pressures the writer, so a host
//!   streaming a cache body through a bounded channel cannot be made to pull an
//!   arbitrarily large object fully into RAM.
//!
//! ## Caveat: synchronous handshake on the worker
//!
//! A worker completes the HORD handshake synchronously inside
//! [`AsyncHordStream::from_accepted`] (a brief CQ busy-poll for one round trip),
//! which momentarily pins that worker — and so its other connections — until it
//! returns. Acceptable here because the handshake is one fast exchange; a
//! latency-sensitive deployment would move it off the worker (a dedicated
//! handshake stage, or an async handshake) so a slow-handshaking peer cannot
//! stall a worker's other connections. The seam to do so is local to the worker
//! loop.

use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::sync::{mpsc, watch};

use hord_stream::{Connection, HordConfig, HordStream, Listener};

// `ReactorFd` (crate root) is the shared "raw fd owned elsewhere; drop deregisters
// but does not close" AsyncFd wrapper — reused here for the listener's CM fd.
use crate::{AsyncHordStream, ReactorFd};

/// Default per-connection-drain bound on shutdown. Matches the 30 s the Carapace
/// direct service already allows its `GracefulShutdown`, so the two layers' bounds
/// line up rather than one cutting the other short.
const DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// Placeholder peer address when the CM cannot report one (e.g. an address family
/// the wrapper does not map). The service still runs; the peer is just `0.0.0.0:0`.
const UNKNOWN_PEER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

/// Acceptor back-off after a failed accept, so a *persistent* error (e.g. a removed
/// device that keeps the CM fd erroring) cannot hot-spin the accept loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// After this many *consecutive* accept failures the acceptor gives up and winds
/// down rather than retry a permanently-broken listener forever. A run is reset by
/// any successful accept or a clean "queue empty".
const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 50;

/// Max connections the acceptor dispatches per fd wakeup before yielding back to
/// the `select!`, so a sustained connection flood can't starve the (biased)
/// shutdown branch. The fd stays readable, so draining resumes on the next wakeup.
const MAX_DRAIN_PER_WAKEUP: u32 = 64;

/// One accepted connection plus the peer address to label it with, handed from the
/// acceptor to a worker.
type Accepted = (Connection, SocketAddr);

/// Fires an internal stop signal when dropped. [`HordListener::serve`] holds one
/// across its join `.await`, so if the `serve()` future is *cancelled* (dropped
/// before it resolves — e.g. a host races it in a `select!` and another branch
/// wins) the acceptor is still told to wind down, instead of the acceptor + worker
/// threads (and the listener) leaking with nothing to stop them.
struct StopOnCancel(watch::Sender<bool>);

impl Drop for StopOnCancel {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// A HORD server that owns its runtime topology (a thread-per-core worker pool and
/// an accept loop) and runs a host-supplied, `!Send`-friendly per-connection
/// service. See the [module docs](self) for the model and the rationale.
pub struct HordListener {
    listener: Listener,
    config: HordConfig,
    workers: usize,
    grace: Duration,
}

impl HordListener {
    /// Bind to `ip:port` and prepare the topology (no threads are started until
    /// [`serve`](Self::serve)). Defaults to one worker per available core and a
    /// 30 s shutdown-drain bound; override with [`workers`](Self::workers) /
    /// [`grace_timeout`](Self::grace_timeout).
    pub fn bind(ip: &str, port: u16, config: HordConfig) -> io::Result<Self> {
        Ok(HordListener {
            listener: Listener::bind(ip, port)?,
            config,
            workers: default_workers(),
            grace: DEFAULT_GRACE,
        })
    }

    /// Set the number of worker threads (clamped to at least 1). Default: the
    /// host's available parallelism (thread-per-core). Note this is a thread
    /// *count*; HORD does not pin threads to cores.
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers.max(1);
        self
    }

    /// Set the upper bound on the per-worker in-flight drain at shutdown. After it
    /// elapses, a worker abandons any still-running connection tasks and exits, so
    /// a hung client cannot block shutdown forever. Default: 30 s.
    pub fn grace_timeout(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Run the server until `shutdown` fires (its value flips to `true`, or its
    /// sender is dropped), then drain in-flight connections and resolve.
    ///
    /// `serve_fn` is the per-connection service: called once per accepted
    /// connection, on the worker thread that will drive it, with the freshly
    /// handshaked [`AsyncHordStream`] and the peer's address. The future it returns
    /// is `spawn_local`d on that worker. Because everything runs on one worker
    /// thread, neither the future nor anything it captures need be `Send`; the
    /// closure itself is `Send + Clone` only so each worker gets its own copy.
    ///
    /// Connections whose handshake fails are logged and dropped — `serve_fn` is
    /// only called for a successfully established stream.
    ///
    /// Must be `.await`ed from within a Tokio runtime (it bridges the worker
    /// threads' completion to async via `spawn_blocking`).
    pub async fn serve<F, Fut>(self, shutdown: watch::Receiver<bool>, serve_fn: F)
    where
        F: FnMut(AsyncHordStream, SocketAddr) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let HordListener {
            listener,
            config,
            workers,
            grace,
        } = self;

        // Spawn the worker pool: one current-thread runtime + LocalSet per worker,
        // each owning the receive end of its handoff channel.
        let mut senders = Vec::with_capacity(workers);
        let mut worker_handles = Vec::with_capacity(workers);
        for id in 0..workers {
            let (tx, rx) = mpsc::unbounded_channel::<Accepted>();
            senders.push(tx);
            let serve_fn = serve_fn.clone();
            let config = config.clone();
            worker_handles.push(std::thread::spawn(move || {
                run_worker(id, rx, serve_fn, config, grace);
            }));
        }

        // Internal stop signal: the acceptor watches it alongside the caller's
        // shutdown, and `StopOnCancel` fires it if *this* future is dropped — so a
        // cancelled serve() still tears the threads down instead of leaking them.
        let (stop_tx, stop_rx) = watch::channel(false);

        // The acceptor owns the listener + the senders; dropping the senders when
        // it stops closes the worker channels, which begins their drain.
        let acc_config = config;
        let acceptor = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("hord: acceptor runtime build failed: {e}");
                    return;
                }
            };
            if let Err(e) =
                rt.block_on(acceptor_loop(listener, senders, shutdown, stop_rx, acc_config))
            {
                log::error!("hord: acceptor loop error: {e}");
            }
        });

        // Bridge the std-thread joins into async so a host `async fn` (e.g.
        // Carapace's `start_service`) can `.await` the full shutdown drain.
        // spawn_blocking runs on the runtime's blocking pool, so parking there for
        // the service lifetime is exactly what that pool is for. The drop guard is
        // declared *before* the await so a cancelled serve() future fires the stop
        // (winding the threads down) on the way out.
        let _stop_on_cancel = StopOnCancel(stop_tx);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = acceptor.join();
            for h in worker_handles {
                let _ = h.join();
            }
        })
        .await;
    }
}

/// The acceptor: park on the listener's CM-channel fd, drain every pending
/// connection request to a worker, and stop when `shutdown` fires. Owns the
/// listener and the worker senders so that returning (and dropping them) closes
/// the worker channels.
async fn acceptor_loop(
    listener: Listener,
    senders: Vec<mpsc::UnboundedSender<Accepted>>,
    mut shutdown: watch::Receiver<bool>,
    mut stop: watch::Receiver<bool>,
    config: HordConfig,
) -> io::Result<()> {
    // Non-blocking + fd-driven so the shutdown signal can interrupt accepting
    // instead of us blocking inside the CM channel waiting for the next peer.
    listener.set_nonblocking(true)?;
    let fd = AsyncFd::new(ReactorFd(listener.cm_fd()))?;
    let mut next = 0usize;
    // Consecutive accept failures; a sustained run terminates the acceptor (see
    // MAX_CONSECUTIVE_ACCEPT_ERRORS) instead of spinning on a broken listener.
    let mut consecutive_errors: u32 = 0;

    'accept: loop {
        tokio::select! {
            // Shutdown wins over a flood of inbound connections.
            biased;

            res = shutdown.changed() => {
                // `Ok` with a still-false value is a spurious/toggle-back notify —
                // keep serving. A flip to `true`, or a dropped sender (`Err`), means
                // wind down.
                match res {
                    Ok(()) if !*shutdown.borrow() => continue,
                    _ => break 'accept,
                }
            }

            // Internal stop (the serve() future was cancelled). Always wind down.
            _ = stop.changed() => break 'accept,

            guard = fd.readable() => {
                let mut guard = guard?;
                // One notification may cover several queued requests; drain until
                // the channel reports empty (Ok(None)) before re-parking. We only
                // `clear_ready()` on that clean-empty case: clearing after an
                // *error* would discard readiness we never observed as consumed —
                // the AsyncFd footgun that hot-spins on a persistent error.
                let mut drained = false;
                let mut accepted = 0u32;
                loop {
                    match HordStream::try_accept_begin(&listener, &config) {
                        Ok(Some(conn)) => {
                            consecutive_errors = 0;
                            let peer = conn.peer_addr().unwrap_or(UNKNOWN_PEER);
                            dispatch(&senders, &mut next, conn, peer);
                            accepted += 1;
                            if accepted >= MAX_DRAIN_PER_WAKEUP {
                                // Yield to the `select!` so a flood can't starve
                                // shutdown; the fd stays readable, so we resume.
                                break;
                            }
                        }
                        Ok(None) => {
                            consecutive_errors = 0;
                            drained = true;
                            break;
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            log::warn!(
                                "hord: accept error ({consecutive_errors}/{MAX_CONSECUTIVE_ACCEPT_ERRORS}): {e}"
                            );
                            break;
                        }
                    }
                }
                if drained {
                    guard.clear_ready();
                } else if consecutive_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                    // Persistent failure (e.g. device removal keeping the fd
                    // erroring) — stop rather than spin forever.
                    log::error!(
                        "hord: stopping acceptor after {consecutive_errors} consecutive accept errors"
                    );
                    break 'accept;
                } else if consecutive_errors > 0 {
                    // Transient error: the bad event was acked, so the fd drains on
                    // the next attempt (which clears readiness). Back off briefly so
                    // a *persistent* error climbs to the cap without hot-spinning.
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                // else: hit the per-wakeup accept cap with no error — fall through to
                // re-enter the `select!` (biased, so shutdown is re-checked) without
                // clearing readiness, so draining resumes immediately next wakeup.
            }
        }
    }

    // Stop accepting: drop the AsyncFd (deregister from the reactor) before the
    // listener closes its fd, then drop the senders to close the worker channels.
    drop(fd);
    drop(senders);
    drop(listener);
    Ok(())
}

/// Round-robin one connection to a live worker, skipping any whose thread has
/// exited (closed receiver). If every worker is gone the connection is dropped
/// (its `Drop` issues a graceful disconnect).
fn dispatch(
    senders: &[mpsc::UnboundedSender<Accepted>],
    next: &mut usize,
    conn: Connection,
    peer: SocketAddr,
) {
    let n = senders.len();
    let mut payload = (conn, peer);
    for _ in 0..n {
        let w = *next;
        *next = (*next + 1) % n;
        match senders[w].send(payload) {
            Ok(()) => return,
            Err(e) => payload = e.0, // worker gone — try the next
        }
    }
    log::warn!("hord: all workers unavailable; dropping connection from {}", payload.1);
}

/// One worker thread: a current-thread runtime + `LocalSet` that builds and drives
/// each accepted connection's `!Send` stream via `spawn_local`, then drains
/// in-flight connections (bounded by `grace`) once the acceptor closes its channel.
fn run_worker<F, Fut>(
    id: usize,
    rx: mpsc::UnboundedReceiver<Accepted>,
    serve_fn: F,
    config: HordConfig,
    grace: Duration,
) where
    F: FnMut(AsyncHordStream, SocketAddr) -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log::error!("hord: worker {id} runtime build failed: {e}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(worker_loop(rx, serve_fn, config, grace)));
}

/// The worker's async body. Spawns one local task per connection; on channel close
/// (shutdown) it stops accepting handoffs and waits for the in-flight tasks to
/// finish, bounded by `grace`. Driven inside `LocalSet::run_until`, so the spawned
/// tasks make progress while this loop awaits them — the drain wall-clock is the
/// slowest connection, not the sum.
async fn worker_loop<F, Fut>(
    mut rx: mpsc::UnboundedReceiver<Accepted>,
    mut serve_fn: F,
    config: HordConfig,
    grace: Duration,
) where
    F: FnMut(AsyncHordStream, SocketAddr) -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    // A JoinSet reaps finished connection tasks in O(1) per task (no per-accept
    // scan of a handle Vec) and surfaces a handler panic instead of swallowing it.
    let mut tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    while let Some((conn, peer)) = rx.recv().await {
        // Reap any connection tasks that have finished since the last accept.
        while let Some(res) = tasks.try_join_next() {
            log_task_result(res);
        }
        // Build the !Send stream on this worker thread — the handshake runs here
        // (see the type's "synchronous handshake" caveat). On failure, drop the
        // connection and keep serving the rest.
        match AsyncHordStream::from_accepted(conn, &config) {
            Ok(stream) => {
                tasks.spawn_local(serve_fn(stream, peer));
            }
            Err(e) => log::warn!("hord: handshake failed for {peer}: {e}"),
        }
    }

    // Channel closed → shutdown. Let in-flight connections finish, bounded by
    // `grace`; they run concurrently on this LocalSet (driven by the enclosing
    // `run_until`), so the wall-clock is the slowest one, not the sum. On timeout
    // `tasks` drops, aborting anything still running.
    let drain = async {
        while let Some(res) = tasks.join_next().await {
            log_task_result(res);
        }
    };
    if tokio::time::timeout(grace, drain).await.is_err() {
        log::warn!("hord: graceful drain timed out after {grace:?}; abandoning in-flight connections");
    }
}

/// Log a connection task's join result, surfacing a panic (which a bare
/// `let _ = handle.await` would have swallowed) rather than letting it vanish.
/// A cancellation (abort at the grace-timeout / `JoinSet` drop) is expected.
fn log_task_result(res: Result<(), tokio::task::JoinError>) {
    if let Err(e) = res {
        if e.is_panic() {
            log::error!("hord: connection task panicked: {e}");
        }
    }
}

/// Number of worker threads to default to: the host's available parallelism, at
/// least 1.
fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}
