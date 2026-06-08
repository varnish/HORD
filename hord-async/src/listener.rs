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
//! `FnMut(`[`AsyncHordStream`]`, Option<SocketAddr>, watch::Receiver<bool>) -> impl
//! Future` — a `!Send`-friendly `serve_conn`. The second argument is the peer's
//! address as resolved by the CM (`None` if it could not resolve one — see the
//! trust-model note below). The closure (and the futures it returns) never leave the worker
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
//!     .serve(shutdown, |stream, peer, _shutdown| async move {
//!         // Drive `stream` (an `AsyncHordStream`: AsyncRead + AsyncWrite) here —
//!         // e.g. `hyper::server::conn::http1::Builder::new().serve_connection(...)`.
//!         // Wrap it in a `SharedAsyncStream` first if the handler needs to reach
//!         // the connection for a zero-copy RDMA write. `_shutdown` is a clone of the
//!         // listener's shutdown signal — watch it to wind this connection down
//!         // promptly on shutdown (drive your HTTP layer's graceful shutdown).
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
//! elapses. To wind such connections down promptly the service closure is handed,
//! as its **third argument**, a clone of the same shutdown
//! [`watch::Receiver<bool>`](tokio::sync::watch::Receiver): watch it and drive your
//! HTTP layer's graceful shutdown when it fires (e.g. call
//! `hyper::server::conn::http1::Connection::graceful_shutdown`, or use
//! `hyper_util`'s `GracefulShutdown`) — exactly as the host would on a `TcpStream`.
//! HORD's grace timeout is the backstop, not the mechanism: a closure that ignores
//! the signal still works, but its idle keep-alive connections pay the full grace
//! timeout at shutdown (the demo server wires this — see `server_async.rs`).
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
//!
//! ## Trust model
//!
//! RDMA queue pairs carry no transport authentication or encryption — there is no
//! TLS handshake and no per-message integrity beyond what the fabric provides.
//! HORD therefore assumes a **last-hop trusted fabric**: the RoCEv2 / InfiniBand
//! network between peers is trusted — typically a dedicated, isolated fabric, or
//! one secured at L2/L3 (e.g. the deployment firewalls RoCEv2's UDP/4791 off any
//! untrusted ingress, since RoCEv2 authenticates nothing on its own).
//!
//! The only peer identity HORD can attest is the peer's source address — the
//! `Option<SocketAddr>` handed to the service closure (and
//! [`AsyncHordStream::peer_addr`] / `ConnMeta`) — which for
//! RoCEv2 is the peer's GID in address form. A host MAY use it as a tenant key
//! (cache-key namespace, a VSL / Prometheus `tenant` label, PURGE authority), but
//! it is only as trustworthy as the fabric: there is no cryptographic binding, so
//! a peer able to place packets on the fabric could spoof a source GID. A
//! multi-tenant host that needs isolation stronger than the fabric provides MUST
//! enforce it below HORD (separate fabrics / RoCEv2 VLANs / InfiniBand partitions
//! per tenant); HORD does not authenticate peers. See SPEC §11.4.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::{Id, JoinError, JoinSet};

use hord_stream::{
    is_connection_setup_failure, is_device_removed, ConnTeardown, Connection, HordConfig,
    HordStream, Listener,
};

// `ReactorFd` (crate root) is the shared "raw fd owned elsewhere; drop deregisters
// but does not close" AsyncFd wrapper — reused here for the listener's CM fd.
use crate::{AsyncHordStream, ReactorFd};

/// Default per-connection-drain bound on shutdown. Matches the 30 s the Carapace
/// direct service already allows its `GracefulShutdown`, so the two layers' bounds
/// line up rather than one cutting the other short.
const DEFAULT_GRACE: Duration = Duration::from_secs(30);

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

/// Per-worker bound on accepted-but-not-yet-handshaked connections queued to one
/// worker. Caps the live QP/CM-id backlog a single *wedged* worker (e.g. parked in
/// its bounded handshake/establish wait, or monopolized by a long zero-copy write)
/// can accumulate before the acceptor fails new connections over to other workers
/// (see [`dispatch`]). Small on purpose: the handshake is normally sub-millisecond,
/// so the queue should rarely hold more than a couple — the bound exists for the
/// wedged / overloaded case, not the steady state.
const WORKER_CHANNEL_CAP: usize = 32;

/// One accepted connection plus the peer address to label it with (or `None` if the
/// CM could not resolve one), handed from the acceptor to a worker.
type Accepted = (Connection, Option<SocketAddr>);

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
    /// handshaked [`AsyncHordStream`], the peer's address as resolved by the CM
    /// (`None` if it could not resolve one — see the [trust-model note](self#trust-model)),
    /// and a clone of the shutdown [`watch::Receiver<bool>`](watch::Receiver) so the connection can
    /// wind itself down promptly on shutdown (see the [module docs](self) on the
    /// per-connection drain — without it an idle keep-alive connection pays the full
    /// `grace_timeout`). The future it returns is `spawn_local`d on that worker.
    /// Because everything runs on one worker thread, neither the future nor anything
    /// it captures need be `Send`; the closure itself is `Send + Clone` only so each
    /// worker gets its own copy.
    ///
    /// Connections whose handshake fails are logged and dropped — `serve_fn` is
    /// only called for a successfully established stream.
    ///
    /// Must be `.await`ed from within a Tokio runtime (it bridges the worker
    /// threads' completion to async via `spawn_blocking`).
    pub async fn serve<F, Fut>(self, shutdown: watch::Receiver<bool>, serve_fn: F)
    where
        F: FnMut(AsyncHordStream, Option<SocketAddr>, watch::Receiver<bool>) -> Fut + Clone + Send + 'static,
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
            let (tx, rx) = mpsc::channel::<Accepted>(WORKER_CHANNEL_CAP);
            senders.push(tx);
            let serve_fn = serve_fn.clone();
            let config = config.clone();
            // Each worker gets its own clone of the shutdown signal to hand to every
            // connection it serves, so a connection can drive its own graceful drain.
            let shutdown = shutdown.clone();
            worker_handles.push(std::thread::spawn(move || {
                run_worker(id, rx, serve_fn, config, grace, shutdown);
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
    senders: Vec<mpsc::Sender<Accepted>>,
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
        // Honour a shutdown that is already set — including one set *before*
        // serve() started: `watch::changed()` only fires on a *change* from the
        // value present at receiver creation, so an already-true signal would
        // otherwise be ignored until a later toggle. A plain `borrow()` doesn't
        // advance the seen-version, so the `changed()` arm below still works.
        if *shutdown.borrow() {
            break 'accept;
        }

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
                // Events handled this wakeup — accepts *and* rejected-bad-peer
                // skips both count, so a flood of either kind yields to the
                // `select!` (and the biased shutdown branch) at the per-wakeup cap.
                let mut handled = 0u32;
                // Set by a terminal listener error (device removal) so the post-loop
                // logic stops the acceptor immediately rather than backing off.
                let mut fatal = false;
                loop {
                    match HordStream::try_accept_begin(&listener, &config) {
                        Ok(Some(conn)) => {
                            // A real accept is the only evidence the listener is
                            // healthy, so it is the only thing that clears the
                            // error run. (`Ok(None)`/empty is *not* recovery — see
                            // below — or a removed device would reset the counter
                            // and the cap could never trip.)
                            consecutive_errors = 0;
                            // Surface the CM's answer faithfully: `None` means
                            // "unresolved", distinct from any real address (the
                            // service decides how to treat an anonymous peer).
                            let peer = conn.peer_addr();
                            dispatch(&senders, &mut next, conn, peer);
                            handled += 1;
                            if handled >= MAX_DRAIN_PER_WAKEUP {
                                // Yield to the `select!` so a flood can't starve
                                // shutdown; the fd stays readable, so we resume.
                                break;
                            }
                        }
                        Ok(None) => {
                            // Channel drained to empty. Do NOT reset
                            // `consecutive_errors`: an empty poll is not proof of
                            // health (a removed device also reads empty after its
                            // one event is acked), so resetting here would let a
                            // persistent error evade MAX_CONSECUTIVE_ACCEPT_ERRORS.
                            drained = true;
                            break;
                        }
                        Err(e) if is_device_removed(&e) => {
                            // Terminal: the device is gone, no further CM events
                            // will arrive. Stop now instead of backing off forever
                            // (the blocking `Listener::accept` surfaces this too).
                            log::error!("hord: {e}; stopping acceptor");
                            fatal = true;
                            break;
                        }
                        Err(e) if is_connection_setup_failure(&e) => {
                            // One peer's setup failed; `hord-core` already rejected
                            // it. This is NOT a listener fault, so it must not touch
                            // `consecutive_errors` or trigger the back-off — just
                            // log, count it toward the per-wakeup cap, and keep
                            // draining the rest of this wakeup's queue (the next
                            // event, if any). The failed event is consumed, so the
                            // loop advances rather than re-seeing it.
                            log::warn!("hord: rejected a peer whose connection setup failed: {e}");
                            handled += 1;
                            if handled >= MAX_DRAIN_PER_WAKEUP {
                                break;
                            }
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
                if fatal {
                    break 'accept;
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

/// Render an optional peer address for a log line: the resolved address, or
/// `<unknown>` when the CM could not report one. Allocation-free in the common case.
fn peer_label(peer: Option<SocketAddr>) -> impl std::fmt::Display {
    struct L(Option<SocketAddr>);
    impl std::fmt::Display for L {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                Some(a) => write!(f, "{a}"),
                None => f.write_str("<unknown>"),
            }
        }
    }
    L(peer)
}

/// Round-robin one connection to a live, non-full worker. A worker whose bounded
/// queue is *full* (wedged or backlogged) is skipped just like one whose thread has
/// *exited* (closed receiver), so connections fail over to the next available worker
/// instead of piling unboundedly behind a stuck one. If every worker is full or gone
/// the connection is dropped (its `Drop` issues a graceful disconnect).
fn dispatch(
    senders: &[mpsc::Sender<Accepted>],
    next: &mut usize,
    conn: Connection,
    peer: Option<SocketAddr>,
) {
    let n = senders.len();
    let mut payload = (conn, peer);
    // `try_send` (not the async `send`) so the single accept loop never blocks on a
    // backlogged worker — blocking here would also stall the shutdown check.
    for _ in 0..n {
        let w = *next;
        *next = (*next + 1) % n;
        match senders[w].try_send(payload) {
            Ok(()) => return,
            // Full (wedged/backlogged) or Closed (worker gone): both hand the
            // payload back so it can be re-offered to the next worker.
            Err(TrySendError::Full(p) | TrySendError::Closed(p)) => payload = p,
        }
    }
    // Every worker is full or gone: shed this connection rather than block the
    // accept loop — back-pressure on accept under sustained overload. Each queued
    // item holds a live QP/CM id, so the bounded queues are what keep that backlog
    // finite; dropping here is the overflow valve.
    log::warn!(
        "hord: all workers full or unavailable; dropping connection from {}",
        peer_label(payload.1)
    );
}

/// One worker thread: a current-thread runtime + `LocalSet` that builds and drives
/// each accepted connection's `!Send` stream via `spawn_local`, then drains
/// in-flight connections (bounded by `grace`) once the acceptor closes its channel.
fn run_worker<F, Fut>(
    id: usize,
    rx: mpsc::Receiver<Accepted>,
    serve_fn: F,
    config: HordConfig,
    grace: Duration,
    shutdown: watch::Receiver<bool>,
) where
    F: FnMut(AsyncHordStream, Option<SocketAddr>, watch::Receiver<bool>) -> Fut,
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
    rt.block_on(local.run_until(worker_loop(rx, serve_fn, config, grace, shutdown)));
}

/// The worker's async body. Spawns one local task per connection; on channel close
/// (shutdown) it stops accepting handoffs and waits for the in-flight tasks to
/// finish, bounded by `grace`. Driven inside `LocalSet::run_until`, so the spawned
/// tasks make progress while this loop awaits them — the drain wall-clock is the
/// slowest connection, not the sum.
///
/// ## Why the per-task teardown map
///
/// At the grace deadline the worker must abandon any still-running task (the
/// `tasks` `JoinSet` is dropped, aborting them). A task parked inside an
/// `RDMA_WRITE` — the documented back-pressure case, a slow/non-reading peer —
/// holds a posted-but-unreaped write WR against a source buffer it also owns.
/// Abort drops that future, freeing the source buffer (deregistering its MR and
/// releasing its storage) **while the QP can still DMA-read it** — a
/// use-after-free / torn delivery. The normal write driver drains every posted
/// WR before returning precisely to prevent this; task abort bypasses it. So the
/// worker keeps a [`ConnTeardown`] per live task and, *before* dropping the
/// `JoinSet`, forces every in-flight connection's QP down — quiescing the NIC so
/// the aborted futures' buffer frees are sound.
async fn worker_loop<F, Fut>(
    mut rx: mpsc::Receiver<Accepted>,
    mut serve_fn: F,
    config: HordConfig,
    grace: Duration,
    shutdown: watch::Receiver<bool>,
) where
    F: FnMut(AsyncHordStream, Option<SocketAddr>, watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    // A JoinSet reaps finished connection tasks in O(1) per task (no per-accept
    // scan of a handle Vec) and surfaces a handler panic instead of swallowing it.
    let mut tasks: JoinSet<()> = JoinSet::new();
    // One QP-teardown handle per live task, keyed by task id. Pruned as tasks
    // finish (so it never pins a dead connection's resources); drained on a
    // grace-timeout abort to quiesce the NIC first (see the fn docs above).
    let mut teardowns: HashMap<Id, ConnTeardown> = HashMap::new();

    while let Some((conn, peer)) = rx.recv().await {
        // Reap any connection tasks that have finished since the last accept.
        reap_finished(&mut tasks, &mut teardowns);
        // Build the !Send stream on this worker thread — the handshake runs here
        // (see the type's "synchronous handshake" caveat). On failure, drop the
        // connection and keep serving the rest.
        match AsyncHordStream::from_accepted(conn, &config) {
            Ok(stream) => {
                // Grab the teardown handle *before* moving the stream into the
                // service future, and key it by the spawned task's id. Each
                // connection gets its own clone of the shutdown signal so it can
                // drive a per-connection graceful drain (see the module docs).
                let teardown = stream.teardown_handle();
                // Hand the service the *post-handshake* peer address, not the
                // accept-time `peer` read off the not-yet-established `conn`. The CM
                // may only resolve the destination address at establishment, so the
                // stream's value is the authoritative one and matches
                // `conn_meta().peer_addr` exactly; on RoCEv2 the two are identical.
                // (`peer` is still used below for the failure log, where no stream
                // exists to query.)
                let peer = stream.peer_addr();
                let handle = tasks.spawn_local(serve_fn(stream, peer, shutdown.clone()));
                teardowns.insert(handle.id(), teardown);
            }
            Err(e) => log::warn!("hord: handshake failed for {}: {e}", peer_label(peer)),
        }
    }

    // Channel closed → shutdown. Let in-flight connections finish, bounded by
    // `grace`; they run concurrently on this LocalSet (driven by the enclosing
    // `run_until`), so the wall-clock is the slowest one, not the sum. Pruning
    // the map as tasks complete keeps it tracking only still-running connections.
    let drain = async {
        while let Some(res) = tasks.join_next_with_id().await {
            reap_one(res, &mut teardowns);
        }
    };
    if tokio::time::timeout(grace, drain).await.is_err() {
        log::warn!("hord: graceful drain timed out after {grace:?}; abandoning in-flight connections");
        // The drain timed out and `tasks` is about to be dropped, aborting every
        // survivor. Force each in-flight connection's QP down FIRST so the NIC is
        // quiescent before the aborted futures free the source buffers their
        // outstanding writes reference (the use-after-free this map exists to
        // close). QP teardown is idempotent, so any entry whose task finished
        // during the drain (but wasn't pruned) is a harmless no-op.
        for teardown in teardowns.values() {
            teardown.force_teardown();
        }
    }
    // `tasks` drops here, aborting any survivors — now sound: their QPs are gone,
    // so freeing their buffers cannot race the NIC.
}

/// Drain every connection task that has already finished, pruning its teardown
/// handle. Non-blocking: stops at the first task still running.
fn reap_finished(tasks: &mut JoinSet<()>, teardowns: &mut HashMap<Id, ConnTeardown>) {
    while let Some(res) = tasks.try_join_next_with_id() {
        reap_one(res, teardowns);
    }
}

/// Handle one finished task's join result: drop its teardown handle (the
/// connection is gone, so the NIC no longer references its buffers) and surface a
/// panic, which a bare `let _ = ...` would have swallowed. A cancellation (abort
/// at the grace deadline) is expected and silent.
fn reap_one(res: Result<(Id, ()), JoinError>, teardowns: &mut HashMap<Id, ConnTeardown>) {
    match res {
        Ok((id, ())) => {
            teardowns.remove(&id);
        }
        Err(e) => {
            teardowns.remove(&e.id());
            if e.is_panic() {
                log::error!("hord: connection task panicked: {e}");
            }
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
