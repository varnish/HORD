//! HORD RDMA transport layer.
//!
//! Safe Rust wrappers over [`sideway`] (which wraps `librdmacm` + `libibverbs`).
//! This crate knows nothing about HTTP or the HORD wire protocol — and, as of
//! the sideway port, nothing about the HORD *handshake* either: it only manages
//! RC queue pairs, memory regions and completions. The HORD envelope, credits,
//! byte-stream **and the connection handshake** live in `hord-stream`, which now
//! exchanges the handshake as the first RDMA message over an established QP (see
//! that crate's `handshake` module). The transport is therefore a pure pipe.
//!
//! Connection setup stays two-phase so the caller can pre-post receive buffers
//! (the handshake recv included) before the QP can carry traffic, avoiding an
//! initial receiver-not-ready (RNR) storm:
//!
//! ```text
//! server: Listener::accept()  -> Connection           (QP in INIT)
//!           register MRs, post_recv * N
//!         Connection::accept_finish()                  (RTR -> RTS -> ESTABLISHED)
//!
//! client: Connection::connect()   -> Connection        (QP in INIT)
//!           register MRs, post_recv * N
//!         Connection::connect_finish()                  (CONNECT -> RTR -> RTS -> established)
//! ```
//!
//! Unlike the previous C-shim implementation, the QP is created by us (via
//! sideway's verbs builder) rather than by `librdmacm`, so we drive the
//! INIT/RTR/RTS transitions ourselves using the attributes the CM computes
//! ([`Identifier::get_qp_attr`]); the CM still supplies every wire parameter.
//!
//! Everything here is synchronous and blocking, which is all the first prototype
//! needs. The completion model is busy-poll ([`Connection::poll`]); an event
//! loop can instead wait on [`Connection::cq_fd`] after [`Connection::arm_cq`].

use std::cell::{Cell, RefCell};
use std::ffi::c_void;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sideway::ibverbs::completion::{
    CompletionChannel, CompletionQueue, ExtendedCompletionQueue, GenericCompletionQueue,
    PollCompletionQueueError,
};
use sideway::ibverbs::memory_region::MemoryRegion;
use sideway::ibverbs::protection_domain::ProtectionDomain;
use sideway::ibverbs::queue_pair::{
    ExtendedQueuePair, PostSendGuard, QueuePair, QueuePairState, SetScatterGatherEntry,
    WorkRequestFlags,
};
use sideway::ibverbs::AccessFlags;
use sideway::rdmacm::communication_manager::{
    ConnectionParameter, Event, EventChannel, EventType, GetEventErrorKind, Identifier, PortSpace,
};

/// IBV_ACCESS_LOCAL_WRITE — the only MR access flag the stream path needs.
///
/// The numeric values intentionally mirror `enum ibv_access_flags`, so callers
/// can keep passing the same constants they did to the old shim.
pub const ACCESS_LOCAL_WRITE: i32 = 1;

/// IBV_ACCESS_REMOTE_WRITE — lets a peer RDMA-write into this MR. Used for the
/// zero-copy extension's client destination buffer. Per IBA, remote write also
/// requires local write, so register such buffers with
/// `ACCESS_LOCAL_WRITE | ACCESS_REMOTE_WRITE`.
pub const ACCESS_REMOTE_WRITE: i32 = 2;

/// Upper bound on the scatter/gather entries HORD packs into one one-sided
/// RDMA-write WR (spec §7, Milestone 3: gathering a fragmented source — e.g. an
/// MSE4 object stored across non-contiguous allocations — into the peer's
/// contiguous buffer). The QP is created with `max_send_sge =
/// min(MAX_WRITE_SGE, device max_sge)` (see [`Connection::max_send_sge`]); a gather
/// list longer than that spans multiple WRs at increasing remote offsets. It also
/// bounds the per-WR stack `ibv_sge` array in [`Connection::post_write_gather`], so
/// it is a compile-time cap, not merely a tunable.
pub const MAX_WRITE_SGE: usize = 16;

/// One local scatter/gather entry for [`Connection::post_write_gather`]: a span
/// `[addr, addr+length)` inside a registered MR named by `lkey`. Mirrors `ibv_sge`
/// (the local address as a `u64`) but is HORD's own type, so the verbs /
/// `rdma-sys` layer never leaks into the public API.
#[derive(Debug, Clone, Copy)]
pub struct Sge {
    /// Local virtual address of the span (must lie within an MR with `lkey`).
    pub addr: u64,
    /// Length of the span in bytes.
    pub length: u32,
    /// Local key of the MR the span lies in.
    pub lkey: u32,
}

/// CM listen backlog. Generous; one HORD server fields many prefetch clients.
const LISTEN_BACKLOG: i32 = 128;

/// Map any sideway / display-able error into an `io::Error`. We render to a
/// string rather than boxing so we don't depend on each sideway error type
/// being `Send + Sync + 'static`.
fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

fn invalid_ip(ip: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("invalid ip address: {ip}"))
}

/// Terminal error carried by the [`io::Error`] that [`Listener::accept`] /
/// [`Listener::try_accept`] return when the RDMA device backing the listener is
/// removed. It is the *inner* error (recognise it with [`is_device_removed`]),
/// so the `Display` text is unchanged from the previous string-only error. A
/// threaded acceptor must treat this as fatal and stop — no further CM events
/// will ever arrive, so retrying would park forever.
#[derive(Debug, Clone, Copy)]
pub struct DeviceRemoved;

impl std::fmt::Display for DeviceRemoved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RDMA device removed while listening for connections")
    }
}

impl std::error::Error for DeviceRemoved {}

/// True if `e` is the terminal listener device-removal error (its inner error is
/// a [`DeviceRemoved`]). An accept loop should stop on this rather than counting
/// it as a transient failure and backing off, since the listener can accept no
/// more connections.
pub fn is_device_removed(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<DeviceRemoved>())
}

/// Marker carried by the [`io::Error`] that [`Listener::accept`] /
/// [`Listener::try_accept`] return when *one* incoming connection could not be set
/// up — its per-connection CM channel, the migrate, the QP, or the INIT transition
/// failed — as opposed to a listener-level fault. The offending peer has already
/// been rejected (`rdma_reject`), so it fails fast instead of waiting out a connect
/// timeout. A threaded acceptor should recognise this (with
/// [`is_connection_setup_failure`]), skip the one connection, and keep accepting,
/// rather than counting a single bad peer against its fatal-error budget. The
/// `Display` text carries the underlying cause.
#[derive(Debug)]
pub struct ConnectionSetupFailed(String);

impl ConnectionSetupFailed {
    fn new(cause: &io::Error) -> Self {
        ConnectionSetupFailed(cause.to_string())
    }
}

impl std::fmt::Display for ConnectionSetupFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incoming connection setup failed (peer rejected): {}", self.0)
    }
}

impl std::error::Error for ConnectionSetupFailed {}

/// True if `e` marks a per-connection setup failure (its inner error is a
/// [`ConnectionSetupFailed`]). A threaded acceptor uses this to skip one bad peer
/// (already rejected) and keep accepting, instead of treating it as a
/// listener-level error and backing off / climbing toward a fatal-error cap.
pub fn is_connection_setup_failure(e: &io::Error) -> bool {
    e.get_ref().is_some_and(|inner| inner.is::<ConnectionSetupFailed>())
}

fn parse_addr(ip: &str, port: u16) -> io::Result<SocketAddr> {
    let addr: IpAddr = ip.parse().map_err(|_| invalid_ip(ip))?;
    Ok(SocketAddr::new(addr, port))
}

/// Translate HORD's `ACCESS_*` bitset into sideway's [`AccessFlags`]. Every HORD
/// caller includes `LOCAL_WRITE`; `REMOTE_WRITE` (which per IBA implies
/// `LOCAL_WRITE`) is added for the zero-copy destination buffer.
fn access_flags(access: i32) -> AccessFlags {
    let mut flags = AccessFlags::LocalWrite;
    if access & ACCESS_REMOTE_WRITE != 0 {
        flags |= AccessFlags::RemoteWrite;
    }
    flags
}

/// Connection-manager retry / timeout parameters (#11).
///
/// # sideway note
/// Only [`resolve_timeout_ms`](Self::resolve_timeout_ms) is currently applied:
/// sideway's `ConnectionParameter` exposes only `qp_number`, so `retry_count` /
/// `rnr_retry_count` cannot be threaded into `rdma_connect` / `rdma_accept` and
/// fall back to sideway's defaults (7 / 7 — the same values the old shim used).
/// The fields are retained so callers (and `CmParams::default`) are unchanged.
#[derive(Debug, Clone, Copy)]
pub struct CmParams {
    /// Transport retry count on connect (initiator side). Valid range 0..=7.
    pub retry_count: u8,
    /// Receiver-not-ready retry count. 7 means infinite RNR retry.
    pub rnr_retry_count: u8,
    /// Timeout (ms) for each of the address/route resolution steps on connect.
    pub resolve_timeout_ms: i32,
}

impl Default for CmParams {
    fn default() -> Self {
        CmParams {
            retry_count: 7,
            rnr_retry_count: 7,
            resolve_timeout_ms: 2000,
        }
    }
}

/// Work-completion opcode, as reported by the NIC. The stream path observes
/// `Send` and `Recv`; the zero-copy extension adds `RdmaWrite` (the sender's
/// completion for a one-sided RDMA write). Protocol splitting (§7.7) adds
/// `RecvRdmaWithImm` — the *receiver's* completion for a write-with-immediate,
/// which consumes a posted recv WR and carries the immediate in
/// [`Completion::imm_data`]. (The sender of a write-with-immediate still reaps
/// an `RdmaWrite` completion.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    Send,
    RdmaWrite,
    Recv,
    RecvRdmaWithImm,
    Other(u32),
}

impl Opcode {
    fn from_raw(v: u32) -> Self {
        match v {
            0 => Opcode::Send,              // IBV_WC_SEND
            1 => Opcode::RdmaWrite,         // IBV_WC_RDMA_WRITE
            128 => Opcode::Recv,            // IBV_WC_RECV (1 << 7)
            129 => Opcode::RecvRdmaWithImm, // IBV_WC_RECV_RDMA_WITH_IMM
            other => Opcode::Other(other),
        }
    }
}

/// A single work completion drained from the CQ.
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    /// Echo of the `wr_id` supplied to `post_send` / `post_recv`.
    pub wr_id: u64,
    /// Bytes received (recv completions) — meaningless for sends.
    pub byte_len: u32,
    pub opcode: Opcode,
    /// Raw `ibv_wc_status`; `0` is `IBV_WC_SUCCESS`.
    pub status: u32,
    /// The 32-bit immediate (host order) on a [`Opcode::RecvRdmaWithImm`]
    /// completion; `0` for every other completion kind.
    pub imm_data: u32,
}

impl Completion {
    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// A heap buffer registered with the NIC for RDMA. Owns its backing storage,
/// the memory-region handle, and a reference to the connection it belongs to.
///
/// This single type closes two soundness holes that a bare MR + `Box<[u8]>`
/// pair left open:
///
/// * **Aliasing.** The NIC DMA-writes some slots while we read/write others in
///   the same allocation. Forming a `&`/`&mut [u8]` over registered memory
///   therefore asserts an exclusivity the NIC violates — UB under the Rust
///   aliasing model. So the storage is `Box<[UnsafeCell<u8>]>` and is *never*
///   sliced as `&[u8]`: every access goes through a raw pointer obtained with
///   [`UnsafeCell::raw_get`] (via [`as_mut_ptr`](Self::as_mut_ptr) or the
///   [`copy_in`](Self::copy_in) / [`copy_out`](Self::copy_out) helpers).
///
/// * **MR/PD lifetime.** The MR belongs to the connection's protection domain.
///   sideway's [`MemoryRegion`] already holds an `Arc<ProtectionDomain>`, so the
///   PD outlives the MR by construction; holding an `Arc<Connection>` in
///   addition keeps the whole endpoint (and its PD) alive for the buffer's life,
///   matching the old shim's ownership and keeping `register_buffer` taking
///   `&Arc<Self>`.
///
/// Field order matters for `Drop`: `_mr` is declared first so it is deregistered
/// (while the PD is still alive via `_conn`) *before* `storage` is freed. The
/// one ordering step the type system still cannot express is that the NIC must
/// be stopped (QP destroyed, DMA quiesced) before an MR is deregistered, so
/// posting a work request against this buffer is `unsafe` (see
/// [`Connection::post_recv`] / [`Connection::post_send`]); in practice the
/// stream layer calls [`Connection::shutdown`] before dropping its buffers.
pub struct RegisteredBuffer {
    // Deregistered first (its `Drop` runs `ibv_dereg_mr`). Holds its own
    // `Arc<ProtectionDomain>`, so the PD cannot be freed before this dereg.
    // Never read after construction (lkey/rkey are cached below); held purely
    // for that drop-time deregistration, hence the leading underscore.
    _mr: Arc<MemoryRegion>,
    // The registered storage. Never sliced as `&[u8]`; reached only via
    // `UnsafeCell::raw_get(storage.as_ptr())`. Freed after `mr` is dropped.
    storage: Box<[std::cell::UnsafeCell<u8>]>,
    lkey: u32,
    rkey: u32,
    // Keeps the endpoint (hence the PD) alive for this buffer's whole life.
    _conn: Arc<Connection>,
}

/// A registered memory region over **caller-owned** storage (spec §7, Milestone 3:
/// zero-copy straight from pages the caller already holds resident — e.g. an MSE4
/// mmap'd store or its AIO buffers). Created by [`Connection::register_external`].
///
/// Unlike [`RegisteredBuffer`] it does **not** allocate or own the backing bytes —
/// it holds only the registration and an `Arc<Connection>` (so the MR cannot
/// outlive its PD). The caller owns the storage and guarantees it stays live,
/// resident, and unmodified for the `Mr`'s whole life and across any in-flight
/// transfer (see [`Connection::register_external`]'s safety contract). As for any
/// registration the NIC must be quiesced (QP destroyed) before the `Mr` is
/// dropped; its `Drop` then runs `ibv_dereg_mr` — `_mr` is declared first so it is
/// deregistered while the PD (kept alive by `_conn`) is still alive.
///
/// `!Send` (it carries a raw pointer into caller memory), like the rest of the
/// zero-copy data path.
pub struct Mr {
    // Deregistered first on drop; holds its own `Arc<ProtectionDomain>`.
    _mr: Arc<MemoryRegion>,
    addr: *mut u8,
    len: usize,
    lkey: u32,
    rkey: u32,
    // Keeps the endpoint (hence the PD) alive for this MR's whole life.
    _conn: Arc<Connection>,
}

impl Mr {
    /// Base address the region was registered over (the caller's pointer).
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.addr
    }
    /// Registered length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Local key — names this region as a source in a one-sided write SGE.
    pub fn lkey(&self) -> u32 {
        self.lkey
    }
    /// Remote key — authorizes a peer to RDMA into this region (only meaningful if
    /// it was registered with [`ACCESS_REMOTE_WRITE`]).
    pub fn rkey(&self) -> u32 {
        self.rkey
    }
}

impl RegisteredBuffer {
    /// Base pointer of the registered region. Derived fresh from the storage's
    /// `UnsafeCell` so no `&`/`&mut [u8]` is ever formed over memory the NIC may
    /// be DMA-ing into. The allocation never moves (it lives behind a `Box`), so
    /// the address is stable for the buffer's whole life.
    pub fn as_mut_ptr(&self) -> *mut u8 {
        std::cell::UnsafeCell::raw_get(self.storage.as_ptr())
    }

    /// Registered length in bytes.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn lkey(&self) -> u32 {
        self.lkey
    }
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Copy `dst.len()` bytes out of the registered region starting at `off`.
    ///
    /// Reads through a raw pointer — no slice reference is formed over the
    /// registered memory. The caller must ensure the NIC is not concurrently
    /// DMA-ing into `[off, off+dst.len())`; the stream upholds this by only
    /// reading a receive slot after its completion is reaped and before it is
    /// re-posted.
    pub fn copy_out(&self, off: usize, dst: &mut [u8]) {
        assert!(
            dst.len() <= self.len() && off <= self.len() - dst.len(),
            "copy_out out of bounds",
        );
        // SAFETY: bounds checked above; `dst` is the caller's separate buffer so
        // the ranges do not overlap; the source pointer is valid for `len` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(self.as_mut_ptr().add(off), dst.as_mut_ptr(), dst.len());
        }
    }

    /// Copy `src` into the registered region starting at `off`.
    ///
    /// Writes through a raw pointer — no slice reference is formed over the
    /// registered memory. The caller must ensure the NIC is not concurrently
    /// accessing `[off, off+src.len())`; the stream upholds this by only writing
    /// a send slot it currently owns (not posted to the NIC).
    pub fn copy_in(&self, off: usize, src: &[u8]) {
        assert!(
            src.len() <= self.len() && off <= self.len() - src.len(),
            "copy_in out of bounds",
        );
        // SAFETY: bounds checked above; `src` does not overlap the region; the
        // destination pointer is valid for `len` bytes and writes are sound
        // because the storage lives behind `UnsafeCell`.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.as_mut_ptr().add(off), src.len());
        }
    }
}

/// A listening RDMA endpoint. Accepts connections one at a time; each accepted
/// connection is migrated to its **own** CM event channel, so it can be handed to
/// another thread and finished/run there while this listener keeps accepting —
/// a looping, multi-threaded acceptor is race-free.
///
/// (Per-connection channels rely on `Identifier::migrate` / `rdma_migrate_id`,
/// currently carried as a local sideway patch — see vendor/sideway/HORD-PATCH.md.)
pub struct Listener {
    event_channel: Arc<EventChannel>,
    _listen_id: Arc<Identifier>,
}

impl Listener {
    /// Bind to `ip:port` and start listening.
    pub fn bind(ip: &str, port: u16) -> io::Result<Listener> {
        let addr = parse_addr(ip, port)?;
        let event_channel = EventChannel::new().map_err(to_io)?;
        let id = event_channel.create_id(PortSpace::Tcp).map_err(to_io)?;
        id.bind_addr(addr).map_err(to_io)?;
        id.listen(LISTEN_BACKLOG).map_err(to_io)?;
        Ok(Listener {
            event_channel,
            _listen_id: id,
        })
    }

    /// Block until a peer requests a connection, returning a not-yet-established
    /// [`Connection`] whose QP is in INIT. Register receive buffers and call
    /// [`Connection::post_recv`] on it, then [`Connection::accept_finish`].
    ///
    /// `send_wr` / `recv_wr` size the QP's send/recv queues.
    pub fn accept(&self, send_wr: usize, recv_wr: usize, cm: CmParams) -> io::Result<Connection> {
        loop {
            let event = self.event_channel.get_cm_event().map_err(to_io)?;
            if let Some(conn) = self.process_event(event, send_wr, recv_wr, cm)? {
                return Ok(conn);
            }
        }
    }

    /// Non-blocking [`accept`](Self::accept): return the next pending connection,
    /// or `Ok(None)` if no connection request is queued right now. Requires the
    /// listener's CM channel to have been put in non-blocking mode first (see
    /// [`set_nonblocking`](Self::set_nonblocking)); pair it with [`cm_fd`](Self::cm_fd)
    /// so an event loop can park on the fd and call this only when it is readable.
    ///
    /// Drains benign non-`ConnectRequest` events (e.g. a stray `TimewaitExit`) in
    /// the same call, so a readable fd that carried only those returns `Ok(None)`
    /// rather than a spurious connection. Device removal is still surfaced as an
    /// error, as in the blocking path.
    pub fn try_accept(
        &self,
        send_wr: usize,
        recv_wr: usize,
        cm: CmParams,
    ) -> io::Result<Option<Connection>> {
        loop {
            match self.event_channel.get_cm_event() {
                Ok(event) => {
                    if let Some(conn) = self.process_event(event, send_wr, recv_wr, cm)? {
                        return Ok(Some(conn));
                    }
                    // Benign event drained; loop to see if another is queued.
                }
                // Channel is non-blocking and empty: no connection pending.
                // `GetEventError` is `#[non_exhaustive]` (its ctor can't be
                // matched), so reach the kind through its public `.0` field.
                Err(e) => match &e.0 {
                    GetEventErrorKind::NoEvent => return Ok(None),
                    _ => return Err(to_io(e)),
                },
            }
        }
    }

    /// Handle one CM event from the listener channel: build + return a
    /// [`Connection`] for a `ConnectRequest`, surface `DeviceRemoval` as a fatal
    /// error, or ack-and-ignore anything else (`Ok(None)`). Shared by the blocking
    /// [`accept`](Self::accept) and non-blocking [`try_accept`](Self::try_accept) so
    /// the connection-setup dance lives in exactly one place.
    fn process_event(
        &self,
        event: Event,
        send_wr: usize,
        recv_wr: usize,
        cm: CmParams,
    ) -> io::Result<Option<Connection>> {
        match event.event_type() {
            EventType::ConnectRequest => {
                // A fresh cm_id for this connection (distinct from the listener
                // id); it is migrated to its own channel below.
                let id = event
                    .cm_id()
                    .ok_or_else(|| io::Error::other("connect request carried no cm id"))?;
                event.ack().map_err(to_io)?;

                // Everything past the ack is THIS connection's setup: its own CM
                // event channel (decoupled from the listener's, so finishing it —
                // and later watching it for disconnect — never competes with the
                // next accept(), making a looping/threaded acceptor race-free), the
                // migrate, the QP, and the INIT transition. If any step fails it is
                // a *per-connection* fault, not a listener fault, so on failure we:
                //   * `reject` the peer — it then fails fast instead of waiting out
                //     a connect timeout, with no half-open id lingering to timewait;
                //   * tag the error `ConnectionSetupFailed`, so a looping/threaded
                //     acceptor skips this one peer and keeps draining the queue,
                //     rather than counting a single bad peer against its
                //     fatal-error budget (see `is_connection_setup_failure`).
                let setup = (|| -> io::Result<Connection> {
                    let conn_channel = EventChannel::new().map_err(to_io)?;
                    id.migrate(&conn_channel).map_err(to_io)?;
                    let ep = Endpoint::build(&id, send_wr, recv_wr)?;
                    let conn = Connection::new(conn_channel, Arc::clone(&id), ep, Role::Server, cm);
                    conn.modify_qp(QueuePairState::Init)?;
                    Ok(conn)
                })();
                match setup {
                    Ok(conn) => Ok(Some(conn)),
                    Err(e) => {
                        // Best-effort: a reject that itself fails (e.g. the id is
                        // already torn down) leaves us no worse than the old
                        // drop-without-reject path.
                        let _ = id.reject();
                        Err(io::Error::other(ConnectionSetupFailed::new(&e)))
                    }
                }
            }
            // Device removal on the listener is terminal — surface it rather than
            // ack-and-spin (the next get_cm_event would block forever). Carry a
            // typed `DeviceRemoved` so a non-blocking acceptor can tell this fatal
            // condition apart from a transient per-connection setup error and stop
            // instead of backing off forever (see `is_device_removed`).
            EventType::DeviceRemoval => {
                let _ = event.ack();
                Err(io::Error::other(DeviceRemoved))
            }
            // Other events can legitimately appear on the listener channel (e.g. a
            // stray TimewaitExit); they are benign — ack and keep waiting.
            _ => {
                let _ = event.ack();
                Ok(None)
            }
        }
    }

    /// Put the listener's CM event channel in (non-)blocking mode. Call with
    /// `true` before driving [`try_accept`](Self::try_accept) from an event loop;
    /// the default (blocking) mode is what [`accept`](Self::accept) needs.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.event_channel.set_nonblocking(nonblocking).map_err(to_io)
    }

    /// File descriptor of the listener's CM event channel, for registration with
    /// an event loop. Readable when a CM event (e.g. a connection request) is
    /// pending; drain it with [`try_accept`](Self::try_accept). Owned by the
    /// listener; valid for its life.
    pub fn cm_fd(&self) -> RawFd {
        self.event_channel.as_raw_fd()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

/// The verbs resources of one connection: PD, completion channel + CQ, and QP.
/// Bundled so `accept` and `connect` share one construction path.
struct Endpoint {
    pd: Arc<ProtectionDomain>,
    comp_channel: Arc<CompletionChannel>,
    cq: Arc<ExtendedCompletionQueue>,
    qp: ExtendedQueuePair,
    /// Effective `max_send_sge` the QP was created with (see
    /// [`Connection::max_send_sge`]) — copied onto the `Connection`.
    max_send_sge: usize,
}

impl Endpoint {
    /// Create PD / completion-channel / CQ / QP on the device the (resolved)
    /// `id` is bound to. The QP is left in RESET; the caller drives it to INIT.
    fn build(id: &Identifier, send_wr: usize, recv_wr: usize) -> io::Result<Endpoint> {
        let ctx = id
            .get_device_context()
            .ok_or_else(|| io::Error::other("cm id is not bound to a device yet"))?;
        let pd = ctx.alloc_pd().map_err(to_io)?;

        let comp_channel = CompletionChannel::new(&ctx).map_err(to_io)?;
        // Non-blocking so an event loop can drain it (and so the sync busy-poll
        // path, which ignores the channel, is unaffected).
        comp_channel.set_nonblocking(true).map_err(to_io)?;

        // A little slack over the WR counts (the old shim used the same +16).
        let cqe = (send_wr + recv_wr + 16) as u32;
        let mut cq_builder = ctx.create_cq_builder();
        cq_builder.setup_cqe(cqe).setup_comp_channel(&comp_channel, 0);
        let cq = cq_builder.build_ex().map_err(to_io)?;

        // One CQ for both send and recv. The builder defaults already enable
        // Send / Write / WriteWithImmediate send-ops, which is everything HORD
        // posts, so no extra send-ops flags are needed.
        // Gather support (spec §7, Milestone 3): build the QP able to carry up to
        // `MAX_WRITE_SGE` scatter/gather entries per send WR, clamped to what the
        // device permits (`max_sge`), so a fragmented zero-copy source becomes one
        // multi-SGE write instead of many. `max_recv_sge` stays the default 1 (a
        // recv lands in a single slot). A device reporting < 1 is implausible, but
        // clamp up so the QP is always created with at least the single-SGE cap the
        // non-gather path needs.
        let dev_max_sge = ctx.query_device().map_err(to_io)?.max_sge();
        let max_send_sge = (dev_max_sge.max(1) as usize).min(MAX_WRITE_SGE);

        let shared_cq = GenericCompletionQueue::from(Arc::clone(&cq));
        let mut qp_builder = pd.create_qp_builder();
        qp_builder
            .setup_send_cq(shared_cq.clone())
            .setup_recv_cq(shared_cq)
            .setup_max_send_wr(send_wr as u32)
            .setup_max_recv_wr(recv_wr as u32)
            .setup_max_send_sge(max_send_sge as u32);
        let qp = qp_builder.build_ex().map_err(to_io)?;

        Ok(Endpoint {
            pd,
            comp_channel,
            cq,
            qp,
            max_send_sge,
        })
    }
}

/// An RC connection: a QP, its CQ/PD, and the CM identifier + event channel.
/// Carries the byte stream once `*_finish` has completed.
///
/// The QP lives behind a `RefCell<Option<_>>`: sideway's post / modify calls
/// take `&mut`, but HORD drives a connection from a single thread through `&self`
/// (the stream layer holds it in an `Arc`), so a `RefCell` gives interior
/// mutability with a cheap runtime borrow check. `Option` lets
/// [`shutdown`](Self::shutdown) destroy the QP early and idempotently.
pub struct Connection {
    // Declared before the Arc resources so the QP is destroyed first on drop
    // (it holds Arcs to the PD and CQ, which are torn down once it lets go).
    qp: RefCell<Option<ExtendedQueuePair>>,
    cq: Arc<ExtendedCompletionQueue>,
    comp_channel: Arc<CompletionChannel>,
    _pd: Arc<ProtectionDomain>,
    id: Arc<Identifier>,
    event_channel: Arc<EventChannel>,
    role: Role,
    // `max_send_sge` the QP was built with — the most SGEs one one-sided write WR
    // may carry, so the gather path knows how many source segments to pack per WR
    // (see `post_write_gather` / `max_send_sge`).
    max_send_sge: usize,
    // Guards `rdma_disconnect` so it runs at most once across
    // disconnect()/shutdown()/Drop. (`Cell` keeps `Connection` `!Sync` already
    // via the `RefCell`, and it's only touched single-threaded.)
    disconnected: Cell<bool>,
    // Retained for completeness / future use: sideway's `ConnectionParameter`
    // can't yet carry retry/rnr counts, and the resolve timeout is consumed in
    // `connect` before construction, so nothing reads this after setup.
    _cm: CmParams,
}

// The connection owns its RDMA resources and is driven from one thread at a time
// by the stream layer. The `RefCell` makes it `!Sync` (no concurrent access),
// while every field is `Send`, so moving it across threads is sound — which the
// async server relies on (accept on one thread, run on another).
unsafe impl Send for Connection {}

impl Connection {
    fn new(
        event_channel: Arc<EventChannel>,
        id: Arc<Identifier>,
        ep: Endpoint,
        role: Role,
        cm: CmParams,
    ) -> Connection {
        Connection {
            qp: RefCell::new(Some(ep.qp)),
            cq: ep.cq,
            comp_channel: ep.comp_channel,
            _pd: ep.pd,
            id,
            event_channel,
            role,
            max_send_sge: ep.max_send_sge,
            disconnected: Cell::new(false),
            _cm: cm,
        }
    }

    /// Client side: resolve `ip:port`, create the endpoint, and drive the QP to
    /// INIT. Not yet connected — post receives, then call
    /// [`connect_finish`](Self::connect_finish).
    pub fn connect(
        ip: &str,
        port: u16,
        send_wr: usize,
        recv_wr: usize,
        cm: CmParams,
    ) -> io::Result<Connection> {
        let addr = parse_addr(ip, port)?;
        let event_channel = EventChannel::new().map_err(to_io)?;
        let id = event_channel.create_id(PortSpace::Tcp).map_err(to_io)?;
        let timeout = Duration::from_millis(cm.resolve_timeout_ms.max(0) as u64);
        // `rdma_resolve_addr`/`_route` self-bound by `timeout` and, on their own
        // expiry, fire an `AddrError`/`RouteError` event — which arrives first and
        // yields a descriptive mismatch error. The event-wait bound is purely a
        // backstop against a CM that delivers *no* event (so it must comfortably
        // exceed the resolve timeout): at least 2x it, and never below the
        // establish bound so a tiny configured resolve timeout can't make us
        // abandon a resolve that is still progressing under load.
        let resolve_wait = timeout.saturating_mul(2).max(ESTABLISH_TIMEOUT);

        id.resolve_addr(None, addr, timeout).map_err(to_io)?;
        wait_event(&event_channel, EventType::AddressResolved, resolve_wait)?;
        id.resolve_route(timeout).map_err(to_io)?;
        wait_event(&event_channel, EventType::RouteResolved, resolve_wait)?;

        let ep = Endpoint::build(&id, send_wr, recv_wr)?;
        let conn = Connection::new(event_channel, id, ep, Role::Client, cm);
        conn.modify_qp(QueuePairState::Init)?;
        Ok(conn)
    }

    /// Client side: connect (sending our QP number), then on the connect
    /// response drive RTR → RTS and complete establishment.
    pub fn connect_finish(&self) -> io::Result<()> {
        debug_assert_eq!(self.role, Role::Client);
        let qp_number = self.with_qp(|qp| Ok(qp.qp_number()))?;

        let mut param = ConnectionParameter::default();
        param.setup_qp_number(qp_number);
        self.id.connect(param).map_err(to_io)?;

        // External-QP mode (we created the QP, not librdmacm) reports a connect
        // *response* rather than ESTABLISHED; we ack it then establish manually.
        // Bounded by the same `ESTABLISH_TIMEOUT` the server's accept uses, so a
        // peer that accepts the request then stalls cannot pin this thread — the
        // "no peer pins a thread forever" guarantee now holds on both ends.
        wait_event(&self.event_channel, EventType::ConnectResponse, ESTABLISH_TIMEOUT)?;
        self.modify_qp(QueuePairState::ReadyToReceive)?;
        self.modify_qp(QueuePairState::ReadyToSend)?;
        self.id.establish().map_err(to_io)?;
        Ok(())
    }

    /// Server side: drive RTR → RTS, accept (sending our QP number), and wait —
    /// **bounded by [`ESTABLISH_TIMEOUT`]** — for ESTABLISHED.
    ///
    /// The timeout matters for a threaded acceptor (e.g. `hord-async::HordListener`):
    /// this runs on the worker that will drive the connection, so a peer that issues
    /// a connect request and then stalls (never establishing) must not block that
    /// worker — and its other connections — forever. On timeout this returns
    /// `TimedOut` and the caller drops the half-open connection.
    pub fn accept_finish(&self) -> io::Result<()> {
        self.accept_establish_begin()?;
        wait_event(&self.event_channel, EventType::Established, ESTABLISH_TIMEOUT)
    }

    /// Server side, establish **phase one**: drive RTR → RTS and `accept` (sending
    /// our QP number), then flip the CM channel non-blocking — **without** waiting
    /// for ESTABLISHED. Pairs with [`poll_established`](Self::poll_established) for a
    /// non-blocking / async establish: a worker that must not block on a slow peer
    /// drives this once, then parks on [`cm_fd`](Self::cm_fd) between `poll_established`
    /// calls. The blocking [`accept_finish`](Self::accept_finish) is exactly this
    /// followed by an inline [`wait_event`] for ESTABLISHED.
    pub fn accept_establish_begin(&self) -> io::Result<()> {
        debug_assert_eq!(self.role, Role::Server);
        let qp_number = self.with_qp(|qp| Ok(qp.qp_number()))?;

        self.modify_qp(QueuePairState::ReadyToReceive)?;
        self.modify_qp(QueuePairState::ReadyToSend)?;

        let mut param = ConnectionParameter::default();
        param.setup_qp_number(qp_number);
        self.id.accept(param).map_err(to_io)?;
        // Non-blocking so `poll_established` never blocks the caller's thread.
        self.event_channel.set_nonblocking(true).map_err(to_io)?;
        Ok(())
    }

    /// Non-blocking step toward ESTABLISHED after
    /// [`accept_establish_begin`](Self::accept_establish_begin): `Ok(true)` once
    /// ESTABLISHED is reaped, `Ok(false)` if nothing is pending yet (park on
    /// [`cm_fd`](Self::cm_fd) and retry), `Err` on a wrong/failed CM event. Never
    /// blocks (the channel was made non-blocking by `accept_establish_begin`).
    pub fn poll_established(&self) -> io::Result<bool> {
        try_wait_event(&self.event_channel, &EventType::Established)
    }

    /// Allocate `len` zeroed bytes and register them as a memory region with the
    /// given access flags, returning a [`RegisteredBuffer`] that owns both the
    /// storage and the registration.
    ///
    /// This is safe: the returned buffer pins its own backing storage (so it
    /// cannot move or be freed early) and holds an `Arc<Connection>` (so the
    /// registration cannot outlive the PD). Posting work requests against the
    /// buffer is still `unsafe` — see [`post_recv`](Self::post_recv) /
    /// [`post_send`](Self::post_send) — and the caller must stop the NIC before
    /// the buffer is dropped (see [`RegisteredBuffer`]).
    pub fn register_buffer(
        self: &Arc<Self>,
        len: usize,
        access: i32,
    ) -> io::Result<RegisteredBuffer> {
        use std::cell::UnsafeCell;
        // `Box<[UnsafeCell<u8>]>`: registered storage is never sliced as `&[u8]`,
        // so the NIC may DMA into it while we touch other regions through raw
        // pointers without violating the aliasing model. Allocated zeroed via
        // `vec![0u8; len]` (lazily-zeroed OS pages), then reinterpreted: a `0u8`
        // is a valid `UnsafeCell<u8>` and the layout is identical.
        let storage: Box<[UnsafeCell<u8>]> = {
            let zeroed: Box<[u8]> = vec![0u8; len].into_boxed_slice();
            let data = Box::into_raw(zeroed) as *mut UnsafeCell<u8>;
            let slice = std::ptr::slice_from_raw_parts_mut(data, len);
            // SAFETY: `data` is the non-null, aligned base of a `len`-element
            // `u8` allocation reinterpreted as layout-identical `UnsafeCell<u8>`;
            // the `Layout` is unchanged, so rebuilding (and later freeing) the
            // `Box` under the new element type is sound.
            unsafe { Box::from_raw(slice) }
        };
        let ptr = UnsafeCell::raw_get(storage.as_ptr()) as usize;

        // SAFETY: `ptr` is valid for `len` bytes for the life of `storage`, which
        // this `RegisteredBuffer` owns alongside the MR; the MR is deregistered
        // before the storage is freed (field order in `RegisteredBuffer`).
        let mr = unsafe { self._pd.reg_mr(ptr, len, access_flags(access)) }.map_err(to_io)?;
        let lkey = mr.lkey();
        let rkey = mr.rkey();
        Ok(RegisteredBuffer {
            _mr: mr,
            storage,
            lkey,
            rkey,
            _conn: Arc::clone(self),
        })
    }

    /// Register **caller-owned** memory `[ptr, ptr+len)` as an MR, returning an
    /// [`Mr`] (carrying its `lkey`/`rkey`) **without allocating or owning the
    /// bytes** — so a caller (e.g. Carapace/MSE4) can DMA straight out of pages it
    /// already holds resident, instead of copying them into a HORD-owned
    /// [`RegisteredBuffer`] first (spec §7, Milestone 3). `access` is the same
    /// `ACCESS_*` bitset as [`register_buffer`](Self::register_buffer); a write
    /// *source* needs only [`ACCESS_LOCAL_WRITE`].
    ///
    /// # Safety
    /// `[ptr, ptr+len)` must be valid and must stay **live, resident, and
    /// unmodified** from now until the returned [`Mr`] is dropped — and, for any
    /// RDMA that references it, until that transfer's completion is reaped
    /// (otherwise the NIC may DMA freed or rewritten memory). As for every
    /// registration the NIC must be quiesced (QP destroyed) before the `Mr` is
    /// dropped; the stream layer enforces that via [`shutdown`](Self::shutdown).
    pub unsafe fn register_external(
        self: &Arc<Self>,
        ptr: *mut u8,
        len: usize,
        access: i32,
    ) -> io::Result<Mr> {
        // SAFETY: the caller's contract (above) guarantees `[ptr, ptr+len)` is
        // valid and stays resident for the MR's life.
        let mr = unsafe { self._pd.reg_mr(ptr as usize, len, access_flags(access)) }.map_err(to_io)?;
        let lkey = mr.lkey();
        let rkey = mr.rkey();
        Ok(Mr {
            _mr: mr,
            addr: ptr,
            len,
            lkey,
            rkey,
            _conn: Arc::clone(self),
        })
    }

    /// The QP's `max_send_sge`: the most scatter/gather entries one one-sided write
    /// WR can carry (`min(`[`MAX_WRITE_SGE`]`, device max_sge)`), i.e. how many
    /// source segments the gather write packs per WR.
    pub fn max_send_sge(&self) -> usize {
        self.max_send_sge
    }

    /// Post a receive WR over `[addr, addr+length)` (must lie within an MR with
    /// the given `lkey`). Valid in any QP state from INIT onward.
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered memory until the matching
    /// completion is reaped.
    pub unsafe fn post_recv(
        &self,
        wr_id: u64,
        addr: *mut u8,
        length: u32,
        lkey: u32,
    ) -> io::Result<()> {
        self.with_qp(|qp| {
            let mut guard = qp.start_post_recv();
            let handle = guard.construct_wr(wr_id);
            // SAFETY: caller guarantees the buffer outlives the completion.
            handle.setup_sge(lkey, addr as u64, length);
            guard.post().map_err(to_io)
        })
    }

    /// Post a signaled send WR over `[addr, addr+length)`. Only valid once the
    /// connection is established (RTS).
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered memory until the matching
    /// send completion is reaped.
    pub unsafe fn post_send(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
    ) -> io::Result<()> {
        self.with_qp(|qp| {
            let mut guard = qp.start_post_send();
            let handle = guard.construct_wr(wr_id, WorkRequestFlags::Signaled).setup_send();
            // SAFETY: caller guarantees the buffer outlives the completion.
            handle.setup_sge(lkey, addr as u64, length);
            guard.post().map_err(to_io)
        })
    }

    /// Post a signaled one-sided RDMA write: copy `[addr, addr+length)` (local,
    /// in an MR with `lkey`) into the peer's memory at `remote_addr`, authorized
    /// by `rkey`. Only valid once established (RTS). The completion carries
    /// [`Opcode::RdmaWrite`]; the peer posts no receive and observes nothing.
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered local memory until the
    /// matching completion is reaped. `remote_addr`/`rkey` must describe a live
    /// remote region the peer authorized for remote write; a stale or wrong rkey
    /// transitions the QP to the error state (closing the connection).
    pub unsafe fn post_write(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> io::Result<()> {
        let sge = [Sge { addr: addr as u64, length, lkey }];
        // SAFETY: the caller upholds `post_write_gather`'s contract for this one span
        // (live registered local memory, a valid peer rkey) — see this method's docs.
        unsafe { self.post_write_gather(wr_id, &sge, remote_addr, rkey, None) }
    }

    /// Post a one-sided RDMA write-with-immediate (§7.7 protocol splitting):
    /// like [`post_write`](Self::post_write), but atomically delivers `imm`
    /// (host order) to the peer's CQ as a [`Opcode::RecvRdmaWithImm`] completion,
    /// consuming one of the peer's posted receive WRs. `length` may be `0`. The
    /// local completion the sender reaps is still an [`Opcode::RdmaWrite`].
    ///
    /// # Safety
    /// Same contract as [`post_write`](Self::post_write); additionally the peer
    /// MUST have a receive WR posted, or the write fails with RNR and the QP
    /// transitions to the error state.
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn post_write_with_imm(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
        imm: u32,
    ) -> io::Result<()> {
        let sge = [Sge { addr: addr as u64, length, lkey }];
        // SAFETY: same single-span contract as `post_write`. The host-order→`__be32`
        // imm conversion lives solely in `post_write_gather` now.
        unsafe { self.post_write_gather(wr_id, &sge, remote_addr, rkey, Some(imm)) }
    }

    /// Post a signaled one-sided RDMA write that **gathers** from multiple local
    /// scatter/gather entries (`sg_list`, each a span inside an MR named by its
    /// `lkey`) into the peer's *contiguous* region starting at `remote_addr`,
    /// authorized by `rkey`. The NIC lays the segments down back-to-back in order,
    /// so a fragmented local source becomes one contiguous remote object (spec §7,
    /// Milestone 3). With `imm` `Some`, it is delivered as write-with-immediate
    /// (§7.7), consuming one of the peer's posted receives; the local completion is
    /// still [`Opcode::RdmaWrite`]. Only valid once established (RTS).
    ///
    /// `sg_list.len()` must be in `0..=`[`MAX_WRITE_SGE`] (and `<=` the QP's
    /// [`max_send_sge`](Self::max_send_sge)); splitting a longer gather list across
    /// WRs is the stream layer's job. An **empty** `sg_list` is allowed *only* with
    /// `imm` `Some`: verbs permits `num_sge == 0` for a write-with-immediate, so this
    /// posts a true imm-only WR that writes zero bytes at `remote_addr` and delivers
    /// the immediate — the §7.7 "deliver a bare transfer ID" signal, no source span
    /// to borrow. A 0-SGE *plain* write (no imm) would be a no-op WR and is rejected.
    ///
    /// # Safety
    /// Every `(addr, length)` in `sg_list` must reference live, registered local
    /// memory until the matching completion is reaped; `remote_addr`/`rkey` carry
    /// the same contract as [`post_write`](Self::post_write).
    pub unsafe fn post_write_gather(
        &self,
        wr_id: u64,
        sg_list: &[Sge],
        remote_addr: u64,
        rkey: u32,
        imm: Option<u32>,
    ) -> io::Result<()> {
        assert!(
            sg_list.len() <= MAX_WRITE_SGE,
            "post_write_gather: sg_list len {} exceeds MAX_WRITE_SGE {MAX_WRITE_SGE}",
            sg_list.len(),
        );
        // An empty SGE list is only meaningful as an imm-only signal (num_sge == 0
        // write-with-immediate). A plain 0-SGE write would post a no-op WR — a caller
        // error — so reject it rather than silently consuming a send slot.
        assert!(
            !sg_list.is_empty() || imm.is_some(),
            "post_write_gather: a 0-SGE write requires an immediate (imm-only signal)",
        );
        // Build the verbs SGE array on the stack (bounded by `MAX_WRITE_SGE`), so
        // there is no per-write heap allocation and the rdma-sys type stays out of
        // the public API.
        let mut sges = [rdma_mummy_sys::ibv_sge { addr: 0, length: 0, lkey: 0 }; MAX_WRITE_SGE];
        for (slot, s) in sges.iter_mut().zip(sg_list) {
            *slot = rdma_mummy_sys::ibv_sge {
                addr: s.addr,
                length: s.length,
                lkey: s.lkey,
            };
        }
        let n = sg_list.len();
        self.with_qp(|qp| {
            let mut guard = qp.start_post_send();
            let wr = guard.construct_wr(wr_id, WorkRequestFlags::Signaled);
            // The imm rides the WR as write-with-immediate; a plain write otherwise.
            // The verbs `imm_data` field is `__be32` (network byte order) and the API
            // does no conversion, so send the big-endian form of the caller's
            // host-order value. `poll` reverses it with `from_be`. On a same-endian
            // peer pair `to_be`∘`from_be` is the identity (loopback unaffected); on a
            // mixed-endian pair the wire carries canonical network order so the peer
            // reads the right value. This is the sole site of the conversion — the
            // single-SGE `post_write_with_imm` delegates here.
            let handle = match imm {
                Some(id) => wr.setup_write_imm(rkey, remote_addr, id.to_be()),
                None => wr.setup_write(rkey, remote_addr),
            };
            // SAFETY: caller guarantees every span outlives the completion.
            handle.setup_sge_list(&sges[..n]);
            guard.post().map_err(to_io)
        })
    }

    /// Poll once for a completion. `Ok(None)` means the CQ was empty.
    pub fn poll(&self) -> io::Result<Option<Completion>> {
        match self.cq.start_poll() {
            Ok(mut poller) => match poller.next() {
                Some(wc) => {
                    let opcode = Opcode::from_raw(wc.opcode());
                    // `imm_data` is only meaningful (and only valid to read) for
                    // a write-with-immediate receive completion. It arrives as
                    // `__be32` (network byte order); convert back to host order to
                    // mirror the `to_be` in `post_write_gather`.
                    let imm_data = if opcode == Opcode::RecvRdmaWithImm {
                        u32::from_be(wc.imm_data())
                    } else {
                        0
                    };
                    Ok(Some(Completion {
                        wr_id: wc.wr_id(),
                        byte_len: wc.byte_len(),
                        opcode,
                        status: wc.status(),
                        imm_data,
                    }))
                }
                // start_poll() returning Ok guarantees at least one CQE, so this
                // arm is unreachable in practice; treat it as "drained".
                None => Ok(None),
            },
            Err(PollCompletionQueueError::CompletionQueueEmpty) => Ok(None),
            Err(e) => Err(to_io(e)),
        }
    }

    /// File descriptor of the CQ completion channel, for registration with an
    /// event loop. Readable (after [`arm_cq`](Self::arm_cq)) when a completion
    /// has been signalled. Owned by the connection; valid until shutdown.
    pub fn cq_fd(&self) -> io::Result<RawFd> {
        Ok(self.comp_channel.as_raw_fd())
    }

    /// Arm the CQ to signal its completion channel on the next completion.
    /// One-shot: re-arm after each drain.
    ///
    /// sideway does not wrap `ibv_req_notify_cq`, so this calls it directly on
    /// the raw CQ handle sideway hands out via its (documented) unsafe escape
    /// hatch.
    pub fn arm_cq(&self) -> io::Result<()> {
        // SAFETY: `cq()` yields the live `ibv_cq` backing `self.cq` (kept alive
        // by the `Arc`); `ibv_req_notify_cq` only reads/arms it.
        let rc = unsafe { rdma_mummy_sys::ibv_req_notify_cq(self.cq.cq().as_ptr(), 0) };
        if rc != 0 {
            return Err(io::Error::from_raw_os_error(rc));
        }
        Ok(())
    }

    /// Drain and acknowledge all pending completion-channel notifications (the
    /// fd is non-blocking). Returns the number consumed. Acknowledging is
    /// required before the CQ can be destroyed.
    ///
    /// sideway does not wrap `ibv_get_cq_event` / `ibv_ack_cq_events`; this uses
    /// the raw completion-channel and CQ handles from sideway's escape hatches.
    pub fn consume_cq_events(&self) -> usize {
        // SAFETY: the channel/CQ handles are kept alive by `self`; we only call
        // get/ack on them, and the channel fd is non-blocking so get returns an
        // error (EAGAIN) once drained rather than blocking.
        let channel = unsafe { self.comp_channel.comp_channel() };
        let mut count = 0u32;
        loop {
            let mut cq_ptr: *mut rdma_mummy_sys::ibv_cq = std::ptr::null_mut();
            let mut cq_ctx: *mut c_void = std::ptr::null_mut();
            let rc =
                unsafe { rdma_mummy_sys::ibv_get_cq_event(channel.as_ptr(), &mut cq_ptr, &mut cq_ctx) };
            if rc != 0 {
                break;
            }
            count += 1;
        }
        if count > 0 {
            unsafe { rdma_mummy_sys::ibv_ack_cq_events(self.cq.cq().as_ptr(), count) };
        }
        count as usize
    }

    /// File descriptor of the connection's CM event channel.
    pub fn cm_fd(&self) -> io::Result<RawFd> {
        Ok(self.event_channel.as_raw_fd())
    }

    /// The peer's resolved socket address, once the connection is established.
    /// For RoCE this is the address the CM derived from the peer's GID. `None`
    /// before establishment or for an address family the wrapper does not map.
    /// A HORD listener uses this to label each connection with its peer for the
    /// per-connection service. (Relies on the vendored sideway `peer_addr`
    /// addition — see vendor/sideway/HORD-PATCH.md.)
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.id.peer_addr()
    }

    /// Make the CM channel non-blocking. Call only *after* the handshake — setup
    /// relies on blocking CM waits.
    pub fn set_cm_nonblock(&self) -> io::Result<()> {
        self.event_channel.set_nonblocking(true).map_err(to_io)
    }

    /// Non-blocking check for a peer-initiated teardown (DISCONNECTED / device
    /// removal / connect error). Requires [`set_cm_nonblock`](Self::set_cm_nonblock)
    /// first. `Ok(true)` means the peer is gone.
    pub fn check_disconnect(&self) -> io::Result<bool> {
        match self.event_channel.get_cm_event() {
            Ok(event) => {
                let gone = matches!(
                    event.event_type(),
                    EventType::Disconnected | EventType::DeviceRemoval | EventType::ConnectError
                );
                let _ = event.ack();
                Ok(gone)
            }
            // No event pending (channel is non-blocking). `GetEventError` is
            // `#[non_exhaustive]` so its ctor can't be matched; reach the kind
            // through its public `.0` field instead.
            Err(e) => match &e.0 {
                GetEventErrorKind::NoEvent => Ok(false),
                _ => Err(to_io(e)),
            },
        }
    }

    /// Issue `rdma_disconnect` at most once. Best-effort — errors (e.g. on a
    /// not-yet-established id) are ignored. Shared by `disconnect`/`shutdown`/Drop
    /// so the peer never sees a redundant disconnect.
    fn do_disconnect(&self) {
        if !self.disconnected.replace(true) {
            let _ = self.id.disconnect();
        }
    }

    /// Begin a graceful disconnect. Best-effort; idempotent.
    pub fn disconnect(&self) {
        self.do_disconnect();
    }

    /// Stop the NIC for this connection: disconnect and destroy the QP.
    /// Idempotent. After this, no further DMA can target registered buffers, so
    /// it is safe to deregister memory regions (which a [`RegisteredBuffer`]
    /// does on drop). The CQ/PD stay alive (held by `Arc`) so any outstanding
    /// `RegisteredBuffer` can still deregister against the PD.
    pub fn shutdown(&self) {
        self.do_disconnect();
        // Dropping the QP runs `ibv_destroy_qp`. `take` makes this idempotent.
        let _ = self.qp.borrow_mut().take();
    }

    /// Run `f` with a `&mut` to the live QP, or error if the connection has been
    /// shut down. Centralises the `RefCell` + `Option` borrow.
    fn with_qp<R>(&self, f: impl FnOnce(&mut ExtendedQueuePair) -> io::Result<R>) -> io::Result<R> {
        let mut slot = self.qp.borrow_mut();
        let qp = slot
            .as_mut()
            .ok_or_else(|| io::Error::other("connection has been shut down"))?;
        f(qp)
    }

    /// Drive the QP toward `state` using the CM-computed attributes. At INIT
    /// (where RC access flags are set) we additionally permit incoming remote
    /// writes — the zero-copy destination side — since the CM-derived attrs don't
    /// always include it; later transitions leave access flags untouched.
    fn modify_qp(&self, state: QueuePairState) -> io::Result<()> {
        let mut attr = self.id.get_qp_attr(state).map_err(to_io)?;
        if matches!(state, QueuePairState::Init) {
            attr.setup_access_flags(AccessFlags::LocalWrite | AccessFlags::RemoteWrite);
        }
        self.with_qp(|qp| qp.modify(&attr).map_err(to_io))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Parity with the old shim's hord_conn_free: a Connection dropped without
        // an explicit shutdown() still issues a graceful rdma_disconnect and
        // destroys the QP. Idempotent if shutdown()/disconnect() already ran.
        self.shutdown();
    }
}

/// Upper bound on the server's wait for ESTABLISHED in
/// [`accept_finish`](Connection::accept_finish). A peer that connects then never
/// establishes must not pin the accepting worker forever; mirrors the spirit of
/// the first-message handshake timeout in `hord-stream`. Exposed so the async
/// server-handshake stage in `hord-async` bounds its non-blocking establish wait
/// (driven via [`poll_established`](Connection::poll_established)) by the same value.
pub const ESTABLISH_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait for the next CM event on `channel`, require it to be `want`, and ack it
/// either way — bounded by `timeout`.
///
/// This is the single CM wait shared by **both ends** of the handshake: the
/// client (address/route resolution, then the connect response) and the server
/// (established). Routing every wait through one function means the deadline
/// guarantee, the ack, and the mismatch-error formatting live in exactly one
/// place, and the two ends can no longer diverge in how they wait.
///
/// It is **fd-driven, not sleep-polled**: it parks on the channel fd with
/// [`poll_readable`] and wakes the instant an event lands, so it adds no polling
/// latency to a handshake and burns no CPU while waiting (the old timed variant
/// busy-looped on a 1 ms [`std::thread::sleep`], which both added up to ~1 ms of
/// latency per handshake and blocked the caller's whole current-thread runtime).
/// A peer that stalls cannot pin the calling thread past the deadline — on
/// expiry this returns [`io::ErrorKind::TimedOut`].
///
/// The channel is flipped non-blocking so the `get_cm_event` after a readable
/// `poll` cannot block on a spurious wakeup (or on a benign event we loop past).
/// Callers do not depend on the mode this leaves: the stream layer sets the
/// post-handshake channel mode explicitly via [`Connection::set_cm_nonblock`].
fn wait_event(channel: &Arc<EventChannel>, want: EventType, timeout: Duration) -> io::Result<()> {
    channel.set_nonblocking(true).map_err(to_io)?;
    let fd = channel.as_raw_fd();
    let deadline = Instant::now() + timeout;
    loop {
        if try_wait_event(channel, &want)? {
            return Ok(());
        }
        // Nothing pending: park on the fd until it is readable or the deadline
        // passes, then loop to drain it.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for CM event {want:?}"),
            ));
        }
        poll_readable(fd, remaining)?;
    }
}

/// One **non-blocking** step toward a CM `want` event, the shared core of both
/// [`wait_event`] (sync: loop this + [`poll_readable`]) and the async establish
/// driver (loop this + a reactor `readable().await`). The channel must already be
/// non-blocking ([`EventChannel::set_nonblocking`]); this never blocks.
///
/// Returns `Ok(true)` when `want` arrived and was acked, `Ok(false)` when nothing
/// is pending right now (the caller parks on the channel fd and retries), and
/// `Err` when a *different* or failed event arrived (acked first) or the channel
/// errored. `GetEventError` is `#[non_exhaustive]`; reach its kind via `.0`.
fn try_wait_event(channel: &Arc<EventChannel>, want: &EventType) -> io::Result<bool> {
    match channel.get_cm_event() {
        Ok(event) => {
            let got = event.event_type();
            let status = event.status();
            event.ack().map_err(to_io)?;
            if got == *want {
                Ok(true)
            } else {
                Err(io::Error::other(format!(
                    "expected CM event {want:?}, got {got:?} (status {status})"
                )))
            }
        }
        Err(e) => match &e.0 {
            GetEventErrorKind::NoEvent => Ok(false),
            _ => Err(to_io(e)),
        },
    }
}

/// Park on `fd` with `poll(2)` until it is readable or `timeout` elapses.
///
/// Returns `Ok(())` on either outcome — a readable fd *or* a poll timeout — and
/// lets the caller re-check its own deadline and re-drain the channel; only a
/// genuine `poll(2)` failure is surfaced. `POLLERR`/`POLLHUP` also satisfy the
/// wait: the follow-up `get_cm_event` is what turns the underlying condition into
/// a real error. `EINTR` retries within the original `timeout`.
fn poll_readable(fd: RawFd, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        // `poll` takes a `c_int` millisecond timeout. Round a sub-millisecond
        // remainder up to 1 ms so a `poll(…, 0)` can't busy-spin out the tail of
        // the deadline, and clamp the (already-bounded) wait to the argument type.
        let ms = match remaining.as_millis() {
            0 => 1,
            n => n.min(libc::c_int::MAX as u128) as libc::c_int,
        };
        let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
        // SAFETY: `pfd` is a single, live, correctly-initialised `pollfd` for the
        // duration of the call; `poll` reads/writes only that one element.
        let rc = unsafe { libc::poll(&mut pfd, 1, ms) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            // EINTR: a signal interrupted the wait; retry within the same deadline.
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        // rc == 0 (timed out) or rc > 0 (readable / error revents): either way,
        // hand back so the caller re-checks the deadline and drains the channel.
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The two listener error markers must be distinguishable and must not
    // cross-trigger: the acceptor stops on device removal but only *skips* a
    // per-connection setup failure, so confusing them is a correctness bug.
    #[test]
    fn connection_setup_failure_is_classified_and_distinct() {
        let cause = io::Error::new(io::ErrorKind::Other, "QP creation failed");
        let tagged = io::Error::other(ConnectionSetupFailed::new(&cause));
        assert!(is_connection_setup_failure(&tagged));
        assert!(!is_device_removed(&tagged));

        let removed = io::Error::other(DeviceRemoved);
        assert!(is_device_removed(&removed));
        assert!(!is_connection_setup_failure(&removed));

        // A plain error is neither marker.
        let plain = io::Error::new(io::ErrorKind::TimedOut, "nope");
        assert!(!is_connection_setup_failure(&plain));
        assert!(!is_device_removed(&plain));
    }

    #[test]
    fn connection_setup_failure_display_carries_the_cause() {
        let cause = io::Error::new(io::ErrorKind::Other, "max_qp_wr exceeded");
        let tagged = io::Error::other(ConnectionSetupFailed::new(&cause));
        assert!(tagged.to_string().contains("max_qp_wr exceeded"));
    }
}
