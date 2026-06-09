//! [`HordStream`]: a reliable, ordered byte stream over RDMA RC send/recv.
//!
//! This is the stream abstraction layer of spec section 6, plus the
//! credit-based flow control of section 9. It presents `std::io::Read` +
//! `std::io::Write`, so any byte-stream consumer — including a hand-rolled
//! HTTP/1.1 codec, or eventually `hyper` once an async wrapper exists — can run
//! over it unmodified.
//!
//! ## Model (prototype)
//!
//! Synchronous and blocking, driven by busy-polling the completion queue.
//! There is exactly one CQ per connection, shared by sends and receives; work
//! requests are tagged by `wr_id` to tell them apart.
//!
//! ## Flow control
//!
//! Each side starts with `peer.max_recv_buffers` send credits. Posting a *data*
//! message costs one credit. As we drain received messages (in `read()`) and
//! re-post their receive buffers we accrue a *grant debt* to the peer, which we
//! repay by stamping the envelope `credits` field of outgoing messages — or, if
//! we have no data to send, via a zero-length `CREDIT_ONLY` message.
//!
//! ### The control lane
//!
//! A `CREDIT_ONLY` credit-return must not itself need a data credit — otherwise
//! two peers that simultaneously hit zero credits while each owes the other
//! grants deadlock forever (neither can send the message that would unstick the
//! other). So credit-returns travel a separate *control lane*:
//!
//! - **Receive:** beyond the `recv_pool` data buffers we keep [`CTRL_RECV_SLACK`]
//!   extra receive buffers permanently posted (re-posted the instant a control
//!   message lands). Since un-read data occupies at most `recv_pool` buffers,
//!   at least `CTRL_RECV_SLACK` receive WRs are always posted, so a peer can
//!   always deliver a credit-return.
//! - **Send:** a reserved control send buffer carries the `CREDIT_ONLY` message.
//!   It is bounded by one in-flight message (`ctrl_send_busy`) rather than by a
//!   data credit, and self-clocks on its own send completion — an RC completion
//!   means the peer accepted it into one of those always-posted WRs. No data
//!   credit is consumed, and no credit is returned for it, so there is no
//!   "credit to return a credit" regress.
//!
//! This rests on both ends polling promptly (true here: we busy-poll the CQ
//! while inside `read`/`write`/`flush`).

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use hord_core::{
    CmParams, Completion, Connection, Listener, Mr, Opcode, RegisteredBuffer, Sge,
    ACCESS_LOCAL_WRITE, ACCESS_REMOTE_WRITE, MAX_WRITE_SGE,
};

use crate::envelope::{flags as env_flags, Envelope, ENVELOPE_LEN};
use crate::handshake::{Handshake, HANDSHAKE_LEN};

/// Stream-layer configuration (subset of spec Appendix A relevant to the
/// stream path).
#[derive(Debug, Clone)]
pub struct HordConfig {
    /// Maximum bytes per RDMA send (envelope + payload). Spec default 64 KiB.
    pub max_message_size: usize,
    /// Pre-posted receive buffers per connection.
    pub recv_pool_size: usize,
    /// Send staging buffers per connection.
    pub send_pool_size: usize,
    /// Connection-manager retry / timeout parameters (#11).
    pub cm: CmParams,
    /// Advertise the zero-copy extension (spec §7) in our handshake. Harmless
    /// when `true` even if unused: it only takes effect once a peer sends an
    /// `X-HORD-RDMA-Write` request and we elect to honour it.
    pub zero_copy: bool,
    /// Advertise protocol splitting (spec §7.7) in our handshake. Per §5.3 it
    /// requires `zero_copy`; advertised only when both are set. When negotiated,
    /// a split-mode request (one carrying an `id`) is served with RDMA
    /// write-with-immediate so the data plane learns of payload arrival from the
    /// CQ rather than the HTTP response.
    pub split_mode: bool,
    /// Transfer credits (spec §7.7.6): receive WRs reserved, on top of the data
    /// pool and control slack, for in-flight split-mode transfers. Each
    /// write-with-immediate consumes one posted recv WR; this headroom bounds
    /// the concurrent split transfers a peer can have in flight without starving
    /// the data receive window. Ignored unless `split_mode`.
    pub split_credits: usize,
}

impl Default for HordConfig {
    fn default() -> Self {
        HordConfig {
            max_message_size: 65536,
            recv_pool_size: 32,
            send_pool_size: 16,
            cm: CmParams::default(),
            zero_copy: true,
            split_mode: true,
            split_credits: 8,
        }
    }
}

// Extra receive buffers, beyond the advertised data pool, kept permanently
// topped up (re-posted on receipt) to carry control messages. This is the
// "reserved pool" that breaks the full-duplex credit deadlock (#3): because
// unconsumed data occupies at most `recv_pool` buffers, at least this many
// receive WRs are always posted, so a peer can always land a credit-return
// message even when every data buffer is full of un-read data.
const CTRL_RECV_SLACK: usize = 2;

// Reserved send buffers for the control lane. One is enough: a credit-return is
// bounded to a single in-flight message and self-clocks on its completion.
const CTRL_SEND_SLOTS: usize = 1;

// One extra send + one extra recv WR for the first-message HORD handshake
// (formerly carried in RDMA-CM private data; see the `handshake` module). The
// handshake recv is posted before the QP reaches RTS (RNR-safe) and the send
// right after establishment; both complete before any data flows, so the slots
// are free for the data phase.
const HS_SEND_SLOTS: usize = 1;
const HS_RECV_SLOTS: usize = 1;

// Transfer-credit (spec §7.7.6) receive headroom: zero unless we advertise split
// mode, otherwise `split_credits`. A write-with-immediate consumes one posted
// recv WR; this slack keeps concurrent split transfers from cannibalising the
// data receive window. We size for our *own* advertised intent (the peer's
// capability isn't known until the handshake completes), but — unlike the data
// pool — we no longer *register or post* it until `apply_peer` confirms split
// mode negotiated, so a connection against a non-split peer never pins
// `split_credits * max_message_size`. See `HordStream::post_split_recvs`.
fn split_slack(config: &HordConfig) -> usize {
    if config.split_mode {
        config.split_credits
    } else {
        0
    }
}

// Receive WRs registered and posted *eagerly*, before the handshake: the data
// pool plus the always-posted control slack. A data SEND can arrive the instant
// the QP is live (before the handshake completes), so these must be pre-posted in
// `new_common`. The split-mode transfer headroom is deliberately *not* part of
// this — it is registered + posted lazily (see `recv_wr_count` /
// `post_split_recvs`).
fn recv_base_count(config: &HordConfig) -> usize {
    config.recv_pool_size + CTRL_RECV_SLACK
}

// Total receive WRs the QP must be *sized* to hold: the eager base plus the
// split-mode transfer headroom. The recv queue (and, since `cqe` in hord-core is
// derived from send_wr + recv_wr, the CQ) is created at this depth so the
// lazily-posted split WRs always have room. We do *not* keep all of these posted
// from the start: the split headroom is registered and posted only once split
// mode negotiates (`post_split_recvs`).
fn recv_wr_count(config: &HordConfig) -> usize {
    recv_base_count(config) + split_slack(config)
}

// wr_id encoding: top bit distinguishes sends from receives; the next bit marks
// the reserved control send (so its completion frees the control slot rather
// than a data slot); a third bit marks a one-sided RDMA write (zero-copy), which
// belongs to neither send pool nor recv pool and is reaped by a separate
// counter; a fourth bit marks the *write-with-immediate* WR of a split-mode
// transfer (§7.7), so its completion frees a transfer credit (`imm_outstanding`)
// as well as the write counter. Low bits are the buffer/chunk index. Control
// *receives* are recognised by the CREDIT_ONLY envelope flag, not by wr_id (the
// NIC consumes receive WRs FIFO regardless of message type, so the slot carries
// no lane).
const SEND_FLAG: u64 = 1 << 63;
const CTRL_FLAG: u64 = 1 << 62;
const WRITE_FLAG: u64 = 1 << 61;
const IMM_FLAG: u64 = 1 << 60;
fn recv_wr_id(slot: usize) -> u64 {
    slot as u64
}
fn send_wr_id(slot: usize) -> u64 {
    SEND_FLAG | slot as u64
}
fn ctrl_send_wr_id(slot: usize) -> u64 {
    SEND_FLAG | CTRL_FLAG | slot as u64
}
fn write_wr_id(chunk: u64) -> u64 {
    WRITE_FLAG | chunk
}
// The imm-bearing WR of a split transfer: a write whose completion also frees a
// transfer credit. `is_write` still matches it (so it is reaped on the write
// path), but `is_imm_write` distinguishes it for credit accounting.
fn imm_write_wr_id(chunk: u64) -> u64 {
    WRITE_FLAG | IMM_FLAG | chunk
}
fn is_send(wr_id: u64) -> bool {
    wr_id & SEND_FLAG != 0
}
fn is_ctrl_send(wr_id: u64) -> bool {
    wr_id & CTRL_FLAG != 0
}
fn is_write(wr_id: u64) -> bool {
    wr_id & WRITE_FLAG != 0
}
fn is_imm_write(wr_id: u64) -> bool {
    wr_id & IMM_FLAG != 0
}
fn slot_of(wr_id: u64) -> usize {
    (wr_id & !(SEND_FLAG | CTRL_FLAG | WRITE_FLAG | IMM_FLAG)) as usize
}

// Reserved wr_ids for the one-time first-message handshake exchange. Distinct
// from every data wr_id (data sends set bit 63; data writes set bit 61 but not
// 63; recvs are small indices) and fully reaped before the data loop starts, so
// they never reach `handle_completion`.
const HS_RECV_WR_ID: u64 = u64::MAX;
const HS_SEND_WR_ID: u64 = u64::MAX - 1;

// Deadline for the first-message handshake exchange. The handshake is one round
// trip after the QP is established, so this only bounds the pathological case
// where a peer reaches ESTABLISHED but never sends its handshake (crash, or a
// non-HORD endpoint) — without it, `exchange_handshake` would spin forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

// How many empty CQ polls the blocking busy-poll spins through before it checks
// the CM channel for a peer half-close. A `get_cm_event` is a syscall, so we
// rate-limit it well below the spin rate — large enough that a data-carrying
// `pump` (which reaps a completion and returns immediately) never reaches it, so
// throughput is untouched, yet small enough to detect a graceful disconnect in
// tens of microseconds. Only consulted when the CQ has been idle this long.
const CM_DISCONNECT_POLL_SPINS: u32 = 4096;

/// Bytes per RDMA-write work request. The NIC segments a single WR into MTU-sized
/// packets, so one WR can carry a very large payload; we cap it only so an
/// enormous object maps to a bounded number of WRs (`begin_rdma_write` requires
/// that count to fit the send queue). 1 GiB matches the demo's body ceiling, so
/// every demo response is a single WR.
const WRITE_WR_MAX: usize = 1 << 30;

/// One contiguous source span of a *scatter-gather* zero-copy write (spec §7,
/// Milestone 3): a `[off, off+len)` slice of a registered region — a
/// [`RegisteredBuffer`] or a caller-owned [`Mr`] — that the NIC reads as part of
/// one logical write. A fragmented object (e.g. an MSE4 object stored across
/// non-contiguous allocations) is described as a `&[WriteSegment]` and laid down
/// contiguously at the peer's offset by
/// [`rdma_write_gather_all`](HordStream::rdma_write_gather_all).
///
/// The `'a` lifetime borrows the source region, so a `WriteSegment` — and hence any
/// slice passed to a gather write — cannot outlive the buffer/`Mr` it points into.
/// That is what makes the gather write *safe*: the borrow guarantees the source
/// stays alive for the whole write (the blocking call drains before it returns; the
/// async future resolves only once every WR is reaped), exactly as the
/// single-buffer [`rdma_write_all`](HordStream::rdma_write_all)'s `&RegisteredBuffer`
/// borrow does. Build them with [`from_registered`](Self::from_registered) /
/// [`from_mr`](Self::from_mr) (bounds-checked, safe), or [`from_raw`](Self::from_raw)
/// (unchecked, `unsafe`).
#[derive(Clone, Copy)]
pub struct WriteSegment<'a> {
    local_addr: *const u8,
    lkey: u32,
    len: usize,
    _src: PhantomData<&'a ()>,
}

impl<'a> WriteSegment<'a> {
    /// A `[off, off+len)` span of a HORD-owned [`RegisteredBuffer`] source. Panics
    /// if the span is out of bounds.
    pub fn from_registered(buf: &'a RegisteredBuffer, off: usize, len: usize) -> Self {
        assert!(
            off.checked_add(len).is_some_and(|end| end <= buf.len()),
            "WriteSegment::from_registered span out of bounds",
        );
        WriteSegment {
            // Pointer derivation only (no read): `off <= buf.len()` and the storage
            // is one allocation, so `add(off)` is in-bounds.
            local_addr: unsafe { buf.as_mut_ptr().add(off) },
            lkey: buf.lkey(),
            len,
            _src: PhantomData,
        }
    }

    /// A `[off, off+len)` span of a caller-owned [`Mr`] source — the true zero-copy
    /// case: DMA straight out of the caller's resident pages. Panics if out of
    /// bounds.
    pub fn from_mr(mr: &'a Mr, off: usize, len: usize) -> Self {
        assert!(
            off.checked_add(len).is_some_and(|end| end <= mr.len()),
            "WriteSegment::from_mr span out of bounds",
        );
        WriteSegment {
            // Pointer derivation only (no read): `off <= mr.len()`.
            local_addr: unsafe { mr.as_mut_ptr().add(off) },
            lkey: mr.lkey(),
            len,
            _src: PhantomData,
        }
    }

    /// A span from a raw `(addr, lkey, len)`, for a source registered out of band.
    ///
    /// # Safety
    /// `[addr, addr+len)` must lie within a memory region registered on this
    /// connection's PD under `lkey`, and stay live, resident, and unmodified until
    /// the write that consumes this segment completes. Prefer the borrow-checked
    /// [`from_registered`](Self::from_registered) / [`from_mr`](Self::from_mr).
    pub unsafe fn from_raw(addr: *const u8, lkey: u32, len: usize) -> Self {
        WriteSegment { local_addr: addr, lkey, len, _src: PhantomData }
    }

    /// Length of this span in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A received data message whose payload still lives in its receive buffer,
/// awaiting `read()`. We hold the buffer (rather than copying out and
/// re-posting on receipt) so the receive buffer is only freed — and a credit
/// only returned to the peer — once the application has *consumed* the bytes.
/// That ties the peer's send window to our read progress, giving real
/// backpressure and a bounded reassembly footprint.
#[derive(Debug, Clone, Copy)]
struct ReadyMsg {
    slot: usize,
    // `start`/`end` are offsets within `slot`'s *backing MR* — `recv` or the
    // split MR — resolved via `recv_slot(slot)`, not into a single buffer (the
    // payload may sit in either pool, per the FIFO note on `recv_slot`).
    start: usize, // offset of the next unread byte
    end: usize,   // offset one past the payload
}

/// Per-connection metadata for host logging and multi-tenancy: a snapshot taken
/// once the handshake completes.
///
/// An embedder (e.g. Carapace's direct listener) uses this to record a
/// transport-accurate connection in its transaction log — the RDMA analogue of a
/// TCP `Connected` event plus the capabilities that were negotiated — and to
/// attach a tenant dimension keyed on the peer. Obtained via
/// [`HordStream::conn_meta`] (and the async forwards). See the trust-model note in
/// the `hord-async` listener docs before keying tenancy or trust on `peer_addr`.
#[derive(Debug, Clone)]
pub struct ConnMeta {
    /// The peer's address as resolved by the RDMA connection manager, or `None`
    /// if the CM could not report one (e.g. an address family the wrapper does
    /// not map). For **RoCEv2** — HORD's target transport — the IP *is* the peer's
    /// GID in address form (a RoCEv2 GID is the IPv6-mapped peer address), so this
    /// is the peer's GID identity. It is the only peer identity HORD can attest,
    /// and only as trustworthy as the fabric: RDMA QPs carry no TLS or
    /// authentication, so keying tenancy on it is a last-hop-trusted-fabric
    /// assumption (see the listener trust-model note).
    pub peer_addr: Option<SocketAddr>,
    /// Wall-clock time the HORD handshake completed and the connection became
    /// ready to serve — the analogue of a TCP `Connected` timestamp, for a
    /// transaction log (e.g. Varnish VSL) that records connection establishment.
    pub established_at: SystemTime,
    /// Whether the zero-copy extension (spec §7) negotiated on this connection.
    pub zero_copy_negotiated: bool,
    /// Whether protocol splitting (spec §7.7) negotiated on this connection.
    pub split_mode_negotiated: bool,
}

/// A HORD byte stream over a single RC connection.
pub struct HordStream {
    conn: Arc<Connection>,
    // Registered RDMA buffers. Each owns its (UnsafeCell-backed) storage, its
    // MR, and an `Arc<Connection>` clone — so the PD outlives every MR by
    // construction and the teardown ordering is no longer hostage to field
    // order or a hand-rolled `Option` dance (see `Drop`). The NIC and we reach
    // them only by raw pointer; we never form a `&[u8]` over them.
    recv: RegisteredBuffer,
    // The split-mode transfer headroom (spec §7.7.6), in its *own* MR so it can
    // be registered lazily: `None` until `apply_peer` confirms split mode
    // negotiated, then a `split_headroom * msg_size` region whose recv WRs occupy
    // slots `[split_base, split_base + split_headroom)`. Kept out of `recv` so a
    // connection that declines split never pins it — the whole point of the lazy
    // path. Resolved alongside `recv` by `recv_slot`.
    split_recv: Option<RegisteredBuffer>,
    send: RegisteredBuffer,
    // Small registered buffer for the first-message handshake exchange:
    // [0..HANDSHAKE_LEN) receives the peer's handshake, [HANDSHAKE_LEN..) holds
    // ours. Held for the connection's life — its two WRs complete during setup,
    // but keeping the registration around is simpler than an early dereg.
    _handshake: RegisteredBuffer,

    msg_size: usize,    // bytes per buffer slot (our max_message_size)
    payload_cap: usize, // max payload per message = min(ours, peer) - ENVELOPE_LEN
    recv_pool: usize,
    // First recv slot index that lives in the split MR rather than in `recv`:
    // `recv_pool + CTRL_RECV_SLACK`. Slots below it are in `recv`; slots at or
    // above it are in `split_recv` (once posted). Drives `recv_slot`.
    split_base: usize,
    // How many split-mode recv WRs we post (= our advertised `split_credits`)
    // once split negotiates; 0 when we don't advertise split. Held so `apply_peer`
    // can size/register the headroom without the construction-time config.
    split_headroom: usize,
    send_pool: usize,

    send_free: Vec<usize>, // free *data* send slot indices (the control slot is separate)
    send_credits: u32,     // data messages we may still post to the peer
    grant_pending: u32,    // data credits we owe the peer (consumed recvs not yet announced)
    ctrl_slot: usize,      // reserved send slot index for control (CREDIT_ONLY) messages
    ctrl_send_busy: bool,  // a control message is in flight on `ctrl_slot`

    tx_stage: Vec<u8>,        // bytes buffered by write(), drained into messages
    rx_ready: VecDeque<ReadyMsg>, // received data, still in its recv buffer, awaiting read()
    peer_closed: bool,        // observed a flush/transport error -> treat as EOF/broken pipe
    // The CM channel has been flipped non-blocking (after the handshake), so the
    // blocking busy-poll may check it for a peer-initiated graceful half-close.
    // `false` if the flip failed at setup — half-close detection is then simply
    // off (the data path is unaffected), matching the async wrapper's stance.
    cm_watch: bool,
    // Connection metadata captured once, in `apply_peer` (the last setup step on
    // both the accept and connect paths), so `conn_meta` is a true post-handshake
    // snapshot rather than a live re-query. `peer_addr` is resolved from the CM id
    // *after* establishment (the address is fixed by then and never changes for the
    // connection's life), so we read it once instead of an FFI `rdma_get_peer_addr`
    // on every accessor call. `established_at` is the wall-clock "Connected" instant
    // (VSL-style logging). Both stay at their `new_common` sentinels (`None` /
    // `UNIX_EPOCH`) only on a stream that errored out before `apply_peer` — never one
    // handed to a caller.
    peer_addr: Option<SocketAddr>,
    established_at: SystemTime,

    // ---- zero-copy extension (spec §7) ----
    // Our config's `zero_copy` at construction, AND-ed with the peer's handshake
    // flag in `apply_peer` — so after the handshake it is exactly "both sides
    // advertised the capability".
    zero_copy: bool,
    writes_outstanding: u32,  // one-sided RDMA writes posted but not yet reaped

    // ---- protocol splitting (spec §7.7) ----
    // Like `zero_copy`: our config AND-ed with the peer's flag (and our own
    // zero-copy) in `apply_peer`.
    split_mode: bool,
    // Transfer IDs from `RECV_RDMA_WITH_IMM` completions, awaiting the data-plane
    // consumer (`next_completed_transfer` / `poll_completed_transfer`). The
    // dispatcher in `handle_completion` demultiplexes these from stream messages
    // by opcode (spec §7.7.1).
    completed_transfers: VecDeque<u32>,
    // Transfer-credit flow control (spec §7.7.6). `peer_split_credits` is the
    // window the peer advertised in its handshake — how many write-with-imm
    // transfers it can receive concurrently. It is stored unconditionally from the
    // handshake, so it carries no invariant on its own: it is `>= 1` only at call
    // sites reached after `split_mode_negotiated()` (which `negotiate_split` only
    // returns true for when the peer advertised `> 0`). It can be `0` on a
    // connection that did not negotiate split — which is exactly why the facades'
    // back-pressure loops keep a "nothing in flight to drain" guard instead of
    // assuming a credit will eventually free.
    //
    // `imm_outstanding` counts our posted (sender-side) write-with-imm WRs not yet
    // acked; we never let it exceed `peer_split_credits`, so we can't overrun the
    // peer's posted recv WRs and RNR-stall. A WR's ack means the peer's NIC has
    // consumed the recv WR and delivered the immediate to the peer's CQ; the peer
    // reposts it (per §7.7.5) when it drains that completion, which may lag the
    // ack — a transient RNR is absorbed by the (infinite) `rnr_retry` in hord-core
    // and self-heals on the repost, so freeing the credit on the ack is safe.
    peer_split_credits: u32,
    imm_outstanding: u32,
}

/// Whether protocol splitting (spec §7.7) negotiates on this connection, given
/// our local intent, the *negotiated* zero-copy result, and the peer's
/// handshake. Split requires zero-copy (§5.3) and a non-zero advertised transfer
/// window (§7.7.6) — a peer that sets the capability bit but advertises 0
/// credits cannot receive any write-with-imm, so split declines to the stream.
/// Factored out so the rule is unit-testable without hardware.
fn negotiate_split(local_split: bool, negotiated_zero_copy: bool, peer: &Handshake) -> bool {
    local_split && negotiated_zero_copy && peer.split_mode_capable() && peer.split_credits > 0
}

impl HordStream {
    /// Server side: accept the next connection on `listener` and complete the
    /// HORD handshake.
    pub fn accept(listener: &Listener, config: &HordConfig) -> io::Result<HordStream> {
        let conn = Self::accept_begin(listener, config)?;
        Self::from_accepted(conn, config)
    }

    /// Server side, phase one: accept the next connection request with this
    /// config's QP sizing (the data pools, the control lane's reserved WRs, and
    /// the handshake's one send + one recv), returning the not-yet-established
    /// [`Connection`] — which **is** `Send`.
    ///
    /// Split out from [`accept`](Self::accept) so an async server can run the
    /// accept loop on one thread and finish each connection on another: the
    /// registered buffers make the resulting `HordStream` thread-affine (`!Send`),
    /// so it must be *built* on the thread that will *run* it. The acceptor moves
    /// the bare `Connection` across the thread boundary and the worker calls
    /// [`from_accepted`](Self::from_accepted) — which is where the handshake is
    /// now exchanged (over the QP as the first messages, no longer in CM private
    /// data), since the peer's handshake isn't available until the QP is up.
    pub fn accept_begin(listener: &Listener, config: &HordConfig) -> io::Result<Connection> {
        let (send_wr, recv_wr) = Self::qp_sizing(config);
        listener.accept(send_wr, recv_wr, config.cm)
    }

    /// Non-blocking [`accept_begin`](Self::accept_begin): return the next pending
    /// connection, or `Ok(None)` if none is queued right now. Requires the
    /// listener to be in non-blocking mode ([`Listener::set_nonblocking`]); pair
    /// with [`Listener::cm_fd`] so an event loop can park on the fd and call this
    /// only when it is readable. This is the accept primitive `hord-async`'s
    /// `HordListener` builds its async accept loop on, so a graceful-shutdown
    /// signal can interrupt accepting instead of blocking inside the CM channel.
    pub fn try_accept_begin(
        listener: &Listener,
        config: &HordConfig,
    ) -> io::Result<Option<Connection>> {
        let (send_wr, recv_wr) = Self::qp_sizing(config);
        listener.try_accept(send_wr, recv_wr, config.cm)
    }

    /// QP send/recv-queue sizing for a connection (server *or* client): the data
    /// pools plus the control lane's reserved WRs, the split-mode transfer headroom,
    /// and the handshake's one send + one recv. Shared by
    /// [`accept_begin`](Self::accept_begin), [`try_accept_begin`](Self::try_accept_begin),
    /// and [`connect`](Self::connect) so the two ends can't drift.
    fn qp_sizing(config: &HordConfig) -> (usize, usize) {
        (
            config.send_pool_size + CTRL_SEND_SLOTS + HS_SEND_SLOTS,
            recv_wr_count(config) + HS_RECV_SLOTS,
        )
    }

    /// Server side, phase two: register buffers, post receives, establish the
    /// connection, then exchange the HORD handshake as the first messages over
    /// the QP and apply the negotiated capabilities.
    pub fn from_accepted(conn: Connection, config: &HordConfig) -> io::Result<HordStream> {
        let mut s = HordStream::new_common(conn, config)?;
        s.conn.accept_finish()?;
        let peer = s.exchange_handshake(config)?;
        s.apply_peer(&peer)?;
        Ok(s)
    }

    /// Client side: connect to `ip:port` and complete the HORD handshake.
    pub fn connect(ip: &str, port: u16, config: &HordConfig) -> io::Result<HordStream> {
        let (send_wr, recv_wr) = Self::qp_sizing(config);
        let conn = Connection::connect(ip, port, send_wr, recv_wr, config.cm)?;
        let mut s = HordStream::new_common(conn, config)?;
        s.conn.connect_finish()?;
        let peer = s.exchange_handshake(config)?;
        s.apply_peer(&peer)?;
        Ok(s)
    }

    /// Build the handshake we advertise: capabilities plus, when we offer split
    /// mode, the transfer-credit window (§7.7.6) we can receive — sized to the
    /// recv headroom (`split_credits`) we pre-post. Per §5.3, split mode is only
    /// advertised when zero-copy is too.
    ///
    /// The capability bit and the credit count are advertised together: we set
    /// `SPLIT_MODE_CAPABLE` only when the advertised window is non-zero. Otherwise
    /// a 0-credit advert (a `split_credits == 0` config, or a value that the
    /// `u16` wire field truncates to 0) would set the bit while the peer's
    /// `negotiate_split` declines on the zero window — leaving the two ends
    /// disagreeing on whether split negotiated. The `usize -> u16` cast saturates
    /// so a window above `u16::MAX` advertises the max rather than wrapping.
    fn my_handshake(config: &HordConfig) -> Handshake {
        let credits = u16::try_from(config.split_credits).unwrap_or(u16::MAX);
        let split = config.split_mode && config.zero_copy && credits > 0;
        Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16)
            .with_zero_copy(config.zero_copy)
            .with_split_mode(split)
            .with_split_credits(if split { credits } else { 0 })
    }

    /// Allocate + register the buffer pools and pre-post all receive buffers.
    /// Leaves `send_credits` / `payload_cap` unset until the peer handshake is
    /// known (see [`apply_peer`]).
    fn new_common(conn: Connection, config: &HordConfig) -> io::Result<HordStream> {
        let msg_size = config.max_message_size;
        let recv_pool = config.recv_pool_size;
        let send_pool = config.send_pool_size;
        assert!(msg_size > ENVELOPE_LEN, "max_message_size too small");

        // recv carries the eager base — the data pool plus the always-posted
        // control slack; the split-mode transfer headroom is registered + posted
        // lazily in `apply_peer` (only if split negotiates), so it is *not* part
        // of this MR. send carries the data pool plus the reserved control send
        // slot (the highest index, `send_pool`).
        let recv_base = recv_base_count(config);
        let split_headroom = split_slack(config);
        let send_slots = send_pool + CTRL_SEND_SLOTS;

        // `register_buffer` allocates the (UnsafeCell-backed) storage and ties
        // each MR's lifetime to the PD via an `Arc<Connection>`, so teardown
        // ordering is no longer something `Drop` has to get right by hand.
        //
        // The Arc is used purely for shared *ownership* (PD outlives the MRs);
        // every clone lives and dies on this one thread — `HordStream` is
        // `!Send` (the buffers hold raw pointers) — so we accept the atomic
        // refcount rather than assert an unsound `Sync` on `Connection`, whose
        // shim methods are not safe to call concurrently.
        #[allow(clippy::arc_with_non_send_sync)]
        let conn = Arc::new(conn);
        let recv = conn.register_buffer(recv_base * msg_size, ACCESS_LOCAL_WRITE)?;
        let send = conn.register_buffer(send_slots * msg_size, ACCESS_LOCAL_WRITE)?;
        // Dedicated buffer for the first-message handshake: recv half + send half.
        let handshake = conn.register_buffer(2 * HANDSHAKE_LEN, ACCESS_LOCAL_WRITE)?;

        let mut s = HordStream {
            conn,
            recv,
            split_recv: None,
            send,
            _handshake: handshake,
            msg_size,
            payload_cap: 0,
            recv_pool,
            split_base: recv_base,
            split_headroom,
            send_pool,
            send_free: (0..send_pool).rev().collect(),
            send_credits: 0,
            grant_pending: 0,
            ctrl_slot: send_pool,
            ctrl_send_busy: false,
            tx_stage: Vec::new(),
            rx_ready: VecDeque::new(),
            peer_closed: false,
            cm_watch: false,
            // Sentinels until `apply_peer` captures the real values post-handshake.
            // `UNIX_EPOCH` is a const (no clock syscall on a value that is always
            // overwritten before any accessor can observe it).
            peer_addr: None,
            established_at: SystemTime::UNIX_EPOCH,
            zero_copy: config.zero_copy,
            writes_outstanding: 0,
            split_mode: config.split_mode,
            completed_transfers: VecDeque::new(),
            peer_split_credits: 0,
            imm_outstanding: 0,
        };
        // Post the handshake recv FIRST: a QP consumes receive WRs in the order
        // they were posted, and the peer's handshake is its very first send, so
        // it must match the first-posted WR. The data pool follows for the
        // messages that come after the handshake. (Each data completion still
        // self-identifies its slot by wr_id, so the order within the pool is
        // immaterial to the data path.)
        s.post_handshake_recv()?;
        s.post_all_recvs()?;
        Ok(s)
    }

    fn post_all_recvs(&mut self) -> io::Result<()> {
        // Post the eager base — the data pool + the always-posted control slack
        // (slots `[0, split_base)`, all in `recv`). These must be live before the
        // QP carries traffic. The split-mode transfer headroom is *not* posted
        // here — `apply_peer` registers and posts it lazily, and only if split mode
        // negotiates (`post_split_recvs`).
        for slot in 0..self.split_base {
            self.post_recv_slot(slot)?;
        }
        Ok(())
    }

    /// Pre-post the receive WR for the peer's handshake into the recv half of
    /// the handshake buffer. Posted in `new_common` (QP in INIT) before the QP
    /// reaches RTS, so the peer's handshake send can never RNR.
    fn post_handshake_recv(&self) -> io::Result<()> {
        let ptr = self._handshake.as_mut_ptr();
        let lkey = self._handshake.lkey();
        // SAFETY: `ptr` is the base of the handshake MR (`lkey`), live until the
        // completion is reaped in `exchange_handshake`; the buffer outlives it.
        unsafe { self.conn.post_recv(HS_RECV_WR_ID, ptr, HANDSHAKE_LEN as u32, lkey) }
    }

    /// Exchange the HORD handshake as the first messages over the established QP:
    /// send ours (landing in the recv WR the peer pre-posted), then busy-poll the
    /// CQ for both our send completion and the peer's handshake, and decode it.
    ///
    /// This replaces the old CM-private-data handshake (spec §5.3 / §12.1): the
    /// peer's handshake is not available until the QP is up, so it is exchanged
    /// here rather than returned from `accept`/`connect`. It runs once, before any
    /// data flows, reaping exactly the two reserved handshake completions; any
    /// other completion at this point is a protocol error.
    fn exchange_handshake(&self, config: &HordConfig) -> io::Result<Handshake> {
        let my = HordStream::my_handshake(config).encode();
        // Our handshake lives in the send half (offset HANDSHAKE_LEN).
        self._handshake.copy_in(HANDSHAKE_LEN, &my);
        let lkey = self._handshake.lkey();
        // SAFETY: the send region is within the handshake MR and stays live until
        // its send completion is reaped in the loop below.
        let send_ptr = unsafe { self._handshake.as_mut_ptr().add(HANDSHAKE_LEN) };
        unsafe {
            self.conn
                .post_send(HS_SEND_WR_ID, send_ptr, HANDSHAKE_LEN as u32, lkey)?;
        }

        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        let (mut got_recv, mut got_send) = (false, false);
        // Bytes the peer actually sent in its handshake (from the recv
        // completion); decode only these, never trailing buffer bytes.
        let mut recv_len = 0usize;
        while !(got_recv && got_send) {
            match self.conn.poll()? {
                Some(wc) => {
                    if !wc.is_success() {
                        return Err(io::Error::other(format!(
                            "handshake completion failed (wr_id {:#x}, status {})",
                            wc.wr_id, wc.status
                        )));
                    }
                    match wc.wr_id {
                        HS_RECV_WR_ID => {
                            got_recv = true;
                            recv_len = wc.byte_len as usize;
                        }
                        HS_SEND_WR_ID => got_send = true,
                        other => {
                            return Err(io::Error::other(format!(
                                "unexpected completion during handshake (wr_id {other:#x})"
                            )))
                        }
                    }
                }
                None => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "peer did not complete the HORD handshake in time",
                        ));
                    }
                    std::hint::spin_loop();
                }
            }
        }

        // Decode exactly the bytes received (capped at the buffer); a short first
        // message must not be padded with stale buffer bytes. `Handshake::decode`
        // rejects anything under 14 bytes.
        let n = recv_len.min(HANDSHAKE_LEN);
        let mut buf = [0u8; HANDSHAKE_LEN];
        self._handshake.copy_out(0, &mut buf);
        Handshake::decode(&buf[..n])
    }

    fn apply_peer(&mut self, peer: &Handshake) -> io::Result<()> {
        let effective = self.msg_size.min(peer.max_message_size as usize);
        if effective <= ENVELOPE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("negotiated message size {effective} too small"),
            ));
        }
        self.payload_cap = effective - ENVELOPE_LEN;
        self.send_credits = peer.max_recv_buffers as u32;
        self.zero_copy &= peer.zero_copy_capable();
        // Split mode requires zero-copy (§5.3) and a non-zero advertised transfer
        // window (§7.7.6); `negotiate_split` enforces both, so split can never be
        // on without zero-copy even if a peer set the bit in violation of the
        // spec, nor against a peer that can't receive any write-with-imm.
        self.split_mode = negotiate_split(self.split_mode, self.zero_copy, peer);
        // The window that bounds *our* sender (`begin_rdma_write_inner`): how many
        // write-with-imm transfers the peer can receive concurrently. Only
        // meaningful when split negotiated, which guarantees it is >= 1.
        self.peer_split_credits = peer.split_credits as u32;
        // Now — and only now — register + post our split-mode receive headroom, so
        // a connection that declined split never pinned `split_credits *
        // max_message_size` (a non-split peer leaves `split_mode` false here, so we
        // skip it). `split_headroom == 0` means we never advertised split, so there
        // is nothing to post. RNR-safe despite the QP having been live since before
        // the handshake: the eager base pool is already posted, so any recv — incl.
        // an early write-with-imm — always has a WR to land in, and a peer cannot
        // legitimately send one until it has seen the `split_credits` we advertised
        // in the handshake just exchanged. The split WRs are pure additional FIFO
        // headroom appended behind the base pool.
        if self.split_mode && self.split_headroom > 0 {
            self.post_split_recvs()?;
        }
        // Enable synchronous half-close detection: flip the CM channel non-blocking
        // so the busy-poll can notice a peer-initiated *graceful* disconnect. Unlike
        // a hard teardown, a graceful disconnect (peer `rdma_disconnect`) leaves our
        // recv WRs un-flushed, so without watching the CM channel a blocked reader
        // would never see EOF. Best-effort: on failure we run without it (data path
        // unaffected), exactly as the async wrapper does. Safe here — all of setup's
        // *blocking* CM waits (resolve / establish) have completed by now.
        self.cm_watch = self.conn.set_cm_nonblock().is_ok();
        // Capture the connection metadata now. The handshake is complete and the
        // connection is ready to serve — the RDMA analogue of TCP "Connected" — so
        // this times readiness, not bare QP establishment. Resolving `peer_addr`
        // here (post-establishment) also pins the authoritative address: the CM has
        // resolved the peer's address by now and it is fixed for the connection's
        // life, so one read here replaces a per-call FFI and makes `conn_meta` a true
        // snapshot. (`apply_peer` is the last setup step on both the accept and
        // connect paths.)
        self.peer_addr = self.conn.peer_addr();
        self.established_at = SystemTime::now();
        Ok(())
    }

    /// Effective max payload bytes per RDMA message after negotiation.
    pub fn payload_capacity(&self) -> usize {
        self.payload_cap
    }

    /// The peer's address as resolved by the connection manager at handshake
    /// completion, or `None` if the CM could not report one. Captured once (the
    /// address is fixed for the connection's life), so this is a cheap field read,
    /// not a per-call CM query. For RoCEv2 this IP is the peer's GID in address
    /// form — see [`ConnMeta::peer_addr`] and the listener trust-model note before
    /// keying tenancy or trust on it.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    /// Wall-clock time the handshake completed and the connection became ready to
    /// serve (the analogue of a TCP `Connected` timestamp). See
    /// [`ConnMeta::established_at`].
    pub fn established_at(&self) -> SystemTime {
        self.established_at
    }

    /// Snapshot of this connection's [`ConnMeta`] — peer identity, establishment
    /// time, and negotiated capabilities — for host logging / multi-tenancy. All
    /// fields were captured at handshake completion, so this is a consistent
    /// snapshot built from cheap field reads.
    pub fn conn_meta(&self) -> ConnMeta {
        ConnMeta {
            peer_addr: self.peer_addr,
            established_at: self.established_at,
            zero_copy_negotiated: self.zero_copy_negotiated(),
            split_mode_negotiated: self.split_mode_negotiated(),
        }
    }

    /// Begin graceful disconnect.
    pub fn disconnect(&self) {
        self.conn.disconnect();
    }

    // ---- zero-copy extension (spec §7) -------------------------------------

    /// Whether the zero-copy extension is usable on this connection: both we and
    /// the peer advertised `ZERO_COPY_CAPABLE` in the handshake. The zero-copy
    /// HTTP layer (`hord-zerocopy`) gates on this before offering / honouring an
    /// `X-HORD-RDMA-Write` exchange.
    pub fn zero_copy_negotiated(&self) -> bool {
        self.zero_copy
    }

    /// Whether protocol splitting (spec §7.7) is usable on this connection: both
    /// peers advertised `SPLIT_MODE_CAPABLE` *and* `ZERO_COPY_CAPABLE`. The
    /// zero-copy HTTP layer gates on this before serving a split-mode request
    /// (one carrying an `id`) with write-with-immediate.
    pub fn split_mode_negotiated(&self) -> bool {
        self.split_mode
    }

    /// Pop the next completed split-mode transfer ID, if a `RECV_RDMA_WITH_IMM`
    /// completion has been reaped (drive the CQ first via
    /// [`drain_completions`](Self::drain_completions) or
    /// [`poll_completed_transfer`](Self::poll_completed_transfer)). This is the
    /// non-blocking data-plane primitive (spec §7.7.5); the async wrapper builds
    /// on it.
    pub fn next_completed_transfer(&mut self) -> Option<u32> {
        self.completed_transfers.pop_front()
    }

    /// Whether any completed split-mode transfer is queued.
    pub fn has_completed_transfers(&self) -> bool {
        !self.completed_transfers.is_empty()
    }

    /// Blocking data-plane receive (sync reference path): busy-poll the CQ until
    /// a split-mode transfer completes, returning its 32-bit ID — or `None` if
    /// the connection closes first. Mirrors [`rdma_write_all`](Self::rdma_write_all)'s
    /// busy-poll style; the async stream parks on the completion fd instead.
    pub fn poll_completed_transfer(&mut self) -> io::Result<Option<u32>> {
        loop {
            if let Some(id) = self.completed_transfers.pop_front() {
                return Ok(Some(id));
            }
            if self.peer_closed {
                return Ok(None);
            }
            self.pump(true)?;
        }
    }

    /// Register `len` zeroed bytes the **peer may RDMA-write into** (client side:
    /// the destination buffer advertised via `X-HORD-RDMA-Write`). Registered
    /// `LOCAL_WRITE | REMOTE_WRITE`; expose its address via
    /// [`RegisteredBuffer::as_mut_ptr`] and key via [`RegisteredBuffer::rkey`].
    pub fn register_remote_writable(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.conn
            .register_buffer(len, ACCESS_LOCAL_WRITE | ACCESS_REMOTE_WRITE)
    }

    /// Register `len` bytes to RDMA-write **from** (server side: the source of a
    /// zero-copy response body). The NIC only reads it, so `LOCAL_WRITE` access
    /// is sufficient; fill it with [`RegisteredBuffer::copy_in`].
    pub fn register_source(&self, len: usize) -> io::Result<RegisteredBuffer> {
        self.conn.register_buffer(len, ACCESS_LOCAL_WRITE)
    }

    /// Register **caller-owned** memory `[ptr, ptr+len)` as a zero-copy write
    /// *source* (spec §7, Milestone 3), returning an [`Mr`] (its `lkey`) without
    /// copying the bytes into a HORD buffer first. Registered `LOCAL_WRITE` (the NIC
    /// only reads a source). Combine its spans with [`WriteSegment::from_mr`] and
    /// deliver them with [`rdma_write_gather_all`](Self::rdma_write_gather_all).
    ///
    /// # Safety
    /// `[ptr, ptr+len)` must stay live, resident, and unmodified until the returned
    /// [`Mr`] is dropped — and across any in-flight write that references it (see
    /// [`Connection::register_external`]). The NIC is quiesced before buffers drop
    /// (the stream destroys the QP in [`Connection::shutdown`] before MRs go).
    pub unsafe fn register_external(&self, ptr: *mut u8, len: usize) -> io::Result<Mr> {
        // SAFETY: forwarded — the caller upholds the residency/lifetime contract.
        unsafe { self.conn.register_external(ptr, len, ACCESS_LOCAL_WRITE) }
    }

    /// The most scatter/gather segments one gather-write WR carries on this
    /// connection (the QP's `max_send_sge`); a longer [`WriteSegment`] list spans
    /// multiple WRs. See [`Connection::max_send_sge`].
    pub fn max_send_sge(&self) -> usize {
        self.conn.max_send_sge()
    }

    // ---- completion engine -------------------------------------------------

    /// Poll for and process exactly one completion. With `block`, busy-waits
    /// until a completion is available; otherwise returns `Ok(false)` when the
    /// CQ is empty. Returns `Ok(true)` if a completion was processed.
    ///
    /// While blocking, it periodically consults the CM channel for a peer-initiated
    /// graceful half-close (see [`poll_cm_disconnect`](Self::poll_cm_disconnect)):
    /// such a disconnect need not flush our recv WRs, so without this a reader
    /// blocked on data that will never come would spin forever. On detecting it the
    /// stream is marked closed and `pump` returns `Ok(false)` (no completion
    /// processed) so the caller re-checks `peer_closed` — `read` then sees EOF,
    /// `poll_completed_transfer` returns `None`. The non-blocking path is untouched:
    /// the async reactor handles the CM channel itself and must not double-consume it.
    fn pump(&mut self, block: bool) -> io::Result<bool> {
        let mut spins: u32 = 0;
        loop {
            match self.conn.poll()? {
                Some(wc) => {
                    self.handle_completion(wc)?;
                    return Ok(true);
                }
                None => {
                    if !block {
                        return Ok(false);
                    }
                    spins += 1;
                    if spins >= CM_DISCONNECT_POLL_SPINS {
                        spins = 0;
                        if self.poll_cm_disconnect()? {
                            return Ok(false);
                        }
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Non-blocking peer half-close check for the synchronous busy-poll: if the CM
    /// channel is being watched (flipped non-blocking after the handshake — see
    /// [`apply_peer`](Self::apply_peer)), poll it once for a peer-initiated
    /// disconnect and mark the stream closed if one is seen. Returns whether the
    /// peer is now observed gone. A no-op (`Ok(false)`) when the flip failed at
    /// setup. This is the sync analogue of the async reactor's `poll_cm_event`.
    fn poll_cm_disconnect(&mut self) -> io::Result<bool> {
        if !self.cm_watch {
            return Ok(false);
        }
        if self.conn.check_disconnect()? {
            self.peer_closed = true;
            return Ok(true);
        }
        Ok(false)
    }

    fn handle_completion(&mut self, wc: Completion) -> io::Result<()> {
        // Data-plane completion (spec §7.7): a write-with-immediate landed its
        // payload in the application's remote-writable buffer and consumed one of
        // our posted recv WRs to deliver the 32-bit transfer ID. This is the
        // dispatcher's demux-by-opcode (§7.7.1) — recognise it *before* the
        // generic recv path, which would otherwise try to decode an envelope from
        // a slot the write never touched. No data credit is returned: the peer
        // spent no stream credit (the recv WR was an implicit transfer credit,
        // §7.7.6); we simply repost the slot to keep the window full (§7.7.5).
        if wc.opcode == Opcode::RecvRdmaWithImm {
            if !wc.is_success() {
                self.peer_closed = true;
                return Ok(());
            }
            self.completed_transfers.push_back(wc.imm_data);
            self.post_recv_slot(slot_of(wc.wr_id))?;
            return Ok(());
        }

        // One-sided RDMA write (zero-copy). It belongs to neither the send pool
        // nor the recv pool — just a counter — so reap it first, on both the
        // success and failure paths. A failed write means the peer's buffer is in
        // an undefined state (spec §7.4 mid-write failure), so close the stream;
        // `rdma_write_all` then surfaces it rather than reporting `complete`.
        if is_write(wc.wr_id) {
            self.writes_outstanding = self.writes_outstanding.saturating_sub(1);
            // The imm-bearing WR of a split transfer (§7.7.6): its ack means the
            // peer consumed its recv WR and will repost it, so free the transfer
            // credit. Freed even on failure — a closed stream returns no credit to
            // a window that no longer matters.
            if is_imm_write(wc.wr_id) {
                self.imm_outstanding = self.imm_outstanding.saturating_sub(1);
            }
            if !wc.is_success() {
                self.peer_closed = true;
            }
            return Ok(());
        }

        if !wc.is_success() {
            // Flush or transport error (commonly seen when the peer
            // disconnects and our outstanding recvs are flushed). Treat as a
            // closed connection rather than a hard error so reads see EOF.
            // Only reclaim a *send* slot: a flushed recv WR (or a write-with-imm
            // recv whose opcode the NIC didn't report as RecvRdmaWithImm) carries
            // a recv-slot wr_id, and pushing that onto `send_free` would corrupt
            // the send free-list with a recv-pool index.
            self.peer_closed = true;
            if is_send(wc.wr_id) {
                self.reclaim_send(wc.wr_id);
            }
            return Ok(());
        }

        if is_send(wc.wr_id) {
            self.reclaim_send(wc.wr_id);
            return Ok(());
        }

        // Receive completion.
        debug_assert_eq!(wc.opcode, Opcode::Recv);
        let slot = slot_of(wc.wr_id);
        // Resolve the slot to its backing MR and the slot's offset within it: the
        // recv queue is one FIFO, so a data SEND can land in either the data pool
        // or the split headroom regardless of message type (see `recv_slot`). `off`
        // is therefore relative to *that* MR, and `ReadyMsg` carries it forward so
        // `read` copies the payload from the same MR.
        let (buf, off) = self.recv_slot(slot);
        // A successful recv can never deliver more than the posted slot size, but
        // clamp defensively so all slot-relative slicing below stays in bounds
        // regardless of what the NIC reports.
        let n = (wc.byte_len as usize).min(self.msg_size);
        if n < ENVELOPE_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("runt message: {n} bytes < envelope header"),
            ));
        }
        // Decode the envelope through a stack copy — never a slice reference
        // over registered (DMA-able) memory.
        let mut hdr = [0u8; ENVELOPE_LEN];
        buf.copy_out(off, &mut hdr);
        let env = Envelope::decode(&hdr);
        // Credits the peer granted us.
        self.send_credits = self.send_credits.saturating_add(env.credits as u32);

        let avail = n - ENVELOPE_LEN;
        let plen = if env.is_credit_only() {
            0
        } else {
            (env.length as usize).min(avail)
        };

        if plen > 0 {
            // Hold the payload in place and defer the re-post / credit-return
            // until `read()` drains it (backpressure — see [`ReadyMsg`]).
            let start = off + ENVELOPE_LEN;
            self.rx_ready.push_back(ReadyMsg {
                slot,
                start,
                end: start + plen,
            });
        } else if env.is_credit_only() {
            // Control message: re-post immediately to keep the control slack
            // topped up, but DON'T return a data credit — the peer self-clocks
            // its control lane on send completions and spent no data credit for
            // this, so granting one back would inflate its window.
            self.post_recv_slot(slot)?;
        } else {
            // Zero-length data message: it did consume a data credit, so
            // re-post and return the credit now (no payload to hold).
            self.post_recv_slot(slot)?;
            self.grant_pending += 1;
        }
        Ok(())
    }

    /// Reclaim a completed send: a control send frees the reserved control
    /// slot; any other send returns its slot to the data free list.
    fn reclaim_send(&mut self, wr_id: u64) {
        if is_ctrl_send(wr_id) {
            self.ctrl_send_busy = false;
        } else {
            self.send_free.push(slot_of(wr_id));
        }
    }

    /// Resolve a recv slot to its backing MR and the slot's byte offset within
    /// that MR. Slots `[0, split_base)` live in the eager `recv` pool; slots at or
    /// above `split_base` live in the lazily-registered `split_recv` MR.
    ///
    /// Both the header decode and the payload copy route through here rather than
    /// assuming `recv`, because the QP consumes recv WRs FIFO *regardless of
    /// message type*: a data SEND and a split write-with-imm draw from the same
    /// queue, so once reposts have reordered the queue either slot index can come
    /// back on either kind of completion. A data SEND can therefore legitimately
    /// land in a split slot (and vice versa), and its bytes must be read from the
    /// MR that slot actually lives in.
    fn recv_slot(&self, slot: usize) -> (&RegisteredBuffer, usize) {
        if slot < self.split_base {
            (&self.recv, slot * self.msg_size)
        } else {
            // Present whenever a split slot can be in flight: it is posted in
            // `apply_peer` before split mode is reported as negotiated, and a
            // split-slot completion can only arrive after that post.
            let buf = self
                .split_recv
                .as_ref()
                .expect("recv on a split slot with no split MR registered");
            (buf, (slot - self.split_base) * self.msg_size)
        }
    }

    /// Post (or re-post) the receive WR for `slot` into its backing MR (`recv` or
    /// `split_recv`, per [`recv_slot`](Self::recv_slot)). The single recv-posting
    /// primitive: the initial fills ([`post_all_recvs`](Self::post_all_recvs),
    /// [`post_split_recvs`](Self::post_split_recvs)) and the per-message re-post all
    /// route through here, so the addr/lkey derivation and SAFETY reasoning live in
    /// one place. Only fails on a dead QP; on failure the connection is marked
    /// closed so reads/writes fail fast instead of silently operating with a
    /// shrunken receive window.
    fn post_recv_slot(&mut self, slot: usize) -> io::Result<()> {
        let (buf, off) = self.recv_slot(slot);
        // SAFETY: `buf` + `off` is slot `slot` inside its MR (`recv` or
        // `split_recv`), each of which holds an Arc<Connection> and so outlives
        // the QP.
        let repost = unsafe {
            let addr = buf.as_mut_ptr().add(off);
            self.conn
                .post_recv(recv_wr_id(slot), addr, self.msg_size as u32, buf.lkey())
        };
        if let Err(e) = repost {
            self.peer_closed = true;
            return Err(e);
        }
        Ok(())
    }

    /// Register the split-mode transfer headroom (spec §7.7.6) and post its recv
    /// WRs. Called once, from `apply_peer`, and only when split mode negotiated —
    /// so the `split_headroom * msg_size` region is never pinned on a connection
    /// that declined split. The WRs take slots `[split_base, split_base +
    /// split_headroom)` and append to the recv-queue tail behind the eager base.
    fn post_split_recvs(&mut self) -> io::Result<()> {
        debug_assert!(self.split_recv.is_none(), "split headroom posted twice");
        let n = self.split_headroom;
        let buf = self.conn.register_buffer(n * self.msg_size, ACCESS_LOCAL_WRITE)?;
        // Store the MR *before* posting, for two reasons: `post_recv_slot` resolves
        // a split slot through `recv_slot`, which reads `self.split_recv`; and a
        // mid-loop post failure must not drop the MR while the WRs already posted
        // reference it (teardown quiesces the NIC before any MR is released).
        self.split_recv = Some(buf);
        for slot in self.split_base..self.split_base + n {
            self.post_recv_slot(slot)?;
        }
        Ok(())
    }

    // ---- sending -----------------------------------------------------------

    /// Whether a *data* message can be posted right now: the stream is live and
    /// we hold both a free send slot and a data credit.
    /// [`post_data_message`](Self::post_data_message) requires this; the blocking
    /// and async senders both wait for it to become true.
    fn can_send_data(&self) -> bool {
        !self.peer_closed && !self.send_free.is_empty() && self.send_credits > 0
    }

    /// Post one *data* message carrying `payload` (<= payload_cap), assuming a
    /// slot + credit are available (caller checks [`can_send_data`](Self::can_send_data)).
    /// Non-blocking: this is the shared posting primitive both the busy-poll
    /// [`Write`] facade (via [`try_write`](Self::try_write)) and the async path
    /// build on. Callers that need to block until a slot/credit frees up loop on
    /// `can_send_data` + [`pump`](Self::pump) themselves.
    fn post_data_message(&mut self, payload: &[u8]) -> io::Result<()> {
        debug_assert!(payload.len() <= self.payload_cap);
        debug_assert!(self.can_send_data());
        let slot = self.send_free.pop().unwrap();
        let grant = self.grant_pending.min(u16::MAX as u32);
        let env = Envelope {
            length: payload.len() as u32,
            credits: grant as u16,
            flags: 0,
        };
        // SAFETY (post): the slot lives in send_buf / the send MR and stays put
        // until the completion is reaped.
        if let Err(e) = unsafe { self.post_into(slot, send_wr_id(slot), &env, payload) } {
            // The message never went out: return the slot to the free list and
            // mark the connection closed (post_send only fails on a dead QP).
            // Leave send_credits / grant_pending untouched — nothing was spent
            // and the grant will be re-stamped on a later message.
            self.send_free.push(slot);
            self.peer_closed = true;
            return Err(e);
        }
        self.send_credits -= 1;
        self.grant_pending -= grant;
        Ok(())
    }

    /// Stamp `env` + `payload` into send slot `slot` and post it with `wr_id`.
    ///
    /// # Safety
    /// `slot` must be a valid send slot whose backing storage stays pinned in
    /// the send MR until the matching completion is reaped (the stream owns
    /// `send_buf` for its whole life, so this holds for any slot index < the
    /// allocated count).
    unsafe fn post_into(
        &mut self,
        slot: usize,
        wr_id: u64,
        env: &Envelope,
        payload: &[u8],
    ) -> io::Result<()> {
        let off = slot * self.msg_size;
        // Stamp the envelope into a stack buffer, then raw-copy header and
        // payload into the send slot — no slice reference over registered memory.
        let mut hdr = [0u8; ENVELOPE_LEN];
        env.encode(&mut hdr);
        self.send.copy_in(off, &hdr);
        self.send.copy_in(off + ENVELOPE_LEN, payload);
        let total = (ENVELOPE_LEN + payload.len()) as u32;
        let base = self.send.as_mut_ptr();
        // SAFETY: `base + off` is the send slot inside `send` / the MR; it stays
        // pinned until the matching completion is reaped.
        unsafe {
            let addr = base.add(off);
            self.conn.post_send(wr_id, addr, total, self.send.lkey())
        }
    }

    /// Return owed data credits to the peer over the control lane: a
    /// `CREDIT_ONLY` message on the reserved control slot. Costs no data credit
    /// and is bounded to one in-flight message (`ctrl_send_busy`), which
    /// self-clocks on its send completion. Non-blocking: if the control slot is
    /// busy we simply skip and let the in-flight message carry the grant.
    fn send_credit_return(&mut self) -> io::Result<()> {
        if self.ctrl_send_busy || self.peer_closed || self.grant_pending == 0 {
            return Ok(());
        }
        let grant = self.grant_pending.min(u16::MAX as u32);
        let env = Envelope {
            length: 0,
            credits: grant as u16,
            flags: env_flags::CREDIT_ONLY,
        };
        let slot = self.ctrl_slot;
        // SAFETY (post): the control slot lives in send_buf / the send MR and
        // stays put until the completion is reaped.
        if let Err(e) = unsafe { self.post_into(slot, ctrl_send_wr_id(slot), &env, &[]) } {
            self.peer_closed = true;
            return Err(e);
        }
        self.ctrl_send_busy = true;
        self.grant_pending -= grant;
        Ok(())
    }

    /// Return owed credits if we owe at least `threshold` of them. Callers pass
    /// a high threshold for proactive top-ups (avoid chatty credit-only traffic)
    /// and `1` when something is actually blocked on the peer granting back.
    fn maybe_return_credits(&mut self, threshold: u32) -> io::Result<()> {
        if self.grant_pending >= threshold {
            self.send_credit_return()?;
        }
        Ok(())
    }

    /// Proactive credit-return threshold: a quarter of the data pool keeps a
    /// one-directional bulk transfer flowing without a credit-only storm.
    fn proactive_threshold(&self) -> u32 {
        (self.recv_pool as u32 / 4).max(1)
    }

    // ---- non-blocking API --------------------------------------------------
    //
    // These are the state machine without the wait. The blocking `Read`/`Write`
    // impls below drive them by busy-polling (`pump(true)`); an async wrapper
    // drives the *same* methods off the CQ completion-channel fd. Neither path
    // duplicates the credit / control-lane logic.

    /// Process every completion currently in the CQ, without waiting. Returns
    /// the number handled. The async driver calls this after the CQ fd signals.
    pub fn drain_completions(&mut self) -> io::Result<usize> {
        let mut n = 0;
        while self.pump(false)? {
            n += 1;
        }
        Ok(n)
    }

    /// Non-blocking write. Accepts as many bytes of `buf` as it can right now —
    /// sending full messages and staging the sub-`payload_cap` remainder — and
    /// returns the count accepted. A return of `0` for a non-empty `buf` means no
    /// progress was possible (no send slot/credit); it has already returned any
    /// owed credits over the control lane, so the caller should wait for a
    /// completion and retry. Maintains the invariant that the staging buffer
    /// holds `< payload_cap` bytes between calls.
    pub fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.peer_closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection closed"));
        }
        let cap = self.payload_cap;
        let mut input = buf;
        let mut consumed = 0;

        // 1. Top up an existing partial staged message.
        if !self.tx_stage.is_empty() {
            let need = cap - self.tx_stage.len();
            let take = need.min(input.len());
            if take < need {
                // Too little to fill the stage; absorb it all, still partial.
                self.tx_stage.extend_from_slice(input);
                return Ok(consumed + input.len());
            }
            // Filling the stage to `cap` means we must send it before accepting
            // more (to keep the stage < cap). That needs a slot + credit.
            if !self.can_send_data() {
                self.maybe_return_credits(1)?;
                return Ok(consumed);
            }
            let mut staged = std::mem::take(&mut self.tx_stage);
            staged.extend_from_slice(&input[..take]);
            self.post_data_message(&staged)?;
            input = &input[take..];
            consumed += take;
        }

        // 2. Whole messages straight from the caller's buffer.
        while input.len() >= cap {
            if !self.can_send_data() {
                self.maybe_return_credits(1)?;
                return Ok(consumed);
            }
            self.post_data_message(&input[..cap])?;
            input = &input[cap..];
            consumed += cap;
        }

        // 3. Stage the sub-cap remainder for the next write or flush.
        if !input.is_empty() {
            self.tx_stage.extend_from_slice(input);
            consumed += input.len();
        }
        Ok(consumed)
    }

    /// Try to emit any staged (sub-`payload_cap`) bytes as a final message.
    /// `Ok(true)` = nothing staged, or it was sent; `Ok(false)` = staged but no
    /// slot/credit yet (owed credits have been returned; wait and retry).
    pub fn try_flush_stage(&mut self) -> io::Result<bool> {
        if self.tx_stage.is_empty() {
            return Ok(true);
        }
        if !self.can_send_data() {
            self.maybe_return_credits(1)?;
            return Ok(false);
        }
        let chunk = std::mem::take(&mut self.tx_stage);
        self.post_data_message(&chunk)?;
        Ok(true)
    }

    /// Whether any *data* send is still unacknowledged (the control slot is
    /// tracked separately). `flush` is complete once this is false.
    pub fn sends_outstanding(&self) -> bool {
        self.send_free.len() < self.send_pool
    }

    /// Non-blocking read. `Ok(None)` = no data buffered yet (would block);
    /// `Ok(Some(0))` = EOF (peer/transport closed); `Ok(Some(n))` = `n` bytes
    /// copied. Drains held receive buffers, re-posting and accruing credit on
    /// consumption exactly like the blocking [`read`](Read::read).
    pub fn try_read(&mut self, out: &mut [u8]) -> io::Result<Option<usize>> {
        if out.is_empty() {
            return Ok(Some(0));
        }
        if self.rx_ready.is_empty() {
            return Ok(if self.peer_closed { Some(0) } else { None });
        }
        let mut written = 0;
        while written < out.len() {
            let Some(&ReadyMsg { slot, start, end }) = self.rx_ready.front() else {
                break;
            };
            let take = (end - start).min(out.len() - written);
            // `start` is relative to the slot's backing MR (`recv` or the split
            // MR), so copy from the MR `recv_slot` resolves, not unconditionally
            // from `recv`.
            let (buf, _) = self.recv_slot(slot);
            buf.copy_out(start, &mut out[written..written + take]);
            written += take;
            if start + take == end {
                self.rx_ready.pop_front();
                self.post_recv_slot(slot)?;
                self.grant_pending += 1;
            } else {
                self.rx_ready.front_mut().unwrap().start = start + take;
            }
        }
        self.maybe_return_credits(self.proactive_threshold())?;
        Ok(Some(written))
    }

    /// Return owed credits to the peer. `urgent` returns any owed credit (used
    /// before an async wait, so a peer blocked on us — the #3 path — unblocks);
    /// otherwise it uses the proactive threshold to avoid a credit-only storm.
    pub fn return_owed_credits(&mut self, urgent: bool) -> io::Result<()> {
        let threshold = if urgent { 1 } else { self.proactive_threshold() };
        self.maybe_return_credits(threshold)
    }

    /// Whether the peer/transport has closed (reads will see EOF, writes error).
    pub fn is_closed(&self) -> bool {
        self.peer_closed
    }

    // ---- zero-copy write driver (spec §7) ----------------------------------

    /// Post a one-sided RDMA write of `src[src_off .. src_off+len]` into the
    /// peer's memory at `peer_addr`, authorized by `peer_rkey` (which the peer
    /// registered with `ACCESS_REMOTE_WRITE` and advertised via
    /// `X-HORD-RDMA-Write`). Non-blocking: it posts the WR(s) and returns; drive
    /// [`drain_completions`](Self::drain_completions) / [`pump`](Self::pump)
    /// until [`writes_pending`](Self::writes_pending) is false. The blocking
    /// [`rdma_write_all`](Self::rdma_write_all) and the async wrapper both build
    /// on this — no logic duplicated.
    ///
    /// Consumes **no stream credit** (one-sided — the peer posts no receive) and
    /// **no send-pool slot**, but each WR occupies a send-queue entry. The whole
    /// write must therefore fit the send queue, and must be issued while no
    /// stream *data* sends are in flight — both hold for the HTTP request →
    /// response pattern (the body is written before the response head is sent).
    pub fn begin_rdma_write(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        self.begin_rdma_write_inner(src, src_off, peer_addr, peer_rkey, len, None)
    }

    /// Like [`begin_rdma_write`](Self::begin_rdma_write), but the final WR is an
    /// RDMA write-with-immediate carrying `transfer_id` (spec §7.7 protocol
    /// splitting): it lands the last payload bytes — or, for a zero-length body,
    /// an empty WR — and delivers `transfer_id` to the peer's CQ, consuming one
    /// of its posted recv WRs (a transfer credit, §7.7.6). Gate on
    /// [`split_mode_negotiated`](Self::split_mode_negotiated). Non-blocking; the
    /// blocking [`rdma_write_all_with_imm`](Self::rdma_write_all_with_imm) builds
    /// on this.
    pub fn begin_rdma_write_with_imm(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
        transfer_id: u32,
    ) -> io::Result<()> {
        self.begin_rdma_write_inner(src, src_off, peer_addr, peer_rkey, len, Some(transfer_id))
    }

    /// Shared driver behind [`begin_rdma_write`](Self::begin_rdma_write) and
    /// [`begin_rdma_write_with_imm`](Self::begin_rdma_write_with_imm). When `imm`
    /// is `Some`, the *last* WR uses write-with-immediate; a zero-length body
    /// still emits one empty write-with-immediate so the transfer ID is always
    /// delivered (§7.7.4 step 2).
    fn begin_rdma_write_inner(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
        imm: Option<u32>,
    ) -> io::Result<()> {
        // A single contiguous source is just a 1-segment scatter-gather write, so it
        // routes through the same WR-posting core as the gather path — no separate
        // chunking loop to keep in sync. The gather core owns the `peer_closed`
        // check, the `check_write_capacity` admission, the `WRITE_WR_MAX` chunking
        // (`plan_gather` emits one 1-SGE WR per chunk), and the zero-length-body
        // imm-only WR (an all-empty 1-segment gather). `from_registered` is
        // bounds-checked, subsuming the old manual `assert!` on the source range.
        let seg = WriteSegment::from_registered(src, src_off, len);
        self.begin_rdma_write_gather_inner(&[seg], peer_addr, peer_rkey, imm)
    }

    /// Shared admission check for a one-sided write of `n_wrs` work requests
    /// (`has_imm` = the batch ends in a write-with-immediate, §7.7). Returns
    /// `InvalidInput` if it can never fit the send pool (a caller error);
    /// `WouldBlock` if the slots or the transfer-credit window (§7.7.6) are
    /// momentarily full (the facade reaps an outstanding send/write and retries);
    /// else `Ok`. Posts nothing. Used by both the single-buffer
    /// ([`begin_rdma_write_inner`](Self::begin_rdma_write_inner)) and the
    /// scatter-gather ([`begin_rdma_write_gather_inner`](Self::begin_rdma_write_gather_inner))
    /// cores, so this accounting lives in one place.
    fn check_write_capacity(&self, n_wrs: usize, has_imm: bool) -> io::Result<()> {
        // `send_free` tracks free data slots; `writes_outstanding` already hold some
        // (writes don't draw from `send_free`). The control slot stays reserved.
        let free_slots = self
            .send_free
            .len()
            .saturating_sub(self.writes_outstanding as usize);
        if n_wrs > self.send_pool {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "zero-copy write needs {n_wrs} send-queue slots, exceeds the send pool of {}",
                    self.send_pool
                ),
            ));
        }
        // Transient back-pressure (a bare kind: callers match the kind, discard text).
        if n_wrs > free_slots {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        // One imm WR is posted per call, so it needs one transfer credit (§7.7.6):
        // a write-with-imm consumes one of the peer's posted recvs, so bound our
        // in-flight imm transfers by the peer's advertised window. Back-pressure,
        // not a transport error — WouldBlock without marking the stream closed.
        if has_imm && self.imm_outstanding >= self.peer_split_credits {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }

    /// Plan a scatter-gather write: walk `segments`, packing them into work
    /// requests of at most `max_sge` SGEs **and** at most [`WRITE_WR_MAX`] bytes
    /// each, invoking `emit(remote_off, &sges)` for every WR in order (`remote_off`
    /// is that WR's running byte offset from the start of the gather). A segment
    /// longer than a WR's remaining byte budget is split across WRs; zero-length
    /// segments contribute nothing. Returns the number of WRs. Pure planning — the
    /// same deterministic packing whether `emit` counts (for admission) or posts. A
    /// single segment `<=` `WRITE_WR_MAX` is one 1-SGE WR, so the single-buffer
    /// path's WR pattern is preserved exactly.
    fn plan_gather(
        segments: &[WriteSegment<'_>],
        max_sge: usize,
        mut emit: impl FnMut(u64, &[Sge]) -> io::Result<()>,
    ) -> io::Result<usize> {
        let max_sge = max_sge.clamp(1, MAX_WRITE_SGE);
        let mut sges = [Sge { addr: 0, length: 0, lkey: 0 }; MAX_WRITE_SGE];
        let mut n_sge = 0usize; // SGEs staged in the in-progress WR
        let mut wr_bytes = 0u64; // bytes staged in the in-progress WR
        let mut remote_off = 0u64; // byte offset of the in-progress WR
        let mut wr_count = 0usize;
        for seg in segments {
            let mut seg_off = 0usize;
            while seg_off < seg.len {
                // Flush the WR once it is full by SGE count or by the byte cap.
                // `>=` not `==`: `take` is capped to `budget` so `wr_bytes` lands
                // exactly on `WRITE_WR_MAX` today, but `>=` keeps the cap robust if
                // that ever changes (never emit an over-cap WR).
                if n_sge == max_sge || wr_bytes >= WRITE_WR_MAX as u64 {
                    emit(remote_off, &sges[..n_sge])?;
                    wr_count += 1;
                    remote_off += wr_bytes;
                    n_sge = 0;
                    wr_bytes = 0;
                }
                let budget = WRITE_WR_MAX as u64 - wr_bytes;
                let take = ((seg.len - seg_off) as u64).min(budget);
                sges[n_sge] = Sge {
                    addr: seg.local_addr as u64 + seg_off as u64,
                    length: take as u32,
                    lkey: seg.lkey,
                };
                n_sge += 1;
                wr_bytes += take;
                seg_off += take as usize;
            }
        }
        if n_sge > 0 {
            emit(remote_off, &sges[..n_sge])?;
            wr_count += 1;
        }
        Ok(wr_count)
    }

    /// Shared driver behind the public scatter-gather write entry points: lay
    /// `segments` down contiguously into the peer's `[peer_addr, peer_addr+total)`
    /// (`total` = sum of segment lengths), packing them into WRs (spec §7,
    /// Milestone 3). With `imm` `Some`, the *last* WR is a write-with-immediate
    /// (§7.7); an all-empty gather with `imm` still emits one empty imm-only WR so
    /// the transfer ID is delivered (§7.7.4 step 2). Posts nothing on a
    /// back-pressure / capacity error (`WouldBlock` / `InvalidInput`, like the
    /// single-buffer path).
    fn begin_rdma_write_gather_inner(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
        imm: Option<u32>,
    ) -> io::Result<()> {
        if self.peer_closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection closed"));
        }
        let max_sge = self.conn.max_send_sge();
        // Count the WRs the packing will produce (no posting), for admission.
        let n_data_wrs = Self::plan_gather(segments, max_sge, |_, _| Ok(()))?;
        // An all-empty gather still needs one WR to carry the immediate.
        let n_wrs = if n_data_wrs == 0 && imm.is_some() { 1 } else { n_data_wrs };
        if n_wrs == 0 {
            return Ok(()); // nothing to write and no immediate to deliver
        }
        self.check_write_capacity(n_wrs, imm.is_some())?;

        // All-empty gather + imm: emit one 0-length imm-only WR. Forming the
        // (zero-length) SGE needs a valid registered (addr, lkey) — use the first
        // segment's; with no segment at all there is nothing to reference, so it is
        // a caller error (the immediate would never be sent) rather than a no-op.
        if n_data_wrs == 0 {
            let Some(seg) = segments.first() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "empty scatter-gather write with an immediate needs at least one segment",
                ));
            };
            let id = imm.expect("n_wrs == 1 with no data implies imm is Some");
            let sge = [Sge { addr: seg.local_addr as u64, length: 0, lkey: seg.lkey }];
            // SAFETY: `seg` borrows a live registered region (its `'a`); a 0-length
            // SGE reads nothing; a bad rkey fails the WR -> QP error -> peer_closed,
            // handled on the completion.
            let r = unsafe {
                self.conn
                    .post_write_gather(imm_write_wr_id(0), &sge, peer_addr, peer_rkey, Some(id))
            };
            if let Err(e) = r {
                self.peer_closed = true;
                return Err(e);
            }
            self.writes_outstanding += 1;
            self.imm_outstanding += 1;
            return Ok(());
        }

        // Post every planned WR. The imm rides the final one (§7.7.4 step 2); each
        // WR writes at `peer_addr + remote_off` so the segments land contiguously.
        // A mid-batch post failure marks the stream closed and returns the error;
        // the WRs already posted are drained by the facade before any source `Mr` is
        // freed (the use-after-free guard).
        let mut idx = 0usize;
        let post_result = Self::plan_gather(segments, max_sge, |remote_off, sges| {
            let is_last = idx == n_wrs - 1;
            let imm_here = if is_last { imm } else { None };
            let wr_id = if imm_here.is_some() {
                imm_write_wr_id(idx as u64)
            } else {
                write_wr_id(idx as u64)
            };
            // SAFETY: each SGE spans a live registered region (the `'a` borrow on
            // `segments` keeps the sources alive for the whole write); the
            // destination is authorized by the peer-supplied rkey.
            unsafe {
                self.conn
                    .post_write_gather(wr_id, sges, peer_addr + remote_off, peer_rkey, imm_here)
            }?;
            self.writes_outstanding += 1;
            if imm_here.is_some() {
                self.imm_outstanding += 1;
            }
            idx += 1;
            Ok(())
        });
        if let Err(e) = post_result {
            self.peer_closed = true;
            return Err(e);
        }
        Ok(())
    }

    /// Begin a scatter-gather one-sided RDMA write (spec §7, Milestone 3): lay the
    /// `segments` down contiguously at the peer's `[peer_addr, …]` and return
    /// immediately (the WRs are posted; the caller drains completions). The
    /// scatter-gather analogue of [`begin_rdma_write`](Self::begin_rdma_write) — for
    /// a single contiguous source that one is simpler. Back-pressure surfaces as
    /// `WouldBlock` (posting nothing), so a facade reaps and retries.
    ///
    /// The `segments` borrow keeps every source [`Mr`] / [`RegisteredBuffer`] alive
    /// for the call; the caller must keep them alive until the matching completions
    /// are reaped — which the blocking
    /// [`rdma_write_gather_all`](Self::rdma_write_gather_all) does before returning.
    ///
    /// **Capacity.** The whole gather must fit the send queue at once: it needs
    /// `ceil(total_segments / max_send_sge)` WRs (plus byte-cap splits for any
    /// segment over [`WRITE_WR_MAX`]), and if that exceeds the send pool the call
    /// fails with `InvalidInput` (a caller error, *not* retryable back-pressure). So
    /// the practical fragment ceiling is `send_pool * `[`max_send_sge`](Self::max_send_sge)
    /// (defaults: 16 × ≤16). A source fragmented beyond that must be delivered in
    /// several gather calls (drain between them), or coalesced first.
    pub fn begin_rdma_write_gather(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
    ) -> io::Result<()> {
        self.begin_rdma_write_gather_inner(segments, peer_addr, peer_rkey, None)
    }

    /// Like [`begin_rdma_write_gather`](Self::begin_rdma_write_gather), but the
    /// final WR is a write-with-immediate carrying `transfer_id` (§7.7). An empty
    /// gather still delivers the immediate via one empty WR.
    pub fn begin_rdma_write_gather_with_imm(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
        transfer_id: u32,
    ) -> io::Result<()> {
        self.begin_rdma_write_gather_inner(segments, peer_addr, peer_rkey, Some(transfer_id))
    }

    /// The blocking post→drain harness shared by the single-buffer and
    /// scatter-gather writes. Post via `begin`, retrying on `WouldBlock`
    /// back-pressure by reaping an outstanding send/write (surfacing a permanent
    /// stall if nothing is in flight to drain); then **drain every posted WR before
    /// returning** — even on a mid-batch post error — so the caller can't free a
    /// source the NIC is still DMA-reading (the use-after-free guard); then surface a
    /// post error or a mid-write peer close. Both [`rdma_write_all_inner`] and
    /// [`rdma_write_gather_all_inner`] route through here, so this drain ordering —
    /// the load-bearing safety step — lives in exactly one place.
    fn drive_write_all(
        &mut self,
        mut begin: impl FnMut(&mut Self) -> io::Result<()>,
    ) -> io::Result<()> {
        // `begin` returns `WouldBlock` having posted nothing when a send-queue slot
        // or the transfer-credit window (§7.7.6) is momentarily full. Reap to free
        // it and retry; if NOTHING is outstanding the block can never clear (e.g. an
        // imm write on a connection that never negotiated split, peer_split_credits
        // == 0), so surface it rather than spin forever.
        let posted = loop {
            match begin(self) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if !self.sends_outstanding() && !self.writes_pending() {
                        break Err(io::Error::other(
                            "RDMA write back-pressured with nothing in flight to drain",
                        ));
                    }
                    self.pump(true)?;
                }
                other => break other,
            }
        };
        // Drain every WR that posted before returning — even on a post error — or the
        // caller could drop the source (deregistering its MR / releasing an external
        // `Mr`'s pin) while the NIC is still DMA-reading it. Drain first, propagate
        // the post error after.
        while self.writes_pending() {
            self.pump(true)?;
        }
        posted?;
        if self.peer_closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed before the RDMA write completed",
            ));
        }
        Ok(())
    }

    fn rdma_write_gather_all_inner(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
        imm: Option<u32>,
    ) -> io::Result<()> {
        self.drive_write_all(|s| s.begin_rdma_write_gather_inner(segments, peer_addr, peer_rkey, imm))
    }

    /// Blocking scatter-gather write (spec §7, Milestone 3): post the gather and
    /// busy-poll (servicing interleaved stream completions) until every WR is
    /// acknowledged — at which point the bytes are in the peer's memory and acked,
    /// so the caller may report `status=complete`. The scatter-gather analogue of
    /// [`rdma_write_all`](Self::rdma_write_all). A transport failure mid-write closes
    /// the stream and returns an error (§7.4: never report `complete` on a partial
    /// write). The `segments` borrow keeps the source regions alive for the whole
    /// call; on return no DMA references them, so they may be dropped.
    pub fn rdma_write_gather_all(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
    ) -> io::Result<()> {
        self.rdma_write_gather_all_inner(segments, peer_addr, peer_rkey, None)
    }

    /// Like [`rdma_write_gather_all`](Self::rdma_write_gather_all) with a final
    /// write-with-immediate carrying `transfer_id` (§7.7).
    pub fn rdma_write_gather_all_with_imm(
        &mut self,
        segments: &[WriteSegment<'_>],
        peer_addr: u64,
        peer_rkey: u32,
        transfer_id: u32,
    ) -> io::Result<()> {
        self.rdma_write_gather_all_inner(segments, peer_addr, peer_rkey, Some(transfer_id))
    }

    /// Whether any one-sided RDMA write is still unacknowledged.
    pub fn writes_pending(&self) -> bool {
        self.writes_outstanding > 0
    }

    /// Blocking facade over [`begin_rdma_write`](Self::begin_rdma_write): post the
    /// write and busy-poll (servicing any interleaved stream completions) until
    /// every WR is acknowledged. For RC, a write completion means the bytes are
    /// in the peer's memory and acked — so on `Ok(())` the caller may report
    /// `status=complete`. A transport failure mid-write closes the stream and
    /// returns an error (spec §7.4: never report `complete` on a partial write).
    pub fn rdma_write_all(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
    ) -> io::Result<()> {
        self.rdma_write_all_inner(src, src_off, peer_addr, peer_rkey, len, None)
    }

    /// Blocking facade over
    /// [`begin_rdma_write_with_imm`](Self::begin_rdma_write_with_imm) (spec §7.7):
    /// post the payload + the final write-with-immediate carrying `transfer_id`,
    /// then busy-poll until every WR is acknowledged. On `Ok(())` the payload is
    /// in the peer's memory and the transfer ID has been delivered to its CQ, so
    /// the caller may report `status=complete`. A transport failure mid-write
    /// closes the stream and returns an error (spec §7.7.7: never report
    /// `complete` on a partial write).
    pub fn rdma_write_all_with_imm(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
        transfer_id: u32,
    ) -> io::Result<()> {
        self.rdma_write_all_inner(src, src_off, peer_addr, peer_rkey, len, Some(transfer_id))
    }

    fn rdma_write_all_inner(
        &mut self,
        src: &RegisteredBuffer,
        src_off: usize,
        peer_addr: u64,
        peer_rkey: u32,
        len: usize,
        imm: Option<u32>,
    ) -> io::Result<()> {
        self.drive_write_all(|s| s.begin_rdma_write_inner(src, src_off, peer_addr, peer_rkey, len, imm))
    }

    /// Mark the stream closed — e.g. when the async layer observes a CM
    /// `DISCONNECTED` event (half-close). Subsequent reads see EOF.
    pub fn mark_closed(&mut self) {
        self.peer_closed = true;
    }

    // ---- async-reactor accessors (pass-throughs to the connection) ---------

    /// CQ completion-channel fd to register with a reactor. See [`Connection::cq_fd`].
    pub fn cq_fd(&self) -> io::Result<RawFd> {
        self.conn.cq_fd()
    }
    /// Arm the CQ before waiting on [`cq_fd`](Self::cq_fd). See [`Connection::arm_cq`].
    pub fn arm_cq(&self) -> io::Result<()> {
        self.conn.arm_cq()
    }
    /// Drain + ack completion-channel notifications after the fd signals.
    pub fn consume_cq_events(&self) -> usize {
        self.conn.consume_cq_events()
    }
    /// CM event-channel fd, for half-close detection. See [`Connection::cm_fd`].
    pub fn cm_fd(&self) -> io::Result<RawFd> {
        self.conn.cm_fd()
    }
    /// Flip the CM channel non-blocking (call once, after the handshake).
    pub fn set_cm_nonblock(&self) -> io::Result<()> {
        self.conn.set_cm_nonblock()
    }
    /// Non-blocking check for a peer-initiated disconnect. See [`Connection::check_disconnect`].
    pub fn check_disconnect(&self) -> io::Result<bool> {
        self.conn.check_disconnect()
    }

    /// A detached handle that can force this connection's NIC resources down
    /// (destroy the QP) out-of-band — independently of the stream's own [`Drop`].
    ///
    /// It exists for one purpose: a server runtime that may **abandon** (abort) a
    /// connection task while it is parked mid-`RDMA_WRITE`. On the normal path the
    /// write driver ([`rdma_write_all`](Self::rdma_write_all) /
    /// `poll_rdma_write`) drains every posted write before returning, so by the
    /// time a caller drops a source [`RegisteredBuffer`] no work request still
    /// references it. Aborting the task bypasses that drain: the future is dropped
    /// with a write WR still posted, and the source buffer it owns is freed (MR
    /// deregistered, storage released) **while the QP can still DMA-read it** — a
    /// use-after-free. Calling [`force_teardown`](ConnTeardown::force_teardown)
    /// *before* the task (and its buffers) drop destroys the QP first, quiescing
    /// the NIC, so the subsequent buffer frees are sound. Generalises to
    /// externally-registered MRs (caller-owned pages) for the same reason.
    pub fn teardown_handle(&self) -> ConnTeardown {
        ConnTeardown {
            conn: Arc::clone(&self.conn),
        }
    }
}

/// A cheap, owned handle to force a connection's QP teardown out-of-band — see
/// [`HordStream::teardown_handle`] for why it exists. Holds an `Arc<Connection>`
/// so the connection (and its CQ/PD, needed to deregister MRs afterward) stays
/// alive until the handle is dropped; it does **not** keep the QP itself alive
/// (the QP lives behind the connection's own `RefCell<Option<_>>` and is taken on
/// teardown).
pub struct ConnTeardown {
    conn: Arc<Connection>,
}

impl ConnTeardown {
    /// Synchronously destroy the connection's QP (idempotent). After this returns
    /// the NIC will not DMA against any buffer registered on the connection, so it
    /// is safe to free or deregister source buffers even if an RDMA write was
    /// still outstanding when the owning task was abandoned. Safe to call from off
    /// the connection's driving task as long as that task is not concurrently
    /// mid-poll (the single-threaded worker upholds this: it tears down between
    /// polls, never during one).
    pub fn force_teardown(&self) {
        self.conn.shutdown();
    }
}

impl Read for HordStream {
    /// Busy-poll facade over [`try_read`](HordStream::try_read): block until data
    /// is available (or EOF), processing completions meanwhile.
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(n) = self.try_read(out)? {
                return Ok(n);
            }
            // No data buffered and not closed: return owed credits, then wait.
            self.return_owed_credits(false)?;
            self.pump(true)?;
        }
    }
}

impl Write for HordStream {
    /// Busy-poll facade over [`try_write`](HordStream::try_write): accept all of
    /// `data` (sending whole messages, staging the sub-`payload_cap` remainder),
    /// blocking on send slots/credits as needed.
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.peer_closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection closed"));
        }
        let mut off = 0;
        while off < data.len() {
            let n = self.try_write(&data[off..])?;
            if n == 0 {
                // Blocked on a send slot/credit; process completions and retry.
                self.pump(true)?;
            } else {
                off += n;
            }
        }
        Ok(data.len())
    }

    /// Flush emits any staged bytes and then waits for *all* posted sends to
    /// complete. For RC, a send completion means the message has been placed in
    /// the peer's receive buffer and acknowledged — so once `flush` returns
    /// `Ok`, the data has been delivered and it is safe to disconnect.
    fn flush(&mut self) -> io::Result<()> {
        // Emit any staged partial message, blocking until it can be posted.
        while !self.try_flush_stage()? {
            if self.peer_closed {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "connection closed before all sends were acknowledged",
                ));
            }
            self.pump(true)?;
        }
        // Then wait for every posted data send *and* one-sided RDMA write to be
        // acknowledged — flush is a full "everything I posted is delivered"
        // barrier, so a caller may safely disconnect after it (a write left
        // pending here would otherwise be truncated, or leave the NIC DMA-ing a
        // buffer the caller is about to drop).
        while self.sends_outstanding() || self.writes_pending() {
            if self.peer_closed {
                // The connection dropped with sends still outstanding, so we
                // can NOT claim the data was delivered. Surface it rather than
                // returning Ok and silently truncating the stream.
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "connection closed before all sends were acknowledged",
                ));
            }
            self.pump(true)?;
        }
        Ok(())
    }
}

impl Drop for HordStream {
    fn drop(&mut self) {
        // The only teardown step still expressed at runtime: stop the NIC
        // (disconnect + destroy QP/CQ) so no DMA can target the registered
        // buffers once we start deregistering them. Everything below is now
        // type-enforced — `recv`/`send` each hold an `Arc<Connection>`, so when
        // they drop (after this body) their MRs are deregistered while the PD is
        // still alive, and the PD is freed only once the last Arc (the two
        // buffers plus `conn`) is gone.
        self.conn.shutdown();
    }
}

#[cfg(test)]
mod negotiate_tests {
    //! Split-mode negotiation rule (spec §5.3 + §7.7.6) — pure, no hardware.
    use super::*;

    fn peer(zero_copy: bool, split: bool, credits: u16) -> Handshake {
        Handshake::new(65536, 32)
            .with_zero_copy(zero_copy)
            .with_split_mode(split)
            .with_split_credits(credits)
    }

    #[test]
    fn split_needs_capability_zerocopy_and_credits() {
        // Happy path: local intent + negotiated zero-copy + peer capable + credits.
        assert!(negotiate_split(true, true, &peer(true, true, 8)));

        // Peer advertises split mode but 0 credits — it can receive no
        // write-with-imm, so split declines to the stream (§7.7.6).
        assert!(!negotiate_split(true, true, &peer(true, true, 0)));

        // Zero-copy not negotiated (§5.3) — no split even with credits.
        assert!(!negotiate_split(true, false, &peer(true, true, 8)));

        // Peer never advertised the capability bit.
        assert!(!negotiate_split(true, true, &peer(true, false, 8)));

        // We didn't offer split mode locally.
        assert!(!negotiate_split(false, true, &peer(true, true, 8)));
    }
}

#[cfg(test)]
mod fullduplex_tests {
    //! Full-duplex bulk transfer over a real RC connection.
    //!
    //! The half-duplex HTTP demo can never reach the credit-return paths that
    //! matter for flow-control correctness: it only ever has data flowing one
    //! way at a time. This test drives a large body in *both* directions at
    //! once, which is the only way to reach the simultaneous-zero-credit
    //! standoff that the control lane exists to break (#3), and to keep a
    //! receiver's buffers full of un-read data while the peer still needs to
    //! send (the backpressure path of #8).
    //!
    //! It needs the host's Soft-RoCE device (see CLAUDE.md), so it is
    //! `#[ignore]`d by default. Run it with:
    //!
    //! ```sh
    //! cargo test -p hord-stream -- --ignored --nocapture full_duplex_bulk
    //! ```

    use super::*;
    use std::io::Read;
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::{Duration, Instant};

    const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
    const PORT: u16 = 18519; // a free port distinct from the demo's 4791
    const BODY: usize = 16 * 1024 * 1024; // 16 MiB each way — far exceeds the pipe
    const STALL: Duration = Duration::from_secs(15); // no-progress watchdog

    /// A position-sensitive byte pattern, distinct per `seed` so each side can
    /// verify exactly what the peer sent.
    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x = seed as u32 | 1;
        for _ in 0..len {
            x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            out.push((x >> 16) as u8);
        }
        out
    }

    /// Drive one endpoint to completion: send `to_send` and receive exactly
    /// `expect_len` bytes. Everything is non-blocking (we only call the blocking
    /// primitives once their precondition already holds), so a single thread
    /// keeps both directions moving. Returns the bytes received.
    ///
    /// `standoff` deterministically manufactures the #3 deadlock condition. In
    /// full-duplex bulk traffic both sides almost always have data queued, so
    /// credits ride home piggybacked on data messages and the control lane is
    /// rarely the *only* way to return them. The exception — and the whole point
    /// of the control lane — is when both sides are at zero credits at once with
    /// more to send: then neither can send a data message to piggyback on, so a
    /// credit-return must travel the control lane. We force exactly that: phase
    /// one fills the peer's window without draining, and the barrier releases
    /// both sides only once they are jointly wedged at zero credits.
    fn drive(s: &mut HordStream, to_send: &[u8], expect_len: usize, standoff: &Barrier) -> Vec<u8> {
        let cap = s.payload_capacity();
        let mut sent = 0;
        let mut got = Vec::with_capacity(expect_len);
        let mut scratch = vec![0u8; 64 * 1024];

        // Phase one: write greedily, never reading, until our credits run out
        // (the body dwarfs the window, so they will). Pump only to recycle send
        // slots. No credit-return is needed — we are spending our initial grant.
        let mut last = Instant::now();
        while s.send_credits > 0 && sent < to_send.len() {
            if !s.send_free.is_empty() {
                let end = (sent + cap).min(to_send.len());
                s.post_data_message(&to_send[sent..end]).expect("post_data_message");
                sent = end;
                last = Instant::now();
            } else if !s.pump(false).expect("pump") && last.elapsed() > STALL {
                panic!("phase-one stalled at {sent}/{} bytes", to_send.len());
            }
        }

        // Both sides are now wedged at zero credits with undrained windows and
        // plenty left to send: the exact #3 standoff. Only the control lane can
        // break it.
        standoff.wait();

        // Phase two: full interleave to completion.
        let mut last_progress = Instant::now();
        while sent < to_send.len() || got.len() < expect_len {
            let mut progressed = false;

            while sent < to_send.len() && !s.send_free.is_empty() && s.send_credits > 0 {
                let end = (sent + cap).min(to_send.len());
                s.post_data_message(&to_send[sent..end]).expect("post_data_message");
                sent = end;
                progressed = true;
            }

            // Drain whatever has arrived (re-posts buffers + returns credits).
            while got.len() < expect_len && !s.rx_ready.is_empty() {
                let n = s.read(&mut scratch).expect("read");
                got.extend_from_slice(&scratch[..n]);
                progressed = true;
            }

            if s.pump(false).expect("pump") {
                progressed = true;
            }

            // Return owed credits over the control lane even at zero data
            // credits — the path that breaks the deadlock.
            s.maybe_return_credits(1).expect("credit return");

            if s.peer_closed && got.len() < expect_len {
                panic!("peer closed early: got {}/{expect_len} bytes", got.len());
            }

            if progressed {
                last_progress = Instant::now();
            } else if last_progress.elapsed() > STALL {
                panic!(
                    "stalled for {STALL:?} (sent {sent}/{}, got {}/{expect_len}) — \
                     likely a credit deadlock",
                    to_send.len(),
                    got.len(),
                );
            }
        }

        // All bytes exchanged; make sure our sends are acknowledged.
        s.flush().expect("flush");
        got
    }

    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn full_duplex_bulk() {
        let config = HordConfig::default();

        // Each stream is created and used entirely within its own thread, so
        // HordStream need not be Send. `standoff` releases both sides only once
        // both are wedged at zero credits (forcing the #3 deadlock); `teardown`
        // holds both QPs open until both have flushed, so neither tears down
        // mid-flush.
        let standoff = Arc::new(Barrier::new(2));
        let teardown = Arc::new(Barrier::new(2));
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let srv_config = config.clone();
        let srv_standoff = Arc::clone(&standoff);
        let srv_teardown = Arc::clone(&teardown);
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            let got = drive(&mut s, &pattern(BODY, 0xA5), BODY, &srv_standoff);
            assert_eq!(got, pattern(BODY, 0x5A), "server received corrupt data");
            srv_teardown.wait();
            // s drops here, after the client has also flushed.
        });

        ready_rx.recv().expect("server ready");
        let mut client = HordStream::connect(IP, PORT, &config).expect("connect");
        let got = drive(&mut client, &pattern(BODY, 0x5A), BODY, &standoff);
        assert_eq!(got, pattern(BODY, 0xA5), "client received corrupt data");
        teardown.wait();
        drop(client);

        server.join().expect("server thread panicked");
    }
}

#[cfg(test)]
mod half_close_tests {
    //! Synchronous half-close detection (the async path already had it).
    //!
    //! A peer's *graceful* disconnect (`rdma_disconnect`, which delivers a CM
    //! `DISCONNECTED` event but leaves the peer's QP — and so our recv WRs —
    //! un-flushed) gives a blocked `read()` no completion to wake on. Without
    //! watching the CM channel the reader would busy-spin forever; with it, the
    //! busy-poll notices the disconnect and `read()` returns EOF.
    //!
    //! The reader runs on its own thread so the test enforces a deadline rather
    //! than hanging if detection regresses (a regression makes this *fail*, not
    //! hang the suite). The server holds its QP alive until the client has
    //! observed EOF, so the CM event is the *only* close signal in play.
    //!
    //! Needs the host's Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d:
    //! ```sh
    //! cargo test -p hord-stream -- --ignored --nocapture sync_half_close
    //! ```
    use super::*;
    use std::io::Read;
    use std::sync::mpsc;
    use std::time::Duration;

    const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
    const PORT: u16 = 18526; // distinct from the other in-crate loopback tests
    const DEADLINE: Duration = Duration::from_secs(15);

    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn sync_half_close_unblocks_read() {
        let config = HordConfig::default();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        // Released by the test once the client has seen EOF, so the server keeps
        // its QP up until then (no teardown that could flush the client by
        // another path — the CM event must be what unblocks the read).
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let srv_config = config.clone();
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let s = HordStream::accept(&listener, &srv_config).expect("accept");
            s.disconnect(); // graceful half-close: DREQ → client's CM DISCONNECTED
            release_rx.recv().expect("client EOF signal");
            drop(s);
        });

        ready_rx.recv().expect("server ready");

        // Blocking read on its own thread, so the test below can bound the wait.
        let (eof_tx, eof_rx) = mpsc::channel::<io::Result<usize>>();
        let cfg = config.clone();
        let reader = std::thread::spawn(move || {
            let mut c = HordStream::connect(IP, PORT, &cfg).expect("connect");
            let mut buf = [0u8; 64];
            eof_tx.send(c.read(&mut buf)).ok();
        });

        match eof_rx.recv_timeout(DEADLINE) {
            Ok(Ok(0)) => {} // EOF — half-close detected.
            Ok(other) => panic!("expected EOF (Ok(0)) on peer half-close, got {other:?}"),
            Err(_) => panic!("read() blocked past {DEADLINE:?} — sync half-close not detected"),
        }

        release_tx.send(()).expect("release server");
        reader.join().expect("reader thread panicked");
        server.join().expect("server thread panicked");
    }
}

#[cfg(test)]
mod split_tests {
    //! Protocol splitting (spec §7.7) end-to-end over a real RC connection: the
    //! server delivers several payloads with RDMA write-with-immediate, and the
    //! client's *data plane* learns of each arrival from its CQ — by transfer ID,
    //! with no stream message and no HTTP parsing.
    //!
    //! The server fires all transfers (non-blocking) before reaping, so the full
    //! default transfer-credit budget (`split_credits` = 8) is in flight at once,
    //! and writes them in a different order than the client registered the
    //! buffers, so the client must demultiplex purely by the ID in `wc.imm_data`
    //! (§7.7.5). It also asserts the credit invariant that matters for this path:
    //! an immediate returns *no* stream data credit.
    //!
    //! Scope note: this confirms `split_credits` concurrent immediates all land
    //! and demux correctly; it does NOT test *over*-saturation (more in-flight
    //! immediates than posted recv WRs), because transfer credits are not
    //! sender-enforced — exceeding them RNR-stalls rather than erroring (a known
    //! limitation; see PROTOTYPE.md / TODO.md).
    //!
    //! Needs the host's Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d.
    //! Run with:
    //!
    //! ```sh
    //! cargo test -p hord-stream -- --ignored --nocapture split_mode_round_trip
    //! ```

    use super::*;
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::{Duration, Instant};

    const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
    const PORT: u16 = 18522; // distinct from full_duplex_bulk (18519) and the smokes
    const PORT_BP: u16 = 18523; // split_credit_backpressure
    const PORT_BPF: u16 = 18524; // split_credit_backpressure_facade
    const STALL: Duration = Duration::from_secs(15);
    // (transfer ID, payload length). Distinct IDs and sizes so a misrouted
    // payload — or a byte-swapped ID — is caught. Exactly `split_credits` (8)
    // entries so the full default transfer-credit budget is in flight at once;
    // 8 <= send_pool (16) and 8 <= posted recv WRs (42), so no overrun.
    const TRANSFERS: &[(u32, usize)] = &[
        (10, 1 << 20), // 1 MiB
        (20, 1 << 19), // 512 KiB
        (30, 3 << 20), // 3 MiB
        (40, 1 << 18), // 256 KiB
        (50, 1 << 20), // 1 MiB
        (60, 1 << 17), // 128 KiB
        (70, 2 << 20), // 2 MiB
        (80, 1 << 16), // 64 KiB
    ];

    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x = seed as u32 | 1;
        for _ in 0..len {
            x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            out.push((x >> 16) as u8);
        }
        out
    }

    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn split_mode_round_trip() {
        let config = HordConfig::default(); // split_mode + zero_copy on by default

        let teardown = Arc::new(Barrier::new(2));
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        // client -> server: (id, dest addr, dest rkey, len) for every buffer.
        let (desc_tx, desc_rx) = mpsc::channel::<Vec<(u32, u64, u32, usize)>>();

        let srv_config = config.clone();
        let srv_teardown = Arc::clone(&teardown);
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            assert!(
                s.split_mode_negotiated(),
                "server: split mode should have negotiated"
            );

            let descs = desc_rx.recv().expect("recv descriptors");
            // Hold every source MR alive until all writes are acked (the NIC
            // DMA-reads them); fill each with its ID-keyed pattern.
            let mut srcs = Vec::new();
            for &(id, _, _, len) in &descs {
                let src = s.register_source(len).expect("register src");
                src.copy_in(0, &pattern(len, id as u8));
                srcs.push(src);
            }
            // Fire them in *reverse* registration order, all in flight at once,
            // so arrival order can't match registration order on the client.
            for (i, &(id, addr, rkey, len)) in descs.iter().enumerate().rev() {
                s.begin_rdma_write_with_imm(&srcs[i], 0, addr, rkey, len, id)
                    .expect("begin_rdma_write_with_imm");
            }
            // Reap every send-side completion before dropping the sources.
            let start = Instant::now();
            while s.writes_pending() {
                s.pump(true).expect("pump");
                assert!(start.elapsed() < STALL, "server writes never drained");
            }
            srv_teardown.wait();
            drop(srcs);
            // s drops after the client has collected everything.
        });

        ready_rx.recv().expect("server ready");
        let mut client = HordStream::connect(IP, PORT, &config).expect("connect");
        assert!(
            client.split_mode_negotiated(),
            "client: split mode should have negotiated"
        );
        let credits_before = client.send_credits;

        // Register a destination buffer per transfer and advertise them.
        let bufs: Vec<RegisteredBuffer> = TRANSFERS
            .iter()
            .map(|&(_, len)| client.register_remote_writable(len).expect("register dst"))
            .collect();
        let descs: Vec<(u32, u64, u32, usize)> = TRANSFERS
            .iter()
            .zip(&bufs)
            .map(|(&(id, len), buf)| (id, buf.as_mut_ptr() as u64, buf.rkey(), len))
            .collect();
        desc_tx.send(descs).expect("send descriptors");

        // Data plane: collect every transfer by ID from the CQ — no HTTP, no
        // stream message. Verify each landed payload against its ID-keyed pattern.
        let mut seen = std::collections::HashSet::new();
        let start = Instant::now();
        while seen.len() < TRANSFERS.len() {
            match client.poll_completed_transfer().expect("poll_completed_transfer") {
                Some(id) => {
                    assert!(seen.insert(id), "transfer {id} completed twice");
                    let (idx, &(_, len)) = TRANSFERS
                        .iter()
                        .enumerate()
                        .find(|(_, &(tid, _))| tid == id)
                        .unwrap_or_else(|| panic!("unknown transfer ID {id} in imm_data"));
                    let mut got = vec![0u8; len];
                    bufs[idx].copy_out(0, &mut got);
                    assert_eq!(got, pattern(len, id as u8), "transfer {id} payload mismatch");
                }
                None => panic!("connection closed before all transfers completed"),
            }
            assert!(start.elapsed() < STALL, "data-plane completions stalled");
        }

        // The whole-buffer demux worked; now the credit invariant (§7.7.6): an
        // immediate consumes a recv WR that cost the peer no stream credit, so it
        // must return none — our send window is untouched and we owe no grant.
        assert_eq!(
            client.send_credits, credits_before,
            "split-mode immediates must not perturb stream send credits"
        );
        assert_eq!(
            client.grant_pending, 0,
            "split-mode immediates must not accrue a data-credit grant debt"
        );

        teardown.wait();
        drop(bufs);
        drop(client);
        server.join().expect("server thread panicked");
    }

    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn split_mode_mid_write_failure() {
        // §7.7.7: when a write-with-immediate fails after posting, the QP enters
        // the error state and the connection closes — the server MUST NOT report
        // `complete`, and the data plane MUST surface the close rather than a
        // phantom transfer. We force the failure deterministically by writing far
        // more than the client's MR covers (an out-of-bounds remote access).
        const PORT_FAIL: u16 = 18524;
        const CLIENT_CAP: usize = 64 * 1024; // client's registered region
        const WRITE_LEN: usize = 1 << 20; // server writes 1 MiB into it -> overflow

        let config = HordConfig::default();
        let teardown = Arc::new(Barrier::new(2));
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (target_tx, target_rx) = mpsc::channel::<(u64, u32)>();

        let srv_config = config.clone();
        let srv_teardown = Arc::clone(&teardown);
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT_FAIL).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            assert!(s.split_mode_negotiated());

            let (addr, rkey) = target_rx.recv().expect("recv target");
            let src = s.register_source(WRITE_LEN).expect("register src");
            src.copy_in(0, &pattern(WRITE_LEN, 0x77));
            // The write overflows the client's MR -> remote access error. The
            // blocking facade drains the (failed) WR and returns Err; it MUST NOT
            // succeed (which would let us report `complete`, §7.7.7).
            let result = s.rdma_write_all_with_imm(&src, 0, addr, rkey, WRITE_LEN, 99);
            assert!(
                result.is_err(),
                "out-of-bounds write-with-imm must fail, got {result:?}"
            );
            srv_teardown.wait();
            drop(src); // safe: rdma_write_all_with_imm drained the WR before returning
        });

        ready_rx.recv().expect("server ready");
        let mut client = HordStream::connect(IP, PORT_FAIL, &config).expect("connect");
        assert!(client.split_mode_negotiated());
        let buf = client.register_remote_writable(CLIENT_CAP).expect("register dst");
        target_tx
            .send((buf.as_mut_ptr() as u64, buf.rkey()))
            .expect("send target");

        // The failed write must surface as a close — never as a completed
        // transfer. Bounded poll: a flushed completion (QP in error) sets
        // peer_closed; a poll-level error is itself a close.
        let start = Instant::now();
        let outcome = loop {
            if client.drain_completions().is_err() {
                break None;
            }
            if let Some(id) = client.next_completed_transfer() {
                break Some(id);
            }
            if client.is_closed() {
                break None;
            }
            assert!(
                start.elapsed() < STALL,
                "neither a completion nor a close was observed after a failed write"
            );
            std::hint::spin_loop();
        };
        assert!(
            outcome.is_none(),
            "a failed write must not yield a transfer completion (got {outcome:?})"
        );

        teardown.wait();
        drop(buf);
        drop(client);
        server.join().expect("server thread panicked");
    }

    /// Transfer-credit flow control (spec §7.7.6): the sender must not exceed the
    /// window the peer advertised. With a tight window of 2, posting a third
    /// write-with-imm before any completes must back-pressure (`WouldBlock`)
    /// having posted nothing — *not* RNR-stall — and must then succeed once an
    /// in-flight transfer drains and frees a credit.
    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn split_credit_backpressure() {
        const WINDOW: usize = 2; // advertised transfer-credit window
        const LEN: usize = 1 << 16; // 64 KiB per transfer
        const N: usize = WINDOW + 1; // one past the window
        let config = HordConfig {
            split_credits: WINDOW,
            ..HordConfig::default()
        };

        let teardown = Arc::new(Barrier::new(2));
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        // client -> server: (id, dest addr, dest rkey) per buffer.
        let (desc_tx, desc_rx) = mpsc::channel::<Vec<(u32, u64, u32)>>();

        let srv_config = config.clone();
        let srv_teardown = Arc::clone(&teardown);
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT_BP).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            assert!(s.split_mode_negotiated(), "server: split mode should negotiate");
            // The bound on *our* sending is the window the client advertised.
            assert_eq!(
                s.peer_split_credits, WINDOW as u32,
                "server should see the client's advertised transfer-credit window"
            );

            let descs = desc_rx.recv().expect("recv descriptors");
            let mut srcs = Vec::new();
            for &(id, _, _) in &descs {
                let src = s.register_source(LEN).expect("register src");
                src.copy_in(0, &pattern(LEN, id as u8));
                srcs.push(src);
            }

            // Fill the window: WINDOW writes post without reaping.
            for i in 0..WINDOW {
                let (id, addr, rkey) = descs[i];
                s.begin_rdma_write_with_imm(&srcs[i], 0, addr, rkey, LEN, id)
                    .expect("writes within the window must post");
            }
            // The next one must back-pressure, not RNR-stall, and post nothing.
            let (id, addr, rkey) = descs[WINDOW];
            let blocked = s.begin_rdma_write_with_imm(&srcs[WINDOW], 0, addr, rkey, LEN, id);
            assert_eq!(
                blocked.as_ref().map_err(|e| e.kind()),
                Err(io::ErrorKind::WouldBlock),
                "a transfer past the window must back-pressure (got {blocked:?})"
            );

            // Reap completions until a credit frees, then the third goes through.
            let start = Instant::now();
            loop {
                s.pump(true).expect("pump");
                match s.begin_rdma_write_with_imm(&srcs[WINDOW], 0, addr, rkey, LEN, id) {
                    Ok(()) => break,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => panic!("unexpected error retrying the third write: {e}"),
                }
                assert!(start.elapsed() < STALL, "transfer-credit window never freed");
            }
            // Drain the rest before dropping the sources (NIC DMA-reads them).
            while s.writes_pending() {
                s.pump(true).expect("pump");
                assert!(start.elapsed() < STALL, "server writes never drained");
            }
            srv_teardown.wait();
            drop(srcs);
        });

        ready_rx.recv().expect("server ready");
        let mut client = HordStream::connect(IP, PORT_BP, &config).expect("connect");
        assert!(client.split_mode_negotiated(), "client: split mode should negotiate");

        let bufs: Vec<RegisteredBuffer> = (0..N)
            .map(|_| client.register_remote_writable(LEN).expect("register dst"))
            .collect();
        let descs: Vec<(u32, u64, u32)> = (0..N)
            .map(|i| (100 + i as u32, bufs[i].as_mut_ptr() as u64, bufs[i].rkey()))
            .collect();
        let ids: Vec<u32> = descs.iter().map(|d| d.0).collect();
        desc_tx.send(descs).expect("send descriptors");

        // All N transfers must arrive on the data plane, integrity-checked.
        let mut seen = std::collections::HashSet::new();
        let start = Instant::now();
        while seen.len() < N {
            match client.poll_completed_transfer().expect("poll_completed_transfer") {
                Some(id) => {
                    assert!(seen.insert(id), "transfer {id} completed twice");
                    let idx = ids.iter().position(|&x| x == id).expect("known transfer ID");
                    let mut got = vec![0u8; LEN];
                    bufs[idx].copy_out(0, &mut got);
                    assert_eq!(got, pattern(LEN, id as u8), "transfer {id} payload mismatch");
                }
                None => panic!("connection closed before all transfers completed"),
            }
            assert!(start.elapsed() < STALL, "data-plane completions stalled");
        }

        teardown.wait();
        drop(bufs);
        drop(client);
        server.join().expect("server thread panicked");
    }

    /// Companion to `split_credit_backpressure` that drives the *blocking facade*
    /// `rdma_write_all_with_imm` — and therefore its WouldBlock retry loop, the
    /// production back-pressure path the raw-primitive test does not run. The
    /// server fills the transfer-credit window with non-blocking writes, then
    /// calls the facade for one more transfer: it must reap a credit and complete
    /// rather than erroring.
    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn split_credit_backpressure_facade() {
        const WINDOW: usize = 2;
        const LEN: usize = 1 << 16;
        const N: usize = WINDOW + 1;
        let config = HordConfig {
            split_credits: WINDOW,
            ..HordConfig::default()
        };

        let teardown = Arc::new(Barrier::new(2));
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (desc_tx, desc_rx) = mpsc::channel::<Vec<(u32, u64, u32)>>();

        let srv_config = config.clone();
        let srv_teardown = Arc::clone(&teardown);
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT_BPF).expect("bind");
            ready_tx.send(()).expect("signal ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            assert!(s.split_mode_negotiated(), "server: split mode should negotiate");

            let descs = desc_rx.recv().expect("recv descriptors");
            let mut srcs = Vec::new();
            for &(id, _, _) in &descs {
                let src = s.register_source(LEN).expect("register src");
                src.copy_in(0, &pattern(LEN, id as u8));
                srcs.push(src);
            }
            // Fill the window with non-blocking writes (no reaping) so the next
            // call enters back-pressure with the window full.
            for i in 0..WINDOW {
                let (id, addr, rkey) = descs[i];
                s.begin_rdma_write_with_imm(&srcs[i], 0, addr, rkey, LEN, id)
                    .expect("window-filling writes must post");
            }
            // The blocking facade must NOT error on the full window: its retry loop
            // reaps a credit and drives the transfer to completion. (This is the
            // production path; the sibling test exercises only the raw primitive.)
            let (id, addr, rkey) = descs[WINDOW];
            s.rdma_write_all_with_imm(&srcs[WINDOW], 0, addr, rkey, LEN, id)
                .expect("facade must back-pressure then complete, not error");
            // The facade drains every posted write before returning.
            assert!(!s.writes_pending(), "facade should have drained all writes");
            srv_teardown.wait();
            drop(srcs);
        });

        ready_rx.recv().expect("server ready");
        let mut client = HordStream::connect(IP, PORT_BPF, &config).expect("connect");
        assert!(client.split_mode_negotiated(), "client: split mode should negotiate");

        let bufs: Vec<RegisteredBuffer> = (0..N)
            .map(|_| client.register_remote_writable(LEN).expect("register dst"))
            .collect();
        let descs: Vec<(u32, u64, u32)> = (0..N)
            .map(|i| (200 + i as u32, bufs[i].as_mut_ptr() as u64, bufs[i].rkey()))
            .collect();
        let ids: Vec<u32> = descs.iter().map(|d| d.0).collect();
        desc_tx.send(descs).expect("send descriptors");

        let mut seen = std::collections::HashSet::new();
        let start = Instant::now();
        while seen.len() < N {
            match client.poll_completed_transfer().expect("poll_completed_transfer") {
                Some(id) => {
                    assert!(seen.insert(id), "transfer {id} completed twice");
                    let idx = ids.iter().position(|&x| x == id).expect("known transfer ID");
                    let mut got = vec![0u8; LEN];
                    bufs[idx].copy_out(0, &mut got);
                    assert_eq!(got, pattern(LEN, id as u8), "transfer {id} payload mismatch");
                }
                None => panic!("connection closed before all transfers completed"),
            }
            assert!(start.elapsed() < STALL, "data-plane completions stalled");
        }

        teardown.wait();
        drop(bufs);
        drop(client);
        server.join().expect("server thread panicked");
    }
}

#[cfg(test)]
mod gather_tests {
    //! Scatter-gather zero-copy write (spec §7, Milestone 3): a *fragmented* source
    //! — several separately-registered, caller-owned MRs, mimicking an MSE4 object
    //! stored across non-contiguous allocations — is laid down **contiguously** into
    //! the client's single registered buffer by one logical
    //! [`HordStream::rdma_write_gather_all`]. Uses more segments than the QP's
    //! `max_send_sge`, so the gather spans multiple WRs (exercising the SGE-packing),
    //! and verifies every byte lands in order.
    //!
    //! Needs the host's Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d:
    //! ```sh
    //! cargo test -p hord-stream -- --ignored --nocapture gather
    //! ```
    use super::*;
    use std::io::{Read, Write};
    use std::sync::mpsc;

    const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
    const PORT: u16 = 18530; // distinct from the other in-crate loopback tests
    const SEG_LEN: usize = 64 * 1024; // per-allocation (per-segment) size
    const N_SEG: usize = 40; // > MAX_WRITE_SGE (16) -> the gather spans several WRs
    const TOTAL: usize = SEG_LEN * N_SEG; // 2.5 MiB contiguous object

    /// The contiguous object's byte at absolute offset `i`.
    fn pattern_byte(i: usize) -> u8 {
        (i % 251) as u8
    }

    #[test]
    #[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
    fn gather_writes_fragments_contiguously() {
        let config = HordConfig::default();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();

        let srv_config = config.clone();
        let server = std::thread::spawn(move || {
            let listener = Listener::bind(IP, PORT).expect("bind");
            ready_tx.send(()).expect("ready");
            let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
            // The whole point of the test is multi-WR packing; the QP cap (<= 16)
            // is always below N_SEG, so this holds — it documents the intent.
            assert!(
                s.max_send_sge() < N_SEG,
                "N_SEG ({N_SEG}) must exceed max_send_sge ({}) to force multi-WR packing",
                s.max_send_sge(),
            );

            // The fragmented source: N_SEG separate caller-owned allocations (each
            // its own Vec — non-contiguous, like MSE4's alloc list), each holding
            // the slice of the global pattern it represents, each its own MR. Keep
            // `backing` (and `mrs`) alive until after the gather drains.
            let mut backing: Vec<Vec<u8>> = Vec::with_capacity(N_SEG);
            let mut mrs: Vec<Mr> = Vec::with_capacity(N_SEG);
            for seg in 0..N_SEG {
                let mut v = vec![0u8; SEG_LEN];
                for (j, b) in v.iter_mut().enumerate() {
                    *b = pattern_byte(seg * SEG_LEN + j);
                }
                // SAFETY: `v` stays live in `backing` until after
                // `rdma_write_gather_all` returns (which drains every WR).
                let mr = unsafe { s.register_external(v.as_mut_ptr(), v.len()) }.expect("reg ext");
                backing.push(v);
                mrs.push(mr);
            }

            // Client's destination descriptor: "addr rkey".
            let line = read_line(&mut s);
            let mut it = line.split_whitespace();
            let addr: u64 = it.next().unwrap().parse().unwrap();
            let rkey: u32 = it.next().unwrap().parse().unwrap();

            let segments: Vec<WriteSegment> =
                mrs.iter().map(|mr| WriteSegment::from_mr(mr, 0, SEG_LEN)).collect();
            s.rdma_write_gather_all(&segments, addr, rkey).expect("gather write");
            write_line(&mut s, "done");
            // The write drained, so the source MRs/backing may now be released.
            drop(segments);
            drop(mrs);
            drop(backing);
            s.disconnect();
        });

        ready_rx.recv().expect("ready");
        let mut c = HordStream::connect(IP, PORT, &config).expect("connect");
        let buf = c.register_remote_writable(TOTAL).expect("reg dst");
        write_line(&mut c, &format!("{} {}", buf.as_mut_ptr() as u64, buf.rkey()));
        assert_eq!(read_line(&mut c), "done");

        // The fragmented source must have landed contiguously, in order.
        let mut got = vec![0u8; TOTAL];
        buf.copy_out(0, &mut got);
        for (i, &b) in got.iter().enumerate() {
            assert_eq!(b, pattern_byte(i), "payload mismatch at byte {i}");
        }
        server.join().expect("server thread panicked");
    }

    fn read_line<S: Read>(s: &mut S) -> String {
        let mut out = Vec::new();
        let mut b = [0u8; 1];
        loop {
            match s.read(&mut b).expect("read") {
                0 => break,
                _ if b[0] == b'\n' => break,
                _ => out.push(b[0]),
            }
        }
        String::from_utf8(out).expect("utf-8")
    }
    fn write_line<S: Write>(s: &mut S, line: &str) {
        s.write_all(line.as_bytes()).expect("write");
        s.write_all(b"\n").expect("write newline");
        s.flush().expect("flush");
    }
}

#[cfg(test)]
mod plan_gather_tests {
    //! Device-free unit tests for the scatter-gather WR packer
    //! ([`HordStream::plan_gather`]). The packer is pure arithmetic over
    //! `(addr, lkey, len)` tuples — it never dereferences a pointer — so these run
    //! under a plain `cargo test` with no RDMA device, and a "segment" can be larger
    //! than any real allocation. They lock the two packing dimensions (the
    //! `max_send_sge` count cap and the [`WRITE_WR_MAX`] byte cap), the contiguous
    //! remote-offset advance, and zero-length handling — the most intricate logic in
    //! the gather path, which the rxe0 integration tests exercise only in the
    //! all-small-segments regime (every WR packed purely by SGE count).
    use super::*;

    /// A fake source span over an arbitrary `(addr, len)`. No memory is touched
    /// (the packer only reads addr/len arithmetically), so `len` may exceed any real
    /// allocation to exercise the byte-cap split.
    fn seg(addr: u64, len: usize) -> WriteSegment<'static> {
        // SAFETY: fed only to plan_gather, which never dereferences the pointer; the
        // lkey is a stand-in and no WR is ever posted from these segments.
        unsafe { WriteSegment::from_raw(addr as *const u8, 0xABCD, len) }
    }

    /// Run the packer, returning each emitted WR as `(remote_off, [(addr, len)])`.
    fn plan(segments: &[WriteSegment<'_>], max_sge: usize) -> Vec<(u64, Vec<(u64, u32)>)> {
        let mut wrs = Vec::new();
        let n = HordStream::plan_gather(segments, max_sge, |remote_off, sges| {
            wrs.push((remote_off, sges.iter().map(|s| (s.addr, s.length)).collect()));
            Ok(())
        })
        .expect("plan_gather");
        assert_eq!(n, wrs.len(), "returned WR count must match the emitted WRs");
        wrs
    }

    #[test]
    fn single_small_segment_is_one_1sge_wr() {
        assert_eq!(plan(&[seg(0x1000, 4096)], 16), vec![(0, vec![(0x1000, 4096)])]);
    }

    #[test]
    fn empty_or_all_zero_length_emits_nothing() {
        assert!(plan(&[], 16).is_empty());
        assert!(plan(&[seg(0x1000, 0), seg(0x2000, 0)], 16).is_empty());
    }

    #[test]
    fn packs_up_to_max_sge_per_wr_at_contiguous_offsets() {
        // 5 segments of 100 bytes, max_sge 2 -> WRs of [2, 2, 1] SGEs; each WR's
        // remote offset is the running total of the prior WRs' bytes.
        let segs: Vec<_> = (0..5).map(|i| seg(0x1000 * (i + 1) as u64, 100)).collect();
        let wrs = plan(&segs, 2);
        assert_eq!(wrs.len(), 3);
        assert_eq!([wrs[0].1.len(), wrs[1].1.len(), wrs[2].1.len()], [2, 2, 1]);
        assert_eq!([wrs[0].0, wrs[1].0, wrs[2].0], [0, 200, 400]);
    }

    #[test]
    fn zero_length_segment_between_data_is_skipped() {
        // A 0-length segment must consume neither an SGE slot nor a WR.
        let wrs = plan(&[seg(0x1000, 50), seg(0x2000, 0), seg(0x3000, 50)], 16);
        assert_eq!(wrs, vec![(0, vec![(0x1000, 50), (0x3000, 50)])]);
    }

    #[test]
    fn segment_larger_than_byte_cap_splits_across_wrs() {
        // A 2.5 * WRITE_WR_MAX segment -> 3 WRs (1 GiB, 1 GiB, 0.5 GiB), 1 SGE each
        // (the byte cap fills a WR before a second SGE of the same segment is added),
        // at contiguous remote offsets. No memory is allocated (the addr is fake).
        let cap = WRITE_WR_MAX as u64;
        let half = WRITE_WR_MAX / 2;
        let total = 2 * WRITE_WR_MAX + half;
        let base = 0x4000_0000u64;
        let wrs = plan(&[seg(base, total)], 16);
        assert_eq!(wrs.len(), 3);
        assert_eq!(wrs[0], (0, vec![(base, WRITE_WR_MAX as u32)]));
        assert_eq!(wrs[1], (cap, vec![(base + cap, WRITE_WR_MAX as u32)]));
        assert_eq!(wrs[2], (2 * cap, vec![(base + 2 * cap, half as u32)]));
        // Every byte of the segment is laid down exactly once.
        let laid: u64 = wrs.iter().flat_map(|(_, s)| s.iter().map(|&(_, l)| l as u64)).sum();
        assert_eq!(laid, total as u64);
    }

    #[test]
    fn max_sge_zero_is_clamped_to_one() {
        // A degenerate max_sge must clamp to 1 (no divide-by-zero, no 0-SGE WR).
        let wrs = plan(&[seg(0x1000, 10), seg(0x2000, 10)], 0);
        assert_eq!(wrs, vec![(0, vec![(0x1000, 10)]), (10, vec![(0x2000, 10)])]);
    }
}
