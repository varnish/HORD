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
use std::os::fd::RawFd;
use std::sync::Arc;

use hord_core::{
    CmParams, Completion, Connection, Listener, Opcode, RegisteredBuffer, ACCESS_LOCAL_WRITE,
    ACCESS_REMOTE_WRITE,
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

// Transfer-credit (spec §7.7.6) receive headroom: zero unless we advertise split
// mode, otherwise `split_credits`. A write-with-immediate consumes one posted
// recv WR; this slack keeps concurrent split transfers from cannibalising the
// data receive window. We size for our *own* advertised intent at construction
// (the peer's capability isn't known until the handshake completes); if the peer
// turns out not to support split mode, the extra posted WRs are simply unused.
fn split_slack(config: &HordConfig) -> usize {
    if config.split_mode {
        config.split_credits
    } else {
        0
    }
}

// Total receive WRs the QP must hold and we keep posted: the data pool, the
// control slack, and the split-mode transfer headroom. `cqe` in hord-core is
// derived from send_wr + recv_wr, so growing this auto-sizes the CQ.
fn recv_wr_count(config: &HordConfig) -> usize {
    config.recv_pool_size + CTRL_RECV_SLACK + split_slack(config)
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

/// Bytes per RDMA-write work request. The NIC segments a single WR into MTU-sized
/// packets, so one WR can carry a very large payload; we cap it only so an
/// enormous object maps to a bounded number of WRs (`begin_rdma_write` requires
/// that count to fit the send queue). 1 GiB matches the demo's body ceiling, so
/// every demo response is a single WR.
const WRITE_WR_MAX: usize = 1 << 30;

/// A received data message whose payload still lives in its receive buffer,
/// awaiting `read()`. We hold the buffer (rather than copying out and
/// re-posting on receipt) so the receive buffer is only freed — and a credit
/// only returned to the peer — once the application has *consumed* the bytes.
/// That ties the peer's send window to our read progress, giving real
/// backpressure and a bounded reassembly footprint.
#[derive(Debug, Clone, Copy)]
struct ReadyMsg {
    slot: usize,
    start: usize, // absolute offset into recv_buf of the next unread byte
    end: usize,   // absolute offset into recv_buf one past the payload
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
    send: RegisteredBuffer,

    msg_size: usize,    // bytes per buffer slot (our max_message_size)
    payload_cap: usize, // max payload per message = min(ours, peer) - ENVELOPE_LEN
    recv_pool: usize,
    recv_total: usize, // recv WRs we keep posted: data pool + ctrl slack + split slack
    send_pool: usize,

    send_free: Vec<usize>, // free *data* send slot indices (the control slot is separate)
    send_credits: u32,     // data messages we may still post to the peer
    grant_pending: u32,    // data credits we owe the peer (consumed recvs not yet announced)
    ctrl_slot: usize,      // reserved send slot index for control (CREDIT_ONLY) messages
    ctrl_send_busy: bool,  // a control message is in flight on `ctrl_slot`

    tx_stage: Vec<u8>,        // bytes buffered by write(), drained into messages
    rx_ready: VecDeque<ReadyMsg>, // received data, still in its recv buffer, awaiting read()
    peer_closed: bool,        // observed a flush/transport error -> treat as EOF/broken pipe

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
        let (conn, peer_bytes) = Self::accept_begin(listener, config)?;
        Self::from_accepted(conn, peer_bytes, config)
    }

    /// Server side, phase one: accept the next connection request with this
    /// config's QP sizing (the data pools plus the control lane's reserved WRs),
    /// returning the not-yet-established [`Connection`] — which **is** `Send` —
    /// and the peer's handshake bytes.
    ///
    /// Split out from [`accept`](Self::accept) so an async server can run the
    /// accept loop on one thread and finish each connection on another: the
    /// registered buffers make the resulting `HordStream` thread-affine (`!Send`),
    /// so it must be *built* on the thread that will *run* it. The acceptor moves
    /// the bare `Connection` across the thread boundary and the worker calls
    /// [`from_accepted`](Self::from_accepted).
    pub fn accept_begin(
        listener: &Listener,
        config: &HordConfig,
    ) -> io::Result<(Connection, Vec<u8>)> {
        // The QP must hold the control lane's extra WRs and the split-mode
        // transfer headroom on top of the data pools.
        listener.accept(
            config.send_pool_size + CTRL_SEND_SLOTS,
            recv_wr_count(config),
            HANDSHAKE_LEN,
            config.cm,
        )
    }

    /// Server side, phase two: register buffers, post receives, and complete the
    /// handshake on a connection returned by [`accept_begin`](Self::accept_begin).
    pub fn from_accepted(
        conn: Connection,
        peer_bytes: Vec<u8>,
        config: &HordConfig,
    ) -> io::Result<HordStream> {
        let mut s = HordStream::new_common(conn, config)?;
        let peer = Handshake::decode(&peer_bytes)?;
        s.apply_peer(&peer)?;
        let my = HordStream::my_handshake(config);
        s.conn.accept_finish(&my.encode())?;
        Ok(s)
    }

    /// Client side: connect to `ip:port` and complete the HORD handshake.
    pub fn connect(ip: &str, port: u16, config: &HordConfig) -> io::Result<HordStream> {
        // The QP must hold the control lane's extra WRs and the split-mode
        // transfer headroom on top of the data pools.
        let conn = Connection::connect(
            ip,
            port,
            config.send_pool_size + CTRL_SEND_SLOTS,
            recv_wr_count(config),
            config.cm,
        )?;
        let mut s = HordStream::new_common(conn, config)?;
        let my = HordStream::my_handshake(config);
        let peer_bytes = s.conn.connect_finish(&my.encode(), HANDSHAKE_LEN)?;
        let peer = Handshake::decode(&peer_bytes)?;
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

        // recv carries the data pool plus the always-posted control slack plus
        // the split-mode transfer headroom; send carries the data pool plus the
        // reserved control send slot (the highest index, `send_pool`).
        let recv_slots = recv_wr_count(config);
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
        let recv = conn.register_buffer(recv_slots * msg_size, ACCESS_LOCAL_WRITE)?;
        let send = conn.register_buffer(send_slots * msg_size, ACCESS_LOCAL_WRITE)?;

        let mut s = HordStream {
            conn,
            recv,
            send,
            msg_size,
            payload_cap: 0,
            recv_pool,
            recv_total: recv_slots,
            send_pool,
            send_free: (0..send_pool).rev().collect(),
            send_credits: 0,
            grant_pending: 0,
            ctrl_slot: send_pool,
            ctrl_send_busy: false,
            tx_stage: Vec::new(),
            rx_ready: VecDeque::new(),
            peer_closed: false,
            zero_copy: config.zero_copy,
            writes_outstanding: 0,
            split_mode: config.split_mode,
            completed_transfers: VecDeque::new(),
            peer_split_credits: 0,
            imm_outstanding: 0,
        };
        s.post_all_recvs()?;
        Ok(s)
    }

    fn post_all_recvs(&mut self) -> io::Result<()> {
        let base = self.recv.as_mut_ptr();
        let msg = self.msg_size;
        let lkey = self.recv.lkey();
        // Post the data pool, the control slack, *and* the split-mode transfer
        // headroom; all are needed before the QP can carry traffic.
        for slot in 0..self.recv_total {
            // SAFETY: `base + slot*msg` lies within `recv` (a single MR with
            // `lkey`), which holds an Arc<Connection> so it outlives the QP.
            unsafe {
                let addr = base.add(slot * msg);
                self.conn
                    .post_recv(recv_wr_id(slot), addr, msg as u32, lkey)?;
            }
        }
        Ok(())
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
        Ok(())
    }

    /// Effective max payload bytes per RDMA message after negotiation.
    pub fn payload_capacity(&self) -> usize {
        self.payload_cap
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

    // ---- completion engine -------------------------------------------------

    /// Poll for and process exactly one completion. With `block`, busy-waits
    /// until a completion is available; otherwise returns `Ok(false)` when the
    /// CQ is empty. Returns `Ok(true)` if a completion was processed.
    fn pump(&mut self, block: bool) -> io::Result<bool> {
        loop {
            match self.conn.poll()? {
                Some(wc) => {
                    self.handle_completion(wc)?;
                    return Ok(true);
                }
                None => {
                    if block {
                        std::hint::spin_loop();
                        continue;
                    }
                    return Ok(false);
                }
            }
        }
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
            self.repost_recv(slot_of(wc.wr_id))?;
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
        let off = slot * self.msg_size;
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
        self.recv.copy_out(off, &mut hdr);
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
            self.repost_recv(slot)?;
        } else {
            // Zero-length data message: it did consume a data credit, so
            // re-post and return the credit now (no payload to hold).
            self.repost_recv(slot)?;
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

    /// Re-post the receive WR for `slot`. Re-posting only fails on a dead QP;
    /// on failure the connection is marked closed so reads/writes fail fast
    /// instead of silently operating with a shrunken receive window.
    fn repost_recv(&mut self, slot: usize) -> io::Result<()> {
        let off = slot * self.msg_size;
        let base = self.recv.as_mut_ptr();
        // SAFETY: `base + off` is slot `slot` inside `recv` / the MR, which
        // holds an Arc<Connection> and so outlives the QP.
        let repost = unsafe {
            let addr = base.add(off);
            self.conn
                .post_recv(recv_wr_id(slot), addr, self.msg_size as u32, self.recv.lkey())
        };
        if let Err(e) = repost {
            self.peer_closed = true;
            return Err(e);
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
            self.recv.copy_out(start, &mut out[written..written + take]);
            written += take;
            if start + take == end {
                self.rx_ready.pop_front();
                self.repost_recv(slot)?;
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
        if self.peer_closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection closed"));
        }
        assert!(
            src_off.checked_add(len).is_some_and(|end| end <= src.len()),
            "rdma write source range out of bounds",
        );
        // Each WR carries up to WRITE_WR_MAX bytes and occupies one send-queue
        // entry. The send queue holds `send_pool + CTRL_SEND_SLOTS` WRs; writes
        // share the `send_pool` data portion with in-flight data sends and any
        // earlier in-flight writes, leaving the control slot reserved for credit
        // returns. Bound the write against the data slots actually free *now* —
        // `send_free` tracks free data slots, of which `writes_outstanding` are
        // already taken by posted writes (writes don't draw from `send_free`). A
        // zero-length split write still needs one slot for its imm-only WR.
        let n_wrs = {
            let data = len.div_ceil(WRITE_WR_MAX);
            if imm.is_some() {
                data.max(1)
            } else {
                data
            }
        };
        let free_slots = self
            .send_free
            .len()
            .saturating_sub(self.writes_outstanding as usize);
        // A write needs `n_wrs` send-queue slots. If it needs more than the entire
        // data send pool it can NEVER fit — a caller error, fatal (InvalidInput).
        if n_wrs > self.send_pool {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "zero-copy write of {len} bytes needs {n_wrs} send-queue slots, \
                     exceeds the send pool of {}",
                    self.send_pool
                ),
            ));
        }
        // Slots are momentarily occupied by in-flight sends/writes: transient, so
        // back-pressure (WouldBlock) rather than erroring — the facades reap an
        // outstanding send or write and retry. (A bare-kind error: every caller
        // matches on the kind and discards the text, so don't allocate a message.)
        if n_wrs > free_slots {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        // Transfer-credit flow control (spec §7.7.6): a write-with-imm consumes
        // one of the peer's posted recv WRs. Bound our in-flight imm transfers by
        // the window the peer advertised so we can't overrun it and RNR-stall.
        // Back-pressure, not a transport error — return `WouldBlock` WITHOUT
        // marking the stream closed and WITHOUT posting anything, so the blocking /
        // async facades can reap an outstanding transfer and retry. One imm WR is
        // posted per call, so one credit is needed.
        if imm.is_some() && self.imm_outstanding >= self.peer_split_credits {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let base = src.as_mut_ptr();
        let lkey = src.lkey();
        let mut off = 0usize;
        let mut chunk = 0u64;
        while off < len {
            let n = (len - off).min(WRITE_WR_MAX);
            let is_last = off + n == len;
            // SAFETY: `[base+src_off+off, +n)` lies within `src`, which the caller
            // keeps alive until the completion is reaped; the destination is
            // authorized by the peer-supplied rkey. A bad/stale rkey fails the WR,
            // transitioning the QP to error → `peer_closed` (handled on the
            // completion). The src pointer is stable (the storage is boxed).
            // The immediate rides the last WR (§7.7.4 step 2); tag it so its
            // completion frees a transfer credit (`imm_outstanding`).
            let posted_imm = imm.is_some() && is_last;
            let r = unsafe {
                match imm {
                    Some(id) if is_last => self.conn.post_write_with_imm(
                        imm_write_wr_id(chunk),
                        base.add(src_off + off),
                        n as u32,
                        lkey,
                        peer_addr + off as u64,
                        peer_rkey,
                        id,
                    ),
                    _ => self.conn.post_write(
                        write_wr_id(chunk),
                        base.add(src_off + off),
                        n as u32,
                        lkey,
                        peer_addr + off as u64,
                        peer_rkey,
                    ),
                }
            };
            if let Err(e) = r {
                self.peer_closed = true;
                return Err(e);
            }
            self.writes_outstanding += 1;
            if posted_imm {
                self.imm_outstanding += 1;
            }
            off += n;
            chunk += 1;
        }
        // Zero-length split write: the loop posted nothing, so emit one empty
        // write-with-immediate purely to deliver the transfer ID.
        if len == 0 {
            if let Some(id) = imm {
                // SAFETY: a 0-byte SGE reads nothing from `src`; `base` is a valid
                // registered pointer and the rkey contract is as above.
                let r = unsafe {
                    self.conn.post_write_with_imm(
                        imm_write_wr_id(chunk),
                        base.add(src_off),
                        0,
                        lkey,
                        peer_addr,
                        peer_rkey,
                        id,
                    )
                };
                if let Err(e) = r {
                    self.peer_closed = true;
                    return Err(e);
                }
                self.writes_outstanding += 1;
                self.imm_outstanding += 1;
            }
        }
        Ok(())
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
        // Post, retrying on back-pressure: `begin` returns `WouldBlock` (posting
        // nothing) when either the transfer-credit window (§7.7.6) or the send
        // pool is momentarily full. Reap an outstanding send/write to free the
        // resource and retry. Something must be in flight to drain — a full credit
        // window implies an imm write is pending, a full send pool implies a
        // send/write is — so if NOTHING is outstanding the block can't clear
        // (e.g. an imm write attempted on a connection that never negotiated
        // split, peer_split_credits == 0); surface that rather than spin forever.
        let posted = loop {
            match self.begin_rdma_write_inner(src, src_off, peer_addr, peer_rkey, len, imm) {
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
        // If a WR fails to post mid-batch, the begin call returns Err but the WRs
        // that DID post are still queued on the NIC. We must drain every posted
        // write (reap its completion) before returning, or the caller will drop
        // `src` — deregistering its MR and freeing the storage — while the NIC is
        // still DMA-reading it (use-after-free). So drain first, propagate the
        // post error after.
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
