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
}

impl Default for HordConfig {
    fn default() -> Self {
        HordConfig {
            max_message_size: 65536,
            recv_pool_size: 32,
            send_pool_size: 16,
            cm: CmParams::default(),
            zero_copy: true,
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

// wr_id encoding: top bit distinguishes sends from receives; the next bit marks
// the reserved control send (so its completion frees the control slot rather
// than a data slot); a third bit marks a one-sided RDMA write (zero-copy), which
// belongs to neither send pool nor recv pool and is reaped by a separate
// counter. Low bits are the buffer/chunk index. Control *receives* are
// recognised by the CREDIT_ONLY envelope flag, not by wr_id (the NIC consumes
// receive WRs FIFO regardless of message type, so the slot carries no lane).
const SEND_FLAG: u64 = 1 << 63;
const CTRL_FLAG: u64 = 1 << 62;
const WRITE_FLAG: u64 = 1 << 61;
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
fn is_send(wr_id: u64) -> bool {
    wr_id & SEND_FLAG != 0
}
fn is_ctrl_send(wr_id: u64) -> bool {
    wr_id & CTRL_FLAG != 0
}
fn is_write(wr_id: u64) -> bool {
    wr_id & WRITE_FLAG != 0
}
fn slot_of(wr_id: u64) -> usize {
    (wr_id & !(SEND_FLAG | CTRL_FLAG | WRITE_FLAG)) as usize
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
        // The QP must hold the control lane's extra WRs on top of the data pools.
        listener.accept(
            config.send_pool_size + CTRL_SEND_SLOTS,
            config.recv_pool_size + CTRL_RECV_SLACK,
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
        let my = Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16)
            .with_zero_copy(config.zero_copy);
        s.conn.accept_finish(&my.encode())?;
        Ok(s)
    }

    /// Client side: connect to `ip:port` and complete the HORD handshake.
    pub fn connect(ip: &str, port: u16, config: &HordConfig) -> io::Result<HordStream> {
        // The QP must hold the control lane's extra WRs on top of the data pools.
        let conn = Connection::connect(
            ip,
            port,
            config.send_pool_size + CTRL_SEND_SLOTS,
            config.recv_pool_size + CTRL_RECV_SLACK,
            config.cm,
        )?;
        let mut s = HordStream::new_common(conn, config)?;
        let my = Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16)
            .with_zero_copy(config.zero_copy);
        let peer_bytes = s.conn.connect_finish(&my.encode(), HANDSHAKE_LEN)?;
        let peer = Handshake::decode(&peer_bytes)?;
        s.apply_peer(&peer)?;
        Ok(s)
    }

    /// Allocate + register the buffer pools and pre-post all receive buffers.
    /// Leaves `send_credits` / `payload_cap` unset until the peer handshake is
    /// known (see [`apply_peer`]).
    fn new_common(conn: Connection, config: &HordConfig) -> io::Result<HordStream> {
        let msg_size = config.max_message_size;
        let recv_pool = config.recv_pool_size;
        let send_pool = config.send_pool_size;
        assert!(msg_size > ENVELOPE_LEN, "max_message_size too small");

        // recv carries the data pool plus the always-posted control slack;
        // send carries the data pool plus the reserved control send slot
        // (the highest index, `send_pool`).
        let recv_slots = recv_pool + CTRL_RECV_SLACK;
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
        };
        s.post_all_recvs()?;
        Ok(s)
    }

    fn post_all_recvs(&mut self) -> io::Result<()> {
        let base = self.recv.as_mut_ptr();
        let msg = self.msg_size;
        let lkey = self.recv.lkey();
        // Post the data pool *and* the control slack; both are needed before the
        // QP can carry traffic.
        for slot in 0..self.recv_pool + CTRL_RECV_SLACK {
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
        // One-sided RDMA write (zero-copy). It belongs to neither the send pool
        // nor the recv pool — just a counter — so reap it first, on both the
        // success and failure paths. A failed write means the peer's buffer is in
        // an undefined state (spec §7.4 mid-write failure), so close the stream;
        // `rdma_write_all` then surfaces it rather than reporting `complete`.
        if is_write(wc.wr_id) {
            self.writes_outstanding = self.writes_outstanding.saturating_sub(1);
            if !wc.is_success() {
                self.peer_closed = true;
            }
            return Ok(());
        }

        if !wc.is_success() {
            // Flush or transport error (commonly seen when the peer
            // disconnects and our outstanding recvs are flushed). Treat as a
            // closed connection rather than a hard error so reads see EOF.
            self.peer_closed = true;
            self.reclaim_send(wc.wr_id);
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
        // already taken by posted writes (writes don't draw from `send_free`).
        let n_wrs = len.div_ceil(WRITE_WR_MAX);
        let free_slots = self
            .send_free
            .len()
            .saturating_sub(self.writes_outstanding as usize);
        if n_wrs > free_slots {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "zero-copy write of {len} bytes needs {n_wrs} send-queue slots, \
                     only {free_slots} free ({} writes already in flight)",
                    self.writes_outstanding
                ),
            ));
        }
        let base = src.as_mut_ptr();
        let lkey = src.lkey();
        let mut off = 0usize;
        let mut chunk = 0u64;
        while off < len {
            let n = (len - off).min(WRITE_WR_MAX);
            // SAFETY: `[base+src_off+off, +n)` lies within `src`, which the caller
            // keeps alive until the completion is reaped; the destination is
            // authorized by the peer-supplied rkey. A bad/stale rkey fails the WR,
            // transitioning the QP to error → `peer_closed` (handled on the
            // completion). The src pointer is stable (the storage is boxed).
            let r = unsafe {
                self.conn.post_write(
                    write_wr_id(chunk),
                    base.add(src_off + off),
                    n as u32,
                    lkey,
                    peer_addr + off as u64,
                    peer_rkey,
                )
            };
            if let Err(e) = r {
                self.peer_closed = true;
                return Err(e);
            }
            self.writes_outstanding += 1;
            off += n;
            chunk += 1;
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
        // If a WR fails to post mid-batch, `begin_rdma_write` returns Err but the
        // WRs that DID post are still queued on the NIC. We must drain every
        // posted write (reap its completion) before returning, or the caller will
        // drop `src` — deregistering its MR and freeing the storage — while the
        // NIC is still DMA-reading it (use-after-free). So drain first, propagate
        // the post error after.
        let posted = self.begin_rdma_write(src, src_off, peer_addr, peer_rkey, len);
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
