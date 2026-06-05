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
use std::sync::Arc;

use hord_core::{
    CmParams, Completion, Connection, Listener, Opcode, RegisteredBuffer, ACCESS_LOCAL_WRITE,
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
}

impl Default for HordConfig {
    fn default() -> Self {
        HordConfig {
            max_message_size: 65536,
            recv_pool_size: 32,
            send_pool_size: 16,
            cm: CmParams::default(),
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
// than a data slot); low bits are the buffer slot index. Control *receives* are
// recognised by the CREDIT_ONLY envelope flag, not by wr_id (the NIC consumes
// receive WRs FIFO regardless of message type, so the slot carries no lane).
const SEND_FLAG: u64 = 1 << 63;
const CTRL_FLAG: u64 = 1 << 62;
fn recv_wr_id(slot: usize) -> u64 {
    slot as u64
}
fn send_wr_id(slot: usize) -> u64 {
    SEND_FLAG | slot as u64
}
fn ctrl_send_wr_id(slot: usize) -> u64 {
    SEND_FLAG | CTRL_FLAG | slot as u64
}
fn is_send(wr_id: u64) -> bool {
    wr_id & SEND_FLAG != 0
}
fn is_ctrl_send(wr_id: u64) -> bool {
    wr_id & CTRL_FLAG != 0
}
fn slot_of(wr_id: u64) -> usize {
    (wr_id & !(SEND_FLAG | CTRL_FLAG)) as usize
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
}

impl HordStream {
    /// Server side: accept the next connection on `listener` and complete the
    /// HORD handshake.
    pub fn accept(listener: &Listener, config: &HordConfig) -> io::Result<HordStream> {
        // The QP must hold the control lane's extra WRs on top of the data pools.
        let (conn, peer_bytes) = listener.accept(
            config.send_pool_size + CTRL_SEND_SLOTS,
            config.recv_pool_size + CTRL_RECV_SLACK,
            HANDSHAKE_LEN,
            config.cm,
        )?;
        let mut s = HordStream::new_common(conn, config)?;
        let peer = Handshake::decode(&peer_bytes)?;
        s.apply_peer(&peer)?;
        let my = Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16);
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
        let my = Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16);
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

    /// Post one *data* message carrying `payload` (<= payload_cap). Blocks until
    /// a send slot and a data credit are available, processing completions
    /// meanwhile. While blocked on credits, returns any owed grants via the
    /// control lane so a peer waiting on us can make progress — this is what
    /// breaks the full-duplex deadlock (#3).
    fn send_message(&mut self, payload: &[u8]) -> io::Result<()> {
        debug_assert!(payload.len() <= self.payload_cap);
        loop {
            if self.peer_closed {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "connection closed",
                ));
            }
            if !self.send_free.is_empty() && self.send_credits > 0 {
                break;
            }
            // We can't send data (no credit and/or no slot). Return whatever we
            // owe the peer now, over the control lane (no data credit needed),
            // so the peer can grant us credits back. Then wait for a completion.
            self.maybe_return_credits(1)?;
            self.pump(true)?;
        }

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
}

impl Read for HordStream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.rx_ready.is_empty() {
            if self.peer_closed {
                return Ok(0); // EOF
            }
            self.pump(true)?;
            self.maybe_return_credits(self.proactive_threshold())?;
        }

        // Copy payload straight out of the receive buffers (no intermediate
        // reassembly copy). As each held message is fully drained, re-post its
        // buffer and record that we now owe the peer one credit — so credit is
        // returned on *consumption*, not receipt.
        let mut written = 0;
        while written < out.len() {
            let Some(&ReadyMsg { slot, start, end }) = self.rx_ready.front() else {
                break;
            };
            let take = (end - start).min(out.len() - written);
            self.recv.copy_out(start, &mut out[written..written + take]);
            written += take;
            if start + take == end {
                // Message fully drained: free its buffer and owe a credit.
                self.rx_ready.pop_front();
                self.repost_recv(slot)?;
                self.grant_pending += 1;
            } else {
                // Partially drained: advance the read cursor and stop (`out` full).
                self.rx_ready.front_mut().unwrap().start = start + take;
            }
        }

        // Returning data frees receive capacity; let the peer know.
        self.maybe_return_credits(self.proactive_threshold())?;
        Ok(written)
    }
}

impl Write for HordStream {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self.peer_closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed",
            ));
        }
        let cap = self.payload_cap;
        let mut input = data;

        // `tx_stage` only ever holds a partial (sub-`cap`) message. First top it
        // up from `input` and, if it fills, send it. We move it out to satisfy
        // the borrow checker, since `send_message` borrows `&mut self`.
        if !self.tx_stage.is_empty() {
            let need = cap - self.tx_stage.len();
            let take = need.min(input.len());
            let mut staged = std::mem::take(&mut self.tx_stage);
            staged.extend_from_slice(&input[..take]);
            input = &input[take..];
            if staged.len() == cap {
                self.send_message(&staged)?;
                staged.clear();
            }
            self.tx_stage = staged;
        }

        // Send whole messages straight from the caller's buffer — no per-message
        // front-draining, so the write path is O(n) in the body size.
        while input.len() >= cap {
            self.send_message(&input[..cap])?;
            input = &input[cap..];
        }

        // Stage the remainder (< cap) for the next write or flush.
        self.tx_stage.extend_from_slice(input);
        Ok(data.len())
    }

    /// Flush emits any staged bytes and then waits for *all* posted sends to
    /// complete. For RC, a send completion means the message has been placed in
    /// the peer's receive buffer and acknowledged — so once `flush` returns
    /// `Ok`, the data has been delivered and it is safe to disconnect.
    fn flush(&mut self) -> io::Result<()> {
        if !self.tx_stage.is_empty() {
            let chunk = std::mem::take(&mut self.tx_stage);
            self.send_message(&chunk)?;
        }
        while self.send_free.len() < self.send_pool {
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
                s.send_message(&to_send[sent..end]).expect("send_message");
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
                s.send_message(&to_send[sent..end]).expect("send_message");
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
