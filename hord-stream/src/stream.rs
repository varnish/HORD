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

use hord_core::{Completion, Connection, Listener, Opcode, ACCESS_LOCAL_WRITE};

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
}

impl Default for HordConfig {
    fn default() -> Self {
        HordConfig {
            max_message_size: 65536,
            recv_pool_size: 32,
            send_pool_size: 16,
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
///
/// Field order matters for `Drop`: `conn` is declared first so the QP is torn
/// down before the memory regions are deregistered, which in turn happens
/// before the backing buffers are freed.
pub struct HordStream {
    conn: Connection,
    // The MRs are held as RAII guards whose `Drop` deregisters the memory
    // regions. They are `Option` so the explicit `Drop` for `HordStream` can
    // deregister them at the right moment — after the QP/CQ are destroyed but
    // before `conn` deallocates the PD they belong to.
    recv_mr: Option<hord_core::MemoryRegion>,
    send_mr: Option<hord_core::MemoryRegion>,
    recv_buf: Box<[u8]>,
    send_buf: Box<[u8]>,

    msg_size: usize,    // bytes per buffer slot (our max_message_size)
    payload_cap: usize, // max payload per message = min(ours, peer) - ENVELOPE_LEN
    recv_pool: usize,
    send_pool: usize,
    recv_lkey: u32,
    send_lkey: u32,

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

        // recv_buf carries the data pool plus the always-posted control slack;
        // send_buf carries the data pool plus the reserved control send slot
        // (the highest index, `send_pool`).
        let recv_slots = recv_pool + CTRL_RECV_SLACK;
        let send_slots = send_pool + CTRL_SEND_SLOTS;
        let mut recv_buf = vec![0u8; recv_slots * msg_size].into_boxed_slice();
        let mut send_buf = vec![0u8; send_slots * msg_size].into_boxed_slice();
        // SAFETY: recv_buf/send_buf are `Box<[u8]>` whose heap storage stays put
        // for the life of the stream (moving the box moves only the pointer).
        // They are dropped after the MRs are deregistered and the QP destroyed —
        // enforced by HordStream's field order and explicit Drop.
        let (recv_mr, send_mr) = unsafe {
            (
                conn.register(&mut recv_buf, ACCESS_LOCAL_WRITE)?,
                conn.register(&mut send_buf, ACCESS_LOCAL_WRITE)?,
            )
        };
        let recv_lkey = recv_mr.lkey();
        let send_lkey = send_mr.lkey();

        let mut s = HordStream {
            conn,
            recv_mr: Some(recv_mr),
            send_mr: Some(send_mr),
            recv_buf,
            send_buf,
            msg_size,
            payload_cap: 0,
            recv_pool,
            send_pool,
            recv_lkey,
            send_lkey,
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
        let base = self.recv_buf.as_mut_ptr();
        let msg = self.msg_size;
        let lkey = self.recv_lkey;
        // Post the data pool *and* the control slack; both are needed before the
        // QP can carry traffic.
        for slot in 0..self.recv_pool + CTRL_RECV_SLACK {
            // SAFETY: `base + slot*msg` lies within recv_buf (a single MR with
            // `lkey`); the buffer outlives the connection (drop order).
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
        let env = Envelope::decode(&self.recv_buf[off..off + ENVELOPE_LEN]);
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
        let base = self.recv_buf.as_mut_ptr();
        // SAFETY: `base + off` is slot `slot` inside recv_buf / the MR, which
        // outlives the connection (drop order).
        let repost = unsafe {
            let addr = base.add(off);
            self.conn
                .post_recv(recv_wr_id(slot), addr, self.msg_size as u32, self.recv_lkey)
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
        env.encode(&mut self.send_buf[off..off + ENVELOPE_LEN]);
        let pstart = off + ENVELOPE_LEN;
        self.send_buf[pstart..pstart + payload.len()].copy_from_slice(payload);
        let total = (ENVELOPE_LEN + payload.len()) as u32;
        let base = self.send_buf.as_ptr();
        unsafe {
            let addr = base.add(off);
            self.conn.post_send(wr_id, addr, total, self.send_lkey)
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
            out[written..written + take].copy_from_slice(&self.recv_buf[start..start + take]);
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
        // RDMA teardown is order-sensitive:
        //   1. stop the NIC (disconnect + destroy QP/CQ) so nothing can DMA
        //      into our buffers,
        //   2. deregister the MRs — still requires the PD to be alive,
        //   3. let `conn` drop (after this body), which deallocates the now
        //      MR-free PD and destroys the id/event channel; the buffers, held
        //      in later fields, are freed last.
        self.conn.shutdown();
        self.recv_mr.take();
        self.send_mr.take();
    }
}
