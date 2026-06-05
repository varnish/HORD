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
//! Each side starts with `peer.max_recv_buffers` send credits. Posting a
//! message costs one credit. As we drain received messages and re-post their
//! receive buffers we accrue a *grant debt* to the peer, which we repay by
//! stamping the envelope `credits` field of outgoing messages — or, if we have
//! no data to send, via a zero-length `CREDIT_ONLY` message.

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

// wr_id encoding: top bit distinguishes sends from receives; low bits are the
// buffer slot index.
const SEND_FLAG: u64 = 1 << 63;
fn recv_wr_id(slot: usize) -> u64 {
    slot as u64
}
fn send_wr_id(slot: usize) -> u64 {
    SEND_FLAG | slot as u64
}
fn is_send(wr_id: u64) -> bool {
    wr_id & SEND_FLAG != 0
}
fn slot_of(wr_id: u64) -> usize {
    (wr_id & !SEND_FLAG) as usize
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

    send_free: Vec<usize>, // free send slot indices
    send_credits: u32,     // messages we may still post to the peer
    grant_pending: u32,    // credits we owe the peer (re-posted recvs not yet announced)

    tx_stage: Vec<u8>,        // bytes buffered by write(), drained into messages
    rx_ready: VecDeque<ReadyMsg>, // received data, still in its recv buffer, awaiting read()
    peer_closed: bool,        // observed a flush/transport error -> treat as EOF/broken pipe
}

impl HordStream {
    /// Server side: accept the next connection on `listener` and complete the
    /// HORD handshake.
    pub fn accept(listener: &Listener, config: &HordConfig) -> io::Result<HordStream> {
        let (conn, peer_bytes) =
            listener.accept(config.send_pool_size, config.recv_pool_size, HANDSHAKE_LEN)?;
        let mut s = HordStream::new_common(conn, config)?;
        let peer = Handshake::decode(&peer_bytes)?;
        s.apply_peer(&peer)?;
        let my = Handshake::new(config.max_message_size as u32, config.recv_pool_size as u16);
        s.conn.accept_finish(&my.encode())?;
        Ok(s)
    }

    /// Client side: connect to `ip:port` and complete the HORD handshake.
    pub fn connect(ip: &str, port: u16, config: &HordConfig) -> io::Result<HordStream> {
        let conn = Connection::connect(ip, port, config.send_pool_size, config.recv_pool_size)?;
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

        let mut recv_buf = vec![0u8; recv_pool * msg_size].into_boxed_slice();
        let mut send_buf = vec![0u8; send_pool * msg_size].into_boxed_slice();
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
        for slot in 0..self.recv_pool {
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
            if is_send(wc.wr_id) {
                self.send_free.push(slot_of(wc.wr_id));
            }
            return Ok(());
        }

        if is_send(wc.wr_id) {
            self.send_free.push(slot_of(wc.wr_id));
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
        } else {
            // Nothing to hold (a CREDIT_ONLY top-up, or a zero-length data
            // message): re-post immediately and return the credit now.
            self.repost_recv(slot)?;
            self.grant_pending += 1;
        }
        Ok(())
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

    /// Post one message carrying `payload` (<= payload_cap). Blocks until a
    /// send slot and a credit are available, processing completions meanwhile.
    fn send_message(&mut self, payload: &[u8], credit_only: bool) -> io::Result<()> {
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
            self.pump(true)?;
        }

        let slot = self.send_free.pop().unwrap();
        let off = slot * self.msg_size;
        let grant = self.grant_pending.min(u16::MAX as u32);
        let flags = if credit_only {
            env_flags::CREDIT_ONLY
        } else {
            0
        };
        let env = Envelope {
            length: payload.len() as u32,
            credits: grant as u16,
            flags,
        };
        env.encode(&mut self.send_buf[off..off + ENVELOPE_LEN]);
        let pstart = off + ENVELOPE_LEN;
        self.send_buf[pstart..pstart + payload.len()].copy_from_slice(payload);
        let total = (ENVELOPE_LEN + payload.len()) as u32;

        let base = self.send_buf.as_ptr();
        // SAFETY: `base + off` is the start of this slot inside send_buf / the
        // send MR; the buffer stays put until the send completion is reaped.
        let post = unsafe {
            let addr = base.add(off);
            self.conn
                .post_send(send_wr_id(slot), addr, total, self.send_lkey)
        };
        if let Err(e) = post {
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

    /// If we owe the peer a meaningful number of credits and have nothing else
    /// to send, return them proactively via a CREDIT_ONLY message. Keeps a
    /// one-directional bulk transfer from stalling.
    fn maybe_flush_credits(&mut self) -> io::Result<()> {
        let threshold = (self.recv_pool as u32 / 4).max(1);
        if self.grant_pending >= threshold
            && self.send_credits > 0
            && self.tx_stage.is_empty()
            && !self.peer_closed
        {
            self.send_message(&[], true)?;
        }
        Ok(())
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
            self.maybe_flush_credits()?;
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
        self.maybe_flush_credits()?;
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
                self.send_message(&staged, false)?;
                staged.clear();
            }
            self.tx_stage = staged;
        }

        // Send whole messages straight from the caller's buffer — no per-message
        // front-draining, so the write path is O(n) in the body size.
        while input.len() >= cap {
            self.send_message(&input[..cap], false)?;
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
            self.send_message(&chunk, false)?;
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
