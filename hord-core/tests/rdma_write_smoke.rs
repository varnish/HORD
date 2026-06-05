//! Smoke test for the one-sided RDMA write verb ([`Connection::post_write`]).
//!
//! This exercises the new transport primitive in isolation — no HORD stream,
//! handshake, envelope or credit logic. One endpoint registers a buffer with
//! `ACCESS_REMOTE_WRITE` and hands its address + rkey to the peer out-of-band
//! (an in-process channel, since both endpoints run in one test); the peer
//! RDMA-writes a pattern straight into it and reaps the single send-side
//! completion. We use a 16 MiB region so the write spans many link MTUs — that
//! is the assumption Pass 4's write driver rests on (a single large WR, segmented
//! by the NIC), so it is the thing worth de-risking first.
//!
//! Needs the host's Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d.
//! Run with:
//!
//! ```sh
//! cargo test -p hord-core -- --ignored --nocapture rdma_write_round_trip
//! ```

// Connection is Send but not Sync (its shim methods aren't safe to call
// concurrently); we use Arc purely for shared ownership on one thread, exactly
// as the stream layer does.
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use hord_core::{
    CmParams, Connection, Listener, Opcode, ACCESS_LOCAL_WRITE, ACCESS_REMOTE_WRITE,
};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18520; // distinct from the demo (4791) and full_duplex_bulk (18519)
const LEN: usize = 16 * 1024 * 1024; // 16 MiB — many MTUs in one WR
const HS: &[u8] = b"hord-write-smoke"; // dummy CM private data (16 bytes)

/// A position-sensitive byte pattern so the reader can verify exactly what was
/// written (and that nothing landed where it should not).
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
fn rdma_write_round_trip() {
    // server -> client: the listener is up.
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    // client -> server: the destination region (addr, rkey).
    let (target_tx, target_rx) = mpsc::channel::<(u64, u32)>();
    // server -> client: the write's completion has been reaped (data has landed).
    let (done_tx, done_rx) = mpsc::channel::<()>();
    // Hold both QPs open until both sides are finished, so neither tears the
    // connection down with the other still using it.
    let teardown = Arc::new(Barrier::new(2));

    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let (conn, _peer) = listener
            .accept(4, 4, HS.len(), CmParams::default())
            .expect("accept");
        let conn = Arc::new(conn);
        // Source region: filled with a known pattern; the NIC only reads it, so
        // local access (no remote flag) suffices.
        let src = conn
            .register_buffer(LEN, ACCESS_LOCAL_WRITE)
            .expect("register src");
        src.copy_in(0, &pattern(LEN, 0xC3));
        conn.accept_finish(HS).expect("accept_finish"); // -> RTS

        let (raddr, rkey) = target_rx.recv().expect("recv target");
        // One WR for the whole 16 MiB region.
        unsafe {
            conn.post_write(1, src.as_mut_ptr(), LEN as u32, src.lkey(), raddr, rkey)
                .expect("post_write");
        }
        // Reap the single send-side completion.
        let start = Instant::now();
        let wc = loop {
            if let Some(wc) = conn.poll().expect("poll") {
                break wc;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "RDMA write never completed"
            );
            std::hint::spin_loop();
        };
        assert!(wc.is_success(), "write completion status {}", wc.status);
        assert_eq!(wc.opcode, Opcode::RdmaWrite, "unexpected completion opcode");
        assert_eq!(wc.wr_id, 1, "unexpected completion wr_id");

        done_tx.send(()).expect("signal done");
        srv_teardown.wait();
        conn.shutdown(); // stop the NIC before src/MR drops
    });

    ready_rx.recv().expect("server ready");
    let conn = Connection::connect(IP, PORT, 4, 4, CmParams::default()).expect("connect");
    let conn = Arc::new(conn);
    // Destination region: zeroed, registered for remote write (local+remote).
    let dst = conn
        .register_buffer(LEN, ACCESS_LOCAL_WRITE | ACCESS_REMOTE_WRITE)
        .expect("register dst");
    let _peer = conn.connect_finish(HS, HS.len()).expect("connect_finish"); // -> RTS

    // Advertise the destination, then wait until the server says the data landed.
    target_tx
        .send((dst.as_mut_ptr() as u64, dst.rkey()))
        .expect("send target");
    done_rx.recv().expect("await write");

    // The bytes must now be in our buffer, untouched outside [0, LEN).
    let mut got = vec![0u8; LEN];
    dst.copy_out(0, &mut got);
    assert_eq!(got, pattern(LEN, 0xC3), "RDMA-written payload mismatch");

    teardown.wait();
    conn.shutdown(); // stop the NIC before dst/MR drops
    drop(dst);
    server.join().expect("server thread panicked");
}

/// Smoke test for RDMA write-with-immediate ([`Connection::post_write_with_imm`])
/// — the verb protocol splitting (§7.7) rests on. Unlike a plain write, this
/// delivers a 32-bit immediate to the *receiver's* CQ as a
/// `RecvRdmaWithImm` completion, consuming one of its posted receive WRs. So the
/// client learns the payload landed from its **own** CQ — no out-of-band signal,
/// no HTTP — which is the entire point of the split data plane.
///
/// We pick an immediate with bytes set across all four octets so a byte-order
/// bug in the `htonl`/`ntohl` round-trip (shim) would corrupt it visibly.
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn rdma_write_with_imm_round_trip() {
    const PORT_IMM: u16 = 18521; // distinct from rdma_write_round_trip (18520)
    const TRANSFER_ID: u32 = 0xA5C3_1234; // all four octets distinct

    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (target_tx, target_rx) = mpsc::channel::<(u64, u32)>();
    let teardown = Arc::new(Barrier::new(2));

    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT_IMM).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let (conn, _peer) = listener
            .accept(4, 4, HS.len(), CmParams::default())
            .expect("accept");
        let conn = Arc::new(conn);
        let src = conn
            .register_buffer(LEN, ACCESS_LOCAL_WRITE)
            .expect("register src");
        src.copy_in(0, &pattern(LEN, 0x5A));
        conn.accept_finish(HS).expect("accept_finish"); // -> RTS

        let (raddr, rkey) = target_rx.recv().expect("recv target");
        // One write-with-immediate for the whole region: lands the payload AND
        // signals completion with the transfer ID.
        unsafe {
            conn.post_write_with_imm(
                1,
                src.as_mut_ptr(),
                LEN as u32,
                src.lkey(),
                raddr,
                rkey,
                TRANSFER_ID,
            )
            .expect("post_write_with_imm");
        }
        // The sender still reaps an ordinary RdmaWrite completion.
        let start = Instant::now();
        let wc = loop {
            if let Some(wc) = conn.poll().expect("poll") {
                break wc;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "RDMA write-with-imm never completed (sender)"
            );
            std::hint::spin_loop();
        };
        assert!(wc.is_success(), "sender completion status {}", wc.status);
        assert_eq!(wc.opcode, Opcode::RdmaWrite, "sender opcode");
        assert_eq!(wc.wr_id, 1, "sender wr_id");

        srv_teardown.wait();
        conn.shutdown();
    });

    ready_rx.recv().expect("server ready");
    let conn = Connection::connect(IP, PORT_IMM, 4, 4, CmParams::default()).expect("connect");
    let conn = Arc::new(conn);
    // Destination for the payload (remote-writable), plus a small receive buffer
    // that the immediate will consume — the payload itself goes to `dst` via the
    // remote address, not into this recv slot.
    let dst = conn
        .register_buffer(LEN, ACCESS_LOCAL_WRITE | ACCESS_REMOTE_WRITE)
        .expect("register dst");
    let rx = conn
        .register_buffer(64, ACCESS_LOCAL_WRITE)
        .expect("register rx");
    // Pre-post the receive before the QP goes live, so the peer's
    // write-with-imm never hits a receiver-not-ready.
    unsafe {
        conn.post_recv(7, rx.as_mut_ptr(), rx.len() as u32, rx.lkey())
            .expect("post_recv");
    }
    let _peer = conn.connect_finish(HS, HS.len()).expect("connect_finish"); // -> RTS

    target_tx
        .send((dst.as_mut_ptr() as u64, dst.rkey()))
        .expect("send target");

    // Wait on our OWN CQ for the immediate — this is the data-plane signal.
    let start = Instant::now();
    let wc = loop {
        if let Some(wc) = conn.poll().expect("poll") {
            break wc;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "write-with-imm completion never arrived (receiver)"
        );
        std::hint::spin_loop();
    };
    assert!(wc.is_success(), "receiver completion status {}", wc.status);
    assert_eq!(
        wc.opcode,
        Opcode::RecvRdmaWithImm,
        "receiver opcode (expected RECV_RDMA_WITH_IMM)"
    );
    assert_eq!(wc.wr_id, 7, "consumed our posted recv WR");
    assert_eq!(
        wc.imm_data, TRANSFER_ID,
        "transfer ID corrupted in flight (byte order?)"
    );

    // QP ordering guarantees the payload is fully landed by the time the
    // immediate's completion surfaces (§7.7.2).
    let mut got = vec![0u8; LEN];
    dst.copy_out(0, &mut got);
    assert_eq!(got, pattern(LEN, 0x5A), "write-with-imm payload mismatch");

    teardown.wait();
    conn.shutdown();
    drop(dst);
    drop(rx);
    server.join().expect("server thread panicked");
}
