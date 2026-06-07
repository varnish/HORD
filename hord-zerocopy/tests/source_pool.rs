//! Server source buffer pool (spec §8.3) end to end over the host's Soft-RoCE
//! device (`rxe0`, see CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-zerocopy --test source_pool -- --ignored --nocapture
//! ```
//!
//! Proves the registration-amortization property: a server that serves several
//! zero-copy responses on one connection via [`serve_rdma_write_pooled`] reuses a
//! pooled source MR instead of registering one per response — so after `K`
//! responses the pool has registered far fewer than `K` buffers (just one, since
//! HTTP-style serving is sequential and only one source is in flight at a time),
//! made no fallbacks, and every buffer has been returned. A companion case forces
//! the fallback path (object larger than the slab) and confirms correctness still
//! holds. Each payload is integrity-checked, so reuse never serves stale bytes.
//!
//! The request/response *framing* is a single newline-terminated header value each
//! way — the HTTP layer lives in the demo; here we test the pool mechanics directly.

// Exercises the RDMA orchestration over Soft-RoCE (`rxe0`), so the whole test
// crate is gated on the `rdma` feature: the default device-free codec build —
// `cargo test -p hord-zerocopy` without the feature — skips it entirely.
#![cfg(feature = "rdma")]

use std::io::{Read, Write};
use std::sync::{mpsc, Arc, Barrier};

use hord_stream::{HordConfig, HordStream, Listener, RegisteredBuffer};
use hord_zerocopy::{serve_rdma_write_pooled, RdmaWriteReq, RdmaWriteStatus, SourcePool, ZeroCopyRequest};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const OBJECT: usize = 2 * 1024 * 1024; // 2 MiB — many MTUs, dwarfs the credit window

/// Deterministic, position-sensitive payload byte (matches the demo's pattern).
fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

fn read_line(s: &mut HordStream) -> String {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        match s.read(&mut b).expect("read line") {
            0 => break,
            _ if b[0] == b'\n' => break,
            _ => out.push(b[0]),
        }
    }
    String::from_utf8(out).expect("line is utf-8")
}

fn write_line(s: &mut HordStream, line: &str) {
    s.write_all(line.as_bytes()).expect("write line");
    s.write_all(b"\n").expect("write newline");
    s.flush().expect("flush line");
}

fn fill(buf: &RegisteredBuffer, n: usize) {
    const CHUNK: usize = 256 * 1024;
    let mut tmp = vec![0u8; CHUNK.min(n.max(1))];
    let mut off = 0;
    while off < n {
        let take = CHUNK.min(n - off);
        for (i, b) in tmp[..take].iter_mut().enumerate() {
            *b = pattern_byte(off + i);
        }
        buf.copy_in(off, &tmp[..take]);
        off += take;
    }
}

fn verify(zc: &ZeroCopyRequest, n: usize) {
    let mut tmp = vec![0u8; n.clamp(1, 256 * 1024)];
    let mut off = 0;
    while off < n {
        let take = tmp.len().min(n - off);
        zc.copy_out(off, &mut tmp[..take]);
        for (i, &got) in tmp[..take].iter().enumerate() {
            assert_eq!(got, pattern_byte(off + i), "payload mismatch at byte {}", off + i);
        }
        off += take;
    }
}

/// Pool counters the server observed after serving `k` responses on one connection.
struct PoolStats {
    registered: usize,
    fallbacks: u64,
    available: usize,
}

/// Serve `k` zero-copy responses of `OBJECT` bytes on a single connection, from a
/// pool of cap `cap` / slab `buf_size`, integrity-checking each payload on the
/// client. Returns the pool counters the server saw afterwards.
fn run_pool_case(port: u16, k: usize, cap: usize, buf_size: usize) -> PoolStats {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (stats_tx, stats_rx) = mpsc::channel::<PoolStats>();
    let teardown = Arc::new(Barrier::new(2));

    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, port).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
        assert!(s.zero_copy_negotiated(), "server: zero-copy not negotiated");

        // One pool reused across every response on this connection.
        let pool = SourcePool::new(cap, buf_size);
        for _ in 0..k {
            let req = RdmaWriteReq::parse(&read_line(&mut s)).expect("parse request header");
            let status = serve_rdma_write_pooled(&mut s, &pool, &req, OBJECT as u64, |buf| {
                fill(buf, OBJECT)
            })
            .expect("serve_rdma_write_pooled");
            write_line(&mut s, &status.header_value());
        }
        stats_tx
            .send(PoolStats {
                registered: pool.registered(),
                fallbacks: pool.fallbacks(),
                available: pool.available(),
            })
            .expect("send stats");
        srv_teardown.wait();
    });

    ready_rx.recv().expect("server ready");
    let mut s = HordStream::connect(IP, port, &config).expect("connect");
    assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");

    // One destination buffer reused across all k requests (the server overwrites it
    // each time; we verify after each, before issuing the next).
    let zc = ZeroCopyRequest::new(&s, OBJECT).expect("register dest");
    for _ in 0..k {
        write_line(&mut s, &zc.request().header_value());
        match RdmaWriteStatus::parse(&read_line(&mut s)).expect("parse response status") {
            RdmaWriteStatus::Complete { bytes_written } => {
                assert_eq!(bytes_written as usize, OBJECT, "unexpected bytes_written");
                verify(&zc, OBJECT);
            }
            other => panic!("expected complete, got {other:?}"),
        }
    }

    let stats = stats_rx.recv().expect("recv pool stats");
    teardown.wait();
    drop(s);
    server.join().expect("server thread panicked");
    stats
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn pool_amortizes_registration() {
    // Slab fits the object and the cap covers the (sequential) in-flight load, so
    // every response after the first reuses the one pooled buffer: 5 responses,
    // 1 registration, 0 fallbacks, and the buffer returned to the pool at the end.
    let stats = run_pool_case(18740, 5, 2, OBJECT);
    assert_eq!(stats.fallbacks, 0, "no response should have fallen back");
    assert_eq!(
        stats.registered, 1,
        "5 sequential responses should reuse a single registered source buffer"
    );
    assert_eq!(stats.available, stats.registered, "every leased buffer should return to the pool");
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn pool_falls_back_for_oversized_objects() {
    // Slab smaller than the object (§8.4): every response falls back to a one-off
    // registration, the pool grows none — and correctness still holds (each payload
    // is integrity-checked in run_pool_case).
    let stats = run_pool_case(18741, 4, 2, OBJECT / 4);
    assert_eq!(stats.registered, 0, "oversized objects must not grow the pool");
    assert_eq!(stats.fallbacks, 4, "every oversized response should fall back once");
    assert_eq!(stats.available, 0, "no pooled buffers to return");
}
