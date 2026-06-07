//! Zero-copy (spec §7.1–7.4) end to end over the host's Soft-RoCE device
//! (`rxe0`, see CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-zerocopy -- --ignored --nocapture
//! ```
//!
//! Exercises the whole sync zero-copy path: capability negotiation, the client
//! registering a remote-writable buffer and advertising it via the request
//! codec, the server's [`serve_rdma_write`] (register source → fill → one-sided
//! RDMA write → status), and the client reading the payload straight out of its
//! buffer. Two cases: an object that fits (→ `complete`, integrity-checked) and
//! one that does not (→ `too_large`, no write). The request/response *framing* is
//! a single newline-terminated header value each way — the HTTP layer lives in
//! the demo; here we test the zero-copy mechanics directly.

// Exercises the RDMA orchestration over Soft-RoCE (`rxe0`), so the whole test
// crate is gated on the `rdma` feature: the default device-free codec build —
// `cargo test -p hord-zerocopy` without the feature — skips it entirely.
#![cfg(feature = "rdma")]

use std::io::{Read, Write};
use std::sync::{mpsc, Arc, Barrier};

use hord_stream::{HordConfig, HordStream, Listener, RegisteredBuffer};
use hord_zerocopy::{serve_rdma_write, RdmaWriteReq, RdmaWriteStatus, ZeroCopyRequest};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const OBJECT: usize = 4 * 1024 * 1024; // 4 MiB — many MTUs, dwarfs the credit window

/// Deterministic, position-sensitive payload byte (matches the demo's pattern).
fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

/// Read one `\n`-terminated line (the lines here are tiny header values).
fn read_line(s: &mut HordStream) -> String {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        match s.read(&mut b).expect("read line") {
            0 => break,             // EOF
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

/// Fill the first `n` bytes of a registered source with the pattern.
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

/// Drive one zero-copy exchange over a fresh connection on `port`: the client
/// advertises a `client_cap`-byte buffer and requests an `OBJECT`-byte object.
/// Returns the response status the client parsed; on `complete` it has already
/// verified the in-buffer payload against the pattern.
fn run_case(port: u16, client_cap: usize) -> RdmaWriteStatus {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let teardown = Arc::new(Barrier::new(2));

    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, port).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
        assert!(s.zero_copy_negotiated(), "server: zero-copy not negotiated");

        let req = RdmaWriteReq::parse(&read_line(&mut s)).expect("parse request header");
        // The object is OBJECT bytes; serve_rdma_write writes it (or reports
        // too_large) and we relay the status back over the stream.
        let status = serve_rdma_write(&mut s, &req, OBJECT as u64, |buf| fill(buf, OBJECT))
            .expect("serve_rdma_write");
        write_line(&mut s, &status.header_value());
        srv_teardown.wait();
    });

    ready_rx.recv().expect("server ready");
    let mut s = HordStream::connect(IP, port, &config).expect("connect");
    assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");

    let zc = ZeroCopyRequest::new(&s, client_cap).expect("register dest");
    write_line(&mut s, &zc.request().header_value());
    let status = RdmaWriteStatus::parse(&read_line(&mut s)).expect("parse response status");

    if let RdmaWriteStatus::Complete { bytes_written } = status {
        let n = bytes_written as usize;
        assert!(n <= zc.capacity(), "bytes_written {n} exceeds buffer {}", zc.capacity());
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

    teardown.wait();
    drop(s);
    server.join().expect("server thread panicked");
    status
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn zero_copy_complete_round_trip() {
    // Buffer fits the object: the body is RDMA-written and integrity-checked.
    let status = run_case(18720, OBJECT);
    assert_eq!(
        status,
        RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 },
        "expected a complete zero-copy write"
    );
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn zero_copy_too_large() {
    // Buffer smaller than the object: the server declines with too_large and
    // performs no write.
    let status = run_case(18721, 1024 * 1024);
    assert_eq!(
        status,
        RdmaWriteStatus::TooLarge { object_size: OBJECT as u64 },
        "expected too_large"
    );
}
