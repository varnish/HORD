//! HORD §7.6 range requests, composed with the zero-copy RDMA-write path, end to
//! end over the host's Soft-RoCE device (`rxe0`, see CLAUDE.md) — so it is
//! `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-demo --test range_loopback -- --ignored --nocapture
//! ```
//!
//! Proves the key §7.6 property: the one-sided write mechanism is
//! *offset-agnostic*, so a sub-range is delivered exactly like a whole object —
//! the server fills its source from the range's absolute object offset and writes
//! `len` bytes into the client's range-sized buffer, which the client verifies
//! against `pattern_byte(start + i)`. Uses the real `parse_range` /
//! `content_range` codec (hord-demo) + `serve_rdma_write` (hord-zerocopy); the
//! `Range` header *string* parsing is unit-tested device-free in the lib.

use std::io::{Read, Write};
use std::sync::{mpsc, Arc, Barrier};

use hord_demo::{
    content_range, content_range_unsatisfied, parse_content_range, parse_range, pattern_byte,
    pattern_fill_registered_from, RangeSpec,
};
use hord_stream::{HordConfig, HordStream, Listener};
use hord_zerocopy::{serve_rdma_write, RdmaWriteReq, RdmaWriteStatus, ZeroCopyRequest};

static IP: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| std::env::var("HORD_TEST_IP").unwrap_or_else(|_| "192.0.2.1".to_string())); // rxe device IP; override via $HORD_TEST_IP (see CLAUDE.md)
const OBJECT: usize = 4 * 1024 * 1024; // 4 MiB object the range carves into

/// Read one `\n`-terminated line (the lines here are tiny header values).
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

/// What the client observed for a range request.
#[derive(Debug)]
enum Outcome {
    /// A satisfied range (`206`): the client has already verified the bytes.
    Complete { start: usize, len: usize },
    /// An unsatisfiable range (`416`): the server's `Content-Range: */total`.
    Unsatisfiable(String),
}

/// Drive one range exchange on a fresh connection: the client resolves
/// `range_spec` against `OBJECT` to size its destination buffer, advertises it,
/// and sends the range; the server resolves the same spec, RDMA-writes the
/// sub-range (or declines as unsatisfiable), and reports the outcome. The control
/// exchange is two `\n`-terminated lines each way (the HTTP layer lives in the
/// demo binary; here we drive the §7.6 mechanics directly).
fn run_range_case(port: u16, range_spec: &str) -> Outcome {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let teardown = Arc::new(Barrier::new(2));

    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, port).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
        assert!(s.zero_copy_negotiated(), "server: zero-copy not negotiated");

        let req = RdmaWriteReq::parse(&read_line(&mut s)).expect("parse request header");
        let range_line = read_line(&mut s);
        match parse_range(&range_line, OBJECT) {
            RangeSpec::Range { start, end } => {
                let len = end - start + 1;
                // serve_rdma_write is offset-agnostic: object_size = the range
                // length, and we fill the source from the range's absolute offset.
                let status = serve_rdma_write(&mut s, &req, len as u64, |buf| {
                    pattern_fill_registered_from(buf, start, len)
                })
                .expect("serve_rdma_write");
                write_line(
                    &mut s,
                    &format!("{}|{}", status.header_value(), content_range(start, end, OBJECT)),
                );
            }
            RangeSpec::Unsatisfiable => {
                // §7.6/§7.4: 416, Content-Range */total, and no write at all.
                write_line(&mut s, &format!("416|{}", content_range_unsatisfied(OBJECT)));
            }
            RangeSpec::Full => panic!("test drives only satisfiable / unsatisfiable ranges"),
        }
        srv_teardown.wait();
    });

    ready_rx.recv().expect("server ready");
    let mut s = HordStream::connect(&IP, port, &config).expect("connect");
    assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");

    // Size the destination buffer to the range (1 byte if unsatisfiable — it is
    // never written), and remember the absolute base offset to verify against.
    let (base, cap) = match parse_range(range_spec, OBJECT) {
        RangeSpec::Range { start, end } => (start, end - start + 1),
        _ => (0, 1),
    };
    let zc = ZeroCopyRequest::new(&s, cap).expect("register dest");
    write_line(&mut s, &zc.request().header_value());
    write_line(&mut s, range_spec);

    let reply = read_line(&mut s);
    let (status_v, cr_v) = reply.split_once('|').expect("reply is status|content-range");

    let outcome = match RdmaWriteStatus::parse(status_v) {
        Some(RdmaWriteStatus::Complete { bytes_written }) => {
            let len = bytes_written as usize;
            let (cr_start, cr_end, cr_total) = parse_content_range(cr_v).expect("content-range");
            assert_eq!(cr_total, OBJECT, "Content-Range total");
            assert_eq!(cr_start, base, "Content-Range start");
            assert_eq!(len, cr_end - cr_start + 1, "Content-Range length vs bytes_written");
            assert!(len <= zc.capacity(), "bytes_written {len} exceeds buffer {}", zc.capacity());
            // Verify the delivered sub-range against the *absolute* object pattern.
            let mut tmp = vec![0u8; len.clamp(1, 256 * 1024)];
            let mut off = 0;
            while off < len {
                let take = tmp.len().min(len - off);
                zc.copy_out(off, &mut tmp[..take]);
                for (i, &got) in tmp[..take].iter().enumerate() {
                    assert_eq!(
                        got,
                        pattern_byte(base + off + i),
                        "payload mismatch at object byte {}",
                        base + off + i
                    );
                }
                off += take;
            }
            Outcome::Complete { start: base, len }
        }
        Some(other) => panic!("unexpected zero-copy status: {other:?}"),
        None => {
            assert_eq!(status_v, "416", "non-status reply must be 416");
            Outcome::Unsatisfiable(cr_v.to_string())
        }
    };

    teardown.wait();
    drop(s);
    server.join().expect("server thread panicked");
    outcome
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn range_zero_copy_round_trip() {
    // A 2 MiB sub-range starting 1 MiB into a 4 MiB object — many MTUs in, far
    // past the transfer-credit window, crossing the 251-byte pattern modulus.
    match run_range_case(18730, "bytes=1048576-3145727") {
        Outcome::Complete { start, len } => {
            assert_eq!(start, 1024 * 1024);
            assert_eq!(len, 2 * 1024 * 1024);
        }
        other => panic!("expected a complete range write, got {other:?}"),
    }
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn range_unsatisfiable() {
    // Start at the object's end → 416, Content-Range bytes */OBJECT, no write.
    match run_range_case(18731, "bytes=4194304-5000000") {
        Outcome::Unsatisfiable(cr) => assert_eq!(cr, format!("bytes */{OBJECT}")),
        other => panic!("expected unsatisfiable, got {other:?}"),
    }
}
