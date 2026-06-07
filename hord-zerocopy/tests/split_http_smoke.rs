//! Protocol-splitting (spec §7.7) orchestration end-to-end, at the HTTP-semantics
//! layer: the server side via [`serve_rdma_write`] (which chooses
//! write-with-immediate because the request carries an `id` and split mode is
//! negotiated), the client side via [`SplitReceiver`] (which collects payload
//! completions off the CQ, keyed by transfer ID).
//!
//! HTTP framing is bypassed — the request descriptors are handed over an
//! in-process channel — so this exercises the §7.7 orchestration, not the codec
//! (which has its own unit tests). Includes a **zero-length object**, the edge
//! where the server must still emit an (empty) write-with-immediate so the data
//! plane's posted transfer credit is consumed and its poll returns.
//!
//! Needs the host's Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d:
//!
//! ```sh
//! cargo test -p hord-zerocopy -- --ignored --nocapture split_http_round_trip
//! ```

// Exercises the RDMA orchestration over Soft-RoCE (`rxe0`), so the whole test
// crate is gated on the `rdma` feature: the default device-free codec build —
// `cargo test -p hord-zerocopy` without the feature — skips it entirely.
#![cfg(feature = "rdma")]

use std::sync::{mpsc, Arc, Barrier};
use std::time::{Duration, Instant};

use hord_stream::{HordConfig, HordStream, Listener};
use hord_zerocopy::{serve_rdma_write, RdmaWriteReq, RdmaWriteStatus, SplitReceiver, ZeroCopyRequest};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18523; // distinct from the stream/core tests and the demo
const STALL: Duration = Duration::from_secs(15);

// (transfer ID, object size, advertised buffer capacity). The middle entry is a
// zero-length object whose buffer is still non-trivial — the server writes 0
// bytes but must still deliver the immediate.
const PLAN: &[(u32, u64, usize)] = &[
    (1, 1 << 20, 1 << 20),       // 1 MiB, exact-fit buffer
    (2, 0, 4096),                // empty object
    (3, 300 << 10, 512 << 10),   // 300 KiB into a 512 KiB buffer (partial fill)
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

fn object_size_for(id: u32) -> u64 {
    PLAN.iter().find(|&&(tid, ..)| tid == id).expect("known id").1
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn split_http_round_trip() {
    let config = HordConfig::default(); // split_mode + zero_copy on by default

    let teardown = Arc::new(Barrier::new(2));
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    // client -> server: the parsed request headers (addr/rkey/len/id) per transfer.
    let (req_tx, req_rx) = mpsc::channel::<Vec<RdmaWriteReq>>();

    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
        assert!(s.zero_copy_negotiated() && s.split_mode_negotiated());

        let reqs = req_rx.recv().expect("recv requests");
        for req in &reqs {
            let id = req.id.expect("every request carries a split id");
            let object_size = object_size_for(id);
            // serve_rdma_write picks write-with-immediate because id is set and
            // split mode negotiated. The fill closure is invoked only for a
            // non-empty body.
            let status = serve_rdma_write(&mut s, req, object_size, |src| {
                src.copy_in(0, &pattern(object_size as usize, id as u8));
            })
            .expect("serve_rdma_write");
            assert_eq!(
                status,
                RdmaWriteStatus::Complete {
                    bytes_written: object_size
                },
                "transfer {id} should complete via RDMA write"
            );
        }
        srv_teardown.wait();
        // s drops after the client has collected everything.
    });

    ready_rx.recv().expect("server ready");
    let mut client = HordStream::connect(IP, PORT, &config).expect("connect");
    assert!(client.zero_copy_negotiated() && client.split_mode_negotiated());

    // Control plane: register a destination per transfer and advertise it with a
    // split `id`. Hold the `ZeroCopyRequest`s (they own the buffers) across the
    // data-plane poll so the registrations outlive the writes.
    let reqs_owned: Vec<ZeroCopyRequest> = PLAN
        .iter()
        .map(|&(id, _, cap)| {
            ZeroCopyRequest::new(&client, cap)
                .expect("register dst")
                .with_id(id)
        })
        .collect();
    let descriptors: Vec<RdmaWriteReq> = reqs_owned.iter().map(|r| r.request()).collect();
    // Every descriptor should advertise its split id.
    for (r, &(id, ..)) in descriptors.iter().zip(PLAN) {
        assert_eq!(r.id, Some(id), "with_id must thread the id into the request");
    }
    req_tx.send(descriptors).expect("send requests");

    // Data plane: collect every transfer off the CQ by id — no HTTP — and verify
    // the landed payload against its id-keyed pattern.
    let mut seen = std::collections::HashSet::new();
    {
        let mut rx = SplitReceiver::new(&mut client).expect("split receiver");
        let start = Instant::now();
        while seen.len() < PLAN.len() {
            match rx.poll_completion().expect("poll_completion") {
                Some(c) => {
                    let id = c.transfer_id;
                    assert!(seen.insert(id), "transfer {id} completed twice");
                    let object_size = object_size_for(id) as usize;
                    let idx = PLAN.iter().position(|&(tid, ..)| tid == id).expect("known id");
                    let mut got = vec![0u8; object_size];
                    reqs_owned[idx].copy_out(0, &mut got);
                    assert_eq!(
                        got,
                        pattern(object_size, id as u8),
                        "transfer {id} payload mismatch"
                    );
                }
                None => panic!("connection closed before all transfers completed"),
            }
            assert!(start.elapsed() < STALL, "data-plane completions stalled");
        }
    }

    teardown.wait();
    drop(reqs_owned);
    drop(client);
    server.join().expect("server thread panicked");
}
