//! Async zero-copy write (spec §7) over the host's Soft-RoCE device (`rxe0`, see
//! CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-async -- --ignored --nocapture zero_copy
//! ```
//!
//! Exercises the async one-sided write primitive without `hyper`: the server
//! drives [`SharedAsyncStream::rdma_write`] (post → park on the completion fd →
//! reap) into a buffer the client registered via
//! [`AsyncHordStream::register_remote_writable`], using the same `borrow-per-poll`
//! shared handle hyper does. The client then reads the payload straight out of
//! its buffer. Control framing is one newline-terminated header value each way.

use std::sync::mpsc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use hord_async::{AsyncHordStream, SharedAsyncStream};
use hord_stream::{HordConfig, HordStream, Listener, Mr, RegisteredBuffer, WriteSegment};
use hord_zerocopy::{RdmaWriteReq, RdmaWriteStatus, SourcePool};

static IP: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| std::env::var("HORD_TEST_IP").unwrap_or_else(|_| "192.0.2.1".to_string())); // rxe device IP; override via $HORD_TEST_IP (see CLAUDE.md)
const PORT: u16 = 18820; // distinct from the demo (4791) and other loopback tests
const PORT_POOLED: u16 = 18821; // serve_rdma_write_pooled_reports_bytes_written
const PORT_TOO_LARGE: u16 = 18822; // serve_rdma_write_too_large_writes_nothing
const PORT_GATHER: u16 = 18823; // gather_write_lands_fragments_contiguously
const OBJECT: usize = 4 * 1024 * 1024; // 4 MiB — many MTUs, dwarfs the credit window

mod common;
use common::{current_thread_rt, pattern_byte};

async fn read_line<R: AsyncRead + Unpin>(s: &mut R) -> String {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        match s.read(&mut b).await.expect("read line") {
            0 => break,
            _ if b[0] == b'\n' => break,
            _ => out.push(b[0]),
        }
    }
    String::from_utf8(out).expect("line is utf-8")
}

async fn write_line<W: AsyncWrite + Unpin>(s: &mut W, line: &str) {
    s.write_all(line.as_bytes()).await.expect("write line");
    s.write_all(b"\n").await.expect("write newline");
    s.flush().await.expect("flush line");
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

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn zero_copy_async_round_trip() {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: accept on this thread, then run the !Send shared stream on a
    // current-thread runtime. It reads the request, RDMA-writes the object into
    // the client's buffer, and relays the status.
    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            assert!(stream.zero_copy_negotiated(), "server: zero-copy not negotiated");
            let mut shared = SharedAsyncStream::new(stream);

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            let src = shared.register_source(OBJECT).expect("register source");
            fill(&src, OBJECT);
            shared
                .rdma_write(&src, 0, req.addr, req.rkey, OBJECT)
                .await
                .expect("rdma_write");
            let status = RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 };
            write_line(&mut shared, &status.header_value()).await;
            // Give the client time to read the status before we tear down.
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(&IP, PORT, &config).expect("connect");
        assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");
        let buf = s.register_remote_writable(OBJECT).expect("register dest");
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: buf.len() as u64,
            id: None,
        };
        write_line(&mut s, &req.header_value()).await;

        let status = tokio::time::timeout(Duration::from_secs(30), read_line(&mut s))
            .await
            .expect("status read timed out");
        let status = RdmaWriteStatus::parse(&status).expect("parse status");
        assert_eq!(
            status,
            RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 },
            "expected a complete zero-copy write"
        );

        // The payload is already in our buffer — verify it in place.
        let mut tmp = vec![0u8; 256 * 1024];
        let mut off = 0;
        while off < OBJECT {
            let take = tmp.len().min(OBJECT - off);
            buf.copy_out(off, &mut tmp[..take]);
            for (i, &got) in tmp[..take].iter().enumerate() {
                assert_eq!(got, pattern_byte(off + i), "payload mismatch at byte {}", off + i);
            }
            off += take;
        }
    });

    server.join().expect("server thread panicked");
}

/// The policy-aware async serve entry point reports the DMA'd byte count. The
/// server delivers the object via [`SharedAsyncStream::serve_rdma_write_pooled`] —
/// one call that runs the §7.3 policy, leases a pooled source, fills it, writes,
/// and returns the status — and the returned [`RdmaWriteStatus::Complete`] carries
/// `bytes_written == OBJECT`: the count a host's transaction log records, since the
/// body bypassed the byte stream. The client then verifies the payload landed. This
/// is the regression lock for surfacing bytes+outcome (Milestone 2).
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn serve_rdma_write_pooled_reports_bytes_written() {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, PORT_POOLED).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            assert!(stream.zero_copy_negotiated(), "server: zero-copy not negotiated");
            let mut shared = SharedAsyncStream::new(stream);
            let pool = SourcePool::new(2, OBJECT);

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            // One library call: decide -> lease -> fill -> write -> status.
            let status = shared
                .serve_rdma_write_pooled(&pool, &req, OBJECT as u64, |src| fill(src, OBJECT))
                .await
                .expect("serve_rdma_write_pooled");
            assert_eq!(
                status,
                RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 },
                "serve must report the DMA'd byte count"
            );
            write_line(&mut shared, &status.header_value()).await;
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(&IP, PORT_POOLED, &config).expect("connect");
        assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");
        let buf = s.register_remote_writable(OBJECT).expect("register dest");
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: buf.len() as u64,
            id: None,
        };
        write_line(&mut s, &req.header_value()).await;

        let status = tokio::time::timeout(Duration::from_secs(30), read_line(&mut s))
            .await
            .expect("status read timed out");
        assert_eq!(
            RdmaWriteStatus::parse(&status),
            Some(RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 }),
            "expected a complete zero-copy write reporting OBJECT bytes"
        );

        // The payload is already in our buffer — verify it in place.
        let mut tmp = vec![0u8; 256 * 1024];
        let mut off = 0;
        while off < OBJECT {
            let take = tmp.len().min(OBJECT - off);
            buf.copy_out(off, &mut tmp[..take]);
            for (i, &got) in tmp[..take].iter().enumerate() {
                assert_eq!(got, pattern_byte(off + i), "payload mismatch at byte {}", off + i);
            }
            off += take;
        }
    });

    server.join().expect("server thread panicked");
}

/// [`SharedAsyncStream::serve_rdma_write`] returns [`RdmaWriteStatus::TooLarge`]
/// without writing when the object exceeds the client's advertised buffer (§7.4):
/// `fill` must not run, nothing is DMA'd, and the client's pre-seeded buffer is left
/// untouched. Covers the non-pooled serve method and the policy short-circuit.
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn serve_rdma_write_too_large_writes_nothing() {
    const SMALL: usize = 64 * 1024; // client buffer — smaller than OBJECT
    const SENTINEL: u8 = 0xAB;
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, PORT_TOO_LARGE).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            assert!(stream.zero_copy_negotiated(), "server: zero-copy not negotiated");
            let mut shared = SharedAsyncStream::new(stream);

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            // OBJECT (4 MiB) > the client's SMALL buffer -> TooLarge, no write.
            let status = shared
                .serve_rdma_write(&req, OBJECT as u64, |_| panic!("fill must not run for TooLarge"))
                .await
                .expect("serve_rdma_write");
            assert_eq!(status, RdmaWriteStatus::TooLarge { object_size: OBJECT as u64 });
            write_line(&mut shared, &status.header_value()).await;
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(&IP, PORT_TOO_LARGE, &config).expect("connect");
        assert!(s.zero_copy_negotiated(), "client: zero-copy not negotiated");
        let buf = s.register_remote_writable(SMALL).expect("register dest");
        // Seed the buffer so we can prove the server wrote nothing into it.
        buf.copy_in(0, &vec![SENTINEL; SMALL]);
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: buf.len() as u64,
            id: None,
        };
        write_line(&mut s, &req.header_value()).await;

        let status = tokio::time::timeout(Duration::from_secs(30), read_line(&mut s))
            .await
            .expect("status read timed out");
        assert_eq!(
            RdmaWriteStatus::parse(&status),
            Some(RdmaWriteStatus::TooLarge { object_size: OBJECT as u64 }),
            "expected too_large for an object larger than the client buffer"
        );

        // Nothing was written: the buffer still holds the sentinel.
        let mut got = vec![0u8; SMALL];
        buf.copy_out(0, &mut got);
        assert!(got.iter().all(|&b| b == SENTINEL), "TooLarge must not write into the buffer");
    });

    server.join().expect("server thread panicked");
}

/// True zero-copy from a *fragmented* caller-owned source (spec §7, Milestone 3):
/// the server registers N separate allocations with
/// [`SharedAsyncStream::register_external`] (mimicking an MSE4 object's
/// non-contiguous `allocs` list — no copy into a HORD buffer), then delivers them
/// as one logical [`SharedAsyncStream::rdma_write_gather`]. With N > the QP's
/// `max_send_sge` the gather spans multiple WRs (exercising the SGE-packing), and
/// the client verifies the fragments landed **contiguously, in order**, in its
/// single destination buffer.
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn gather_write_lands_fragments_contiguously() {
    const SEG: usize = 256 * 1024; // per-allocation size
    const N: usize = 24; // > MAX_WRITE_SGE (16) -> multiple gather WRs
    const TOTAL: usize = SEG * N; // 6 MiB contiguous object
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, PORT_GATHER).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            let mut shared = SharedAsyncStream::new(stream);
            // The point of the test is multi-WR packing; assert (don't just comment)
            // that N exceeds the QP's per-WR SGE cap so the gather really spans
            // several WRs — otherwise this would silently degrade to a 1-WR test if
            // MAX_WRITE_SGE ever rose.
            assert!(
                shared.max_send_sge() < N,
                "N ({N}) must exceed max_send_sge ({}) to force multi-WR packing",
                shared.max_send_sge(),
            );

            // Fragmented caller-owned source: N separate allocations, each holding
            // its slice of the global pattern, each its own externally-registered
            // MR. Keep `backing` (and `mrs`) alive until after the gather drains.
            let mut backing: Vec<Vec<u8>> = Vec::with_capacity(N);
            let mut mrs: Vec<Mr> = Vec::with_capacity(N);
            for seg in 0..N {
                let mut v = vec![0u8; SEG];
                for (j, b) in v.iter_mut().enumerate() {
                    *b = pattern_byte(seg * SEG + j);
                }
                // SAFETY: `v` stays live in `backing` until after the gather write
                // completes (it drains every WR before resolving).
                let mr = unsafe { shared.register_external(v.as_mut_ptr(), v.len()) }.expect("reg ext");
                backing.push(v);
                mrs.push(mr);
            }

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            let segments: Vec<WriteSegment> =
                mrs.iter().map(|mr| WriteSegment::from_mr(mr, 0, SEG)).collect();
            shared
                .rdma_write_gather(&segments, req.addr, req.rkey)
                .await
                .expect("rdma_write_gather");
            let status = RdmaWriteStatus::Complete { bytes_written: TOTAL as u64 };
            write_line(&mut shared, &status.header_value()).await;
            drop(segments);
            drop(mrs);
            drop(backing);
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(&IP, PORT_GATHER, &config).expect("connect");
        let buf = s.register_remote_writable(TOTAL).expect("register dest");
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: buf.len() as u64,
            id: None,
        };
        write_line(&mut s, &req.header_value()).await;

        let status = tokio::time::timeout(Duration::from_secs(30), read_line(&mut s))
            .await
            .expect("status read timed out");
        assert_eq!(
            RdmaWriteStatus::parse(&status),
            Some(RdmaWriteStatus::Complete { bytes_written: TOTAL as u64 }),
        );

        // The fragmented source must have landed contiguously, in order.
        let mut tmp = vec![0u8; 256 * 1024];
        let mut off = 0;
        while off < TOTAL {
            let take = tmp.len().min(TOTAL - off);
            buf.copy_out(off, &mut tmp[..take]);
            for (i, &got) in tmp[..take].iter().enumerate() {
                assert_eq!(got, pattern_byte(off + i), "payload mismatch at byte {}", off + i);
            }
            off += take;
        }
    });

    server.join().expect("server thread panicked");
}

/// An **over-cap** async gather — more WRs than the send pool holds — is delivered
/// in several drained batches by the async driver (deferral 1: the async
/// counterpart of `HordStream::rdma_write_gather_all`'s batching), rather than
/// rejected with `InvalidInput`. A small `send_pool` (2) forces batching for the
/// 24-fragment source; the client verifies every fragment landed contiguously, so
/// the per-batch post→drain→advance loop is exercised end to end.
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn over_cap_async_gather_batches_and_lands_contiguously() {
    const PORT_BATCH: u16 = 18824; // distinct from the plain gather test (18823)
    const SEG: usize = 256 * 1024; // per-fragment size
    // 40 fragments => >=3 WRs for any max_send_sge in 1..=16, comfortably above the
    // send_pool of 2 below, so the gather always spans several drained batches.
    const N: usize = 40;
    const TOTAL: usize = SEG * N; // 10 MiB contiguous object
    // send_pool below the gather's WR count -> forces batching.
    let config = HordConfig { send_pool_size: 2, ..HordConfig::default() };
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(&IP, PORT_BATCH).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            let mut shared = SharedAsyncStream::new(stream);
            let n_wrs = N.div_ceil(shared.max_send_sge());
            assert!(
                n_wrs > srv_config.send_pool_size,
                "gather of {n_wrs} WRs must exceed send_pool {} to exercise batching",
                srv_config.send_pool_size,
            );

            // One caller-owned allocation sliced into N separate segments (each its
            // own SGE) — the fragmentation that forces multi-WR, multi-batch packing.
            // Kept alive until the gather drains every WR of every batch.
            let mut whole = vec![0u8; TOTAL];
            for (i, b) in whole.iter_mut().enumerate() {
                *b = pattern_byte(i);
            }
            // SAFETY: `whole` stays live until after the gather completes below.
            let mr = unsafe { shared.register_external(whole.as_mut_ptr(), whole.len()) }
                .expect("reg ext");
            let segments: Vec<WriteSegment> =
                (0..N).map(|k| WriteSegment::from_mr(&mr, k * SEG, SEG)).collect();

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            shared
                .rdma_write_gather(&segments, req.addr, req.rkey)
                .await
                .expect("over-cap rdma_write_gather");
            let status = RdmaWriteStatus::Complete { bytes_written: TOTAL as u64 };
            write_line(&mut shared, &status.header_value()).await;
            drop(segments);
            drop(mr);
            drop(whole);
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(&IP, PORT_BATCH, &config).expect("connect");
        let buf = s.register_remote_writable(TOTAL).expect("register dest");
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: buf.len() as u64,
            id: None,
        };
        write_line(&mut s, &req.header_value()).await;

        let status = tokio::time::timeout(Duration::from_secs(30), read_line(&mut s))
            .await
            .expect("status read timed out");
        assert_eq!(
            RdmaWriteStatus::parse(&status),
            Some(RdmaWriteStatus::Complete { bytes_written: TOTAL as u64 }),
        );

        // Every fragment must have landed contiguously, in order.
        let mut tmp = vec![0u8; 256 * 1024];
        let mut off = 0;
        while off < TOTAL {
            let take = tmp.len().min(TOTAL - off);
            buf.copy_out(off, &mut tmp[..take]);
            for (i, &got) in tmp[..take].iter().enumerate() {
                assert_eq!(got, pattern_byte(off + i), "payload mismatch at byte {}", off + i);
            }
            off += take;
        }
    });

    server.join().expect("server thread panicked");
}
