//! Two-task full-duplex over a split async HORD stream, on the host's Soft-RoCE
//! device (`rxe0`, see CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-async -- --ignored --nocapture async_full_duplex_split
//! ```
//!
//! This is the async analogue of `hord-stream`'s `full_duplex_bulk`, and the
//! regression test for the multi-waiter pump ([`AsyncHordStream::into_split`]).
//! Each endpoint splits its stream and drives the **read half and write half from
//! two independent tasks at once**, pushing 16 MiB each way concurrently — far
//! beyond the credit window, so both directions repeatedly exhaust and refill
//! credits while two tasks share the one completion fd. That is exactly the case
//! the single-task driver (and `tokio::io::split`) deadlocks on: without the pump
//! waking both halves, one task would park forever and the watchdog timeout would
//! fire.

use std::io;
use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::LocalSet;

use hord_async::{AsyncHordStream, SplitParts};
use hord_stream::{HordConfig, HordStream, Listener};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18920; // distinct from the demo (4791) and other loopback tests
const BODY: usize = 16 * 1024 * 1024; // 16 MiB each way — dwarfs the credit window
const WATCHDOG: Duration = Duration::from_secs(30); // a stall (deadlock) fails fast

mod common;
use common::{current_thread_rt, pattern};

/// Drive one already-handshaked endpoint: split it, write `send` from one task,
/// `read_exact(BODY)` into another, and verify the received bytes equal `expect`.
/// `teardown` holds both endpoints' connections open until both have exchanged
/// everything, so neither disconnects mid-flight (mirrors `full_duplex_bulk`).
async fn run_endpoint(stream: AsyncHordStream, send_seed: u8, recv_seed: u8, teardown: Arc<Barrier>) {
    let SplitParts { read, write, data } = stream.into_split();

    // Writer task: push our whole body, then flush to a full delivery barrier.
    let send = pattern(BODY, send_seed);
    let writer = tokio::task::spawn_local(async move {
        let mut w = write;
        w.write_all(&send).await?;
        w.flush().await
    });

    // Reader task: pull exactly the peer's body. Runs concurrently with the writer
    // (and with the peer's mirror image), so both directions are live at once.
    let reader = tokio::task::spawn_local(async move {
        let mut r = read;
        let mut got = vec![0u8; BODY];
        r.read_exact(&mut got).await?;
        io::Result::Ok(got)
    });

    // Capture both outcomes (bounded by the watchdog) WITHOUT panicking yet.
    let read_res = tokio::time::timeout(WATCHDOG, reader).await;
    let write_res = tokio::time::timeout(WATCHDOG, writer).await;

    // Rendezvous BEFORE asserting, so a failure on one endpoint can't strand the
    // peer at the barrier — both sides reach it within WATCHDOG no matter the
    // outcome. `data` (still held) keeps the pump and the connection alive across
    // the barrier; both sides are quiescent here (all bytes exchanged, or both
    // timed out).
    teardown.wait();
    drop(data);

    // Now surface any failure and verify the payload.
    let got = read_res
        .expect("reader timed out — likely a multi-waiter deadlock")
        .expect("reader task panicked")
        .expect("read_exact");
    write_res
        .expect("writer timed out — likely a multi-waiter deadlock")
        .expect("writer task panicked")
        .expect("write_all/flush");
    let expect = pattern(BODY, recv_seed);
    assert_eq!(got.len(), BODY, "wrong body length");
    let mismatch = got.iter().zip(&expect).position(|(a, b)| a != b);
    assert!(mismatch.is_none(), "payload mismatch at byte {mismatch:?}");
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn async_full_duplex_split() {
    let config = HordConfig::default();
    let teardown = Arc::new(Barrier::new(2));
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: accept on this thread (yielding the Send `Connection`), then build
    // and run the !Send split stream on the same thread's current-thread runtime.
    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        let rt = current_thread_rt();
        let local = LocalSet::new();
        rt.block_on(local.run_until(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            run_endpoint(stream, 0xA5, 0x5A, srv_teardown).await;
        }));
    });

    ready_rx.recv().expect("server ready");
    let rt = current_thread_rt();
    let local = LocalSet::new();
    rt.block_on(local.run_until(async move {
        let stream = AsyncHordStream::connect(IP, PORT, &config).expect("connect");
        run_endpoint(stream, 0x5A, 0xA5, teardown).await;
    }));

    server.join().expect("server thread panicked");
}
