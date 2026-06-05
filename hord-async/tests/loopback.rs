//! Async stream loopback over the host's Soft-RoCE device (`rxe0`, see
//! CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-async -- --ignored --nocapture
//! ```
//!
//! Exercises the whole async path end to end: an async client writes a request
//! and `read_to_end`s a multi-megabyte response from an async server, which runs
//! on its own thread (the stream is `!Send`). It covers `AsyncRead`/`AsyncWrite`,
//! the credit flow control under the reactor, half-close → EOF, and a
//! `tokio::time::timeout` guard around the read (review item #11).

use std::sync::mpsc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use hord_async::AsyncHordStream;
use hord_stream::{HordConfig, HordStream, Listener};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18620; // distinct from the demo (4791) and full_duplex (18519)
const BODY: usize = 4 * 1024 * 1024; // 4 MiB — many messages, dwarfs the window

/// Deterministic, position-sensitive payload byte (matches the demo's pattern).
fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn async_request_response() {
    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: accept on this thread (yielding the Send `Connection`), then build
    // and run the !Send async stream on the same thread's current-thread runtime.
    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let (conn, peer) = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let mut s = AsyncHordStream::from_accepted(conn, peer, &srv_config).expect("accept");
            // Read the (small) request, then stream the response body and close.
            let mut req = [0u8; 256];
            let n = s.read(&mut req).await.expect("read request");
            assert!(n > 0, "server read an empty request");
            let mut body = vec![0u8; BODY];
            for (i, b) in body.iter_mut().enumerate() {
                *b = pattern_byte(i);
            }
            s.write_all(&body).await.expect("write body");
            // shutdown() flushes (waits for every send to be acked) then
            // disconnects, so the client sees a clean EOF.
            s.shutdown().await.expect("shutdown");
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(IP, PORT, &config).expect("connect");
        s.write_all(b"GET /async HTTP/1.1\r\nConnection: close\r\n\r\n")
            .await
            .expect("write request");
        s.flush().await.expect("flush");

        // Read the whole body to EOF, bounded by a deadline (a stalled peer must
        // error here rather than hang — the #11 timeout story).
        let mut got = Vec::with_capacity(BODY);
        let n = tokio::time::timeout(Duration::from_secs(30), s.read_to_end(&mut got))
            .await
            .expect("read timed out — likely a credit/EOF deadlock")
            .expect("read_to_end");

        assert_eq!(n, BODY, "wrong body length");
        let mismatch = got.iter().enumerate().find(|(i, &b)| b != pattern_byte(*i));
        assert!(mismatch.is_none(), "payload mismatch at {:?}", mismatch.map(|(i, _)| i));
    });

    server.join().expect("server thread panicked");
}
