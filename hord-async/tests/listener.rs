//! `HordListener` end-to-end over the host's Soft-RoCE device (`rxe0`, see
//! CLAUDE.md), so these are `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-async --test listener -- --ignored --nocapture
//! ```
//!
//! Covers the Carapace "Blocker 0" surface:
//!
//! * `keep_alive_many_requests_one_qp` — N requests over one QP, with the server's
//!   `AsyncRead` returning EOF only on the client's real half-close (not between
//!   requests), then a graceful listener shutdown that drains and returns.
//! * `poll_write_backpressures_slow_reader` — a server writing far more than the
//!   credit window blocks (`poll_write` → `Pending`) until the client reads, rather
//!   than buffering the whole object; the payload still arrives intact.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use hord_async::{AsyncHordStream, HordListener};
use hord_stream::HordConfig;

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)

/// Deterministic, position-sensitive payload byte (matches the other tests/demo).
fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

fn pattern_vec(n: usize) -> Vec<u8> {
    (0..n).map(pattern_byte).collect()
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

/// Server-side: read one `REQ <n>\n` line; `Ok(None)` on a clean EOF (the peer
/// half-closed before sending another request — the keep-alive boundary).
async fn read_req(stream: &mut AsyncHordStream) -> std::io::Result<Option<usize>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if stream.read(&mut byte).await? == 0 {
            // EOF: a clean close between requests (or a partial line we treat the
            // same way — the client never half-sends a request in this test).
            return Ok(None);
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    let s = String::from_utf8_lossy(&line);
    let n = s
        .trim()
        .strip_prefix("REQ ")
        .and_then(|x| x.parse::<usize>().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad request: {s:?}")))?;
    Ok(Some(n))
}

/// Client-side: send `REQ <n>` and read back exactly `n` bytes. Fallible (never
/// panics) so a caller can clean up before asserting.
async fn request(stream: &mut AsyncHordStream, n: usize) -> std::io::Result<Vec<u8>> {
    stream.write_all(format!("REQ {n}\n").as_bytes()).await?;
    stream.flush().await?;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Spawn a `HordListener` on its own thread (own runtime) running `serve_fn`.
/// Returns the shutdown trigger and the thread handle (join it after firing the
/// trigger to confirm `serve` drained and returned).
///
/// Binds (and `listen`s) on the *calling* thread before spawning, so a client that
/// connects as soon as this returns cannot race ahead of the `listen()` syscall —
/// rdma_cm queues incoming connect requests once the id is listening.
fn spawn_listener<F, Fut>(
    port: u16,
    serve_fn: F,
) -> (watch::Sender<bool>, std::thread::JoinHandle<()>)
where
    F: FnMut(AsyncHordStream, std::net::SocketAddr) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let (tx, rx) = watch::channel(false);
    let listener = HordListener::bind(IP, port, HordConfig::default())
        .expect("bind listener")
        // One worker is plenty for these tests and keeps them deterministic.
        .workers(1)
        .grace_timeout(Duration::from_secs(10));
    let handle = std::thread::spawn(move || {
        current_thread_rt().block_on(listener.serve(rx, serve_fn));
    });
    (tx, handle)
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn keep_alive_many_requests_one_qp() {
    const PORT: u16 = 18630;
    const N: usize = 4096; // small — no backpressure interplay, just framing
    const REQUESTS: usize = 3;

    // The server loops serving requests on one connection and reports how many it
    // served once it observes a clean EOF — proving keep-alive (it never broke the
    // loop on a spurious between-request EOF) and a clean half-close.
    let (served_tx, served_rx) = mpsc::channel::<usize>();
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, _peer| {
        let served_tx = served_tx.clone();
        async move {
            let mut served = 0usize;
            while let Some(n) = read_req(&mut stream).await.expect("read_req") {
                stream.write_all(&pattern_vec(n)).await.expect("write body");
                stream.flush().await.expect("flush body");
                served += 1;
            }
            let _ = served_tx.send(served);
        }
    });

    // Client: many requests over ONE connection, then a graceful half-close. The
    // work returns a Result instead of panicking, so the cleanup below (capture the
    // count, fire shutdown, join) ALWAYS runs — a failure can't leak the listener
    // thread. Asserts come after cleanup.
    let client: Result<(), String> = current_thread_rt().block_on(async {
        let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default())
            .map_err(|e| format!("connect: {e}"))?;
        for k in 0..REQUESTS {
            // A stall here (the server wrongly seeing EOF between requests) would
            // hang read_exact; bound it so the test fails fast instead.
            let body = tokio::time::timeout(Duration::from_secs(10), request(&mut s, N))
                .await
                .map_err(|_| format!("request {k} timed out — keep-alive likely broke"))?
                .map_err(|e| format!("request {k}: {e}"))?;
            if body.len() != N || !body.iter().enumerate().all(|(i, &b)| b == pattern_byte(i)) {
                return Err(format!("request {k}: payload mismatch (len {})", body.len()));
            }
        }
        // Real half-close — the only thing that should surface as EOF on the server.
        s.shutdown().await.map_err(|e| format!("client shutdown: {e}"))?;
        Ok(())
    });

    // Cleanup runs unconditionally. The server reports its served count on the
    // clean EOF (after the client's half-close, or its connection dropping).
    let served = served_rx.recv_timeout(Duration::from_secs(10));
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked");

    // Now assert (client error first — it's the informative one).
    client.expect("client work failed");
    assert_eq!(
        served.expect("server never reported (no clean EOF?)"),
        REQUESTS,
        "server did not serve every keep-alive request",
    );
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn poll_write_backpressures_slow_reader() {
    const PORT: u16 = 18631;
    const PAYLOAD: usize = 16 * 1024 * 1024; // 16 MiB — dwarfs the credit window

    // The server writes PAYLOAD bytes, flips `write_done` only once the whole write
    // is acknowledged, then half-closes. With a non-reading client the write must
    // block on credit exhaustion long before completing.
    let write_done = Arc::new(AtomicBool::new(false));
    let wd = write_done.clone();
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, _peer| {
        let wd = wd.clone();
        async move {
            let Some(n) = read_req(&mut stream).await.expect("read_req") else {
                return;
            };
            stream.write_all(&pattern_vec(n)).await.expect("write body");
            stream.flush().await.expect("flush body");
            wd.store(true, SeqCst);
            let _ = stream.shutdown().await;
        }
    });

    let (blocked_observed, got) = current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default()).expect("connect");
        s.write_all(format!("REQ {PAYLOAD}\n").as_bytes())
            .await
            .expect("write request");
        s.flush().await.expect("flush request");

        // Deliberately do NOT read for a while. The server fills the window (a few
        // MiB at most) and then blocks in poll_write — so `write_done` stays false.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let blocked_observed = !write_done.load(SeqCst);

        // Now drain everything; the backpressured write resumes as we read.
        let mut got = Vec::with_capacity(PAYLOAD);
        tokio::time::timeout(Duration::from_secs(30), s.read_to_end(&mut got))
            .await
            .expect("read timed out")
            .expect("read_to_end");
        (blocked_observed, got)
    });

    // Clean up the listener before asserting, so an assertion failure can't leak
    // the server threads.
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked");

    assert!(
        blocked_observed,
        "server completed a {PAYLOAD}-byte write while the client read nothing — \
         poll_write buffered unbounded instead of back-pressuring",
    );
    assert_eq!(got.len(), PAYLOAD, "wrong payload length");
    let mismatch = got.iter().enumerate().find(|(i, &b)| b != pattern_byte(*i));
    assert!(mismatch.is_none(), "payload mismatch at {:?}", mismatch.map(|(i, _)| i));
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn serve_cancellation_winds_down_threads() {
    const PORT: u16 = 18632;

    // A current-thread runtime is enough: tokio::spawn (the serve task),
    // JoinHandle::abort, and spawn_blocking (serve's thread-join bridge) all work
    // on it, and the listener owns its own OS threads regardless.
    current_thread_rt().block_on(async {
        // An Arc the per-connection service closure owns; `serve` clones the closure
        // into each worker, so each worker holds a clone. Once every worker (and
        // serve's own copy) has wound down, the only ref left is ours — which also
        // transitively observes the acceptor stopping, since the acceptor is what
        // closes the worker channels that let the workers exit.
        let marker = std::sync::Arc::new(());
        let m = marker.clone();

        let listener = HordListener::bind(IP, PORT, HordConfig::default())
            .expect("bind")
            .workers(1)
            .grace_timeout(Duration::from_secs(2));

        // Keep the shutdown SENDER alive for the whole test, so shutdown never
        // fires: the ONLY thing that may wind the threads down is cancelling the
        // serve() future itself (the StopOnCancel drop guard — finding #1).
        let (_never_fires, never_rx) = watch::channel(false);
        let serve = tokio::spawn(listener.serve(never_rx, move |_s, _p| {
            // Capture `m` by value (so cloning the closure clones the Arc); a real
            // service would use the stream here.
            let per_conn = m.clone();
            async move {
                let _ = per_conn;
            }
        }));

        // Let the acceptor + worker threads start (and grab their closure clones).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            std::sync::Arc::strong_count(&marker) > 1,
            "acceptor/worker threads did not start",
        );

        // Cancel serve() WITHOUT firing shutdown. The drop guard must still wind the
        // acceptor + workers down, releasing every closure clone.
        serve.abort();

        // Poll for wind-down: strong_count back to 1 means the workers exited and
        // serve dropped its copy — no leaked threads. (With the bug, the acceptor
        // kept running, the worker never saw its channel close, and the count would
        // stay > 1 forever.)
        let mut wound_down = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if std::sync::Arc::strong_count(&marker) == 1 {
                wound_down = true;
                break;
            }
        }
        assert!(
            wound_down,
            "cancelling serve() leaked the acceptor/worker threads (closure refs not released)",
        );
    });
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn shutdown_already_set_returns_promptly() {
    const PORT: u16 = 18633;

    // A shutdown receiver that is ALREADY `true` when serve() starts must make the
    // acceptor wind down immediately, never accepting. `watch::channel(true)` is
    // the trigger: the receiver's seen-version already equals the (true) initial
    // value, so `Receiver::changed()` never fires — without the acceptor's own
    // initial `*shutdown.borrow()` check it would park on the CM fd forever.
    // (Regression for the "already-true shutdown ignored until a later toggle" bug.)
    let (_keep_sender_alive, rx) = watch::channel(true);
    let listener = HordListener::bind(IP, PORT, HordConfig::default())
        .expect("bind")
        .workers(1)
        .grace_timeout(Duration::from_secs(5));

    let done = std::thread::spawn(move || {
        current_thread_rt().block_on(listener.serve(rx, |_s, _p| async {}));
    });

    // serve() must return well within the grace window; if the pre-set signal were
    // ignored the acceptor would park forever and this join would hang (the test
    // harness then kills it — a clear failure either way).
    let start = std::time::Instant::now();
    while !done.is_finished() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "serve() did not return on an already-set shutdown — acceptor ignored the pre-set signal",
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    done.join().expect("serve thread panicked");
}
