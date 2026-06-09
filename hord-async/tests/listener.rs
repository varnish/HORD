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
//! * `conn_meta_surfaces_peer_addr_and_caps` — the per-connection metadata seam:
//!   the service receives a real `Some(peer)` (the lossy `0.0.0.0:0` sentinel is
//!   gone) and `conn_meta()` reports a matching peer, a stamped `established_at`,
//!   and the negotiated capabilities.

// The whole suite exercises `HordListener`, which only exists under the
// `listener` feature (on by default); compile to nothing without it so a
// `--no-default-features` test build stays clean.
#![cfg(feature = "listener")]

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{oneshot, watch};

use hord_async::{AsyncHordStream, ConnMeta, HordListener, SharedAsyncStream};
use hord_stream::{Connection, HordConfig};
use hord_zerocopy::RdmaWriteReq;

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)

mod common;
use common::{current_thread_rt, pattern_byte, pattern_vec};

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
    F: FnMut(AsyncHordStream, Option<std::net::SocketAddr>, watch::Receiver<bool>) -> Fut
        + Clone
        + Send
        + 'static,
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
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, _peer, _shutdown| {
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
fn slow_handshake_does_not_stall_the_worker() {
    // A peer that establishes the QP but never sends its HORD handshake must not
    // head-of-line block a worker's *other* connections. The handshake is an async
    // stage off the worker (CM establish + the first-message exchange park on fds),
    // so the server's task for the stalled peer parks while a well-behaved client on
    // the SAME single worker is served promptly. On the old synchronous worker the
    // stalled peer would pin the worker in its handshake busy-poll for the full
    // HANDSHAKE_TIMEOUT (~10s), so a tight wall-clock bound is the regression guard.
    // (It also exercises that the stalled connection fails only itself: its task
    // times out at HANDSHAKE_TIMEOUT and is dropped while the listener keeps serving.)
    const PORT: u16 = 18636;
    const N: usize = 4096;

    // Inline (not `spawn_listener`) for a short grace timeout: at shutdown the only
    // in-flight task is the stalled peer's, which is parked in its handshake and
    // would otherwise pay the full HANDSHAKE_TIMEOUT to drain. A 1 s grace force-tears
    // its QP down and abandons it promptly — and exercises the in-handshake-task
    // teardown (the use-after-free guard the worker takes the handle for pre-spawn).
    let (shutdown, rx) = watch::channel(false);
    let listener = HordListener::bind(IP, PORT, HordConfig::default())
        .expect("bind listener")
        .workers(1)
        .grace_timeout(Duration::from_secs(1));
    let listener_thread = std::thread::spawn(move || {
        current_thread_rt().block_on(listener.serve(rx, move |mut stream, _peer, _shutdown| async move {
            if let Some(n) = read_req(&mut stream).await.expect("read_req") {
                stream.write_all(&pattern_vec(n)).await.expect("write body");
                stream.flush().await.expect("flush body");
            }
        }));
    });

    // The stalling peer: drive the QP to ESTABLISHED with the raw connection
    // primitives, then deliberately send no handshake. Run on its own thread (the
    // connection API is blocking) and hold the connection open until released —
    // dropping it would free the server's parked handshake task.
    let (estab_tx, estab_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let staller = std::thread::spawn(move || {
        let conn = Connection::connect(IP, PORT, 4, 4, HordConfig::default().cm)
            .expect("stalling connect");
        conn.connect_finish().expect("stalling connect_finish");
        // Established but silent: never exchange the HORD handshake.
        estab_tx.send(()).expect("signal established");
        let _ = release_rx.recv();
        drop(conn);
    });
    estab_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("stalling peer never established");

    // A well-behaved client on the same single worker must complete well under
    // HANDSHAKE_TIMEOUT. `connect` (the client-side handshake) is timed too, since on
    // the old path the worker — stuck on the staller — would never `accept` it, so
    // even connecting would block ~10s.
    let started = Instant::now();
    let client: Result<(), String> = current_thread_rt().block_on(async {
        let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default())
            .map_err(|e| format!("connect: {e}"))?;
        let body = tokio::time::timeout(Duration::from_secs(5), request(&mut s, N))
            .await
            .map_err(|_| "good client request timed out — worker stalled behind the slow handshake".to_string())?
            .map_err(|e| format!("request: {e}"))?;
        if body.len() != N || !body.iter().enumerate().all(|(i, &b)| b == pattern_byte(i)) {
            return Err(format!("payload mismatch (len {})", body.len()));
        }
        s.shutdown().await.map_err(|e| format!("shutdown: {e}"))?;
        Ok(())
    });
    let elapsed = started.elapsed();

    // Cleanup runs unconditionally: release the staller, then wind the listener down.
    let _ = release_tx.send(());
    let _ = staller.join();
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked");

    client.expect("good client work failed");
    // Headline assertion: the good client was served promptly despite the stalled
    // handshake — generous 3s bound vs the ~10s HANDSHAKE_TIMEOUT a synchronous
    // worker would impose.
    assert!(
        elapsed < Duration::from_secs(3),
        "good client took {elapsed:?} — the slow handshake head-of-line blocked the worker (regression)",
    );
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn conn_meta_surfaces_peer_addr_and_caps() {
    const PORT: u16 = 18634;
    const N: usize = 4096;

    // The server reports, for its one accepted connection, the peer address it was
    // handed *and* the stream's own ConnMeta snapshot — so the test can assert the
    // lossy-sentinel fix (a real `Some(addr)`, not `0.0.0.0:0`) and that the
    // handshake metadata (established_at + negotiated caps) is populated.
    let started = SystemTime::now();
    let (meta_tx, meta_rx) = mpsc::channel::<(Option<std::net::SocketAddr>, ConnMeta)>();
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, peer, _shutdown| {
        let meta_tx = meta_tx.clone();
        async move {
            // Snapshot the metadata first — it is fixed at handshake completion.
            let _ = meta_tx.send((peer, stream.conn_meta()));
            if let Some(n) = read_req(&mut stream).await.expect("read_req") {
                stream.write_all(&pattern_vec(n)).await.expect("write body");
                stream.flush().await.expect("flush body");
            }
        }
    });

    let client: Result<(), String> = current_thread_rt().block_on(async {
        let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default())
            .map_err(|e| format!("connect: {e}"))?;
        let body = tokio::time::timeout(Duration::from_secs(10), request(&mut s, N))
            .await
            .map_err(|_| "request timed out".to_string())?
            .map_err(|e| format!("request: {e}"))?;
        if body.len() != N {
            return Err(format!("payload len {}", body.len()));
        }
        // The client end exposes the same metadata; the negotiated cap must agree.
        if !s.conn_meta().zero_copy_negotiated {
            return Err("client: zero-copy did not negotiate".to_string());
        }
        s.shutdown().await.map_err(|e| format!("shutdown: {e}"))?;
        Ok(())
    });

    let reported = meta_rx.recv_timeout(Duration::from_secs(10));
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked");

    client.expect("client work failed");
    let (peer, meta) = reported.expect("server never reported metadata");

    // Lossy-sentinel fix: a loopback peer resolves to a real address, surfaced as
    // `Some` — never folded into the old `0.0.0.0:0` placeholder.
    let peer = peer.expect("peer should resolve on a loopback RoCEv2 connection");
    assert!(!peer.ip().is_unspecified(), "peer must not be the 0.0.0.0 sentinel");
    assert_eq!(meta.peer_addr, Some(peer), "conn_meta.peer_addr must match the dispatched peer");

    // Negotiated caps: the default config advertises both on both ends.
    assert!(meta.zero_copy_negotiated, "zero-copy should negotiate (default config)");
    assert!(meta.split_mode_negotiated, "split mode should negotiate (default config)");

    // `established_at` is stamped at handshake completion. `SystemTime` is NOT
    // monotonic (an NTP step can move it backward mid-run), so assert a generous
    // window around the test rather than strict ordering — the regression we care
    // about is that `apply_peer` stamped a real, recent time and did not leave the
    // `UNIX_EPOCH` placeholder, not sub-second wall-clock ordering.
    const SKEW: Duration = Duration::from_secs(300);
    assert!(
        meta.established_at > SystemTime::UNIX_EPOCH,
        "established_at was not stamped (still the placeholder)",
    );
    assert!(
        meta.established_at + SKEW >= started,
        "established_at is implausibly far before the test start",
    );
    assert!(
        meta.established_at <= SystemTime::now() + SKEW,
        "established_at is implausibly far in the future",
    );
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn poll_write_backpressures_slow_reader() {
    const PORT: u16 = 18631;
    const PAYLOAD: usize = 16 * 1024 * 1024; // 16 MiB — dwarfs the credit window
    // A prefix far below PAYLOAD: big enough to prove the write genuinely started
    // and bytes are flowing, small enough that draining it can't let the full write
    // complete — the server re-blocks on credits well short of PAYLOAD.
    const PREFIX: usize = 1024 * 1024; // 1 MiB

    // The server writes PAYLOAD bytes, flips `write_done` only once the whole write
    // is acknowledged, then half-closes. With a barely-reading client the write must
    // block on credit exhaustion long before completing.
    let write_done = Arc::new(AtomicBool::new(false));
    let wd = write_done.clone();
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, _peer, _shutdown| {
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

        // Read a bounded PREFIX first. This proves the write GENUINELY STARTED and
        // bytes are flowing: the old "sleep, then check !write_done" couldn't tell a
        // back-pressured mid-flight stall from a write that never began — both leave
        // write_done false. A timeout here means the write produced no data at all,
        // which fails cleanly (its own message) instead of hanging.
        let mut got = vec![0u8; PREFIX];
        tokio::time::timeout(Duration::from_secs(10), s.read_exact(&mut got))
            .await
            .expect("server sent no data — the write may never have started")
            .expect("read prefix");

        // Now STOP reading. With the prefix proven flowing, the server refills the
        // credit window and blocks in poll_write; only PREFIX of PAYLOAD bytes have
        // been consumed, so `write_done` must still be false. The `got.len() < PAYLOAD`
        // conjunct states the "mid-flight" half explicitly, next to the "started" half
        // (the read_exact above) — together they pin a genuine stall, not a timer.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let blocked_observed = !write_done.load(SeqCst) && got.len() < PAYLOAD;

        // Now drain the rest; the backpressured write resumes as we read.
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
        "server completed the {PAYLOAD}-byte write while the client had read only a \
         {PREFIX}-byte prefix — poll_write buffered unbounded instead of back-pressuring",
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
        let serve = tokio::spawn(listener.serve(never_rx, move |_s, _p, _shutdown| {
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
        current_thread_rt().block_on(listener.serve(rx, |_s, _p, _shutdown| async {}));
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

/// Read a single `\n`-terminated line (no trailing newline); empty string on EOF.
async fn read_line<R: AsyncRead + Unpin>(s: &mut R) -> std::io::Result<String> {
    let mut out = Vec::new();
    let mut b = [0u8; 1];
    loop {
        match s.read(&mut b).await? {
            0 => break,
            _ if b[0] == b'\n' => break,
            _ => out.push(b[0]),
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Regression for the 🔴 use-after-free / torn delivery when a connection task is
/// aborted *mid-`RDMA_WRITE`* at the grace deadline (TODO.md, "recall pass").
///
/// Setup: the server protocol-splits (write-with-immediate, §7.7) into a client
/// buffer, but the client never drains its data plane, so after its pre-posted
/// recv WRs are consumed the server's writes RNR-stall and the connection task
/// parks inside `poll_rdma_write` with write WRs **outstanding** against a source
/// `RegisteredBuffer` the task owns. We then fire the listener's graceful
/// shutdown. The drain cannot complete (the task is wedged), so it hits the grace
/// timeout and the worker aborts the task — which drops the source buffer.
///
/// Before the fix that drop deregistered the source MR and freed its storage
/// while the QP still had the write posted (NIC DMA-into-freed-memory). The fix
/// makes the worker force every in-flight connection's QP down *before* the abort,
/// so the NIC is quiescent first. This test exercises that path end to end: it
/// asserts the server genuinely reached the wedged write (so the abort path ran),
/// that `serve()` returns bounded by the grace window (no hang, no panic/segfault
/// from a torn teardown), and that it did pay ~the grace timeout (proving a task
/// was stuck at the deadline, i.e. the abort — not a clean drain — happened).
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn shutdown_mid_backpressured_rdma_write_is_safe() {
    const PORT: u16 = 18634;
    const CHUNK: usize = 64 * 1024; // per-transfer payload; size is immaterial
    const GRACE: Duration = Duration::from_secs(2);
    // Generous slack so a slow CI box doesn't flake; the point is "bounded".
    const SLACK: Duration = Duration::from_secs(8);

    // Server: split-write in a loop until it wedges (credit window exhausted,
    // peer not draining). It signals once it is actively writing, then loops —
    // the loop parks inside `rdma_write_with_imm` and never returns, so the task
    // is in-flight at the grace deadline.
    let (writing_tx, writing_rx) = mpsc::channel::<()>();
    let (shutdown, listener_thread) = {
        let (tx, rx) = watch::channel(false);
        let listener = HordListener::bind(IP, PORT, HordConfig::default())
            .expect("bind listener")
            .workers(1)
            .grace_timeout(GRACE);
        let writing_tx = writing_tx.clone();
        let handle = std::thread::spawn(move || {
            current_thread_rt().block_on(listener.serve(rx, move |stream, _peer, _shutdown| {
                let writing_tx = writing_tx.clone();
                async move {
                    let mut shared = SharedAsyncStream::new(stream);
                    assert!(
                        shared.split_mode_negotiated(),
                        "server: split mode not negotiated (needed to reach the backpressure park)",
                    );
                    // The client's "addr rkey" line — where to write.
                    let req = RdmaWriteReq::parse(&read_line(&mut shared).await.expect("read req"))
                        .expect("parse request");
                    let src = shared.register_source(CHUNK).expect("register source");
                    src.copy_in(0, &pattern_vec(CHUNK));
                    let _ = writing_tx.send(()); // we are about to drive writes
                    // Loop write-with-immediate. The first N (the client's recv-WR
                    // count) land; thereafter the peer has no recv WR and the writes
                    // RNR-stall, so this call parks in `poll_rdma_write` forever — the
                    // task is wedged with a write outstanding against `src`.
                    let mut id = 0u32;
                    loop {
                        if shared
                            .rdma_write_with_imm(&src, 0, req.addr, req.rkey, CHUNK, id)
                            .await
                            .is_err()
                        {
                            break; // QP torn down (force_teardown) — clean exit
                        }
                        id = id.wrapping_add(1);
                    }
                }
            }));
        });
        (tx, handle)
    };

    // Client: register a destination buffer, send its coordinates, then hold the
    // connection open WITHOUT ever draining — so the server's data plane stalls.
    // `release_rx` keeps the !Send stream (and its runtime) alive until the test
    // has finished tearing the server down.
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let client_thread = std::thread::spawn(move || {
        current_thread_rt().block_on(async move {
            let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default()).expect("connect");
            assert!(s.split_mode_negotiated(), "client: split mode not negotiated");
            let buf = s.register_remote_writable(CHUNK).expect("register dest");
            let req = RdmaWriteReq {
                addr: buf.as_mut_ptr() as u64,
                rkey: buf.rkey(),
                len: buf.len() as u64,
                id: None,
            };
            s.write_all(format!("{}\n", req.header_value()).as_bytes())
                .await
                .expect("write request");
            s.flush().await.expect("flush request");
            // Deliberately never read / never drain the data plane. Just stay alive.
            let _ = release_rx.await;
            drop(buf);
            drop(s);
        });
    });

    // Wait until the server is actively writing, then give it a moment to fill the
    // credit window and wedge before we shut down — so the abort genuinely lands
    // on a task parked mid-write.
    writing_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("server never reached the write phase");
    std::thread::sleep(Duration::from_secs(1));

    // Fire shutdown and time how long the drain+abort takes to return.
    let t0 = Instant::now();
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked (torn teardown?)");
    let elapsed = t0.elapsed();

    // Release the client and join it (no assertion needed — it must not have
    // crashed when the server tore its QP out from under the in-flight write).
    let _ = release_tx.send(());
    client_thread.join().expect("client thread panicked");

    // The wedged task could only end via the grace-timeout abort, so serve() must
    // have paid ~GRACE — proving the abort path (not a clean drain) executed —
    // and must still be bounded (no hang from a teardown that blocked).
    assert!(
        elapsed >= GRACE.saturating_sub(Duration::from_millis(500)),
        "serve() returned in {elapsed:?}, well under the {GRACE:?} grace — the task wasn't \
         wedged mid-write, so this didn't exercise the abort path",
    );
    assert!(
        elapsed < GRACE + SLACK,
        "serve() took {elapsed:?} (> {GRACE:?} + {SLACK:?}) — the mid-write teardown hung",
    );
}

/// Regression for the "idle keep-alive connections pay the full `grace_timeout` at
/// shutdown" footgun (TODO.md, Blocker-0 deferred). `HordListener` now hands every
/// connection a clone of the shutdown signal as the serve closure's *third*
/// argument; a connection that watches it can wind itself down promptly instead of
/// parking — idle, mid-keep-alive — until the grace timeout elapses.
///
/// Setup: the client makes one request, then holds the connection open and silent
/// (the idle keep-alive state). The server's serve closure serves requests in a
/// loop but `select!`s that loop against the handed-in shutdown receiver, so when
/// shutdown fires it breaks the loop at once rather than blocking in the next-read.
/// We fire shutdown while the server is parked idle and assert `serve()` returns far
/// inside the (deliberately large, 10 s via `spawn_listener`) grace window — proving
/// the per-connection signal drove the drain. Without it the server would sit in the
/// next-request read and `serve()` would only return when the grace timeout fired.
#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn idle_keep_alive_drains_promptly_on_shutdown() {
    const PORT: u16 = 18635;
    const N: usize = 4096;
    // `spawn_listener` uses a 10 s grace; a prompt drain returns in milliseconds, so
    // this bound cleanly separates "wound down via the signal" from "waited out grace".
    const PROMPT: Duration = Duration::from_secs(4);

    // Server: keep-alive serve loop, but `select!`ed against the shutdown signal the
    // listener hands in as the third arg — so an idle connection winds down promptly.
    let (shutdown, listener_thread) = spawn_listener(PORT, move |mut stream, _peer, mut sd: watch::Receiver<bool>| async move {
        loop {
            tokio::select! {
                req = read_req(&mut stream) => match req {
                    Ok(Some(n)) => {
                        if stream.write_all(&pattern_vec(n)).await.is_err() { break; }
                        if stream.flush().await.is_err() { break; }
                    }
                    _ => break, // clean EOF or error
                },
                // Flip to `true` (or dropped sender, `Err`) → wind this connection
                // down at once instead of looping back into the next-request read.
                res = sd.changed() => {
                    if res.is_err() || *sd.borrow() { break; }
                }
            }
        }
    });

    // Client: one request, then sit idle (open, silent) until released — the
    // keep-alive idle state the server parks in. `release_rx` keeps the !Send stream
    // alive until the test has torn the server down.
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let client_thread = std::thread::spawn(move || {
        current_thread_rt().block_on(async move {
            let mut s = AsyncHordStream::connect(IP, PORT, &HordConfig::default()).expect("connect");
            let body = request(&mut s, N).await.expect("request");
            assert_eq!(body.len(), N, "wrong payload length");
            let _ = ready_tx.send(()); // one request done; now idle on keep-alive
            let _ = release_rx.await; // hold the connection open and silent
            drop(s);
        });
    });

    // Wait until the one request has completed and the connection is idle, then give
    // the server a beat to loop back and park awaiting the (never-coming) next request.
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("client never completed its first request");
    std::thread::sleep(Duration::from_millis(200));

    // Fire shutdown with the connection parked idle, and time serve()'s return.
    let t0 = Instant::now();
    shutdown.send(true).expect("send shutdown");
    listener_thread.join().expect("listener thread panicked");
    let elapsed = t0.elapsed();

    // Release and join the client (it must not have crashed).
    let _ = release_tx.send(());
    client_thread.join().expect("client thread panicked");

    assert!(
        elapsed < PROMPT,
        "serve() took {elapsed:?} to drain an IDLE keep-alive connection — the \
         per-connection shutdown signal was ignored, so it waited out the grace \
         timeout (the footgun this fix closes)",
    );
}
