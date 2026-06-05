//! Evidence for review item #15: the async data path *parks* a blocked
//! connection instead of busy-polling the CQ.
//!
//! Both tests need the Soft-RoCE device (`rxe0`, see CLAUDE.md), so they are
//! `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test -p hord-async -- --ignored --nocapture idle
//! ```
//!
//! `async_idle_read_parks` blocks an async read on an idle (but alive) peer for
//! ~1s and asserts this thread burned almost no CPU — and that the read times
//! out rather than hanging (the #11 deadline story). `sync_idle_read_busy_polls`
//! is the contrast: the synchronous stream's busy-poll burns a whole core for
//! the same wait. Run together they print both numbers.

use std::io::{Read, Write};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;

use hord_async::AsyncHordStream;
use hord_stream::{HordConfig, HordStream, Listener};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)

/// CPU time consumed by the *calling thread* so far.
fn thread_cpu() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid, owned timespec; CLOCK_THREAD_CPUTIME_ID is a
    // standard clock id. clock_gettime writes only into ts.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime failed");
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn async_idle_read_parks() {
    const PORT: u16 = 18621;
    const IDLE: Duration = Duration::from_millis(1000);

    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: accept, then hold the connection open and idle (send nothing).
    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("ready");
        let (conn, peer) = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let _s = AsyncHordStream::from_accepted(conn, peer, &srv_config).expect("accept");
            tokio::time::sleep(IDLE + Duration::from_millis(500)).await;
            // _s dropped here -> disconnect.
        });
    });

    ready_rx.recv().expect("server ready");
    current_thread_rt().block_on(async move {
        let mut s = AsyncHordStream::connect(IP, PORT, &config).expect("connect");
        // Block a read on the idle peer for IDLE. With no data and no close, the
        // task parks on the CQ fd; this thread should sit in epoll_wait.
        let cpu0 = thread_cpu();
        let wall0 = Instant::now();
        let mut buf = [0u8; 64];
        let r = tokio::time::timeout(IDLE, s.read(&mut buf)).await;
        let cpu = thread_cpu().saturating_sub(cpu0);
        let wall = wall0.elapsed();

        // #11: a stalled-but-alive peer makes the read deadline out, not hang.
        assert!(r.is_err(), "expected the idle read to time out, got {r:?}");
        eprintln!("[async] idle {wall:?}: this thread consumed {cpu:?} of CPU");
        // Parking should cost a tiny fraction of the wall time; a busy-poll would
        // consume ~IDLE. Generous slack for runtime/timer bookkeeping.
        assert!(
            cpu < Duration::from_millis(150),
            "idle async read burned {cpu:?} CPU over {wall:?} — busy-polling?",
        );
    });

    server.join().expect("server thread panicked");
}

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn sync_idle_read_busy_polls() {
    const PORT: u16 = 18622;
    const HOLD: Duration = Duration::from_millis(800);

    let config = HordConfig::default();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: accept (sync), stay silent for HOLD, then send one byte. (We make
    // the read return via *data arrival*, not EOF: the synchronous path has no
    // half-close detection — see PROTOTYPE.md — so a peer disconnect would never
    // wake a blocking read. The point here is only to measure CPU during the
    // busy-polled wait that precedes the byte.)
    let srv_config = config.clone();
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("ready");
        let mut s = HordStream::accept(&listener, &srv_config).expect("accept");
        std::thread::sleep(HOLD);
        s.write_all(b"x").expect("write");
        s.flush().expect("flush");
    });

    ready_rx.recv().expect("server ready");
    let mut s = HordStream::connect(IP, PORT, &config).expect("connect");
    let cpu0 = thread_cpu();
    let wall0 = Instant::now();
    let mut buf = [0u8; 64];
    // Synchronous read busy-polls (pump(true) spins) until the byte arrives.
    let n = s.read(&mut buf).expect("read");
    let cpu = thread_cpu().saturating_sub(cpu0);
    let wall = wall0.elapsed();

    assert_eq!(n, 1, "expected the one byte the server sent after HOLD");
    eprintln!("[sync]  blocked {wall:?}: this thread consumed {cpu:?} of CPU (busy-poll)");
    // This is the #15 problem the async path fixes: the busy-poll burns CPU
    // roughly equal to the wait. Assert it's substantial to keep the contrast
    // honest (and catch an accidental regression to a sleeping sync read).
    assert!(
        cpu > Duration::from_millis(200),
        "sync blocked read used only {cpu:?} over {wall:?} — expected a busy-poll",
    );

    server.join().expect("server thread panicked");
}
