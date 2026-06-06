//! Concurrent-accept test for per-connection CM event channels
//! ([`Identifier::migrate`] / `rdma_migrate_id`, carried as a local sideway
//! patch — see `vendor/sideway/HORD-PATCH.md`).
//!
//! A *looping* acceptor accepts each connection and hands the (Send)
//! [`Connection`] to its own worker thread, which finishes the handshake while
//! the acceptor immediately accepts the next — exactly the async server's
//! pattern. Because `Listener::accept` migrates each accepted id to its own event
//! channel, a worker's `accept_finish` (which blocks for `ESTABLISHED`) waits on
//! *that* channel, never competing with the acceptor's next `accept`.
//!
//! On the previous shared-channel design this deadlocks or errors: two threads
//! call `get_cm_event` on the same channel, so an `ESTABLISHED` can be delivered
//! to the acceptor (which ignores it) and a `ConnectRequest` to a worker (which
//! rejects it). Passing this test is the evidence the migrate patch works.
//!
//! The whole exchange runs under a watchdog so a regression fails *deterministically*
//! (a timeout panic) instead of hanging the test process.
//!
//! Needs the Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d:
//!
//! ```sh
//! cargo test -p hord-core -- --ignored --nocapture concurrent_accept
//! ```

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use hord_core::{CmParams, Connection, Listener};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18522; // distinct from the write smoke tests (18520/18521)
const N: usize = 4; // concurrent connections
const WATCHDOG: Duration = Duration::from_secs(30); // generous; loopback is ~instant

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn concurrent_accept_via_migrate() {
    // Run the whole exchange on a worker thread and bound it: a migrate
    // regression manifests as a deadlock (a worker's accept_finish never sees
    // its ESTABLISHED), which without a watchdog would hang forever. recv_timeout
    // turns that into a clean test failure.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let runner = thread::spawn(move || {
        run_concurrent_accept();
        let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(WATCHDOG) {
        Ok(()) => runner.join().expect("test runner panicked"),
        // Sender dropped without signalling -> the runner panicked; join surfaces it.
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            runner.join().expect("test runner panicked");
            panic!("test runner exited without completing");
        }
        // Still running after the watchdog -> a real deadlock. Do NOT join (it
        // would block forever); fail loudly. The leaked thread dies with the test.
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "concurrent_accept timed out after {WATCHDOG:?} — likely a per-connection \
             channel (migrate) regression: a worker's accept_finish raced the acceptor"
        ),
    }
}

/// The actual exchange: a looping acceptor finishing each connection on its own
/// worker thread, with N clients connecting concurrently.
fn run_concurrent_accept() {
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let server = thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");

        let mut workers = Vec::with_capacity(N);
        for _ in 0..N {
            // Returns once a connect request arrives; the id is now on its own
            // (migrated) event channel.
            let conn = listener.accept(2, 2, CmParams::default()).expect("accept");
            workers.push(thread::spawn(move || {
                // Blocks for ESTABLISHED on this connection's *own* channel —
                // must not be disturbed by the acceptor's next accept().
                conn.accept_finish().expect("accept_finish");
                conn.shutdown();
            }));
        }
        for w in workers {
            w.join().expect("worker thread panicked");
        }
    });

    ready_rx.recv().expect("server ready");

    // Clients connect concurrently, so connect requests and establishments
    // interleave at the server.
    let mut clients = Vec::with_capacity(N);
    for _ in 0..N {
        clients.push(thread::spawn(|| {
            let conn = Connection::connect(IP, PORT, 2, 2, CmParams::default()).expect("connect");
            conn.connect_finish().expect("connect_finish");
            conn.shutdown();
        }));
    }
    for c in clients {
        c.join().expect("client thread panicked");
    }
    server.join().expect("server thread panicked");
}
