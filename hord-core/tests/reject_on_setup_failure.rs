//! Per-connection setup failure is rejected, not abandoned.
//!
//! When a `ConnectRequest` is accepted but its per-connection setup fails after
//! the event is acked (its own CM channel, the migrate, the QP, or the INIT
//! transition), `Listener::process_event` must:
//!
//!   1. `rdma_reject` the peer — so the client's `connect` fails *fast* (a
//!      `Rejected` event) instead of waiting out a connect timeout, and no
//!      half-open id lingers until timewait; and
//!   2. return an error that [`is_connection_setup_failure`] recognises — so a
//!      threaded acceptor (`hord-async::HordListener`) skips this one peer and
//!      keeps accepting rather than counting it as a listener-level fault.
//!
//! We induce the failure deterministically by accepting with an impossibly large
//! QP work-request count (`1 << 28`), which `Endpoint::build` cannot satisfy on
//! any real device — the CQ/QP creation fails, exercising the post-ack
//! per-connection failure path.
//!
//! Needs the Soft-RoCE device (see CLAUDE.md), so it is `#[ignore]`d:
//!
//! ```sh
//! cargo test -p hord-core -- --ignored --nocapture reject_on_setup_failure
//! ```

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use hord_core::{is_connection_setup_failure, CmParams, Connection, Listener};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18523; // distinct from the other device tests (18520-18522)
const WATCHDOG: Duration = Duration::from_secs(30); // generous; loopback is ~instant
// Way past any device's max_qp_wr / max_cqe, so Endpoint::build is guaranteed to
// fail at CQ/QP creation — a per-connection setup failure after the ack.
const IMPOSSIBLE_WR: usize = 1 << 28;

#[test]
#[ignore = "requires the Soft-RoCE device (rxe0); run with --ignored"]
fn rejected_peer_fails_fast_and_is_classified() {
    // The exchange runs on a worker thread under a watchdog: a regression in the
    // reject (e.g. dropping the id without rejecting) manifests as the client's
    // connect_finish blocking until its connect timeout, which the watchdog turns
    // into a clean, deterministic test failure rather than a hang.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let runner = thread::spawn(move || {
        run_reject_exchange();
        let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(WATCHDOG) {
        Ok(()) => runner.join().expect("test runner panicked"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            runner.join().expect("test runner panicked");
            panic!("test runner exited without completing");
        }
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "reject_on_setup_failure timed out after {WATCHDOG:?} — the rejected peer's \
             connect_finish likely hung, i.e. the connection was dropped without rdma_reject"
        ),
    }
}

fn run_reject_exchange() {
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    let server = thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");

        // Accept with an impossible QP sizing so the per-connection Endpoint::build
        // fails *after* the ConnectRequest is acked + migrated — the exact path
        // that must reject the peer and tag the error. (`Connection` is not
        // `Debug`, so match rather than `expect_err`.)
        match listener.accept(IMPOSSIBLE_WR, IMPOSSIBLE_WR, CmParams::default()) {
            Ok(_) => panic!("accept with impossible WR count unexpectedly set up the connection"),
            Err(err) => assert!(
                is_connection_setup_failure(&err),
                "a post-ack per-connection setup failure must be tagged \
                 ConnectionSetupFailed, got: {err}"
            ),
        }
    });

    ready_rx.recv().expect("server ready");

    // The client issues the connect request (connect_finish), then must observe a
    // prompt rejection rather than a timeout — evidence the server's rdma_reject
    // reached the peer. connect_finish surfaces the `Rejected` CM event as an Err.
    let client = thread::spawn(|| {
        let conn = Connection::connect(IP, PORT, 2, 2, CmParams::default()).expect("connect");
        let res = conn.connect_finish();
        assert!(
            res.is_err(),
            "client connect_finish must fail (peer rejected), not succeed"
        );
    });

    client.join().expect("client thread panicked");
    server.join().expect("server thread panicked");
}
