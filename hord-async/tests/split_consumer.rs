//! Split-mode (§7.7) data-plane consumer running on its **own task**, concurrent
//! with the HTTP control plane — over the host's Soft-RoCE device (`rxe0`, see
//! CLAUDE.md), so it is `#[ignore]`d by default. Run with:
//!
//! ```sh
//! cargo test -p hord-async -- --ignored --nocapture split_data_plane_separate_task
//! ```
//!
//! The other half of the multi-waiter story ([`AsyncHordStream::into_split`]): a
//! client splits its stream and drives the split-mode payload completions
//! ([`DataPlane::next_split_completion`]) from a task **separate** from the one
//! doing the request/response over the read/write halves. Both the data task and
//! the control read are parked on the shared pump at once — the exact case that,
//! pre-pump, had to share the single control-plane driver. The server is the
//! ordinary single-task async path (it RDMA-writes-with-immediate, then sends the
//! status), proving the split is purely a client-side, transport-compatible change.

use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::LocalSet;

use hord_async::{AsyncHordStream, SharedAsyncStream, SplitParts};
use hord_stream::{HordConfig, HordStream, Listener, RegisteredBuffer};
use hord_zerocopy::{RdmaWriteReq, RdmaWriteStatus};

const IP: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const PORT: u16 = 18921; // distinct from the demo (4791) and other loopback tests
const OBJECT: usize = 4 * 1024 * 1024; // 4 MiB — many MTUs, dwarfs the credit window
const TRANSFER_ID: u32 = 0x00C0_FFEE; // the §7.7 id echoed back on the data plane
const DEADLINE: Duration = Duration::from_secs(30);

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
fn split_data_plane_separate_task() {
    let config = HordConfig::default();
    let teardown = Arc::new(Barrier::new(2));
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    // Server: ordinary single-task async path. Read the request, RDMA-write the
    // object into the client's buffer *with the immediate* carrying the id, then
    // send the status line.
    let srv_config = config.clone();
    let srv_teardown = Arc::clone(&teardown);
    let server = std::thread::spawn(move || {
        let listener = Listener::bind(IP, PORT).expect("bind");
        ready_tx.send(()).expect("signal ready");
        let conn = HordStream::accept_begin(&listener, &srv_config).expect("accept_begin");
        current_thread_rt().block_on(async move {
            let stream = AsyncHordStream::from_accepted(conn, &srv_config).expect("accept");
            assert!(stream.split_mode_negotiated(), "server: split mode not negotiated");
            let mut shared = SharedAsyncStream::new(stream);

            let req = RdmaWriteReq::parse(&read_line(&mut shared).await).expect("parse request");
            let id = req.id.expect("request carried no split id");
            let src = shared.register_source(OBJECT).expect("register source");
            fill(&src, OBJECT);
            shared
                .rdma_write_with_imm(&src, 0, req.addr, req.rkey, OBJECT, id)
                .await
                .expect("rdma_write_with_imm");
            let status = RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 };
            write_line(&mut shared, &status.header_value()).await;
            srv_teardown.wait();
            shared.disconnect();
        });
    });

    ready_rx.recv().expect("server ready");
    let rt = current_thread_rt();
    let local = LocalSet::new();
    rt.block_on(local.run_until(async move {
        let stream = AsyncHordStream::connect(IP, PORT, &config).expect("connect");
        let SplitParts { read, write, data } = stream.into_split();
        assert!(data.split_mode_negotiated(), "client: split mode not negotiated");

        // Register the destination buffer and advertise it (with the split id).
        let buf = data.register_remote_writable(OBJECT).expect("register dest");
        let req = RdmaWriteReq {
            addr: buf.as_mut_ptr() as u64,
            rkey: buf.rkey(),
            len: OBJECT as u64,
            id: Some(TRANSFER_ID),
        };

        // Data plane on its OWN task: park on the pump waiting for the payload's
        // write-with-immediate completion while the control plane runs below.
        let data_task = tokio::task::spawn_local(async move {
            tokio::time::timeout(DEADLINE, data.next_split_completion()).await
        });

        // Control plane on this task: send the request, read the status. Both this
        // read and the data task above are parked on the one pump concurrently.
        // Capture the outcomes (bounded by DEADLINE) WITHOUT asserting yet.
        let mut write = write;
        let mut read = read;
        write_line(&mut write, &req.header_value()).await;
        let status_res = tokio::time::timeout(DEADLINE, read_line(&mut read)).await;
        let data_res = data_task.await;

        // Rendezvous BEFORE asserting, so a verification failure here can't strand
        // the server at its barrier — both sides reach it after the exchange.
        teardown.wait();

        // Now surface failures and verify.
        let status = status_res.expect("status read timed out");
        let status = RdmaWriteStatus::parse(&status).expect("parse status");
        assert_eq!(
            status,
            RdmaWriteStatus::Complete { bytes_written: OBJECT as u64 },
            "expected a complete zero-copy write",
        );

        // The data plane should have surfaced exactly our transfer id.
        let got_id = data_res
            .expect("data task panicked")
            .expect("data-plane completion timed out")
            .expect("data-plane io error");
        assert_eq!(got_id, Some(TRANSFER_ID), "wrong/absent split transfer id");

        // The payload landed in our buffer out-of-band — verify it in place.
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
    }));

    server.join().expect("server thread panicked");
}
