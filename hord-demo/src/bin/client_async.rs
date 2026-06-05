//! HORD async demo client: one `hyper` HTTP/1.1 GET over the async RDMA stream.
//!
//! Usage:
//!   hord-client-async [--server <ip>] [--port <port>] [--path <path>]
//!                     [--zero-copy] [--zc-buf <bytes>] [--quiet]
//!
//! Mirrors the synchronous client: for a `/size/<n>` path it verifies the body
//! against the server's deterministic byte pattern. With `--zero-copy` (and a
//! server that negotiated it), the client registers a destination buffer,
//! advertises it via `X-HORD-RDMA-Write`, and on `status=complete` reads the body
//! straight out of that buffer — the server placed it there by RDMA write, so
//! nothing came over the stream (the HTTP body is empty). It falls back to the
//! stream body on `declined` / `too_large`.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::rt::TokioIo;

use hord_async::AsyncHordStream;
use hord_demo::{size_from_path, verify_stream_body, verify_zero_copy};
use hord_stream::HordConfig;
use hord_zerocopy::{RdmaWriteStatus, ZeroCopyRequest, HEADER};

const DEFAULT_SERVER: &str = "77.40.251.67";
const DEFAULT_PORT: u16 = 4791;
const DEFAULT_ZC_BUF: usize = 1 << 20; // 1 MiB, when the size isn't in the path
const DEADLINE: Duration = Duration::from_secs(120); // bound the whole exchange (#11)

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    let mut server = DEFAULT_SERVER.to_string();
    let mut port = DEFAULT_PORT;
    let mut path = "/".to_string();
    let mut quiet = false;
    let mut zero_copy = false;
    let mut zc_buf: Option<usize> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().unwrap_or(server),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--path" => path = args.next().unwrap_or(path),
            "--zero-copy" => zero_copy = true,
            "--zc-buf" => zc_buf = args.next().and_then(|n| n.parse().ok()),
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: hord-client-async [--server <ip>] [--port <port>] [--path <path>] \
                     [--zero-copy] [--zc-buf <bytes>] [--quiet]"
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[client] runtime build failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    // A LocalSet lets us spawn_local the hyper connection task (the stream is
    // !Send, so it cannot use tokio::spawn on a multi-thread runtime).
    let local = tokio::task::LocalSet::new();
    let opts = Opts { server, port, path, zero_copy, zc_buf, quiet };
    match rt.block_on(local.run_until(run(opts))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("[client] error: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Opts {
    server: String,
    port: u16,
    path: String,
    zero_copy: bool,
    zc_buf: Option<usize>,
    quiet: bool,
}

async fn run(opts: Opts) -> Result<(), BoxError> {
    let Opts { server, port, path, zero_copy, zc_buf, quiet } = opts;
    let config = HordConfig::default();
    if !quiet {
        eprintln!("[client] connecting to {server}:{port} ...");
    }
    let connect_start = Instant::now();
    let stream = AsyncHordStream::connect(&server, port, &config)?;
    if !quiet {
        eprintln!(
            "[client] connected in {:?} (payload capacity {} bytes/msg, zero_copy_negotiated={})",
            connect_start.elapsed(),
            stream.payload_capacity(),
            stream.zero_copy_negotiated()
        );
    }

    // Offer zero-copy only if we asked and the peer negotiated it. Register the
    // destination buffer up front, before the stream is handed to hyper, so its
    // address/rkey can ride in the request header. The buffer (inside the
    // ZeroCopyRequest) is independent of the stream — it owns its own connection
    // handle — so we keep it alongside and it outlives the stream's teardown.
    let capacity = zc_buf.or_else(|| size_from_path(&path)).unwrap_or(DEFAULT_ZC_BUF);
    let dest: Option<ZeroCopyRequest> = if zero_copy && stream.zero_copy_negotiated() && capacity > 0 {
        let zc = ZeroCopyRequest::from_buffer(stream.register_remote_writable(capacity)?);
        if !quiet {
            eprintln!("[client] zero-copy: advertising a {capacity}-byte buffer");
        }
        Some(zc)
    } else {
        if zero_copy && !quiet {
            // capacity == 0 (e.g. /size/0): a zero-length destination MR is not
            // portable and a 0-byte zero-copy transfer is pointless, so fall back.
            let why = if !stream.zero_copy_negotiated() {
                "peer did not negotiate it"
            } else {
                "buffer would be 0 bytes"
            };
            eprintln!("[client] --zero-copy requested but {why}; using the stream");
        }
        None
    };

    // Low-level http1 handshake: a sender + a connection future we must drive.
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let conn_task = tokio::task::spawn_local(async move {
        if let Err(e) = conn.await {
            eprintln!("[client] connection task ended: {e}");
        }
    });

    let mut builder = Request::builder()
        .uri(&path)
        .header("host", server.as_str())
        .header("user-agent", "hord-client-async/0.1")
        .header("connection", "close");
    if let Some(zc) = &dest {
        builder = builder.header(HEADER, zc.request().header_value());
    }
    let request = builder.body(Empty::<Bytes>::new())?;

    let req_start = Instant::now();
    // Bound the request + full body read by a deadline: a stalled-but-alive peer
    // errors here rather than hanging forever (review item #11). Returns the
    // status, the parsed zero-copy response status (if any), and the stream body.
    let offered_zc = dest.is_some();
    let (status, zc_status, body) = tokio::time::timeout(DEADLINE, async {
        let res = sender.send_request(request).await?;
        let status = res.status();
        if !quiet {
            eprintln!("[client] {:?} {status}", res.version());
        }
        let zc_status = if offered_zc {
            res.headers()
                .get(HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(RdmaWriteStatus::parse)
        } else {
            None
        };
        let collected = res.into_body().collect().await?;
        Ok::<_, BoxError>((status, zc_status, collected.to_bytes()))
    })
    .await
    .map_err(|_| -> BoxError { "request timed out".into() })??;
    let elapsed = req_start.elapsed();

    // Determine where the body came from and verify the pattern.
    let to_err = |m: String| -> BoxError { m.into() };
    let (body_len, delivery, verified) = match zc_status {
        Some(RdmaWriteStatus::Complete { bytes_written }) => {
            let zc = dest.as_ref().expect("dest set when zc status parsed");
            let n = bytes_written as usize;
            // Trust the peer's bytes_written only as far as our own buffer (see
            // the sync client) — keeps the in-place verify in range.
            if n > zc.capacity() {
                return Err(format!("server reported bytes_written={n} > buffer {}", zc.capacity()).into());
            }
            let verified = verify_zero_copy(zc, n, &path).map_err(to_err)?;
            (n, "zero-copy (RDMA write)", verified)
        }
        Some(RdmaWriteStatus::TooLarge { object_size }) => {
            if !quiet {
                eprintln!("[client] zero-copy declined: object_size={object_size} exceeds our buffer");
            }
            (0usize, "none (too_large)", false)
        }
        // Declined / no zero-copy: the body arrived on the stream.
        _ => {
            let verified = verify_stream_body(&body, status.as_u16() == 200, &path).map_err(to_err)?;
            (body.len(), "stream", verified)
        }
    };

    // Dropping the sender lets the connection close; wait for its task to end.
    drop(sender);
    let _ = conn_task.await;

    let secs = elapsed.as_secs_f64();
    let mb = body_len as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };

    println!("status:      {status}");
    println!("delivery:    {delivery}");
    println!("body bytes:  {body_len}");
    println!("elapsed:     {elapsed:?}");
    println!("throughput:  {throughput:.1} MiB/s");
    if verified {
        println!("integrity:   OK (byte pattern verified)");
    }
    Ok(())
}
