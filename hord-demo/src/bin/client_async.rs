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

use hord_async::{AsyncHordStream, SharedAsyncStream};
use hord_demo::{size_from_path, verify_stream_body, verify_zero_copy};
use hord_stream::HordConfig;
use hord_zerocopy::{RdmaWriteStatus, ZeroCopyRequest, HEADER};

const DEFAULT_SERVER: &str = "77.40.251.67";
const DEFAULT_PORT: u16 = 4791;
const DEFAULT_ZC_BUF: usize = 1 << 20; // 1 MiB, when the size isn't in the path
const DEFAULT_SPLIT_COUNT: usize = 4; // transfers issued by --split
const DEADLINE: Duration = Duration::from_secs(120); // bound the whole exchange (#11)

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    let mut server = DEFAULT_SERVER.to_string();
    let mut port = DEFAULT_PORT;
    let mut path = "/".to_string();
    let mut quiet = false;
    let mut zero_copy = false;
    let mut zc_buf: Option<usize> = None;
    let mut split = false;
    let mut count = DEFAULT_SPLIT_COUNT;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().unwrap_or(server),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--path" => path = args.next().unwrap_or(path),
            "--zero-copy" => zero_copy = true,
            "--zc-buf" => zc_buf = args.next().and_then(|n| n.parse().ok()),
            "--split" => split = true,
            "--count" => count = args.next().and_then(|n| n.parse().ok()).unwrap_or(count),
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: hord-client-async [--server <ip>] [--port <port>] [--path <path>] \
                     [--zero-copy] [--zc-buf <bytes>] [--split] [--count <n>] [--quiet]\n\
                     \n  --split   issue --count GETs (default {DEFAULT_SPLIT_COUNT}) in split mode (§7.7); \
                     payloads are collected off the CQ by transfer id, not from the HTTP body."
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
    let opts = Opts { server, port, path, zero_copy, zc_buf, split, count, quiet };
    let fut = async {
        if opts.split {
            run_split(opts).await
        } else {
            run(opts).await
        }
    };
    match rt.block_on(local.run_until(fut)) {
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
    split: bool,
    count: usize,
    quiet: bool,
}

async fn run(opts: Opts) -> Result<(), BoxError> {
    let Opts { server, port, path, zero_copy, zc_buf, quiet, .. } = opts;
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

/// Protocol-splitting client (spec §7.7): issue `count` GETs, each advertising a
/// distinct buffer with a split `id`, then collect the payloads off the CQ — by
/// transfer id, with no HTTP body parsing.
///
/// Driver model (single-task, per hord-async): the `hyper` control plane runs on
/// its own connection task and reaps every data-plane completion into the
/// stream's transfer queue while it reads each (empty) HTTP response. Once the
/// control plane is done, the data plane drains that queue — so the payloads were
/// already signalled on the CQ, independent of (in fact before) we looked. The
/// shared handle lets both planes reach the one stream without a second CQ waiter
/// (which the prototype does not support).
async fn run_split(opts: Opts) -> Result<(), BoxError> {
    let Opts { server, port, path, zc_buf, count, quiet, .. } = opts;
    if count == 0 {
        return Err("--count must be >= 1".into());
    }
    let config = HordConfig::default();
    if !quiet {
        eprintln!("[client] connecting to {server}:{port} (split mode) ...");
    }
    let stream = AsyncHordStream::connect(&server, port, &config)?;
    if !stream.zero_copy_negotiated() || !stream.split_mode_negotiated() {
        return Err(format!(
            "peer did not negotiate split mode (zero_copy={}, split={})",
            stream.zero_copy_negotiated(),
            stream.split_mode_negotiated()
        )
        .into());
    }

    // One destination buffer per transfer, each advertised with a distinct id.
    // A zero-length MR is not portable, so floor the capacity at 1 (lets
    // /size/0 still drive a completion).
    let object_size = size_from_path(&path);
    let capacity = zc_buf.or(object_size).unwrap_or(DEFAULT_ZC_BUF).max(1);
    let shared = SharedAsyncStream::new(stream);
    let mut reqs: Vec<ZeroCopyRequest> = Vec::with_capacity(count);
    for i in 0..count {
        let zc = ZeroCopyRequest::from_buffer(shared.register_remote_writable(capacity)?)
            .with_id(i as u32);
        reqs.push(zc);
    }
    if !quiet {
        eprintln!("[client] split: {count} transfers, {capacity}-byte buffers, path {path}");
    }

    // Control plane: hyper over one clone of the shared stream.
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(shared.clone())).await?;
    let conn_task = tokio::task::spawn_local(async move {
        if let Err(e) = conn.await {
            eprintln!("[client] connection task ended: {e}");
        }
    });

    // Issue the GETs sequentially (http1 keep-alive). We await each response head
    // only to confirm status=complete and to free the sender for the next
    // request; the body is empty (the payload travelled out-of-band).
    let start = Instant::now();
    for (i, zc) in reqs.iter().enumerate() {
        let request = Request::builder()
            .uri(&path)
            .header("host", server.as_str())
            .header("user-agent", "hord-client-async/0.1")
            .header(HEADER, zc.request().header_value())
            .body(Empty::<Bytes>::new())?;
        let (status, zc_status) = tokio::time::timeout(DEADLINE, async {
            let res = sender.send_request(request).await?;
            let status = res.status();
            let zc_status = res
                .headers()
                .get(HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(RdmaWriteStatus::parse);
            res.into_body().collect().await?; // drain the (empty) body
            Ok::<_, BoxError>((status, zc_status))
        })
        .await
        .map_err(|_| -> BoxError { format!("request {i} timed out").into() })??;
        match zc_status {
            Some(RdmaWriteStatus::Complete { .. }) => {
                if !quiet {
                    eprintln!("[client] request id={i}: {status} status=complete");
                }
            }
            other => {
                return Err(format!(
                    "request id={i}: expected split status=complete, got {status} / {other:?}"
                )
                .into());
            }
        }
    }

    // Close the control plane; its task drops its stream clone.
    drop(sender);
    let _ = conn_task.await;
    let control_elapsed = start.elapsed();

    // Data plane: collect `count` completions by id (already reaped above) and
    // verify each landed payload against the deterministic pattern. Each wait is
    // bounded by DEADLINE (spec §7.7.7: "Clients SHOULD implement a timeout for
    // data-plane completions") so a transfer the server reported `complete` over
    // HTTP but never signalled on the CQ — or any lost immediate — surfaces as a
    // timeout error instead of hanging forever.
    let mut seen = std::collections::HashSet::new();
    let mut verified = 0usize;
    while seen.len() < count {
        let next = tokio::time::timeout(DEADLINE, shared.next_split_completion())
            .await
            .map_err(|_| -> BoxError {
                format!(
                    "data-plane completion timed out after {DEADLINE:?} ({} of {count} received)",
                    seen.len()
                )
                .into()
            })??;
        match next {
            Some(id) => {
                if !seen.insert(id) {
                    return Err(format!("transfer id={id} completed twice").into());
                }
                let zc = reqs
                    .get(id as usize)
                    .ok_or_else(|| -> BoxError { format!("unknown transfer id {id}").into() })?;
                if let Some(n) = object_size {
                    let n = n.min(zc.capacity());
                    if verify_zero_copy(zc, n, &path).map_err(|m: String| -> BoxError { m.into() })? {
                        verified += 1;
                    }
                }
                if !quiet {
                    eprintln!("[client] data plane: transfer id={id} landed");
                }
            }
            None => {
                return Err(format!(
                    "connection closed; only {} of {count} transfers completed",
                    seen.len()
                )
                .into());
            }
        }
    }

    println!("delivery:    split (RDMA write-with-immediate, §7.7)");
    println!("transfers:   {count} (collected off the CQ by id)");
    println!("control:     {control_elapsed:?} (HTTP control plane)");
    if object_size.is_some() {
        println!("integrity:   {verified}/{count} payloads verified");
    }
    Ok(())
}
