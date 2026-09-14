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
use hord_demo::{
    resolve_range, size_from_path, verify_stream_body_at, verify_zero_copy, verify_zero_copy_at,
};
use hord_stream::HordConfig;
use hord_zerocopy::{RdmaWriteStatus, ZeroCopyRequest, HEADER};

const DEFAULT_SERVER: &str = "192.0.2.1"; // rxe device IP fallback; override via $HORD_TEST_IP or --server
const DEFAULT_PORT: u16 = 4791;
const DEFAULT_ZC_BUF: usize = 1 << 20; // 1 MiB, when the size isn't in the path
const DEFAULT_SPLIT_COUNT: usize = 4; // transfers issued by --split
const DEADLINE: Duration = Duration::from_secs(120); // bound the whole exchange (#11)

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Progress reporting to stderr (stdout carries the machine-readable result),
/// silenced by `--quiet`. Exists so the protocol steps in `run` / `run_split`
/// read as a straight sequence — one unconditional `say!` per step — instead of
/// an `if !quiet { eprintln!(…) }` block wrapped around every message. (Mirrors
/// the sync client; kept per-binary rather than in the `hord_demo` library,
/// which is the transport-independent codec and has no business writing to
/// stderr.)
struct Report {
    enabled: bool,
}

impl Report {
    fn new(quiet: bool) -> Report {
        Report { enabled: !quiet }
    }

    /// Takes pre-captured [`std::fmt::Arguments`] rather than a `String` so a
    /// quiet run pays nothing: the formatting happens inside `eprintln!`, which
    /// we never reach when silenced.
    fn say(&self, msg: std::fmt::Arguments<'_>) {
        if self.enabled {
            eprintln!("{msg}");
        }
    }
}

/// `say!(report, "…{x}")` — same call shape as `eprintln!`, but `format_args!`
/// only captures references to the arguments, so nothing is formatted unless
/// [`Report::say`] decides to print.
macro_rules! say {
    ($report:expr, $($arg:tt)*) => {
        $report.say(format_args!($($arg)*))
    };
}

fn main() -> ExitCode {
    let mut server = std::env::var("HORD_TEST_IP").unwrap_or_else(|_| DEFAULT_SERVER.to_string());
    let mut port = DEFAULT_PORT;
    let mut path = "/".to_string();
    let mut quiet = false;
    let mut zero_copy = false;
    let mut zc_buf: Option<usize> = None;
    let mut range: Option<String> = None;
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
            "--range" => range = args.next(),
            "--split" => split = true,
            "--count" => count = args.next().and_then(|n| n.parse().ok()).unwrap_or(count),
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: hord-client-async [--server <ip>] [--port <port>] [--path <path>] \
                     [--zero-copy] [--zc-buf <bytes>] [--range <spec>] [--split] [--count <n>] [--quiet]\n\
                     \n  --range <spec>  request a single byte range (§7.6) of a /size/<n> object \
                     (<spec> is a-b, a-, or -n); yields 206 (composed with --zero-copy) or 416.\
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
    let opts = Opts {
        server,
        port,
        path,
        zero_copy,
        zc_buf,
        range,
        split,
        count,
        quiet,
    };
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
    range: Option<String>,
    split: bool,
    count: usize,
    quiet: bool,
}

/// Where the response body came from, and what we could prove about it.
struct Delivery {
    body_len: usize,
    /// Human-readable delivery path, for the `delivery:` summary line.
    source: &'static str,
    /// True when the bytes were checked against the `/size/<n>` pattern.
    verified: bool,
}

async fn run(opts: Opts) -> Result<(), BoxError> {
    let Opts {
        server,
        port,
        path,
        zero_copy,
        zc_buf,
        range,
        quiet,
        ..
    } = opts;
    let report = Report::new(quiet);
    let config = HordConfig::default();
    say!(report, "[client] connecting to {server}:{port} ...");
    let connect_start = Instant::now();
    let stream = AsyncHordStream::connect(&server, port, &config)?;
    say!(
        report,
        "[client] connected in {:?} (payload capacity {} bytes/msg, zero_copy_negotiated={})",
        connect_start.elapsed(),
        stream.payload_capacity(),
        stream.zero_copy_negotiated()
    );

    // §7.6: resolve the requested range locally (mirrors the sync client), so the
    // destination buffer is sized to the range rather than the whole object and
    // the delivered bytes are verified at their absolute object offset.
    let total = size_from_path(&path);
    let (range_base, range_len) = resolve_range(range.as_deref(), total);
    let capacity = zc_buf.or(range_len).or(total).unwrap_or(DEFAULT_ZC_BUF);
    // Register before the stream is handed to hyper, so the address/rkey can ride
    // in the request header.
    let dest = offer_zero_copy(&stream, zero_copy, capacity, &report)?;

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
    if let Some(spec) = &range {
        builder = builder.header("range", format!("bytes={spec}"));
    }
    if let Some(zc) = &dest {
        builder = builder.header(HEADER, zc.request().header_value());
    }
    let request = builder.body(Empty::<Bytes>::new())?;

    let req_start = Instant::now();
    let exchange = exchange(&mut sender, request, dest.is_some(), &report).await?;
    let elapsed = req_start.elapsed();

    // §7.6: an unsatisfiable range → 416 with `Content-Range: bytes */total` and no
    // body. Nothing to verify; report and finish.
    if exchange.status.as_u16() == 416 {
        drop(sender);
        let _ = conn_task.await;
        println!("status:      {}", exchange.status);
        println!("delivery:    none (range not satisfiable)");
        println!(
            "content-range: {}",
            exchange.content_range.as_deref().unwrap_or("(none)")
        );
        return Ok(());
    }

    let delivery = take_delivery(&exchange, dest.as_ref(), range_base, &path, &report)?;

    // Dropping the sender lets the connection close; wait for its task to end.
    drop(sender);
    let _ = conn_task.await;

    print_summary(
        &exchange.status.to_string(),
        exchange.content_range.as_deref(),
        &delivery,
        elapsed,
    );
    Ok(())
}

/// Register the zero-copy destination buffer, if we asked for zero-copy *and*
/// the peer negotiated it. Returns `None` — after saying why — when either half
/// is missing, which is the signal to take the ordinary stream body instead.
///
/// The buffer (inside the [`ZeroCopyRequest`]) is independent of the stream — it
/// owns its own connection handle — so the caller keeps it alongside and it
/// outlives the stream's teardown inside `hyper`.
fn offer_zero_copy(
    stream: &AsyncHordStream,
    requested: bool,
    capacity: usize,
    report: &Report,
) -> Result<Option<ZeroCopyRequest>, BoxError> {
    if !requested {
        return Ok(None);
    }
    // capacity == 0 (e.g. /size/0): a zero-length destination MR is not portable
    // and a 0-byte zero-copy transfer is pointless, so fall back.
    let why = if !stream.zero_copy_negotiated() {
        "peer did not negotiate it"
    } else if capacity == 0 {
        "buffer would be 0 bytes"
    } else {
        let zc = ZeroCopyRequest::from_buffer(stream.register_remote_writable(capacity)?);
        say!(
            report,
            "[client] zero-copy: advertising a {capacity}-byte buffer"
        );
        return Ok(Some(zc));
    };
    say!(
        report,
        "[client] --zero-copy requested but {why}; using the stream"
    );
    Ok(None)
}

/// What the HTTP exchange yielded, once the response head has been read and the
/// (possibly empty) stream body collected.
struct Exchange {
    status: hyper::StatusCode,
    /// The parsed `X-HORD-RDMA-Write` response status (§7.3), when we offered.
    zc_status: Option<RdmaWriteStatus>,
    /// §7.6: a 206/416 carries `Content-Range`.
    content_range: Option<String>,
    body: Bytes,
}

/// Send the request and read the whole response, bounded by [`DEADLINE`] so a
/// stalled-but-alive peer errors here rather than hanging forever (review #11).
async fn exchange(
    sender: &mut hyper::client::conn::http1::SendRequest<Empty<Bytes>>,
    request: Request<Empty<Bytes>>,
    offered_zc: bool,
    report: &Report,
) -> Result<Exchange, BoxError> {
    tokio::time::timeout(DEADLINE, async {
        let res = sender.send_request(request).await?;
        let status = res.status();
        say!(report, "[client] {:?} {status}", res.version());
        let zc_status = if offered_zc {
            res.headers()
                .get(HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(RdmaWriteStatus::parse)
        } else {
            None
        };
        // Capture Content-Range while the response is in hand (the body collect
        // below consumes it).
        let content_range = res
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let collected = res.into_body().collect().await?;
        Ok::<_, BoxError>(Exchange {
            status,
            zc_status,
            content_range,
            body: collected.to_bytes(),
        })
    })
    .await
    .map_err(|_| -> BoxError { "request timed out".into() })?
}

/// Determine where the body came from and verify it at `range_base`, its
/// absolute offset in the object (0 for a whole object).
///
/// `X-HORD-RDMA-Write: status=complete` (§7.3) means the bytes are already in
/// our registered buffer and the HTTP body is empty; anything else — `declined`,
/// `too_large`, malformed, or no zero-copy at all — means the body arrived on
/// the stream.
fn take_delivery(
    exchange: &Exchange,
    dest: Option<&ZeroCopyRequest>,
    range_base: usize,
    path: &str,
    report: &Report,
) -> Result<Delivery, BoxError> {
    let to_err = |m: String| -> BoxError { m.into() };
    match exchange.zc_status {
        Some(RdmaWriteStatus::Complete { bytes_written }) => {
            let zc = dest.expect("dest set when zc status parsed");
            let n = bytes_written as usize;
            // Trust the peer's bytes_written only as far as our own buffer (see
            // the sync client) — keeps the in-place verify in range.
            if n > zc.capacity() {
                return Err(format!(
                    "server reported bytes_written={n} > buffer {}",
                    zc.capacity()
                )
                .into());
            }
            let verified = verify_zero_copy_at(zc, range_base, n, path).map_err(to_err)?;
            Ok(Delivery {
                body_len: n,
                source: "zero-copy (RDMA write)",
                verified,
            })
        }
        Some(RdmaWriteStatus::TooLarge { object_size }) => {
            say!(
                report,
                "[client] zero-copy declined: object_size={object_size} exceeds our buffer"
            );
            Ok(Delivery {
                body_len: 0,
                source: "none (too_large)",
                verified: false,
            })
        }
        // Declined / no zero-copy: the body arrived on the stream.
        _ => {
            let is_success = matches!(exchange.status.as_u16(), 200 | 206);
            let verified = verify_stream_body_at(&exchange.body, is_success, path, range_base)
                .map_err(to_err)?;
            Ok(Delivery {
                body_len: exchange.body.len(),
                source: "stream",
                verified,
            })
        }
    }
}

/// The machine-readable result, on stdout (progress goes to stderr via [`Report`]).
fn print_summary(
    status: &str,
    content_range: Option<&str>,
    delivery: &Delivery,
    elapsed: Duration,
) {
    let secs = elapsed.as_secs_f64();
    let mb = delivery.body_len as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };

    println!("status:      {status}");
    println!("delivery:    {}", delivery.source);
    if let Some(cr) = content_range {
        println!("content-range: {cr}");
    }
    println!("body bytes:  {}", delivery.body_len);
    println!("elapsed:     {elapsed:?}");
    println!("throughput:  {throughput:.1} MiB/s");
    if delivery.verified {
        println!("integrity:   OK (byte pattern verified)");
    }
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
    let Opts {
        server,
        port,
        path,
        zc_buf,
        count,
        quiet,
        ..
    } = opts;
    if count == 0 {
        return Err("--count must be >= 1".into());
    }
    let report = Report::new(quiet);
    let config = HordConfig::default();
    say!(
        report,
        "[client] connecting to {server}:{port} (split mode) ..."
    );
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
    say!(
        report,
        "[client] split: {count} transfers, {capacity}-byte buffers, path {path}"
    );

    // Control plane: hyper over one clone of the shared stream.
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(shared.clone())).await?;
    let conn_task = tokio::task::spawn_local(async move {
        if let Err(e) = conn.await {
            eprintln!("[client] connection task ended: {e}");
        }
    });

    let start = Instant::now();
    issue_split_requests(&mut sender, &server, &path, &reqs, &report).await?;

    // Close the control plane; its task drops its stream clone.
    drop(sender);
    let _ = conn_task.await;
    let control_elapsed = start.elapsed();

    let verified = collect_split_completions(&shared, &reqs, object_size, &path, &report).await?;

    println!("delivery:    split (RDMA write-with-immediate, §7.7)");
    println!("transfers:   {count} (collected off the CQ by id)");
    println!("control:     {control_elapsed:?} (HTTP control plane)");
    if object_size.is_some() {
        println!("integrity:   {verified}/{count} payloads verified");
    }
    Ok(())
}

/// Control plane: issue one GET per advertised buffer, sequentially (http1
/// keep-alive). Each response head is awaited only to confirm `status=complete`
/// and to free the sender for the next request — the body is empty, because the
/// payload travelled out-of-band.
async fn issue_split_requests(
    sender: &mut hyper::client::conn::http1::SendRequest<Empty<Bytes>>,
    server: &str,
    path: &str,
    reqs: &[ZeroCopyRequest],
    report: &Report,
) -> Result<(), BoxError> {
    for (i, zc) in reqs.iter().enumerate() {
        let request = Request::builder()
            .uri(path)
            .header("host", server)
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
                say!(report, "[client] request id={i}: {status} status=complete");
            }
            other => {
                return Err(format!(
                    "request id={i}: expected split status=complete, got {status} / {other:?}"
                )
                .into());
            }
        }
    }
    Ok(())
}

/// Data plane: collect one completion per transfer by id (already reaped by the
/// control-plane task) and verify each landed payload against the deterministic
/// pattern, returning how many verified.
///
/// Each wait is bounded by [`DEADLINE`] (spec §7.7.7: "Clients SHOULD implement a
/// timeout for data-plane completions") so a transfer the server reported
/// `complete` over HTTP but never signalled on the CQ — or any lost immediate —
/// surfaces as a timeout error instead of hanging forever.
async fn collect_split_completions(
    shared: &SharedAsyncStream,
    reqs: &[ZeroCopyRequest],
    object_size: Option<usize>,
    path: &str,
    report: &Report,
) -> Result<usize, BoxError> {
    let count = reqs.len();
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
        let Some(id) = next else {
            return Err(format!(
                "connection closed; only {} of {count} transfers completed",
                seen.len()
            )
            .into());
        };
        if !seen.insert(id) {
            return Err(format!("transfer id={id} completed twice").into());
        }
        let zc = reqs
            .get(id as usize)
            .ok_or_else(|| -> BoxError { format!("unknown transfer id {id}").into() })?;
        if let Some(n) = object_size {
            let n = n.min(zc.capacity());
            if verify_zero_copy(zc, n, path).map_err(|m: String| -> BoxError { m.into() })? {
                verified += 1;
            }
        }
        say!(report, "[client] data plane: transfer id={id} landed");
    }
    Ok(verified)
}
