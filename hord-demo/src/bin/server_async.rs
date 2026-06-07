//! HORD async demo server: `hyper` HTTP/1.1 over the async RDMA stream.
//!
//! Usage:
//!   hord-server-async [--bind <ip>] [--port <port>]
//!
//! Routes match the synchronous demo:
//!   GET /                -> a short text greeting
//!   GET /size/<n>        -> <n> bytes of a verifiable byte pattern, *streamed*
//!                           in fixed-size chunks (no up-front allocation — #14)
//!   (anything else)      -> 404
//!
//! Zero-copy (spec §7): when negotiated and a `GET /size/<n>` carries an
//! `X-HORD-RDMA-Write` header, the body is delivered by a one-sided RDMA write
//! into the client's buffer and the HTTP response carries `Content-Length: 0`.
//! The write is driven from inside the request handler via a [`SharedAsyncStream`]
//! handle — `hyper`'s `service_fn` never receives the connection, and the write
//! shares the one completion queue `hyper` drains, so the handler must reach the
//! same stream object (see hord-async). Other body responses to a zero-copy
//! request echo `status=declined`.
//!
//! Concurrency model: **thread-per-core**. A fixed pool of worker threads (one per
//! core by default, `--workers N` to override) each runs a current-thread Tokio
//! runtime + `LocalSet`. A blocking acceptor loop on the main thread round-robins
//! each accepted connection — the `Send` `Connection` from `accept_begin` — to a
//! worker over a channel; the worker `spawn_local`s a task that builds and drives
//! the `!Send` async stream. One worker thus drives *many* connections
//! concurrently on one core via its reactor (each connection still has its own CQ
//! completion-channel fd, registered with that runtime's epoll — the 1:1 model;
//! the N:1 completion-channel demux in 113.md is a later fd-economy optimization,
//! not required here). This replaces the previous one-OS-thread-per-connection
//! model, so connection count no longer maps to thread count.

use std::convert::Infallible;
use std::pin::Pin;
use std::process::ExitCode;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;

use hord_async::{AsyncHordStream, SharedAsyncStream};
use hord_demo::{
    content_range, content_range_unsatisfied, parse_range, pattern_byte,
    pattern_fill_registered_from, RangeSpec,
};
use hord_stream::{HordConfig, HordStream, Listener};
use hord_zerocopy::{RdmaWriteAction, RdmaWriteReq, RdmaWriteStatus, SourcePool, HEADER};

const DEFAULT_BIND: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const DEFAULT_PORT: u16 = 4791;
const MAX_BODY: usize = 1usize << 30; // 1 GiB guard on /size/<n>
const CHUNK: usize = 256 * 1024; // streamed body chunk size
// Per-connection zero-copy source pool (§8.3): up to this many reusable source
// buffers of this size, grown lazily and reused across a connection's zero-copy
// responses instead of registering an MR per response. Split mode (and any
// keep-alive client) serves many responses per connection, so the registrations
// amortize there; an object larger than the slab — or past the cap — falls back to
// a one-off registration (§8.4), so these tune efficiency, not correctness.
const SOURCE_POOL_CAP: usize = 4;
const SOURCE_POOL_BUF_SIZE: usize = 4 << 20; // 4 MiB

type DemoBody = BoxBody<Bytes, Infallible>;

/// A `/size/<n>` response body — or a `[start, end]` sub-range of it (§7.6) —
/// generated on the fly in [`CHUNK`]-sized frames, so the server never
/// materialises the whole body (review item #14). `offset`/`end` are *absolute*
/// object offsets, so each frame carries `pattern_byte(absolute offset)` and a
/// sub-range delivers exactly the bytes the client verifies against
/// `pattern_byte(start + i)`. Whole-object is just the `[0, total)` case.
struct PatternBody {
    offset: usize, // next absolute object offset to emit
    end: usize,    // one past the last absolute offset (start+len, or total)
}

impl PatternBody {
    /// Whole object: bytes `[0, total)`.
    fn new(total: usize) -> Self {
        PatternBody { offset: 0, end: total }
    }

    /// A single byte range (§7.6): `len` bytes starting at absolute offset `start`.
    fn range(start: usize, len: usize) -> Self {
        PatternBody { offset: start, end: start + len }
    }
}

impl Body for PatternBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        if this.offset >= this.end {
            return Poll::Ready(None);
        }
        let n = CHUNK.min(this.end - this.offset);
        let mut buf = vec![0u8; n];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = pattern_byte(this.offset + i);
        }
        this.offset += n;
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(buf)))))
    }

    fn is_end_stream(&self) -> bool {
        self.offset >= self.end
    }

    fn size_hint(&self) -> SizeHint {
        // Exact size -> hyper emits a Content-Length (not chunked encoding).
        SizeHint::with_exact((self.end - self.offset) as u64)
    }
}

/// Build a response with a stream body, optionally echoing a zero-copy status
/// (e.g. `declined` — §7.4 requires it on a body-bearing response to a request
/// that carried `X-HORD-RDMA-Write`).
fn respond(status: u16, content_type: &str, body: DemoBody, zc: Option<RdmaWriteStatus>) -> Response<DemoBody> {
    let mut b = Response::builder().status(status).header("content-type", content_type);
    if let Some(zc) = zc {
        b = b.header(HEADER, zc.header_value());
    }
    b.body(body).expect("valid response")
}

/// Build a bodiless (`Content-Length: 0`) response carrying a zero-copy status —
/// the payload travelled out-of-band via RDMA write. `content_range` is `Some`
/// only for a satisfied range (§7.6): a `206` echoes `Content-Range`, while a
/// `200`/`413` carries none (mirrors the sync `serve_size_zero_copy`).
fn zc_response(status_code: u16, zc: RdmaWriteStatus, content_range: Option<String>) -> Response<DemoBody> {
    let mut b = Response::builder()
        .status(status_code)
        .header("content-type", "application/octet-stream")
        .header(HEADER, zc.header_value());
    if let Some(cr) = content_range {
        b = b.header("content-range", cr);
    }
    b.body(empty()).expect("valid response")
}

/// §7.6: a satisfied range delivered on the *stream* (no zero-copy) — `206
/// Partial Content` with a `Content-Range` and the sub-range body, optionally
/// echoing a zero-copy status (`declined`, per §7.4) when the request carried the
/// header.
fn range_response(body: DemoBody, content_range: String, zc: Option<RdmaWriteStatus>) -> Response<DemoBody> {
    let mut b = Response::builder()
        .status(206)
        .header("content-type", "application/octet-stream")
        .header("content-range", content_range);
    if let Some(zc) = zc {
        b = b.header(HEADER, zc.header_value());
    }
    b.body(body).expect("valid response")
}

/// §7.6: a `Range` entirely past the end of the object → `416 Range Not
/// Satisfiable` with `Content-Range: bytes */total` and no body. Per §7.4 a
/// bodiless response omits `X-HORD-RDMA-Write` even if the request carried it.
fn unsatisfiable_response(total: usize) -> Response<DemoBody> {
    Response::builder()
        .status(416)
        .header("content-type", "application/octet-stream")
        .header("content-range", content_range_unsatisfied(total))
        .body(empty())
        .expect("valid response")
}

fn full(bytes: &'static [u8]) -> DemoBody {
    Full::new(Bytes::from_static(bytes)).boxed()
}

fn empty() -> DemoBody {
    Empty::<Bytes>::new().boxed()
}

async fn serve(
    req: Request<Incoming>,
    stream: SharedAsyncStream,
    pool: SourcePool,
) -> Result<Response<DemoBody>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    eprintln!("[server] {method} {path}");

    // Did the request carry a zero-copy header, and is it one we'll honour?
    let raw_header = req.headers().get(HEADER).and_then(|v| v.to_str().ok());
    let had_header = raw_header.is_some();
    let zc_req = if stream.zero_copy_negotiated() {
        raw_header.and_then(RdmaWriteReq::parse)
    } else {
        None
    };
    // §7.6: capture the (single) `Range` header value while the request is in
    // hand — it is resolved against the object size below.
    let range = req.headers().get("range").and_then(|v| v.to_str().ok()).map(str::to_string);
    // Drop the request before any await (we never read a GET body); nothing below
    // borrows it.
    drop(req);
    // §7.4: any body-bearing response to a request that carried the header must
    // echo a status; for non-zero-copy responses that is `declined`.
    let declined = had_header.then_some(RdmaWriteStatus::Declined);

    if method != Method::GET {
        return Ok(respond(405, "text/plain", full(b"only GET is supported\n"), declined));
    }
    if path == "/" {
        return Ok(respond(
            200,
            "text/plain",
            full(b"HORD async server. Try GET /size/<bytes>.\n"),
            declined,
        ));
    }
    if let Some(rest) = path.strip_prefix("/size/") {
        return Ok(match rest.parse::<usize>() {
            Ok(n) if n <= MAX_BODY => {
                // §7.6: resolve a single-range `Range` header against the object
                // size — absent/ignored → whole object (200); a satisfiable range
                // → 206 + Content-Range; a range past the end → 416. The one-sided
                // write is offset-agnostic, so a range composes with zero-copy by
                // writing the sub-range from its absolute object offset.
                match range.as_deref().map(|r| parse_range(r, n)).unwrap_or(RangeSpec::Full) {
                    RangeSpec::Unsatisfiable => unsatisfiable_response(n),
                    RangeSpec::Range { start, end } => {
                        let len = end - start + 1;
                        if let Some(req) = zc_req {
                            serve_zero_copy(&stream, &pool, &req, start, len, n, true).await
                        } else {
                            range_response(PatternBody::range(start, len).boxed(), content_range(start, end, n), declined)
                        }
                    }
                    RangeSpec::Full => {
                        if let Some(req) = zc_req {
                            serve_zero_copy(&stream, &pool, &req, 0, n, n, false).await
                        } else {
                            respond(200, "application/octet-stream", PatternBody::new(n).boxed(), declined)
                        }
                    }
                }
            }
            Ok(_) => respond(413, "text/plain", full(b"size exceeds server limit\n"), declined),
            Err(_) => respond(400, "text/plain", full(b"bad size\n"), declined),
        });
    }
    Ok(respond(404, "text/plain", full(b"not found\n"), declined))
}

/// Deliver a `/size/<n>` body — or a `[base, base+len)` sub-range of it (§7.6) —
/// by RDMA-writing it into the client's buffer, then return a bodiless response
/// with the outcome. The write is awaited before the response is built, so the
/// payload has landed by the time the client reads it. `partial` selects `206
/// Partial Content` + `Content-Range` over a plain `200`; `total` is the whole
/// object size for the `Content-Range`.
///
/// The one-sided write is offset-agnostic, so a sub-range is delivered exactly
/// like a whole object: `decide`'s gate runs against the range length (`len`) and
/// the source is filled from the range's absolute object offset (`base`).
///
/// In split mode (§7.7) — the request carries an `id` and we negotiated it — the
/// body is delivered with RDMA write-with-immediate, signalling the data plane on
/// the client's CQ; otherwise a plain one-sided write. The HTTP response is the
/// same either way (split vs. plain is purely the delivery mechanism, §7.7.4).
async fn serve_zero_copy(
    stream: &SharedAsyncStream,
    pool: &SourcePool,
    req: &RdmaWriteReq,
    base: usize,
    len: usize,
    total: usize,
    partial: bool,
) -> Response<DemoBody> {
    // A satisfied range echoes `Content-Range`; a `200`/`413` carries none.
    let range_hdr = |status: &RdmaWriteStatus| -> Option<String> {
        match status {
            RdmaWriteStatus::Complete { .. } if partial => Some(content_range(base, base + len - 1, total)),
            _ => None,
        }
    };
    // The §7.3/§7.7 policy (too-large gate, split vs. plain, the zero-length
    // 1-byte-source workaround) lives in the library so it can't drift from the
    // sync `serve_rdma_write`; here we only run the chosen plan with async writes
    // and map the outcome to HTTP. Gated on the range *length*, not the object size.
    match RdmaWriteAction::decide(req, len as u64, stream.split_mode_negotiated()) {
        // Nothing to write — map the status to a code and respond. Matched
        // exhaustively (no wildcard) so adding an RdmaWriteStatus variant is a
        // compile error here rather than a silent 200; decide() only ever yields
        // TooLarge or Complete in this path.
        RdmaWriteAction::Respond(status) => {
            let code = match status {
                RdmaWriteStatus::TooLarge { .. } => 413,
                RdmaWriteStatus::Complete { .. } | RdmaWriteStatus::Declined if partial => 206,
                RdmaWriteStatus::Complete { .. } | RdmaWriteStatus::Declined => 200,
            };
            eprintln!("[server] -> {code} (zero-copy: {})", status.header_value());
            zc_response(code, status, range_hdr(&status))
        }
        RdmaWriteAction::Write { payload_len, source_len, transfer_id } => {
            // NOTE: the too_large response above and the register_source-500 below
            // do NOT deliver an immediate; per §7.7.7 those are control-plane
            // fallbacks the client reconciles via the HTTP status, and a data-plane
            // consumer must bound its wait with a timeout rather than assume every
            // request yields a completion.
            // Lease a source from the per-connection pool (§8.3) — reusing its
            // registration — falling back to a one-off only when the object exceeds
            // the slab or the pool is exhausted. The lease (owning its buffer and
            // only an `Rc`) is held across the await and dropped at function end,
            // after the write is acknowledged, so the buffer returns to the pool with
            // no DMA still referencing it.
            let lease = match pool.acquire(source_len, |n| stream.register_source(n)) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("[server] source acquire failed: {e}");
                    return respond(500, "text/plain", full(b"internal error\n"), None);
                }
            };
            let src = lease.buffer();
            if payload_len > 0 {
                pattern_fill_registered_from(src, base, payload_len);
            }
            // Split mode delivers via write-with-immediate (signalling the client's
            // data plane on its CQ); plain mode is a one-sided write. The HTTP
            // response is identical either way (§7.7.4); a mid-write Err puts the QP
            // in error and the connection will close, so returning anything is
            // best-effort (§7.4). The logs keep the split/plain distinction (and the
            // transfer id) so a §7.7 data-plane stall can be correlated server-side.
            let result = match transfer_id {
                Some(id) => stream.rdma_write_with_imm(src, 0, req.addr, req.rkey, payload_len, id).await,
                None => stream.rdma_write(src, 0, req.addr, req.rkey, payload_len).await,
            };
            match result {
                Ok(()) => {
                    let code = if partial { 206 } else { 200 };
                    match transfer_id {
                        Some(id) => eprintln!(
                            "[server] -> {code} (split: complete id={id} bytes_written={payload_len})"
                        ),
                        None => eprintln!(
                            "[server] -> {code} (zero-copy: complete bytes_written={payload_len})"
                        ),
                    }
                    let status = RdmaWriteStatus::Complete { bytes_written: payload_len as u64 };
                    zc_response(code, status, range_hdr(&status))
                }
                Err(e) => {
                    match transfer_id {
                        Some(_) => eprintln!("[server] rdma_write_with_imm failed: {e}"),
                        None => eprintln!("[server] rdma_write failed: {e}"),
                    }
                    respond(500, "text/plain", full(b"rdma write failed\n"), None)
                }
            }
        }
    }
}

/// Drive one accepted connection to completion as a `spawn_local` task on a
/// worker's runtime. The stream is wrapped in a [`SharedAsyncStream`] so the
/// request handler can reach it to perform a zero-copy RDMA write while hyper owns
/// it for HTTP. `!Send` (it builds the `!Send` async stream), so it must be
/// `spawn_local`d — never `tokio::spawn`.
///
/// Caveat: `from_accepted` completes the HORD handshake *synchronously* (a brief
/// CQ busy-poll for one round trip), which momentarily pins this worker — and so
/// its other connections — until it returns. Acceptable here because the handshake
/// is a single fast exchange and the test fleet confirms one worker multiplexes
/// many connections; a production thread-per-core server would run the handshake
/// asynchronously (or on a dedicated handshake stage) so a slow-handshaking peer
/// cannot stall a worker's other connections.
async fn run_connection(conn: hord_stream::Connection, config: HordConfig) {
    let stream = match AsyncHordStream::from_accepted(conn, &config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[server] handshake failed: {e}");
            return;
        }
    };
    let shared = SharedAsyncStream::new(stream);
    let handle = shared.clone();
    // One lazy source pool per connection (MRs are PD-scoped); cloned into the
    // request handler (an Rc bump). Reused across this connection's zero-copy
    // responses — the win shows on split / keep-alive (many per connection).
    let pool = SourcePool::new(SOURCE_POOL_CAP, SOURCE_POOL_BUF_SIZE);
    if let Err(e) = http1::Builder::new()
        .serve_connection(
            TokioIo::new(shared),
            service_fn(move |req| serve(req, handle.clone(), pool.clone())),
        )
        .await
    {
        eprintln!("[server] connection error: {e}");
    }
}

/// One worker thread of the thread-per-core pool: a current-thread runtime +
/// `LocalSet` that receives accepted connections from the acceptor and
/// `spawn_local`s a [`run_connection`] task for each. The `LocalSet` drives all of
/// them concurrently on this one core while the loop keeps accepting more, so a
/// single worker fans out over many connections (each parked on its own CQ fd via
/// the runtime's reactor) — not one thread per connection. Returns when the
/// acceptor drops its sender (server shutdown).
fn worker_loop(worker_id: usize, mut rx: mpsc::UnboundedReceiver<hord_stream::Connection>, config: HordConfig) {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[server] worker {worker_id} runtime build failed: {e}");
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        while let Some(conn) = rx.recv().await {
            tokio::task::spawn_local(run_connection(conn, config.clone()));
        }
    }));
}

fn main() -> ExitCode {
    let mut bind = DEFAULT_BIND.to_string();
    let mut port = DEFAULT_PORT;
    let mut workers: Option<usize> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = args.next().unwrap_or(bind),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--workers" => workers = args.next().and_then(|n| n.parse().ok()),
            "-h" | "--help" => {
                eprintln!("usage: hord-server-async [--bind <ip>] [--port <port>] [--workers <n>]");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    let config = HordConfig::default();
    let listener = match Listener::bind(&bind, port) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[server] bind {bind}:{port} failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Default to one worker per core (thread-per-core). `--workers` overrides.
    let workers = workers
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
        .max(1);
    eprintln!(
        "[server] listening on {bind}:{port} (async/hyper, {workers} worker thread(s), spawn_local per connection, zero_copy={})",
        config.zero_copy
    );

    // Spawn the worker pool: one current-thread runtime per worker, each driving
    // many connections concurrently via spawn_local. The acceptor (this thread)
    // round-robins accepted connections to them. accept_begin migrates each
    // connection to its own CM event channel (Identifier::migrate /
    // rdma_migrate_id), so the acceptor, the workers, and the connections never
    // compete on a shared channel. (migrate is a local sideway patch — see
    // vendor/sideway/HORD-PATCH.md.)
    let mut senders = Vec::with_capacity(workers);
    let mut handles = Vec::with_capacity(workers);
    for id in 0..workers {
        let (tx, rx) = mpsc::unbounded_channel::<hord_stream::Connection>();
        senders.push(tx);
        let cfg = config.clone();
        handles.push(std::thread::spawn(move || worker_loop(id, rx, cfg)));
    }

    let mut next = 0usize;
    loop {
        match HordStream::accept_begin(&listener, &config) {
            Ok(conn) => {
                // Hand to the next worker; skip any whose thread has exited
                // (closed receiver). If every worker is gone there is nothing left
                // to serve the connection, so drop it with a warning.
                let mut conn = Some(conn);
                for _ in 0..workers {
                    let w = next;
                    next = (next + 1) % workers;
                    match senders[w].send(conn.take().expect("connection in hand")) {
                        Ok(()) => break,
                        Err(e) => conn = Some(e.0), // worker gone — try the next
                    }
                }
                if conn.is_some() {
                    eprintln!("[server] all workers unavailable; dropping connection");
                }
            }
            Err(e) => eprintln!("[server] accept failed: {e}"),
        }
    }
}
