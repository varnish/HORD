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
//! Concurrency model: **thread-per-core**, owned by [`hord_async::HordListener`].
//! The listener runs an accept loop and a fixed pool of worker threads (one per
//! core by default, `--workers N` to override), each a current-thread Tokio
//! runtime + `LocalSet`; it round-robins each accepted connection to a worker,
//! which builds and `spawn_local`s the `!Send` async stream. One worker thus
//! drives *many* connections concurrently on one core via its reactor (each with
//! its own CQ completion-channel fd — the 1:1 model; the N:1 demux in 113.md is a
//! later fd-economy optimization). The demo supplies only the per-connection
//! service — a closure `(AsyncHordStream, SocketAddr, watch::Receiver<bool>) ->
//! impl Future` — which is exactly the seam a host like Carapace plugs into. Ctrl-C
//! fires the listener's graceful-shutdown signal, which stops accepting and drains
//! in-flight connections before returning; each connection also receives a clone of
//! that signal (the closure's third argument) and drives hyper's per-connection
//! graceful shutdown off it, so an idle keep-alive connection winds down promptly
//! instead of waiting out the listener's grace timeout.

use std::convert::Infallible;
use std::net::SocketAddr;
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
use tokio::sync::watch;

use hord_async::{AsyncHordStream, HordListener, SharedAsyncStream};
use hord_demo::{
    content_range, content_range_unsatisfied, parse_range, pattern_byte,
    pattern_fill_registered_from, RangeSpec,
};
use hord_stream::HordConfig;
use hord_zerocopy::{RdmaWriteReq, RdmaWriteStatus, SourcePool, HEADER};

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
    // One borrow-sound library call runs the whole §7.3/§7.7 server policy (the
    // too-large gate, split vs. plain, the zero-length 1-byte-source workaround) AND
    // the async write, then hands back the §7.4 status carrying `bytes_written` — so
    // the policy can't drift from the sync `serve_rdma_write`, and we only map the
    // outcome to HTTP here. `fill` is this demo's one server-side copy: the range's
    // bytes from their absolute object offset `base` (Milestone 3 removes it). Gated
    // on the range *length*, not the whole object size.
    match stream
        .serve_rdma_write_pooled(pool, req, len as u64, |src| pattern_fill_registered_from(src, base, len))
        .await
    {
        // Delivered. Keep the split/plain distinction (and the transfer id) in the
        // log so a §7.7 data-plane stall can be correlated server-side.
        Ok(status @ RdmaWriteStatus::Complete { bytes_written }) => {
            let code = if partial { 206 } else { 200 };
            match req.id.filter(|_| stream.split_mode_negotiated()) {
                Some(id) => eprintln!("[server] -> {code} (split: complete id={id} bytes_written={bytes_written})"),
                None => eprintln!("[server] -> {code} (zero-copy: complete bytes_written={bytes_written})"),
            }
            zc_response(code, status, range_hdr(&status))
        }
        // Object exceeds the client's buffer (§7.4): nothing was written.
        Ok(status @ RdmaWriteStatus::TooLarge { .. }) => {
            eprintln!("[server] -> 413 (zero-copy: {})", status.header_value());
            zc_response(413, status, None)
        }
        // `decide` never declines: declining is a host choice made *before* the call
        // (we serve the body on the stream instead). Matched explicitly so a new
        // RdmaWriteStatus variant is a compile error here, not a silent 200.
        Ok(RdmaWriteStatus::Declined) => unreachable!("serve_rdma_write never declines"),
        // A failed source registration or a mid-write transport error (the QP goes
        // to error and the connection closes); per §7.4/§7.7.7 we must not report
        // `complete`, so respond 500 (best-effort — no immediate was delivered).
        Err(e) => {
            eprintln!("[server] rdma write failed: {e}");
            respond(500, "text/plain", full(b"rdma write failed\n"), None)
        }
    }
}

/// The per-connection service the demo hands to [`HordListener`]: wrap the stream
/// in a [`SharedAsyncStream`] so the request handler can reach it for a zero-copy
/// RDMA write while hyper owns it for HTTP, give it a per-connection source pool,
/// and serve HTTP/1.1. This is the seam a host (e.g. Carapace) plugs into — the
/// listener owns the thread topology; this only describes one connection.
///
/// `shutdown` is the third argument [`HordListener`] hands every connection: a
/// clone of the listener's shutdown signal. We drive hyper's per-connection
/// graceful shutdown off it — without this an idle keep-alive connection (parked in
/// hyper waiting for the next request) would not wind down until the client closed
/// or the listener's `grace_timeout` (30 s) elapsed, so Ctrl-C with an idle client
/// would appear to hang. With it, the in-flight response (if any) finishes and the
/// keep-alive loop ends promptly. HORD's grace timeout stays the backstop.
///
/// (The HORD handshake already completed inside the listener's
/// `AsyncHordStream::from_accepted`, on this same worker thread — see the
/// `HordListener` "synchronous handshake" caveat.)
async fn serve_connection(stream: AsyncHordStream, peer: SocketAddr, mut shutdown: watch::Receiver<bool>) {
    let shared = SharedAsyncStream::new(stream);
    let handle = shared.clone();
    // One lazy source pool per connection (MRs are PD-scoped); cloned into the
    // request handler (an Rc bump). Reused across this connection's zero-copy
    // responses — the win shows on split / keep-alive (many per connection).
    let pool = SourcePool::new(SOURCE_POOL_CAP, SOURCE_POOL_BUF_SIZE);
    let conn = http1::Builder::new().serve_connection(
        TokioIo::new(shared),
        service_fn(move |req| serve(req, handle.clone(), pool.clone())),
    );
    tokio::pin!(conn);

    // Drive the connection, but break out the moment the listener signals shutdown
    // so we can ask hyper to wind this connection down gracefully instead of parking
    // for the next keep-alive request. Honour an already-set signal (a connection
    // accepted just as shutdown fired) without waiting for a `changed()` edge.
    let mut winding_down = *shutdown.borrow();
    while !winding_down {
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(e) = res {
                    eprintln!("[server] connection error ({peer}): {e}");
                }
                return;
            }
            res = shutdown.changed() => {
                // A flip to `true` (or a dropped sender, `Err`) means wind down; a
                // spurious toggle-back to `false` is ignored (keep serving).
                winding_down = res.is_err() || *shutdown.borrow();
            }
        }
    }
    // Shutdown signalled: let hyper finish any in-flight response and stop the
    // keep-alive loop, then drive the connection to completion.
    conn.as_mut().graceful_shutdown();
    if let Err(e) = conn.as_mut().await {
        eprintln!("[server] connection error ({peer}): {e}");
    }
}

fn main() -> ExitCode {
    // A `log` backend so HordListener's diagnostics (accept errors, handshake
    // failures, drain timeout, "all workers unavailable") reach stderr; without
    // one they would be silently dropped. RUST_LOG overrides the default level.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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
    // HordListener owns the whole thread-per-core topology (accept loop + worker
    // pool, each worker its own current-thread runtime/LocalSet + completion
    // domain) and the graceful-shutdown drain — see hord-async. The demo only
    // supplies the per-connection service (`serve_connection`).
    let mut listener = match HordListener::bind(&bind, port, config.clone()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[server] bind {bind}:{port} failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(n) = workers {
        listener = listener.workers(n);
    }
    eprintln!(
        "[server] listening on {bind}:{port} (async/hyper via HordListener, workers={}, zero_copy={})",
        workers.map_or_else(|| "auto".to_string(), |n| n.to_string()),
        config.zero_copy,
    );

    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[server] runtime build failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(async move {
        // Ctrl-C fires the listener's graceful-shutdown signal: it stops accepting
        // and drains in-flight connections before `serve` returns. This is the same
        // watch::Receiver<bool> a host (Carapace's ShutdownWatch) would hand in.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("[server] Ctrl-C received — draining in-flight connections…");
                let _ = shutdown_tx.send(true);
            }
        });
        listener.serve(shutdown_rx, serve_connection).await;
    });
    eprintln!("[server] shut down");
    ExitCode::SUCCESS
}
