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
//! Concurrency model: a blocking acceptor loop hands each connection (the `Send`
//! `Connection` from `accept_begin`) to a fresh thread running a current-thread
//! Tokio runtime. The async stream is `!Send`, so it is built and driven on that
//! one thread.

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

use hord_async::{AsyncHordStream, SharedAsyncStream};
use hord_demo::{pattern_byte, pattern_fill_registered};
use hord_stream::{HordConfig, HordStream, Listener};
use hord_zerocopy::{RdmaWriteReq, RdmaWriteStatus, HEADER};

const DEFAULT_BIND: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const DEFAULT_PORT: u16 = 4791;
const MAX_BODY: usize = 1usize << 30; // 1 GiB guard on /size/<n>
const CHUNK: usize = 256 * 1024; // streamed body chunk size

type DemoBody = BoxBody<Bytes, Infallible>;

/// A `/size/<n>` response body generated on the fly in [`CHUNK`]-sized frames,
/// so the server never materialises the whole body (review item #14). Each frame
/// carries the same verifiable pattern the client checks.
struct PatternBody {
    offset: usize,
    total: usize,
}

impl PatternBody {
    fn new(total: usize) -> Self {
        PatternBody { offset: 0, total }
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
        if this.offset >= this.total {
            return Poll::Ready(None);
        }
        let n = CHUNK.min(this.total - this.offset);
        let mut buf = vec![0u8; n];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = pattern_byte(this.offset + i);
        }
        this.offset += n;
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(buf)))))
    }

    fn is_end_stream(&self) -> bool {
        self.offset >= self.total
    }

    fn size_hint(&self) -> SizeHint {
        // Exact size -> hyper emits a Content-Length (not chunked encoding).
        SizeHint::with_exact((self.total - self.offset) as u64)
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
/// the payload travelled out-of-band via RDMA write.
fn zc_response(status_code: u16, zc: RdmaWriteStatus) -> Response<DemoBody> {
    Response::builder()
        .status(status_code)
        .header("content-type", "application/octet-stream")
        .header(HEADER, zc.header_value())
        .body(empty())
        .expect("valid response")
}

fn full(bytes: &'static [u8]) -> DemoBody {
    Full::new(Bytes::from_static(bytes)).boxed()
}

fn empty() -> DemoBody {
    Empty::<Bytes>::new().boxed()
}

async fn serve(req: Request<Incoming>, stream: SharedAsyncStream) -> Result<Response<DemoBody>, Infallible> {
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
                if let Some(req) = zc_req {
                    serve_zero_copy(&stream, &req, n).await
                } else {
                    respond(200, "application/octet-stream", PatternBody::new(n).boxed(), declined)
                }
            }
            Ok(_) => respond(413, "text/plain", full(b"size exceeds server limit\n"), declined),
            Err(_) => respond(400, "text/plain", full(b"bad size\n"), declined),
        });
    }
    Ok(respond(404, "text/plain", full(b"not found\n"), declined))
}

/// Deliver a `/size/<n>` body by RDMA-writing it into the client's buffer, then
/// return a bodiless response with the outcome. The write is awaited before the
/// response is built, so the payload has landed by the time the client reads it.
///
/// In split mode (§7.7) — the request carries an `id` and we negotiated it — the
/// body is delivered with RDMA write-with-immediate, signalling the data plane on
/// the client's CQ; otherwise a plain one-sided write. The HTTP response is the
/// same either way (split vs. plain is purely the delivery mechanism, §7.7.4).
async fn serve_zero_copy(stream: &SharedAsyncStream, req: &RdmaWriteReq, n: usize) -> Response<DemoBody> {
    if n as u64 > req.len {
        eprintln!("[server] -> 413 (zero-copy: too_large object_size={n})");
        return zc_response(413, RdmaWriteStatus::TooLarge { object_size: n as u64 });
    }
    // Use split mode only if the client asked (id present) and we negotiated it
    // (§7.7.3); otherwise the id is ignored and a plain write is used.
    let split_id = req.id.filter(|_| stream.split_mode_negotiated());

    if let Some(id) = split_id {
        // On the *success* path the immediate is delivered (so the data plane's
        // posted transfer credit is consumed and its poll returns) — even for an
        // empty body, backed by a 1-byte source (the WR still writes 0 bytes).
        // NOTE: the 413/too_large early return above and the register_source-500
        // below do NOT deliver an immediate; per §7.7.7 those are control-plane
        // fallbacks the client reconciles via the HTTP status, and a data-plane
        // consumer must bound its wait with a timeout (§7.7.7) rather than assume
        // every request yields a CQ completion.
        let src = match stream.register_source(n.max(1)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[server] register_source failed: {e}");
                return respond(500, "text/plain", full(b"internal error\n"), None);
            }
        };
        if n > 0 {
            pattern_fill_registered(&src, n);
        }
        return match stream.rdma_write_with_imm(&src, 0, req.addr, req.rkey, n, id).await {
            Ok(()) => {
                eprintln!("[server] -> 200 (split: complete id={id} bytes_written={n})");
                zc_response(200, RdmaWriteStatus::Complete { bytes_written: n as u64 })
            }
            Err(e) => {
                eprintln!("[server] rdma_write_with_imm failed: {e}");
                respond(500, "text/plain", full(b"rdma write failed\n"), None)
            }
        };
    }

    // --- plain zero-copy write (§7.3) ---
    if n == 0 {
        // Nothing to place; a zero-length MR is not portable, so short-circuit
        // (matching the sync hord_zerocopy::serve_rdma_write path) rather than
        // calling register_source(0)/ibv_reg_mr(.., 0, ..).
        return zc_response(200, RdmaWriteStatus::Complete { bytes_written: 0 });
    }
    let src = match stream.register_source(n) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[server] register_source failed: {e}");
            return respond(500, "text/plain", full(b"internal error\n"), None);
        }
    };
    pattern_fill_registered(&src, n);
    match stream.rdma_write(&src, 0, req.addr, req.rkey, n).await {
        Ok(()) => {
            eprintln!("[server] -> 200 (zero-copy: complete bytes_written={n})");
            zc_response(200, RdmaWriteStatus::Complete { bytes_written: n as u64 })
        }
        Err(e) => {
            // Mid-write failure (§7.4): the QP is in error, the connection will
            // close. Returning anything is best-effort.
            eprintln!("[server] rdma_write failed: {e}");
            respond(500, "text/plain", full(b"rdma write failed\n"), None)
        }
    }
}

/// Drive one accepted connection to completion on its own current-thread runtime.
/// The stream is wrapped in a [`SharedAsyncStream`] so the request handler can
/// reach it to perform a zero-copy RDMA write while hyper owns it for HTTP.
fn serve_connection(conn: hord_stream::Connection, config: &HordConfig) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[server] runtime build failed: {e}");
            return;
        }
    };
    rt.block_on(async move {
        let stream = match AsyncHordStream::from_accepted(conn, config) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[server] handshake failed: {e}");
                return;
            }
        };
        let shared = SharedAsyncStream::new(stream);
        let handle = shared.clone();
        if let Err(e) = http1::Builder::new()
            .serve_connection(
                TokioIo::new(shared),
                service_fn(move |req| serve(req, handle.clone())),
            )
            .await
        {
            eprintln!("[server] connection error: {e}");
        }
    });
}

fn main() -> ExitCode {
    let mut bind = DEFAULT_BIND.to_string();
    let mut port = DEFAULT_PORT;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = args.next().unwrap_or(bind),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "-h" | "--help" => {
                eprintln!("usage: hord-server-async [--bind <ip>] [--port <port>]");
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
    eprintln!(
        "[server] listening on {bind}:{port} (async/hyper, per-connection threads, zero_copy={})",
        config.zero_copy
    );

    // Blocking accept loop: each accepted connection is handed (as the Send
    // `Connection`) to its own thread, which builds and runs the !Send stream.
    // accept_begin migrates each accepted connection to its own CM event channel
    // (Identifier::migrate / rdma_migrate_id), so this looping acceptor and the
    // per-connection workers never compete on a shared channel. (migrate is a
    // local sideway patch — see vendor/sideway/HORD-PATCH.md.)
    loop {
        match HordStream::accept_begin(&listener, &config) {
            Ok(conn) => {
                let config = config.clone();
                std::thread::spawn(move || serve_connection(conn, &config));
            }
            Err(e) => eprintln!("[server] accept failed: {e}"),
        }
    }
}
