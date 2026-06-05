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
//! Concurrency model: a blocking acceptor loop hands each connection (the `Send`
//! `Connection` from `accept_begin`) to a fresh thread running a current-thread
//! Tokio runtime. The async stream is `!Send`, so it is built and driven on that
//! one thread. This serves many connections at once (one thread each) while the
//! data path itself never busy-polls.

use std::convert::Infallible;
use std::pin::Pin;
use std::process::ExitCode;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;

use hord_async::AsyncHordStream;
use hord_demo::pattern_byte;
use hord_stream::{HordConfig, HordStream, Listener};

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

fn respond(status: u16, content_type: &str, body: DemoBody) -> Response<DemoBody> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(body)
        .expect("valid response")
}

fn full(bytes: &'static [u8]) -> DemoBody {
    Full::new(Bytes::from_static(bytes)).boxed()
}

async fn serve(req: Request<Incoming>) -> Result<Response<DemoBody>, Infallible> {
    let path = req.uri().path();
    eprintln!("[server] {} {path}", req.method());

    if req.method() != Method::GET {
        return Ok(respond(405, "text/plain", full(b"only GET is supported\n")));
    }
    if path == "/" {
        return Ok(respond(
            200,
            "text/plain",
            full(b"HORD async server. Try GET /size/<bytes>.\n"),
        ));
    }
    if let Some(rest) = path.strip_prefix("/size/") {
        return Ok(match rest.parse::<usize>() {
            Ok(n) if n <= MAX_BODY => respond(
                200,
                "application/octet-stream",
                PatternBody::new(n).boxed(),
            ),
            Ok(_) => respond(413, "text/plain", full(b"size exceeds server limit\n")),
            Err(_) => respond(400, "text/plain", full(b"bad size\n")),
        });
    }
    Ok(respond(404, "text/plain", full(b"not found\n")))
}

/// Drive one accepted connection to completion on its own current-thread
/// runtime. hyper calls `poll_shutdown` on close, which flushes (waits for every
/// RDMA send to be acked) before disconnecting, so the response is fully
/// delivered.
fn serve_connection(conn: hord_stream::Connection, peer: Vec<u8>, config: &HordConfig) {
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
        let stream = match AsyncHordStream::from_accepted(conn, peer, config) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[server] handshake failed: {e}");
                return;
            }
        };
        if let Err(e) = http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service_fn(serve))
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
    eprintln!("[server] listening on {bind}:{port} (async/hyper, per-connection threads)");

    // Blocking accept loop: each accepted connection is handed (as the Send
    // `Connection`) to its own thread, which builds and runs the !Send stream.
    loop {
        match HordStream::accept_begin(&listener, &config) {
            Ok((conn, peer)) => {
                let config = config.clone();
                std::thread::spawn(move || serve_connection(conn, peer, &config));
            }
            Err(e) => eprintln!("[server] accept failed: {e}"),
        }
    }
}
