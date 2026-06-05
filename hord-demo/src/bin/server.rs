//! HORD demo server: a minimal HTTP/1.1 origin served over RDMA.
//!
//! Usage:
//!   hord-server [--bind <ip>] [--port <port>]
//!
//! Routes:
//!   GET /                -> a short text greeting
//!   GET /size/<n>        -> <n> bytes of a verifiable byte pattern
//!   (anything else)      -> 404
//!
//! Zero-copy (spec §7): if the client and server both advertised the capability
//! in the handshake and the request carries an `X-HORD-RDMA-Write` header, a
//! `GET /size/<n>` body is delivered by a one-sided RDMA write straight into the
//! client's buffer; the HTTP response then carries `Content-Length: 0` and
//! `X-HORD-RDMA-Write: status=complete;bytes_written=<n>`. The body never
//! touches the stream. Other body responses to a zero-copy request echo
//! `status=declined` (the body follows on the stream as usual).
//!
//! Connections are handled one at a time (the prototype transport is
//! single-connection); the server loops to serve the next client.

use std::io::{self, Write};
use std::process::ExitCode;

use hord_demo::{pattern_fill, pattern_fill_registered, read_head, Head};
use hord_stream::{HordConfig, HordStream, Listener};
use hord_zerocopy::{serve_rdma_write, RdmaWriteReq, RdmaWriteStatus, HEADER};

const DEFAULT_BIND: &str = "77.40.251.67"; // rxe0 / enp14s0 (see CLAUDE.md)
const DEFAULT_PORT: u16 = 4791;
const MAX_BODY: usize = 1usize << 30; // 1 GiB guard on /size/<n>

fn main() -> ExitCode {
    let mut bind = DEFAULT_BIND.to_string();
    let mut port = DEFAULT_PORT;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => bind = args.next().unwrap_or(bind),
            "--port" => {
                port = args
                    .next()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(port)
            }
            "-h" | "--help" => {
                eprintln!("usage: hord-server [--bind <ip>] [--port <port>]");
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
        "[server] listening on {bind}:{port} (max_message_size={}, recv_pool={}, send_pool={}, zero_copy={})",
        config.max_message_size, config.recv_pool_size, config.send_pool_size, config.zero_copy
    );

    loop {
        match HordStream::accept(&listener, &config) {
            Ok(mut stream) => {
                if let Err(e) = serve_one(&mut stream) {
                    eprintln!("[server] connection error: {e}");
                }
                // Dropping the stream flushes nothing implicitly; serve_one is
                // responsible for flush() before returning. Drop disconnects.
            }
            Err(e) => {
                eprintln!("[server] accept failed: {e}");
                // Keep serving; a single failed handshake shouldn't kill us.
            }
        }
    }
}

fn serve_one(stream: &mut HordStream) -> io::Result<()> {
    let (head_bytes, _leftover) = read_head(stream)?;
    let head = Head::parse(&head_bytes)?;
    let (method, path, version) = &head.start;
    eprintln!("[server] {method} {path} {version}");

    // A zero-copy request we will honour: the capability was negotiated and the
    // request carries a well-formed X-HORD-RDMA-Write header.
    let zc_req = if stream.zero_copy_negotiated() {
        head.header(HEADER).and_then(RdmaWriteReq::parse)
    } else {
        None
    };
    // §7.4 "header presence": any *body-bearing* response to a request that
    // carried the header must echo a status. When we don't use zero-copy, that
    // status is `declined`. (Bodiless responses would omit it, but every demo
    // response below carries a body.)
    let declined = if head.header(HEADER).is_some() {
        Some(RdmaWriteStatus::Declined)
    } else {
        None
    };

    if method != "GET" {
        return respond(stream, 405, "Method Not Allowed", b"only GET is supported\n", "text/plain", declined);
    }

    if path == "/" {
        let body = b"HORD prototype server. Try GET /size/<bytes>.\n";
        return respond(stream, 200, "OK", body, "text/plain", declined);
    }

    if let Some(rest) = path.strip_prefix("/size/") {
        match rest.parse::<usize>() {
            Ok(n) if n <= MAX_BODY => {
                if let Some(req) = zc_req {
                    return serve_size_zero_copy(stream, &req, n);
                }
                let mut body = vec![0u8; n];
                pattern_fill(&mut body);
                return respond(stream, 200, "OK", &body, "application/octet-stream", declined);
            }
            Ok(_) => {
                return respond(
                    stream,
                    413,
                    "Content Too Large",
                    b"size exceeds server limit\n",
                    "text/plain",
                    declined,
                );
            }
            Err(_) => {
                return respond(stream, 400, "Bad Request", b"bad size\n", "text/plain", declined);
            }
        }
    }

    respond(stream, 404, "Not Found", b"not found\n", "text/plain", declined)
}

/// Serve a `/size/<n>` body via a one-sided RDMA write into the client's
/// advertised buffer, then a bodiless HTTP response carrying the outcome.
fn serve_size_zero_copy(stream: &mut HordStream, req: &RdmaWriteReq, n: usize) -> io::Result<()> {
    // serve_rdma_write registers a source region, fills it with the pattern, and
    // RDMA-writes it into the client's [addr, rkey] — blocking until the write is
    // acknowledged before it returns. The HTTP response is sent *after*, so the
    // payload has provably landed by the time the client reads the head.
    let status = serve_rdma_write(stream, req, n as u64, |buf| pattern_fill_registered(buf, n))?;
    let (code, reason) = match status {
        RdmaWriteStatus::Complete { .. } => (200u16, "OK"),
        RdmaWriteStatus::TooLarge { .. } => (413, "Content Too Large"),
        // serve_rdma_write only ever returns Complete/TooLarge; declining is the
        // caller's choice not to call it at all (handled in serve_one).
        RdmaWriteStatus::Declined => unreachable!("serve_rdma_write never declines"),
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\n\
         Content-Type: application/octet-stream\r\n\
         Content-Length: 0\r\n\
         {}\r\n\
         Connection: close\r\n\
         \r\n",
        status.header_line()
    );
    stream.write_all(head.as_bytes())?;
    stream.flush()?;
    eprintln!("[server] -> {code} {reason} (zero-copy: {})", status.header_value());
    Ok(())
}

fn respond(
    stream: &mut HordStream,
    status: u16,
    reason: &str,
    body: &[u8],
    content_type: &str,
    zc: Option<RdmaWriteStatus>,
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n",
        body.len()
    );
    if let Some(zc) = zc {
        head.push_str(&zc.header_line());
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    // flush() blocks until every byte has been delivered to the peer's receive
    // buffers and acknowledged, so it is safe to drop/disconnect afterwards.
    stream.flush()?;
    eprintln!("[server] -> {status} {reason} ({} body bytes)", body.len());
    Ok(())
}
