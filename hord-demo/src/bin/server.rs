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
//! Connections are handled one at a time (the prototype transport is
//! single-connection); the server loops to serve the next client.

use std::io::{self, Write};
use std::process::ExitCode;

use hord_demo::{pattern_fill, read_head, Head};
use hord_stream::{HordConfig, HordStream, Listener};

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
        "[server] listening on {bind}:{port} (max_message_size={}, recv_pool={}, send_pool={})",
        config.max_message_size, config.recv_pool_size, config.send_pool_size
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

    if method != "GET" {
        return respond(stream, 405, "Method Not Allowed", b"only GET is supported\n", "text/plain");
    }

    if path == "/" {
        let body = b"HORD prototype server. Try GET /size/<bytes>.\n";
        return respond(stream, 200, "OK", body, "text/plain");
    }

    if let Some(rest) = path.strip_prefix("/size/") {
        match rest.parse::<usize>() {
            Ok(n) if n <= MAX_BODY => {
                let mut body = vec![0u8; n];
                pattern_fill(&mut body);
                return respond(stream, 200, "OK", &body, "application/octet-stream");
            }
            Ok(_) => {
                return respond(
                    stream,
                    413,
                    "Content Too Large",
                    b"size exceeds server limit\n",
                    "text/plain",
                );
            }
            Err(_) => {
                return respond(stream, 400, "Bad Request", b"bad size\n", "text/plain");
            }
        }
    }

    respond(stream, 404, "Not Found", b"not found\n", "text/plain")
}

fn respond(
    stream: &mut HordStream,
    status: u16,
    reason: &str,
    body: &[u8],
    content_type: &str,
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    // flush() blocks until every byte has been delivered to the peer's receive
    // buffers and acknowledged, so it is safe to drop/disconnect afterwards.
    stream.flush()?;
    eprintln!("[server] -> {status} {reason} ({} body bytes)", body.len());
    Ok(())
}
