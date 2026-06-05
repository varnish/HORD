//! HORD demo client: issue one HTTP/1.1 GET over RDMA and report the result.
//!
//! Usage:
//!   hord-client [--server <ip>] [--port <port>] [--path <path>]
//!               [--zero-copy] [--zc-buf <bytes>] [--quiet]
//!
//! For a `/size/<n>` path the client verifies the body against the server's
//! deterministic byte pattern, proving end-to-end integrity through the
//! envelope framing, segmentation and reassembly.
//!
//! With `--zero-copy` (and a server that negotiated the capability), the client
//! registers a destination buffer, advertises it via `X-HORD-RDMA-Write`, and —
//! on `status=complete` — reads the body straight out of that buffer (the server
//! placed it there by RDMA write; nothing came over the stream). It falls back
//! to the stream body on `status=declined` / `too_large`. `--zc-buf` overrides
//! the destination size (default: the `/size/<n>` value), e.g. to force a
//! `too_large` outcome.

use std::io::{self, Write};
use std::process::ExitCode;
use std::time::Instant;

use hord_demo::{read_body, read_head, size_from_path, verify_stream_body, verify_zero_copy, Head};
use hord_stream::{HordConfig, HordStream};
use hord_zerocopy::{RdmaWriteStatus, ZeroCopyRequest, HEADER};

const DEFAULT_SERVER: &str = "77.40.251.67";
const DEFAULT_PORT: u16 = 4791;
const DEFAULT_ZC_BUF: usize = 1 << 20; // 1 MiB, when the size isn't in the path

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
                    "usage: hord-client [--server <ip>] [--port <port>] [--path <path>] \
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

    match run(&server, port, &path, zero_copy, zc_buf, quiet) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("[client] error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(
    server: &str,
    port: u16,
    path: &str,
    zero_copy: bool,
    zc_buf: Option<usize>,
    quiet: bool,
) -> io::Result<()> {
    let config = HordConfig::default();
    if !quiet {
        eprintln!("[client] connecting to {server}:{port} ...");
    }
    let connect_start = Instant::now();
    let mut stream = HordStream::connect(server, port, &config)?;
    if !quiet {
        eprintln!(
            "[client] connected in {:?} (payload capacity {} bytes/msg, zero_copy_negotiated={})",
            connect_start.elapsed(),
            stream.payload_capacity(),
            stream.zero_copy_negotiated()
        );
    }

    // Offer zero-copy only if both we asked and the peer negotiated it. Register
    // the destination buffer up front so its address/rkey can ride in the GET.
    let capacity = zc_buf.or_else(|| size_from_path(path)).unwrap_or(DEFAULT_ZC_BUF);
    let zc = if zero_copy && stream.zero_copy_negotiated() && capacity > 0 {
        let req = ZeroCopyRequest::new(&stream, capacity)?;
        if !quiet {
            eprintln!("[client] zero-copy: advertising a {capacity}-byte buffer");
        }
        Some(req)
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

    // Build and send the request (adding the zero-copy header when offered).
    let mut request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {server}\r\n\
         User-Agent: hord-client/0.1\r\n\
         Connection: close\r\n"
    );
    if let Some(zc) = &zc {
        request.push_str(&zc.header_line());
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    let req_start = Instant::now();
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    // Read the response head.
    let (head_bytes, leftover) = read_head(&mut stream)?;
    let head = Head::parse(&head_bytes)?;
    let (version, status, reason) = &head.start;
    if !quiet {
        eprintln!("[client] {version} {status} {reason}");
    }

    // Interpret the zero-copy response header, if we offered zero-copy.
    let zc_status = zc.as_ref().and(head.header(HEADER)).and_then(RdmaWriteStatus::parse);

    let to_io = |m: String| io::Error::new(io::ErrorKind::InvalidData, m);
    let (body_len, delivery, verified) = match zc_status {
        Some(RdmaWriteStatus::Complete { bytes_written }) => {
            let n = bytes_written as usize;
            let zc = zc.as_ref().expect("zc set when status parsed");
            // We trust the peer's bytes_written only as far as our own buffer: a
            // conforming server never reports more than it wrote (≤ our advertised
            // len), and the bound keeps copy_out in range regardless. (RoCEv2 is
            // unauthenticated; a real consumer would also confirm the transfer.)
            if n > zc.capacity() {
                return Err(to_io(format!(
                    "server reported bytes_written={n} > buffer {}",
                    zc.capacity()
                )));
            }
            // The body is already in our buffer — verify it in place.
            let verified = verify_zero_copy(zc, n, path).map_err(to_io)?;
            (n, "zero-copy (RDMA write)", verified)
        }
        Some(RdmaWriteStatus::TooLarge { object_size }) => {
            if !quiet {
                eprintln!("[client] zero-copy declined: object_size={object_size} exceeds our buffer");
            }
            (0, "none (too_large)", false)
        }
        // Declined, malformed, or no zero-copy: read the body off the stream.
        _ => {
            let content_length = head.content_length().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "response lacked a Content-Length")
            })?;
            if !quiet {
                eprintln!("[client] Content-Length: {content_length}");
            }
            let body = read_body(&mut stream, leftover, content_length)?;
            let verified = verify_stream_body(&body, status == "200", path).map_err(to_io)?;
            (body.len(), "stream", verified)
        }
    };
    let elapsed = req_start.elapsed();

    // Drop the stream (which destroys the QP — stopping the NIC) BEFORE `zc`'s
    // destination buffer is dropped at end of scope, so the MR is deregistered
    // only after no DMA can target it. The payload was already read out above.
    drop(stream);

    let secs = elapsed.as_secs_f64();
    let mb = body_len as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };

    println!("status:      {status} {reason}");
    println!("delivery:    {delivery}");
    println!("body bytes:  {body_len}");
    println!("elapsed:     {elapsed:?}");
    println!("throughput:  {throughput:.1} MiB/s");
    if verified {
        println!("integrity:   OK (byte pattern verified)");
    }
    Ok(())
}
