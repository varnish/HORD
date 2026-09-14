//! HORD demo client: issue one HTTP/1.1 GET over RDMA and report the result.
//!
//! Usage:
//!   hord-client [--server <ip>] [--port <port>] [--path <path>]
//!               [--zero-copy] [--zc-buf <bytes>] [--range <spec>] [--quiet]
//!
//! For a `/size/<n>` path the client verifies the body against the server's
//! deterministic byte pattern, proving end-to-end integrity through the
//! envelope framing, segmentation and reassembly.
//!
//! `--range <spec>` (spec §7.6) requests a single byte range of a `/size/<n>`
//! object — `<spec>` is `a-b`, `a-`, or `-n` — yielding `206 Partial Content`
//! (composed with `--zero-copy`), or `416` if it lies past the end. Delivered
//! bytes are verified at their absolute object offset.
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

use hord_demo::{
    read_body, read_head, resolve_range, size_from_path, verify_stream_body_at,
    verify_zero_copy_at, Head,
};
use hord_stream::{HordConfig, HordStream};
use hord_zerocopy::{RdmaWriteStatus, ZeroCopyRequest, HEADER};

const DEFAULT_SERVER: &str = "192.0.2.1"; // rxe device IP fallback; override via $HORD_TEST_IP or --server
const DEFAULT_PORT: u16 = 4791;
const DEFAULT_ZC_BUF: usize = 1 << 20; // 1 MiB, when the size isn't in the path

/// Progress reporting to stderr (stdout carries the machine-readable result),
/// silenced by `--quiet`. Exists so the protocol steps in `run` read as a
/// straight sequence — one unconditional `say!` per step — instead of an
/// `if !quiet { eprintln!(…) }` block wrapped around every message. (The async
/// client carries a copy: this is deliberately not in the `hord_demo` library,
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

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().unwrap_or(server),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--path" => path = args.next().unwrap_or(path),
            "--zero-copy" => zero_copy = true,
            "--zc-buf" => zc_buf = args.next().and_then(|n| n.parse().ok()),
            "--range" => range = args.next(),
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: hord-client [--server <ip>] [--port <port>] [--path <path>] \
                     [--zero-copy] [--zc-buf <bytes>] [--range <spec>] [--quiet]"
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    match run(&server, port, &path, zero_copy, zc_buf, range, quiet) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("[client] error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Where the response body came from, and what we could prove about it.
struct Delivery {
    body_len: usize,
    /// Human-readable delivery path, for the `delivery:` summary line.
    source: &'static str,
    /// True when the bytes were checked against the `/size/<n>` pattern.
    verified: bool,
}

fn run(
    server: &str,
    port: u16,
    path: &str,
    zero_copy: bool,
    zc_buf: Option<usize>,
    range: Option<String>,
    quiet: bool,
) -> io::Result<()> {
    let report = Report::new(quiet);
    let config = HordConfig::default();
    say!(report, "[client] connecting to {server}:{port} ...");
    let connect_start = Instant::now();
    let mut stream = HordStream::connect(server, port, &config)?;
    say!(
        report,
        "[client] connected in {:?} (payload capacity {} bytes/msg, zero_copy_negotiated={})",
        connect_start.elapsed(),
        stream.payload_capacity(),
        stream.zero_copy_negotiated()
    );

    // §7.6: resolve the requested range locally, so the destination buffer is
    // sized to the range (not the whole object) and the delivered bytes are
    // verified at their absolute object offset.
    let total = size_from_path(path);
    let (range_base, range_len) = resolve_range(range.as_deref(), total);
    let capacity = zc_buf.or(range_len).or(total).unwrap_or(DEFAULT_ZC_BUF);
    let zc = offer_zero_copy(&stream, zero_copy, capacity, &report)?;

    let request = build_request(server, path, range.as_deref(), zc.as_ref());
    let req_start = Instant::now();
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let (head_bytes, leftover) = read_head(&mut stream)?;
    let head = Head::parse(&head_bytes)?;
    let (version, status, reason) = &head.start;
    say!(report, "[client] {version} {status} {reason}");

    // §7.6: an unsatisfiable range → 416 with `Content-Range: bytes */total` and
    // no body. Nothing to verify; report and finish.
    if status == "416" {
        drop(stream);
        println!("status:      {status} {reason}");
        println!("delivery:    none (range not satisfiable)");
        println!(
            "content-range: {}",
            head.header("Content-Range").unwrap_or("(none)")
        );
        return Ok(());
    }

    let delivery = receive_body(
        &mut stream,
        &head,
        leftover,
        zc.as_ref(),
        range_base,
        path,
        &report,
    )?;
    let elapsed = req_start.elapsed();

    // Drop the stream (which destroys the QP — stopping the NIC) BEFORE `zc`'s
    // destination buffer is dropped at end of scope, so the MR is deregistered
    // only after no DMA can target it. The payload was already read out above.
    drop(stream);

    print_summary(
        &format!("{status} {reason}"),
        head.header("Content-Range"),
        &delivery,
        elapsed,
    );
    Ok(())
}

/// Register the zero-copy destination buffer, if we asked for zero-copy *and*
/// the peer negotiated it. Returns `None` — after saying why — when either half
/// is missing, which is the signal to take the ordinary stream body instead.
fn offer_zero_copy(
    stream: &HordStream,
    requested: bool,
    capacity: usize,
    report: &Report,
) -> io::Result<Option<ZeroCopyRequest>> {
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
        let req = ZeroCopyRequest::new(stream, capacity)?;
        say!(
            report,
            "[client] zero-copy: advertising a {capacity}-byte buffer"
        );
        return Ok(Some(req));
    };
    say!(
        report,
        "[client] --zero-copy requested but {why}; using the stream"
    );
    Ok(None)
}

/// The GET, carrying `Range` (§7.6) and `X-HORD-RDMA-Write` (§7.2) when offered.
fn build_request(
    server: &str,
    path: &str,
    range: Option<&str>,
    zc: Option<&ZeroCopyRequest>,
) -> String {
    let mut request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {server}\r\n\
         User-Agent: hord-client/0.1\r\n\
         Connection: close\r\n"
    );
    if let Some(spec) = range {
        request.push_str(&format!("Range: bytes={spec}\r\n"));
    }
    if let Some(zc) = zc {
        request.push_str(&zc.header_line());
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request
}

/// Take delivery of the body and verify it at `range_base`, its absolute offset
/// in the object (0 for a whole object).
///
/// Which of the two paths ran is decided by the server's `X-HORD-RDMA-Write`
/// response status (§7.3): `complete` means the bytes are already in our
/// registered buffer and nothing came over the stream; anything else —
/// `declined`, `too_large`, malformed, or no zero-copy at all — means an
/// ordinary `Content-Length`-framed body to read.
fn receive_body(
    stream: &mut HordStream,
    head: &Head,
    leftover: Vec<u8>,
    zc: Option<&ZeroCopyRequest>,
    range_base: usize,
    path: &str,
    report: &Report,
) -> io::Result<Delivery> {
    let to_io = |m: String| io::Error::new(io::ErrorKind::InvalidData, m);
    let zc_status = zc.and(head.header(HEADER)).and_then(RdmaWriteStatus::parse);
    match zc_status {
        Some(RdmaWriteStatus::Complete { bytes_written }) => {
            let n = bytes_written as usize;
            let zc = zc.expect("zc set when status parsed");
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
            let verified = verify_zero_copy_at(zc, range_base, n, path).map_err(to_io)?;
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
        // Declined, malformed, or no zero-copy: read the body off the stream.
        _ => {
            let content_length = head.content_length().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response lacked a Content-Length",
                )
            })?;
            say!(report, "[client] Content-Length: {content_length}");
            let body = read_body(stream, leftover, content_length)?;
            let status = &head.start.1;
            let verified =
                verify_stream_body_at(&body, status == "200" || status == "206", path, range_base)
                    .map_err(to_io)?;
            Ok(Delivery {
                body_len: body.len(),
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
    elapsed: std::time::Duration,
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
