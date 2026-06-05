//! HORD demo client: issue one HTTP/1.1 GET over RDMA and report the result.
//!
//! Usage:
//!   hord-client [--server <ip>] [--port <port>] [--path <path>] [--quiet]
//!
//! For a `/size/<n>` path the client verifies the body against the server's
//! deterministic byte pattern, proving end-to-end integrity through the
//! envelope framing, segmentation and reassembly.

use std::io::{self, Write};
use std::process::ExitCode;
use std::time::Instant;

use hord_demo::{pattern_byte, read_body, read_head, Head};
use hord_stream::{HordConfig, HordStream};

const DEFAULT_SERVER: &str = "77.40.251.67";
const DEFAULT_PORT: u16 = 4791;

fn main() -> ExitCode {
    let mut server = DEFAULT_SERVER.to_string();
    let mut port = DEFAULT_PORT;
    let mut path = "/".to_string();
    let mut quiet = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server = args.next().unwrap_or(server),
            "--port" => port = args.next().and_then(|p| p.parse().ok()).unwrap_or(port),
            "--path" => path = args.next().unwrap_or(path),
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                eprintln!(
                    "usage: hord-client [--server <ip>] [--port <port>] [--path <path>] [--quiet]"
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    match run(&server, port, &path, quiet) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("[client] error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(server: &str, port: u16, path: &str, quiet: bool) -> io::Result<()> {
    let config = HordConfig::default();
    if !quiet {
        eprintln!("[client] connecting to {server}:{port} ...");
    }
    let connect_start = Instant::now();
    let mut stream = HordStream::connect(server, port, &config)?;
    if !quiet {
        eprintln!(
            "[client] connected in {:?} (payload capacity {} bytes/msg)",
            connect_start.elapsed(),
            stream.payload_capacity()
        );
    }

    // Send the request.
    let request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {server}\r\n\
         User-Agent: hord-client/0.1\r\n\
         Connection: close\r\n\
         \r\n"
    );
    let req_start = Instant::now();
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    // Read the response head, then the body.
    let (head_bytes, leftover) = read_head(&mut stream)?;
    let head = Head::parse(&head_bytes)?;
    let (version, status, reason) = &head.start;
    let content_length = head.content_length().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "response lacked a Content-Length",
        )
    })?;

    if !quiet {
        eprintln!("[client] {version} {status} {reason}");
        eprintln!("[client] Content-Length: {content_length}");
    }

    let body = read_body(&mut stream, leftover, content_length)?;
    let elapsed = req_start.elapsed();

    // Integrity check for the /size/<n> route (only meaningful on a 200).
    let mut verified = None;
    if status == "200" && path.starts_with("/size/") {
        let mismatch = body.iter().enumerate().find(|(i, &b)| b != pattern_byte(*i));
        match mismatch {
            None => verified = Some(true),
            Some((i, &got)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "payload mismatch at byte {i}: got {got}, expected {}",
                        pattern_byte(i)
                    ),
                ));
            }
        }
    }

    stream.disconnect();

    let secs = elapsed.as_secs_f64();
    let mb = body.len() as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };

    println!("status:      {status} {reason}");
    println!("body bytes:  {}", body.len());
    println!("elapsed:     {elapsed:?}");
    println!("throughput:  {throughput:.1} MiB/s");
    if let Some(true) = verified {
        println!("integrity:   OK (byte pattern verified)");
    }
    Ok(())
}
