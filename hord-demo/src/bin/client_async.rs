//! HORD async demo client: one `hyper` HTTP/1.1 GET over the async RDMA stream.
//!
//! Usage:
//!   hord-client-async [--server <ip>] [--port <port>] [--path <path>] [--quiet]
//!
//! Mirrors the synchronous client: for a `/size/<n>` path it verifies the body
//! against the server's deterministic byte pattern, proving end-to-end integrity
//! through the async transport.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::rt::TokioIo;

use hord_async::AsyncHordStream;
use hord_demo::pattern_byte;
use hord_stream::HordConfig;

const DEFAULT_SERVER: &str = "77.40.251.67";
const DEFAULT_PORT: u16 = 4791;
const DEADLINE: Duration = Duration::from_secs(120); // bound the whole exchange (#11)

type BoxError = Box<dyn std::error::Error + Send + Sync>;

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
                    "usage: hord-client-async [--server <ip>] [--port <port>] [--path <path>] [--quiet]"
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
    match rt.block_on(local.run_until(run(server, port, path, quiet))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("[client] error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(server: String, port: u16, path: String, quiet: bool) -> Result<(), BoxError> {
    let config = HordConfig::default();
    if !quiet {
        eprintln!("[client] connecting to {server}:{port} ...");
    }
    let connect_start = Instant::now();
    let stream = AsyncHordStream::connect(&server, port, &config)?;
    if !quiet {
        eprintln!(
            "[client] connected in {:?} (payload capacity {} bytes/msg)",
            connect_start.elapsed(),
            stream.payload_capacity()
        );
    }

    // Low-level http1 handshake: a sender + a connection future we must drive.
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let conn_task = tokio::task::spawn_local(async move {
        if let Err(e) = conn.await {
            eprintln!("[client] connection task ended: {e}");
        }
    });

    let req = Request::builder()
        .uri(&path)
        .header("host", server.as_str())
        .header("user-agent", "hord-client-async/0.1")
        .header("connection", "close")
        .body(Empty::<Bytes>::new())?;

    let req_start = Instant::now();
    // Bound the request + full body read by a deadline: a stalled-but-alive peer
    // errors here rather than hanging forever (review item #11).
    let body = tokio::time::timeout(DEADLINE, async {
        let res = sender.send_request(req).await?;
        let status = res.status();
        if !quiet {
            eprintln!("[client] {:?} {status}", res.version());
        }
        let collected = res.into_body().collect().await?;
        Ok::<_, BoxError>((status, collected.to_bytes()))
    })
    .await
    .map_err(|_| -> BoxError { "request timed out".into() })??;
    let (status, body) = body;
    let elapsed = req_start.elapsed();

    // Integrity check for the /size/<n> route (only meaningful on a 200).
    let mut verified = false;
    if status.as_u16() == 200 && path.starts_with("/size/") {
        if let Some((i, &got)) = body.iter().enumerate().find(|(i, &b)| b != pattern_byte(*i)) {
            return Err(format!(
                "payload mismatch at byte {i}: got {got}, expected {}",
                pattern_byte(i)
            )
            .into());
        }
        verified = true;
    }

    // Dropping the sender lets the connection close; wait for its task to end.
    drop(sender);
    let _ = conn_task.await;

    let secs = elapsed.as_secs_f64();
    let mb = body.len() as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };

    println!("status:      {}", status);
    println!("body bytes:  {}", body.len());
    println!("elapsed:     {elapsed:?}");
    println!("throughput:  {throughput:.1} MiB/s");
    if verified {
        println!("integrity:   OK (byte pattern verified)");
    }
    Ok(())
}
