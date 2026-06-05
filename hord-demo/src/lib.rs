//! Minimal HTTP/1.1 helpers for the HORD demo.
//!
//! This is deliberately tiny and not a general HTTP implementation — just
//! enough to prove that ordinary HTTP/1.1 request/response bytes flow
//! correctly over a [`hord_stream::HordStream`]. The point of HORD is that the
//! HTTP layer is unmodified and transport-agnostic; swapping this for `hyper`
//! (once an async stream wrapper exists) changes nothing below the socket.

use std::io::{self, Read};

/// Read bytes from `r` until the `\r\n\r\n` header terminator is seen.
/// Returns `(header_bytes, leftover)` where `leftover` is any body bytes that
/// were read past the terminator.
/// Maximum HTTP header section we will buffer before giving up. Bounds memory
/// against a peer that streams bytes but never sends the `\r\n\r\n` terminator
/// (RoCEv2 is unauthenticated — see CLAUDE.md).
pub const MAX_HEAD_BYTES: usize = 64 * 1024;

pub fn read_head<R: Read>(r: &mut R) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(idx) = find_double_crlf(&buf) {
            let body_start = idx + 4;
            let leftover = buf[body_start..].to_vec();
            buf.truncate(body_start);
            return Ok((buf, leftover));
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("HTTP header section exceeded {MAX_HEAD_BYTES} bytes"),
            ));
        }
        let got = r.read(&mut chunk)?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before end of HTTP headers",
            ));
        }
        buf.extend_from_slice(&chunk[..got]);
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Parsed request/response head (start line + headers).
#[derive(Debug)]
pub struct Head {
    /// The three space-separated tokens of the start line.
    pub start: (String, String, String),
    pub headers: Vec<(String, String)>,
}

impl Head {
    pub fn parse(bytes: &[u8]) -> io::Result<Head> {
        let text = String::from_utf8_lossy(bytes);
        let mut lines = text.split("\r\n").filter(|l| !l.is_empty());
        let start_line = lines
            .next()
            .ok_or_else(|| bad("empty HTTP head"))?
            .to_string();
        let mut it = start_line.splitn(3, ' ');
        let a = it.next().unwrap_or("").to_string();
        let b = it.next().unwrap_or("").to_string();
        let c = it.next().unwrap_or("").to_string();

        let mut headers = Vec::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        Ok(Head {
            start: (a, b, c),
            headers,
        })
    }

    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn content_length(&self) -> Option<usize> {
        self.header("Content-Length").and_then(|v| v.trim().parse().ok())
    }
}

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Read exactly `total` body bytes, given any `leftover` already in hand.
pub fn read_body<R: Read>(r: &mut R, leftover: Vec<u8>, total: usize) -> io::Result<Vec<u8>> {
    let mut body = leftover;
    body.reserve(total.saturating_sub(body.len()));
    let mut chunk = vec![0u8; 1 << 20]; // 1 MiB
    while body.len() < total {
        let want = (total - body.len()).min(chunk.len());
        let got = r.read(&mut chunk[..want])?;
        if got == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("connection closed after {}/{total} body bytes", body.len()),
            ));
        }
        body.extend_from_slice(&chunk[..got]);
    }
    Ok(body)
}

/// Deterministic, verifiable payload byte at position `i`. Used by the
/// `/size/<n>` test route so the client can check integrity end to end.
pub fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

/// Fill a buffer with [`pattern_byte`].
pub fn pattern_fill(buf: &mut [u8]) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = pattern_byte(i);
    }
}
