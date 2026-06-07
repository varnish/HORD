//! Minimal HTTP/1.1 helpers for the HORD demo.
//!
//! This is deliberately tiny and not a general HTTP implementation — just
//! enough to prove that ordinary HTTP/1.1 request/response bytes flow
//! correctly over a [`hord_stream::HordStream`]. The point of HORD is that the
//! HTTP layer is unmodified and transport-agnostic; swapping this for `hyper`
//! (once an async stream wrapper exists) changes nothing below the socket.

use std::io::{self, Read};

use hord_stream::RegisteredBuffer;
use hord_zerocopy::ZeroCopyRequest;

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

// ---- HTTP Range / Content-Range (spec §7.6, profile §4.1.1) ------------------

/// Outcome of resolving a single HTTP `Range` request header against a known
/// object size. HORD supports single-range requests only (spec §4.1.1);
/// multipart byteranges are out of scope (§4.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeSpec {
    /// A satisfiable byte range, inclusive: bytes `[start, end]` → `206`.
    Range { start: usize, end: usize },
    /// A well-formed range entirely past the end of the object → `416`.
    Unsatisfiable,
    /// No usable single range — header absent, malformed, a non-`bytes` unit, or
    /// multiple ranges (no multipart). The server MAY ignore `Range` (RFC 9110)
    /// and serve the whole object (`200`).
    Full,
}

/// Parse a single-range `Range` header *value* (e.g. `bytes=0-499`) against an
/// object of `total` bytes. Forms (RFC 9110 §14.1.2): `a-b` (clamps `b` to
/// `total-1`), `a-` (to the end), `-n` (last `n` bytes). A comma (multiple
/// ranges), a non-`bytes` unit, or any malformed token yields [`RangeSpec::Full`]
/// so the caller serves the whole object. See [`RangeSpec`] for the outcomes.
pub fn parse_range(value: &str, total: usize) -> RangeSpec {
    let spec = match value.trim().strip_prefix("bytes=") {
        Some(s) => s.trim(),
        None => return RangeSpec::Full, // unknown / missing unit → ignore
    };
    if spec.contains(',') {
        return RangeSpec::Full; // multiple ranges → multipart, unsupported
    }
    let (a, b) = match spec.split_once('-') {
        Some((a, b)) => (a.trim(), b.trim()),
        None => return RangeSpec::Full,
    };
    match (a.is_empty(), b.is_empty()) {
        // "-n": the last n bytes.
        (true, false) => {
            let n: usize = match b.parse() {
                Ok(n) => n,
                Err(_) => return RangeSpec::Full,
            };
            if n == 0 || total == 0 {
                return RangeSpec::Unsatisfiable;
            }
            RangeSpec::Range { start: total.saturating_sub(n), end: total - 1 }
        }
        // "a-": from a to the end.
        (false, true) => {
            let start: usize = match a.parse() {
                Ok(s) => s,
                Err(_) => return RangeSpec::Full,
            };
            if start >= total {
                return RangeSpec::Unsatisfiable;
            }
            RangeSpec::Range { start, end: total - 1 }
        }
        // "a-b": explicit inclusive range.
        (false, false) => {
            let (start, last): (usize, usize) = match (a.parse(), b.parse()) {
                (Ok(s), Ok(e)) => (s, e),
                _ => return RangeSpec::Full,
            };
            if start > last {
                return RangeSpec::Full; // invalid ordering → ignore
            }
            if start >= total {
                return RangeSpec::Unsatisfiable;
            }
            RangeSpec::Range { start, end: last.min(total - 1) }
        }
        (true, true) => RangeSpec::Full, // bare "-"
    }
}

/// Format a satisfied `Content-Range` value: `bytes start-end/total` (for a
/// `206` response).
pub fn content_range(start: usize, end: usize, total: usize) -> String {
    format!("bytes {start}-{end}/{total}")
}

/// Format an unsatisfied `Content-Range` value: `bytes */total` (for a `416`).
pub fn content_range_unsatisfied(total: usize) -> String {
    format!("bytes */{total}")
}

/// Parse a `Content-Range: bytes start-end/total` *value* from a `206` response,
/// returning `(start, end, total)`. Returns `None` for the unsatisfied form
/// (`bytes */total`) or anything malformed.
pub fn parse_content_range(value: &str) -> Option<(usize, usize, usize)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.trim().split_once('-')?;
    Some((
        start.trim().parse().ok()?,
        end.trim().parse().ok()?,
        total.trim().parse().ok()?,
    ))
}

/// Deterministic, verifiable payload byte at position `i`. Used by the
/// `/size/<n>` test route so the client can check integrity end to end.
pub fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

/// Fill a buffer so that byte `i` is [`pattern_byte`]`(base + i)` — the slice
/// represents the object starting at absolute offset `base` (for a range
/// response). [`pattern_fill`] is the whole-object (`base == 0`) case.
pub fn pattern_fill_from(buf: &mut [u8], base: usize) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = pattern_byte(base + i);
    }
}

/// Fill a buffer with [`pattern_byte`] from offset 0.
pub fn pattern_fill(buf: &mut [u8]) {
    pattern_fill_from(buf, 0);
}

/// Fill the first `n` bytes of a registered buffer so that byte `i` is
/// [`pattern_byte`]`(base + i)`, in bounded chunks via raw copies into the
/// registered memory (never forming a `&mut [u8]` over it). Used to populate a
/// zero-copy RDMA-write source region — `base` is the object offset of a range
/// response (0 for a whole object, see [`pattern_fill_registered`]).
pub fn pattern_fill_registered_from(buf: &RegisteredBuffer, base: usize, n: usize) {
    const CHUNK: usize = 256 * 1024;
    let mut tmp = vec![0u8; CHUNK.min(n.max(1))];
    let mut off = 0;
    while off < n {
        let take = CHUNK.min(n - off);
        for (i, b) in tmp[..take].iter_mut().enumerate() {
            *b = pattern_byte(base + off + i);
        }
        buf.copy_in(off, &tmp[..take]);
        off += take;
    }
}

/// Fill the first `n` bytes of a registered buffer with [`pattern_byte`] from
/// offset 0 (whole-object zero-copy source).
pub fn pattern_fill_registered(buf: &RegisteredBuffer, n: usize) {
    pattern_fill_registered_from(buf, 0, n);
}

/// Parse the `<n>` from a `/size/<n>` request path, if that is the route.
pub fn size_from_path(path: &str) -> Option<usize> {
    path.strip_prefix("/size/").and_then(|n| n.parse().ok())
}

/// Verify the first `n` bytes of a zero-copy destination buffer against
/// [`pattern_byte`] starting at absolute offset `base` (buffer byte `i` must
/// equal `pattern_byte(base + i)`) — for a range response `base` is the range
/// start. Reads out in bounded chunks (the consumer reading its own buffer — not
/// a transport copy). Same return convention as [`verify_zero_copy`].
pub fn verify_zero_copy_at(zc: &ZeroCopyRequest, base: usize, n: usize, path: &str) -> Result<bool, String> {
    if size_from_path(path).is_none() {
        return Ok(false);
    }
    const CHUNK: usize = 256 * 1024;
    let mut tmp = vec![0u8; CHUNK.min(n.max(1))];
    let mut off = 0;
    while off < n {
        let take = CHUNK.min(n - off);
        zc.copy_out(off, &mut tmp[..take]);
        for (i, &got) in tmp[..take].iter().enumerate() {
            let want = pattern_byte(base + off + i);
            if got != want {
                return Err(format!("payload mismatch at byte {}: got {got}, expected {want}", base + off + i));
            }
        }
        off += take;
    }
    Ok(true)
}

/// Verify a whole-object zero-copy buffer against [`pattern_byte`] (`base == 0`).
/// `Ok(true)` = verified, `Ok(false)` = not a `/size/` route so there's nothing
/// to check, `Err(msg)` = mismatch. Shared by the sync and async demo clients.
pub fn verify_zero_copy(zc: &ZeroCopyRequest, n: usize, path: &str) -> Result<bool, String> {
    verify_zero_copy_at(zc, 0, n, path)
}

/// Verify a stream-delivered body against [`pattern_byte`] starting at absolute
/// offset `base` (body byte `i` must equal `pattern_byte(base + i)`); `base` is
/// the range start for a `206` response. `is_success` is true for a 200 or 206.
/// Same return convention as [`verify_zero_copy`].
pub fn verify_stream_body_at(body: &[u8], is_success: bool, path: &str, base: usize) -> Result<bool, String> {
    if !is_success || size_from_path(path).is_none() {
        return Ok(false);
    }
    if let Some((i, &got)) = body.iter().enumerate().find(|(i, &b)| b != pattern_byte(base + *i)) {
        return Err(format!("payload mismatch at byte {}: got {got}, expected {}", base + i, pattern_byte(base + i)));
    }
    Ok(true)
}

/// Verify a whole-object (200) stream body against [`pattern_byte`] (`base == 0`).
/// Shared by the sync and async demo clients.
pub fn verify_stream_body(body: &[u8], is_200: bool, path: &str) -> Result<bool, String> {
    verify_stream_body_at(body, is_200, path, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_range_explicit() {
        assert_eq!(parse_range("bytes=0-499", 1000), RangeSpec::Range { start: 0, end: 499 });
        assert_eq!(parse_range("bytes=0-0", 1000), RangeSpec::Range { start: 0, end: 0 });
        assert_eq!(parse_range("bytes=500-999", 1000), RangeSpec::Range { start: 500, end: 999 });
    }

    #[test]
    fn parse_range_clamps_end_to_object() {
        assert_eq!(parse_range("bytes=0-100000", 1000), RangeSpec::Range { start: 0, end: 999 });
        assert_eq!(parse_range("bytes=900-100000", 1000), RangeSpec::Range { start: 900, end: 999 });
    }

    #[test]
    fn parse_range_open_ended() {
        assert_eq!(parse_range("bytes=500-", 1000), RangeSpec::Range { start: 500, end: 999 });
        assert_eq!(parse_range("bytes=0-", 1000), RangeSpec::Range { start: 0, end: 999 });
    }

    #[test]
    fn parse_range_suffix() {
        assert_eq!(parse_range("bytes=-500", 1000), RangeSpec::Range { start: 500, end: 999 });
        // suffix bigger than the object → whole object
        assert_eq!(parse_range("bytes=-5000", 1000), RangeSpec::Range { start: 0, end: 999 });
    }

    #[test]
    fn parse_range_unsatisfiable() {
        assert_eq!(parse_range("bytes=1000-1001", 1000), RangeSpec::Unsatisfiable); // start == total
        assert_eq!(parse_range("bytes=5000-6000", 1000), RangeSpec::Unsatisfiable);
        assert_eq!(parse_range("bytes=1000-", 1000), RangeSpec::Unsatisfiable);
        assert_eq!(parse_range("bytes=-0", 1000), RangeSpec::Unsatisfiable); // zero-length suffix
        assert_eq!(parse_range("bytes=0-0", 0), RangeSpec::Unsatisfiable);   // empty object
    }

    #[test]
    fn parse_range_ignored_forms_serve_full() {
        assert_eq!(parse_range("bytes=0-1,2-3", 1000), RangeSpec::Full); // multi-range (no multipart)
        assert_eq!(parse_range("items=0-1", 1000), RangeSpec::Full);     // unknown unit
        assert_eq!(parse_range("bytes=abc", 1000), RangeSpec::Full);     // malformed
        assert_eq!(parse_range("bytes=zz-10", 1000), RangeSpec::Full);   // bad number
        assert_eq!(parse_range("bytes=500-100", 1000), RangeSpec::Full); // start > end
        assert_eq!(parse_range("", 1000), RangeSpec::Full);              // empty
    }

    #[test]
    fn content_range_round_trips() {
        let v = content_range(0, 499, 1000);
        assert_eq!(v, "bytes 0-499/1000");
        assert_eq!(parse_content_range(&v), Some((0, 499, 1000)));
        assert_eq!(content_range_unsatisfied(1000), "bytes */1000");
        // the unsatisfied form is not a concrete range
        assert_eq!(parse_content_range("bytes */1000"), None);
    }

    #[test]
    fn pattern_fill_from_uses_absolute_offset() {
        let mut buf = [0u8; 8];
        pattern_fill_from(&mut buf, 250);
        for (i, &b) in buf.iter().enumerate() {
            assert_eq!(b, pattern_byte(250 + i));
        }
        assert_eq!(buf[1], pattern_byte(251)); // crosses the 251 modulus → 0
    }
}
