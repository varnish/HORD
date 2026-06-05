//! HORD zero-copy extension (spec §7.1–7.4): the `X-HORD-RDMA-Write` HTTP
//! semantics, layered over the one-sided RDMA-write driver in `hord-stream`.
//!
//! The split of responsibility:
//!
//! * **`hord-stream`** owns the *mechanism* — capability negotiation
//!   ([`HordStream::zero_copy_negotiated`]), buffer registration, and the
//!   one-sided write ([`HordStream::rdma_write_all`]).
//! * **this crate** owns the *HTTP semantics* — the `X-HORD-RDMA-Write` request
//!   and response header codec (§12.3 / §12.4) and small orchestration helpers
//!   that drive a write and report the right status.
//!
//! It is plain `std` Rust with no third-party dependencies; the HTTP framing
//! itself stays with the caller (the demo's hand-rolled codec, or `hyper`).
//!
//! ## Flow
//!
//! **Client.** Gate on [`HordStream::zero_copy_negotiated`]; build a
//! [`ZeroCopyRequest`] (registers a destination buffer); add
//! [`ZeroCopyRequest::header_line`] to the GET. After reading the response head,
//! parse the `X-HORD-RDMA-Write` response header with [`RdmaWriteStatus::parse`]:
//! [`RdmaWriteStatus::Complete`] means the body is already in the buffer (read it
//! with [`ZeroCopyRequest::copy_out`]); otherwise fall back to the stream body.
//!
//! **Server.** Gate on negotiation + the presence of the request header; parse it
//! with [`RdmaWriteReq::parse`] and call [`serve_rdma_write`], which writes the
//! body into the client's buffer and returns the [`RdmaWriteStatus`] to put in
//! the response (with `Content-Length: 0`).

use std::io;

use hord_stream::{HordStream, RegisteredBuffer};

/// The HORD zero-copy header name, used for both request and response.
pub const HEADER: &str = "X-HORD-RDMA-Write";

// ---- request header (spec §12.3) ---------------------------------------------

/// A parsed `X-HORD-RDMA-Write` *request* header: the client's registered
/// destination region. `addr`/`rkey`/`len` tell the server where it may place
/// the response body. `id` (optional) requests split mode (§7.7): when present
/// and split mode is negotiated, the server delivers the body with RDMA
/// write-with-immediate carrying this ID; otherwise it is ignored and the server
/// performs a plain write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdmaWriteReq {
    /// Start address of the client's registered receive buffer (its virtual
    /// address; opaque to the server — may be host or GPU memory).
    pub addr: u64,
    /// Remote key authorizing the server's writes into that buffer.
    pub rkey: u32,
    /// Buffer capacity in bytes.
    pub len: u64,
    /// Optional split-mode transfer ID (§7.7).
    pub id: Option<u32>,
}

impl RdmaWriteReq {
    /// Parse a header *value* (everything after `X-HORD-RDMA-Write:`). Returns
    /// `None` if a required field (`addr`, `rkey`, `len`) is missing or
    /// malformed. Unknown parameters are ignored (forward-compatible).
    pub fn parse(value: &str) -> Option<Self> {
        let (mut addr, mut rkey, mut len, mut id) = (None, None, None, None);
        for part in value.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (k, v) = part.split_once('=')?;
            match k.trim() {
                "addr" => addr = Some(parse_hex_u64(v.trim())?),
                "rkey" => rkey = Some(parse_hex_u32(v.trim())?),
                "len" => len = Some(v.trim().parse().ok()?),
                "id" => id = Some(v.trim().parse().ok()?),
                _ => {}
            }
        }
        Some(RdmaWriteReq {
            addr: addr?,
            rkey: rkey?,
            len: len?,
            id,
        })
    }

    /// The header value: `addr=0x..;rkey=0x..;len=N[;id=N]`.
    pub fn header_value(&self) -> String {
        let mut s = format!("addr=0x{:x};rkey=0x{:x};len={}", self.addr, self.rkey, self.len);
        if let Some(id) = self.id {
            s.push_str(&format!(";id={id}"));
        }
        s
    }

    /// The full header line: `X-HORD-RDMA-Write: addr=..;rkey=..;len=..`.
    pub fn header_line(&self) -> String {
        format!("{HEADER}: {}", self.header_value())
    }
}

// ---- response header (spec §12.4) --------------------------------------------

/// A parsed `X-HORD-RDMA-Write` *response* header (spec §7.4 outcomes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaWriteStatus {
    /// Payload was placed in the client's buffer via RDMA write; `bytes_written`
    /// is the authoritative payload size (the response's `Content-Length` is 0).
    Complete { bytes_written: u64 },
    /// Payload exceeds the client's buffer; nothing was written. The client may
    /// retry with a larger buffer or a `Range`.
    TooLarge { object_size: u64 },
    /// The server elected not to use zero-copy; the body is sent on the stream.
    Declined,
}

impl RdmaWriteStatus {
    /// Parse a response header *value*. Returns `None` on an unknown status or a
    /// missing required field.
    pub fn parse(value: &str) -> Option<Self> {
        let (mut status, mut bytes_written, mut object_size) = (None, None, None);
        for part in value.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (k, v) = part.split_once('=')?;
            match k.trim() {
                "status" => status = Some(v.trim().to_string()),
                "bytes_written" => bytes_written = v.trim().parse().ok(),
                "object_size" => object_size = v.trim().parse().ok(),
                _ => {}
            }
        }
        match status?.as_str() {
            "complete" => Some(RdmaWriteStatus::Complete {
                bytes_written: bytes_written?,
            }),
            "too_large" => Some(RdmaWriteStatus::TooLarge {
                object_size: object_size?,
            }),
            "declined" => Some(RdmaWriteStatus::Declined),
            _ => None,
        }
    }

    /// The header value, e.g. `status=complete;bytes_written=N`.
    pub fn header_value(&self) -> String {
        match self {
            RdmaWriteStatus::Complete { bytes_written } => {
                format!("status=complete;bytes_written={bytes_written}")
            }
            RdmaWriteStatus::TooLarge { object_size } => {
                format!("status=too_large;object_size={object_size}")
            }
            RdmaWriteStatus::Declined => "status=declined".to_string(),
        }
    }

    /// The full header line: `X-HORD-RDMA-Write: status=..`.
    pub fn header_line(&self) -> String {
        format!("{HEADER}: {}", self.header_value())
    }
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    u64::from_str_radix(s, 16).ok()
}

fn parse_hex_u32(s: &str) -> Option<u32> {
    let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    u32::from_str_radix(s, 16).ok()
}

// ---- client orchestration ----------------------------------------------------

/// A registered destination buffer for a zero-copy response, together with the
/// request header advertising it. Hold it across the request/response: once the
/// response head reports [`RdmaWriteStatus::Complete`], the payload is already in
/// this buffer (delivered out-of-band by the server's RDMA write — RC ordering
/// guarantees it has landed by the time the response head arrives). Read it with
/// [`copy_out`](Self::copy_out).
pub struct ZeroCopyRequest {
    buf: RegisteredBuffer,
    id: Option<u32>,
}

impl ZeroCopyRequest {
    /// Register a `capacity`-byte destination region the server may RDMA-write
    /// into. Gate on [`HordStream::zero_copy_negotiated`] before offering it.
    pub fn new(stream: &HordStream, capacity: usize) -> io::Result<Self> {
        Ok(Self::from_buffer(stream.register_remote_writable(capacity)?))
    }

    /// Wrap an already-registered remote-writable buffer. Use this when the
    /// buffer was registered elsewhere — e.g. via an async stream's
    /// `register_remote_writable` — so the async client gets the same request
    /// descriptor / verify path as the sync client without re-deriving it.
    pub fn from_buffer(buf: RegisteredBuffer) -> Self {
        ZeroCopyRequest { buf, id: None }
    }

    /// Request split mode (§7.7) for this transfer: the emitted header carries
    /// `id=<transfer_id>`, and a split-capable server delivers the body via RDMA
    /// write-with-immediate, signalling `transfer_id` on the data-plane CQ (see
    /// [`SplitReceiver`]). Gate on [`HordStream::split_mode_negotiated`] before
    /// using it — on a non-split connection the `id` is simply ignored by the
    /// server. Chainable on [`new`](Self::new) / [`from_buffer`](Self::from_buffer).
    pub fn with_id(mut self, transfer_id: u32) -> Self {
        self.id = Some(transfer_id);
        self
    }

    /// The split-mode transfer ID this request advertises, if any.
    pub fn id(&self) -> Option<u32> {
        self.id
    }

    /// The request descriptor (`addr`/`rkey`/`len`[/`id`]) for this buffer.
    pub fn request(&self) -> RdmaWriteReq {
        RdmaWriteReq {
            addr: self.buf.as_mut_ptr() as u64,
            rkey: self.buf.rkey(),
            len: self.buf.len() as u64,
            id: self.id,
        }
    }

    /// The `X-HORD-RDMA-Write: addr=..;rkey=..;len=..` line to add to the GET.
    pub fn header_line(&self) -> String {
        self.request().header_line()
    }

    /// Destination capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Copy `dst.len()` delivered bytes out of the buffer, starting at `off`.
    /// (Consuming/verifying the payload reads the application's own buffer — this
    /// is not a transport copy; a real consumer can use it in place instead.)
    pub fn copy_out(&self, off: usize, dst: &mut [u8]) {
        self.buf.copy_out(off, dst);
    }
}

// ---- server orchestration ----------------------------------------------------

/// Perform the server side of a zero-copy response (spec §7.3, and §7.7 in split
/// mode).
///
/// If `object_size` fits the client's advertised buffer (`req.len`), register a
/// source region, let `fill` populate it, RDMA-write it into the client's
/// `[addr, rkey]`, and return [`RdmaWriteStatus::Complete`]. If it does not fit,
/// return [`RdmaWriteStatus::TooLarge`] without writing. The caller turns the
/// returned status into the HTTP response header — always with `Content-Length:
/// 0` for `Complete` (the bytes travel out-of-band).
///
/// **Split mode (§7.7).** When `req.id` is present *and*
/// [`HordStream::split_mode_negotiated`] holds, the body is delivered with RDMA
/// write-with-immediate carrying `req.id`, so the client's data plane is
/// signalled on its CQ (the HTTP response is still returned, as §7.7.4 step 3).
/// Otherwise `req.id` is ignored and a plain write is used. Either way the
/// returned status is the same — split vs. plain is purely a delivery mechanism.
///
/// Gate on [`HordStream::zero_copy_negotiated`] (and your own policy) before
/// calling. On a transport failure mid-write the stream is closed and an `Err`
/// is returned; the caller MUST NOT report `complete` in that case (§7.4/§7.7.7).
///
/// The source region is registered per call and released once the write is
/// acknowledged. A production server would amortize registration with a pool
/// (spec §8.3) rather than register per response.
pub fn serve_rdma_write(
    stream: &mut HordStream,
    req: &RdmaWriteReq,
    object_size: u64,
    fill: impl FnOnce(&RegisteredBuffer),
) -> io::Result<RdmaWriteStatus> {
    if object_size > req.len {
        return Ok(RdmaWriteStatus::TooLarge { object_size });
    }
    // Use split mode only if the client asked (id present) and we negotiated it;
    // otherwise the id is ignored (§7.7.3).
    let split_id = req.id.filter(|_| stream.split_mode_negotiated());

    match split_id {
        // --- split mode: deliver via write-with-immediate (§7.7) ---
        Some(transfer_id) => {
            // The immediate must always be delivered so the data plane's posted
            // transfer credit is consumed and its `poll_completion` returns — even
            // for a zero-length object. A zero-length MR is not portable, so back
            // an empty body with a 1-byte source (the WR still writes 0 bytes).
            let n = object_size as usize;
            let src = stream.register_source(n.max(1))?;
            if n > 0 {
                fill(&src);
            }
            stream.rdma_write_all_with_imm(&src, 0, req.addr, req.rkey, n, transfer_id)?;
            Ok(RdmaWriteStatus::Complete {
                bytes_written: object_size,
            })
        }
        // --- plain zero-copy write (§7.3) ---
        None => {
            if object_size == 0 {
                // Nothing to place; a zero-length MR is not portable, so
                // short-circuit (no data plane is waiting in plain mode).
                return Ok(RdmaWriteStatus::Complete { bytes_written: 0 });
            }
            let n = object_size as usize;
            let src = stream.register_source(n)?;
            fill(&src);
            stream.rdma_write_all(&src, 0, req.addr, req.rkey, n)?;
            // `src` drops here: rdma_write_all blocked until the write completed
            // and was acked, so no DMA references the MR — deregistration is sound.
            Ok(RdmaWriteStatus::Complete {
                bytes_written: object_size,
            })
        }
    }
}

// ---- client data plane (spec §7.7) -------------------------------------------

/// A completed split-mode transfer, identified by the `id` the client placed in
/// its `X-HORD-RDMA-Write` request ([`ZeroCopyRequest::with_id`]). By the time
/// this is returned the payload is fully in the client's registered buffer (QP
/// ordering guarantees the write landed before the immediate's completion,
/// §7.7.2), so the consumer can use it immediately without the HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitCompletion {
    /// The transfer ID, echoed from the write-with-immediate's `imm_data`.
    pub transfer_id: u32,
}

/// The client's *data plane* for protocol splitting (§7.7): payload completions
/// arrive straight off the CQ — keyed by transfer ID, with no HTTP parsing — for
/// requests issued with [`ZeroCopyRequest::with_id`]. The consumer maps each
/// returned [`SplitCompletion::transfer_id`] back to the buffer it advertised.
///
/// In this prototype the data plane shares the one driver task — and the one
/// [`HordStream`] — with the control plane (see PROTOTYPE.md, "single-task
/// driver"), so a `SplitReceiver` *borrows* the stream for a poll rather than
/// owning an independent CQ waiter. The intended use is: the control plane issues
/// its requests, then the data plane drains completions. A production split would
/// run the data plane on its own thread polling the shared CQ directly; that
/// needs a multi-waiter scheme and is deferred.
pub struct SplitReceiver<'s> {
    stream: &'s mut HordStream,
}

impl<'s> SplitReceiver<'s> {
    /// Borrow `stream` as a data-plane receiver. Errors if protocol splitting was
    /// not negotiated on this connection (gate with
    /// [`HordStream::split_mode_negotiated`]).
    pub fn new(stream: &'s mut HordStream) -> io::Result<Self> {
        if !stream.split_mode_negotiated() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "protocol splitting (§7.7) was not negotiated on this connection",
            ));
        }
        Ok(SplitReceiver { stream })
    }

    /// Block until the next transfer completes, returning its [`SplitCompletion`]
    /// — or `None` if the connection closed first. Mirrors the spec §13.2
    /// `poll_completion()` shape (synchronous here; the async stream parks on the
    /// completion fd instead of busy-polling).
    pub fn poll_completion(&mut self) -> io::Result<Option<SplitCompletion>> {
        Ok(self
            .stream
            .poll_completed_transfer()?
            .map(|transfer_id| SplitCompletion { transfer_id }))
    }

    /// Non-blocking: drain the CQ once and return a transfer if one is already
    /// complete, else `None`. A `None` here means "nothing ready *yet*" — it does
    /// NOT signal end-of-stream; a consumer looping on `try_completion` MUST also
    /// check [`is_closed`](Self::is_closed) to detect a closed connection (unlike
    /// the blocking [`poll_completion`](Self::poll_completion), whose `None` is
    /// EOF). This asymmetry is why `is_closed` exists.
    pub fn try_completion(&mut self) -> io::Result<Option<SplitCompletion>> {
        self.stream.drain_completions()?;
        Ok(self
            .stream
            .next_completed_transfer()
            .map(|transfer_id| SplitCompletion { transfer_id }))
    }

    /// Whether the connection has closed. A non-blocking consumer driving
    /// [`try_completion`](Self::try_completion) checks this to distinguish EOF
    /// from "no transfer ready yet" — once `is_closed()` is true and
    /// `try_completion` returns `None`, no further transfers will arrive.
    pub fn is_closed(&self) -> bool {
        self.stream.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_round_trips() {
        let r = RdmaWriteReq {
            addr: 0x7f4a_2c00_0000,
            rkey: 0x01ab_3f00,
            len: 16_777_216,
            id: None,
        };
        assert_eq!(RdmaWriteReq::parse(&r.header_value()), Some(r));

        let with_id = RdmaWriteReq { id: Some(42), ..r };
        assert_eq!(RdmaWriteReq::parse(&with_id.header_value()), Some(with_id));
    }

    #[test]
    fn req_parses_spec_example() {
        // The exact value from spec §7.2 / §12.3.
        let r = RdmaWriteReq::parse("addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216").unwrap();
        assert_eq!(r.addr, 0x7f4a_2c00_0000);
        assert_eq!(r.rkey, 0x01ab_3f00);
        assert_eq!(r.len, 16_777_216);
        assert_eq!(r.id, None);
        // Split-mode variant from §7.7.3.
        let s = RdmaWriteReq::parse("addr=0x7f4a2c000000;rkey=0x01ab3f00;len=16777216;id=42").unwrap();
        assert_eq!(s.id, Some(42));
    }

    #[test]
    fn req_rejects_malformed() {
        assert_eq!(RdmaWriteReq::parse("rkey=0x1;len=10"), None); // no addr
        assert_eq!(RdmaWriteReq::parse("addr=0x1;len=10"), None); // no rkey
        assert_eq!(RdmaWriteReq::parse("addr=0x1;rkey=0x2"), None); // no len
        assert_eq!(RdmaWriteReq::parse("addr=zz;rkey=0x2;len=1"), None); // bad hex
        assert_eq!(RdmaWriteReq::parse("addr=0x1;rkey=0x2;len=nope"), None); // bad dec
    }

    #[test]
    fn req_hex_is_lenient_and_trailing_semicolon_ok() {
        // Accept hex with or without the 0x prefix, and a trailing ';'.
        let r = RdmaWriteReq::parse("addr=1000;rkey=0X2A;len=5;").unwrap();
        assert_eq!(r.addr, 0x1000);
        assert_eq!(r.rkey, 0x2a);
        assert_eq!(r.len, 5);
    }

    #[test]
    fn status_round_trips() {
        for s in [
            RdmaWriteStatus::Complete { bytes_written: 14_680_064 },
            RdmaWriteStatus::TooLarge { object_size: 1_073_741_824 },
            RdmaWriteStatus::Declined,
        ] {
            assert_eq!(RdmaWriteStatus::parse(&s.header_value()), Some(s));
        }
    }

    #[test]
    fn status_rejects_unknown_and_missing() {
        assert_eq!(RdmaWriteStatus::parse("status=bogus"), None);
        assert_eq!(RdmaWriteStatus::parse("status=complete"), None); // no bytes_written
        assert_eq!(RdmaWriteStatus::parse("status=too_large"), None); // no object_size
        assert_eq!(RdmaWriteStatus::parse("bytes_written=5"), None); // no status
    }

    #[test]
    fn header_lines_carry_the_name() {
        let line = RdmaWriteStatus::Declined.header_line();
        assert_eq!(line, "X-HORD-RDMA-Write: status=declined");
        assert!(RdmaWriteReq {
            addr: 1,
            rkey: 2,
            len: 3,
            id: None
        }
        .header_line()
        .starts_with("X-HORD-RDMA-Write: "));
    }
}
