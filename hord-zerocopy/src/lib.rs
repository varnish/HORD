//! HORD zero-copy extension (spec §7.1–7.4): the `X-HORD-RDMA-Write` HTTP
//! semantics for delivering a response body by one-sided `RDMA_WRITE` straight
//! into the client's (or a GPU's) registered buffer.
//!
//! # Two layers, one feature
//!
//! * The **header codec** — [`RdmaWriteReq`] and [`RdmaWriteStatus`] (the §12.3 /
//!   §12.4 request / response header) plus the server-policy decision
//!   [`RdmaWriteAction`] — is plain `std` Rust with **no third-party or RDMA
//!   dependency** and is **always** compiled. An embedder can parse and emit the
//!   header, and compute the §7.3/§7.7 server policy, on a machine with no NIC and
//!   no `rdma-core` — e.g. unit-testing header handling on a laptop, where the
//!   default (feature-off) build pulls in nothing at all.
//! * The **`rdma` feature** (off by default) adds the write *orchestration* —
//!   `ZeroCopyRequest`, `serve_rdma_write` / `serve_rdma_write_pooled`,
//!   `SourcePool`, and the split-mode data plane (`SplitReceiver` /
//!   `SplitCompletion`) — which drives the actual one-sided write and therefore
//!   pulls in `hord-stream` → `hord-core` → `sideway` / `libibverbs` / `librdmacm`.
//!
//! These two layers are also split structurally: the codec (and the pure
//! [`RdmaWriteAction`] policy) lives in this file and is always compiled; the
//! orchestration lives entirely in a private `rdma` module, gated by a **single**
//! `#[cfg(feature = "rdma")]` and re-exported at the crate root. So
//! the device-free boundary is enforced by the module wall, not by remembering a
//! `#[cfg]` on each declaration: the default build compiles the whole module out,
//! and nothing in this file can reference a `hord-stream` type.
//!
//! The HTTP framing itself stays with the caller (the demo's hand-rolled codec,
//! or `hyper`); the *mechanism* the orchestration drives — capability negotiation,
//! buffer registration, the one-sided write — lives in `hord-stream`.
// The detailed client/server flow references the orchestration types and
// `HordStream`, which exist only under `rdma`; gate the prose so the codec-only
// docs (and `cargo doc --no-default-features`) carry no broken intra-doc links.
#![cfg_attr(
    feature = "rdma",
    doc = r#"
# Flow (`rdma` feature)

**Client.** Gate on [`HordStream::zero_copy_negotiated`](hord_stream::HordStream::zero_copy_negotiated);
build a [`ZeroCopyRequest`] (registers a destination buffer); add
[`ZeroCopyRequest::header_line`] to the GET. After reading the response head,
parse the `X-HORD-RDMA-Write` response header with [`RdmaWriteStatus::parse`]:
[`RdmaWriteStatus::Complete`] means the body is already in the buffer (read it
with [`ZeroCopyRequest::copy_out`]); otherwise fall back to the stream body.

**Server.** Gate on negotiation + the presence of the request header; parse it
with [`RdmaWriteReq::parse`] and call [`serve_rdma_write`], which writes the body
into the client's buffer and returns the [`RdmaWriteStatus`] to put in the
response (with `Content-Length: 0`).
"#
)]

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

// ---- server policy (pure; spec §7.3 / §7.7) ----------------------------------

/// The action `serve_rdma_write` takes for a given request and object size —
/// the §7.3 / §7.7 *server policy*, factored out as a pure decision function so
/// the synchronous library path here and the async demo server share one source
/// of truth. (They can't share the *mechanism*: one drives the blocking
/// `HordStream::rdma_write_all`, the other an async `rdma_write` future — but the
/// policy that used to drift between them is all here.)
///
/// This is **pure** — no device, no I/O — so it lives in the default codec build
/// and unit-tests without an RDMA library; only its executors (`serve_rdma_write`
/// and friends, under the `rdma` feature) need a device.
///
/// Compute it with [`decide`](Self::decide); then either return the status (for
/// [`Respond`](Self::Respond)) or run the write with your path's own register /
/// fill / write calls (for [`Write`](Self::Write)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaWriteAction {
    /// No write is needed — respond with this status directly. Covers
    /// [`RdmaWriteStatus::TooLarge`] (object exceeds the client's buffer, §7.4)
    /// and the zero-length plain-mode case ([`RdmaWriteStatus::Complete`]
    /// `{ bytes_written: 0 }` — a zero-length MR is not portable and, unlike split
    /// mode, no data plane is waiting on an immediate, §7.3).
    Respond(RdmaWriteStatus),
    /// Deliver the body, then respond [`RdmaWriteStatus::Complete`]
    /// `{ bytes_written: payload_len }`: register a `source_len`-byte source, fill
    /// its first `payload_len` bytes, then RDMA-write `payload_len` bytes into the
    /// client's buffer — with write-with-immediate carrying `transfer_id` if
    /// `Some` (split mode, §7.7), else a plain one-sided write (§7.3).
    Write {
        /// Bytes to write into the client's buffer (the object size; `0` only in
        /// split mode, where the immediate must still ride an empty body).
        payload_len: usize,
        /// Bytes to register as the source MR: `payload_len.max(1)` in split mode
        /// (a zero-length MR is not portable, so a 1-byte source backs an empty
        /// body — the WR still writes 0 bytes), `payload_len` otherwise.
        source_len: usize,
        /// `Some(id)` delivers via write-with-immediate (split mode, §7.7); `None`
        /// is a plain one-sided write.
        transfer_id: Option<u32>,
    },
}

impl RdmaWriteAction {
    /// Decide the action for delivering `object_size` bytes against the client's
    /// request `req`, given whether protocol splitting was negotiated on the
    /// connection (`split_negotiated` — pass `HordStream::split_mode_negotiated`).
    /// Pure: no device and no I/O, so it unit-tests on its own.
    pub fn decide(req: &RdmaWriteReq, object_size: u64, split_negotiated: bool) -> Self {
        if object_size > req.len {
            return RdmaWriteAction::Respond(RdmaWriteStatus::TooLarge { object_size });
        }
        // The object must fit `usize` to register a source MR and write it — and so
        // the reported `bytes_written` round-trips `object_size` exactly. On a
        // 64-bit target this always holds; if it somehow does not, we cannot serve
        // it zero-copy, so report too_large rather than silently truncating the size.
        let Ok(n) = usize::try_from(object_size) else {
            return RdmaWriteAction::Respond(RdmaWriteStatus::TooLarge { object_size });
        };
        // Split mode only if the client asked (id present) and we negotiated it
        // (§7.7.3); otherwise the id is ignored and a plain write is used.
        let split_id = req.id.filter(|_| split_negotiated);
        match split_id {
            Some(transfer_id) => RdmaWriteAction::Write {
                payload_len: n,
                source_len: n.max(1),
                transfer_id: Some(transfer_id),
            },
            // Plain mode with nothing to place: short-circuit, no write.
            None if n == 0 => {
                RdmaWriteAction::Respond(RdmaWriteStatus::Complete { bytes_written: 0 })
            }
            None => RdmaWriteAction::Write {
                payload_len: n,
                source_len: n,
                transfer_id: None,
            },
        }
    }
}

// ==============================================================================
// Write *orchestration* (§7.3 / §7.7) — the device-dependent layer.
//
// Everything that drives the actual one-sided write lives in one private module
// behind a *single* `#[cfg(feature = "rdma")]`, re-exported at the crate root so
// callers keep using `hord_zerocopy::ZeroCopyRequest` etc. unchanged. Collapsing
// the orchestration into one gated module (rather than a `#[cfg]` per item) makes
// the device-free boundary structural: the default codec build compiles the whole
// module — and its `hord-stream` -> `hord-core` -> sideway / libibverbs /
// librdmacm dependency — out wholesale, and nothing above this line can reach a
// `hord-stream` type. The pure `RdmaWriteAction` policy above is deliberately on
// the codec side of that wall.
// ==============================================================================
#[cfg(feature = "rdma")]
mod rdma;
#[cfg(feature = "rdma")]
pub use rdma::*;

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

    // ---- RdmaWriteAction::decide (the §7.3/§7.7 server policy) ----------------

    /// A request advertising a 1 KiB buffer, optionally requesting split mode.
    fn req(len: u64, id: Option<u32>) -> RdmaWriteReq {
        RdmaWriteReq { addr: 0x1000, rkey: 0x2a, len, id }
    }

    #[test]
    fn decide_too_large_writes_nothing() {
        // object_size > buffer -> TooLarge, regardless of split (precedence).
        let a = RdmaWriteAction::decide(&req(1024, None), 2048, false);
        assert_eq!(a, RdmaWriteAction::Respond(RdmaWriteStatus::TooLarge { object_size: 2048 }));
        let split = RdmaWriteAction::decide(&req(1024, Some(7)), 2048, true);
        assert_eq!(split, RdmaWriteAction::Respond(RdmaWriteStatus::TooLarge { object_size: 2048 }));
    }

    #[test]
    fn decide_plain_zero_length_short_circuits() {
        // Plain mode, empty object: respond Complete{0} with no write (no portable
        // zero-length MR, and no data plane waiting).
        let a = RdmaWriteAction::decide(&req(1024, None), 0, false);
        assert_eq!(a, RdmaWriteAction::Respond(RdmaWriteStatus::Complete { bytes_written: 0 }));
    }

    #[test]
    fn decide_plain_write() {
        let a = RdmaWriteAction::decide(&req(1024, None), 512, false);
        assert_eq!(
            a,
            RdmaWriteAction::Write { payload_len: 512, source_len: 512, transfer_id: None }
        );
    }

    #[test]
    fn decide_split_write() {
        let a = RdmaWriteAction::decide(&req(1024, Some(42)), 512, true);
        assert_eq!(
            a,
            RdmaWriteAction::Write { payload_len: 512, source_len: 512, transfer_id: Some(42) }
        );
    }

    #[test]
    fn decide_split_zero_length_keeps_immediate_with_one_byte_source() {
        // Split + empty body: still a Write (the immediate must ride so the data
        // plane's credit is consumed), backed by a 1-byte source.
        let a = RdmaWriteAction::decide(&req(1024, Some(9)), 0, true);
        assert_eq!(
            a,
            RdmaWriteAction::Write { payload_len: 0, source_len: 1, transfer_id: Some(9) }
        );
    }

    #[test]
    fn decide_ignores_id_when_split_not_negotiated() {
        // id present but split not negotiated (§7.7.3): falls back to a plain
        // write — and to the plain zero-length short-circuit for an empty body.
        let a = RdmaWriteAction::decide(&req(1024, Some(5)), 512, false);
        assert_eq!(
            a,
            RdmaWriteAction::Write { payload_len: 512, source_len: 512, transfer_id: None }
        );
        let empty = RdmaWriteAction::decide(&req(1024, Some(5)), 0, false);
        assert_eq!(empty, RdmaWriteAction::Respond(RdmaWriteStatus::Complete { bytes_written: 0 }));
    }

    #[test]
    fn decide_exact_fit_is_not_too_large() {
        // object_size == buffer len fits (the gate is strictly greater-than).
        let a = RdmaWriteAction::decide(&req(1024, None), 1024, false);
        assert_eq!(
            a,
            RdmaWriteAction::Write { payload_len: 1024, source_len: 1024, transfer_id: None }
        );
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
