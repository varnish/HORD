//! HORD handshake, exchanged in the RDMA CM private-data field during
//! connect/accept (spec section 12.1).
//!
//! All multi-byte fields are big-endian (network byte order). With big-endian
//! the 32-bit magic `0x484F5244` serialises to the ASCII bytes `H O R D`,
//! which is convenient when staring at packet dumps.
//!
//! ## Deviations from the draft spec
//!
//! 1. **Size.** Spec 12.1 describes a 60-byte structure (14 meaningful bytes +
//!    46 reserved). The RDMA CM private-data area for an RC connection is only
//!    ~56 bytes on IB/RoCE, so a 60-byte handshake does not reliably fit. This
//!    prototype transmits just the 16 meaningful bytes below and drops the
//!    reserved tail.
//! 2. **Transfer credits.** Bytes 14..16 (reserved in the spec) carry a
//!    `split_credits` count: the transfer-credit window (§7.7.6) this side can
//!    receive. The spec describes transfer credits as an *implicit per-request
//!    grant* with no explicit advertisement; we advertise a static window so the
//!    sender can bound its in-flight write-with-immediates instead of overrunning
//!    the peer's posted recv WRs and RNR-stalling. `0` (the legacy reserved
//!    value) means "no transfer credits", so split mode declines gracefully.
//!
//! (Both worth folding back into the spec.)

use std::io;

/// Spec magic: ASCII "HORD".
pub const MAGIC: u32 = 0x484F_5244;
/// Protocol version.
pub const VERSION: u16 = 1;
/// Wire size of the handshake we actually transmit.
pub const HANDSHAKE_LEN: usize = 16;

/// Capability flags (spec 5.3). Bit 0 (`ZERO_COPY_CAPABLE`) is negotiated as of
/// Pass 4; bit 1 (`SPLIT_MODE_CAPABLE`) as of Pass 7.
pub mod flags {
    /// Peer supports the zero-copy extension (spec §7).
    pub const ZERO_COPY_CAPABLE: u16 = 1 << 0;
    /// Peer supports protocol splitting (spec §7.7). Per §5.3 this requires
    /// `ZERO_COPY_CAPABLE`; a peer MUST NOT set it without bit 0.
    pub const SPLIT_MODE_CAPABLE: u16 = 1 << 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub version: u16,
    pub flags: u16,
    pub max_message_size: u32,
    pub max_recv_buffers: u16,
    /// Transfer-credit window (spec §7.7.6): how many in-flight write-with-imm
    /// transfers this side can receive. `0` means none (split mode declines). A
    /// legacy peer that left bytes 14..16 reserved-zero decodes to `0` here.
    pub split_credits: u16,
}

impl Handshake {
    pub fn new(max_message_size: u32, max_recv_buffers: u16) -> Self {
        Handshake {
            version: VERSION,
            flags: 0,
            max_message_size,
            max_recv_buffers,
            split_credits: 0,
        }
    }

    /// Advertise (or clear) the `ZERO_COPY_CAPABLE` flag. Chainable on [`new`].
    pub fn with_zero_copy(mut self, on: bool) -> Self {
        if on {
            self.flags |= flags::ZERO_COPY_CAPABLE;
        } else {
            self.flags &= !flags::ZERO_COPY_CAPABLE;
        }
        self
    }

    /// Whether this handshake advertises the zero-copy extension (spec §7).
    pub fn zero_copy_capable(&self) -> bool {
        self.flags & flags::ZERO_COPY_CAPABLE != 0
    }

    /// Advertise (or clear) the `SPLIT_MODE_CAPABLE` flag (spec §7.7). Chainable.
    /// The caller is responsible for the §5.3 dependency — only set this when
    /// zero-copy is also advertised.
    pub fn with_split_mode(mut self, on: bool) -> Self {
        if on {
            self.flags |= flags::SPLIT_MODE_CAPABLE;
        } else {
            self.flags &= !flags::SPLIT_MODE_CAPABLE;
        }
        self
    }

    /// Whether this handshake advertises protocol splitting (spec §7.7).
    pub fn split_mode_capable(&self) -> bool {
        self.flags & flags::SPLIT_MODE_CAPABLE != 0
    }

    /// Advertise the transfer-credit window (spec §7.7.6) this side can receive.
    /// Chainable on [`new`]. Set alongside [`with_split_mode`](Self::with_split_mode);
    /// a `0` count makes a split-capable peer decline split mode.
    pub fn with_split_credits(mut self, credits: u16) -> Self {
        self.split_credits = credits;
        self
    }

    /// Serialise to the 16-byte wire form.
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut b = [0u8; HANDSHAKE_LEN];
        b[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        b[4..6].copy_from_slice(&self.version.to_be_bytes());
        b[6..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.max_message_size.to_be_bytes());
        b[12..14].copy_from_slice(&self.max_recv_buffers.to_be_bytes());
        b[14..16].copy_from_slice(&self.split_credits.to_be_bytes());
        b
    }

    /// Parse and validate a peer handshake.
    pub fn decode(buf: &[u8]) -> io::Result<Handshake> {
        if buf.len() < 14 {
            return Err(err(format!(
                "handshake too short: {} bytes (need >= 14)",
                buf.len()
            )));
        }
        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(err(format!(
                "bad handshake magic 0x{magic:08X} (expected 0x{MAGIC:08X})"
            )));
        }
        let version = u16::from_be_bytes(buf[4..6].try_into().unwrap());
        if version != VERSION {
            return Err(err(format!(
                "unsupported HORD version {version} (this build speaks {VERSION})"
            )));
        }
        // Transfer credits live in bytes 14..16, reserved-zero before this field
        // was defined — tolerate a 14-byte legacy frame by defaulting to 0.
        let split_credits = if buf.len() >= 16 {
            u16::from_be_bytes(buf[14..16].try_into().unwrap())
        } else {
            0
        };
        Ok(Handshake {
            version,
            flags: u16::from_be_bytes(buf[6..8].try_into().unwrap()),
            max_message_size: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            max_recv_buffers: u16::from_be_bytes(buf[12..14].try_into().unwrap()),
            split_credits,
        })
    }
}

fn err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let h = Handshake::new(65536, 32);
        let decoded = Handshake::decode(&h.encode()).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn magic_is_ascii_hord() {
        assert_eq!(&MAGIC.to_be_bytes(), b"HORD");
    }

    #[test]
    fn rejects_bad_magic() {
        let mut b = Handshake::new(65536, 32).encode();
        b[0] = 0;
        assert!(Handshake::decode(&b).is_err());
    }

    #[test]
    fn zero_copy_flag_round_trips() {
        let off = Handshake::new(65536, 32);
        assert!(!off.zero_copy_capable());
        assert!(!Handshake::decode(&off.encode()).unwrap().zero_copy_capable());

        let on = Handshake::new(65536, 32).with_zero_copy(true);
        assert!(on.zero_copy_capable());
        assert!(Handshake::decode(&on.encode()).unwrap().zero_copy_capable());

        // Setter is idempotent / clearable.
        assert!(!on.with_zero_copy(false).zero_copy_capable());
    }

    #[test]
    fn split_mode_flag_round_trips_independently() {
        let plain = Handshake::new(65536, 32);
        assert!(!plain.split_mode_capable());

        // Split mode is a distinct bit from zero-copy: setting one leaves the
        // other untouched.
        let both = Handshake::new(65536, 32)
            .with_zero_copy(true)
            .with_split_mode(true);
        let decoded = Handshake::decode(&both.encode()).unwrap();
        assert!(decoded.zero_copy_capable());
        assert!(decoded.split_mode_capable());

        assert!(!both.with_split_mode(false).split_mode_capable());
        assert!(both.with_split_mode(false).zero_copy_capable());
    }

    #[test]
    fn split_credits_round_trips() {
        let h = Handshake::new(65536, 32).with_split_credits(8);
        assert_eq!(h.split_credits, 8);
        assert_eq!(Handshake::decode(&h.encode()).unwrap().split_credits, 8);

        // Default is 0 (split mode then declines on the peer).
        assert_eq!(Handshake::new(65536, 32).split_credits, 0);
    }

    #[test]
    fn decode_tolerates_14_byte_legacy_frame() {
        // A peer predating the split_credits field sends only 14 meaningful
        // bytes; decode must succeed with split_credits == 0 rather than error.
        let full = Handshake::new(65536, 32).with_split_credits(8).encode();
        let legacy = &full[..14];
        let decoded = Handshake::decode(legacy).unwrap();
        assert_eq!(decoded.max_recv_buffers, 32);
        assert_eq!(decoded.split_credits, 0);
    }
}
