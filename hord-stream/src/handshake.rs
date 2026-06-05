//! HORD handshake, exchanged in the RDMA CM private-data field during
//! connect/accept (spec section 12.1).
//!
//! All multi-byte fields are big-endian (network byte order). With big-endian
//! the 32-bit magic `0x484F5244` serialises to the ASCII bytes `H O R D`,
//! which is convenient when staring at packet dumps.
//!
//! ## Deviation from the draft spec
//!
//! Spec 12.1 describes a 60-byte structure (14 meaningful bytes + 46 reserved).
//! The RDMA CM private-data area for an RC connection is only ~56 bytes on
//! IB/RoCE, so a 60-byte handshake does not reliably fit. This prototype
//! transmits just the 16 meaningful bytes below and drops the reserved tail.
//! (Worth folding back into the spec.)

use std::io;

/// Spec magic: ASCII "HORD".
pub const MAGIC: u32 = 0x484F_5244;
/// Protocol version.
pub const VERSION: u16 = 1;
/// Wire size of the handshake we actually transmit.
pub const HANDSHAKE_LEN: usize = 16;

/// Capability flags (spec 5.3). Bit 0 (`ZERO_COPY_CAPABLE`) is negotiated as of
/// Pass 4; split mode (§7.7) is not yet implemented.
pub mod flags {
    /// Peer supports the zero-copy extension (spec §7).
    pub const ZERO_COPY_CAPABLE: u16 = 1 << 0;
    /// Peer supports protocol splitting (spec §7.7). Not yet implemented.
    pub const SPLIT_MODE_CAPABLE: u16 = 1 << 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub version: u16,
    pub flags: u16,
    pub max_message_size: u32,
    pub max_recv_buffers: u16,
}

impl Handshake {
    pub fn new(max_message_size: u32, max_recv_buffers: u16) -> Self {
        Handshake {
            version: VERSION,
            flags: 0,
            max_message_size,
            max_recv_buffers,
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

    /// Serialise to the 16-byte wire form.
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut b = [0u8; HANDSHAKE_LEN];
        b[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        b[4..6].copy_from_slice(&self.version.to_be_bytes());
        b[6..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.max_message_size.to_be_bytes());
        b[12..14].copy_from_slice(&self.max_recv_buffers.to_be_bytes());
        // bytes 14..16 reserved, left zero
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
        Ok(Handshake {
            version,
            flags: u16::from_be_bytes(buf[6..8].try_into().unwrap()),
            max_message_size: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            max_recv_buffers: u16::from_be_bytes(buf[12..14].try_into().unwrap()),
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
}
