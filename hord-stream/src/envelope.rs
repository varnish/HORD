//! HORD message envelope (spec section 12.2).
//!
//! Every RDMA send carries one envelope followed by its payload:
//!
//! ```text
//! 0       4       length   (payload bytes, u32 BE)
//! 4       2       credits  (flow-control credits granted to peer, u16 BE)
//! 6       2       flags    (u16 BE)
//! 8       ...     payload
//! ```
//!
//! Integers are big-endian, matching the handshake.

/// Envelope header size in bytes.
pub const ENVELOPE_LEN: usize = 8;

pub mod flags {
    /// Payload is empty; the message exists only to replenish credits.
    pub const CREDIT_ONLY: u16 = 1 << 0;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    pub length: u32,
    pub credits: u16,
    pub flags: u16,
}

impl Envelope {
    pub fn is_credit_only(&self) -> bool {
        self.flags & flags::CREDIT_ONLY != 0
    }

    /// Write the 8-byte header into `out` (must be at least `ENVELOPE_LEN`).
    pub fn encode(&self, out: &mut [u8]) {
        out[0..4].copy_from_slice(&self.length.to_be_bytes());
        out[4..6].copy_from_slice(&self.credits.to_be_bytes());
        out[6..8].copy_from_slice(&self.flags.to_be_bytes());
    }

    /// Parse an 8-byte header from the front of `buf`.
    pub fn decode(buf: &[u8]) -> Envelope {
        debug_assert!(buf.len() >= ENVELOPE_LEN);
        Envelope {
            length: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
            credits: u16::from_be_bytes(buf[4..6].try_into().unwrap()),
            flags: u16::from_be_bytes(buf[6..8].try_into().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let e = Envelope {
            length: 1234,
            credits: 7,
            flags: flags::CREDIT_ONLY,
        };
        let mut buf = [0u8; ENVELOPE_LEN];
        e.encode(&mut buf);
        assert_eq!(Envelope::decode(&buf), e);
        assert!(e.is_credit_only());
    }
}
