//! Shared test-support for the `hord-async` integration tests.
//!
//! Each file under `tests/` compiles as its own crate, so before this module
//! the current-thread runtime builder and the payload-pattern helpers were
//! copy-pasted into every one of them (six identical `current_thread_rt`, four
//! identical `pattern_byte`). Centralising them here removes that drift surface.
//! `#![allow(dead_code)]` keeps a test crate that pulls in only a subset quiet —
//! each `tests/*.rs` is a separate compilation, so the unused helpers in it
//! would otherwise warn.

#![allow(dead_code)]

/// A current-thread tokio runtime with all drivers enabled — the executor the
/// `!Send` `AsyncHordStream` (and the listener's per-core workers) run on.
pub fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

/// Deterministic, position-sensitive payload byte (matches the demo's pattern).
pub fn pattern_byte(i: usize) -> u8 {
    (i % 251) as u8
}

/// [`pattern_byte`] materialised into an `n`-byte buffer.
pub fn pattern_vec(n: usize) -> Vec<u8> {
    (0..n).map(pattern_byte).collect()
}

/// Position-sensitive byte pattern, distinct per `seed`, so each side of a
/// duplex transfer verifies exactly what the peer sent (an LCG, matching
/// `full_duplex_bulk`).
pub fn pattern(len: usize, seed: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed as u32 | 1;
    for _ in 0..len {
        x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        out.push((x >> 16) as u8);
    }
    out
}
