//! HORD stream layer — the byte-stream abstraction (spec section 6) and
//! credit-based flow control (section 9) over RDMA send/recv.
//!
//! See [`HordStream`] for the entry point. This crate deliberately excludes the
//! zero-copy / RDMA-write extension (spec section 7); it implements only the
//! two-sided stream path.

pub mod envelope;
pub mod handshake;
mod stream;

pub use stream::{HordConfig, HordStream};

// Re-export the transport handle types so callers only need this crate.
pub use hord_core::{Connection, Listener};
