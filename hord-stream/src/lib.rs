//! HORD stream layer — the byte-stream abstraction (spec section 6) and
//! credit-based flow control (section 9) over RDMA send/recv.
//!
//! See [`HordStream`] for the entry point. This crate implements the two-sided
//! stream path and the *transport half* of the zero-copy extension (spec
//! section 7): the one-sided RDMA-write driver ([`HordStream::rdma_write_all`])
//! and capability negotiation. The zero-copy *HTTP semantics* (the
//! `X-HORD-RDMA-Write` header exchange) live one layer up, in `hord-zerocopy`.

pub mod envelope;
pub mod handshake;
mod stream;

pub use stream::{ConnMeta, ConnTeardown, HordConfig, HordStream, WriteSegment, HANDSHAKE_TIMEOUT};

// Re-export the transport handle types so callers only need this crate.
pub use hord_core::{
    is_connection_setup_failure, is_device_removed, Connection, ConnectionSetupFailed,
    DeviceRemoved, Listener, Mr, RegisteredBuffer, SharedPd, ESTABLISH_TIMEOUT,
};
