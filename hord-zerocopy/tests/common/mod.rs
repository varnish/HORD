//! Shared test-support for the `hord-zerocopy` integration tests.
//!
//! Each file under `tests/` compiles as its own crate, so anything they share
//! has to live in a module like this one or be copy-pasted into every file.
//! `#![allow(dead_code)]` keeps a test crate that pulls in only a subset quiet.

#![allow(dead_code)]

/// The RDMA device IP the loopback tests dial: `$HORD_TEST_IP`, falling back to
/// the RFC 5737 documentation address so that no host IP is baked into the tree.
/// See CLAUDE.md for pointing this at the dev host's `rxe0`.
pub static TEST_IP: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    std::env::var("HORD_TEST_IP").unwrap_or_else(|_| "192.0.2.1".to_string())
});
