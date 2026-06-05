//! HORD RDMA transport layer.
//!
//! Safe-ish Rust wrappers over the C shim (`csrc/shim.c`) which in turn drives
//! `librdmacm` + `libibverbs`. This crate knows nothing about HTTP or the HORD
//! wire protocol; it only manages RC queue pairs, memory regions and
//! completions. The HORD envelope, credits and byte-stream live in `hord-stream`.
//!
//! Connection setup is two-phase to let the caller pre-post receive buffers
//! before the QP can carry traffic:
//!
//! ```text
//! server: Listener::accept() -> Connection (+ peer handshake)
//!           register MRs, post_recv * N
//!         Connection::accept_finish(my_handshake)
//!
//! client: Connection::connect()   -> Connection
//!           register MRs, post_recv * N
//!         Connection::connect_finish(my_handshake) -> peer handshake
//! ```
//!
//! Everything here is synchronous and blocking, which is all the first
//! prototype needs. The completion model is busy-poll ([`Connection::poll`]).

use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int, c_void};

mod ffi {
    use std::os::raw::{c_char, c_int, c_void};

    #[repr(C)]
    pub struct HordListener {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct HordConn {
        _private: [u8; 0],
    }
    #[repr(C)]
    pub struct IbvMr {
        _private: [u8; 0],
    }

    extern "C" {
        pub fn hord_listen(
            ip: *const c_char,
            port: u16,
            err: *mut c_char,
            errlen: usize,
        ) -> *mut HordListener;

        pub fn hord_accept_begin(
            l: *mut HordListener,
            send_wr: c_int,
            recv_wr: c_int,
            cqe: c_int,
            peer_priv: *mut u8,
            peer_priv_cap: usize,
            peer_priv_len: *mut u32,
            err: *mut c_char,
            errlen: usize,
        ) -> *mut HordConn;

        pub fn hord_accept_finish(
            c: *mut HordConn,
            my_priv: *const u8,
            my_priv_len: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;

        pub fn hord_connect_begin(
            ip: *const c_char,
            port: u16,
            send_wr: c_int,
            recv_wr: c_int,
            cqe: c_int,
            err: *mut c_char,
            errlen: usize,
        ) -> *mut HordConn;

        pub fn hord_connect_finish(
            c: *mut HordConn,
            my_priv: *const u8,
            my_priv_len: u32,
            peer_priv: *mut u8,
            peer_priv_cap: usize,
            peer_priv_len: *mut u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;

        pub fn hord_reg_mr(
            c: *mut HordConn,
            addr: *mut c_void,
            length: usize,
            access: c_int,
            err: *mut c_char,
            errlen: usize,
        ) -> *mut IbvMr;
        pub fn hord_mr_lkey(mr: *mut IbvMr) -> u32;
        pub fn hord_mr_rkey(mr: *mut IbvMr) -> u32;
        pub fn hord_dereg_mr(mr: *mut IbvMr);

        pub fn hord_post_recv(
            c: *mut HordConn,
            wr_id: u64,
            addr: *mut c_void,
            length: u32,
            lkey: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;
        pub fn hord_post_send(
            c: *mut HordConn,
            wr_id: u64,
            addr: *mut c_void,
            length: u32,
            lkey: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;
        pub fn hord_poll(
            c: *mut HordConn,
            wr_id: *mut u64,
            byte_len: *mut u32,
            opcode: *mut u32,
            status: *mut u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;

        pub fn hord_disconnect(c: *mut HordConn);
        pub fn hord_conn_shutdown(c: *mut HordConn);
        pub fn hord_conn_free(c: *mut HordConn);
        pub fn hord_listener_free(l: *mut HordListener);
    }
}

/// IBV_ACCESS_LOCAL_WRITE — the only MR access flag the stream path needs.
pub const ACCESS_LOCAL_WRITE: i32 = 1;

const ERRBUF: usize = 256;

/// Work-completion opcode, as reported by the NIC. The prototype only ever
/// observes `Send` and `Recv` (no RDMA write / write-with-immediate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    Send,
    Recv,
    Other(u32),
}

impl Opcode {
    fn from_raw(v: u32) -> Self {
        match v {
            0 => Opcode::Send,   // IBV_WC_SEND
            128 => Opcode::Recv, // IBV_WC_RECV
            other => Opcode::Other(other),
        }
    }
}

/// A single work completion drained from the CQ.
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    /// Echo of the `wr_id` supplied to `post_send` / `post_recv`.
    pub wr_id: u64,
    /// Bytes received (recv completions) — meaningless for sends.
    pub byte_len: u32,
    pub opcode: Opcode,
    /// Raw `ibv_wc_status`; `0` is `IBV_WC_SUCCESS`.
    pub status: u32,
}

impl Completion {
    pub fn is_success(&self) -> bool {
        self.status == 0
    }
}

/// Helper: turn the shim's (err buffer, return code) convention into io::Result.
fn check_ptr<T>(p: *mut T, err: &[c_char]) -> io::Result<*mut T> {
    if p.is_null() {
        Err(shim_error(err))
    } else {
        Ok(p)
    }
}

fn check_rc(rc: c_int, err: &[c_char]) -> io::Result<()> {
    if rc < 0 {
        Err(shim_error(err))
    } else {
        Ok(())
    }
}

fn shim_error(err: &[c_char]) -> io::Error {
    // err is a NUL-terminated C string buffer.
    let bytes: Vec<u8> = err
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    let msg = String::from_utf8_lossy(&bytes).into_owned();
    io::Error::other(if msg.is_empty() {
        "unknown RDMA error".to_string()
    } else {
        msg
    })
}

fn errbuf() -> [c_char; ERRBUF] {
    [0; ERRBUF]
}

/// A registered memory region. Holds the lkey/rkey; the backing storage is
/// owned by the caller and must outlive this handle.
pub struct MemoryRegion {
    raw: *mut ffi::IbvMr,
    lkey: u32,
    rkey: u32,
}

impl MemoryRegion {
    pub fn lkey(&self) -> u32 {
        self.lkey
    }
    pub fn rkey(&self) -> u32 {
        self.rkey
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        unsafe { ffi::hord_dereg_mr(self.raw) }
    }
}

/// A listening RDMA endpoint. Accepts one connection at a time.
pub struct Listener {
    raw: *mut ffi::HordListener,
}

impl Listener {
    /// Bind to `ip:port` and start listening.
    pub fn bind(ip: &str, port: u16) -> io::Result<Listener> {
        let c_ip = CString::new(ip).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "ip contained a NUL byte")
        })?;
        let mut err = errbuf();
        let raw = unsafe {
            ffi::hord_listen(c_ip.as_ptr(), port, err.as_mut_ptr(), err.len())
        };
        Ok(Listener {
            raw: check_ptr(raw, &err)?,
        })
    }

    /// Block until a peer requests a connection, returning a not-yet-accepted
    /// [`Connection`] plus the peer's handshake bytes (CM private data).
    ///
    /// Register receive buffers and call [`Connection::post_recv`] on the
    /// returned connection, then [`Connection::accept_finish`].
    pub fn accept(
        &self,
        send_wr: usize,
        recv_wr: usize,
        handshake_cap: usize,
    ) -> io::Result<(Connection, Vec<u8>)> {
        let mut peer = vec![0u8; handshake_cap];
        let mut peer_len: u32 = 0;
        let mut err = errbuf();
        let cqe = (send_wr + recv_wr + 16) as c_int;
        let raw = unsafe {
            ffi::hord_accept_begin(
                self.raw,
                send_wr as c_int,
                recv_wr as c_int,
                cqe,
                peer.as_mut_ptr(),
                peer.len(),
                &mut peer_len,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        let raw = check_ptr(raw, &err)?;
        peer.truncate(peer_len as usize);
        Ok((Connection { raw, role: Role::Server }, peer))
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        unsafe { ffi::hord_listener_free(self.raw) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

/// An RC connection: a QP, its CQ and PD. Carries the byte stream once
/// `*_finish` has completed.
pub struct Connection {
    raw: *mut ffi::HordConn,
    role: Role,
}

// The connection owns its RDMA resources and is only ever driven from one
// thread at a time by the stream layer; moving it across threads is fine.
unsafe impl Send for Connection {}

impl Connection {
    /// Client side: resolve + create the endpoint. Not yet connected — post
    /// receives, then call [`Connection::connect_finish`].
    pub fn connect(
        ip: &str,
        port: u16,
        send_wr: usize,
        recv_wr: usize,
    ) -> io::Result<Connection> {
        let c_ip = CString::new(ip).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "ip contained a NUL byte")
        })?;
        let mut err = errbuf();
        let cqe = (send_wr + recv_wr + 16) as c_int;
        let raw = unsafe {
            ffi::hord_connect_begin(
                c_ip.as_ptr(),
                port,
                send_wr as c_int,
                recv_wr as c_int,
                cqe,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        Ok(Connection {
            raw: check_ptr(raw, &err)?,
            role: Role::Client,
        })
    }

    /// Client side: connect, sending `handshake`, and return the peer's
    /// handshake bytes once ESTABLISHED.
    pub fn connect_finish(
        &self,
        handshake: &[u8],
        handshake_cap: usize,
    ) -> io::Result<Vec<u8>> {
        debug_assert_eq!(self.role, Role::Client);
        let mut peer = vec![0u8; handshake_cap];
        let mut peer_len: u32 = 0;
        let mut err = errbuf();
        let rc = unsafe {
            ffi::hord_connect_finish(
                self.raw,
                handshake.as_ptr(),
                handshake.len() as u32,
                peer.as_mut_ptr(),
                peer.len(),
                &mut peer_len,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        check_rc(rc, &err)?;
        peer.truncate(peer_len as usize);
        Ok(peer)
    }

    /// Server side: accept the connection, sending `handshake`, and block
    /// until ESTABLISHED.
    pub fn accept_finish(&self, handshake: &[u8]) -> io::Result<()> {
        debug_assert_eq!(self.role, Role::Server);
        let mut err = errbuf();
        let rc = unsafe {
            ffi::hord_accept_finish(
                self.raw,
                handshake.as_ptr(),
                handshake.len() as u32,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        check_rc(rc, &err)
    }

    /// Register `buf` as a memory region with the given access flags.
    ///
    /// # Safety
    /// The returned [`MemoryRegion`] records the NIC registration but does not
    /// own or borrow `buf`. The caller MUST ensure the backing storage stays
    /// valid and pinned (not moved or freed) until the `MemoryRegion` — and any
    /// work request referencing it — is dropped, and that the `MemoryRegion` is
    /// dropped before this `Connection` (whose PD owns the registration). The
    /// type system does not enforce this, hence `unsafe`.
    pub unsafe fn register(&self, buf: &mut [u8], access: i32) -> io::Result<MemoryRegion> {
        let mut err = errbuf();
        let raw = unsafe {
            ffi::hord_reg_mr(
                self.raw,
                buf.as_mut_ptr() as *mut c_void,
                buf.len(),
                access as c_int,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        let raw = check_ptr(raw, &err)?;
        let (lkey, rkey) = unsafe { (ffi::hord_mr_lkey(raw), ffi::hord_mr_rkey(raw)) };
        Ok(MemoryRegion { raw, lkey, rkey })
    }

    /// Post a receive WR over `[addr, addr+length)` (must lie within an MR with
    /// the given `lkey`). Valid in any QP state from INIT onward.
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered memory until the
    /// matching completion is reaped.
    pub unsafe fn post_recv(
        &self,
        wr_id: u64,
        addr: *mut u8,
        length: u32,
        lkey: u32,
    ) -> io::Result<()> {
        let mut err = errbuf();
        let rc = ffi::hord_post_recv(
            self.raw,
            wr_id,
            addr as *mut c_void,
            length,
            lkey,
            err.as_mut_ptr(),
            err.len(),
        );
        check_rc(rc, &err)
    }

    /// Post a signaled send WR over `[addr, addr+length)`. Only valid once the
    /// connection is established (RTS).
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered memory until the
    /// matching send completion is reaped.
    pub unsafe fn post_send(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
    ) -> io::Result<()> {
        let mut err = errbuf();
        let rc = ffi::hord_post_send(
            self.raw,
            wr_id,
            addr as *mut c_void,
            length,
            lkey,
            err.as_mut_ptr(),
            err.len(),
        );
        check_rc(rc, &err)
    }

    /// Poll once for a completion. `Ok(None)` means the CQ was empty.
    pub fn poll(&self) -> io::Result<Option<Completion>> {
        let mut wr_id = 0u64;
        let mut byte_len = 0u32;
        let mut opcode = 0u32;
        let mut status = 0u32;
        let mut err = errbuf();
        let rc = unsafe {
            ffi::hord_poll(
                self.raw,
                &mut wr_id,
                &mut byte_len,
                &mut opcode,
                &mut status,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        if rc < 0 {
            return Err(shim_error(&err));
        }
        if rc == 0 {
            return Ok(None);
        }
        Ok(Some(Completion {
            wr_id,
            byte_len,
            opcode: Opcode::from_raw(opcode),
            status,
        }))
    }

    /// Begin a graceful disconnect. Best-effort.
    pub fn disconnect(&self) {
        unsafe { ffi::hord_disconnect(self.raw) }
    }

    /// Stop the NIC for this connection: disconnect and destroy the QP/CQ.
    /// Idempotent. After this, no further DMA can target registered buffers, so
    /// it is safe to deregister memory regions — which must happen before the
    /// connection is dropped (otherwise the PD leaks; see [`Drop`]).
    pub fn shutdown(&self) {
        unsafe { ffi::hord_conn_shutdown(self.raw) }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // hord_conn_free shuts down (disconnect + destroy QP/CQ) if not already
        // done, then deallocates the PD and destroys the id/event channel.
        // Any MemoryRegions over this connection must already be deregistered.
        unsafe { ffi::hord_conn_free(self.raw) }
    }
}
