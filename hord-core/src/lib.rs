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

use std::cell::UnsafeCell;
use std::ffi::CString;
use std::io;
use std::os::fd::RawFd;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;

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
            rnr_retry_count: u8,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;

        pub fn hord_connect_begin(
            ip: *const c_char,
            port: u16,
            send_wr: c_int,
            recv_wr: c_int,
            cqe: c_int,
            resolve_timeout_ms: c_int,
            err: *mut c_char,
            errlen: usize,
        ) -> *mut HordConn;

        pub fn hord_connect_finish(
            c: *mut HordConn,
            my_priv: *const u8,
            my_priv_len: u32,
            retry_count: u8,
            rnr_retry_count: u8,
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
        pub fn hord_post_write(
            c: *mut HordConn,
            wr_id: u64,
            addr: *mut c_void,
            length: u32,
            lkey: u32,
            remote_addr: u64,
            rkey: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;
        pub fn hord_post_write_with_imm(
            c: *mut HordConn,
            wr_id: u64,
            addr: *mut c_void,
            length: u32,
            lkey: u32,
            remote_addr: u64,
            rkey: u32,
            imm: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;
        pub fn hord_poll(
            c: *mut HordConn,
            wr_id: *mut u64,
            byte_len: *mut u32,
            opcode: *mut u32,
            status: *mut u32,
            imm_data: *mut u32,
            err: *mut c_char,
            errlen: usize,
        ) -> c_int;

        pub fn hord_conn_cq_fd(c: *mut HordConn) -> c_int;
        pub fn hord_cq_arm(c: *mut HordConn, err: *mut c_char, errlen: usize) -> c_int;
        pub fn hord_cq_consume(c: *mut HordConn) -> c_int;
        pub fn hord_conn_cm_fd(c: *mut HordConn) -> c_int;
        pub fn hord_conn_cm_set_nonblock(c: *mut HordConn) -> c_int;
        pub fn hord_conn_check_disconnect(c: *mut HordConn) -> c_int;

        pub fn hord_disconnect(c: *mut HordConn);
        pub fn hord_conn_shutdown(c: *mut HordConn);
        pub fn hord_conn_free(c: *mut HordConn);
        pub fn hord_listener_free(l: *mut HordListener);
    }
}

/// IBV_ACCESS_LOCAL_WRITE — the only MR access flag the stream path needs.
pub const ACCESS_LOCAL_WRITE: i32 = 1;

/// IBV_ACCESS_REMOTE_WRITE — lets a peer RDMA-write into this MR. Used for the
/// zero-copy extension's client destination buffer. Per IBA, remote write also
/// requires local write, so register such buffers with
/// `ACCESS_LOCAL_WRITE | ACCESS_REMOTE_WRITE`.
pub const ACCESS_REMOTE_WRITE: i32 = 2;

const ERRBUF: usize = 256;

/// Connection-manager retry / timeout parameters (#11). The defaults reproduce
/// the values the prototype hardcoded before they were made tunable, so they
/// change no existing behaviour; the async layer can lower them to bound how
/// long a stalled peer holds a connection.
#[derive(Debug, Clone, Copy)]
pub struct CmParams {
    /// Transport retry count on connect (initiator side). Valid range 0..=7.
    pub retry_count: u8,
    /// Receiver-not-ready retry count. 7 means infinite RNR retry.
    pub rnr_retry_count: u8,
    /// Timeout (ms) for each of the address/route resolution steps on connect.
    pub resolve_timeout_ms: i32,
}

impl Default for CmParams {
    fn default() -> Self {
        CmParams {
            retry_count: 7,
            rnr_retry_count: 7,
            resolve_timeout_ms: 2000,
        }
    }
}

/// Work-completion opcode, as reported by the NIC. The stream path observes
/// `Send` and `Recv`; the zero-copy extension adds `RdmaWrite` (the sender's
/// completion for a one-sided RDMA write). Protocol splitting (§7.7) adds
/// `RecvRdmaWithImm` — the *receiver's* completion for a write-with-immediate,
/// which consumes a posted recv WR and carries the immediate in
/// [`Completion::imm_data`]. (The sender of a write-with-immediate still reaps
/// an `RdmaWrite` completion.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opcode {
    Send,
    RdmaWrite,
    Recv,
    RecvRdmaWithImm,
    Other(u32),
}

impl Opcode {
    fn from_raw(v: u32) -> Self {
        match v {
            0 => Opcode::Send,             // IBV_WC_SEND
            1 => Opcode::RdmaWrite,        // IBV_WC_RDMA_WRITE
            128 => Opcode::Recv,           // IBV_WC_RECV (1 << 7)
            129 => Opcode::RecvRdmaWithImm, // IBV_WC_RECV_RDMA_WITH_IMM
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
    /// The 32-bit immediate (host order) on a [`Opcode::RecvRdmaWithImm`]
    /// completion; `0` for every other completion kind.
    pub imm_data: u32,
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

/// A heap buffer registered with the NIC for RDMA. Owns its backing storage,
/// the memory-region handle, and a reference to the connection it belongs to.
///
/// This single type closes two soundness holes that a bare MR + `Box<[u8]>`
/// pair left open:
///
/// * **Aliasing.** The NIC DMA-writes some slots while we read/write others in
///   the same allocation. Forming a `&`/`&mut [u8]` over registered memory
///   therefore asserts an exclusivity the NIC violates — UB under the Rust
///   aliasing model. So the storage is `Box<[UnsafeCell<u8>]>` and is *never*
///   sliced as `&[u8]`: every access goes through a raw pointer obtained with
///   [`UnsafeCell::raw_get`] (via [`as_mut_ptr`](Self::as_mut_ptr) or the
///   [`copy_in`](Self::copy_in) / [`copy_out`](Self::copy_out) helpers), which
///   is exactly the sanctioned way to mutate through a shared `UnsafeCell`.
///
/// * **MR/PD lifetime.** The MR belongs to the connection's protection domain,
///   so the PD must outlive the MR. Holding an `Arc<Connection>` makes that a
///   type-system guarantee: the connection (and its PD) cannot be freed while
///   any `RegisteredBuffer` over it is alive, regardless of drop order.
///
/// The one ordering step the type system still cannot express is that the NIC
/// must be stopped (QP destroyed, DMA quiesced) *before* an MR is deregistered.
/// Posting a work request against this buffer is `unsafe` precisely because the
/// caller owns that obligation; in practice the stream layer calls
/// [`Connection::shutdown`] before dropping its buffers.
pub struct RegisteredBuffer {
    // `_conn` keeps the PD alive until after this buffer's MR is deregistered.
    // It is dropped (decrementing the refcount) only after the `Drop` body below
    // has already torn down the MR, so the dereg-before-PD-free order holds for
    // free, independent of where this struct sits in any owner's field order.
    _conn: Arc<Connection>,
    // The registered storage. Never sliced as `&[u8]`; reached only via
    // `UnsafeCell::raw_get(storage.as_ptr())`. Reading `storage` here (for the
    // base pointer and length) is what makes it a live field, not dead weight.
    storage: Box<[UnsafeCell<u8>]>,
    raw: *mut ffi::IbvMr,
    lkey: u32,
    rkey: u32,
}

impl RegisteredBuffer {
    /// Base pointer of the registered region. Derived fresh from the storage's
    /// `UnsafeCell` so no `&`/`&mut [u8]` is ever formed over memory the NIC may
    /// be DMA-ing into. The allocation never moves (it lives behind a `Box`), so
    /// the address is stable for the buffer's whole life.
    pub fn as_mut_ptr(&self) -> *mut u8 {
        UnsafeCell::raw_get(self.storage.as_ptr())
    }

    /// Registered length in bytes.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn lkey(&self) -> u32 {
        self.lkey
    }
    pub fn rkey(&self) -> u32 {
        self.rkey
    }

    /// Copy `dst.len()` bytes out of the registered region starting at `off`.
    ///
    /// Reads through a raw pointer — no slice reference is formed over the
    /// registered memory. The caller must ensure the NIC is not concurrently
    /// DMA-ing into `[off, off+dst.len())`; the stream upholds this by only
    /// reading a receive slot after its completion is reaped and before it is
    /// re-posted.
    pub fn copy_out(&self, off: usize, dst: &mut [u8]) {
        assert!(
            dst.len() <= self.len() && off <= self.len() - dst.len(),
            "copy_out out of bounds",
        );
        // SAFETY: bounds checked above; `dst` is the caller's separate buffer so
        // the ranges do not overlap; the source pointer is valid for `len` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(self.as_mut_ptr().add(off), dst.as_mut_ptr(), dst.len());
        }
    }

    /// Copy `src` into the registered region starting at `off`.
    ///
    /// Writes through a raw pointer — no slice reference is formed over the
    /// registered memory. The caller must ensure the NIC is not concurrently
    /// accessing `[off, off+src.len())`; the stream upholds this by only writing
    /// a send slot it currently owns (not posted to the NIC).
    pub fn copy_in(&self, off: usize, src: &[u8]) {
        assert!(
            src.len() <= self.len() && off <= self.len() - src.len(),
            "copy_in out of bounds",
        );
        // SAFETY: bounds checked above; `src` does not overlap the region; the
        // destination pointer is valid for `len` bytes and writes are sound
        // because the storage lives behind `UnsafeCell`.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.as_mut_ptr().add(off), src.len());
        }
    }
}

impl Drop for RegisteredBuffer {
    fn drop(&mut self) {
        // Deregister the MR while the PD is still alive (guaranteed: we still
        // hold `_conn`, dropped only after this body). The NIC must already be
        // stopped — see the type-level docs.
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
        cm: CmParams,
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
        Ok((Connection { raw, role: Role::Server, cm }, peer))
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
    cm: CmParams,
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
        cm: CmParams,
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
                cm.resolve_timeout_ms as c_int,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        Ok(Connection {
            raw: check_ptr(raw, &err)?,
            role: Role::Client,
            cm,
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
                self.cm.retry_count,
                self.cm.rnr_retry_count,
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
                self.cm.rnr_retry_count,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        check_rc(rc, &err)
    }

    /// Allocate `len` zeroed bytes and register them as a memory region with the
    /// given access flags, returning a [`RegisteredBuffer`] that owns both the
    /// storage and the registration.
    ///
    /// This is safe: the returned buffer pins its own backing storage (so it
    /// cannot move or be freed early) and holds an `Arc<Connection>` (so the
    /// registration cannot outlive the PD). Posting work requests against the
    /// buffer is still `unsafe` — see [`Connection::post_recv`] /
    /// [`Connection::post_send`] — and the caller must stop the NIC before the
    /// buffer is dropped (see [`RegisteredBuffer`]).
    pub fn register_buffer(self: &Arc<Self>, len: usize, access: i32) -> io::Result<RegisteredBuffer> {
        // `Box<[UnsafeCell<u8>]>`: the registered storage is never sliced as
        // `&[u8]`, so the NIC may DMA into it while we touch other regions
        // through raw pointers without violating the aliasing model.
        //
        // Allocated zeroed via `vec![0u8; len]` (which lowers to `alloc_zeroed`,
        // i.e. lazily-zeroed OS pages on Linux) rather than an eager per-element
        // `(0..len).map(|_| UnsafeCell::new(0u8)).collect()` memset. For an
        // RDMA-write *source* the caller overwrites the whole region immediately,
        // so an eager zeroing pass over it is wasted work; receive / destination
        // buffers still see the zeros they rely on.
        let storage: Box<[UnsafeCell<u8>]> = {
            let zeroed: Box<[u8]> = vec![0u8; len].into_boxed_slice();
            // `UnsafeCell<u8>` is `#[repr(transparent)]` over `u8`: identical
            // size/alignment, and an initialized `0u8` is a valid `UnsafeCell<u8>`.
            let data = Box::into_raw(zeroed) as *mut UnsafeCell<u8>;
            let slice = std::ptr::slice_from_raw_parts_mut(data, len);
            // SAFETY: `data` is the (non-null, aligned) base of a `len`-element
            // allocation of `u8`, reinterpreted as layout-identical
            // `UnsafeCell<u8>`; the allocation's `Layout` is unchanged, so
            // rebuilding the `Box` under the new element type — and freeing it as
            // such on drop — is sound.
            unsafe { Box::from_raw(slice) }
        };
        // Base pointer for registration; valid for the whole life of the box.
        let ptr = UnsafeCell::raw_get(storage.as_ptr());
        let mut err = errbuf();
        let raw = unsafe {
            ffi::hord_reg_mr(
                self.raw,
                ptr as *mut c_void,
                len,
                access as c_int,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        let raw = check_ptr(raw, &err)?;
        let (lkey, rkey) = unsafe { (ffi::hord_mr_lkey(raw), ffi::hord_mr_rkey(raw)) };
        Ok(RegisteredBuffer {
            _conn: Arc::clone(self),
            storage,
            raw,
            lkey,
            rkey,
        })
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

    /// Post a signaled one-sided RDMA write: copy `[addr, addr+length)` (local,
    /// in an MR with `lkey`) into the peer's memory at `remote_addr`, authorized
    /// by `rkey` (an rkey the peer registered with `ACCESS_REMOTE_WRITE` and
    /// advertised to us). Only valid once the connection is established (RTS).
    /// The completion carries [`Opcode::RdmaWrite`]; the peer posts no receive
    /// and observes nothing.
    ///
    /// # Safety
    /// `addr`/`length` must reference live, registered local memory until the
    /// matching completion is reaped. `remote_addr`/`rkey` must describe a live
    /// remote region the peer authorized for remote write; a stale or wrong rkey
    /// transitions the QP to the error state (closing the connection).
    pub unsafe fn post_write(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
    ) -> io::Result<()> {
        let mut err = errbuf();
        let rc = ffi::hord_post_write(
            self.raw,
            wr_id,
            addr as *mut c_void,
            length,
            lkey,
            remote_addr,
            rkey,
            err.as_mut_ptr(),
            err.len(),
        );
        check_rc(rc, &err)
    }

    /// Post a one-sided RDMA write-with-immediate (§7.7 protocol splitting):
    /// like [`post_write`](Self::post_write), but atomically delivers `imm`
    /// (host order) to the peer's CQ as a [`Opcode::RecvRdmaWithImm`] completion,
    /// consuming one of the peer's posted receive WRs. `length` may be `0` (the
    /// WR then carries only the immediate). The local completion the sender reaps
    /// is still an [`Opcode::RdmaWrite`].
    ///
    /// # Safety
    /// Same contract as [`post_write`](Self::post_write); additionally the peer
    /// MUST have a receive WR posted, or the write fails with RNR and the QP
    /// transitions to the error state.
    // One argument over clippy's default threshold: this mirrors the verbs WR
    // fields one-to-one (the sibling `post_write` sits exactly at the limit).
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn post_write_with_imm(
        &self,
        wr_id: u64,
        addr: *const u8,
        length: u32,
        lkey: u32,
        remote_addr: u64,
        rkey: u32,
        imm: u32,
    ) -> io::Result<()> {
        let mut err = errbuf();
        let rc = ffi::hord_post_write_with_imm(
            self.raw,
            wr_id,
            addr as *mut c_void,
            length,
            lkey,
            remote_addr,
            rkey,
            imm,
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
        let mut imm_data = 0u32;
        let mut err = errbuf();
        let rc = unsafe {
            ffi::hord_poll(
                self.raw,
                &mut wr_id,
                &mut byte_len,
                &mut opcode,
                &mut status,
                &mut imm_data,
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
            imm_data,
        }))
    }

    /// File descriptor of the CQ completion channel. Once the CQ is armed (see
    /// [`arm_cq`](Self::arm_cq)) it becomes readable when a completion is
    /// signalled — register it with an async reactor instead of busy-polling
    /// [`poll`](Self::poll). Owned by the connection; valid until shutdown.
    pub fn cq_fd(&self) -> io::Result<RawFd> {
        let fd = unsafe { ffi::hord_conn_cq_fd(self.raw) };
        if fd < 0 {
            return Err(io::Error::other("connection has no completion channel"));
        }
        Ok(fd)
    }

    /// Arm the CQ to signal its completion channel on the next completion.
    /// Notifications are one-shot, so the sequence is: arm → wait on
    /// [`cq_fd`](Self::cq_fd) → [`consume_cq_events`](Self::consume_cq_events) →
    /// re-arm → drain with [`poll`](Self::poll). Re-arming before the final
    /// drain closes the race where a completion lands between drain and arm.
    pub fn arm_cq(&self) -> io::Result<()> {
        let mut err = errbuf();
        let rc = unsafe { ffi::hord_cq_arm(self.raw, err.as_mut_ptr(), err.len()) };
        check_rc(rc, &err)
    }

    /// Drain and acknowledge all pending completion-channel notifications (the
    /// fd is non-blocking). Returns the number consumed. Acknowledging is
    /// required before the CQ can be destroyed.
    pub fn consume_cq_events(&self) -> usize {
        let n = unsafe { ffi::hord_cq_consume(self.raw) };
        if n < 0 {
            0
        } else {
            n as usize
        }
    }

    /// File descriptor of the connection's CM event channel.
    pub fn cm_fd(&self) -> io::Result<RawFd> {
        let fd = unsafe { ffi::hord_conn_cm_fd(self.raw) };
        if fd < 0 {
            return Err(io::Error::other("connection has no CM channel"));
        }
        Ok(fd)
    }

    /// Make the CM channel non-blocking. Call only *after* the handshake — setup
    /// relies on blocking CM waits.
    pub fn set_cm_nonblock(&self) -> io::Result<()> {
        let rc = unsafe { ffi::hord_conn_cm_set_nonblock(self.raw) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Non-blocking check for a peer-initiated teardown (DISCONNECTED / device
    /// removal / connect error). Requires [`set_cm_nonblock`](Self::set_cm_nonblock)
    /// first. `Ok(true)` means the peer is gone.
    pub fn check_disconnect(&self) -> io::Result<bool> {
        let rc = unsafe { ffi::hord_conn_check_disconnect(self.raw) };
        if rc < 0 {
            return Err(io::Error::other("CM disconnect poll failed"));
        }
        Ok(rc == 1)
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
