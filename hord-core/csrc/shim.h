/*
 * HORD transport shim.
 *
 * A thin, dependency-free C layer over librdmacm + libibverbs. It exists for
 * two reasons:
 *
 *   1. The verbs data-path entry points (ibv_post_send / ibv_post_recv /
 *      ibv_poll_cq) and several rdma_cm helpers are `static inline` in the
 *      rdma-core headers, so they are not exported symbols a Rust FFI layer
 *      could bind to. The shim gives them real, linkable addresses.
 *
 *   2. It lets Rust deal exclusively in opaque handles, scalars and byte
 *      buffers — never the layout-sensitive verbs/CM structs. That removes a
 *      whole class of FFI struct-layout bugs.
 *
 * The shim is intentionally synchronous and single-connection-at-a-time, which
 * is all the first prototype needs. Connection setup is two-phase
 * (begin -> [caller registers MRs and posts receives] -> finish) so the caller
 * can pre-post receive buffers before the QP becomes usable, avoiding an
 * initial receiver-not-ready (RNR) storm.
 *
 * Every fallible call takes an (err, errlen) pair. On failure a human-readable
 * message is written there and the function returns NULL / a negative value.
 */
#ifndef HORD_SHIM_H
#define HORD_SHIM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque to callers. Defined in shim.c. */
typedef struct hord_listener hord_listener;
typedef struct hord_conn hord_conn;

/* ibv_mr is a real verbs struct; callers treat the pointer as opaque and use
 * the accessors below. */
struct ibv_mr;

/* Work-completion opcode/status values the caller cares about. These mirror
 * the libibverbs enums (stable ABI values) so Rust need not include verbs.h. */
#define HORD_WC_STATUS_SUCCESS 0u
#define HORD_WC_OPCODE_SEND    0u   /* IBV_WC_SEND */
#define HORD_WC_OPCODE_RECV    128u /* IBV_WC_RECV */

/* IBV_ACCESS_LOCAL_WRITE — the only access flag the stream path needs. */
#define HORD_ACCESS_LOCAL_WRITE 1

/* ---- Server ---------------------------------------------------------- */

/* Bind to ip:port and start listening. */
hord_listener *hord_listen(const char *ip, uint16_t port,
                           char *err, size_t errlen);

/* Block for the next connection request, then create this connection's
 * PD/CQ/QP. The peer's handshake (CM private data) is copied into peer_priv;
 * *peer_priv_len receives the byte count actually present. The returned
 * connection is NOT yet accepted — register MRs and post receives, then call
 * hord_accept_finish(). */
hord_conn *hord_accept_begin(hord_listener *l,
                             int send_wr, int recv_wr, int cqe,
                             uint8_t *peer_priv, size_t peer_priv_cap,
                             uint32_t *peer_priv_len,
                             char *err, size_t errlen);

/* Accept the connection, sending our handshake as CM private data, and block
 * until the connection is ESTABLISHED. */
int hord_accept_finish(hord_conn *c,
                       const uint8_t *my_priv, uint32_t my_priv_len,
                       char *err, size_t errlen);

/* ---- Client ---------------------------------------------------------- */

/* Resolve address + route to ip:port and create the PD/CQ/QP. The connection
 * is NOT yet connected — register MRs and post receives, then call
 * hord_connect_finish(). */
hord_conn *hord_connect_begin(const char *ip, uint16_t port,
                              int send_wr, int recv_wr, int cqe,
                              char *err, size_t errlen);

/* Connect, sending our handshake as CM private data, block until ESTABLISHED,
 * and copy the peer's handshake into peer_priv. */
int hord_connect_finish(hord_conn *c,
                        const uint8_t *my_priv, uint32_t my_priv_len,
                        uint8_t *peer_priv, size_t peer_priv_cap,
                        uint32_t *peer_priv_len,
                        char *err, size_t errlen);

/* ---- Memory regions -------------------------------------------------- */

struct ibv_mr *hord_reg_mr(hord_conn *c, void *addr, size_t length,
                           int access, char *err, size_t errlen);
uint32_t hord_mr_lkey(struct ibv_mr *mr);
uint32_t hord_mr_rkey(struct ibv_mr *mr);
void hord_dereg_mr(struct ibv_mr *mr);

/* ---- Data path ------------------------------------------------------- */

/* Post a single-SGE receive. wr_id is echoed back on completion. */
int hord_post_recv(hord_conn *c, uint64_t wr_id, void *addr, uint32_t length,
                   uint32_t lkey, char *err, size_t errlen);

/* Post a single-SGE, signaled send. wr_id is echoed back on completion. */
int hord_post_send(hord_conn *c, uint64_t wr_id, void *addr, uint32_t length,
                   uint32_t lkey, char *err, size_t errlen);

/* Poll for one completion. Returns 1 and fills the out-params if a completion
 * was available, 0 if the CQ was empty, or -1 on a poll error. A retrieved
 * completion may still carry a non-success status in *status. */
int hord_poll(hord_conn *c, uint64_t *wr_id, uint32_t *byte_len,
              uint32_t *opcode, uint32_t *status, char *err, size_t errlen);

/* ---- Teardown -------------------------------------------------------- */

/* Best-effort graceful disconnect (rdma_disconnect). Safe to call once. */
void hord_disconnect(hord_conn *c);

/* Stop the NIC: disconnect + destroy the QP and CQ. Idempotent. Leaves the PD
 * alive so the caller can still deregister memory regions belonging to it.
 * Call this, then deregister MRs, then hord_conn_free. */
void hord_conn_shutdown(hord_conn *c);

/* Destroy the (remaining) PD/id/event-channel and free the handle. Runs
 * hord_conn_shutdown first if needed. The caller MUST have deregistered all
 * memory regions before calling this, or the PD will leak. */
void hord_conn_free(hord_conn *c);

void hord_listener_free(hord_listener *l);

#ifdef __cplusplus
}
#endif

#endif /* HORD_SHIM_H */
