/* HORD transport shim — see shim.h for the contract. */
#include "shim.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

struct hord_listener {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
};

struct hord_conn {
    struct rdma_event_channel *ec; /* per-connection channel (migrated/owned) */
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_comp_channel *comp_channel; /* CQ completion channel (pollable fd) */
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    int disconnected;
};

/* Set O_NONBLOCK on an fd. Returns 0 on success, -1 on failure. */
static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ---- error helpers --------------------------------------------------- */

/* message + errno */
static void set_err(char *err, size_t errlen, const char *msg) {
    if (err && errlen)
        snprintf(err, errlen, "%s: %s", msg, strerror(errno));
}

/* message only */
static void set_msg(char *err, size_t errlen, const char *msg) {
    if (err && errlen)
        snprintf(err, errlen, "%s", msg);
}

/* ---- address helpers ------------------------------------------------- */

static int fill_sockaddr(const char *ip, uint16_t port,
                         struct sockaddr_in *sa,
                         char *err, size_t errlen) {
    memset(sa, 0, sizeof(*sa));
    sa->sin_family = AF_INET;
    sa->sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &sa->sin_addr) != 1) {
        set_msg(err, errlen, "invalid IPv4 address");
        return -1;
    }
    return 0;
}

/* Wait for a single CM event, require it to match `expected`, and ack it.
 * Returns 0 on the expected event, -1 otherwise. */
static int expect_event(struct rdma_event_channel *ec,
                        enum rdma_cm_event_type expected,
                        struct rdma_cm_event **out,
                        char *err, size_t errlen) {
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(ec, &ev)) {
        set_err(err, errlen, "rdma_get_cm_event");
        return -1;
    }
    if (ev->event != expected) {
        char buf[160];
        snprintf(buf, sizeof(buf), "unexpected CM event %s (wanted %s)",
                 rdma_event_str(ev->event), rdma_event_str(expected));
        set_msg(err, errlen, buf);
        rdma_ack_cm_event(ev);
        return -1;
    }
    if (out)
        *out = ev; /* caller must ack */
    else
        rdma_ack_cm_event(ev);
    return 0;
}

/* Standard single-SGE RC QP attributes, CQ shared for send + recv. */
static void init_qp_attr(struct ibv_qp_init_attr *a, struct ibv_cq *cq,
                         int send_wr, int recv_wr) {
    memset(a, 0, sizeof(*a));
    a->send_cq = cq;
    a->recv_cq = cq;
    a->cap.max_send_wr = (uint32_t)send_wr;
    a->cap.max_recv_wr = (uint32_t)recv_wr;
    a->cap.max_send_sge = 1;
    a->cap.max_recv_sge = 1;
    a->qp_type = IBV_QPT_RC;
    a->sq_sig_all = 0; /* we set IBV_SEND_SIGNALED per-WR */
}

/* Build PD/CQ/QP on an id whose device (id->verbs) is already resolved. */
static int build_endpoint(struct hord_conn *c, int send_wr, int recv_wr,
                          int cqe, char *err, size_t errlen) {
    c->pd = ibv_alloc_pd(c->id->verbs);
    if (!c->pd) {
        set_err(err, errlen, "ibv_alloc_pd");
        return -1;
    }
    /* Completion channel: gives the CQ a pollable fd. The async layer arms the
     * CQ (ibv_req_notify_cq), waits on this fd, then drains — instead of
     * busy-polling. The sync layer ignores it and just polls the CQ directly,
     * which works regardless: an un-armed channel never delivers notifications. */
    c->comp_channel = ibv_create_comp_channel(c->id->verbs);
    if (!c->comp_channel) {
        set_err(err, errlen, "ibv_create_comp_channel");
        return -1;
    }
    /* Non-blocking so ibv_get_cq_event() (a read() under the hood) returns
     * EAGAIN rather than blocking a reactor thread on a spurious wakeup. */
    if (set_nonblock(c->comp_channel->fd)) {
        set_err(err, errlen, "fcntl(comp_channel, O_NONBLOCK)");
        return -1;
    }
    c->cq = ibv_create_cq(c->id->verbs, cqe, NULL, c->comp_channel, 0);
    if (!c->cq) {
        set_err(err, errlen, "ibv_create_cq");
        return -1;
    }
    struct ibv_qp_init_attr attr;
    init_qp_attr(&attr, c->cq, send_wr, recv_wr);
    if (rdma_create_qp(c->id, c->pd, &attr)) {
        set_err(err, errlen, "rdma_create_qp");
        return -1;
    }
    c->qp = c->id->qp;
    return 0;
}

static void copy_peer_priv(const struct rdma_cm_event *ev,
                           uint8_t *peer_priv, size_t cap, uint32_t *out_len) {
    uint32_t n = ev->param.conn.private_data_len;
    const void *src = ev->param.conn.private_data;
    if (!src)
        n = 0;
    if (n > cap)
        n = (uint32_t)cap;
    if (n && peer_priv)
        memcpy(peer_priv, src, n);
    if (out_len)
        *out_len = n;
}

/* ---- server ---------------------------------------------------------- */

hord_listener *hord_listen(const char *ip, uint16_t port,
                           char *err, size_t errlen) {
    struct sockaddr_in sa;
    if (fill_sockaddr(ip, port, &sa, err, errlen))
        return NULL;

    hord_listener *l = calloc(1, sizeof(*l));
    if (!l) {
        set_msg(err, errlen, "out of memory");
        return NULL;
    }
    l->ec = rdma_create_event_channel();
    if (!l->ec) {
        set_err(err, errlen, "rdma_create_event_channel");
        goto fail;
    }
    if (rdma_create_id(l->ec, &l->id, NULL, RDMA_PS_TCP)) {
        set_err(err, errlen, "rdma_create_id");
        goto fail;
    }
    if (rdma_bind_addr(l->id, (struct sockaddr *)&sa)) {
        set_err(err, errlen, "rdma_bind_addr");
        goto fail;
    }
    if (rdma_listen(l->id, 8)) {
        set_err(err, errlen, "rdma_listen");
        goto fail;
    }
    return l;

fail:
    hord_listener_free(l);
    return NULL;
}

hord_conn *hord_accept_begin(hord_listener *l,
                             int send_wr, int recv_wr, int cqe,
                             uint8_t *peer_priv, size_t peer_priv_cap,
                             uint32_t *peer_priv_len,
                             char *err, size_t errlen) {
    struct rdma_cm_event *ev = NULL;
    if (expect_event(l->ec, RDMA_CM_EVENT_CONNECT_REQUEST, &ev, err, errlen))
        return NULL;

    struct rdma_cm_id *cid = ev->id; /* fresh id for this connection */
    copy_peer_priv(ev, peer_priv, peer_priv_cap, peer_priv_len);
    /* private_data is only valid until we ack the event, so it has been
     * copied out above. */

    hord_conn *c = calloc(1, sizeof(*c));
    if (!c) {
        set_msg(err, errlen, "out of memory");
        rdma_ack_cm_event(ev);
        return NULL;
    }
    c->id = cid;

    /* Give the connection its own event channel so its ESTABLISHED /
     * DISCONNECTED events don't race the listener's accept loop. */
    c->ec = rdma_create_event_channel();
    if (!c->ec) {
        set_err(err, errlen, "rdma_create_event_channel");
        rdma_ack_cm_event(ev);
        free(c);
        return NULL;
    }
    if (rdma_migrate_id(cid, c->ec)) {
        set_err(err, errlen, "rdma_migrate_id");
        rdma_ack_cm_event(ev);
        rdma_destroy_event_channel(c->ec);
        free(c);
        return NULL;
    }
    rdma_ack_cm_event(ev);

    if (build_endpoint(c, send_wr, recv_wr, cqe, err, errlen)) {
        hord_conn_free(c);
        return NULL;
    }
    return c;
}

int hord_accept_finish(hord_conn *c,
                       const uint8_t *my_priv, uint32_t my_priv_len,
                       uint8_t rnr_retry_count,
                       char *err, size_t errlen) {
    if (my_priv_len > 255) {
        /* private_data_len is a uint8_t; reject rather than silently truncate. */
        set_msg(err, errlen, "handshake exceeds 255-byte CM private-data limit");
        return -1;
    }
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.private_data = my_priv;
    cp.private_data_len = (uint8_t)my_priv_len;
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.rnr_retry_count = rnr_retry_count; /* caller's choice; 7 == "infinite" */

    if (rdma_accept(c->id, &cp)) {
        set_err(err, errlen, "rdma_accept");
        return -1;
    }
    if (expect_event(c->ec, RDMA_CM_EVENT_ESTABLISHED, NULL, err, errlen))
        return -1;
    return 0;
}

/* ---- client ---------------------------------------------------------- */

hord_conn *hord_connect_begin(const char *ip, uint16_t port,
                              int send_wr, int recv_wr, int cqe,
                              int resolve_timeout_ms,
                              char *err, size_t errlen) {
    struct sockaddr_in sa;
    if (fill_sockaddr(ip, port, &sa, err, errlen))
        return NULL;

    hord_conn *c = calloc(1, sizeof(*c));
    if (!c) {
        set_msg(err, errlen, "out of memory");
        return NULL;
    }
    c->ec = rdma_create_event_channel();
    if (!c->ec) {
        set_err(err, errlen, "rdma_create_event_channel");
        goto fail;
    }
    if (rdma_create_id(c->ec, &c->id, NULL, RDMA_PS_TCP)) {
        set_err(err, errlen, "rdma_create_id");
        goto fail;
    }
    if (rdma_resolve_addr(c->id, NULL, (struct sockaddr *)&sa, resolve_timeout_ms)) {
        set_err(err, errlen, "rdma_resolve_addr");
        goto fail;
    }
    if (expect_event(c->ec, RDMA_CM_EVENT_ADDR_RESOLVED, NULL, err, errlen))
        goto fail;
    if (rdma_resolve_route(c->id, resolve_timeout_ms)) {
        set_err(err, errlen, "rdma_resolve_route");
        goto fail;
    }
    if (expect_event(c->ec, RDMA_CM_EVENT_ROUTE_RESOLVED, NULL, err, errlen))
        goto fail;

    if (build_endpoint(c, send_wr, recv_wr, cqe, err, errlen))
        goto fail;
    return c;

fail:
    hord_conn_free(c);
    return NULL;
}

int hord_connect_finish(hord_conn *c,
                        const uint8_t *my_priv, uint32_t my_priv_len,
                        uint8_t retry_count, uint8_t rnr_retry_count,
                        uint8_t *peer_priv, size_t peer_priv_cap,
                        uint32_t *peer_priv_len,
                        char *err, size_t errlen) {
    if (my_priv_len > 255) {
        /* private_data_len is a uint8_t; reject rather than silently truncate. */
        set_msg(err, errlen, "handshake exceeds 255-byte CM private-data limit");
        return -1;
    }
    struct rdma_conn_param cp;
    memset(&cp, 0, sizeof(cp));
    cp.private_data = my_priv;
    cp.private_data_len = (uint8_t)my_priv_len;
    cp.responder_resources = 1;
    cp.initiator_depth = 1;
    cp.retry_count = retry_count;
    cp.rnr_retry_count = rnr_retry_count;

    if (rdma_connect(c->id, &cp)) {
        set_err(err, errlen, "rdma_connect");
        return -1;
    }
    struct rdma_cm_event *ev = NULL;
    if (expect_event(c->ec, RDMA_CM_EVENT_ESTABLISHED, &ev, err, errlen))
        return -1;
    copy_peer_priv(ev, peer_priv, peer_priv_cap, peer_priv_len);
    rdma_ack_cm_event(ev);
    return 0;
}

/* ---- memory regions -------------------------------------------------- */

struct ibv_mr *hord_reg_mr(hord_conn *c, void *addr, size_t length,
                           int access, char *err, size_t errlen) {
    struct ibv_mr *mr = ibv_reg_mr(c->pd, addr, length, access);
    if (!mr)
        set_err(err, errlen, "ibv_reg_mr");
    return mr;
}

uint32_t hord_mr_lkey(struct ibv_mr *mr) { return mr->lkey; }
uint32_t hord_mr_rkey(struct ibv_mr *mr) { return mr->rkey; }
void hord_dereg_mr(struct ibv_mr *mr) {
    if (mr)
        ibv_dereg_mr(mr);
}

/* ---- data path ------------------------------------------------------- */

int hord_post_recv(hord_conn *c, uint64_t wr_id, void *addr, uint32_t length,
                   uint32_t lkey, char *err, size_t errlen) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)addr,
        .length = length,
        .lkey = lkey,
    };
    struct ibv_recv_wr wr = {
        .wr_id = wr_id,
        .sg_list = &sge,
        .num_sge = 1,
    };
    struct ibv_recv_wr *bad = NULL;
    int rc = ibv_post_recv(c->qp, &wr, &bad);
    if (rc) {
        errno = rc;
        set_err(err, errlen, "ibv_post_recv");
        return -1;
    }
    return 0;
}

int hord_post_send(hord_conn *c, uint64_t wr_id, void *addr, uint32_t length,
                   uint32_t lkey, char *err, size_t errlen) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)addr,
        .length = length,
        .lkey = lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = wr_id,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(c->qp, &wr, &bad);
    if (rc) {
        errno = rc;
        set_err(err, errlen, "ibv_post_send");
        return -1;
    }
    return 0;
}

int hord_post_write(hord_conn *c, uint64_t wr_id, void *addr, uint32_t length,
                    uint32_t lkey, uint64_t remote_addr, uint32_t rkey,
                    char *err, size_t errlen) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)addr,
        .length = length,
        .lkey = lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = wr_id,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
        .send_flags = IBV_SEND_SIGNALED,
    };
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(c->qp, &wr, &bad);
    if (rc) {
        errno = rc;
        set_err(err, errlen, "ibv_post_send (rdma write)");
        return -1;
    }
    return 0;
}

int hord_post_write_with_imm(hord_conn *c, uint64_t wr_id, void *addr,
                             uint32_t length, uint32_t lkey,
                             uint64_t remote_addr, uint32_t rkey, uint32_t imm,
                             char *err, size_t errlen) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)addr,
        .length = length,
        .lkey = lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = wr_id,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
    };
    /* imm_data is __be32 on the wire; convert from the caller's host-order value
     * so the receiver (after the ntohl in hord_poll) reads back the same u32
     * regardless of either host's endianness (spec §12). A zero-length WR is
     * legal here — it still delivers the immediate and consumes a recv WR. */
    wr.imm_data = htonl(imm);
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(c->qp, &wr, &bad);
    if (rc) {
        errno = rc;
        set_err(err, errlen, "ibv_post_send (rdma write with imm)");
        return -1;
    }
    return 0;
}

int hord_poll(hord_conn *c, uint64_t *wr_id, uint32_t *byte_len,
              uint32_t *opcode, uint32_t *status, uint32_t *imm_data,
              char *err, size_t errlen) {
    struct ibv_wc wc;
    int n = ibv_poll_cq(c->cq, 1, &wc);
    if (n < 0) {
        set_msg(err, errlen, "ibv_poll_cq failed");
        return -1;
    }
    if (n == 0)
        return 0;
    if (wr_id)
        *wr_id = wc.wr_id;
    if (byte_len)
        *byte_len = wc.byte_len;
    if (opcode)
        *opcode = (uint32_t)wc.opcode;
    if (status)
        *status = (uint32_t)wc.status;
    /* Only meaningful when wc_flags has IBV_WC_WITH_IMM (a RECV_RDMA_WITH_IMM
     * completion); ntohl mirrors the htonl on the send side. Zero otherwise. */
    if (imm_data)
        *imm_data = (wc.wc_flags & IBV_WC_WITH_IMM) ? ntohl(wc.imm_data) : 0u;
    return 1;
}

/* ---- async readiness ------------------------------------------------- */

/* The CQ completion-channel fd. Readable (after arming) when a completion has
 * been signalled. The async layer registers it with an event loop. */
int hord_conn_cq_fd(hord_conn *c) {
    if (!c || !c->comp_channel)
        return -1;
    return c->comp_channel->fd;
}

/* Arm the CQ to signal the completion channel on the next completion. Must be
 * called before each wait, and re-called after draining (notifications are
 * one-shot). */
int hord_cq_arm(hord_conn *c, char *err, size_t errlen) {
    if (!c || !c->cq) {
        set_msg(err, errlen, "no CQ");
        return -1;
    }
    if (ibv_req_notify_cq(c->cq, 0)) {
        set_err(err, errlen, "ibv_req_notify_cq");
        return -1;
    }
    return 0;
}

/* Drain and acknowledge all pending completion-channel notifications (the fd is
 * non-blocking, so this returns once the channel is empty). Returns the count
 * consumed (>= 0); never an error in practice. The caller re-arms with
 * hord_cq_arm and drains the CQ itself via hord_poll. Acking is required before
 * the CQ can be destroyed. */
int hord_cq_consume(hord_conn *c) {
    if (!c || !c->comp_channel || !c->cq)
        return 0;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    int n = 0;
    while (ibv_get_cq_event(c->comp_channel, &ev_cq, &ev_ctx) == 0)
        n++;
    if (n > 0)
        ibv_ack_cq_events(c->cq, (unsigned int)n);
    return n;
}

/* The connection's CM event-channel fd. After the handshake the caller flips it
 * non-blocking (hord_conn_cm_set_nonblock) and polls it for DISCONNECTED. */
int hord_conn_cm_fd(hord_conn *c) {
    if (!c || !c->ec)
        return -1;
    return c->ec->fd;
}

/* Make the CM channel non-blocking. Called only after the handshake completes:
 * the setup path (expect_event) relies on the channel being blocking. */
int hord_conn_cm_set_nonblock(hord_conn *c) {
    if (!c || !c->ec)
        return -1;
    return set_nonblock(c->ec->fd);
}

/* Non-blocking check for a peer-initiated teardown. Returns 1 if a
 * DISCONNECTED / device-removal / connect-error event is pending (and acks it),
 * 0 if no event (or an unrelated one, which is acked and ignored), -1 on error.
 * Requires the CM channel to be non-blocking (see hord_conn_cm_set_nonblock). */
int hord_conn_check_disconnect(hord_conn *c) {
    if (!c || !c->ec)
        return -1;
    struct rdma_cm_event *ev = NULL;
    if (rdma_get_cm_event(c->ec, &ev)) {
        if (errno == EAGAIN)
            return 0;
        return -1;
    }
    int disc = (ev->event == RDMA_CM_EVENT_DISCONNECTED ||
                ev->event == RDMA_CM_EVENT_DEVICE_REMOVAL ||
                ev->event == RDMA_CM_EVENT_CONNECT_ERROR);
    rdma_ack_cm_event(ev);
    return disc ? 1 : 0;
}

/* ---- teardown -------------------------------------------------------- */

void hord_disconnect(hord_conn *c) {
    if (c && c->id && !c->disconnected) {
        rdma_disconnect(c->id);
        c->disconnected = 1;
    }
}

/* Stop the NIC: disconnect and tear down the QP and CQ so nothing can DMA into
 * the caller's buffers anymore. Idempotent — pointers are nulled after destroy.
 * The PD is intentionally left alive: the caller must deregister its memory
 * regions (which belong to this PD) before hord_conn_free deallocates it. */
void hord_conn_shutdown(hord_conn *c) {
    if (!c)
        return;
    if (c->id && !c->disconnected) {
        rdma_disconnect(c->id);
        c->disconnected = 1;
    }
    if (c->id && c->qp) {
        rdma_destroy_qp(c->id); /* destroys c->id->qp */
        c->qp = NULL;
    }
    if (c->cq) {
        /* Ack any notification armed-but-not-yet-consumed, or ibv_destroy_cq
         * fails with EBUSY and the CQ leaks. */
        hord_cq_consume(c);
        ibv_destroy_cq(c->cq);
        c->cq = NULL;
    }
    if (c->comp_channel) {
        ibv_destroy_comp_channel(c->comp_channel);
        c->comp_channel = NULL;
    }
}

void hord_conn_free(hord_conn *c) {
    if (!c)
        return;
    hord_conn_shutdown(c); /* QP + CQ gone (idempotent) */
    if (c->pd) {
        /* Must be MR-free by now, or this fails with EBUSY and leaks. */
        ibv_dealloc_pd(c->pd);
        c->pd = NULL;
    }
    if (c->id) {
        rdma_destroy_id(c->id);
        c->id = NULL;
    }
    if (c->ec) {
        rdma_destroy_event_channel(c->ec);
        c->ec = NULL;
    }
    free(c);
}

void hord_listener_free(hord_listener *l) {
    if (!l)
        return;
    if (l->id)
        rdma_destroy_id(l->id);
    if (l->ec)
        rdma_destroy_event_channel(l->ec);
    free(l);
}
