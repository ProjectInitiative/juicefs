/*
 * vb_echo — native verbs RC-QP reproducer (Phase 1 + Phase 2 resource hardening).
 *
 * Data path: libibverbs RC queue pairs, SEND/RECV with explicit receive
 * posting, per-connection CQ pairs (no shared/global poller — a wedged or
 * failed QP cannot affect unrelated connections). Connection establishment
 * via librdmacm rdma_cm (rdma_create_ep / rdma_create_qp / rdma_connect /
 * rdma_get_request / rdma_accept); the data path never touches rsockets.
 *
 * Wire: framed messages [u32 total-len LE][u32 nonce LE][payload]; payloads
 * larger than MAX_MSG are delivered as multiple SENDs and reassembled.
 * Every echoed byte is verified against an LCG seeded from the nonce.
 *
 * Resource lifecycle (Phase 2 P0):
 *   - All buffers are C-allocated and registered; a buffer is reused only
 *     after its completion has been reaped from the CQ.
 *   - Teardown order: quiesce recv thread → reap remaining send WCs →
 *     rdma_disconnect → ibv_destroy_qp → ibv_dereg_mr → ibv_destroy_cq →
 *     ibv_dealloc_pd → rdma_destroy_id. Every return value is checked.
 *   - Partial initialization frees everything already allocated.
 *   - The server tracks every accepted connection and reaps disconnected
 *     ones (their vconn is fully reclaimed); live QP/CQ/MR/thread counters
 *     are logged periodically so wave tests prove reclamation (no monotonic
 *     growth).
 *   - All completion waits are deadline-bounded; no loop spins forever.
 *
 * Modes:
 *   vb_echo server PORT                     (env VB_SRV_IP = rail IP)
 *   vb_echo client ADDR PORT CONNS SECS [PAYLOAD] [WAVES] [stall-ms]
 *                                           (env VB_SRC_IP = rail IP,
 *                                            VB_DEBUG=1 for data-path logs)
 *
 * Build: gcc -O2 -Wall -o vb_echo vb_echo.c -libverbs -lrdmacm -lpthread
 */
#define _GNU_SOURCE
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define SLOTS        64      /* posted receive ring per connection */
#define MAX_MSG      65536   /* max SEND payload per WR (frame chunk) */
#define DEF_PAYLOAD  4096
#define OP_TIMEOUT_MS 5000   /* bound for every completion wait */

static int dbg = 0;             /* VB_DEBUG=1: per-step data-path prints */
#define DBG(...) do { if (dbg) fprintf(stderr, __VA_ARGS__); } while (0)

static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static void die(const char *what) {
    fprintf(stderr, "vb_echo: %s: %s\n", what, strerror(errno));
    exit(1);
}

/* global live-resource counters (proves reclamation across waves) */
static atomic_long g_live_qps, g_live_cqs, g_live_mrs, g_live_threads;

/* ---------------- wire frame ---------------- */
typedef struct {
    uint32_t len;      /* total payload length of the frame */
    uint32_t nonce;    /* echo verifier */
    uint8_t  data[];   /* len bytes follow */
} frame_hdr;

/* ---------------- per-connection resources ---------------- */
struct buf {
    void      *data;
    struct ibv_mr *mr;
    uint32_t   len;
};

enum vstate { VS_CONNECTING = 0, VS_LIVE = 1, VS_STOPPING = 2, VS_DEAD = 3 };

struct vconn {
    int is_server;
    atomic_int frame_done;   /* client: a full echo frame is assembled */
    struct rdma_cm_id *id;
    struct ibv_pd     *pd;
    struct ibv_qp     *qp;
    struct ibv_cq     *scq, *rcq;   /* send / recv completions */
    pthread_t         rth;
    int               has_rth;
    atomic_int        stop;         /* quiesce flag for the recv thread */
    atomic_int        state;        /* enum vstate */

    struct buf        sendbuf;
    struct buf        recvbuf[SLOTS];
    uint8_t          *frame;
    uint32_t          framecap, framegot;
    uint32_t          expect_total, expect_nonce;
    int               have_hdr;
    pthread_mutex_t   mu;           /* guards reassembly + sendbuf */

    /* client-side stats */
    uint64_t          ok;
    int               idx;
};

static struct buf reg_buf(struct ibv_pd *pd, size_t len) {
    struct buf b;
    memset(&b, 0, sizeof b);
    b.data = calloc(1, len);
    if (!b.data) die("calloc");
    b.len = (uint32_t)len;
    b.mr = ibv_reg_mr(pd, b.data, len, IBV_ACCESS_LOCAL_WRITE);
    if (!b.mr) { free(b.data); die("ibv_reg_mr"); }
    atomic_fetch_add(&g_live_mrs, 1);
    return b;
}

static void unreg_buf(struct buf *b) {
    if (b->mr) {
        if (ibv_dereg_mr(b->mr)) fprintf(stderr, "vb_echo: ibv_dereg_mr failed: %s\n", strerror(errno));
        atomic_fetch_sub(&g_live_mrs, 1);
        b->mr = NULL;
    }
    free(b->data);
    memset(b, 0, sizeof *b);
}

/* post one receive; the buffer returns via the WC's wr_id */
static int post_recv(struct vconn *c, struct buf *b) {
    struct ibv_recv_wr rr = {0}, *bad = NULL;
    struct ibv_sge sge = {0};
    sge.addr   = (uintptr_t)b->data;
    sge.length = b->len;
    sge.lkey   = b->mr->lkey;
    rr.wr_id   = (uintptr_t)b;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    return ibv_post_recv(c->qp, &rr, &bad);
}

/* Bounded send-completion wait: poll the send CQ until the next WC arrives
   or the deadline passes. Returns 0 on success, -1 on timeout/error. */
static int wait_send_wc(struct vconn *c, struct ibv_wc *out, double deadline_us) {
    for (;;) {
        int n = ibv_poll_cq(c->scq, 1, out);
        if (n > 0) return 0;
        if (n < 0) return -1;
        if (now_us() >= deadline_us) return -1; /* bounded: never spin forever */
        struct timespec ts = {0, 50000}; /* 50 µs */
        nanosleep(&ts, NULL);
    }
}

/* ---------------- completion threads ---------------- */
/* There is deliberately NO background send-completion thread: send WCs are
   consumed inline by whoever posted the send (client worker / server echo).
   A second poller on the same CQ would steal WCs and starve the poster's
   wait (this exact race hung the first POC run). The only background poller
   is the recv thread, which owns the recv CQ exclusively. */

/* reassemble chunks into c->frame; returns 1 when a full frame is ready */
static int reassemble(struct vconn *c, uint8_t **out, uint32_t *out_len, uint32_t n, uint8_t **p_in) {
    uint8_t *p = *p_in;
    while (n > 0) {
        if (!c->have_hdr) {
            if (c->framegot < 8 && c->framegot + n >= 8) {
                memcpy(c->frame + c->framegot, p, 8 - c->framegot);
                n -= (8 - c->framegot); p += (8 - c->framegot);
                c->framegot = 8;
                frame_hdr *h = (frame_hdr *)c->frame;
                c->expect_total = h->len;
                c->expect_nonce = h->nonce;
                if (c->expect_total + 8 > c->framecap) return -1;
                c->have_hdr = 1;
                continue;
            }
            uint32_t take = (8 - c->framegot) < n ? (8 - c->framegot) : n;
            memcpy(c->frame + c->framegot, p, take);
            c->framegot += take; p += take; n -= take;
            if (c->framegot == 8) {
                frame_hdr *h = (frame_hdr *)c->frame;
                c->expect_total = h->len; c->expect_nonce = h->nonce;
                if (c->expect_total + 8 > c->framecap) return -1;
                c->have_hdr = 1;
            }
            continue;
        }
        uint32_t need = c->expect_total + 8 - c->framegot;
        uint32_t take = need < n ? need : n;
        memcpy(c->frame + c->framegot, p, take);
        c->framegot += take; p += take; n -= take;
        if (c->framegot == c->expect_total + 8) {
            *out = c->frame; *out_len = c->expect_total + 8;
            *p_in = p;
            return 1;
        }
    }
    *p_in = p;
    return 0;
}

/* server: echo the completed frame back in chunked SENDs (bounded) */
static int server_echo(struct vconn *c, uint32_t total) {
    /* echo the reassembled frame (c->frame, total bytes) via the registered
       sendbuf — chunked; the frame content must be COPIED into the
       registered buffer (DMA reads only from registered memory) */
    double deadline = now_us() + (double)OP_TIMEOUT_MS * 1000;
    uint32_t remaining = total, off = 0;
    while (remaining > 0) {
        uint32_t chunk = remaining < MAX_MSG ? remaining : MAX_MSG;
        memcpy(c->sendbuf.data, c->frame + off, chunk);
        struct ibv_send_wr wr = {0}, *bad = NULL;
        struct ibv_sge sge = {0};
        sge.addr = (uintptr_t)c->sendbuf.data;
        sge.length = chunk;
        sge.lkey = c->sendbuf.mr->lkey;
        wr.wr_id = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED | (chunk <= 64 ? IBV_SEND_INLINE : 0);
        wr.sg_list = &sge; wr.num_sge = 1;
        int pr = ibv_post_send(c->qp, &wr, &bad);
        DBG("[srv] post_send rv=%d chunk=%u\n", pr, chunk);
        if (pr) return -1;
        struct ibv_wc wc;
        if (wait_send_wc(c, &wc, deadline)) return -1; /* timeout or CQ error */
        if (wc.status != IBV_WC_SUCCESS) return -1;
        off += chunk; remaining -= chunk;
    }
    return 0;
}

static void handle_recv(struct vconn *c, struct buf *b, uint32_t n) {
    uint8_t *p = b->data;
    for (;;) {
        uint8_t *frame = NULL; uint32_t flen = 0;
        int rv = reassemble(c, &frame, &flen, n, &p);
        if (rv < 0) { atomic_store(&c->stop, 1); return; }
        if (rv == 0) return;
        n = 0; /* consumed the whole WR payload */
        if (c->is_server) {
            DBG("[srv] frame assembled total=%u nonce=%u\n", c->expect_total, c->expect_nonce);
            if (server_echo(c, flen)) { atomic_store(&c->stop, 1); return; }
            /* re-arm for the next frame */
            c->have_hdr = 0; c->framegot = 0;
        } else {
            /* client: hand the frame to the worker via frame_done */
            atomic_store(&c->frame_done, 1);
            return; /* worker resets reassembly after consuming */
        }
    }
}

static void *recv_thread(void *arg) {
    struct vconn *c = arg;
    atomic_fetch_add(&g_live_threads, 1);
    struct timespec ts = {0, 100000};
    while (!atomic_load(&c->stop)) {
        struct ibv_wc wc; int n;
        while ((n = ibv_poll_cq(c->rcq, 1, &wc)) > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                /* peer disconnect / QP error: quiesce; teardown reclaims */
                atomic_store(&c->stop, 1);
                atomic_store(&c->state, VS_STOPPING);
                atomic_fetch_sub(&g_live_threads, 1);
                return NULL;
            }
            struct buf *b = (struct buf *)wc.wr_id;
            DBG("recv WC len=%u\n", wc.byte_len);
            handle_recv(c, b, wc.byte_len);
            if (post_recv(c, b)) { atomic_store(&c->stop, 1); atomic_fetch_sub(&g_live_threads, 1); return NULL; }
        }
        if (n < 0) { atomic_store(&c->stop, 1); atomic_fetch_sub(&g_live_threads, 1); return NULL; }
        nanosleep(&ts, NULL);
    }
    atomic_fetch_sub(&g_live_threads, 1);
    return NULL;
}

/* ---------------- connection setup/teardown ---------------- */

/* free everything already allocated on a partially-initialized vconn */
static void vconn_free_partial(struct vconn *c) {
    if (!c) return;
    if (c->frame) free(c->frame);
    for (int i = 0; i < SLOTS; i++)
        if (c->recvbuf[i].mr) unreg_buf(&c->recvbuf[i]);
    if (c->sendbuf.mr) unreg_buf(&c->sendbuf);
    if (c->qp) {
        if (ibv_destroy_qp(c->qp)) fprintf(stderr, "vb_echo: destroy_qp failed: %s\n", strerror(errno));
        else atomic_fetch_sub(&g_live_qps, 1);
        c->qp = NULL;
    }
    if (c->rcq && ibv_destroy_cq(c->rcq)) fprintf(stderr, "vb_echo: destroy rcq failed: %s\n", strerror(errno));
    else if (c->rcq) atomic_fetch_sub(&g_live_cqs, 1);
    if (c->scq && ibv_destroy_cq(c->scq)) fprintf(stderr, "vb_echo: destroy scq failed: %s\n", strerror(errno));
    else if (c->scq) atomic_fetch_sub(&g_live_cqs, 1);
    c->rcq = c->scq = NULL;
    if (c->pd) {
        if (ibv_dealloc_pd(c->pd)) fprintf(stderr, "vb_echo: dealloc_pd failed: %s\n", strerror(errno));
        c->pd = NULL;
    }
    if (c->id) { rdma_destroy_id(c->id); c->id = NULL; }
    free(c);
}

/* common resource bring-up shared by client and server-accept paths */
static int vconn_setup(struct vconn *c) {
    c->pd = ibv_alloc_pd(c->id->verbs);
    if (!c->pd) { fprintf(stderr, "vb_echo: ibv_alloc_pd: %s\n", strerror(errno)); return -1; }
    c->scq = ibv_create_cq(c->id->verbs, SLOTS * 2, c, NULL, 0);
    c->rcq = ibv_create_cq(c->id->verbs, SLOTS * 2, c, NULL, 0);
    if (!c->scq || !c->rcq) { fprintf(stderr, "vb_echo: ibv_create_cq: %s\n", strerror(errno)); return -1; }
    atomic_fetch_add(&g_live_cqs, 2);

    struct ibv_qp_init_attr qpattr;
    memset(&qpattr, 0, sizeof qpattr);
    qpattr.cap.max_send_wr  = SLOTS;
    qpattr.cap.max_recv_wr  = SLOTS;
    qpattr.cap.max_send_sge = qpattr.cap.max_recv_sge = 1;
    qpattr.cap.max_inline_data = 64;
    qpattr.qp_type = IBV_QPT_RC;
    qpattr.send_cq = c->scq;
    qpattr.recv_cq = c->rcq;
    qpattr.sq_sig_all = 1; /* every send gets a CQE: completion is reaped
                              before the send buffer is reused (req 8) */
    if (rdma_create_qp(c->id, c->pd, &qpattr)) {
        fprintf(stderr, "vb_echo: rdma_create_qp: %s\n", strerror(errno));
        return -1;
    }
    c->qp = c->id->qp;
    atomic_fetch_add(&g_live_qps, 1);

    c->sendbuf = reg_buf(c->pd, MAX_MSG);
    c->framecap = 8 + 4u * 1024 * 1024;
    c->frame = calloc(1, c->framecap);
    if (!c->frame) { fprintf(stderr, "vb_echo: frame calloc\n"); return -1; }
    pthread_mutex_init(&c->mu, NULL);
    for (int i = 0; i < SLOTS; i++) {
        c->recvbuf[i] = reg_buf(c->pd, MAX_MSG);
        if (post_recv(c, &c->recvbuf[i])) {
            fprintf(stderr, "vb_echo: post_recv: %s\n", strerror(errno));
            return -1;
        }
    }
    return 0;
}

/* Full teardown in the librdmacm/libibverbs contract order:
   quiesce thread → reap send WCs → disconnect → destroy QP → dereg MRs →
   destroy CQs → dealloc PD → destroy CM id. Every return value checked. */
static void vconn_destroy(struct vconn *c) {
    if (!c) return;
    double t0 = now_us();
    fprintf(stderr, "[destroy] quiesce\n");
    atomic_store(&c->stop, 1);
    if (c->has_rth) pthread_join(c->rth, NULL);
    fprintf(stderr, "[destroy] joined %.1fms\n", now_us() - t0);

    /* reap any still-outstanding send WCs so buffers are quiesced before
       deregistration (req 8 / P0-3); bounded by OP_TIMEOUT */
    double deadline = now_us() + (double)OP_TIMEOUT_MS * 1000;
    struct ibv_wc wc;
    while (ibv_poll_cq(c->scq, 1, &wc) > 0) {
        if (now_us() >= deadline) break;
    }

    if (c->qp) {
        if (atomic_load(&c->state) == VS_LIVE && c->id) {
            if (rdma_disconnect(c->id))
                DBG("rdma_disconnect: %s\n", strerror(errno));
        }
        if (ibv_destroy_qp(c->qp))
            fprintf(stderr, "vb_echo: ibv_destroy_qp: %s\n", strerror(errno));
        else
            atomic_fetch_sub(&g_live_qps, 1);
        c->qp = NULL;
    }
    fprintf(stderr, "[destroy] qp gone %.1fms\n", now_us() - t0);
    for (int i = 0; i < SLOTS; i++) unreg_buf(&c->recvbuf[i]);
    unreg_buf(&c->sendbuf);
    if (c->rcq && ibv_destroy_cq(c->rcq)) fprintf(stderr, "vb_echo: destroy rcq: %s\n", strerror(errno));
    else if (c->rcq) atomic_fetch_sub(&g_live_cqs, 1);
    if (c->scq && ibv_destroy_cq(c->scq)) fprintf(stderr, "vb_echo: destroy scq: %s\n", strerror(errno));
    else if (c->scq) atomic_fetch_sub(&g_live_cqs, 1);
    c->rcq = c->scq = NULL;
    if (c->pd) {
        if (ibv_dealloc_pd(c->pd)) fprintf(stderr, "vb_echo: dealloc_pd: %s\n", strerror(errno));
        c->pd = NULL;
    }
    if (c->id) {
        if (rdma_destroy_id(c->id)) fprintf(stderr, "vb_echo: rdma_destroy_id: %s\n", strerror(errno));
        c->id = NULL;
    }
    fprintf(stderr, "[destroy] done %.1fms\n", now_us() - t0);
    free(c->frame);
    pthread_mutex_destroy(&c->mu);
    free(c);
}

/* ---- server: tracked connection list + reaper (replaces g_conns leak) ---- */
#define SRV_MAX 4096
static struct vconn *srv_conns[SRV_MAX];
static int srv_n = 0;
static pthread_mutex_t srv_mu = PTHREAD_MUTEX_INITIALIZER;

static void srv_track(struct vconn *c) {
    pthread_mutex_lock(&srv_mu);
    if (srv_n < SRV_MAX) srv_conns[srv_n++] = c;
    else { fprintf(stderr, "vb_echo: too many server conns; rejecting\n"); vconn_destroy(c); }
    pthread_mutex_unlock(&srv_mu);
}

/* reclaim connections whose recv thread observed an error/disconnect */
static void srv_reap(void) {
    pthread_mutex_lock(&srv_mu);
    int w = 0;
    for (int i = 0; i < srv_n; i++) {
        struct vconn *c = srv_conns[i];
        if (atomic_load(&c->stop) || atomic_load(&c->state) == VS_STOPPING) {
            atomic_store(&c->state, VS_DEAD);
            vconn_destroy(c); /* joins the (already-stopped) thread */
        } else {
            srv_conns[w++] = c;
        }
    }
    srv_n = w;
    pthread_mutex_unlock(&srv_mu);
}

static void *srv_monitor(void *arg) {
    (void)arg;
    atomic_fetch_add(&g_live_threads, 1);
    for (;;) {
        struct timespec ts = {5, 0};
        nanosleep(&ts, NULL);
        srv_reap();
        printf("[vb-srv] live: conns=%d qps=%ld cqs=%ld mrs=%ld threads=%ld\n",
               srv_n, (long)atomic_load(&g_live_qps), (long)atomic_load(&g_live_cqs),
               (long)atomic_load(&g_live_mrs), (long)atomic_load(&g_live_threads));
        fflush(stdout);
    }
    return NULL;
}

/* ---------------- client connect ---------------- */
static struct vconn *client_connect(const char *src_ip, const char *host, const char *port) {
    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_port_space = RDMA_PS_TCP;
    char svc[16]; snprintf(svc, sizeof svc, "%s", port);
    static struct sockaddr_in src_sin; /* static: must outlive the call */
    if (src_ip && *src_ip) {
        /* bind the source to the rail IP so the CM picks the mlx5 device on
           that rail. Supply the src in the HINTS — mutating the returned
           res is invalid (ai_src_addr may be NULL). */
        memset(&src_sin, 0, sizeof src_sin);
        src_sin.sin_family = AF_INET;
        if (inet_pton(AF_INET, src_ip, &src_sin.sin_addr) != 1) die("src ip");
        hints.ai_src_addr = (struct sockaddr *)&src_sin;
        hints.ai_src_len = sizeof src_sin;
    }
    if (rdma_getaddrinfo((char *)host, svc, &hints, &res)) die("rdma_getaddrinfo");
    DBG("resolving route %s:%s (src %s)\n", host, port, src_ip ? src_ip : "any");

    /* Endpoint WITHOUT a QP first: PD/CQs must be ours before QP creation,
       otherwise librdmacm attaches its own internal CQs and our poll loops
       never see completions (this exact bug hung the first POC runs). */
    struct rdma_cm_id *id = NULL;
    if (rdma_create_ep(&id, res, NULL, NULL)) {
        fprintf(stderr, "vb_echo: rdma_create_ep: %s\n", strerror(errno));
        rdma_freeaddrinfo(res);
        return NULL;
    }
    rdma_freeaddrinfo(res);

    struct vconn *c = calloc(1, sizeof *c);
    if (!c) { rdma_destroy_id(id); return NULL; }
    c->is_server = 0;
    c->id = id;
    pthread_mutex_init(&c->mu, NULL);
    atomic_store(&c->state, VS_CONNECTING);

    if (vconn_setup(c)) { vconn_free_partial(c); return NULL; }

    DBG("rdma_connect...\n");
    if (rdma_connect(id, NULL)) {
        fprintf(stderr, "vb_echo: rdma_connect: %s\n", strerror(errno));
        vconn_free_partial(c);
        return NULL;
    }
    atomic_store(&c->state, VS_LIVE);
    DBG("conn ready (PD/CQs/recvs posted)\n");
    if (pthread_create(&c->rth, NULL, recv_thread, c)) {
        vconn_free_partial(c);
        return NULL;
    }
    c->has_rth = 1;
    return c;
}

/* ---------------- payload pattern ---------------- */
static void fill_payload(uint8_t *p, uint32_t len, uint32_t nonce) {
    uint32_t x = nonce ^ 0x5eed1234;
    for (uint32_t i = 0; i < len; i++) {
        x = x * 1664525 + 1013904223;
        p[i] = (uint8_t)(x >> 24);
    }
}

static int check_payload(uint8_t *p, uint32_t len, uint32_t nonce) {
    uint32_t x = nonce ^ 0x5eed1234;
    for (uint32_t i = 0; i < len; i++) {
        x = x * 1664525 + 1013904223;
        if (p[i] != (uint8_t)(x >> 24)) return i;
    }
    return -1;
}

/* ---------------- client worker ---------------- */
struct cli_args {
    int idx;
    int secs;
    uint32_t plen;
    int stall_ms;
    uint64_t ok;
    int err;
    struct vconn *c;
};

static void *cli_worker(void *arg) {
    struct cli_args *a = arg;
    struct vconn *c = a->c;
    uint32_t nonce = (uint32_t)(a->idx * 7919 + 13);
    double t0 = now_us();
    double last_beat = now_us();
    while (now_us() - t0 < (double)a->secs * 1e6) {
        if (now_us() - last_beat > 2e6) {
            printf("[cli] alive ok=%llu\n", (unsigned long long)a->ok);
            fflush(stdout);
            last_beat = now_us();
        }
        uint8_t *frame = malloc(8 + a->plen);
        if (!frame) { a->err = 8; goto out; }
        frame_hdr *h = (frame_hdr *)frame;
        h->len = a->plen; h->nonce = nonce;
        fill_payload(frame + 8, a->plen, nonce);
        /* send chunked; every completion bounded by stall_ms */
        double deadline = now_us() + (double)a->stall_ms * 1000;
        uint32_t off = 0, remaining = 8 + a->plen;
        while (remaining > 0) {
            uint32_t chunk = remaining < MAX_MSG ? remaining : MAX_MSG;
            pthread_mutex_lock(&c->mu);
            memcpy(c->sendbuf.data, frame + off, chunk);
            struct ibv_send_wr wr = {0}, *bad = NULL;
            struct ibv_sge sge = {0};
            sge.addr = (uintptr_t)c->sendbuf.data;
            sge.length = chunk;
            sge.lkey = c->sendbuf.mr->lkey;
            wr.wr_id = 1; wr.opcode = IBV_WR_SEND;
            wr.send_flags = IBV_SEND_SIGNALED | (chunk <= 64 ? IBV_SEND_INLINE : 0);
            wr.sg_list = &sge; wr.num_sge = 1;
            int pr = ibv_post_send(c->qp, &wr, &bad);
            DBG("[cli %d] post_send rv=%d chunk=%u\n", a->idx, pr, chunk);
            if (pr) { pthread_mutex_unlock(&c->mu); a->err = 1; goto out; }
            struct ibv_wc wc;
            int rv = wait_send_wc(c, &wc, deadline);
            pthread_mutex_unlock(&c->mu);
            if (rv) { a->err = 3; goto out; }
            if (wc.status) { a->err = 2; goto out; }
            off += chunk; remaining -= chunk;
        }
        /* bounded wait for the echoed frame */
        deadline = now_us() + (double)a->stall_ms * 1000;
        while (!atomic_load(&c->frame_done)) {
            if (atomic_load(&c->stop)) { DBG("[cli %d] conn stopped in wait\n", a->idx); a->err = 4; goto out; }
            if (now_us() >= deadline) { DBG("[cli %d] STALL in frame wait\n", a->idx); a->err = 5; goto out; }
            struct timespec ts = {0, 20000}; nanosleep(&ts, NULL);
        }
        DBG("[cli %d] echo frame done (total=%u nonce=%u)\n", a->idx, c->expect_total, c->expect_nonce);
        pthread_mutex_lock(&c->mu);
        int plen_ok = (c->expect_total == a->plen);
        int bad_at = plen_ok ? check_payload(c->frame + 8, a->plen, nonce) : 0;
        /* reset reassembly for the next frame */
        c->have_hdr = 0; c->framegot = 0;
        pthread_mutex_unlock(&c->mu);
        atomic_store(&c->frame_done, 0);
        if (!plen_ok) { a->err = 6; goto out; }
        if (bad_at >= 0) { a->err = 7; goto out; } /* mismatch */
        a->ok++;
        nonce += 9973;
        free(frame);
        continue;
out:
        fprintf(stderr, "vb_echo: [cli %d] worker exit err=%d nonce=%u after %.1fs\n",
                a->idx, a->err, nonce, (now_us() - t0) / 1e6);
        free(frame);
        return NULL;
    }
    return NULL;
}

/* ---------------- client run ---------------- */
static int run_client(const char *src_ip, const char *host, const char *port, int conns, int secs,
                      uint32_t plen, int waves, int stall_ms) {
    if (conns > 512) { fprintf(stderr, "conns>512 unsupported in POC\n"); return 1; }
    double grand_ok = 0;
    for (int w = 0; w < waves; w++) {
        double t0 = now_us();
        pthread_t th[512];
        struct cli_args args[512];
        struct vconn *vcs[512];
        for (int i = 0; i < conns; i++) {
            vcs[i] = client_connect(src_ip, host, port);
            if (!vcs[i]) {
                fprintf(stderr, "vb_echo: wave %d conn %d failed to establish\n", w, i);
                /* reclaim already-established conns from this failed wave */
                for (int j = 0; j < i; j++) vconn_destroy(vcs[j]);
                return 1;
            }
            vcs[i]->idx = i;
            memset(&args[i], 0, sizeof args[i]);
            args[i].idx = i; args[i].secs = secs; args[i].plen = plen;
            args[i].stall_ms = stall_ms; args[i].c = vcs[i];
            if (pthread_create(&th[i], NULL, cli_worker, &args[i])) {
                fprintf(stderr, "vb_echo: pthread_create\n");
                vconn_destroy(vcs[i]);
                for (int j = 0; j < i; j++) vconn_destroy(vcs[j]);
                return 1;
            }
        }
        uint64_t ok = 0; int errs = 0;
        for (int i = 0; i < conns; i++) {
            pthread_join(th[i], NULL);
            ok += args[i].ok;
            if (args[i].err) errs++;
        }
        /* teardown + reclamation proof: destroy, then verify counters fell */
        for (int i = 0; i < conns; i++) vconn_destroy(vcs[i]);
        double dt = now_us() - t0;
        printf("[wave %d/%d] conns=%d ok=%llu errs=%d wall=%.1fs throughput=%.0f reads/s "
               "| resources: qps=%ld cqs=%ld mrs=%ld threads=%ld\n",
               w + 1, waves, conns, (unsigned long long)ok, errs, dt / 1e6,
               ok / (dt / 1e6),
               (long)atomic_load(&g_live_qps), (long)atomic_load(&g_live_cqs),
               (long)atomic_load(&g_live_mrs), (long)atomic_load(&g_live_threads));
        fflush(stdout);
        grand_ok += ok;
        if (errs > 0) {
            printf("[wave %d] FAIL: %d connection(s) errored\n", w + 1, errs);
            return 1;
        }
        if (atomic_load(&g_live_qps) != 0 || atomic_load(&g_live_cqs) != 0 ||
            atomic_load(&g_live_mrs) != 0) {
            printf("[wave %d] FAIL: resources not reclaimed (qps=%ld cqs=%ld mrs=%ld)\n",
                   w + 1, (long)atomic_load(&g_live_qps), (long)atomic_load(&g_live_cqs),
                   (long)atomic_load(&g_live_mrs));
            return 1;
        }
        struct timespec ts = {0, 200000000}; nanosleep(&ts, NULL);
    }
    printf("[client] TOTAL verified reads=%llu waves=%d — PASS\n",
           (unsigned long long)grand_ok, waves);
    return 0;
}

/* ---------------- server ---------------- */
static int run_server(const char *port) {
    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    /* Bind explicitly to the rail IP (VB_SRV_IP): a wildcard passive bind
       also covers the mgmt NIC (no RDMA device) and fails with
       EADDRNOTAVAIL before the listener ever opens. */
    static struct sockaddr_in srv_sin;
    const char *rail = getenv("VB_SRV_IP");
    if (rail && *rail) {
        memset(&srv_sin, 0, sizeof srv_sin);
        srv_sin.sin_family = AF_INET;
        if (inet_pton(AF_INET, rail, &srv_sin.sin_addr) != 1) die("srv ip");
        hints.ai_src_addr = (struct sockaddr *)&srv_sin;
        hints.ai_src_len = sizeof srv_sin;
    }
    if (rdma_getaddrinfo(rail && *rail ? (char *)rail : NULL, (char *)port, &hints, &res)) die("rdma_getaddrinfo");

    struct rdma_cm_id *lid = NULL;
    if (rdma_create_ep(&lid, res, NULL, NULL)) die("rdma_create_ep (server)");
    if (rdma_listen(lid, 256)) die("rdma_listen");
    printf("vb_echo server: listening on %s:%s (RC QPs, per-conn CQs)\n",
           rail && *rail ? rail : "*", port);
    fflush(stdout);
    rdma_freeaddrinfo(res);

    pthread_t mon;
    if (pthread_create(&mon, NULL, srv_monitor, NULL)) die("pthread_create monitor");

    for (;;) {
        struct rdma_cm_id *cid = NULL;
        if (rdma_get_request(lid, &cid)) {
            fprintf(stderr, "vb_echo: rdma_get_request: %s\n", strerror(errno));
            continue;
        }
        DBG("[srv] got request; allocating resources\n");
        struct vconn *c = calloc(1, sizeof *c);
        if (!c) { fprintf(stderr, "vb_echo: calloc vconn\n"); rdma_destroy_id(cid); continue; }
        c->is_server = 1;
        c->id = cid;
        pthread_mutex_init(&c->mu, NULL);
        atomic_store(&c->state, VS_CONNECTING);
        if (vconn_setup(c)) {
            fprintf(stderr, "vb_echo: vconn_setup failed; reclaiming\n");
            vconn_free_partial(c);
            continue;
        }
        if (rdma_accept(cid, NULL)) {
            fprintf(stderr, "vb_echo: rdma_accept: %s\n", strerror(errno));
            vconn_free_partial(c);
            continue;
        }
        atomic_store(&c->state, VS_LIVE);
        if (pthread_create(&c->rth, NULL, recv_thread, c)) {
            fprintf(stderr, "vb_echo: pthread_create recv\n");
            vconn_free_partial(c);
            continue;
        }
        c->has_rth = 1;
        srv_track(c);
        DBG("[srv] conn established\n");
    }
    return 0;
}

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0); /* line-buffered: no lost-heartbeat confusion */
    dbg = getenv("VB_DEBUG") != NULL;
    if (argc < 3) {
        fprintf(stderr,
            "usage: %s server PORT                        (env VB_SRV_IP=rail IP)\n"
            "       %s client ADDR PORT CONNS SECS [PAYLOAD] [WAVES] [stall-ms]\n"
            "                                              (env VB_SRC_IP=rail IP)\n",
            argv[0], argv[0]);
        return 2;
    }
    if (!strcmp(argv[1], "server")) return run_server(argv[2]);
    if (!strcmp(argv[1], "client") && argc >= 6) {
        const char *host = argv[2], *port = argv[3];
        const char *src = getenv("VB_SRC_IP");
        int conns = atoi(argv[4]), secs = atoi(argv[5]);
        uint32_t plen = argc > 6 ? (uint32_t)strtoul(argv[6], NULL, 10) : DEF_PAYLOAD;
        int waves = argc > 7 ? atoi(argv[7]) : 3;
        int stall = argc > 8 ? atoi(argv[8]) : 3000;
        return run_client(src, host, port, conns, secs, plen, waves, stall);
    }
    return 2;
}
