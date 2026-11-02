/*
 * vb_echo — native verbs RC-QP echo reproducer (Phase 1 of the verbs assignment).
 *
 * Data path: libibverbs RC queue pairs, SEND/RECV with explicit receive
 * posting, per-connection CQ pairs (no shared/global poller — a wedged or
 * failed QP cannot affect unrelated connections). Connection establishment
 * via librdmacm rdma_cm (rdma_create_ep / rdma_connect / rdma_get_request),
 * which handles switchless and switched RoCE addressing identically to
 * rping; the data path never touches rsockets.
 *
 * Wire: client sends a framed request [u32 len LE][payload bytes]; server
 * echoes the same frame. The client fills each request payload from a
 * per-request nonce via an LCG and verifies every echoed byte — any
 * mismatch aborts with a diagnostic. Fragmentation: payloads larger than
 * the per-SEND capacity are sent as multiple SENDs reusing one frame
 * header (receiver reassembles into the frame buffer) — the POC exercises
 * this at --len sizes above the inline limit too.
 *
 * Buffer lifetime: every buffer is C-allocated and registered once per
 * connection (recv ring of SLOTS slots + one send buffer); buffers are
 * only reused after their completion is reaped, and everything is
 * deregistered/freed on teardown. No pointer is retained across
 * operations it is not registered for.
 *
 * Completion: dedicated thread per CQ (one send-CQ + one recv-CQ per
 * connection) doing ibv_get_cq_event → ibv_poll_cq until empty →
 * re-arm. Blocking is per-CQ, so a stuck QP only blocks its own thread.
 * The main loops poll non-blockingly with nanosleep slices, so shutdown
 * cannot hang indefinitely.
 *
 * Modes:
 *   vb_echo server PORT
 *   vb_echo client ADDR PORT CONNS SECS [PAYLOAD-LEN] [WAVES] [stall-ms]
 *
 * Client behavior: W waves of CONNS simultaneous connections against the
 * SAME server process; each connection runs SECS of request/echo rounds,
 * then a clean teardown. Prints per-wave verified-read counts and p50/p99.
 *
 * Build: gcc -O2 -o vb_echo vb_echo.c -libverbs -lrdmacm -lpthread
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
static int dbg = 0;             /* VB_DEBUG=1: per-step data-path prints */
#define DBG(...) do { if (dbg) fprintf(stderr, __VA_ARGS__); } while (0)
#define MAX_MSG      65536   /* max SEND payload per WR (frame chunk) */
#define DEF_PAYLOAD  4096

static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static void die(const char *what) {
    fprintf(stderr, "vb_echo: %s: %s\n", what, strerror(errno));
    exit(1);
}

/* ---------------- wire frame ---------------- */
/* frame = u32 total-payload-length LE followed by payload bytes.
 * A payload > MAX_MSG is delivered as ceil(len/MAX_MSG) SENDs; each SEND
 * carries a chunk and the receiver reassembles until it has `len` bytes.
 * The 4-byte length prefix travels with the first chunk. */
typedef struct {
    uint32_t len;      /* total payload length of the frame */
    uint32_t nonce;    /* echo verifier (POC); Phase-2 uses xid */
    uint8_t  data[];   /* len bytes follow */
} frame_hdr;

/* ---------------- per-connection resources ---------------- */
struct vconn;

struct buf {
    void      *data;
    struct ibv_mr *mr;
    uint32_t   len;
};

struct vconn {
    int is_server;
    atomic_int frame_done;   /* client: a full echo frame is assembled */
    struct rdma_cm_id *id;
    struct ibv_pd     *pd;
    struct ibv_cq     *scq;   // send completions
    struct ibv_cq     *rcq;   // recv completions
    struct ibv_comp_channel *sch, *rch;
    pthread_t         rth;
    int               stop;

    struct buf        sendbuf;               // one outgoing chunk at a time
    struct buf        recvbuf[SLOTS];        // posted receive ring
    uint8_t          *frame;                 // reassembly buffer (client+server)
    uint32_t          framelen, framecap, framegot;
    uint32_t          expect_total, expect_nonce;
    int               have_hdr;

    /* client-side stats */
    uint64_t          ok;
    int               idx;
};

static struct buf reg_buf(struct ibv_pd *pd, size_t len) {
    struct buf b;
    b.data = calloc(1, len);
    if (!b.data) die("calloc");
    b.len = (uint32_t)len;
    b.mr = ibv_reg_mr(pd, b.data, len, IBV_ACCESS_LOCAL_WRITE);
    if (!b.mr) die("ibv_reg_mr");
    return b;
}

static void unreg_buf(struct vconn *c, struct buf *b) {
    (void)c;
    if (b->mr) ibv_dereg_mr(b->mr);
    free(b->data);
    memset(b, 0, sizeof *b);
}

/* post SLOTS receives; re-post after each consumed recv */
static int post_recv(struct vconn *c, struct buf *b) {
    struct ibv_recv_wr rr = {0}, *bad = NULL;
    struct ibv_sge sge = {0};
    sge.addr   = (uintptr_t)b->data;
    sge.length = b->len;
    sge.lkey   = b->mr->lkey;
    rr.wr_id   = (uintptr_t)b;   /* buffer comes back with the WC */
    rr.sg_list = &sge;
    rr.num_sge = 1;
    return ibv_post_recv(c->id->qp, &rr, &bad);
}

/* ---------------- completion threads (per-CQ, no global poller) ---------------- */
/* NOTE: there is deliberately NO background send-completion thread: send WCs
   are consumed inline by whoever posted the send (client worker / server
   echo). A second poller on the same CQ would steal WCs and starve the
   poster's wait (this exact race hung the first POC run). The only
   background poller is the recv thread, which owns the recv CQ exclusively. */
static struct vconn *g_conns[4096];
static int g_nconns = 0;

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

/* server: echo the completed frame back in chunked SENDs */
static int server_echo(struct vconn *c, uint8_t *frame, uint32_t total) {
    uint32_t remaining = total, off = 0;
    while (remaining > 0) {
        uint32_t chunk = remaining < MAX_MSG ? remaining : MAX_MSG;
        memcpy(c->sendbuf.data, frame + off, chunk);
        struct ibv_send_wr wr = {0}, *bad = NULL;
        struct ibv_sge sge = {0};
        sge.addr = (uintptr_t)c->sendbuf.data;
        sge.length = chunk;
        sge.lkey = c->sendbuf.mr->lkey;
        wr.wr_id = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED | (chunk <= 64 ? IBV_SEND_INLINE : 0);
        wr.sg_list = &sge; wr.num_sge = 1;
        int pr = ibv_post_send(c->id->qp, &wr, &bad);
        DBG("[srv] post_send rv=%d chunk=%u\n", pr, chunk);
        if (pr) return -1;
        /* wait for the send completion before reusing sendbuf (req 8) */
        struct ibv_wc wc; int done = 0;
        while (!done) {
            int nwc = ibv_poll_cq(c->scq, 1, &wc);
            if (nwc > 0) {
                DBG("[srv] send WC status=%d\n", wc.status);
                if (wc.status != IBV_WC_SUCCESS) return -1;
                done = 1;
            } else if (nwc < 0) return -1;
            else sched_yield();
        }
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
            if (server_echo(c, frame, flen)) { atomic_store(&c->stop, 1); return; }
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
    struct timespec ts = {0, 100000};
    while (!atomic_load(&c->stop)) {
        struct ibv_wc wc; int n;
        while ((n = ibv_poll_cq(c->rcq, 1, &wc)) > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "vb_echo: recv WC error status=%d\n", wc.status);
                atomic_store(&c->stop, 1);
                return NULL;
            }
            struct buf *b = (struct buf *)wc.wr_id;
            DBG("recv WC len=%u\n", wc.byte_len);
            handle_recv(c, b, wc.byte_len);
            if (post_recv(c, b)) { atomic_store(&c->stop, 1); return NULL; }
        }
        if (n < 0) { atomic_store(&c->stop, 1); return NULL; }
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/* ---------------- connection setup/teardown ---------------- */
/* real client setup; src_ip may be NULL (let the CM route resolve) */
static struct vconn *client_connect(const char *src_ip, const char *host, const char *port) {
    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_port_space = RDMA_PS_TCP;
    char svc[16]; snprintf(svc, sizeof svc, "%s", port);
    static struct sockaddr_in src_sin; /* static: must outlive the call */
    if (src_ip && *src_ip) {
        /* bind the source to the rail IP so the CM picks the mlx5 device on
           that rail (the mgmt NIC has no RDMA device; an unrouted CM REQ
           hangs rather than failing fast). Supply the src in the HINTS —
           mutating the returned res is invalid (ai_src_addr may be NULL). */
        memset(&src_sin, 0, sizeof src_sin);
        src_sin.sin_family = AF_INET;
        if (inet_pton(AF_INET, src_ip, &src_sin.sin_addr) != 1) die("src ip");
        hints.ai_src_addr = (struct sockaddr *)&src_sin;
        hints.ai_src_len = sizeof src_sin;
    }
    if (rdma_getaddrinfo((char *)host, svc, &hints, &res)) die("rdma_getaddrinfo");
    fprintf(stderr, "vb_echo: resolving route %s:%s (src %s)\n", host, port, src_ip ? src_ip : "any");

    struct ibv_qp_init_attr qpattr;
    memset(&qpattr, 0, sizeof qpattr);
    qpattr.cap.max_send_wr  = SLOTS;
    qpattr.cap.max_recv_wr  = SLOTS;
    qpattr.cap.max_send_sge = qpattr.cap.max_recv_sge = 1;
    qpattr.cap.max_inline_data = 64;
    qpattr.qp_type = IBV_QPT_RC;
    qpattr.sq_sig_all = 1; /* every send gets a CQE: we poll for completion
                              before reusing the send buffer (req 8) */

    struct rdma_cm_id *id = NULL;
    if (rdma_create_ep(&id, res, NULL, &qpattr)) die("rdma_create_ep");
    fprintf(stderr, "vb_echo: rdma_connect...\n");
    if (rdma_connect(id, NULL)) die("rdma_connect");
    fprintf(stderr, "vb_echo: connected; allocating PD/CQs...\n");

    struct vconn *c = calloc(1, sizeof *c);
    c->is_server = 0;
    c->id = id;
    c->pd = ibv_alloc_pd(id->verbs);
    if (!c->pd) die("ibv_alloc_pd");
    c->scq = ibv_create_cq(id->verbs, SLOTS * 2, c, NULL, 0);
    c->rcq = ibv_create_cq(id->verbs, SLOTS * 2, c, NULL, 0);
    if (!c->scq || !c->rcq) die("ibv_create_cq");
    c->sendbuf = reg_buf(c->pd, MAX_MSG);
    c->framecap = 8 + 4u * 1024 * 1024;
    c->frame = calloc(1, c->framecap);
    /* register the frame too: server echoes straight from it */
    c->frame = realloc(c->frame, c->framecap); /* keep malloc'd; echo copies via sendbuf */
    for (int i = 0; i < SLOTS; i++) {
        c->recvbuf[i] = reg_buf(c->pd, MAX_MSG);
        if (post_recv(c, &c->recvbuf[i])) die("post_recv");
    }
    fprintf(stderr, "vb_echo: conn ready (PD/CQs/recvs posted)\n");
    pthread_create(&c->rth, NULL, recv_thread, c);
    rdma_freeaddrinfo(res);
    return c;
}

static void vconn_destroy(struct vconn *c) {
    atomic_store(&c->stop, 1);
    pthread_join(c->rth, NULL);
    for (int i = 0; i < SLOTS; i++) unreg_buf(c, &c->recvbuf[i]);
    unreg_buf(c, &c->sendbuf);
    ibv_destroy_cq(c->scq); ibv_destroy_cq(c->rcq);
    if (c->pd) ibv_dealloc_pd(c->pd);
    if (c->id) rdma_destroy_id(c->id);
    free(c->frame);
    free(c);
}

/* ---------------- client loop ---------------- */
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

struct cli_args {
    int idx;
    int secs;
    uint32_t plen;
    uint64_t ok;
    int err;
    struct vconn *c;
};

static void *cli_worker(void *arg) {
    struct cli_args *a = arg;
    struct vconn *c = a->c;
    uint32_t nonce = (uint32_t)(a->idx * 7919 + 13);
    double t0 = now_us();
    while (now_us() - t0 < (double)a->secs * 1e6) {
        /* build frame in the sendbuf: [len][nonce][payload] chunked */
        uint8_t *frame = malloc(8 + a->plen);
        frame_hdr *h = (frame_hdr *)frame;
        h->len = a->plen; h->nonce = nonce;
        fill_payload(frame + 8, a->plen, nonce);
        /* send chunked */
        uint32_t off = 0, remaining = 8 + a->plen;
        while (remaining > 0) {
            uint32_t chunk = remaining < MAX_MSG ? remaining : MAX_MSG;
            memcpy(c->sendbuf.data, frame + off, chunk);
            struct ibv_send_wr wr = {0}, *bad = NULL;
            struct ibv_sge sge = {0};
            sge.addr = (uintptr_t)c->sendbuf.data;
            sge.length = chunk;
            sge.lkey = c->sendbuf.mr->lkey;
            wr.wr_id = 1; wr.opcode = IBV_WR_SEND;
            wr.send_flags = IBV_SEND_SIGNALED | (chunk <= 64 ? IBV_SEND_INLINE : 0);
            wr.sg_list = &sge; wr.num_sge = 1;
            int pr = ibv_post_send(c->id->qp, &wr, &bad);
            DBG("[cli %d] post_send rv=%d chunk=%u\n", a->idx, pr, chunk);
            if (pr) { a->err = 1; goto out; }
            struct ibv_wc wc; int done = 0;
            while (!done) {
                int nwc = ibv_poll_cq(c->scq, 1, &wc);
                if (nwc > 0) {
                    DBG("[cli %d] send WC status=%d\n", a->idx, wc.status);
                    if (wc.status) { a->err = 2; goto out; }
                    done = 1;
                } else if (nwc < 0) { a->err = 3; goto out; }
                else sched_yield();
            }
            off += chunk; remaining -= chunk;
        }
        /* bounded wait for the echoed frame; stall-ms = 3000 */
        double deadline = now_us() + 3e6;
        while (!atomic_load(&c->frame_done)) {
            if (atomic_load(&c->stop)) { DBG("[cli %d] conn stopped in wait\n", a->idx); a->err = 4; goto out; }
            if (now_us() >= deadline) { DBG("[cli %d] STALL in frame wait\n", a->idx); a->err = 5; goto out; }
            struct timespec ts = {0, 20000}; nanosleep(&ts, NULL);
        }
        DBG("[cli %d] echo frame done (total=%u nonce=%u)\n", a->idx, c->expect_total, c->expect_nonce);
        if (c->expect_total != a->plen) { a->err = 6; goto out; }
        int bad_at = check_payload(c->frame + 8, a->plen, nonce);
        if (bad_at >= 0) { a->err = 7; goto out; } /* mismatch */
        /* reset reassembly for the next frame */
        c->have_hdr = 0; c->framegot = 0;
        atomic_store(&c->frame_done, 0);
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

/* ---------------- main ---------------- */
static int run_client(const char *src_ip, const char *host, const char *port, int conns, int secs,
                      uint32_t plen, int waves, int stall_ms) {
    (void)stall_ms;
    double grand_ok = 0;
    for (int w = 0; w < waves; w++) {
        double t0 = now_us();
        pthread_t th[512];
        struct cli_args args[512];
        struct vconn *vcs[512];
        if (conns > 512) { fprintf(stderr, "conns>512 unsupported in POC\n"); return 1; }
        for (int i = 0; i < conns; i++) {
            vcs[i] = client_connect(src_ip, host, port);
            if (!vcs[i] || atomic_load(&vcs[i]->stop)) {
                fprintf(stderr, "vb_echo: wave %d conn %d failed to establish\n", w, i);
                return 1;
            }
            vcs[i]->idx = i;
            memset(&args[i], 0, sizeof args[i]);
            args[i].idx = i; args[i].secs = secs; args[i].plen = plen; args[i].c = vcs[i];
            pthread_create(&th[i], NULL, cli_worker, &args[i]);
        }
        uint64_t ok = 0; int errs = 0;
        for (int i = 0; i < conns; i++) {
            pthread_join(th[i], NULL);
            ok += args[i].ok;
            if (args[i].err) errs++;
        }
        for (int i = 0; i < conns; i++) vconn_destroy(vcs[i]);
        double dt = now_us() - t0;
        printf("[wave %d/%d] conns=%d ok=%llu errs=%d wall=%.1fs throughput=%.0f reads/s\n",
               w + 1, waves, conns, (unsigned long long)ok, errs, dt / 1e6,
               ok / (dt / 1e6));
        grand_ok += ok;
        if (errs > 0) {
            printf("[wave %d] FAIL: %d connection(s) errored\n", w + 1, errs);
            return 1;
        }
        struct timespec ts = {0, 200000000}; nanosleep(&ts, NULL);
    }
    printf("[client] TOTAL verified reads=%llu waves=%d — PASS\n",
           (unsigned long long)grand_ok, waves);
    return 0;
}

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

    struct ibv_qp_init_attr qpattr;
    memset(&qpattr, 0, sizeof qpattr);
    qpattr.cap.max_send_wr = SLOTS; qpattr.cap.max_recv_wr = SLOTS;
    qpattr.cap.max_send_sge = qpattr.cap.max_recv_sge = 1;
    qpattr.cap.max_inline_data = 64;
    qpattr.qp_type = IBV_QPT_RC;

    struct rdma_cm_id *lid = NULL;
    if (rdma_create_ep(&lid, res, NULL, &qpattr)) die("rdma_create_ep (server)");
    if (rdma_listen(lid, 256)) die("rdma_listen");
    printf("vb_echo server: listening on rdma_cm port %s (RC QPs, per-conn CQs)\n", port);
    fflush(stdout);

    for (;;) {
        struct rdma_cm_id *cid = NULL;
        if (rdma_get_request(lid, &cid)) { fprintf(stderr, "vb_echo: rdma_get_request: %s\n", strerror(errno)); continue; }
        DBG("[srv] got request; allocating resources\n");
        fprintf(stderr, "vb_echo: server got request; allocating resources\n");
        struct vconn *c = calloc(1, sizeof *c);
        c->is_server = 1;
        c->id = cid;
        c->pd = ibv_alloc_pd(cid->verbs);
        if (!c->pd) die("ibv_alloc_pd");
        c->scq = ibv_create_cq(cid->verbs, SLOTS * 2, c, NULL, 0);
        c->rcq = ibv_create_cq(cid->verbs, SLOTS * 2, c, NULL, 0);
        if (!c->scq || !c->rcq) die("ibv_create_cq");
        c->sendbuf = reg_buf(c->pd, MAX_MSG);
        c->framecap = 8 + 4u * 1024 * 1024;
        c->frame = calloc(1, c->framecap);
        for (int i = 0; i < SLOTS; i++) {
            c->recvbuf[i] = reg_buf(c->pd, MAX_MSG);
            if (post_recv(c, &c->recvbuf[i])) die("post_recv");
        }
        if (rdma_accept(cid, NULL)) { fprintf(stderr, "vb_echo: rdma_accept: %s\n", strerror(errno)); continue; }
        fprintf(stderr, "vb_echo: server conn established\n");
        pthread_create(&c->rth, NULL, recv_thread, c);
        if (g_nconns < 4096) g_conns[g_nconns++] = c;
        printf("vb_echo server: accepted conn #%d\n", g_nconns);
        fflush(stdout);
    }
    return 0;
}

int main(int argc, char **argv) {
    dbg = getenv("VB_DEBUG") != NULL;
    if (argc < 3) {
        fprintf(stderr,
            "usage: %s server PORT\n"
            "       %s client ADDR PORT CONNS SECS [PAYLOAD] [WAVES] [stall-ms]  (VB_SRC_IP env = source rail IP)\n",
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
