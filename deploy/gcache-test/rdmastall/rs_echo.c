/*
 * rs_echo — minimal rsockets reproducer for the ghost-serve stall.
 *
 * Wire: client sends 8 bytes {magic "REQ1", uint32 nonce}; server answers
 * 4096 bytes whose first 4 bytes echo the nonce (misdelivery-detectable).
 *
 * Modes:
 *   rs_echo server PORT [pattern-seed]
 *   rs_echo client ADDR PORT CONNS SECS [stall-timeout-ms]
 *
 * Diagnostics on stall (client): per-conn rpoll state, SO_ERROR, FD count,
 * thread count, backtrace(); the client CONTINUES after the stall window to
 * observe recovery. Server: per-second totals + per-conn idle ages.
 *
 * Build: gcc -O2 -o rs_echo rs_echo.c -lrdmacm -lpthread
 */
#define _GNU_SOURCE
#include <rdma/rsocket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <execinfo.h>
#include <time.h>
#include <dirent.h>

#define MSG_MAGIC 0x31514552 /* "REQ1" LE */
#define PAYLOAD   4096

static double now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static int fd_count(void) {
    char path[64];
    snprintf(path, sizeof path, "/proc/self/fd");
    int n = 0;
    DIR *d = opendir(path);
    if (!d) return -1;
    while (readdir(d)) n++;
    closedir(d);
    return n - 2;
}
// (needs dirent.h)

static uint32_t payload_seed = 0x12345678;

static void fill_payload(uint8_t *p, uint32_t nonce) {
    uint32_t x = nonce ^ payload_seed;
    for (int i = 0; i < PAYLOAD; i++) {
        x = x * 1664525 + 1013904223;
        p[i] = (uint8_t)(x >> 24);
    }
    memcpy(p, &nonce, 4); /* nonce echo in the clear */
}

/* ---------------- server ---------------- */
static volatile uint64_t srv_served = 0;
static volatile uint64_t srv_errors = 0;
static int srv_nconns = 0;
static volatile int64_t *srv_last_us; /* per-conn-index last activity */
static int *srv_fds;

static void *srv_thread(void *arg) {
    int fd = (int)(intptr_t)arg;
    uint8_t req[8], out[PAYLOAD];
    for (;;) {
        size_t got = 0;
        while (got < 8) {
            ssize_t n = rread(fd, req + got, 8 - got);
            if (n <= 0) { __atomic_add_fetch(&srv_errors, 1, __ATOMIC_RELAXED); return NULL; }
            got += (size_t)n;
        }
        uint32_t magic, nonce;
        memcpy(&magic, req, 4);
        memcpy(&nonce, req + 4, 4);
        if (magic != MSG_MAGIC) { __atomic_add_fetch(&srv_errors, 1, __ATOMIC_RELAXED); return NULL; }
        fill_payload(out, nonce);
        size_t sent = 0;
        while (sent < PAYLOAD) {
            ssize_t n = rwrite(fd, out + sent, PAYLOAD - sent);
            if (n <= 0) { __atomic_add_fetch(&srv_errors, 1, __ATOMIC_RELAXED); return NULL; }
            sent += (size_t)n;
        }
        __atomic_add_fetch(&srv_served, 1, __ATOMIC_RELAXED);
    }
}

static int srv_run(int port) {
    int lfd = rsocket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("rsocket"); return 1; }
    struct sockaddr_in sa; memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET; sa.sin_port = htons(port); sa.sin_addr.s_addr = INADDR_ANY;
    if (rbind(lfd, (struct sockaddr *)&sa, sizeof sa) < 0) { perror("rbind"); return 1; }
    if (rlisten(lfd, 256) < 0) { perror("rlisten"); return 1; }
    struct sockaddr_in la; socklen_t ll = sizeof la;
    if (rgetsockname(lfd, (struct sockaddr *)&la, &ll) == 0)
        printf("rs_echo server: bound %s:%d (rsocket)\n", inet_ntoa(la.sin_addr), ntohs(la.sin_port));
    else
        printf("rs_echo server: bound 0.0.0.0:%d (rsocket)\n", port);
    srv_last_us = calloc(4096, sizeof(int64_t));
    srv_fds = calloc(4096, sizeof(int));
    double t0 = now_us();
    for (;;) {
        int cfd = raccept(lfd, NULL, NULL);
        if (cfd < 0) { perror("raccept"); continue; }
        if (srv_nconns < 4096) {
            srv_fds[srv_nconns] = cfd;
            srv_last_us[srv_nconns++] = (int64_t)now_us();
        }
        pthread_t th; pthread_create(&th, NULL, srv_thread, (void *)(intptr_t)cfd);
        pthread_detach(th);
        double up = now_us() - t0;
        printf("[srv %6.1fs] conns=%d served=%llu errors=%llu fds=%d\n",
               up / 1e6, srv_nconns,
               (unsigned long long)__atomic_load_n(&srv_served, __ATOMIC_RELAXED),
               (unsigned long long)__atomic_load_n(&srv_errors, __ATOMIC_RELAXED),
               fd_count());
        fflush(stdout);
    }
}

/* ---------------- client ---------------- */
typedef struct {
    int idx;
    int fd;
    uint64_t ok, timeouts, errors;
    int64_t last_ok_us;
    int stalled;         /* set once a stall was observed */
    double stall_at_us;
    uint32_t nonce;
} client_conn_t;

static client_conn_t *ccs;
static int cc_n;
static volatile uint64_t cli_ok = 0, cli_timeouts = 0, cli_errors = 0;
static double *lat; static size_t lat_n, lat_cap;
static pthread_mutex_t lat_mu = PTHREAD_MUTEX_INITIALIZER;
static int stall_ms = 3000;

static void dump_diag(client_conn_t *cc) {
    printf("  [DIAG conn %d fd=%d] stalled since %.1fms ago\n",
           cc->idx, cc->fd, (now_us() - cc->stall_at_us) / 1e3);
    struct pollfd p = { .fd = cc->fd, .events = POLLIN | POLLOUT };
    int rv = rpoll(&p, 1, 0);
    printf("    rpoll(0)=%d revents=0x%x (POLLIN=%d POLLOUT=%d POLLERR=%d POLLHUP=%d)\n",
           rv, rv > 0 ? p.revents : 0,
           !!(rv > 0 && (p.revents & POLLIN)), !!(rv > 0 && (p.revents & POLLOUT)),
           !!(rv > 0 && (p.revents & POLLERR)), !!(rv > 0 && (p.revents & POLLHUP)));
    int soerr = -1; socklen_t sl = sizeof soerr;
    if (rgetsockopt(cc->fd, SOL_SOCKET, SO_ERROR, &soerr, &sl) == 0)
        printf("    SO_ERROR=%d (%s)\n", soerr, soerr ? strerror(soerr) : "clear");
    void *bt[16]; int n = backtrace(bt, 16);
    printf("    backtrace(this thread):");
    for (int i = 0; i < n; i++) printf(" %p", bt[i]);
    printf("\n    fds=%d\n", fd_count());
    fflush(stdout);
}

static void *cli_thread(void *arg) {
    client_conn_t *cc = (client_conn_t *)arg;
    uint8_t req[8], in[PAYLOAD];
    uint32_t nonce = (uint32_t)(cc->idx * 7919);
    for (;;) {
        memcpy(req, &(uint32_t){MSG_MAGIC}, 4);
        memcpy(req + 4, &nonce, 4);
        size_t sent = 0;
        while (sent < 8) {
            ssize_t n = rwrite(cc->fd, req + sent, 8 - sent);
            if (n <= 0) { __atomic_add_fetch(&cli_errors, 1, __ATOMIC_RELAXED); cc->errors++; return NULL; }
            sent += (size_t)n;
        }
        /* bounded wait: rpoll slices until deadline */
        double t0 = now_us();
        int got = 0;
        for (;;) {
            struct pollfd p = { .fd = cc->fd, .events = POLLIN };
            int rv = rpoll(&p, 1, 50);
            if (rv < 0) { __atomic_add_fetch(&cli_errors, 1, __ATOMIC_RELAXED); cc->errors++; return NULL; }
            if (rv > 0 && (p.revents & POLLIN)) break;
            if (now_us() - t0 > (double)stall_ms * 1000) {
                /* stall: record + diagnose ONCE per conn, then keep waiting
                   (recovery observation) */
                if (!cc->stalled) {
                    cc->stalled = 1;
                    cc->stall_at_us = now_us();
                    __atomic_add_fetch(&cli_timeouts, 1, __ATOMIC_RELAXED);
                    cc->timeouts++;
                    dump_diag(cc);
                }
                __atomic_add_fetch(&cli_timeouts, 1, __ATOMIC_RELAXED);
                cc->timeouts++;
                if (now_us() - t0 > (double)stall_ms * 1000 * 10) {
                    /* give up on this conn after 10x stall window */
                    __atomic_add_fetch(&cli_errors, 1, __ATOMIC_RELAXED);
                    cc->errors++;
                    return NULL;
                }
            }
        }
        /* read exactly PAYLOAD */
        got = 0;
        while (got < PAYLOAD) {
            ssize_t n = rread(cc->fd, in + got, PAYLOAD - got);
            if (n <= 0) { __atomic_add_fetch(&cli_errors, 1, __ATOMIC_RELAXED); cc->errors++; return NULL; }
            got += (int)n;
        }
        uint32_t echo;
        memcpy(&echo, in, 4);
        if (echo != nonce) {
            __atomic_add_fetch(&cli_errors, 1, __ATOMIC_RELAXED);
            cc->errors++;
            fprintf(stderr, "[cli %d] NONCE MISMATCH sent=%u got=%u\n", cc->idx, nonce, echo);
            return NULL;
        }
        double d = now_us() - t0;
        pthread_mutex_lock(&lat_mu);
        if (lat_n == lat_cap) { lat_cap = lat_cap ? lat_cap * 2 : 65536; lat = realloc(lat, lat_cap * sizeof(double)); }
        lat[lat_n++] = d;
        pthread_mutex_unlock(&lat_mu);
        __atomic_add_fetch(&cli_ok, 1, __ATOMIC_RELAXED);
        cc->ok++;
        cc->last_ok_us = (int64_t)now_us();
        nonce += 9973;
    }
}

static int cmp(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

static int cli_run(const char *host, int port, int nconns, int secs) {
    struct sockaddr_in sa; memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET; sa.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) { fprintf(stderr, "bad addr\n"); return 1; }
    ccs = calloc(nconns, sizeof(client_conn_t));
    cc_n = nconns;
    double t0 = now_us();
    for (int i = 0; i < nconns; i++) {
        int fd = rsocket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { perror("rsocket"); return 1; }
        if (rconnect(fd, (struct sockaddr *)&sa, sizeof sa) < 0) { perror("rconnect"); return 1; }
        ccs[i].idx = i; ccs[i].fd = fd; ccs[i].last_ok_us = (int64_t)now_us();
        pthread_t th; pthread_create(&th, NULL, cli_thread, &ccs[i]);
        pthread_detach(th);
    }
    printf("rs_echo client: %d conns to %s:%d connected in %.1fms\n",
           nconns, host, port, (now_us() - t0) / 1e3);
    int64_t last_ok = 0;
    double leg_start = now_us();
    for (;;) {
        usleep(1000000);
        uint64_t ok = __atomic_load_n(&cli_ok, __ATOMIC_RELAXED);
        uint64_t to = __atomic_load_n(&cli_timeouts, __ATOMIC_RELAXED);
        uint64_t er = __atomic_load_n(&cli_errors, __ATOMIC_RELAXED);
        int live = 0;
        for (int i = 0; i < nconns; i++) if (ccs[i].fd >= 0) live++;
        printf("[cli %6.1fs] ok=%llu timeouts=%llu errors=%llu live=%d/%d fds=%d\n",
               (now_us() - leg_start) / 1e6,
               (unsigned long long)ok, (unsigned long long)to,
               (unsigned long long)er, live, nconns, fd_count());
        fflush(stdout);
        if (now_us() - leg_start > (double)secs * 1e6) break;
    }
    usleep(300000); /* let in-flight reads land in the histogram */
    qsort(lat, lat_n, sizeof(double), cmp);
    if (lat_n) {
        printf("[cli result] reads=%zu p50=%.0fus p95=%.0fus p99=%.0fus max=%.0fus\n",
               lat_n, lat[lat_n / 2], lat[(size_t)(lat_n * .95)], lat[(size_t)(lat_n * .99)], lat[lat_n - 1]);
    } else {
        printf("[cli result] reads=0 — TOTAL STALL\n");
    }
    int stalled_conns = 0;
    for (int i = 0; i < nconns; i++) if (ccs[i].stalled) stalled_conns++;
    printf("[cli result] conns=%d stalled_conns=%d timeouts=%llu errors=%llu\n",
           nconns, stalled_conns,
           (unsigned long long)__atomic_load_n(&cli_timeouts, __ATOMIC_RELAXED),
           (unsigned long long)__atomic_load_n(&cli_errors, __ATOMIC_RELAXED));
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s server PORT | %s client HOST PORT CONNS SECS [stall-ms]\n", argv[0], argv[0]);
        return 2;
    }
    if (!strcmp(argv[1], "server")) return srv_run(atoi(argv[2]));
    if (!strcmp(argv[1], "client") && argc >= 7) {
        if (argc >= 8) stall_ms = atoi(argv[7]);
        return cli_run(argv[2], atoi(argv[3]), atoi(argv[4]), atoi(argv[5]));
    }
    return 2;
}
