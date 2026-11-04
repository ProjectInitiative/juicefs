//go:build rdma

// Native verbs RC-QP transport: data path + net.Conn adapter.
//
// Resource model (P0/P2 — see docs/VERBS_TRANSPORT.md):
//   - One ibv_pd + one recv comp_channel per PROCESS (shared); one send CQ +
//     one recv CQ PER CONNECTION (failure isolation per Phase-1 lessons).
//   - Every wire buffer is C-allocated and registered once
//     (recv ring: vbRecvDepth × vbMsgSize; send pool: vbSendDepth × vbMsgSize).
//     No Go-managed pointer is retained by C across async operations: wire
//     I/O lives entirely in registered C buffers; Go data is copied in/out
//     (memcpy correctness first — zero-copy is later opt-in work).
//   - Send slots are never reused while a WR referencing them is outstanding
//     (completion is reaped inline, deadline-bounded, before release).
//   - Frames: [u32 len LE][payload]; payloads > vbMsgSize are segmented into
//     vbMsgSize SENDs and reassembled into a growable Go staging buffer
//     (16 MiB cap ⇒ 4 MiB block responses work unchanged).
//
// Deadlines (P1): every completion wait is deadline-bounded; a deadline miss
// marks the conn broken (ops fail fast with os.ErrDeadlineExceeded). Close()
// disconnects the QP (flushing outstanding WRs as error WCs, waking pollers)
// and tears down in the librdmacm/libibverbs contract order; idempotent.
package gcache

/*
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/mman.h>

static int vb_errno() { return errno; }

#define VB_SEND_DEPTH_C 8
#define VB_RECV_DEPTH_C 512

struct vb_buf {
    void *data;
    struct ibv_mr *mr;
    uint32_t len;
};

struct vb_conn {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_qp *qp;
    struct ibv_cq *scq, *rcq;
    struct vb_buf sendbufs[VB_SEND_DEPTH_C];
    struct vb_buf recvbufs[VB_RECV_DEPTH_C];
    int nsend, nrecv;         // ACTUALLY REGISTERED buffers (cleanup bound)
    int nsend_cap, nrecv_cap; // configured capacity (QP WR sizing)
    uint32_t msg_size;
};

// Compile-time guard that the fixed arrays track the depth #defines; the Go side
// cross-checks these against vbSendDepth/vbRecvDepth at dial time (vb_depths_ok).
_Static_assert(sizeof(((struct vb_conn *)0)->sendbufs) == sizeof(struct vb_buf) * VB_SEND_DEPTH_C, "sendbufs size");
_Static_assert(sizeof(((struct vb_conn *)0)->recvbufs) == sizeof(struct vb_buf) * VB_RECV_DEPTH_C, "recvbufs size");

static int vb_send_depth() { return VB_SEND_DEPTH_C; }
static int vb_recv_depth() { return VB_RECV_DEPTH_C; }

static struct vb_conn *vb_alloc_conn(int nsend_cap, int nrecv_cap, uint32_t msg_size) {
    struct vb_conn *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->nsend_cap = nsend_cap;
    c->nrecv_cap = nrecv_cap;
    c->msg_size = msg_size;
    return c;
}

// Release every SUCCESSFULLY REGISTERED buffer (nsend/nrecv are the registered
// counts, not the capacity — the old code hardcoded 8/32 and leaked 480 MRs per
// conn when vbRecvDepth went 32→512). Dereg failures are reported via the
// return value, not swallowed; the allocation is freed regardless (an
// un-deregistered region is unrecoverable but must not also leak the heap).
static int vb_free_bufs(struct vb_conn *c) {
    int fail = 0;
    for (int i = 0; i < c->nsend; i++) {
        if (c->sendbufs[i].mr) { if (ibv_dereg_mr(c->sendbufs[i].mr)) fail = 1; c->sendbufs[i].mr = NULL; }
        munmap(c->sendbufs[i].data, c->sendbufs[i].len); c->sendbufs[i].data = NULL;
    }
    for (int i = 0; i < c->nrecv; i++) {
        if (c->recvbufs[i].mr) { if (ibv_dereg_mr(c->recvbufs[i].mr)) fail = 1; c->recvbufs[i].mr = NULL; }
        munmap(c->recvbufs[i].data, c->recvbufs[i].len); c->recvbufs[i].data = NULL;
    }
    c->nsend = c->nrecv = 0;
    return fail;
}

// teardown step bitmask — which destroy calls failed (resource accounting)
#define VB_TEARDOWN_QP    (1 << 0)
#define VB_TEARDOWN_MR    (1 << 1)
#define VB_TEARDOWN_CQ    (1 << 2)
#define VB_TEARDOWN_PD    (1 << 3)
#define VB_TEARDOWN_ID    (1 << 4)


// Full teardown in contract order: QP first (nothing may reference bufs or
// CQs afterwards), then MRs/buffers, then CQs, then PD, then the CM id
// (only when destroy_id is nonzero — the id is owned by the caller that
// created it and may outlive a partial-setup conn).
// Returns a bitmask of failed steps; 0 means every step succeeded.
static int vb_destroy_conn_impl(struct vb_conn *c, int destroy_id) {
    if (!c) return 0;
    int fail = 0;
    if (c->qp) {
        rdma_destroy_qp(c->id); // void; pairs with rdma_create_qp
        c->qp = NULL;
    }
    // QP destroyed above: no outstanding WR can reference the bufs anymore,
    // so deregistering them here is safe (never while WRs are in flight).
    if (vb_free_bufs(c)) fail |= VB_TEARDOWN_MR;
    if (c->rcq) { if (ibv_destroy_cq(c->rcq)) fail |= VB_TEARDOWN_CQ; c->rcq = NULL; }
    if (c->scq) { if (ibv_destroy_cq(c->scq)) fail |= VB_TEARDOWN_CQ; c->scq = NULL; }
    if (c->pd)  { if (ibv_dealloc_pd(c->pd))   fail |= VB_TEARDOWN_PD; c->pd = NULL; }
    if (destroy_id && c->id) { if (rdma_destroy_id(c->id)) fail |= VB_TEARDOWN_ID; c->id = NULL; }
    free(c);
    return fail;
}

static int vb_reg(struct ibv_pd *pd, struct vb_buf *b, uint32_t len) {
    memset(b, 0, sizeof *b);
    // mmap, not malloc: 64 KiB sits below glibc's mmap threshold, so malloc'd
    // buffers come from the sbrk heap and freed chunks are NOT returned to the
    // OS — under the wave matrix the server retained ~100 GiB of freed buffer
    // memory (RSS == accepted-conns × per-conn buffers). mmap'd anonymous
    // memory is unmapped on munmap → RSS returns to baseline after every wave.
    b->data = mmap(NULL, len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (b->data == MAP_FAILED) { b->data = NULL; return -1; }
    b->len = len;
    b->mr = ibv_reg_mr(pd, b->data, len, IBV_ACCESS_LOCAL_WRITE);
    if (!b->mr) { munmap(b->data, len); b->data = NULL; return -1; }
    return 0;
}

static int vb_create_qp(struct vb_conn *c) {
    struct ibv_qp_init_attr a;
    memset(&a, 0, sizeof a);
    a.cap.max_send_wr = c->nsend_cap + 8;
    a.cap.max_recv_wr = c->nrecv_cap;
    a.cap.max_send_sge = a.cap.max_recv_sge = 1;
    a.cap.max_inline_data = 64;
    a.qp_type = IBV_QPT_RC;
    a.send_cq = c->scq;
    a.recv_cq = c->rcq;
    a.sq_sig_all = 1; // completion reaped before buffer reuse
    if (rdma_create_qp(c->id, c->pd, &a)) return -1;
    c->qp = c->id->qp;
    return 0;
}

static int vb_post_recv(struct vb_conn *c, struct vb_buf *b) {
    struct ibv_recv_wr rr, *bad = NULL;
    struct ibv_sge sge;
    memset(&rr, 0, sizeof rr);
    memset(&sge, 0, sizeof sge);
    sge.addr = (uintptr_t)b->data;
    sge.length = b->len;
    sge.lkey = b->mr->lkey;
    rr.wr_id = (uintptr_t)b;
    rr.sg_list = &sge;
    rr.num_sge = 1;
    return ibv_post_recv(c->qp, &rr, &bad);
}

static int vb_post_send(struct vb_conn *c, struct vb_buf *b, uint32_t len, int inline_ok) {
    struct ibv_send_wr wr, *bad = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof wr);
    memset(&sge, 0, sizeof sge);
    sge.addr = (uintptr_t)b->data;
    sge.length = len;
    sge.lkey = b->mr->lkey;
    wr.wr_id = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED | (inline_ok ? IBV_SEND_INLINE : 0);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    return ibv_post_send(c->qp, &wr, &bad);
}

// create a passive CM id bound to host:port (host in dotted-quad or NULL).
// All rdma_addrinfo memory is C-allocated (cgo pointer rules).
static int vb_listen(const char *host, const char *port, struct rdma_cm_id **out) {
    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    struct sockaddr_in *sin = calloc(1, sizeof *sin);
    if (!sin) return -1;
    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = host ? inet_addr(host) : INADDR_ANY;
    hints.ai_src_addr = (struct sockaddr *)sin;
    hints.ai_src_len = sizeof *sin;
    if (rdma_getaddrinfo(host, port, &hints, &res)) { free(sin); return -1; }
    free(sin);
    struct rdma_cm_id *lid = NULL;
    if (rdma_create_ep(&lid, res, NULL, NULL)) { rdma_freeaddrinfo(res); return -2; }
    rdma_freeaddrinfo(res);
    if (!lid->verbs) { rdma_destroy_id(lid); return -3; } // no device context for the route
    if (rdma_listen(lid, 256)) { rdma_destroy_id(lid); return -1; }
    *out = lid;
    return 0;
}

// create an ACTIVE CM id (no QP) resolved to host:port from src_ip.
static int vb_dial_id(const char *src_ip, const char *host, const char *port, struct rdma_cm_id **out) {
    struct rdma_addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof hints);
    hints.ai_port_space = RDMA_PS_TCP;
    struct sockaddr_in *sin = calloc(1, sizeof *sin);
    if (!sin) return -1;
    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = inet_addr(src_ip);
    hints.ai_src_addr = (struct sockaddr *)sin;
    hints.ai_src_len = sizeof *sin;
    if (rdma_getaddrinfo(host, port, &hints, &res)) { free(sin); return -1; }
    free(sin);
    struct rdma_cm_id *id = NULL;
    if (rdma_create_ep(&id, res, NULL, NULL)) { rdma_freeaddrinfo(res); return -2; }
    rdma_freeaddrinfo(res);
    if (!id->verbs) { rdma_destroy_id(id); return -3; } // no device context for the route
    *out = id;
    return 0;
}

// vb_qp_to_err transitions the QP to ERR: flushes outstanding WRs locally,
// wakes pollers, and surfaces flush-error WCs on the peer — without the
// blocking CM DREQ handshake of rdma_disconnect (which stalls the
// destructor for the full CM timeout when the peer is gone).
static int vb_qp_to_err(struct vb_conn *c) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof attr);
    attr.qp_state = IBV_QPS_ERR;
    return ibv_modify_qp(c->qp, &attr, IBV_QP_STATE);
}

// 1 wc, 0 none, -1 CQ error
static int vb_poll_scq(struct vb_conn *c, struct ibv_wc *wc) {
    int n = ibv_poll_cq(c->scq, 1, wc);
    return n > 0 ? 1 : (n < 0 ? -1 : 0);
}
static int vb_poll_rcq(struct vb_conn *c, struct ibv_wc *wc) {
    int n = ibv_poll_cq(c->rcq, 1, wc);
    return n > 0 ? 1 : (n < 0 ? -1 : 0);
}
*/
import "C"

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	vbMsgSize   = 64 * 1024        // per-SEND chunk (registered)
	vbRecvDepth = 512              // recv ring slots (32 MiB/conn)
	vbSendDepth = 8                // send pool slots
	vbMaxFrame  = 16 * 1024 * 1024 // 4 MiB blocks + headroom
	vbOpTimeout = 5 * time.Second  // default op deadline when none set
	vbDialTimeout = 5 * time.Second

	// Frame-queue accounting (item 10, revised after bug 12): the frame queue
	// itself is bounded by the RECV RING — at most vbRecvDepth chunks of
	// vbMsgSize can be in flight per conn (32 MiB), and the peer's send queue
	// (vbSendDepth) bounds outstanding SENDs. The recvLoop must NEVER stop
	// polling/reposting: the old pre-poll "queue full → sleep" check let WCs
	// pile up past the recv CQ depth (32), producing a CQ overrun that silently
	// dropped completion entries under the NVMe qd=64 workload (hardware-verified
	// F2 corruption: response wire streams with missing prefix chunks). The
	// counters below are now advisory (metrics/tests) and a paranoid failsafe.
	vbMaxQueuedFrames = 1024
	vbMaxQueuedBytes  = 48 << 20
)

// vbDestroyConn tears down the conn's resources. When destroyID is false
// the CM id is left untouched (the dial/accept caller owns it and may need
// to destroy it after a failed setup). Returns an error describing the
// first failed teardown step, or nil when everything was released cleanly.
func vbDestroyConn(c *C.struct_vb_conn, destroyID bool) error {
	if c == nil {
		return nil
	}
	fail := C.vb_destroy_conn_impl(c, cBool(destroyID))
	switch {
	case fail&C.VB_TEARDOWN_MR != 0:
		return fmt.Errorf("verbs: teardown: dereg_mr failed (errno %d)", C.vb_errno())
	case fail&C.VB_TEARDOWN_CQ != 0:
		return fmt.Errorf("verbs: teardown: destroy_cq failed (errno %d)", C.vb_errno())
	case fail&C.VB_TEARDOWN_PD != 0:
		return fmt.Errorf("verbs: teardown: dealloc_pd failed (errno %d)", C.vb_errno())
	case fail&C.VB_TEARDOWN_ID != 0:
		return fmt.Errorf("verbs: teardown: destroy_id failed (errno %d)", C.vb_errno())
	}
	return nil
}

// vbDestroyID destroys a cm id the caller owns (dial/accept error paths).
func vbDestroyID(id *C.struct_rdma_cm_id) error {
	if id == nil {
		return nil
	}
	if C.rdma_destroy_id(id) != 0 {
		return fmt.Errorf("verbs: destroy_id failed (errno %d)", C.vb_errno())
	}
	return nil
}


// verbsAddr is the net.Addr for verbs connections (host:port opaque).
type verbsAddr struct {
	network string
	addr    string
}

func (a *verbsAddr) Network() string { return a.network }
func (a *verbsAddr) String() string  { return a.addr }

// firstNICIPv4For returns the local IPv4 whose subnet best matches destIP
// (/24 first, then /16, then the first non-loopback). RoCE rails are /24s,
// so the destination's /24 identifies the right rail NIC — no explicit
// source flag needed on the client.
func firstNICIPv4For(destIP string) (string, error) {
	dest := net.ParseIP(destIP)
	if dest == nil {
		return "", fmt.Errorf("bad dest IP %q", destIP)
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var first, same16, same24 string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			if first == "" {
				first = ip4.String()
			}
			if same16 == "" && ip4[0] == dest.To4()[0] && ip4[1] == dest.To4()[1] {
				same16 = ip4.String()
			}
			if same24 == "" && ip4[0] == dest.To4()[0] && ip4[1] == dest.To4()[1] && ip4[2] == dest.To4()[2] {
				same24 = ip4.String()
			}
		}
	}
	if same24 != "" {
		return same24, nil
	}
	if same16 != "" {
		return same16, nil
	}
	if first != "" {
		return first, nil
	}
	return "", fmt.Errorf("no non-loopback IPv4 address")
}

// verbsListener accepts connections on a bound rdma_cm id.
// Shutdown (item 6): Close() flips closed, wakes every pending Accept via
// wakeCh, and destroys the listening CM id exactly once (guarded by mu +
// closeOnce). Pending Accepts return net.ErrClosed promptly instead of
// polling a dead identifier forever.
type verbsListener struct {
	id        *C.struct_rdma_cm_id
	mu        sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once
	wakeCh    chan struct{}
}

// Server-side connection accounting (Phase 2b P0): the hardware matrix showed an
// accept stall at 840 live conns with no visibility into whether conns were
// leaking or the counter was wrong. Every accepted conn is registered here and
// removed exactly once at Close; ServeLiveConns()/ServeLiveStats() expose the
// numbers for the periodic [gcache-srv] report and the idle reaper.
var (
	srvMu       sync.Mutex
	srvConns    = map[*verbsConn]int64{} // conn -> accepted-at unix nanos
	srvAccepted atomic.Int64
	srvClosed   atomic.Int64
)

// vbServerIdleTimeout is the safety-net reaper threshold for server-side conns
// with no inbound bytes. 0 disables reaping. Default covers the serve deadline
// (65s) plus margin; it is NOT the primary disconnect mechanism (a remote close
// surfaces as a flush-error WC → markBroken → Read error → deferred Close). It
// exists so a wedged or half-open peer cannot pin QPs/MRs forever.
var vbServerIdleTimeout atomic.Int64 // nanos; 0 = default below

const vbServerIdleDefault = 120 * time.Second

func srvIdleTimeout() time.Duration {
	if v := vbServerIdleTimeout.Load(); v > 0 {
		return time.Duration(v)
	}
	return vbServerIdleDefault
}

func srvRegister(c *verbsConn) {
	srvMu.Lock()
	srvConns[c] = time.Now().UnixNano()
	srvMu.Unlock()
	srvAccepted.Add(1)
}

func srvUnregister(c *verbsConn) {
	srvMu.Lock()
	if _, ok := srvConns[c]; ok {
		delete(srvConns, c)
		srvClosed.Add(1)
	}
	srvMu.Unlock()
}

// ServeLiveConns returns the number of currently-open server-side verbs conns.
func ServeLiveConns() int {
	srvMu.Lock()
	defer srvMu.Unlock()
	return len(srvConns)
}

// ServeLiveStats returns cumulative accepted/closed plus the live count.
func ServeLiveStats() (accepted, closed, live int64) {
	srvMu.Lock()
	live = int64(len(srvConns))
	srvMu.Unlock()
	return srvAccepted.Load(), srvClosed.Load(), live
}

// reapIdleServerConns closes server conns idle longer than `idle` and returns
// how many were reaped. Separate from the loop so tests can drive it directly.
func reapIdleServerConns(idle time.Duration) int {
	cutoff := time.Now().Add(-idle).UnixNano()
	srvMu.Lock()
	var stale []*verbsConn
	for c := range srvConns {
		if c.lastActive.Load() < cutoff {
			stale = append(stale, c)
		}
	}
	srvMu.Unlock()
	for _, c := range stale {
		vbDebugf("srv janitor: reaping idle conn %p", unsafe.Pointer(c.cn))
		_ = c.Close() // idempotent; wakes any blocked Read
	}
	return len(stale)
}

// startServerJanitor runs the idle-reaper loop for the life of the listener.
func (ln *verbsListener) startServerJanitor() {
	go func() {
		for {
			if ln.closed.Load() {
				return
			}
			time.Sleep(10 * time.Second)
			if ln.closed.Load() {
				return
			}
			reapIdleServerConns(srvIdleTimeout())
		}
	}()
}

func newVerbsListener(id *C.struct_rdma_cm_id) *verbsListener {
	ln := &verbsListener{id: id, wakeCh: make(chan struct{})}
	ln.startServerJanitor()
	return ln
}

// ListenVerbs binds a native-verbs listener to the rail address derived
// from advertise/nics. The advertised address is the bound host:port.
func verbsListenC(nics []string, advertise string) (net.Listener, string, error) {
	host, port, err := verbsSplitAdvertise(advertise, nics)
	if err != nil {
		return nil, "", err
	}
	var lid *C.struct_rdma_cm_id
	cHost, cPort := C.CString(host), C.CString(fmt.Sprint(port))
	defer C.free(unsafe.Pointer(cHost))
	defer C.free(unsafe.Pointer(cPort))
	if C.vb_listen(cHost, cPort, &lid) != 0 {
		return nil, "", fmt.Errorf("verbs listen: errno %d", C.vb_errno())
	}
	return newVerbsListener(lid), net.JoinHostPort(host, fmt.Sprint(port)), nil
}

func verbsSplitAdvertise(advertise string, nics []string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(advertise)
	if err != nil {
		host, portStr = advertise, "0"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host, err = firstNICIPv4(nics)
		if err != nil {
			return "", 0, fmt.Errorf("verbs listen: no rail address: %w", err)
		}
	}
	port := 0
	for _, ch := range portStr {
		if ch < '0' || ch > '9' {
			return "", 0, fmt.Errorf("verbs listen: bad port %q", portStr)
		}
		port = port*10 + int(ch-'0')
	}
	if port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("verbs listen: bad port %q", portStr)
	}
	return host, port, nil
}

func verbsHtons(v uint16) uint16 { return v<<8 | v>>8 }

func (ln *verbsListener) Accept() (net.Conn, error) {
	for {
		if ln.closed.Load() {
			return nil, net.ErrClosed
		}
		ln.mu.Lock()
		id := ln.id
		ln.mu.Unlock()
		if id == nil {
			return nil, net.ErrClosed
		}
		select {
		case <-ln.wakeCh:
			if ln.closed.Load() {
				return nil, net.ErrClosed
			}
		default:
		}
		var cid *C.struct_rdma_cm_id
		if C.rdma_get_request(id, &cid) == 0 {
			conn, err := newVerbsConn(cid, nil, true)
			if err != nil {
				// newVerbsConn does not own the id — destroy it here.
				if cid != nil {
					if cerr := vbDestroyID(cid); cerr != nil {
						vbDebugf("accept: %v", cerr)
					}
				}
				return nil, err
			}
			if C.rdma_accept(cid, nil) != 0 {
				conn.Close()
				return nil, fmt.Errorf("verbs accept: errno %d", C.vb_errno())
			}
			conn.state.Store(int32(stateLive))
			conn.lastActive.Store(time.Now().UnixNano())
			srvRegister(conn)
			return conn, nil
		}
		if ln.closed.Load() {
			return nil, net.ErrClosed
		}
		// bounded, non-spinning accept poll; Close wakes us immediately
		select {
		case <-ln.wakeCh:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Close stops the listener: wakes pending Accepts and releases the
// listening CM id exactly once. Safe to call from multiple goroutines.
func (ln *verbsListener) Close() error {
	ln.closeOnce.Do(func() {
		ln.closed.Store(true)
		ln.mu.Lock()
		id := ln.id
		ln.id = nil
		ln.mu.Unlock()
		close(ln.wakeCh) // interrupt every pending Accept
		if id != nil {
			if err := vbDestroyID(id); err != nil {
				vbDebugf("listener close: %v", err)
			}
		}
	})
	return nil
}

func (ln *verbsListener) Addr() net.Addr { return &verbsAddr{} }

// verbsDialImpl dials addr (host:port) over the rail; src binds to the
// first rail NIC so the CM resolves the mlx5 device (mgmt NICs have none).
func verbsDialImpl(ctx context.Context, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("verbs dial %s: %w", addr, err)
	}
	railIP, rerr := firstNICIPv4For(host)
	if rerr != nil {
		return nil, fmt.Errorf("verbs dial %s: no rail source IP: %w", addr, rerr)
	}
	port := 0
	for _, ch := range portStr {
		if ch < '0' || ch > '9' {
			return nil, fmt.Errorf("verbs dial %s: bad port", addr)
		}
		port = port*10 + int(ch-'0')
	}

	var id *C.struct_rdma_cm_id
	cRail := C.CString(railIP)
	cHost, cPort := C.CString(host), C.CString(portStr)
	defer C.free(unsafe.Pointer(cRail))
	defer C.free(unsafe.Pointer(cHost))
	defer C.free(unsafe.Pointer(cPort))
	if C.vb_dial_id(cRail, cHost, cPort, &id) != 0 {
		return nil, fmt.Errorf("verbs dial %s: resolve/create_ep: errno %d", addr, C.vb_errno())
	}
	// The QP must be created with OUR CQs before connecting.
	conn, err := newVerbsConn(id, nil, false)
	if err != nil {
		// newVerbsConn never owns the id; destroy it here (ownership rule).
		if cerr := vbDestroyID(id); cerr != nil {
			vbDebugf("dial %s: %v", addr, cerr)
		}
		return nil, err
	}
	// LIFETIME RACE (review item 5): rdma_connect blocks in the CM for the
	// full negotiation; a dial timeout must not free the id (or anything the
	// CM call touches) while that goroutine is still inside C. Track it:
	// Close() waits (bounded) for the connect goroutine BEFORE releasing any
	// C resource. If the goroutine is wedged beyond the bound, the id is
	// deliberately leaked (documented) rather than freed under an active
	// cgo call — a bounded, observable leak beats memory corruption.
	conn.connectDone = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		defer close(conn.connectDone) // signals Close that C code has been left
		done <- cgoErr(C.rdma_connect(id, nil))
	}()
	deadline := time.Now().Add(vbDialTimeout)
	for {
		select {
		case e := <-done:
			if e != nil {
				conn.Close()
				return nil, fmt.Errorf("verbs dial %s: connect: %w", addr, e)
			}
			conn.state.Store(int32(stateLive))
			return conn, nil
		case <-ctx.Done():
			conn.Close()
			return nil, ctx.Err()
		default:
		}
		if time.Now().After(deadline) {
			conn.Close()
			return nil, fmt.Errorf("verbs dial %s: connect timeout after %s", addr, vbDialTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cgoErr(rv C.int) error {
	if rv == 0 {
		return nil
	}
	return fmt.Errorf("errno %d", C.vb_errno())
}

// verbsConn implements net.Conn over one RC queue pair.
type verbsConn struct {
	id *C.struct_rdma_cm_id
	cn *C.struct_vb_conn

	isServer bool
	state    atomic.Int32 // connState

	// wire state
	wmu      sync.Mutex
	sendRR   int
	inflight [vbSendDepth]int32

	rmu        sync.Mutex
	readOff    int
	pending    []byte // reassembly (Go memory; wire chunks land in registered bufs)
	pendingLen int
	pendingTotal int
	head       []byte // sub-4-byte prefix fragment carry (arbitrary chunk boundaries)

	frames     [][]byte
	framesMu   sync.Mutex
	framesCond *sync.Cond
	queuedBytes int // protected by framesMu; bytes of unconsumed frame payloads

	rdeadline, wdeadline atomic.Int64 // unix nanos; 0 = default

	recvErr    error
	errMu      sync.Mutex
	recvDone   chan struct{}
	closeOnce   sync.Once
	connectDone chan struct{} // closed when the dial goroutine has left C code
	loopStarted bool          // recvLoop was started (guards the recvDone wait)
	lastActive  atomic.Int64  // unix nanos of last inbound byte (server idle reaper)
}

var _ net.Conn = (*verbsConn)(nil)

type connState int32

const (
	stateInit connState = iota
	stateLive
	stateBroken
	stateClosed
)

// dialMu serializes connection setup: concurrent ibv_reg_mr calls storm the
// mlx5 driver's pinning path (observed: 40 goroutines stuck in reg_mr on a
// node with pinned-memory pressure). Registration is per-conn one-shot, so
// serializing it costs nothing in steady state.
var dialMu sync.Mutex

func newVerbsConn(id *C.struct_rdma_cm_id, sharedPD *C.struct_ibv_pd, isServer bool) (*verbsConn, error) {
	dialMu.Lock()
	defer dialMu.Unlock()
	// Cross-check the C fixed arrays against the Go depth constants (bug 12
	// shakeout: vbRecvDepth 32→512 left recvbufs[32] and panicked on dial).
	if int(C.vb_send_depth()) != vbSendDepth || int(C.vb_recv_depth()) != vbRecvDepth {
		return nil, fmt.Errorf("verbs: C buffer depths (%d/%d) out of sync with Go constants (%d/%d)",
			C.vb_send_depth(), C.vb_recv_depth(), vbSendDepth, vbRecvDepth)
	}
	cn := C.vb_alloc_conn(vbSendDepth, vbRecvDepth, vbMsgSize)
	if cn == nil {
		return nil, fmt.Errorf("verbs: alloc conn")
	}
	// CM-ID OWNERSHIP: the caller (dial/accept) created the id and owns it
	// through every failure path below; we must NOT destroy it here, or a
	// caller-side destroy would double-free. vb_destroy_conn_impl's
	// destroy_id flag enforces this.
	const destroyID = C.int(0)
	c := &verbsConn{id: id, cn: cn, isServer: isServer}
	cn.id = id       // C side needs the cm id for QP creation and teardown
	// (is_server is Go-side only)
	c.framesCond = sync.NewCond(&c.framesMu)
	c.recvDone = make(chan struct{})

	if sharedPD != nil {
		cn.pd = sharedPD
	} else {
		cn.pd = C.ibv_alloc_pd(id.verbs)
		if cn.pd == nil {
			C.vb_destroy_conn_impl(cn, destroyID)
			return nil, fmt.Errorf("verbs: alloc_pd: errno %d", C.vb_errno())
		}
	}
	cn.scq = C.ibv_create_cq(id.verbs, C.int(vbSendDepth+8), nil, nil, 0)
	cn.rcq = C.ibv_create_cq(id.verbs, C.int(vbRecvDepth), nil, nil, 0)
	if cn.scq == nil || cn.rcq == nil {
		C.vb_destroy_conn_impl(cn, destroyID)
		return nil, fmt.Errorf("verbs: create_cq: errno %d", C.vb_errno())
	}
	for i := 0; i < vbSendDepth; i++ {
		if C.vb_reg(cn.pd, &cn.sendbufs[i], vbMsgSize) != 0 {
			C.vb_destroy_conn_impl(cn, destroyID)
			return nil, fmt.Errorf("verbs: reg send buf")
		}
		cn.nsend++
	}
	for i := 0; i < vbRecvDepth; i++ {
		if C.vb_reg(cn.pd, &cn.recvbufs[i], vbMsgSize) != 0 {
			C.vb_destroy_conn_impl(cn, destroyID)
			return nil, fmt.Errorf("verbs: reg recv buf")
		}
		cn.nrecv++
	}
	if C.vb_create_qp(cn) != 0 {
		C.vb_destroy_conn_impl(cn, destroyID)
		return nil, fmt.Errorf("verbs: create_qp: errno %d", C.vb_errno())
	}
	for i := 0; i < vbRecvDepth; i++ {
		if C.vb_post_recv(cn, &cn.recvbufs[i]) != 0 {
			C.vb_destroy_conn_impl(cn, destroyID)
			return nil, fmt.Errorf("verbs: post_recv: errno %d", C.vb_errno())
		}
	}
	c.loopStarted = true
	go c.recvLoop()
	return c, nil
}

// recvLoop owns the recv CQ exclusively: reap WCs, reassemble frames,
// re-post buffers. Exits when the conn leaves stateLive.
func (c *verbsConn) recvLoop() {
	defer close(c.recvDone)
	for {
		st := connState(c.state.Load())
		if st == stateBroken || st == stateClosed {
			return
		}
		if st == stateInit {
			// not yet connected (dial/accept in flight); recvLoop must survive
			// until the conn goes live (it posts nothing until then)
			time.Sleep(100 * time.Microsecond)
			continue
		}
		var wc C.struct_ibv_wc
		n := C.vb_poll_rcq(c.cn, &wc)
		if n == 0 {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		if n < 0 {
			c.markBroken(fmt.Errorf("verbs: recv CQ error"))
			// Deterministic teardown (Phase 2b P0): a fatal CQ means the conn is
			// finished — release its QP/CQs/MRs now instead of waiting for a
			// deferred Close that may never run (ServeConn wedged elsewhere).
			go c.Close()
			return
		}
		if wc.status != C.IBV_WC_SUCCESS {
			// peer disconnect or QP error: expected on Close — and the signal
			// that an ABANDONED conn (client exited without graceful Close)
			// must be torn down NOW. markBroken alone leaves the QP/CQs/MRs
			// allocated until something else calls Close — hardware showed
			// ~800 conns accumulating exactly that way under the wave matrix.
			if connState(c.state.Load()) == stateLive {
				c.markBroken(fmt.Errorf("verbs: recv WC status %d (%s) — peer disconnect, QP error, or RNR exhaustion", wc.status, C.GoString(C.ibv_wc_status_str(wc.status))))
			}
			go c.Close() // idempotent; also wakes any blocked Read via Broadcast
			return
		}
		// Bug 12 (hardware-verified): copy out of the registered buffer and
		// RE-POST IT IMMEDIATELY, before ingesting. The old code ingested
		// first and skipped polling entirely while the frame queue was full —
		// unpolled WCs then piled up past the recv CQ depth and the CQ
		// overran (silent completion loss → desynchronized wire streams).
		// Backpressure now comes from the recv ring itself: once all
		// vbRecvDepth buffers are outstanding the RQ stops accepting and the
		// peer's SENDs park on RNR instead of data being lost.
		b := (*C.struct_vb_buf)(unsafe.Pointer(uintptr(wc.wr_id)))
		chunk := (*[vbMsgSize]byte)(unsafe.Pointer(b.data))[:wc.byte_len:wc.byte_len]
		cp := make([]byte, len(chunk))
		copy(cp, chunk)
		c.lastActive.Store(time.Now().UnixNano()) // server idle-reaper heartbeat
		if C.vb_post_recv(c.cn, b) != 0 {
			c.markBroken(fmt.Errorf("verbs: repost recv"))
			return
		}
		vbDebugf("conn %p recv WC len=%d first16=%x", unsafe.Pointer(c.cn), wc.byte_len, cp[:min(16, len(cp))])
		if err := c.ingest(cp); err != nil {
			c.markBroken(err)
			return
		}
	}
}

// ingest reassembles chunk(s) into frames; called from recvLoop only.
//
// The wire framing is [u32 total][total payload bytes]; a chunk (one recv WC)
// may carry ANY byte boundary: a lone prefix, a payload piece, a whole frame,
// or several frames coalesced (and one frame split across many chunks). The
// reassembler is therefore written as a strict stream consumer: every input
// byte is accounted for exactly once — a frame completes the moment its last
// byte arrives, and any tail bytes beyond the frame roll into the next frame.
// The previous implementation silently DROPPED chunk tails that exceeded the
// current frame's remaining capacity (copy() truncation) and never completed
// a whole-frame chunk until the next chunk arrived — either case permanently
// desynchronized the frame stream by the dropped size, surfacing downstream as
// garbage request pairs (e.g. off parsed from 'GC\x01\x06' magic + wire
// prefixes) and payload mismatches with correct lengths (hardware-verified,
// Phase-2 F2/F3/F4).
func (c *verbsConn) ingest(chunk []byte) error {
	// Carry a sub-4-byte head fragment until enough bytes for the prefix.
	if c.pending == nil && len(c.head) > 0 {
		need := 4 - len(c.head)
		take := need
		if len(chunk) < take {
			take = len(chunk)
		}
		c.head = append(c.head, chunk[:take]...)
		chunk = chunk[take:]
		if len(c.head) < 4 {
			return nil // still mid-prefix
		}
		// fall through with the assembled prefix at the head of `chunk`
		chunk = append(c.head, chunk...)
		c.head = nil
	}
	for len(chunk) > 0 {
		if c.pending == nil {
			if len(chunk) < 4 {
				// mid-prefix fragment: hold it
				c.head = append(c.head, chunk...)
				return nil
			}
			total := binary.LittleEndian.Uint32(chunk[0:4])
			if total > vbMaxFrame {
				return fmt.Errorf("verbs: frame %d > max %d", total, vbMaxFrame)
			}
			c.pending = make([]byte, 4+int(total))
			c.pendingTotal = int(total)
			copy(c.pending[0:4], chunk[0:4])
			c.pendingLen = 4
			chunk = chunk[4:]
			if len(chunk) == 0 {
				return nil // bare prefix: payload arrives in later chunks
			}
		}
		n := copy(c.pending[c.pendingLen:], chunk)
		c.pendingLen += n
		chunk = chunk[n:]
		if c.pendingLen < 4+c.pendingTotal {
			return nil // more chunks expected
		}
		payload := c.pending[4:] // surface payload only; wire prefix consumed
		// framesMu: the frame queue is shared with Read()/pop — the append and
		// the byte accounting must be atomic against a concurrent pop that
		// re-slices c.frames (torn slice headers otherwise — hardware faulted
		// in Read's memmove with a garbage data pointer, Phase-2 F2 probe).
		// Broadcast WHILE HOLDING framesMu: Read()'s wait protocol checks the
		// empty queue and calls Cond.Wait under one continuous hold, so a
		// Broadcast released outside the lock could land in the check→wait
		// window and be lost forever (lost-wakeup). Holding the lock here makes
		// producer notification and consumer wait registration mutually
		// exclusive by construction.
		c.framesMu.Lock()
		c.frames = append(c.frames, payload)
		c.queuedBytes += len(payload)
		// Hard bound (Phase 2b P1-2): the ring (vbRecvDepth × vbMsgSize) plus the
		// peer's send depth is the physical ceiling of unread data — the peer
		// cannot have more in flight than its send queue holds, and once all our
		// recv buffers are outstanding its SENDs park on RNR. Exceeding the
		// bound means a peer/protocol violation: fail loudly instead of growing
		// without bound. Never silently drops a frame.
		if c.queuedBytes > vbRecvDepth*vbMsgSize {
			qb := c.queuedBytes
			c.framesMu.Unlock()
			return fmt.Errorf("verbs: frame queue %d bytes exceeds ring bound %d (protocol violation)",
				qb, vbRecvDepth*vbMsgSize)
		}
		c.framesCond.Broadcast()
		c.framesMu.Unlock()
		vbDebugf("conn %p frame complete total=%d", unsafe.Pointer(c.cn), c.pendingTotal)
		c.pending = nil
		c.pendingLen = 0
		// loop: any chunk tail belongs to the next frame
	}
	return nil
}

func (c *verbsConn) markBroken(err error) {
	c.errMu.Lock()
	if c.recvErr == nil {
		c.recvErr = err
	}
	c.errMu.Unlock()
	c.state.CompareAndSwap(int32(stateLive), int32(stateBroken))
	c.framesMu.Lock()
	c.framesCond.Broadcast()
	c.framesMu.Unlock()
}

func (c *verbsConn) brokenErr() error {
	if connState(c.state.Load()) == stateLive {
		return nil
	}
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.recvErr != nil {
		return c.recvErr
	}
	return os.ErrClosed
}

// Read returns the next frame payload with stream semantics (a frame larger
// than len(b) is delivered across consecutive Reads; the 4-byte header is
// consumed internally and not surfaced).
//
// Synchronization (item 3): the ENTIRE read path — frame queue and read
// offset — is serialized under rmu, so concurrent Reads cannot interleave
// copies into the same frame or double-pop it. framesMu protects only the
// queue produced by recvLoop. Deadlines are re-loaded every wait iteration
// so SetReadDeadline affects a pending Read (item 7); the zero deadline
// means "wait indefinitely" (no synthetic default), and a zero-time
// SetDeadline means non-blocking.
func (c *verbsConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()

	for {
		// The empty-check → Cond.Wait sequence must happen under ONE continuous
		// framesMu hold: ingest/markBroken/Close Broadcast while holding
		// framesMu, so a notification cannot land between the check and the
		// wait registration (the previous unlock→goroutine→Wait dance had
		// exactly that lost-wakeup window, plus a goroutine per wait).
		c.framesMu.Lock()
		if len(c.frames) > 0 {
			frame := c.frames[0]
			c.framesMu.Unlock()

			n := copy(b, frame[c.readOff:])
			c.readOff += n
			if c.readOff >= len(frame) {
				c.framesMu.Lock()
				if len(c.frames) > 0 && &c.frames[0][0] == &frame[0] {
					c.frames = c.frames[1:]
					c.queuedBytes -= len(frame)
				}
				c.framesMu.Unlock()
				c.readOff = 0
			}
			return n, nil
		}

		// no frame available under the same hold: check state before waiting
		if st := connState(c.state.Load()); st != stateLive {
			c.framesMu.Unlock()
			if st == stateClosed {
				return 0, os.ErrClosed
			}
			c.errMu.Lock()
			e := c.recvErr
			c.errMu.Unlock()
			if e != nil {
				return 0, e
			}
			return 0, os.ErrClosed
		}

		// deadline handling (item 7): re-loaded per iteration so a
		// SetReadDeadline issued while this Read is pending takes effect.
		// A zero deadline means "wait indefinitely" (no synthetic default); a
		// zero-TIME deadline means non-blocking (fail immediately).
		dl := c.rdeadline.Load()
		stopTimer := func() {}
		if dl != 0 {
			d := time.Until(time.Unix(0, dl))
			if d <= 0 {
				c.framesMu.Unlock()
				return 0, c.deadlineErr("read")
			}
			if d > vbOpTimeout {
				d = vbOpTimeout // re-check state periodically even w/o deadline
			}
			// One-shot waker: broadcasts on the cond at expiry. It exits early
			// (stopTimer) when the wait completes first, so no accumulation.
			stop := make(chan struct{})
			stopTimer = func() { close(stop) }
			go func(d time.Duration, stop <-chan struct{}) {
				t := time.NewTimer(d)
				defer t.Stop()
				select {
				case <-t.C:
					c.framesMu.Lock()
					c.framesCond.Broadcast()
					c.framesMu.Unlock()
				case <-stop:
				}
			}(d, stop)
		}
		// Atomically release framesMu and sleep; any Broadcast (frame, close,
		// error, deadline waker) re-acquires it before Wait returns.
		c.framesCond.Wait()
		stopTimer()
		c.framesMu.Unlock()
		// loop: re-check queue/state/deadline from the top
	}
}

// Write sends b as (the payload continuation of) one framed message.
// The first Write of a message prepends the u32 length; the frame is
// closed when ServeConn finishes the response (stream semantics, like TCP).
// For the gcache protocol each Write is a complete frame, so the frame is
// flushed at the end of every Write that ends a logical message. We treat
// every Write as a complete frame: ServeConn writes whole frames per op
// (writeResp assembles the full response then writes once) — verified
// against pkg/gcache/server.go's framing.
func (c *verbsConn) Write(b []byte) (int, error) {
	if err := c.brokenErr(); err != nil {
		return 0, err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()

	total := len(b)
	if total > vbMaxFrame {
		return 0, fmt.Errorf("verbs: frame %d > max %d", total, vbMaxFrame)
	}
	if total == 0 {
		// zero-length write: no-op. Sending a total=0 frame would be a
		// wire-level no-payload frame; the gcache protocol never emits one
		// and io.Writer permits skipping empty writes.
		return 0, nil
	}
	// item 7: the deadline is re-loaded for every chunk, so a
	// SetWriteDeadline issued while a multi-chunk Write is pending
	// affects the outstanding chunks.
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(total))
	if err := c.sendChunk(hdr, c.chunkDeadline()); err != nil {
		return 0, err
	}
	written := 0
	for written < total {
		end := written + vbMsgSize
		if end > total {
			end = total
		}
		if err := c.sendChunk(b[written:end], c.chunkDeadline()); err != nil {
			return written, err
		}
		written = end
	}
	return total, nil
}

// chunkDeadline converts the current write deadline into a concrete time
// for one sendChunk. Zero sentinel → default op timeout (unchanged
// behavior); explicit past deadlines → immediate expiry.
func (c *verbsConn) chunkDeadline() time.Time {
	n := c.wdeadline.Load()
	if n == 0 {
		return time.Now().Add(vbOpTimeout)
	}
	return time.Unix(0, n)
}

// sendChunk acquires a free send slot, copies data in, posts, and waits
// (deadline-bounded) for the completion before releasing the slot.
func (c *verbsConn) sendChunk(data []byte, deadline time.Time) error {
	if len(data) > vbMsgSize {
		return fmt.Errorf("verbs: chunk %d > %d", len(data), vbMsgSize)
	}
	for {
		for i := 0; i < vbSendDepth; i++ {
			slot := (c.sendRR + i) % vbSendDepth
			if atomic.LoadInt32(&c.inflight[slot]) != 0 {
				continue
			}
			buf := &c.cn.sendbufs[slot]
			dst := (*[vbMsgSize]byte)(unsafe.Pointer(buf.data))[:len(data):len(data)]
			copy(dst, data)
			inline := len(data) <= 64
			if C.vb_post_send(c.cn, buf, C.uint32_t(len(data)), cBool(inline)) != 0 {
				return fmt.Errorf("verbs: post_send: errno %d", C.vb_errno())
			}
			atomic.StoreInt32(&c.inflight[slot], 1)
			for {
				var wc C.struct_ibv_wc
				n := C.vb_poll_scq(c.cn, &wc)
				if n > 0 {
					atomic.StoreInt32(&c.inflight[slot], 0)
					if wc.status != C.IBV_WC_SUCCESS {
						s := C.GoString(C.ibv_wc_status_str(wc.status))
						c.markBroken(fmt.Errorf("verbs: send WC status %d (%s)", wc.status, s))
						return fmt.Errorf("verbs: send WC status %d (%s)", wc.status, s)
					}
					c.sendRR = (slot + 1) % vbSendDepth
					return nil
				}
				if n < 0 {
					atomic.StoreInt32(&c.inflight[slot], 0)
					return fmt.Errorf("verbs: send CQ error")
				}
				if !timeBeforeNow(deadline) {
					atomic.StoreInt32(&c.inflight[slot], 0)
					// the WR is still outstanding: park the slot forever —
					// it can only be reclaimed at teardown (state Broken
					// means the conn is finished anyway)
					c.markBroken(c.deadlineErr("write"))
					return c.deadlineErr("write")
				}
				runtime.Gosched()
			}
		}
		if !timeBeforeNow(deadline) {
			return c.deadlineErr("write")
		}
		runtime.Gosched()
	}
}

// Close tears down in contract order; idempotent and safe for Live, Broken
// and Init states (item 4).
//
// Quiescence guarantee: active read/write operations finish (or fail) before
// any C resource is released. The sequence is:
//  1. state → Closed: wakes every waiter (Read cond, recvLoop, accept polls)
//  2. QP → ERR (when Live): flushes outstanding WRs as error WCs so no
//     poller can be stranded mid-completion-wait
//  3. wait for the dial-time rdma_connect goroutine (item 5) — bounded by
//     vbDialTimeout+2s; past that the id is LEAKED (never freed under an
//     active cgo call) and the failure is reported
//  4. wait for recvLoop (unconditional): its polls are non-blocking and it
//     exits on the state flip, so this returns promptly. We NEVER free
//     resources merely because a join timeout expired — if the loop is wedged
//     we leak (and report) instead of corrupting memory.
//  5. release C resources in contract order; per-step failures are returned.
func (c *verbsConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		srvUnregister(c) // server-side accounting; no-op for client conns
		prev := connState(c.state.Swap(int32(stateClosed)))

		// wake every waiter
		c.framesMu.Lock()
		c.framesCond.Broadcast()
		c.framesMu.Unlock()

		// flush the QP so pollers observe error WCs and exit their loops
		if prev == stateLive && c.cn != nil && c.cn.qp != nil {
			C.vb_qp_to_err(c.cn) // idempotent; error folded into teardown below
		}

		// item 5: the dial goroutine may still be inside rdma_connect(id).
		quiesced := c.waitConnect()
		if !quiesced {
			err = fmt.Errorf("verbs: close: rdma_connect still in flight after %s; cm id leaked to avoid use-after-free", vbDialTimeout+2*time.Second)
			return // never free while a C call is active
		}

		// item 4: wait for the recv thread unconditionally — it exits on the
		// state flip (non-blocking poll + 100µs slice), so this is prompt.
		// No timeout-and-free: a wedged loop means we leak, not corrupt.
		if c.recvDone != nil && c.loopStarted {
			<-c.recvDone
		}

		if c.cn != nil {
			if terr := vbDestroyConn(c.cn, true); terr != nil && err == nil {
				err = terr
			}
			c.cn = nil
			c.id = nil
		} else if c.id != nil {
			// cn never built (partial init): the id is still ours to destroy
			if derr := vbDestroyID(c.id); derr != nil && err == nil {
				err = derr
			}
			c.id = nil
		}
	})
	return err
}

// waitConnect blocks until the dial-time rdma_connect goroutine has left C
// code. Returns false if it is still in flight after a generous bound —
// in which case Close must NOT release the id (leak, report).
func (c *verbsConn) waitConnect() bool {
	if c.connectDone == nil {
		return true // no dial in flight (accepted conns)
	}
	select {
	case <-c.connectDone:
		return true
	case <-time.After(vbDialTimeout + 2*time.Second):
		return false
	}
}

func (c *verbsConn) LocalAddr() net.Addr  { return &verbsAddr{} }
func (c *verbsConn) RemoteAddr() net.Addr { return &verbsAddr{} }

func (c *verbsConn) SetDeadline(t time.Time) error {
	n := t.UnixNano()
	c.rdeadline.Store(n)
	c.wdeadline.Store(n)
	return nil
}
func (c *verbsConn) SetReadDeadline(t time.Time) error  { c.rdeadline.Store(t.UnixNano()); return nil }
func (c *verbsConn) SetWriteDeadline(t time.Time) error { c.wdeadline.Store(t.UnixNano()); return nil }

// deadline semantics (item 7): the zero unixNanos sentinel means "no
// deadline set" — callers treat it as wait-indefinitely (with periodic
// state re-checks). A caller may still request a non-blocking or
// already-expired deadline explicitly: time.Time{} stores the minimum
// unix nano (already past → immediate ErrDeadlineExceeded), and
// time.Now() stores "expire immediately" — both handled by the
// time.Until() <= 0 path in Read/sendChunk.
const vbNoDeadline = int64(0)

func (c *verbsConn) deadlineErr(op string) error {
	return fmt.Errorf("verbs: %s deadline exceeded: %w", op, os.ErrDeadlineExceeded)
}

func timeBeforeNow(t time.Time) bool { return time.Now().Before(t) }

func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

func init() {
	verbsDialFn = verbsDialImpl
	verbsListenFn = verbsListenC
	verbsListenImpl = verbsListenC
}
