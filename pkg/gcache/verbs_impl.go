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

static int vb_errno() { return errno; }

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
    struct vb_buf sendbufs[8];
    struct vb_buf recvbufs[32];
    int nsend, nrecv;
    uint32_t msg_size;
};

static struct vb_conn *vb_alloc_conn(int nsend, int nrecv, uint32_t msg_size) {
    struct vb_conn *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->nsend = nsend; c->nrecv = nrecv; c->msg_size = msg_size;
    return c;
}

static void vb_free_bufs(struct vb_conn *c) {
    for (int i = 0; i < 8; i++) {
        if (c->sendbufs[i].mr) { ibv_dereg_mr(c->sendbufs[i].mr); c->sendbufs[i].mr = NULL; }
        free(c->sendbufs[i].data); c->sendbufs[i].data = NULL;
    }
    for (int i = 0; i < 32; i++) {
        if (c->recvbufs[i].mr) { ibv_dereg_mr(c->recvbufs[i].mr); c->recvbufs[i].mr = NULL; }
        free(c->recvbufs[i].data); c->recvbufs[i].data = NULL;
    }
    c->nsend = c->nrecv = 0;
}

static void vb_destroy_conn(struct vb_conn *c) {
    if (!c) return;
    vb_free_bufs(c);
    if (c->qp) { ibv_destroy_qp(c->qp); c->qp = NULL; }
    if (c->rcq) { ibv_destroy_cq(c->rcq); c->rcq = NULL; }
    if (c->scq) { ibv_destroy_cq(c->scq); c->scq = NULL; }
    if (c->pd) { ibv_dealloc_pd(c->pd); c->pd = NULL; }
    if (c->id) { rdma_destroy_id(c->id); c->id = NULL; }
    free(c);
}

static int vb_reg(struct ibv_pd *pd, struct vb_buf *b, uint32_t len) {
    memset(b, 0, sizeof *b);
    b->data = malloc(len);
    if (!b->data) return -1;
    b->len = len;
    b->mr = ibv_reg_mr(pd, b->data, len, IBV_ACCESS_LOCAL_WRITE);
    if (!b->mr) { free(b->data); b->data = NULL; return -1; }
    return 0;
}

static int vb_create_qp(struct vb_conn *c) {
    struct ibv_qp_init_attr a;
    memset(&a, 0, sizeof a);
    a.cap.max_send_wr = c->nsend + 8;
    a.cap.max_recv_wr = c->nrecv;
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
	"log"
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
	vbRecvDepth = 32               // recv ring slots (2 MiB/conn)
	vbSendDepth = 8                // send pool slots
	vbMaxFrame  = 16 * 1024 * 1024 // 4 MiB blocks + headroom
	vbOpTimeout = 5 * time.Second  // default op deadline when none set
	vbDialTimeout = 5 * time.Second
)

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
type verbsListener struct {
	id     *C.struct_rdma_cm_id
	mu     sync.Mutex
	closed atomic.Bool
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
	return &verbsListener{id: lid}, net.JoinHostPort(host, fmt.Sprint(port)), nil
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
		var cid *C.struct_rdma_cm_id
		if C.rdma_get_request(id, &cid) == 0 {
			conn, err := newVerbsConn(cid, nil, true)
			if err != nil {
				if cid != nil {
					C.rdma_destroy_id(cid)
				}
				return nil, err
			}
			if C.rdma_accept(cid, nil) != 0 {
				conn.Close()
				return nil, fmt.Errorf("verbs accept: errno %d", C.vb_errno())
			}
			conn.state.Store(int32(stateLive))
			return conn, nil
		}
		if ln.closed.Load() {
			return nil, net.ErrClosed
		}
		time.Sleep(20 * time.Millisecond) // bounded, non-spinning accept poll
	}
}

func (ln *verbsListener) Close() error {
	ln.closed.Store(true)
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
		C.rdma_destroy_id(id)
		return nil, err
	}
	// bounded connect: run on a goroutine, poll state with a deadline
	done := make(chan error, 1)
	go func() { done <- cgoErr(C.rdma_connect(id, nil)) }()
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
	wpend    []byte // unsent tail of a partially-written frame (stream semantics)
	sendRR   int
	inflight [vbSendDepth]int32

	rmu        sync.Mutex
	readOff    int
	pending    []byte // reassembly (Go memory; wire chunks land in registered bufs)
	pendingLen int
	pendingTotal int

	frames     [][]byte
	framesMu   sync.Mutex
	framesCond *sync.Cond

	rdeadline, wdeadline atomic.Int64 // unix nanos; 0 = default

	recvErr    error
	errMu      sync.Mutex
	recvDone   chan struct{}
	closeOnce  sync.Once
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
	cn := C.vb_alloc_conn(vbSendDepth, vbRecvDepth, vbMsgSize)
	if cn == nil {
		return nil, fmt.Errorf("verbs: alloc conn")
	}
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
			C.vb_destroy_conn(cn)
			return nil, fmt.Errorf("verbs: alloc_pd: errno %d", C.vb_errno())
		}
	}
	cn.scq = C.ibv_create_cq(id.verbs, C.int(vbSendDepth+8), nil, nil, 0)
	cn.rcq = C.ibv_create_cq(id.verbs, C.int(vbRecvDepth), nil, nil, 0)
	if cn.scq == nil || cn.rcq == nil {
		C.vb_destroy_conn(cn)
		return nil, fmt.Errorf("verbs: create_cq: errno %d", C.vb_errno())
	}
	for i := 0; i < vbSendDepth; i++ {
		if C.vb_reg(cn.pd, &cn.sendbufs[i], vbMsgSize) != 0 {
			C.vb_destroy_conn(cn)
			return nil, fmt.Errorf("verbs: reg send buf")
		}
		cn.nsend++
	}
	for i := 0; i < vbRecvDepth; i++ {
		if C.vb_reg(cn.pd, &cn.recvbufs[i], vbMsgSize) != 0 {
			C.vb_destroy_conn(cn)
			return nil, fmt.Errorf("verbs: reg recv buf")
		}
		cn.nrecv++
	}
	if C.vb_create_qp(cn) != 0 {
		C.vb_destroy_conn(cn)
		return nil, fmt.Errorf("verbs: create_qp: errno %d", C.vb_errno())
	}
	for i := 0; i < vbRecvDepth; i++ {
		if C.vb_post_recv(cn, &cn.recvbufs[i]) != 0 {
			C.vb_destroy_conn(cn)
			return nil, fmt.Errorf("verbs: post_recv: errno %d", C.vb_errno())
		}
	}
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
			return
		}
		if wc.status != C.IBV_WC_SUCCESS {
			// peer disconnect or QP error: expected on Close
			if connState(c.state.Load()) == stateLive {
				c.markBroken(fmt.Errorf("verbs: recv WC status %d", wc.status))
			}
			return
		}
		b := (*C.struct_vb_buf)(unsafe.Pointer(uintptr(wc.wr_id)))
		chunk := (*[vbMsgSize]byte)(unsafe.Pointer(b.data))[:wc.byte_len:wc.byte_len]
		cp := make([]byte, len(chunk))
		copy(cp, chunk)
		vbDebugf("conn %p recv WC len=%d first16=%x", unsafe.Pointer(c.cn), wc.byte_len, cp[:min(16, len(cp))])
		if err := c.ingest(cp); err != nil {
			c.markBroken(err)
			return
		}
		if C.vb_post_recv(c.cn, b) != 0 {
			c.markBroken(fmt.Errorf("verbs: repost recv"))
			return
		}
	}
}

// ingest reassembles chunk(s) into frames; called from recvLoop only.
var verbsDebug = os.Getenv("VB_DEBUG") != ""

func vbDebugf(format string, args ...any) {
	if verbsDebug {
		log.Printf("[vb] "+format, args...)
	}
}

func (c *verbsConn) ingest(chunk []byte) error {
	if c.pending == nil {
		if len(chunk) < 4 {
			return fmt.Errorf("verbs: short chunk %d", len(chunk))
		}
		total := binary.LittleEndian.Uint32(chunk[0:4])
		if total > vbMaxFrame {
			return fmt.Errorf("verbs: frame %d > max %d", total, vbMaxFrame)
		}
		c.pending = make([]byte, 4+int(total))
		binary.LittleEndian.PutUint32(c.pending[0:4], total)
		c.pendingLen, c.pendingTotal = 4, int(total)
	}
	n := copy(c.pending[c.pendingLen:], chunk)
	c.pendingLen += n
	if c.pendingLen < 4+c.pendingTotal {
		return nil // more chunks expected
	}
	c.frames = append(c.frames, c.pending[4:]) // surface payload only; wire prefix consumed
	vbDebugf("conn %p frame complete total=%d", unsafe.Pointer(c.cn), c.pendingTotal)
	c.pending = nil
	c.framesCond.Broadcast()
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
func (c *verbsConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	deadline := c.deadline(c.rdeadline.Load())
	c.framesMu.Lock()
	for len(c.frames) == 0 {
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
		if !timeBeforeNow(deadline) {
			c.framesMu.Unlock()
			return 0, c.deadlineErr("read")
		}
		// bounded wait: sleep-slice re-check honors deadlines
		c.framesMu.Unlock()
		time.Sleep(100 * time.Microsecond)
		c.framesMu.Lock()
	}
	frame := c.frames[0]
	c.framesMu.Unlock()

	c.rmu.Lock()
	defer c.rmu.Unlock()
	n := copy(b, frame[c.readOff:])
	c.readOff += n
	if c.readOff >= len(frame) {
		c.framesMu.Lock()
		if len(c.frames) > 0 && len(c.frames[0]) == len(frame) {
			c.frames = c.frames[1:]
		}
		c.framesMu.Unlock()
		c.readOff = 0
	}
	return n, nil
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
	deadline := c.deadline(c.wdeadline.Load())

	c.wmu.Lock()
	defer c.wmu.Unlock()

	total := len(b)
	if total > vbMaxFrame {
		return 0, fmt.Errorf("verbs: frame %d > max %d", total, vbMaxFrame)
	}
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(total))
	if err := c.sendChunk(hdr, deadline); err != nil {
		return 0, err
	}
	written := 0
	for written < total {
		end := written + vbMsgSize
		if end > total {
			end = total
		}
		if err := c.sendChunk(b[written:end], deadline); err != nil {
			return written, err
		}
		written = end
	}
	return total, nil
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
						c.markBroken(fmt.Errorf("verbs: send WC status %d", wc.status))
						return fmt.Errorf("verbs: send WC status %d", wc.status)
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

// Close tears down in contract order; idempotent. The QP transition flushes
// outstanding WRs as error WCs, so no poller can be stranded mid-C-call.
func (c *verbsConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		prev := connState(c.state.Swap(int32(stateClosed)))
		c.framesMu.Lock()
		c.framesCond.Broadcast()
		c.framesMu.Unlock()
		if prev == stateLive && c.id != nil && c.cn != nil {
			C.vb_qp_to_err(c.cn) // flushes QP; error ignored (idempotent)
		}
		if c.recvDone != nil {
			select {
			case <-c.recvDone:
			case <-time.After(2 * time.Second): // bounded; teardown proceeds
			}
		}
		if c.cn != nil {
			// vb_destroy_conn tears down bufs/QP/CQs/PD AND the cm id (contract order)
			C.vb_destroy_conn(c.cn)
			c.cn = nil
			c.id = nil
		} else if c.id != nil {
			// cn never built (partial init): the id is still ours to destroy
			if C.rdma_destroy_id(c.id) != 0 {
				err = fmt.Errorf("verbs: destroy_id: errno %d", C.vb_errno())
			}
			c.id = nil
		}
	})
	return err
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

func (c *verbsConn) deadline(unixNanos int64) time.Time {
	if unixNanos == 0 {
		return time.Now().Add(vbOpTimeout)
	}
	return time.Unix(0, unixNanos)
}

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
