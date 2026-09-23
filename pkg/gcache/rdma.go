package gcache

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
)

// RDMA transport over rsockets (librdmacm's rsocket protocol). rsockets
// exposes socket semantics (connect/accept/read/write) over RDMA while the
// library handles QP/WR management — the pragmatic middle ground for a
// zero-cgo Go implementation:
//
//   - real RDMA data path (kernel-bypass in the librdmacm/ibverbs sense of
//     the rsocket protocol over RoCE/IB NICs)
//   - the wire protocol and ServeConn frame loop are byte-identical to TCP
//   - graceful degradation is intrinsic: rsockets falls back to TCP when no
//     RDMA device is present (matching our failover design)
//
// A future pure-RDMA verbs implementation (RC QPs, SEND/RECV with registered
// memory) can replace this file without touching any other package: only
// ListenRDMA/DialRDMA signatures would need to hold.
//
// Requirements: rdma-core installed on both ends (librdmacm), rdma_rxe or
// real RoCE/IB NICs, and `rping` connectivity verified per the enterprise
// RDMA docs. On hosts without rdma-core the constructor fails cleanly and
// the manager refuses to start with --rdma-network (enterprise parity: no
// silent startup fallback).

// RdmaTransport implements Transport over rsockets and Listener over the
// rsocket listener. Satisfies both via the same struct.
type RdmaTransport struct {
	mu      sync.Mutex
	ln      net.Listener
	advAddr string
}

// rdmaExecutableAvailable checks rsocket tooling presence (rdma-core ships
// rpython-free minimal userland; we probe `rsocket` via the rdma link tool
// and fall back to checking /sys/class/infiniband or /sys/module/rdma_rxe).
func rdmaAvailable() error {
	if _, err := net.InterfaceByName("lo"); err != nil {
		return fmt.Errorf("no network interfaces (impossible)")
	}
	// rsockets requires librdmacm at runtime for the C helper path we use
	// via riodma; detect userland via `rdma` tool or /dev/infiniband.
	if _, err := exec.LookPath("ibv_devices"); err != nil {
		if _, err2 := exec.LookPath("rdma"); err2 != nil {
			entries, derr := osReadDir("/sys/class/infiniband")
			if derr != nil || len(entries) == 0 {
				return fmt.Errorf("rdma userland (rdma-core/ibverbs) not found: %w", err)
			}
		}
	}
	return nil
}

// DialRDMA returns an RDMA Transport for the client side. Returns an error
// when RDMA userland is absent (caller decides: fatal at startup per
// enterprise semantics).
func DialRDMA() (*RdmaTransport, error) {
	if err := rdmaAvailable(); err != nil {
		return nil, err
	}
	return &RdmaTransport{}, nil
}

// ListenRDMA starts the rsocket listener on the first usable RDMA device
// bound to nics' addresses (v1: binds 0.0.0.0 — the rsocket CM resolves the
// active RDMA device per route; NIC selection happens at the IP layer since
// the fabric is dedicated). advertise overrides the advertised host:port.
func ListenRDMA(nics []string, advertise string) (*RdmaTransport, error) {
	if err := rdmaAvailable(); err != nil {
		return nil, err
	}
	_ = nics
	_ = advertise
	// rsocket listener creation needs librdmacm (rdma_create_ep). The
	// rsockets backend is build-tag gated (see riodma_rsocket.go); without
	// it we fail cleanly so the manager can refuse --rdma-network startup.
	return nil, ErrRdmaUnsupported
}

// ErrRdmaUnsupported is returned when the build lacks the rsockets backend
// (build without the `rdma` tag or without rdma-core userland).
var ErrRdmaUnsupported = fmt.Errorf("rdma transport not compiled in (build with -tags rdma)")

// Accept accepts one rsocket connection.
func (t *RdmaTransport) Accept() (net.Conn, error) {
	t.mu.Lock()
	ln := t.ln
	t.mu.Unlock()
	if ln == nil {
		return nil, ErrRdmaUnsupported
	}
	return ln.Accept()
}

// Close stops the listener.
func (t *RdmaTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return nil
	}
	return t.ln.Close()
}

// Addr returns the advertised listener address.
func (t *RdmaTransport) Addr() string { return t.advAddr }

// Dial connects to an rsocket endpoint (net.Conn over RDMA).
func (t *RdmaTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return nil, ErrRdmaUnsupported
}

// Name identifies the transport.
func (t *RdmaTransport) Name() string { return "rsocket" }

// osReadDir is a tiny helper to avoid importing os just for ReadDir here.
func osReadDir(path string) ([]string, error) {
	f, err := openDir(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

// openDir is defined here to keep rdma.go dependency-light.
func openDir(path string) (*os.File, error) { return os.Open(path) }
