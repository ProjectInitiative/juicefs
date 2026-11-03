package gcache

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// VerbsTransport implements Transport and Listener over native libibverbs
// RC queue pairs (data path) with librdmacm rdma_cm for connection
// establishment. Like the rsockets transport, the actual datapath lives
// behind build-tag hooks: builds with `-tags rdma` link libibverbs +
// librdmacm (verbs_impl.go); without the tag both entry points return
// ErrRdmaUnsupported so normal builds need no RDMA headers or libraries.
//
// Selection is EXPLICIT: the manager's RdmaProto field chooses "rsockets"
// (default, unchanged behavior) or "verbs". An existing rsockets deployment
// is never silently switched.
type VerbsTransport struct {
	mu      sync.Mutex
	ln      net.Listener
	advAddr string
}

// verbsAvailable checks for RDMA userland (ibverbs + rdmacm) without
// requiring a device (a client-only build machine may compile fine).
func verbsAvailable() error {
	if err := rdmaAvailable(); err != nil {
		return err
	}
	return nil
}

// DialVerbs returns a native-verbs Transport for the client side. Fails
// cleanly when the build lacks the verbs backend or userland is absent.
func DialVerbs() (*VerbsTransport, error) {
	if err := verbsAvailable(); err != nil {
		return nil, err
	}
	if verbsDialFn == nil {
		return nil, ErrVerbsUnsupported
	}
	return &VerbsTransport{}, nil
}

// ListenVerbs starts the native-verbs listener bound to nics' rail
// addresses. advertise overrides the advertised host:port.
func ListenVerbs(nics []string, advertise string) (*VerbsTransport, error) {
	if err := verbsAvailable(); err != nil {
		return nil, err
	}
	if verbsListenFn == nil {
		return nil, ErrVerbsUnsupported
	}
	ln, adv, err := verbsListenFn(nics, advertise)
	if err != nil {
		return nil, err
	}
	return &VerbsTransport{ln: ln, advAddr: adv}, nil
}

// ErrVerbsUnsupported is returned when the build lacks the native verbs
// backend (build without `-tags rdma`, or rdma-core userland absent).
var ErrVerbsUnsupported = fmt.Errorf("native verbs transport not compiled in (build with -tags rdma)")

// Accept accepts one verbs connection (Listener role).
func (t *VerbsTransport) Accept() (net.Conn, error) {
	t.mu.Lock()
	ln := t.ln
	t.mu.Unlock()
	if ln == nil {
		return nil, ErrVerbsUnsupported
	}
	return ln.Accept()
}

// Close stops the listener.
func (t *VerbsTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return nil
	}
	return t.ln.Close()
}

// Addr returns the advertised listener address.
func (t *VerbsTransport) Addr() string { return t.advAddr }

// Dial connects to a verbs endpoint (net.Conn over RC queue pairs).
func (t *VerbsTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if verbsDialFn == nil {
		return nil, ErrVerbsUnsupported
	}
	return verbsDialFn(ctx, addr)
}

// Name identifies the transport ("verbs").
func (t *VerbsTransport) Name() string { return "verbs" }

// hook vars set by the rdma-tagged verbs_impl.go; nil ⇒ unsupported.
var (
	verbsDialFn   func(ctx context.Context, addr string) (net.Conn, error)
	verbsListenFn func(nics []string, advertise string) (net.Listener, string, error)
)

// verbsListenImpl is the manager-facing listen hook: nil (unsupported) in
// plain builds; the rdma-tagged implementation otherwise.
var verbsListenImpl func(nics []string, advertise string) (net.Listener, string, error)

// VerbListenForBench exposes the verbs listener for benchmark harnesses
// (nil in plain builds). Returns the listener and its advertised address.
func VerbListenForBench(nics []string, advertise string) (net.Listener, string, error) {
	if verbsListenImpl == nil {
		return nil, "", ErrVerbsUnsupported
	}
	return verbsListenImpl(nics, advertise)
}
