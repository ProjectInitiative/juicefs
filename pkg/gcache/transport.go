package gcache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Transport abstracts the peer connection dialer. TCP is the default; RDMA
// (rsockets via riodma) plugs in with the same interface. The wire protocol
// and frame handling are transport-agnostic (ServeConn takes a net.Conn).
type Transport interface {
	// Dial connects to addr (host:port) and returns a stream connection.
	Dial(ctx context.Context, addr string) (net.Conn, error)
	// Name identifies the transport in logs/metrics ("tcp", "rsocket").
	Name() string
}

// TCPTransport dials with net.Dialer (default).
type TCPTransport struct{ Timeout time.Duration }

func (t *TCPTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: t.Timeout}
	return d.DialContext(ctx, "tcp", addr)
}

func (t *TCPTransport) Name() string { return "tcp" }

// endpointState tracks RDMA failover state per peer endpoint (enterprise
// 5.3.9 parity): RDMADown since t0 => new dials use TCP until retryAt.
type endpointState struct {
	rdmaDown bool
	downAt   time.Time
}

// dialFailover dials member with transport failover: prefer RDMA when the
// member advertises an RdmaAddr and RDMA isn't in backoff; on RDMA failure
// fall through to TCP and start a retry backoff; on success over TCP during
// backoff the backoff persists (failback is periodic, not per-dial).
func (c *RingClient) dialFailover(ctx context.Context, m Member) (net.Conn, Transport, error) {
	c.mu.Lock()
	st := c.epState[m.Addr]
	c.mu.Unlock()

	if m.RdmaAddr != "" && !(st.rdmaDown && time.Since(st.downAt) < rdmaRetryInterval) {
		conn, err := c.rdma.Dial(ctx, m.RdmaAddr)
		if err == nil {
			c.clearRdmaDown(m.Addr)
			return conn, c.rdma, nil
		}
		logger.Warnf("gcache rdma dial %s (%s) failed, falling back to tcp: %v", m.RdmaAddr, c.rdma.Name(), err)
		c.markRdmaDown(m.Addr)
	}
	conn, err := c.tcp.Dial(ctx, m.Addr)
	if err != nil {
		return nil, nil, err
	}
	return conn, c.tcp, nil
}

func (c *RingClient) markRdmaDown(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.epState[addr].rdmaDown {
		c.epState[addr] = endpointState{rdmaDown: true, downAt: time.Now()}
	}
}

func (c *RingClient) clearRdmaDown(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.epState, addr)
}

// rdmaRetryInterval controls how long a failed RDMA endpoint stays on TCP
// before RDMA is retried (enterprise default 5m, IORPC_RDMA_RETRY_DURATION).
var rdmaRetryInterval = 5 * time.Minute

// unused guard: keep fmt import if logging formats change
var _ = fmt.Sprintf
var _ sync.Mutex
