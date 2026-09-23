package gcache

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeTransport counts dials and can fail on demand. Success returns an
// in-memory net.Pipe conn so tests never touch real TCP.
type fakeTransport struct {
	mu      sync.Mutex
	dials   int
	failAll bool
	failN   int // fail the first N dials
	name    string
}

func (f *fakeTransport) Dial(_ context.Context, _ string) (net.Conn, error) {
	f.mu.Lock()
	f.dials++
	n := f.dials
	fail := f.failAll || (f.failN > 0 && n <= f.failN)
	f.mu.Unlock()
	if fail {
		return nil, errors.New("fake transport failure")
	}
	// in-memory conn pair via net.Pipe; server side is dropped (dials are
	// only counted in these tests)
	c1, _ := net.Pipe()
	return c1, nil
}

func (f *fakeTransport) Name() string { return f.name }

func TestDialFailoverPrefersRDMA(t *testing.T) {
	rc := NewRingClient(time.Second, 31, "self")
	rt := &fakeTransport{name: "rsocket"}
	rc.SetRDMA(rt)
	m := Member{UUID: "p", Addr: "10.0.0.1:3910", RdmaAddr: "10.0.0.1:3911"}
	conn, tr, err := rc.dialFailover(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if tr.Name() != "rsocket" {
		t.Fatalf("want rsocket, got %s", tr.Name())
	}
	rt.mu.Lock()
	dials := rt.dials
	rt.mu.Unlock()
	if dials != 1 {
		t.Fatalf("rdma dials = %d, want 1", dials)
	}
}

// RDMA failure falls through to TCP and starts the backoff: the next dials
// go TCP-only until the retry interval elapses.
func TestDialFailoverBackoff(t *testing.T) {
	// both transports succeed after configured failures; no real network
	rc := NewRingClient(time.Second, 31, "self")
	rt := &fakeTransport{name: "rsocket", failAll: true}
	tt := &fakeTransport{name: "tcp"}
	if t2, ok := rc.tcp.(*TCPTransport); ok {
		_ = t2
	}
	rc.SetRDMA(rt)
	// replace the TCP transport with a fake that always succeeds
	tcpFake := tt
	rc.mu.Lock()
	rc.tcp = tcpFake
	rc.mu.Unlock()
	m := Member{UUID: "p", Addr: "10.0.0.1:3910", RdmaAddr: "10.0.0.1:3911"}

	conn, tr, err := rc.dialFailover(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if tr.Name() != "tcp" {
		t.Fatalf("after rdma failure want tcp, got %s", tr.Name())
	}
	rt.mu.Lock()
	d1 := rt.dials
	rt.mu.Unlock()
	if d1 != 1 {
		t.Fatalf("rdma dials = %d, want 1", d1)
	}

	// during backoff: no further RDMA attempts
	for i := 0; i < 3; i++ {
		conn, tr, err = rc.dialFailover(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if tr.Name() != "tcp" {
			t.Fatalf("dial %d during backoff must use tcp, got %s", i+2, tr.Name())
		}
	}
	rt.mu.Lock()
	d2 := rt.dials
	rt.mu.Unlock()
	if d2 != 1 {
		t.Fatalf("rdma dials during backoff = %d, want 1 (no retry)", d2)
	}

	// shrink the retry interval and confirm retry happens
	old := rdmaRetryInterval
	rdmaRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { rdmaRetryInterval = old })
	time.Sleep(20 * time.Millisecond)
	conn, tr, err = rc.dialFailover(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	rt.mu.Lock()
	d3 := rt.dials
	rt.mu.Unlock()
	if d3 != 2 {
		t.Fatalf("rdma dials after backoff = %d, want 2", d3)
	}
}

// RDMA success clears the backoff (failback path).
func TestDialFailoverFailback(t *testing.T) {
	rc := NewRingClient(time.Second, 31, "self")
	rt := &fakeTransport{name: "rsocket", failN: 1} // fail once, then succeed
	rc.SetRDMA(rt)
	rc.mu.Lock()
	rc.tcp = &fakeTransport{name: "tcp"}
	rc.mu.Unlock()
	m := Member{UUID: "p", Addr: "10.0.0.1:3910", RdmaAddr: "10.0.0.1:3911"}

	// first dial: rdma fails, tcp used, backoff set
	_, tr, err := rc.dialFailover(context.Background(), m)
	if err != nil || tr.Name() != "tcp" {
		t.Fatalf("first dial: %v %s", err, tr.Name())
	}
	// shrink backoff so the second dial retries RDMA
	old := rdmaRetryInterval
	rdmaRetryInterval = time.Millisecond
	t.Cleanup(func() { rdmaRetryInterval = old })
	time.Sleep(5 * time.Millisecond)

	_, tr, err = rc.dialFailover(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Name() != "rsocket" {
		t.Fatalf("failback want rsocket, got %s", tr.Name())
	}
}

// TCP-only members never touch the RDMA transport.
func TestDialFailoverTCPOnlyMember(t *testing.T) {
	rc := NewRingClient(time.Second, 31, "self")
	rt := &fakeTransport{name: "rsocket"}
	rc.SetRDMA(rt)
	rc.mu.Lock()
	rc.tcp = &fakeTransport{name: "tcp"}
	rc.mu.Unlock()
	m := Member{UUID: "p", Addr: "10.0.0.1:3910"} // no RdmaAddr
	for i := 0; i < 2; i++ {
		_, tr, err := rc.dialFailover(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		if tr.Name() != "tcp" {
			t.Fatalf("tcp-only member: want tcp, got %s", tr.Name())
		}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.dials != 0 {
		t.Fatalf("rdma dials = %d, want 0", rt.dials)
	}
}
