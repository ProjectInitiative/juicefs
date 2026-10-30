/*
 * JuiceFS, Copyright 2026 ProjectInitiative, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gcache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// clientKick lets the client request a membership refresh (wired by the
// decorator when a Manager is available).
type kickFunc func()

// RingClient fetches blocks from cache-group peers over the CONTRACT.md §6
// wire protocol. Concurrency-safe. Failure handling is enterprise-parity
// (CONTRACT.md §7): one retry per request, maxFailures consecutive failures
// evict a peer from this consumer's view until a success un-evicts it.
type RingClient struct {
	timeout     time.Duration
	maxFailures int
	selfUUID    string
	kick        kickFunc // optional: requests membership refresh
	tcp         Transport
	rdma        Transport // nil => TCP-only
	strictRDMA  bool      // verification mode: no TCP fallback when RDMA configured
	epState     map[string]endpointState
	pool        *connPool

	mu      sync.Mutex
	failCnt map[string]int    // peer addr -> consecutive failures
	evicted map[string]time.Time
	rangeNo map[string]time.Time // peer addr -> range-unsupported until
}

// NewRingClient creates a client with per-request timeout and eviction
// threshold (enterprise default: 65s, 31). selfUUID identifies this node
// in the ring; kick, when set, requests an immediate membership refresh
// (used when the member view is empty at fetch time).
func NewRingClient(timeout time.Duration, maxFailures int, selfUUID string) *RingClient {
	if timeout <= 0 {
		timeout = DefaultRemoteTimeout
	}
	if maxFailures <= 0 {
		maxFailures = DefaultMaxFailures
	}
	return &RingClient{
		timeout:     timeout,
		maxFailures: maxFailures,
		selfUUID:    selfUUID,
		tcp:         &TCPTransport{Timeout: timeout},
		rdma:        nil,
		epState:     make(map[string]endpointState),
		pool:        newConnPool(0, 0),
		failCnt:     make(map[string]int),
		evicted:     make(map[string]time.Time),
		rangeNo:     make(map[string]time.Time),
	}
}

// isEvicted reports whether peer is currently evicted.
func (c *RingClient) isEvicted(peer string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.evicted[peer]
	return ok
}

// MarkSuccess clears the failure counter for peer; if it was evicted,
// un-evict it and log the add-back (enterprise log line).
func (c *RingClient) MarkSuccess(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.failCnt, peer)
	if t, ok := c.evicted[peer]; ok {
		delete(c.evicted, peer)
		logger.Infof("add peer %s back after %s", peer, time.Since(t))
	}
}

// MarkFailure increments the consecutive-failure counter for peer; at
// maxFailures the peer is evicted from this consumer's view and the removal
// is logged (enterprise log line).
func (c *RingClient) MarkFailure(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failCnt[peer]++
	if metricFailures != nil {
		metricFailures.WithLabelValues(peer).Inc()
	}
	if c.failCnt[peer] >= c.maxFailures && c.failCnt[peer]%c.maxFailures == 0 {
		if _, ok := c.evicted[peer]; !ok {
			c.evicted[peer] = time.Now()
			logger.Warnf("remove peer %s after %d failures in a row", peer, c.failCnt[peer])
		}
	}
}

// errFrame is returned when the peer answered with a MsgError frame.
type errFrame struct{ msg string }

func (e *errFrame) Error() string { return e.msg }

// errNoOwners: every candidate peer was evicted.
var errNoOwners = errors.New("no available peer for key")

// Ranged-read outcomes (fork extension). Callers fall back to the standard
// paths on any of these; errRangeUnsupported additionally memoizes the peer
// so later ranged reads skip it (re-probed after rangeReprobeInterval).
var (
	errSelfOwned        = errors.New("block owned by self")
	errRangeUnsupported = errors.New("peer does not support ranged reads")
)

// rangeReprobeInterval: how long a peer is assumed range-incapable after a
// rejection (covers rolling upgrades: upgraded peers are picked up again).
const rangeReprobeInterval = 5 * time.Minute

// rangeUnsupported reports whether peer rejected ranged reads recently.
func (c *RingClient) rangeUnsupported(peer string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	until, ok := c.rangeNo[peer]
	return ok && time.Now().Before(until)
}

func (c *RingClient) markRangeUnsupported(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rangeNo == nil {
		c.rangeNo = make(map[string]time.Time)
	}
	c.rangeNo[peer] = time.Now().Add(rangeReprobeInterval)
}

// SetKick wires the membership-refresh hook (called when the member view
// is empty or yields no owner for a key).
func (c *RingClient) SetKick(k func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kick = k
}

// SetRDMA attaches the RDMA transport (nil disables). When set, dials
// prefer the member's RdmaAddr with automatic TCP failover and periodic
// RDMA retry (enterprise 5.3.9 parity).
func (c *RingClient) SetRDMA(t Transport) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdma = t
}

// SetStrictRDMA disables TCP fallback while RDMA is configured: dials that
// cannot use RDMA fail instead of silently degrading. This is a verification
// mode for benchmarks and transport checks — production keeps fallback so
// reads stay serviceable (review finding A: a labeled "rsocket" result must
// not be able to hide TCP traffic).
func (c *RingClient) SetStrictRDMA(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.strictRDMA = v
}

// candidate returns the best non-evicted, non-self member for key, or nil.
func (c *RingClient) candidate(key string, members []Member, selfUUID string) *Member {
	owners := Owners(key, members, len(members))
	for i := range owners {
		if owners[i].UUID == selfUUID {
			continue // self-owned: caller serves locally, never fetch
		}
		if !c.isEvicted(owners[i].Addr) {
			m := owners[i]
			return &m
		}
	}
	return nil
}

// Fetch retrieves the block key from the ring. It picks the best
// non-evicted peer, sends a MsgBlockReq, and returns an io.ReadCloser over
// the payload (read-capped at plen). Transient failures are retried once
// (enterprise parity); the failed attempt marks the peer. group is
// currently informational (multi-group placement is a follow-up); members
// is the live member list for the group.
func (c *RingClient) Fetch(ctx context.Context, group, key string, members []Member) (io.ReadCloser, error) {
	if len(members) == 0 {
		if c.kick != nil {
			c.kick() // refresh membership before giving up
		}
		return nil, ErrNoMembers
	}
	r, err := c.fetchOnce(ctx, key, members)
	if err == nil {
		return r, nil
	}
	if err == errNoOwners && c.kick != nil {
		// member view may be stale (or predated a peer joining): refresh
		// and give the fetch one more chance via the retry below.
		c.kick()
	}
	// Caller cancelation: not a peer failure, not an error metric.
	if ctx.Err() != nil {
		return nil, err
	}
	// one retry on any transient error
	r, err2 := c.fetchOnce(ctx, key, members)
	if err2 == nil {
		return r, nil
	}
	return nil, err2
}

// fetchOnce performs one request against the best candidate peer. The
// connection comes from the per-peer pool (fresh dial on miss); it is
// returned to the pool only after the response is fully consumed (or drained)
// in payloadReader.Close — any error discards it.
func (c *RingClient) fetchOnce(ctx context.Context, key string, members []Member) (*payloadReader, error) {
	m := c.candidate(key, members, c.selfUUID)
	if m == nil {
		return nil, errNoOwners
	}
	peer := m.Addr
	start := time.Now()
	var fetched int64
	var reader *payloadReader
	var err error
	func() {
		pc, _, derr := c.pool.acquire(peer, func() (net.Conn, Transport, error) {
			return c.dialFailover(ctx, *m)
		})
		if derr != nil {
			err = fmt.Errorf("dial peer %s: %w", peer, derr)
			return
		}
		conn := pc.Conn
		if err = c.sendReq(ctx, conn, key); err != nil {
			c.pool.discard(conn)
			err = fmt.Errorf("send to peer %s: %w", peer, err)
			return
		}
		var algo uint8
		var xid uint32
		var plen uint64
		var isErr bool
		algo, xid, plen, isErr, err = c.readRespHeader(ctx, conn)
		if err != nil {
			c.pool.discard(conn)
			err = fmt.Errorf("read from peer %s: %w", peer, err)
			return
		}
		if isErr {
			c.pool.discard(conn) // error frame: frame sync is fine, but keep the simple path
			c.MarkSuccess(peer)  // peer answered; fetch-level failure, not peer failure
			return               // err already holds the *errFrame
		}
		c.MarkSuccess(peer) // header OK; payload streams below
		fetched = int64(plen)
		reader = &payloadReader{
			conn:      conn,
			remaining: int64(plen),
			plen:      int64(plen),
			algo:      algo,
			xid:       xid,
			timeout:   c.timeout,
			pool:      c.pool,
			peer:      peer,
			tr:        pc.tr,
		}
	}()
	observeFetch(peer, fetched, err, time.Since(start).Seconds())
	if err != nil {
		// Caller cancelation (speculative readahead torn down) is not a peer
		// health signal. net.Dialer maps context.Canceled to its own
		// "operation was canceled" sentinel which does NOT match
		// errors.Is(err, context.Canceled) — so check ctx directly.
		if ctx.Err() == nil {
			c.MarkFailure(peer)
		}
		return nil, err
	}
	return reader, nil
}

// sendReq writes one MsgBlockReq frame for key.
func (c *RingClient) sendReq(ctx context.Context, conn net.Conn, key string) error {
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = MsgBlockReq
	// hdr[4] algo = 0, hdr[11] reserved = 0
	binary.LittleEndian.PutUint32(hdr[5:9], 0) // xid (v1: one request per conn)
	binary.LittleEndian.PutUint16(hdr[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint64(hdr[12:20], 0) // plen
	if err := ctxSend(ctx, conn, hdr); err != nil {
		return err
	}
	return ctxSend(ctx, conn, []byte(key))
}

// readRespHeader reads and validates the response header. For MsgError it
// drains the error string and returns it as *errFrame with isErr=true.
func (c *RingClient) readRespHeader(ctx context.Context, conn net.Conn) (algo uint8, xid uint32, plen uint64, isErr bool, err error) {
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	hdr := make([]byte, headerLen)
	if err = ctxRead(ctx, conn, hdr); err != nil {
		return
	}
	if hdr[0] != magic0 || hdr[1] != magic1 || hdr[2] != protoVer {
		err = errors.New("invalid response header")
		return
	}
	typ := hdr[3]
	plen = binary.LittleEndian.Uint64(hdr[12:20])
	if typ == MsgError {
		if plen != ^uint64(0) {
			err = errors.New("malformed error frame")
			return
		}
		klen := int(binary.LittleEndian.Uint16(hdr[9:11]))
		msg := make([]byte, klen)
		if err = ctxRead(ctx, conn, msg); err != nil {
			return
		}
		isErr = true
		err = &errFrame{msg: string(msg)}
		return
	}
	if typ != MsgBlockResp {
		err = fmt.Errorf("unexpected response type 0x%x", typ)
		return
	}
	algo = hdr[4]
	xid = binary.LittleEndian.Uint32(hdr[5:9])
	return
}

// ctxSend writes b, aborting on ctx cancellation.
func ctxSend(ctx context.Context, conn net.Conn, b []byte) error {
	done := make(chan error, 1)
	go func() { done <- writeFull(conn, b) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = conn.SetDeadline(pastDeadline())
		<-done // let the writer observe the deadline and exit
		return ctx.Err()
	}
}

// ctxRead reads len(b), aborting on ctx cancellation.
func ctxRead(ctx context.Context, conn net.Conn, b []byte) error {
	done := make(chan error, 1)
	go func() { done <- readFull(conn, b) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = conn.SetDeadline(pastDeadline())
		<-done // let the reader observe the deadline and exit
		return ctx.Err()
	}
}

// pastDeadline returns a deadline guaranteed to be in the past.
func pastDeadline() time.Time { return time.Unix(0, 1) }

// writeFull/writeFull helpers: net.Conn Write/Read may short-read/write.
func writeFull(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func readFull(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Read(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// payloadReader streams the payload of one MsgBlockResp, read-capped at
// plen bytes, with a fresh per-Read deadline (payload transfer is not
// bounded by the header deadline). On Close the underlying connection is
// returned to the per-peer pool when the payload was fully consumed (or the
// small remainder drained); otherwise the conn is closed.
type payloadReader struct {
	conn      net.Conn
	remaining int64
	plen      int64 // expected total payload length (premature-EOF detection)
	algo      uint8
	xid       uint32
	timeout   time.Duration
	pool      *connPool // nil => close conn on Close (legacy/test path)
	peer      string
	tr        Transport
}

func (p *payloadReader) Read(b []byte) (int, error) {
	if p.remaining <= 0 {
		return 0, io.EOF
	}
	_ = p.conn.SetReadDeadline(time.Now().Add(p.timeout))
	if int64(len(b)) > p.remaining {
		b = b[:p.remaining]
	}
	n, err := p.conn.Read(b)
	p.remaining -= int64(n)
	return n, err
}

func (p *payloadReader) Close() error {
	// Best-effort drain of a small remainder so the peer sees a clean
	// request/response boundary; hard-close otherwise. A fully consumed (or
	// drained) conn is byte-clean for the next request and goes back to the
	// per-peer pool.
	const maxDrain = 1 << 20
	if p.remaining > 0 && p.remaining <= maxDrain {
		_ = p.conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = io.CopyN(io.Discard, p, p.remaining)
	}
	if p.remaining <= 0 && p.pool != nil {
		p.pool.release(p.peer, p.conn, p.tr)
		return nil
	}
	return p.conn.Close()
}

// NoLocalCache marks peer-fetched payloads: strict one-copy sharded
// ownership keeps them out of the local persistent block cache (the ring
// owner holds the copy). detected via a structural interface in pkg/chunk.
func (p *payloadReader) NoLocalCache() {}

// peerReader serves one peer-fetched block payload with mid-stream recovery:
// if the peer stream breaks before plen bytes arrive (peer timeout,
// eviction, socket reset), the block is re-read once from object storage and
// the already-consumed prefix is skipped, so a transient peer failure never
// surfaces as EIO to the read path.
type peerReader struct {
	cur      io.ReadCloser
	total    int64
	consumed int64
	refill   func(consumed int64) (io.ReadCloser, error)
	refilled bool
}

func (r *peerReader) NoLocalCache() {}

func (r *peerReader) Read(b []byte) (int, error) {
	for {
		if r.consumed >= r.total {
			return 0, io.EOF
		}
		n, err := r.cur.Read(b)
		r.consumed += int64(n)
		if n > 0 {
			return n, err
		}
		if err == nil {
			return 0, nil
		}
		if !errors.Is(err, io.EOF) {
			return n, err
		}
		// Stream ended before plen bytes: heal once via refill, otherwise
		// surface the truncation explicitly instead of a silent short read.
		if !r.refilled && r.refill != nil {
			r.refilled = true
			nr, rerr := r.refill(r.consumed)
			if rerr == nil {
				_ = r.cur.Close()
				r.cur = nr
				continue
			}
			return 0, rerr
		}
		return 0, io.ErrUnexpectedEOF
	}
}

func (r *peerReader) Close() error { return r.cur.Close() }

// noCacheReader marks a non-owner fallback read (straight from object
// storage): the block belongs to another ring member, so it must not enter
// the local persistent cache.
type noCacheReader struct{ io.ReadCloser }

func (noCacheReader) NoLocalCache() {}

// RangeRead fetches exactly [off, off+len(buf)) of a block's raw (decompressed)
// bytes from the ring owner via MsgRangeReq. It is the fast path for 4 KiB-
// class random reads: the owner serves positionally from its cache without
// transferring the whole block. Returned errors are final for this read — the
// caller falls back to the standard paths:
//   - errSelfOwned / errNoOwners: placement says serve locally / no peer
//   - errRangeUnsupported: the peer predates MsgRangeReq (memoized per peer
//     for rangeReprobeInterval, so rolling upgrades re-enable it)
//   - anything else: transport/protocol failure (peer health is updated)
func (c *RingClient) RangeRead(ctx context.Context, group, key string, off int64, buf []byte, members []Member) (int, error) {
	if len(members) == 0 {
		if c.kick != nil {
			c.kick()
		}
		return 0, ErrNoMembers
	}
	// Self-ownership FIRST: candidate() skips self, so checking ownership
	// only after it returns would route a locally-owned key to a non-owner
	// peer — that peer would then fill and persist a copy belonging here
	// (strict one-copy sharding violation, DEC-0050).
	if owners := Owners(key, members, 1); len(owners) > 0 && owners[0].UUID == c.selfUUID {
		return 0, errSelfOwned
	}
	m := c.candidate(key, members, c.selfUUID)
	if m == nil {
		// no non-evicted non-self member: the true owner is either evicted
		// or unreachable — the caller falls back to the standard paths
		// (identical placement to the full-block Fetch path).
		return 0, errNoOwners
	}
	peer := m.Addr
	if c.rangeUnsupported(peer) {
		return 0, errRangeUnsupported
	}
	start := time.Now()
	n, err := c.rangeOnce(ctx, m, key, off, buf)
	observeRange(peer, n, err, time.Since(start).Seconds())
	if err != nil {
		if ctx.Err() == nil {
			c.MarkFailure(peer)
		}
		return 0, err
	}
	c.MarkSuccess(peer)
	return n, nil
}

// rangeOnce performs one ranged request against peer member m.
func (c *RingClient) rangeOnce(ctx context.Context, m *Member, key string, off int64, buf []byte) (int, error) {
	peer := m.Addr
	pc, _, err := c.pool.acquire(peer, func() (net.Conn, Transport, error) {
		return c.dialFailover(ctx, *m)
	})
	if err != nil {
		return 0, fmt.Errorf("dial peer %s: %w", peer, err)
	}
	conn := pc.Conn
	if err = c.sendRangeReq(ctx, conn, key, off, len(buf)); err != nil {
		c.pool.discard(conn)
		return 0, fmt.Errorf("send to peer %s: %w", peer, err)
	}
	algo, _, plen, isErr, err := c.readRespHeader(ctx, conn)
	if err != nil {
		c.pool.discard(conn)
		if isErr {
			if e, ok := err.(*errFrame); ok && strings.Contains(e.msg, "bad request type") {
				// legacy peer: it rejects the unknown type and closes the conn
				c.markRangeUnsupported(peer)
				return 0, errRangeUnsupported
			}
		}
		return 0, fmt.Errorf("read from peer %s: %w", peer, err)
	}
	if algo != WireRaw {
		c.pool.discard(conn)
		return 0, fmt.Errorf("unexpected range payload algo %d from %s", algo, peer)
	}
	if plen != uint64(len(buf)) {
		// protocol violation (short block or corrupt peer): do not guess
		c.pool.discard(conn)
		return 0, fmt.Errorf("range payload length mismatch: got %d want %d", plen, len(buf))
	}
	pr := &payloadReader{
		conn:      conn,
		remaining: int64(plen),
		plen:      int64(plen),
		algo:      algo,
		timeout:   c.timeout,
		pool:      c.pool,
		peer:      peer,
		tr:        pc.tr,
	}
	if _, err = io.ReadFull(pr, buf); err != nil {
		pr.Close() // drains/releases when clean, closes otherwise
		return 0, fmt.Errorf("read range payload from %s: %w", peer, err)
	}
	pr.Close() // fully consumed: release to pool
	return len(buf), nil
}

// sendRangeReq writes one MsgRangeReq frame: header + key + off/len pair.
func (c *RingClient) sendRangeReq(ctx context.Context, conn net.Conn, key string, off int64, n int) error {
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = MsgRangeReq
	binary.LittleEndian.PutUint32(hdr[5:9], 0)  // xid (single-flight per conn)
	binary.LittleEndian.PutUint16(hdr[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint64(hdr[12:20], 16) // trailing off/len pair
	if err := ctxSend(ctx, conn, hdr); err != nil {
		return err
	}
	if err := ctxSend(ctx, conn, []byte(key)); err != nil {
		return err
	}
	var pair [16]byte
	binary.LittleEndian.PutUint64(pair[0:8], uint64(off))
	binary.LittleEndian.PutUint64(pair[8:16], uint64(n))
	return ctxSend(ctx, conn, pair[:])
}
