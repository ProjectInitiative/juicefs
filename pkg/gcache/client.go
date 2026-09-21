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
	"sync"
	"time"
)

// RingClient fetches blocks from cache-group peers over the CONTRACT.md §6
// wire protocol. Concurrency-safe. Failure handling is enterprise-parity
// (CONTRACT.md §7): one retry per request, maxFailures consecutive failures
// evict a peer from this consumer's view until a success un-evicts it.
type RingClient struct {
	timeout     time.Duration
	maxFailures int

	mu      sync.Mutex
	failCnt map[string]int // peer addr -> consecutive failures
	evicted map[string]time.Time
}

// NewRingClient creates a client with per-request timeout and eviction
// threshold (enterprise default: 65s, 31).
func NewRingClient(timeout time.Duration, maxFailures int) *RingClient {
	if timeout <= 0 {
		timeout = DefaultRemoteTimeout
	}
	if maxFailures <= 0 {
		maxFailures = DefaultMaxFailures
	}
	return &RingClient{
		timeout:     timeout,
		maxFailures: maxFailures,
		failCnt:     make(map[string]int),
		evicted:     make(map[string]time.Time),
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

// candidate returns the best non-evicted member for key, or nil.
func (c *RingClient) candidate(key string, members []Member) *Member {
	owners := Owners(key, members, len(members))
	for i := range owners {
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
		return nil, ErrNoMembers
	}
	r, err := c.fetchOnce(ctx, key, members)
	if err == nil {
		return r, nil
	}
	// one retry on any transient error
	r, err2 := c.fetchOnce(ctx, key, members)
	if err2 == nil {
		return r, nil
	}
	return nil, err2
}

// fetchOnce performs one request against the best candidate peer.
func (c *RingClient) fetchOnce(ctx context.Context, key string, members []Member) (io.ReadCloser, error) {
	m := c.candidate(key, members)
	if m == nil {
		return nil, errNoOwners
	}
	peer := m.Addr

	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", peer)
	if err != nil {
		c.MarkFailure(peer)
		return nil, fmt.Errorf("dial peer %s: %w", peer, err)
	}
	if err := c.sendReq(ctx, conn, key); err != nil {
		_ = conn.Close()
		c.MarkFailure(peer)
		return nil, fmt.Errorf("send to peer %s: %w", peer, err)
	}
	algo, xid, plen, isErr, rerr := c.readRespHeader(ctx, conn)
	if rerr != nil {
		_ = conn.Close()
		c.MarkFailure(peer)
		return nil, fmt.Errorf("read from peer %s: %w", peer, rerr)
	}
	if isErr {
		_ = conn.Close()
		c.MarkSuccess(peer) // peer answered; fetch-level failure, not peer failure
		return nil, rerr    // *errFrame from readRespHeader
	}
	c.MarkSuccess(peer) // header OK; payload streams below
	return &payloadReader{
		conn:      conn,
		remaining: int64(plen),
		algo:      algo,
		xid:       xid,
		timeout:   c.timeout,
	}, nil
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
// bounded by the header deadline).
type payloadReader struct {
	conn      net.Conn
	remaining int64
	algo      uint8
	xid       uint32
	timeout   time.Duration
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
	// connection close; hard-close otherwise.
	const maxDrain = 1 << 20
	if p.remaining > 0 && p.remaining <= maxDrain {
		_ = p.conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = io.CopyN(io.Discard, p, p.remaining)
	}
	return p.conn.Close()
}
