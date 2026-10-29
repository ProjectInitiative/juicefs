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
	"net"
	"sync"
	"time"
)

// poolIdleTTL retires idle connections before the server's 65s frame deadline
// (DefaultRemoteTimeout), so reuse never races a server-side timeout.
const poolIdleTTL = 30 * time.Second

// poolMaxIdlePerPeer bounds idle inventory per peer; conns beyond the cap are
// closed on release. Open conns are bounded by request concurrency.
const poolMaxIdlePerPeer = 16

// connPool reuses peer connections across requests. Rationale: a 4 KiB
// random-read workload pays a full TCP handshake per request when dialing per
// fetch; at ~70 concurrent reads per decoding round that is 70 handshakes per
// round. The wire protocol is already request/response per connection —
// ServeConn loops over frames — so a connection that has fully consumed one
// response is byte-clean for the next request. One request is in flight per
// pooled connection at a time (xid stays 0; no pipelining), which keeps
// ordering trivial and the protocol unchanged.
//
// Lifecycle rules:
//   - acquire: pop the newest idle conn for the peer if it is younger than
//     the idle TTL, else dial fresh (and count it).
//   - release: on clean request completion (response fully consumed or
//     drained); refused when the peer's idle list is at the cap.
//   - any error: discard — a conn that observed an error is never reused
//     (frame sync cannot be trusted after a partial write/read).
//
// Transport-agnostic: works identically for TCP and rsockets/verbs conns
// (both are net.Conn).
type connPool struct {
	mu      sync.Mutex
	idle    map[string][]*poolConn
	maxIdle int
	ttl     time.Duration

	dials  int // fresh dials performed
	reuses int // conns handed out from the idle list
}

// poolConn is a pooled connection with its transport (for logging/metrics).
type poolConn struct {
	net.Conn
	tr         Transport
	releasedAt time.Time
}

func newConnPool(maxIdle int, ttl time.Duration) *connPool {
	if maxIdle <= 0 {
		maxIdle = poolMaxIdlePerPeer
	}
	if ttl <= 0 {
		ttl = poolIdleTTL
	}
	return &connPool{idle: make(map[string][]*poolConn), maxIdle: maxIdle, ttl: ttl}
}

// acquire returns a connection for peer, reusing an idle one when fresh.
// reused reports whether the conn came from the idle list. When peer is "",
// no reuse happens (dial-only path).
func (p *connPool) acquire(peer string, dial func() (net.Conn, Transport, error)) (*poolConn, bool, error) {
	if peer != "" {
		p.mu.Lock()
		for {
			stack := p.idle[peer]
			if len(stack) == 0 {
				break
			}
			pc := stack[len(stack)-1]
			p.idle[peer] = stack[:len(stack)-1]
			if time.Since(pc.releasedAt) > p.ttl {
				_ = pc.Conn.Close() // stale: retire and keep popping
				continue
			}
			p.reuses++
			p.mu.Unlock()
			return pc, true, nil
		}
		p.mu.Unlock()
	}
	conn, tr, err := dial()
	if err != nil {
		return nil, false, err
	}
	p.mu.Lock()
	p.dials++
	p.mu.Unlock()
	return &poolConn{Conn: conn, tr: tr}, false, nil
}

// release returns a healthy connection to the peer's idle list (or closes it
// when the list is full or peer is unknown).
func (p *connPool) release(peer string, conn net.Conn, tr Transport) {
	p.mu.Lock()
	if peer == "" || len(p.idle[peer]) >= p.maxIdle {
		p.mu.Unlock()
		_ = conn.Close()
		return
	}
	p.idle[peer] = append(p.idle[peer], &poolConn{Conn: conn, tr: tr, releasedAt: time.Now()})
	p.mu.Unlock()
}

// discard closes a connection unconditionally (after any error).
func (p *connPool) discard(conn net.Conn) { _ = conn.Close() }

// stats exposes dial/reuse counters and the idle inventory (tests/metrics).
func (p *connPool) stats() (dials, reuses, idle int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, stack := range p.idle {
		idle += len(stack)
	}
	return p.dials, p.reuses, idle
}
