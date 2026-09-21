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
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
)

var logger = utils.GetLogger("juicefs")

// NewUUID returns a random RFC-4122-shaped uuid string (member identity).
// Membership uuids are intentionally independent of meta session ids: they
// are fresh per mount process and never require meta internals.
func NewUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst, b[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], b[10:16])
	return string(dst)
}

// Config configures a cache-group Manager.
type Config struct {
	Groups        []string      // --cache-group values
	ListenAddr    string        // listen addr for the peer listener ("": ephemeral on all ifs)
	AdvertiseAddr string        // advertised host:port ("" : derived from listener)
	Weight        int           // --group-weight
	Heartbeat     time.Duration // membership refresh interval
	RPCTimeout    time.Duration // per-request peer timeout
	NoSharing     bool          // consumer-only: no listener, no registration
	FillOnUpload  bool          // push uploaded blocks to owners (best-effort)
}

func (m *Manager) self(addr string) Member {
	w := m.cfg.Weight
	if w <= 0 {
		w = DefaultWeight
	}
	return Member{UUID: m.uuid, Addr: addr, Weight: w, Version: "1.0", TS: time.Now().Unix()}
}

// Manager runs the cache-group membership loop, the peer listener, and hands
// out the current member view for placement decisions.
type Manager struct {
	cfg      Config
	registry Registry
	src      ServerSource // nil => consumer-only even if NoSharing is false
	uuid     string

	mu      sync.Mutex
	members map[string][]Member // group -> live members (self included)
	addr    string              // advertised listener addr ("" when NoSharing)
	kick    chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewManager creates a Manager. src may be nil for pure consumers (--no-sharing).
func NewManager(cfg Config, reg Registry, src ServerSource) *Manager {
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = DefaultHeartbeat
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = DefaultRemoteTimeout
	}
	return &Manager{
		cfg:      cfg,
		registry: reg,
		src:      src,
		uuid:     NewUUID(),
		members:  make(map[string][]Member),
		kick:     make(chan struct{}, 1),
	}
}

// SetSource attaches the ServerSource after construction (two-phase init:
// the decorator wrapping blob needs Manager.Members before the store that
// provides the ServerSource exists). Must be called before Start unless
// NoSharing. Not concurrency-safe with Start; call once.
func (m *Manager) SetSource(src ServerSource) {
	m.src = src
}

// Start launches the listener (unless NoSharing) and the membership loop.
// It blocks briefly to bind the listener, then runs in the background.
func (m *Manager) Start(ctx context.Context) error {
	ctx, m.cancel = context.WithCancel(ctx)
	if !m.cfg.NoSharing && m.src != nil {
		ln, err := net.Listen("tcp", m.cfg.ListenAddr)
		if err != nil {
			m.cancel()
			return err
		}
		addr := m.cfg.AdvertiseAddr
		if addr == "" {
			addr = ln.Addr().String()
		}
		m.mu.Lock()
		m.addr = addr
		m.mu.Unlock()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			<-ctx.Done()
			_ = ln.Close()
		}()
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			for {
				conn, err := ln.Accept()
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					logger.Debugf("gcache accept: %v", err)
					continue
				}
				m.wg.Add(1)
				go func() {
					defer m.wg.Done()
					defer conn.Close()
					ServeConn(ctx, conn, m.src, m.cfg.RPCTimeout)
				}()
			}
		}()
	}
	m.wg.Add(1)
	go m.heartbeatLoop(ctx)
	return nil
}

// Stop cancels all background work and waits for goroutines to exit.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

// Addr returns the advertised listener address (empty for consumer-only).
func (m *Manager) Addr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addr
}

// UUID returns this member's uuid.
func (m *Manager) UUID() string { return m.uuid }

// Members returns the cached live member list for a group (excludes self).
func (m *Manager) Members(group string) []Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	ms := m.members[group]
	out := make([]Member, 0, len(ms))
	for _, mem := range ms {
		if mem.UUID != m.uuid {
			out = append(out, mem)
		}
	}
	return out
}

// AllMembers returns the cached live member list INCLUDING self. Placement
// (ring ownership) MUST be computed over this full universe: every node
// deriving owners from the same set is what makes ownership globally
// consistent — exactly one owner per key. A node whose owner is itself
// serves locally; everyone else fetches from the owner.
func (m *Manager) AllMembers(group string) []Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	ms := m.members[group]
	out := make([]Member, len(ms))
	copy(out, ms)
	return out
}

// Kick requests an immediate membership refresh (non-blocking). Called by
// the client when it observes an empty/stale member view, so cold-start
// converges at first use instead of waiting for the next heartbeat.
func (m *Manager) Kick() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

func (m *Manager) heartbeatLoop(ctx context.Context) {
	defer m.wg.Done()
	m.beat(ctx) // register immediately
	// Convergence beats: other members may have registered just after our
	// initial list ran (simultaneous startup is the common case), so poll
	// briefly at heartbeat/3 until the member view is populated (max two
	// empty beats), then settle into the regular heartbeat cadence.
	// Membership is the only input to placement, so fast convergence
	// directly shortens cold-start.
	interval := m.cfg.Heartbeat / 3
	if interval <= 0 {
		interval = time.Second
	}
	settle := time.NewTicker(interval)
	defer settle.Stop()
	for settled := 0; settled < 3; {
		select {
		case <-ctx.Done():
			return
		case <-settle.C:
			m.beat(ctx)
			m.mu.Lock()
			n := len(m.members[m.cfg.Groups[0]])
			m.mu.Unlock()
			if n > 0 {
				settled = 3 // view populated: converge now
			} else {
				settled++
			}
		}
	}
	tick := time.NewTicker(m.cfg.Heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.beat(ctx)
		case <-m.kick:
			m.beat(ctx)
		}
	}
}

func (m *Manager) beat(ctx context.Context) {
	if m.registry == nil {
		return
	}
	addr := m.Addr()
	self := m.self(addr)
	stale := m.cfg.Heartbeat * DefaultStaleMultiplier
	cctx, cancel := context.WithTimeout(ctx, m.cfg.Heartbeat)
	defer cancel()
	for _, g := range m.cfg.Groups {
		if !m.cfg.NoSharing && m.src != nil {
			if err := m.registry.Register(cctx, g, self); err != nil {
				logger.Debugf("gcache register %s: %v", g, err)
			}
			_ = m.registry.Refresh(cctx, g, self)
		}
		ms, err := m.registry.List(cctx, g)
		if err != nil {
			logger.Debugf("gcache list %s: %v", g, err)
			continue
		}
		live := make([]Member, 0, len(ms))
		now := time.Now()
		for _, mem := range ms {
			if mem.Stale(stale, now) {
				continue
			}
			live = append(live, mem)
		}
		m.mu.Lock()
		m.members[g] = live
		m.mu.Unlock()
	}
}
