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

// Package gcache implements distributed (pooled) cache for JuiceFS mounts.
// Members sharing a --cache-group form a rendezvous-hash ring; on a local
// miss for a ring-owned block, a member fetches from the owning peer instead
// of object storage. See CONTRACT.md and rfcs/distributed-cache.md.
package gcache

import (
	"errors"
	"time"
)

// Member describes one cache-group member (a mount process serving cache).
// JSON-serialized into the registry (see CONTRACT.md §5).
type Member struct {
	UUID    string `json:"uuid"`    // member session uuid (registry key)
	Addr    string `json:"addr"`    // host:port for the peer RPC listener
	Weight  int    `json:"weight"`  // relative cache capacity (default 1)
	Version string `json:"version"` // juicefs version, informational
	TS      int64  `json:"ts"`      // unix seconds of last heartbeat
}

// Stale returns true when the member's heartbeat is older than maxAge.
func (m *Member) Stale(maxAge time.Duration, now time.Time) bool {
	return now.Sub(time.Unix(m.TS, 0)) > maxAge
}

// Sentinel errors.
var (
	// ErrNotPeerManaged is returned by the client-side decorator when a Get
	// must fall through to the underlying object storage.
	ErrNotPeerManaged = errors.New("key not managed by cache group")
	// ErrNoMembers is returned when the group has no members (or only self).
	ErrNoMembers = errors.New("cache group has no peers")
	// ErrEvicted is returned when a peer has been evicted after repeated
	// failures (enterprise-parity: 31 consecutive failures).
	ErrEvicted = errors.New("peer evicted after repeated failures")
)

// Wire protocol constants (CONTRACT.md §6).
// Frame layout (little-endian), fixed 20-byte header:
//
//	magic  3 bytes: 'G','C', protoVer
//	type   uint8:  MsgBlockReq / MsgBlockResp / MsgError
//	algo   uint8:  WireRaw / WireCompressed (0 in requests)
//	xid    uint32: pipelining id, echoed in responses
//	klen   uint16: block key length (0 in responses)
//	plen   uint64: payload length; MsgError: ^uint64(0) then klen+err string
const (
	magic0    = 'G'
	magic1    = 'C'
	protoVer  = 1
	headerLen = 20
)

// Message types.
const (
	MsgBlockReq  uint8 = 0x01
	MsgBlockResp uint8 = 0x02
	MsgError     uint8 = 0x03
)

// Payload format flags carried in the response header 'algo' byte.
// The wire ALWAYS carries volume-format bytes (what storage.Get would return):
// for compressed volumes the server re-compresses cached (decompressed)
// blocks on serve; the client decorator hands bytes to load() untouched.
// gcache never knows WHICH compressor is used — the bridge does the work.
const (
	WireRaw        uint8 = 0 // volume is uncompressed
	WireCompressed uint8 = 1 // payload is volume-format compressed bytes
)

// Default tuning (enterprise parity where applicable).
const (
	DefaultHeartbeat       = 10 * time.Second // membership heartbeat interval
	DefaultStaleMultiplier = 3                // TTL/staleness = 3x heartbeat
	DefaultRemoteTimeout   = 65 * time.Second // --remote-timeout (enterprise default)
	DefaultMaxFailures     = 31               // consecutive failures before eviction
	DefaultWeight          = 1
)
