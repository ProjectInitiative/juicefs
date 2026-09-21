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

package meta

// gcache_registry.go — fork addition: cache-group membership registry backed
// by the meta engine, so the registry inherits the meta engine's HA (see
// pkg/gcache/CONTRACT.md §5). v1 supports Redis-family meta engines; tkv and
// SQL adapters are follow-up work (the gcache.Registry interface already
// accommodates them).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/juicedata/juicefs/pkg/gcache"
	"github.com/redis/go-redis/v9"
)

// GcacheRegistry implements gcache.Registry over a Redis meta engine.
// Layout per group: HASH <prefix>gcache/<group> with field = member uuid,
// value = JSON-encoded gcache.Member. The whole hash carries a TTL that is
// refreshed on every Register/Refresh; stale members (ts older than
// 3x heartbeat) are pruned at List time as a backstop.
type GcacheRegistry struct {
	rdb    redis.UniversalClient
	prefix string
	ttl    time.Duration
}

// NewGcacheRegistryFromMeta builds a registry from an open meta client.
// Returns an error for non-Redis engines (v1 limitation; tkv/SQL follow-up).
func NewGcacheRegistryFromMeta(m Meta) (*GcacheRegistry, error) {
	rm, ok := m.(*redisMeta)
	if !ok {
		return nil, fmt.Errorf("gcache registry: unsupported meta engine %T (redis only in v1)", m)
	}
	return &GcacheRegistry{
		rdb:    rm.rdb,
		prefix: rm.prefix + "gcache/",
		ttl:    gcache.DefaultHeartbeat * gcache.DefaultStaleMultiplier,
	}, nil
}

func (r *GcacheRegistry) key(group string) string {
	return r.prefix + group
}

// Register writes (or overwrites) this member's record and refreshes TTL.
func (r *GcacheRegistry) Register(ctx context.Context, group string, self gcache.Member) error {
	b, err := json.Marshal(self)
	if err != nil {
		return err
	}
	key := r.key(group)
	pipe := r.rdb.Pipeline()
	pipe.HSet(ctx, key, self.UUID, b)
	pipe.Expire(ctx, key, r.ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// Refresh rewrites the heartbeat (fresh ts) and extends the TTL.
func (r *GcacheRegistry) Refresh(ctx context.Context, group string, self gcache.Member) error {
	return r.Register(ctx, group, self)
}

// List returns live members of the group, pruning stale heartbeats.
func (r *GcacheRegistry) List(ctx context.Context, group string) ([]gcache.Member, error) {
	vals, err := r.rdb.HGetAll(ctx, r.key(group)).Result()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	staleAfter := r.ttl
	members := make([]gcache.Member, 0, len(vals))
	for _, v := range vals {
		var m gcache.Member
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			continue // corrupt record: skip, TTL cleans it up
		}
		if m.Stale(staleAfter, now) {
			continue
		}
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].UUID < members[j].UUID })
	return members, nil
}

// Close releases nothing: the connection is owned by the meta client.
func (r *GcacheRegistry) Close() {}

// compile-time interface check
var _ gcache.Registry = (*GcacheRegistry)(nil)
