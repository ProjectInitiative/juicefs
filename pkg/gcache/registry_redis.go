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
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisRegistry implements Registry on top of a Redis meta engine
// (CONTRACT.md §5): per-group HASH `<prefix>gcache/<group>` with one field
// per member uuid, refreshed with an EXPIRE each heartbeat. The registry
// does NOT own the client (it is the meta engine's connection) — Close is
// a no-op; membership lapses via TTL + ts-staleness.
type redisRegistry struct {
	client *redis.Client
	prefix string
}

// NewRedisRegistry wraps an existing go-redis client (the meta engine's).
// prefix is the JuiceFS meta prefix for the volume (e.g. from meta engine
// setup); keys are `<prefix>gcache/<group>`.
func NewRedisRegistry(client *redis.Client, prefix string) Registry {
	return &redisRegistry{client: client, prefix: prefix}
}

func (r *redisRegistry) key(group string) string {
	return r.prefix + "gcache/" + group
}

// Register writes the member record with fresh TS and a TTL of
// DefaultHeartbeat*DefaultStaleMultiplier (enterprise: 10s heartbeat, 90s TTL).
func (r *redisRegistry) Register(ctx context.Context, group string, self Member) error {
	self.TS = time.Now().Unix()
	b, err := json.Marshal(self)
	if err != nil {
		return err
	}
	ttl := DefaultHeartbeat * DefaultStaleMultiplier
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.key(group), self.UUID, b)
	pipe.Expire(ctx, r.key(group), ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// Refresh rewrites the heartbeat (fresh TS) and slides the group TTL.
func (r *redisRegistry) Refresh(ctx context.Context, group string, self Member) error {
	self.TS = time.Now().Unix()
	b, err := json.Marshal(self)
	if err != nil {
		return err
	}
	ttl := DefaultHeartbeat * DefaultStaleMultiplier
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.key(group), self.UUID, b)
	pipe.Expire(ctx, r.key(group), ttl)
	_, err = pipe.Exec(ctx)
	return err
}

// List returns live members of the group: HGETALL, unmarshal, then prune
// members whose TS is older than the staleness window (backstop for clocks
// and for members that died before their last EXPIRE refresh).
func (r *redisRegistry) List(ctx context.Context, group string) ([]Member, error) {
	vals, err := r.client.HGetAll(ctx, r.key(group)).Result()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	stale := DefaultHeartbeat * DefaultStaleMultiplier
	out := make([]Member, 0, len(vals))
	for uuid, raw := range vals {
		var m Member
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			continue // corrupt record: skip
		}
		if m.UUID == "" {
			m.UUID = uuid
		}
		if m.Stale(stale, now) {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// Close is a no-op: the redis client belongs to the meta engine.
func (r *redisRegistry) Close() {}
