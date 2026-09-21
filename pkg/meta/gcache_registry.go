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
// pkg/gcache/CONTRACT.md §5). Supported engines: Redis-family (HASH records)
// and SQL-family pgx/mysql/sqlite3 via dbMeta (gcache_members table); tkv is
// follow-up work (the gcache.Registry interface already accommodates it).

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/gcache"
	"github.com/redis/go-redis/v9"
	"xorm.io/xorm"
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
// Dispatches on engine family: redisMeta (HASH records) or dbMeta (SQL:
// pgx/mysql/sqlite3). tkv engines are not supported yet.
func NewGcacheRegistryFromMeta(m Meta) (gcache.Registry, error) {
	switch rm := m.(type) {
	case *redisMeta:
		return &GcacheRegistry{
			rdb:    rm.rdb,
			prefix: rm.prefix + "gcache/",
			ttl:    gcache.DefaultHeartbeat * gcache.DefaultStaleMultiplier,
		}, nil
	case *dbMeta:
		return newGcacheSQLRegistry(rm)
	default:
		return nil, fmt.Errorf("gcache registry: unsupported meta engine %T (redis and sql supported; tkv follow-up)", m)
	}
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

// ---- SQL backend (dbMeta: pgx / mysql / sqlite3) ----

// gcacheMembers is one cache-group membership heartbeat row. The struct name
// is deliberately plural: the engine's table mapper is prefixMapper{SnakeMapper,
// tablePrefix} (sql.go newSQLMeta), so Obj2Table("gcacheMembers") ->
// "gcache_members" -> table "<tablePrefix>gcache_members", matching the raw
// upsert SQL below. Do NOT add a TableName() method — it bypasses the prefix
// mapper (names.GetTableName checks the interface first) and would break
// multiple volumes sharing one database with different table prefixes.
// Columns carry explicit quoted names because SnakeMapper would expand UUID
// to "u_u_i_d"; group_name (not "group") avoids a reserved word in
// postgres/mysql.
type gcacheMembers struct {
	GroupName string `xorm:"pk 'group_name' varchar(255) notnull"`
	UUID      string `xorm:"pk 'uuid' varchar(64) notnull"`
	Addr      string `xorm:"'addr' varchar(255) notnull"`
	Weight    int    `xorm:"'weight' notnull"`
	Version   string `xorm:"'version' varchar(64) notnull"`
	TS        int64  `xorm:"'ts' notnull"` // unix seconds of last heartbeat
}

// upsertStmt is the driver-specific upsert text; placeholders are always '?'
// (xorm converts for pgx at exec time, as in dbMeta.initStatement).
type upsertStmt struct {
	pg    string // pgx, sqlite3
	mysql string
}

var gcacheUpsert = upsertStmt{
	pg:    "INSERT INTO %sgcache_members (group_name, uuid, addr, weight, version, ts) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (group_name, uuid) DO UPDATE SET addr = ?, weight = ?, version = ?, ts = ?",
	mysql: "INSERT INTO %sgcache_members (group_name, uuid, addr, weight, version, ts) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE addr = ?, weight = ?, version = ?, ts = ?",
}

// GcacheSQLRegistry implements gcache.Registry over a dbMeta engine
// (pgx / mysql / sqlite3). Layout: table <tablePrefix>gcache_members,
// PK (group_name, uuid). No TTL machinery: staleness is ts-based, pruned at
// List time (and by a best-effort global DELETE of dead rows).
type GcacheSQLRegistry struct {
	db          *xorm.Engine
	driver      string
	tablePrefix string
	ttl         time.Duration

	once    sync.Once
	initErr error // table-creation error, reported by the first operation
}

// gcacheTable is the raw-SQL table name; must stay in sync with the mapper
// derivation of the gcacheMembers struct (see struct comment).
func (r *GcacheSQLRegistry) gcacheTable() string {
	return r.tablePrefix + "gcache_members"
}

// newGcacheSQLRegistry builds the SQL registry from an open dbMeta. The table
// is created lazily on first use (we must not run DDL inside meta startup).
func newGcacheSQLRegistry(m *dbMeta) (*GcacheSQLRegistry, error) {
	if m == nil || m.db == nil {
		return nil, fmt.Errorf("gcache sql registry: nil dbMeta engine")
	}
	return &GcacheSQLRegistry{
		db:          m.db,
		driver:      m.db.DriverName(),
		tablePrefix: m.tablePrefix,
		ttl:         gcache.DefaultHeartbeat * gcache.DefaultStaleMultiplier,
	}, nil
}

// ensureTable creates <prefix>gcache_members on first use. Follows
// dbMeta.syncTable's tolerance of duplicate-table noise on concurrent creates.
func (r *GcacheSQLRegistry) ensureTable() error {
	r.once.Do(func() {
		err := r.db.Sync2(new(gcacheMembers))
		if err != nil && strings.Contains(err.Error(), "Duplicate") {
			err = nil
		}
		r.initErr = err
	})
	return r.initErr
}

// upsert inserts or updates this member's heartbeat row.
func (r *GcacheSQLRegistry) upsert(ctx context.Context, group string, self gcache.Member) error {
	if err := r.ensureTable(); err != nil {
		return err
	}
	stmt := gcacheUpsert.pg
	if r.driver == "mysql" {
		stmt = gcacheUpsert.mysql
	}
	stmt = fmt.Sprintf(stmt, r.tablePrefix)
	_, err := r.db.Context(ctx).
		Exec(stmt,
			group, self.UUID, self.Addr, self.Weight, self.Version, self.TS,
			self.Addr, self.Weight, self.Version, self.TS)
	return err
}

// Register upserts this member's record (fresh ts) — the SQL analogue of the
// Redis HSet+Expire pipeline.
func (r *GcacheSQLRegistry) Register(ctx context.Context, group string, self gcache.Member) error {
	return r.upsert(ctx, group, self)
}

// Refresh rewrites the heartbeat (fresh ts).
func (r *GcacheSQLRegistry) Refresh(ctx context.Context, group string, self gcache.Member) error {
	return r.upsert(ctx, group, self)
}

// List returns the group's live members ordered by uuid, pruning stale
// heartbeats and deleting dead rows (any group) as a cheap backstop.
func (r *GcacheSQLRegistry) List(ctx context.Context, group string) ([]gcache.Member, error) {
	if err := r.ensureTable(); err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-r.ttl).Unix()
	var rows []gcacheMembers
	err := r.db.Context(ctx).
		Where("group_name = ? AND ts > ?", group, cutoff).
		OrderBy("uuid").
		Find(&rows)
	if err != nil {
		return nil, err
	}
	// best-effort stale-row deletion (any group); failures are non-fatal
	_, _ = r.db.Context(ctx).
		Where("ts <= ?", cutoff).
		Delete(new(gcacheMembers))
	members := make([]gcache.Member, 0, len(rows))
	for _, row := range rows {
		members = append(members, gcache.Member{
			UUID:    row.UUID,
			Addr:    row.Addr,
			Weight:  row.Weight,
			Version: row.Version,
			TS:      row.TS,
		})
	}
	return members, nil
}

// Close releases nothing: the engine is owned by the meta client.
func (r *GcacheSQLRegistry) Close() {}

// compile-time interface check
var _ gcache.Registry = (*GcacheSQLRegistry)(nil)
