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

// Tests for the SQL gcache registry backend (dbMeta: sqlite3 in tests;
// pgx/mysql share the same statements — see gcacheUpsert in
// gcache_registry.go). Uses newSQLMeta so the engine carries the real
// prefixMapper, exercising the same table-naming path as production.

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/gcache"
)

func newTestSQLRegistry(t *testing.T) (*GcacheSQLRegistry, *dbMeta) {
	t.Helper()
	raw, err := newSQLMeta("sqlite3", path.Join(t.TempDir(), "gcache-registry.db"), testConfig())
	if err != nil {
		t.Fatalf("newSQLMeta: %s", err)
	}
	t.Cleanup(func() { _ = raw.Shutdown() })
	m, ok := raw.(*dbMeta)
	if !ok {
		t.Fatalf("expected *dbMeta, got %T", raw)
	}
	reg, err := newGcacheSQLRegistry(m)
	if err != nil {
		t.Fatalf("newGcacheSQLRegistry: %s", err)
	}
	return reg, m
}

func TestGcacheSQLRegistryRegisterList(t *testing.T) {
	reg, _ := newTestSQLRegistry(t)
	ctx := context.Background()

	now := time.Now().Unix()
	a := gcache.Member{UUID: "aaa", Addr: "10.0.0.1:1000", Weight: 1, Version: "v1", TS: now}
	b := gcache.Member{UUID: "bbb", Addr: "10.0.0.2:1000", Weight: 2, Version: "v1", TS: now}
	other := gcache.Member{UUID: "ccc", Addr: "10.0.0.3:1000", Weight: 1, Version: "v1", TS: now}

	if err := reg.Register(ctx, "g1", a); err != nil {
		t.Fatalf("register a: %s", err)
	}
	if err := reg.Register(ctx, "g1", b); err != nil {
		t.Fatalf("register b: %s", err)
	}
	if err := reg.Register(ctx, "g2", other); err != nil {
		t.Fatalf("register other: %s", err)
	}

	ms, err := reg.List(ctx, "g1")
	if err != nil {
		t.Fatalf("list g1: %s", err)
	}
	if len(ms) != 2 {
		t.Fatalf("expected 2 members in g1, got %d: %+v", len(ms), ms)
	}
	if ms[0].UUID != "aaa" || ms[1].UUID != "bbb" {
		t.Fatalf("members not ordered by uuid: %+v", ms)
	}
	if ms[0].Addr != "10.0.0.1:1000" || ms[1].Weight != 2 {
		t.Fatalf("member fields lost: %+v", ms)
	}

	ms, err = reg.List(ctx, "g2")
	if err != nil {
		t.Fatalf("list g2: %s", err)
	}
	if len(ms) != 1 || ms[0].UUID != "ccc" {
		t.Fatalf("group isolation broken: %+v", ms)
	}
}

func TestGcacheSQLRegistryUpsertOverwrites(t *testing.T) {
	reg, _ := newTestSQLRegistry(t)
	ctx := context.Background()

	m1 := gcache.Member{UUID: "u1", Addr: "1.1.1.1:1", Weight: 1, Version: "v1", TS: time.Now().Unix()}
	if err := reg.Register(ctx, "g", m1); err != nil {
		t.Fatalf("register 1: %s", err)
	}
	m2 := gcache.Member{UUID: "u1", Addr: "2.2.2.2:2", Weight: 5, Version: "v2", TS: time.Now().Unix() + 1}
	if err := reg.Register(ctx, "g", m2); err != nil {
		t.Fatalf("register 2: %s", err)
	}
	ms, err := reg.List(ctx, "g")
	if err != nil {
		t.Fatalf("list: %s", err)
	}
	if len(ms) != 1 {
		t.Fatalf("upsert duplicated rows: %+v", ms)
	}
	if ms[0].Addr != "2.2.2.2:2" || ms[0].Weight != 5 || ms[0].Version != "v2" || ms[0].TS != time.Now().Unix()+1 {
		t.Fatalf("upsert did not overwrite: %+v", ms[0])
	}
}

func TestGcacheSQLRegistryRefreshUpdatesTS(t *testing.T) {
	reg, _ := newTestSQLRegistry(t)
	ctx := context.Background()

	if err := reg.Register(ctx, "g", gcache.Member{UUID: "u1", Addr: "a:1", TS: 1}); err != nil {
		t.Fatalf("register: %s", err)
	}
	fresh := time.Now().Unix()
	if err := reg.Refresh(ctx, "g", gcache.Member{UUID: "u1", Addr: "a:1", TS: fresh}); err != nil {
		t.Fatalf("refresh: %s", err)
	}
	ms, err := reg.List(ctx, "g")
	if err != nil {
		t.Fatalf("list: %s", err)
	}
	if len(ms) != 1 || ms[0].TS != fresh {
		t.Fatalf("refresh did not update ts: %+v", ms)
	}
}

func TestGcacheSQLRegistryStalePruned(t *testing.T) {
	reg, _ := newTestSQLRegistry(t)
	ctx := context.Background()

	fresh := time.Now().Unix()
	staleTS := time.Now().Add(-gcache.DefaultHeartbeat * gcache.DefaultStaleMultiplier * 2).Unix()
	if err := reg.Register(ctx, "g", gcache.Member{UUID: "live", Addr: "a:1", TS: fresh}); err != nil {
		t.Fatalf("register live: %s", err)
	}
	if err := reg.Register(ctx, "g", gcache.Member{UUID: "dead", Addr: "a:2", TS: staleTS}); err != nil {
		t.Fatalf("register dead: %s", err)
	}
	if err := reg.Register(ctx, "g", gcache.Member{UUID: "otherGroupDead", Addr: "a:3", TS: staleTS}); err != nil {
		t.Fatalf("register other: %s", err)
	}
	_ = reg.Register(ctx, "g2", gcache.Member{UUID: "dead-in-g2", Addr: "a:4", TS: staleTS})

	ms, err := reg.List(ctx, "g")
	if err != nil {
		t.Fatalf("list: %s", err)
	}
	if len(ms) != 1 || ms[0].UUID != "live" {
		t.Fatalf("stale member not pruned from list: %+v", ms)
	}

	// the global backstop DELETE must have removed dead rows in ALL groups
	dead, err := reg.db.Context(ctx).Where("ts <= ?", time.Now().Add(-reg.ttl).Unix()).Count(new(gcacheMembers))
	if err != nil {
		t.Fatalf("count dead: %s", err)
	}
	if dead != 0 {
		t.Fatalf("stale rows not deleted: %d remain", dead)
	}
}

func TestGcacheSQLRegistryTableNaming(t *testing.T) {
	reg, m := newTestSQLRegistry(t)
	ctx := context.Background()

	if err := reg.Register(ctx, "g", gcache.Member{UUID: "u1", Addr: "a:1", TS: time.Now().Unix()}); err != nil {
		t.Fatalf("register: %s", err)
	}
	// raw-SQL table and mapper-derived table must be the same physical table
	count, err := m.db.Context(ctx).Count(new(gcacheMembers))
	if err != nil {
		t.Fatalf("count via mapper: %s", err)
	}
	if count != 1 {
		t.Fatalf("mapper/raw SQL table mismatch: %d rows visible via mapper", count)
	}
	if reg.gcacheTable() != m.tablePrefix+"gcache_members" {
		t.Fatalf("table name %q does not match prefix %q", reg.gcacheTable(), m.tablePrefix)
	}
}

func TestGcacheRegistryDispatch(t *testing.T) {
	// dbMeta -> SQL registry
	raw, err := newSQLMeta("sqlite3", path.Join(t.TempDir(), "gcache-dispatch.db"), testConfig())
	if err != nil {
		t.Fatalf("newSQLMeta: %s", err)
	}
	t.Cleanup(func() { _ = raw.Shutdown() })
	reg, err := NewGcacheRegistryFromMeta(raw)
	if err != nil {
		t.Fatalf("dbMeta dispatch: %s", err)
	}
	if _, ok := reg.(*GcacheSQLRegistry); !ok {
		t.Fatalf("expected *GcacheSQLRegistry, got %T", reg)
	}
}
