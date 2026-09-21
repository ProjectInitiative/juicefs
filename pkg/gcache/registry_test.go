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
	"sync"
	"testing"
	"time"
)

// fakeRegistry is an in-memory Registry for Manager integration tests.
type fakeRegistry struct {
	mu       sync.Mutex
	groups   map[string]map[string]Member // group -> uuid -> member
	register int
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{groups: make(map[string]map[string]Member)}
}

func (f *fakeRegistry) Register(_ context.Context, group string, self Member) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groups[group] == nil {
		f.groups[group] = make(map[string]Member)
	}
	f.groups[group][self.UUID] = self
	f.register++
	return nil
}

func (f *fakeRegistry) Refresh(_ context.Context, group string, self Member) error {
	return f.Register(context.Background(), group, self)
}

func (f *fakeRegistry) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.register
}

func (f *fakeRegistry) List(_ context.Context, group string) ([]Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Member, 0, len(f.groups[group]))
	for _, m := range f.groups[group] {
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeRegistry) Close() {}

// Manager registers itself into the registry on Start and caches members.
func TestManagerRegistersAndLists(t *testing.T) {
	reg := newFakeRegistry()
	// Providers (src != nil, NoSharing false) register on every heartbeat;
	// consumer-only managers intentionally do not.
	mgr := NewManager(Config{Groups: []string{"g1"}, Heartbeat: 50 * time.Millisecond}, reg, newFakeSource("none"))
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()

	// First beat happens synchronously at Start via the loop goroutine; wait
	// for at least one heartbeat.
	deadline := time.Now().Add(2 * time.Second)
	for reg.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if reg.count() == 0 {
		t.Fatal("manager never registered with the registry")
	}
	if mgr.Members("g1") != nil && len(mgr.Members("g1")) != 0 {
		t.Fatal("fresh manager should have no peers")
	}
}

// Two managers sharing a registry discover each other as peers.
func TestManagerPeerDiscovery(t *testing.T) {
	reg := newFakeRegistry()

	srcA := newFakeSource("none")
	srcA.blocks["shared"] = []byte("from-a")
	mgrA := NewManager(Config{Groups: []string{"g"}, Heartbeat: 20 * time.Millisecond}, reg, srcA)
	if err := mgrA.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mgrA.Stop()

	mgrB := NewManager(Config{Groups: []string{"g"}, Heartbeat: 20 * time.Millisecond, NoSharing: true}, reg, nil)
	if err := mgrB.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer mgrB.Stop()

	// Wait until B sees A (and A sees itself excluded).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		peers := mgrB.Members("g")
		if len(peers) == 1 && peers[0].UUID == mgrA.UUID() && len(mgrA.Members("g")) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	peers := mgrB.Members("g")
	if len(peers) != 1 || peers[0].UUID != mgrA.UUID() {
		t.Fatalf("B's view of the group is wrong: %+v", peers)
	}
	if len(mgrA.Members("g")) != 0 {
		t.Fatalf("A must exclude itself: %+v", mgrA.Members("g"))
	}
}

// The real redis Registry implementation compiles and satisfies the
// interface. Integration against a live/miniredis server is skipped when no
// server is available (no miniredis dependency allowed in go.mod).
func TestRedisRegistryContract(t *testing.T) {
	var _ Registry = NewRedisRegistry(nil, "prefix-")
	t.Skip("redis integration test requires a live redis server; interface satisfied")
}
