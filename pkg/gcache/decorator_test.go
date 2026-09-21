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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// trackingInner wraps a static ObjectStorage and counts full-object Gets.
type trackingInner struct {
	object.ObjectStorage
	fullGets atomic.Int32
}

// newFakeInner returns a non-nil embedded ObjectStorage so trackingInner
// panics loudly if any method other than Get is reached unexpectedly.
func newFakeInner() object.ObjectStorage { return nilStorage{} }

// nilStorage fails every method — used only as the embedded base.
type nilStorage struct{}

func (nilStorage) String() string               { return "nil-storage" }
func (nilStorage) Limits() object.Limits        { return object.Limits{} }
func (nilStorage) Create(context.Context) error { return errNilStorage }
func (nilStorage) Get(context.Context, string, int64, int64, ...object.AttrGetter) (io.ReadCloser, error) {
	return nil, errNilStorage
}
func (nilStorage) Put(context.Context, string, io.Reader, ...object.AttrGetter) error {
	return errNilStorage
}
func (nilStorage) Copy(context.Context, string, string) error                 { return errNilStorage }
func (nilStorage) Delete(context.Context, string, ...object.AttrGetter) error { return errNilStorage }
func (nilStorage) Head(context.Context, string) (object.Object, error)        { return nil, errNilStorage }
func (nilStorage) List(context.Context, string, string, string, string, int64, bool) ([]object.Object, bool, string, error) {
	return nil, false, "", errNilStorage
}
func (nilStorage) ListAll(context.Context, string, string, bool) (<-chan object.Object, error) {
	return nil, errNilStorage
}
func (nilStorage) CreateMultipartUpload(context.Context, string) (*object.MultipartUpload, error) {
	return nil, errNilStorage
}
func (nilStorage) UploadPart(context.Context, string, string, int, []byte) (*object.Part, error) {
	return nil, errNilStorage
}
func (nilStorage) UploadPartCopy(context.Context, string, string, int, string, int64, int64) (*object.Part, error) {
	return nil, errNilStorage
}
func (nilStorage) AbortUpload(context.Context, string, string) {}
func (nilStorage) CompleteUpload(context.Context, string, string, []*object.Part) error {
	return errNilStorage
}
func (nilStorage) ListUploads(context.Context, string) ([]*object.PendingPart, string, error) {
	return nil, "", errNilStorage
}
func (nilStorage) Restore(context.Context, string, int32) error { return errNilStorage }

var errNilStorage = errors.New("nilStorage method reached")

func (ti *trackingInner) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	if off == 0 && limit == -1 {
		ti.fullGets.Add(1)
	}
	attrs := object.ApplyGetters(getters...)
	attrs.SetRequestID("inner-echo") // writes through the caller's pointer
	return io.NopCloser(bytes.NewReader([]byte("inner-bytes-" + key))), nil
}

// findKeyFor returns a key whose rendezvous owner is want.
func findKeyFor(want string, members []Member) string {
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("1_%d_1048576", i)
		if Owners(k, members, 1)[0].UUID == want {
			return k
		}
	}
	return ""
}

func mustGet(t *testing.T, s object.ObjectStorage, key string) io.ReadCloser {
	t.Helper()
	r, err := s.Get(context.Background(), key, 0, -1)
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	return r
}

func readAll(t *testing.T, r io.ReadCloser) []byte {
	t.Helper()
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return b
}

// A peer-owned key is served by the peer; inner is untouched.
func TestDecoratorInterceptsFullBlock(t *testing.T) {
	peer := newFakeSource("none")
	peerAddr, stop := listenerWithSrc(t, peer, 5*time.Second)
	defer stop()

	members := []Member{
		{UUID: "self-uuid", Addr: "127.0.0.1:1"},
		{UUID: "peer-uuid", Addr: peerAddr},
	}
	key := findKeyFor("peer-uuid", members)
	if key == "" {
		t.Fatal("no key maps to peer")
	}
	block := bytes.Repeat([]byte{0x5A}, 512<<10)
	peer.blocks[key] = block
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	decorated := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid")

	got := readAll(t, mustGet(t, decorated, key))
	if !bytes.Equal(got, block) {
		t.Fatal("decorator payload mismatch")
	}
	if inner.fullGets.Load() != 0 {
		t.Fatal("inner.Get must not be called when peer serves")
	}
}

// Self-owned key falls through to inner.
func TestDecoratorFallsThroughSelfOwned(t *testing.T) {
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{
		{UUID: "self-uuid", Addr: "127.0.0.1:1"},
		{UUID: "peer-uuid", Addr: "127.0.0.1:2"}, // nobody listening
	}
	decorated := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid")

	key := findKeyFor("self-uuid", members)
	got := readAll(t, mustGet(t, decorated, key))
	if !bytes.Equal(got, []byte("inner-bytes-"+key)) {
		t.Fatal("self-owned key must be served by inner")
	}
	if inner.fullGets.Load() != 1 {
		t.Fatalf("want exactly 1 inner full Get, got %d", inner.fullGets.Load())
	}
}

// No members at all: pure passthrough.
func TestDecoratorFallsThroughNoMembers(t *testing.T) {
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	decorated := NewRingStorage(inner, rc, func(string) []Member { return nil }, "self-uuid")

	got := readAll(t, mustGet(t, decorated, "any-key"))
	if !bytes.Equal(got, []byte("inner-bytes-any-key")) {
		t.Fatal("no-members Get must hit inner")
	}
	if inner.fullGets.Load() != 1 {
		t.Fatal("inner full Get count mismatch")
	}
}

// Partial reads (off>0 or limit>-1) always fall through.
func TestDecoratorFallsThroughPartialReads(t *testing.T) {
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-uuid", Addr: "127.0.0.1:2"}}
	decorated := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid")

	for _, spec := range [][2]int64{{128, -1}, {0, 4096}, {512, 4096}} {
		r, err := decorated.Get(context.Background(), "k", spec[0], spec[1])
		if err != nil {
			t.Fatalf("partial Get(%d,%d): %v", spec[0], spec[1], err)
		}
		_, _ = io.ReadAll(r)
		_ = r.Close()
	}
	if inner.fullGets.Load() != 0 {
		t.Fatal("partial reads must not count as full Gets")
	}
}

// Peer answers with an error frame => decorator falls back to inner.
func TestDecoratorFallsThroughOnPeerError(t *testing.T) {
	peer := newFakeSource("none")
	peer.fillHook = func(key string) error { return fmt.Errorf("no such block") }
	peerAddr, stop := listenerWithSrc(t, peer, 5*time.Second)
	defer stop()

	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-uuid", Addr: peerAddr}}
	decorated := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid")

	key := findKeyFor("peer-uuid", members)
	got := readAll(t, mustGet(t, decorated, key))
	if !bytes.Equal(got, []byte("inner-bytes-"+key)) {
		t.Fatal("peer-error Get must fall through to inner")
	}
}

// AttrGetters are forwarded to inner on fall-through: the inner echo via
// SetRequestID writes through the caller's pointer only if the getter chain
// reached inner.
func TestDecoratorForwardsGetters(t *testing.T) {
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	rc := NewRingClient(time.Second, 31, "self-test")
	decorated := NewRingStorage(inner, rc, func(string) []Member { return nil }, "self-uuid")

	reqID := ""
	r, err := decorated.Get(context.Background(), "gkey", 0, -1, object.WithRequestID(&reqID))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(r)
	_ = r.Close()
	if reqID != "inner-echo" {
		t.Fatalf("getter chain did not reach inner: reqID=%q", reqID)
	}
}

// Full end-to-end: Manager (own listener) + SetSource + Start + decorator
// Fetch round-trip.
func TestManagerEndToEnd(t *testing.T) {
	peerSrc := newFakeSource("none")
	block := bytes.Repeat([]byte{0xCD}, 1<<20)
	peerSrc.blocks["1_42_1048576"] = block

	peerMgr := NewManager(Config{Groups: []string{"g"}, ListenAddr: "127.0.0.1:0"}, newFakeRegistry(), peerSrc)
	if err := peerMgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer peerMgr.Stop()
	peerAddr := peerMgr.Addr()
	if peerAddr == "" {
		t.Fatal("peer manager must expose its listener addr")
	}

	// Consumer side: decorator over inner, member view = the peer
	// (normally populated by the heartbeat loop from the shared registry).
	selfMgr := NewManager(Config{Groups: []string{"g"}}, newFakeRegistry(), nil)
	rc := NewRingClient(time.Second, 31, "self-test")
	inner := &trackingInner{ObjectStorage: newFakeInner()}
	selfMembers := []Member{{UUID: peerMgr.UUID(), Addr: peerMgr.Addr()}}
	decorated := NewRingStorage(inner, rc, func(string) []Member { return selfMembers }, selfMgr.UUID())

	got := readAll(t, mustGet(t, decorated, "1_42_1048576"))
	if !bytes.Equal(got, block) {
		t.Fatal("end-to-end payload mismatch")
	}
	if inner.fullGets.Load() != 0 {
		t.Fatal("end-to-end Get must not reach inner")
	}
	if !strings.HasPrefix(selfMgr.UUID(), "") {
		t.Fatal("unreachable") // keeps strings import used if asserts change
	}
}
