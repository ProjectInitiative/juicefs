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

package chunk

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// fakePeerRange is a PeerRangeLoader serving windows from a backing store,
// with call accounting and failure injection. It models the gcache
// RingClient.RangeRead wiring from cmd/mount.go.
type fakePeerRange struct {
	mu      sync.Mutex
	blocks  map[string][]byte
	calls   map[string]int // key -> calls
	failFor map[string]bool
	shortFor map[string]bool // return n < len(buf)
}

func newFakePeerRange() *fakePeerRange {
	return &fakePeerRange{
		blocks:   make(map[string][]byte),
		calls:    make(map[string]int),
		failFor:  make(map[string]bool),
		shortFor: make(map[string]bool),
	}
}

func (f *fakePeerRange) loader() PeerRangeLoader {
	return func(_ context.Context, key string, off int64, buf []byte) (int, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls[key]++
		if f.failFor[key] {
			return 0, fmt.Errorf("peer unavailable")
		}
		b, ok := f.blocks[key]
		if !ok || off >= int64(len(b)) {
			return 0, fmt.Errorf("no such block")
		}
		n := len(buf)
		if off+int64(n) > int64(len(b)) {
			n = int(int64(len(b)) - off)
		}
		copy(buf, b[off:off+int64(n)])
		if f.shortFor[key] && n > 4 {
			n -= 4
		}
		return n, nil
	}
}

func (f *fakePeerRange) callsFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[key]
}

func blockKey(sliceID uint64, indx, size int) string {
	return fmt.Sprintf("chunks/0/0/%d_%d_%d", sliceID, indx, size)
}

func readSlice(t *testing.T, store ChunkStore, sliceID uint64, size, off, length int) []byte {
	t.Helper()
	r := store.NewReader(sliceID, size)
	p := NewPage(make([]byte, length))
	n, err := r.ReadAt(ctx, p, off)
	if err != nil || n != length {
		t.Fatalf("read off=%d len=%d: n=%d err=%v", off, length, n, err)
	}
	return p.Data
}

// Small reads (<= BlockSize/4) must be served by the peer-range loader with
// correct content at block-start, mid, and end offsets — without touching
// object storage (the backing block is absent there; a fallback would fail).
func TestPeerRangeServesSmallReads(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, defaultConf, nil)
	fp := newFakePeerRange()
	const sliceID = 700
	bs := defaultConf.BlockSize
	key := blockKey(sliceID, 0, bs)
	block := make([]byte, bs)
	for i := range block {
		block[i] = byte(i % 251)
	}
	fp.blocks[key] = block
	SetPeerRangeLoaderOnStore(store, fp.loader())
	defer SetPeerRangeLoaderOnStore(store, nil)

	for _, off := range []int{0, 4096, 4096 * 7, bs - 4096, bs/2 + 512} {
		got := readSlice(t, store, sliceID, bs, off, 4096)
		if !bytes.Equal(got, block[off:off+4096]) {
			t.Fatalf("content mismatch at off=%d", off)
		}
	}
	if c := fp.callsFor(key); c != 5 {
		t.Fatalf("want 5 loader calls, got %d", c)
	}
	// the loader path must not have populated the local disk cache for this
	// key (strict one-copy sharding: the owner holds the copy)
	if _, err := store.(*cachedStore).bcache.load(key); err == nil {
		t.Fatal("peer-range hit must not persist the block locally")
	}
}

// Reads above the threshold go through the standard full-block path.
func TestPeerRangeSkipsLargeReads(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, defaultConf, nil)
	fp := newFakePeerRange()
	const sliceID = 701
	bs := defaultConf.BlockSize
	SetPeerRangeLoaderOnStore(store, fp.loader())
	defer SetPeerRangeLoaderOnStore(store, nil)

	// a full-block read (== blockSize): standard path, loader untouched
	if err := forgetSlice(store, sliceID, bs); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // flush to storage
	got := readSlice(t, store, sliceID, bs, 0, bs)
	if !bytes.Equal(got, bytes.Repeat([]byte{0x41}, bs)) {
		t.Fatal("full-block content mismatch")
	}
	if fp.callsFor(blockKey(sliceID, 0, bs)) != 0 {
		t.Fatal("large read must not use the peer-range loader")
	}
}

// Loader failure falls back to the standard path (object storage) and the
// read still succeeds with correct content.
func TestPeerRangeFallsBackOnError(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, defaultConf, nil)
	fp := newFakePeerRange()
	const sliceID = 702
	bs := defaultConf.BlockSize
	key := blockKey(sliceID, 0, bs)
	block := make([]byte, bs)
	for i := range block {
		block[i] = byte(200 - i%200)
	}
	fp.failFor[key] = true
	SetPeerRangeLoaderOnStore(store, fp.loader())
	defer SetPeerRangeLoaderOnStore(store, nil)

	// put the block into object storage under its real key; the fallback
	// full-block load must find it
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(block)))

	got := readSlice(t, store, sliceID, bs, 8192, 4096)
	if !bytes.Equal(got, block[8192:8192+4096]) {
		t.Fatal("fallback content mismatch")
	}
	if fp.callsFor(key) == 0 {
		t.Fatal("loader was not attempted")
	}
}

// A short loader response (n < len(buf)) is treated as a failure: the read
// falls back instead of returning partial data.
func TestPeerRangeShortResponseFallsBack(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, defaultConf, nil)
	fp := newFakePeerRange()
	const sliceID = 703
	bs := defaultConf.BlockSize
	key := blockKey(sliceID, 0, bs)
	block := make([]byte, bs)
	for i := range block {
		block[i] = byte(i % 17)
	}
	fp.blocks[key] = block
	fp.shortFor[key] = true
	SetPeerRangeLoaderOnStore(store, fp.loader())
	defer SetPeerRangeLoaderOnStore(store, nil)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(block)))

	got := readSlice(t, store, sliceID, bs, 4096, 4096)
	if !bytes.Equal(got, block[4096:4096+4096]) {
		t.Fatal("short-response fallback content mismatch")
	}
}

// No loader wired: behavior identical to upstream (control test).
func TestPeerRangeDisabledByDefault(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, defaultConf, nil)
	if cs := store.(*cachedStore); cs.peerRangeLimit() != 0 {
		t.Fatal("peer range must be disabled unless wired")
	}
}
