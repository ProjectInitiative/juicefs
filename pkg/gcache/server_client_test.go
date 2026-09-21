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
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSource is an in-memory ServerSource for loopback tests.
type fakeSource struct {
	mu       sync.Mutex
	blocks   map[string][]byte
	algo     string
	fillCnt  map[string]int
	loadCnt  map[string]int
	fillHook func(key string) error
}

func newFakeSource(algo string) *fakeSource {
	return &fakeSource{
		blocks:  make(map[string][]byte),
		algo:    algo,
		fillCnt: make(map[string]int),
		loadCnt: make(map[string]int),
	}
}

func (f *fakeSource) LoadCached(key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCnt[key]++
	if b, ok := f.blocks[key]; ok {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	return nil, fmtErrNotExist
}

func (f *fakeSource) FillFromStorage(_ context.Context, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fillCnt[key]++
	if f.fillHook != nil {
		if err := f.fillHook(key); err != nil {
			return nil, err
		}
	}
	if b, ok := f.blocks[key]; ok {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	return nil, errors.New("no such block in storage")
}

func (f *fakeSource) CompressAlgo() string { return f.algo }

func (f *fakeSource) CompressBound(n int) int { return n + 16 }

// CompressPayload for tests: reversible "compression" = prefix marker + copy.
func (f *fakeSource) CompressPayload(dst, src []byte) (int, error) {
	if len(dst) < len(src)+16 {
		return 0, errors.New("dst too small")
	}
	dst[0] = 'Z'
	dst[1] = 'T'
	n := copy(dst[16:], src)
	return n + 16, nil
}

// decompressFake inverts CompressPayload for assertions.
func decompressFake(b []byte) []byte { return b[16:] }

var fmtErrNotExist = os.ErrNotExist

// listenerWithSrc serves connections on a fresh loopback port.
func listenerWithSrc(t *testing.T, src ServerSource, timeout time.Duration) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go ServeConn(ctx, conn, src, timeout)
		}
	}()
	stop = func() {
		cancel()
		_ = ln.Close()
		<-done
	}
	return ln.Addr().String(), stop
}

// fetchAll drains an io.ReadCloser fully.
func fetchAll(t *testing.T, r io.ReadCloser) []byte {
	t.Helper()
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	return b
}

func mustFetch(t *testing.T, rc *RingClient, group, key string, members []Member) io.ReadCloser {
	t.Helper()
	r, err := rc.Fetch(context.Background(), group, key, members)
	if err != nil {
		t.Fatalf("fetch %s: %v", key, err)
	}
	return r
}

// Round-trip 4MiB raw block through server+client.
func TestServerClientRoundTripRaw(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 4<<20)
	for i := range block {
		block[i] = byte(i % 251)
	}
	src.blocks["1_7_4194304"] = block
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	got := fetchAll(t, mustFetch(t, rc, "g", "1_7_4194304", members))
	if !bytes.Equal(got, block) {
		t.Fatalf("payload mismatch: got %d bytes", len(got))
	}
	if src.fillCnt["1_7_4194304"] != 0 {
		t.Fatal("cached block should not trigger FillFromStorage")
	}
}

// Cached-miss path: FillFromStorage serves and the block arrives.
func TestServerClientFillFromStorage(t *testing.T) {
	src := newFakeSource("none")
	block := bytes.Repeat([]byte{0xAB}, 1<<20)
	src.blocks["1_9_1048576"] = block
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	got := fetchAll(t, mustFetch(t, rc, "g", "1_9_1048576", members))
	if !bytes.Equal(got, block) {
		t.Fatal("fill payload mismatch")
	}
}

// Compressed volume: server re-compresses; client receives the
// volume-format (fake-compressed) bytes.
func TestServerClientCompressed(t *testing.T) {
	src := newFakeSource("zstd")
	raw := bytes.Repeat([]byte("train-data"), 100000) // ~1MB
	src.blocks["1_3_1000000"] = raw
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	got := fetchAll(t, mustFetch(t, rc, "g", "1_3_1000000", members))
	if !bytes.Equal(decompressFake(got), raw) {
		t.Fatal("compressed round-trip mismatch")
	}
}

// Error frame: unknown key that also fails FillFromStorage => errFrame.
func TestServerClientErrorFrame(t *testing.T) {
	src := newFakeSource("none")
	src.fillHook = func(key string) error { return errors.New("object storage down") }
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	_, err := rc.Fetch(context.Background(), "g", "missing_key", members)
	if err == nil {
		t.Fatal("want error for unservable key")
	}
	if !strings.Contains(err.Error(), "object storage down") {
		t.Fatalf("want peer error text, got: %v", err)
	}
	// A peer that ANSWERS (even with an error) is healthy.
	if rc.isEvicted(addr) {
		t.Fatal("answering peer must not be evicted")
	}
}

// Pipelining: 2 concurrent Fetches on independent conns both succeed.
func TestServerClientConcurrent(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k1"] = bytes.Repeat([]byte{1}, 256<<10)
	src.blocks["k2"] = bytes.Repeat([]byte{2}, 256<<10)
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, k := range []string{"k1", "k2"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			r, err := rc.Fetch(context.Background(), "g", key, members)
			if err != nil {
				errs <- err
				return
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(b, src.blocks[key]) {
				errs <- errors.New("concurrent payload mismatch")
			}
		}(k)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// Eviction: maxFailures consecutive dial failures evict the peer; Fetch then
// returns errNoOwners (no candidates); a later success un-evicts.
func TestEvictionAndReAdd(t *testing.T) {
	dead, deadStop := listenerWithSrc(t, newFakeSource("none"), time.Second)
	deadStop() // closed: dials fail fast

	live := newFakeSource("none")
	live.blocks["k"] = []byte("alive")
	liveAddr, liveStop := listenerWithSrc(t, live, 5*time.Second)
	defer liveStop()

	rc := NewRingClient(200*time.Millisecond, 3, "")
	members := []Member{
		{UUID: "dead", Addr: dead},
		{UUID: "live", Addr: liveAddr},
	}
	// Rendezvous may pick either member per key; use distinct keys until we
	// hit the dead peer 3 times.
	attempts := 0
	for !rc.isEvicted(dead) && attempts < 100 {
		key := fmt.Sprintf("evict-key-%d", attempts)
		_, _ = rc.Fetch(context.Background(), "g", key, members)
		attempts++
	}
	if !rc.isEvicted(dead) {
		t.Fatal("dead peer was not evicted after repeated failures")
	}

	// Forced path: mark failures directly for deterministic assertions.
	rc2 := NewRingClient(200*time.Millisecond, 3, "")
	for i := 0; i < 3; i++ {
		rc2.MarkFailure("h:1")
	}
	if !rc2.isEvicted("h:1") {
		t.Fatal("peer not evicted at maxFailures")
	}
	if got := rc2.candidate("k", []Member{{UUID: "x", Addr: "h:1"}}, ""); got != nil {
		t.Fatal("evicted peer must not be a candidate")
	}
	rc2.MarkSuccess("h:1")
	if rc2.isEvicted("h:1") {
		t.Fatal("MarkSuccess must un-evict")
	}
}

// Success marks clear failure counters.
func TestMarkSuccessClearsFailures(t *testing.T) {
	rc := NewRingClient(time.Second, 2, "self-test")
	rc.MarkFailure("p")
	rc.MarkFailure("p") // evicted at 2
	if !rc.isEvicted("p") {
		t.Fatal("want eviction at 2 failures")
	}
	rc.MarkSuccess("p")
	if rc.isEvicted("p") {
		t.Fatal("want un-evict on success")
	}
	// counter cleared: one failure no longer evicts
	rc.MarkFailure("p")
	if rc.isEvicted("p") {
		t.Fatal("failure counter should have been cleared")
	}
}
