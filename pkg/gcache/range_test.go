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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// serveRangeKey builds a plain-mode key with an embedded size (CONTRACT §1).
func serveRangeKey(id, indx, size int) string {
	return fmt.Sprintf("chunks/0/0/%d_%d_%d", id, indx, size)
}

func mustRangeRead(t *testing.T, rc *RingClient, key string, off int64, n int, members []Member) []byte {
	t.Helper()
	buf := make([]byte, n)
	got, err := rc.RangeRead(context.Background(), "g", key, off, buf, members)
	if err != nil {
		t.Fatalf("RangeRead(%s, off=%d, n=%d): %v", key, off, n, err)
	}
	if got != n {
		t.Fatalf("short range read: got %d want %d", got, n)
	}
	return buf
}

// Warm hit: positional read from a cached block, exact bytes at boundaries.
func TestRangeReadWarmHit(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 1<<20)
	for i := range block {
		block[i] = byte(i % 253)
	}
	key := serveRangeKey(1, 0, len(block))
	src.blocks[key] = block
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}

	for _, off := range []int64{0, 4096, 1 << 12, 1<<20 - 4096, 1<<20/2 + 512} {
		got := mustRangeRead(t, rc, key, off, 4096, members)
		if string(got) != string(block[off:off+4096]) {
			t.Fatalf("content mismatch at off=%d", off)
		}
	}
	if src.fillCnt[key] != 0 {
		t.Fatal("warm hit must not trigger FillFromStorage")
	}
	if src.rangeCnt[key] == 0 {
		t.Fatal("owner never used the ranged source")
	}
}

// Cold: owner miss → FillFromStorage (the designated-copy fill) → serve range;
// the second read is warm (no re-fill).
func TestRangeReadColdFill(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 1<<20)
	for i := range block {
		block[i] = byte(255 - i%251)
	}
	key := serveRangeKey(2, 0, len(block))
	src.blocks[key] = block    // what object storage holds
	src.rangeMiss[key] = true  // but not yet in the owner's cache
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	got := mustRangeRead(t, rc, key, 8192, 4096, members)
	if string(got) != string(block[8192:8192+4096]) {
		t.Fatal("cold fill range content mismatch")
	}
	if src.fillCnt[key] != 1 {
		t.Fatalf("want exactly 1 fill, got %d", src.fillCnt[key])
	}
	_ = mustRangeRead(t, rc, key, 0, 4096, members)
	if src.fillCnt[key] != 1 {
		t.Fatalf("second read re-filled: %d", src.fillCnt[key])
	}
}

// Compressed volume: the ranged payload is RAW block bytes (no re-compress on
// the wire) — the exact bytes the owner's local-cache ReadAt would return.
func TestRangeReadCompressedVolumeServesRaw(t *testing.T) {
	src := newFakeSource("zstd")
	raw := make([]byte, 512<<10)
	for i := range raw {
		raw[i] = byte(i * 7)
	}
	key := serveRangeKey(3, 0, len(raw))
	src.blocks[key] = raw
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	got := mustRangeRead(t, rc, key, 12345, 8192, members)
	if string(got) != string(raw[12345:12345+8192]) {
		t.Fatal("compressed-volume range payload mismatch (want raw window)")
	}
}

// legacySource is a pre-extension source: ServerSource only, no
// LoadCachedRange. Delegates to a fakeSource without promoting its methods.
type legacySource struct{ inner *fakeSource }

func (l *legacySource) LoadCached(key string) (io.ReadCloser, error) {
	return l.inner.LoadCached(key)
}
func (l *legacySource) FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error) {
	return l.inner.FillFromStorage(ctx, key)
}
func (l *legacySource) CompressAlgo() string                     { return l.inner.CompressAlgo() }
func (l *legacySource) CompressBound(n int) int                  { return l.inner.CompressBound(n) }
func (l *legacySource) CompressPayload(d, s []byte) (int, error) { return l.inner.CompressPayload(d, s) }
func (l *legacySource) StorePushed(ctx context.Context, k string, d []byte) error {
	return l.inner.StorePushed(ctx, k, d)
}
func (l *legacySource) DropCached(key string) error { return l.inner.DropCached(key) }

// Legacy peer: rejects MsgRangeReq with the classic error frame + close.
// The client returns errRangeUnsupported, memoizes the peer, and later calls
// skip the network entirely (rejection counter stops advancing); after the
// re-probe window it retries once.
func TestRangeReadLegacyPeerMemoized(t *testing.T) {
	var rejections int
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
			go func(c net.Conn) {
				defer c.Close()
				hdr := make([]byte, headerLen)
				if _, err := io.ReadFull(c, hdr); err != nil {
					return
				}
				if hdr[3] == MsgRangeReq {
					rejections++
					klen := int(binary.LittleEndian.Uint16(hdr[9:11]))
					rest := make([]byte, klen+16)
					_, _ = io.ReadFull(c, rest)
					_ = sendError(ctx, c, fmt.Errorf("bad request type 0x%x", hdr[3]), time.Second)
					return
				}
				_ = sendError(ctx, c, errors.New("unexpected"), time.Second)
			}(conn)
		}
	}()
	defer func() { cancel(); _ = ln.Close(); <-done }()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: ln.Addr().String()}}
	key := serveRangeKey(4, 0, 1<<20)
	buf := make([]byte, 4096)
	if _, err := rc.RangeRead(context.Background(), "g", key, 0, buf, members); !errors.Is(err, errRangeUnsupported) {
		t.Fatalf("want errRangeUnsupported, got %v", err)
	}
	if rejections != 1 {
		t.Fatalf("want 1 rejection, got %d", rejections)
	}
	// memoized: no further network attempt
	for i := 0; i < 3; i++ {
		if _, err := rc.RangeRead(context.Background(), "g", key, 0, buf, members); !errors.Is(err, errRangeUnsupported) {
			t.Fatalf("memoization failed: %v", err)
		}
	}
	if rejections != 1 {
		t.Fatalf("memoized peer still hit the network: %d rejections", rejections)
	}
	// expire the memoization: one re-probe
	rc.mu.Lock()
	for k := range rc.rangeNo {
		rc.rangeNo[k] = time.Now().Add(-time.Second)
	}
	rc.mu.Unlock()
	_, _ = rc.RangeRead(context.Background(), "g", key, 0, buf, members)
	if rejections != 2 {
		t.Fatalf("want a re-probe after expiry, rejections=%d", rejections)
	}
}

// Self-owned blocks are never ranged-fetched.
func TestRangeReadSelfOwned(t *testing.T) {
	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "self-test", Addr: "127.0.0.1:1"}}
	_, err := rc.RangeRead(context.Background(), "g", "k", 0, make([]byte, 10), members)
	if !errors.Is(err, errSelfOwned) {
		t.Fatalf("want errSelfOwned, got %v", err)
	}
}

// Out-of-range requests are rejected by the owner (validation paths).
func TestRangeReadValidation(t *testing.T) {
	src := newFakeSource("none")
	key := serveRangeKey(5, 0, 1<<20)
	src.blocks[key] = make([]byte, 1<<20)
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	// off past end → error frame (not errRangeUnsupported)
	_, err := rc.RangeRead(context.Background(), "g", key, 1<<20, make([]byte, 4096), members)
	if err == nil || errors.Is(err, errRangeUnsupported) {
		t.Fatalf("want a plain error for off past end, got %v", err)
	}
	// clamped window (off+size beyond end) → plen mismatch → protocol error
	_, err = rc.RangeRead(context.Background(), "g", key, 1<<20-2, make([]byte, 4096), members)
	if err == nil || errors.Is(err, errRangeUnsupported) {
		t.Fatalf("want a plain error for clamped window, got %v", err)
	}
}

// Concurrent ranged reads share pooled connections and stay correct.
func TestRangeReadConcurrent(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 1<<20)
	for i := range block {
		block[i] = byte(i * 3)
	}
	key := serveRangeKey(6, 0, len(block))
	src.blocks[key] = block
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(2*time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				off := int64((w*25+i)%256) * 4096
				buf := make([]byte, 4096)
				if _, err := rc.RangeRead(context.Background(), "g", key, off, buf, members); err != nil {
					errCh <- err
					return
				}
				if string(buf) != string(block[off:off+4096]) {
					errCh <- fmt.Errorf("content mismatch at %d (worker %d)", off, w)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	dials, reuses, _ := rc.pool.stats()
	if dials > 8 || reuses == 0 {
		t.Fatalf("pooling ineffective under concurrency: dials=%d reuses=%d", dials, reuses)
	}
}

// A source without LoadCachedRange (legacy bridge) gets a clean error frame,
// not a panic; the client surfaces it and callers fall back.
func TestRangeReadSourceWithoutRange(t *testing.T) {
	addr, stop := listenerWithSrc(t, &legacySource{inner: newFakeSource("none")}, 5*time.Second)
	defer stop()
	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	_, err := rc.RangeRead(context.Background(), "g", "k_0_100", 0, make([]byte, 10), members)
	if err == nil || errors.Is(err, errRangeUnsupported) {
		t.Fatalf("want plain unsupported-source error, got %v", err)
	}
	if !strings.Contains(err.Error(), "range reads not supported by this peer's source") {
		t.Fatalf("unexpected error text: %v", err)
	}
}

// Regression (review finding 2): a locally-owned key must return
// errSelfOwned WITHOUT contacting any peer. candidate() skips self, so the
// ownership check has to run first or the request would be routed to a
// non-owner peer that would fill+persist a copy belonging here (strict
// one-copy sharding violation).
func TestRangeReadSelfOwnedNotRoutedToNonOwner(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 1<<20)
	key := serveRangeKey(7, 0, len(block))
	src.blocks[key] = block
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	// two members: self + a live peer. Find a key whose rendezvous owner is
	// self (deterministic search over the HRW scores).
	members := []Member{
		{UUID: "self-test", Addr: "127.0.0.1:1"},
		{UUID: "peer-1", Addr: addr},
	}
	var selfKey string
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("self-owned-%d", i)
		if o := Owners(k, members, 1); len(o) > 0 && o[0].UUID == "self-test" {
			selfKey = k
			break
		}
	}
	if selfKey == "" {
		t.Fatal("no self-owned key found (impossible with 10k candidates)")
	}
	if _, err := rc.RangeRead(context.Background(), "g", selfKey, 0, make([]byte, 4096), members); !errors.Is(err, errSelfOwned) {
		t.Fatalf("want errSelfOwned for locally-owned key, got %v", err)
	}
	// the peer must never have been contacted
	if src.rangeCnt[selfKey] != 0 || src.fillCnt[selfKey] != 0 {
		t.Fatalf("self-owned key was routed to a non-owner peer (rangeCnt=%d fillCnt=%d)",
			src.rangeCnt[selfKey], src.fillCnt[selfKey])
	}
}

// Regression (review finding 3): the cold-fill path must close the reader
// returned by FillFromStorage, or its descriptor leaks per cold fill.
func TestRangeReadColdFillClosesReader(t *testing.T) {
	src := newFakeSource("none")
	block := make([]byte, 1<<20)
	key := serveRangeKey(8, 0, len(block))
	src.blocks[key] = block
	src.rangeMiss[key] = true // cold: fill required
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	if got := mustRangeRead(t, rc, key, 0, 4096, members); string(got) != string(block[0:4096]) {
		t.Fatal("cold fill content mismatch")
	}
	if src.fillCnt[key] != 1 {
		t.Fatalf("want 1 fill, got %d", src.fillCnt[key])
	}
	if src.fillCloses != 1 {
		t.Fatalf("fill reader leaked: %d closes for 1 fill", src.fillCloses)
	}
	// warm re-read: no new fill, no new closes
	_ = mustRangeRead(t, rc, key, 4096, 4096, members)
	if src.fillCnt[key] != 1 || src.fillCloses != 1 {
		t.Fatalf("warm read re-filled or re-closed: fills=%d closes=%d", src.fillCnt[key], src.fillCloses)
	}
}
