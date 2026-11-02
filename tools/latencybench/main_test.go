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

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// newTestFileSource builds a fileSource over a temp file of nblocks blocks.
// Uses buffered IO (O_DIRECT is exercised on the DGX nodes; the alignment
// path is identical code).
func newTestFileSource(t *testing.T, nblocks int, blockSize int64) *fileSource {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "blocks.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	// block i is filled with byte(i); distinct per block for mapping asserts
	data := make([]byte, int64(nblocks)*blockSize)
	for b := int64(0); b < int64(nblocks); b++ {
		for j := int64(0); j < blockSize; j++ {
			data[b*blockSize+j] = byte(b)
		}
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	rf, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := rf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return &fileSource{f: rf, size: st.Size(), blockSize: blockSize}
}

func readAllSrc(t *testing.T, rc interface {
	Read([]byte) (int, error)
	Close() error
}) []byte {
	t.Helper()
	defer rc.Close()
	var out []byte
	buf := make([]byte, 32<<10)
	for {
		n, err := rc.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out
		}
	}
}

// Regression (review round 2, finding C): LoadCached reads exactly ONE block,
// never the whole file — content must match the addressed block, including
// the short tail block.
func TestFileSourceLoadCachedBounded(t *testing.T) {
	const blockSize = 8192
	s := newTestFileSource(t, 3, blockSize) // 3 blocks; total 24 KiB

	for _, idx := range []int{0, 1, 2} {
		r, err := s.LoadCached(fmt.Sprintf("bench-key-%d", idx))
		if err != nil {
			t.Fatalf("block %d: %v", idx, err)
		}
		got := readAllSrc(t, r)
		if int64(len(got)) != blockSize {
			t.Fatalf("block %d: len %d != %d", idx, len(got), blockSize)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{byte(idx)}, blockSize)) {
			t.Fatalf("block %d: wrong block content", idx)
		}
	}
	// beyond the block space: clean error, no huge allocation
	if _, err := s.LoadCached("bench-key-99"); err == nil {
		t.Fatal("load beyond the block space must fail")
	}
}

// Regression (review round 2, finding F): ranged reads address the key's
// block anywhere in the file, not just the first 4 MiB.
func TestFileSourceRangeMapsBlocks(t *testing.T) {
	const blockSize = 8192
	s := newTestFileSource(t, 4, blockSize)

	for _, idx := range []int{0, 1, 2, 3} {
		r, err := s.LoadCachedRange(fmt.Sprintf("bench-key-%d", idx), 4096, 4096)
		if err != nil {
			t.Fatalf("block %d: %v", idx, err)
		}
		got := readAllSrc(t, r)
		if !bytes.Equal(got, bytes.Repeat([]byte{byte(idx)}, 4096)) {
			t.Fatalf("block %d: ranged read returned another block's data", idx)
		}
	}
	// unaligned offset+length inside one block
	r, err := s.LoadCachedRange("bench-key-1", 5000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	got := readAllSrc(t, r)
	if len(got) != 1000 {
		t.Fatalf("unaligned window: %d bytes", len(got))
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{byte(1)}, 1000)) {
		t.Fatal("unaligned window content mismatch")
	}
	// past the block end: error (the client treats it as protocol mismatch)
	if _, err := s.LoadCachedRange("bench-key-0", blockSize, 4096); err == nil {
		t.Fatal("offset past block end must fail")
	}
	// short tail block clamps the window
	r, err = s.LoadCachedRange("bench-key-3", blockSize-100, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAllSrc(t, r); int64(len(got)) != 100 {
		t.Fatalf("tail clamp: %d bytes, want 100", len(got))
	}
}

// Regression (review round 2, finding D): concurrent cold fills + warm
// ranged reads must not race (run under -race).
func TestMemSourceConcurrentRangeAndFill(t *testing.T) {
	m := newMemSource(4, 1<<20)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("bench-key-%d", (w+i)%6) // mixes warm and cold keys
				if i%2 == 0 {
					_, err := m.LoadCachedRange(key, 0, 4096)
					if err == os.ErrNotExist { // serve protocol: miss → fill → range
						if _, err = m.FillFromStorage(context.Background(), key); err != nil {
							t.Errorf("fill %s: %v", key, err)
							return
						}
						if _, err = m.LoadCachedRange(key, 0, 4096); err != nil {
							t.Errorf("post-fill range %s: %v", key, err)
							return
						}
					} else if err != nil {
						t.Errorf("range %s: %v", key, err)
						return
					}
				} else {
					if _, err := m.FillFromStorage(context.Background(), key); err != nil {
						t.Errorf("fill %s: %v", key, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
}

// The bench pattern must round-trip through gen -> file -> fileSource ->
// patWindow: the exact chain a --verify run depends on across nodes.
func TestGenDatasetVerifyRoundtrip(t *testing.T) {
	const bs = 8192
	path := filepath.Join(t.TempDir(), "bench.bin")
	if err := genDataset(path, 4, bs, false); err != nil {
		t.Fatal(err)
	}
	// idempotent rerun at the right size
	if err := genDataset(path, 4, bs, false); err != nil {
		t.Fatalf("regen at correct size should be a no-op: %v", err)
	}
	// wrong size without --force must be refused
	if err := genDataset(path, 5, bs, false); err == nil {
		t.Fatal("regen at wrong size must be refused without --force")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	s := &fileSource{f: f, size: st.Size(), blockSize: bs}

	for _, idx := range []int64{0, 1, 2, 3} {
		// unaligned windows across page boundaries
		for _, w := range [][2]int64{{0, 100}, {4090, 12}, {5000, 3000}, {bs - 4096, 4096}} {
			r, err := s.LoadCachedRange(fmt.Sprintf("bench-key-%d", idx), w[0], w[1])
			if err != nil {
				t.Fatalf("block %d window %v: %v", idx, w, err)
			}
			got := readAllSrc(t, r)
			exp, err := patWindow(idx, w[0], w[1], bs)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, exp) {
				t.Fatalf("block %d window %v: served bytes != pattern", idx, w)
			}
		}
	}
}

// --verify must catch a corrupted payload (wrong bytes, right size).
func TestVerifyDetectsCorruption(t *testing.T) {
	pc := &peerClient{bs: 8192, verify: true}
	buf, err := patWindow(3, 100, 500, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.checkPayload("bench-key-3", 100, buf); err != nil {
		t.Fatalf("clean payload flagged: %v", err)
	}
	buf[7] ^= 0xff
	err = pc.checkPayload("bench-key-3", 100, buf)
	if err == nil {
		t.Fatal("corrupted payload not detected")
	}
	if !strings.Contains(err.Error(), "VERIFY MISMATCH") || !strings.Contains(err.Error(), "first-diff@7") {
		t.Fatalf("mismatch diagnostic wrong: %v", err)
	}
	// off-by-one-block payload (the wrong-owner bug shape)
	wrong, _ := patWindow(4, 100, 500, 8192)
	if err := pc.checkPayload("bench-key-3", 100, wrong); err == nil {
		t.Fatal("wrong-block payload not detected")
	}
}

// memSource must serve the same pattern as gen'd files (verify over mem servers).
func TestMemSourcePatternContent(t *testing.T) {
	m := newMemSource(3, 8192)
	for _, idx := range []int{0, 1, 2} {
		r, err := m.LoadCachedRange(fmt.Sprintf("bench-key-%d", idx), 4096, 1000)
		if err != nil {
			t.Fatal(err)
		}
		got := readAllSrc(t, r)
		exp, _ := patWindow(int64(idx), 4096, 1000, 8192)
		if !bytes.Equal(got, exp) {
			t.Fatalf("mem block %d content != pattern", idx)
		}
	}
	// cold fill (unique path) also pattern-consistent
	r, err := m.FillFromStorage(context.Background(), "bench-key-9")
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	r2, err := m.LoadCachedRange("bench-key-9", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	got := readAllSrc(t, r2)
	exp, _ := patWindow(9, 0, 1000, 8192)
	if !bytes.Equal(got, exp) {
		t.Fatal("cold-filled mem block content != pattern")
	}
}

// Multi-peer flag parsing: --addr a,b -> 2 members; --rdma-addr parallel list.
func TestSplitCSVAndMembers(t *testing.T) {
	if got := splitCSV(""); got != nil {
		t.Fatalf("empty: %v", got)
	}
	if got := splitCSV("a:1, b:2 ,c:3"); len(got) != 3 || got[1] != "b:2" {
		t.Fatalf("splitCSV: %v", got)
	}
}

// Regression (review round 3, finding 1): the bench member must advertise an
// RDMA endpoint under --rdma, or strict mode rejects every fetch.
func TestBenchMemberEndpointWiring(t *testing.T) {
	// no RDMA: plain member, no RdmaAddr
	if m := benchMember("10.0.0.1:9000", "", false); m.RdmaAddr != "" || m.Addr != "10.0.0.1:9000" {
		t.Fatalf("plain member wrong: %+v", m)
	}
	// --rdma without --rdma-addr: --addr IS the RDMA endpoint
	if m := benchMember("10.42.1.2:7410", "", true); m.RdmaAddr != "10.42.1.2:7410" {
		t.Fatalf("--rdma member missing RdmaAddr: %+v", m)
	}
	// --rdma with an explicit --rdma-addr
	if m := benchMember("10.42.1.2:7410", "10.42.1.2:17410", true); m.RdmaAddr != "10.42.1.2:17410" || m.Addr != "10.42.1.2:7410" {
		t.Fatalf("explicit --rdma-addr not honored: %+v", m)
	}
}

// The aligned-window reader must agree with plain reads of the same region.
func TestFileSourcePreadAlignedMatchesPlain(t *testing.T) {
	const blockSize = 8192
	s := newTestFileSource(t, 2, blockSize)
	plain := make([]byte, 3333)
	if err := s.preadAt(plain, 5000); err != nil {
		t.Fatal(err)
	}
	aligned := make([]byte, 3333)
	if err := s.preadAligned(aligned, 5000); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, aligned) {
		t.Fatal("aligned and plain preads disagree")
	}
}
