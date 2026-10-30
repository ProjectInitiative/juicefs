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

package readsvc

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// closeCountingFile counts Close() calls (disconnect/handle-leak regressions).
type closeCountingFile struct {
	fakeFile
	closes atomic.Int32
}

func (f *closeCountingFile) Close(_ meta.Context) syscall.Errno {
	f.closes.Add(1)
	return 0
}

// closingBackend hands out closeCountingFiles.
type closingBackend struct {
	files map[string]*closeCountingFile
}

func (b *closingBackend) Open(_ meta.Context, path string) (handleFile, int64, error) {
	if f, ok := b.files[path]; ok {
		return f, int64(len(f.data)), nil
	}
	return nil, 0, syscall.Errno(2) // ENOENT
}

// Regression (review finding 4): handle ids are per-connection — an id from
// connection A must be invalid on connection B.
func TestHandlesScopedPerConnection(t *testing.T) {
	if !peerCredsSupported() {
		t.Skip("peer credentials unsupported on this platform")
	}
	data := make([]byte, 1<<20)
	sockPath, stop := startTestServer(t, map[string][]byte{"/f": data})
	defer stop()

	c1, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	h1a, err := c1.OpenHandle("/f")
	if err != nil {
		t.Fatal(err)
	}
	// a second handle on c1: per-conn id space means its id may collide with
	// ids on other connections — forge THAT one onto c2.
	h1b, err := c1.OpenHandle("/f")
	if err != nil {
		t.Fatal(err)
	}
	// c2 opens its own handle
	h2, err := c2.OpenHandle("/f")
	if err != nil {
		t.Fatal(err)
	}
	// forge c1's second handle id onto c2's connection: must be rejected
	forged := &Handle{c: c2, id: h1b.id, Size: h1b.Size}
	if _, err := forged.ReadAt(make([]byte, 16), 0); err == nil {
		t.Fatal("cross-connection handle id must be rejected")
	}
	// the legitimate handles still work
	if _, err := h1a.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("legitimate handle broke: %v", err)
	}
	if _, err := h1b.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("legitimate handle broke: %v", err)
	}
	if _, err := h2.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatalf("legitimate handle broke: %v", err)
	}
}

// Regression (review finding 4): a disconnect closes the connection's
// remaining handles (no fd/handle leak on unclean client exit).
func TestDisconnectClosesHandles(t *testing.T) {
	if !peerCredsSupported() {
		t.Skip("peer credentials unsupported on this platform")
	}
	f := &closeCountingFile{fakeFile: fakeFile{data: make([]byte, 1<<20)}}
	backend := &closingBackend{files: map[string]*closeCountingFile{"/f": f}}
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(backend, sockPath)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ctx, ln) }()
	defer func() { cancel(); ln.Close(); srv.Close(); <-done }()

	c, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.OpenHandle("/f")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatal(err)
	}
	// unclean exit: no Close on the handle
	_ = c.Close()
	// wait for the server to notice and clean up
	deadline := time.Now().Add(5 * time.Second)
	for f.closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.closes.Load() == 0 {
		t.Fatal("disconnect did not close the connection's handles")
	}
}

// Regression (review finding 1): batches whose aggregate size exceeds the
// budget are rejected client- and server-side; small batches do not depend
// on per-entry MaxReadLen capacity reservations.
func TestBatchAggregateBudget(t *testing.T) {
	if !peerCredsSupported() {
		t.Skip("peer credentials unsupported on this platform")
	}
	data := make([]byte, 1<<20)
	sockPath, stop := startTestServer(t, map[string][]byte{"/f": data})
	defer stop()

	c, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h, err := c.OpenHandle("/f")
	if err != nil {
		t.Fatal(err)
	}
	// 40 × 1 MiB = 40 MiB > MaxBatchBytes(32 MiB): rejected before sending
	big := make([][]byte, 40)
	offs := make([]int64, 40)
	for i := range big {
		big[i] = make([]byte, 1<<20)
		offs[i] = 0
	}
	if _, err := h.BatchReadAt(big, offs); err == nil {
		t.Fatal("over-budget batch must be rejected client-side")
	}
	// 256 × 4 KiB (the reviewer's example shape) works and returns exact data
	small := make([][]byte, 256)
	offs2 := make([]int64, 256)
	for i := range small {
		small[i] = make([]byte, 4096)
		offs2[i] = int64(i*4096) % (1<<20 - 4096)
	}
	got, err := h.BatchReadAt(small, offs2)
	if err != nil {
		t.Fatalf("256×4KiB batch failed: %v", err)
	}
	for i := range got {
		if got[i] != 4096 {
			t.Fatalf("entry %d: got %d", i, got[i])
		}
	}
}
