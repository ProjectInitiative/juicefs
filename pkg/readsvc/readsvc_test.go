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
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

// fakeFile is a handleFile over a byte slice.
type fakeFile struct {
	data []byte
}

func (f *fakeFile) Pread(_ meta.Context, b []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, fmt.Errorf("offset past end")
	}
	n := len(b)
	if off+int64(n) > int64(len(f.data)) {
		n = int(int64(len(f.data)) - off)
	}
	copy(b, f.data[off:off+int64(n)])
	return n, nil
}
func (f *fakeFile) Close(_ meta.Context) syscall.Errno { return 0 }

// fakeBackend opens files from a map.
type fakeBackend struct {
	files map[string][]byte
}

func (b *fakeBackend) Open(_ meta.Context, path string) (handleFile, int64, error) {
	if d, ok := b.files[path]; ok {
		return &fakeFile{data: d}, int64(len(d)), nil
	}
	return nil, 0, fmt.Errorf("no such file: %s", path)
}

func startTestServer(t *testing.T, files map[string][]byte) (sockPath string, stop func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "readsvc-test")
	if err != nil {
		t.Fatal(err)
	}
	sockPath = filepath.Join(dir, "read.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(&fakeBackend{files: files}, sockPath)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ctx, ln) }()
	return sockPath, func() {
		cancel()
		_ = ln.Close()
		srv.Close()
		<-done
		_ = os.RemoveAll(dir)
	}
}

func TestClientServerRoundTrip(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i * 5)
	}
	sockPath, stop := startTestServer(t, map[string][]byte{"/models/table.bin": data})
	defer stop()

	c, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h, err := c.OpenHandle("/models/table.bin")
	if err != nil {
		t.Fatal(err)
	}
	if h.Size != int64(len(data)) {
		t.Fatalf("size mismatch: %d != %d", h.Size, len(data))
	}
	// exactness at boundaries
	for _, off := range []int64{0, 4096, 1 << 12, int64(len(data)) - 4096, 12345} {
		buf := make([]byte, 4096)
		n, err := h.ReadAt(buf, off)
		if err != nil {
			t.Fatalf("readat off=%d: %v", off, err)
		}
		_ = n
		end := off + 4096
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		want := data[off:end]
		if !bytes.Equal(buf[:len(want)], want) {
			t.Fatalf("content mismatch at off=%d", off)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	// handle is gone server-side
	buf := make([]byte, 16)
	if _, err := h.ReadAt(buf, 0); err == nil {
		t.Fatal("read after close must fail")
	}
}

func TestClientBatchReadAt(t *testing.T) {
	data := make([]byte, 256<<10)
	for i := range data {
		data[i] = byte(i)
	}
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
	const batch = 32
	bufs := make([][]byte, batch)
	offs := make([]int64, batch)
	for i := range bufs {
		bufs[i] = make([]byte, 4096)
		offs[i] = int64(i*4096) % (256<<10 - 4096)
	}
	got, err := h.BatchReadAt(bufs, offs)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if got[i] != 4096 {
			t.Fatalf("entry %d: got %d", i, got[i])
		}
		if !bytes.Equal(bufs[i], data[offs[i]:offs[i]+4096]) {
			t.Fatalf("entry %d content mismatch", i)
		}
	}
	// oversized entries are rejected
	big := make([][]byte, 1)
	big[0] = make([]byte, MaxReadLen+1)
	if _, err := h.BatchReadAt(big, []int64{0}); err == nil {
		t.Fatal("oversized batch entry must be rejected client-side")
	}
}

// Concurrent connections (one Client per worker) must interleave safely and
// serve correct data — the pattern for the 70-way inference workload.
func TestConcurrentClients(t *testing.T) {
	data := make([]byte, 4<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}
	sockPath, stop := startTestServer(t, map[string][]byte{"/w": data})
	defer stop()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := Dial(sockPath)
			if err != nil {
				errCh <- err
				return
			}
			defer c.Close()
			h, err := c.OpenHandle("/w")
			if err != nil {
				errCh <- err
				return
			}
			defer h.Close()
			for i := 0; i < 50; i++ {
				off := int64((w*50+i)%1024) * 4096
				buf := make([]byte, 4096)
				if _, err := h.ReadAt(buf, off); err != nil {
					errCh <- err
					return
				}
				if !bytes.Equal(buf, data[off:off+4096]) {
					errCh <- fmt.Errorf("worker %d content mismatch at %d", w, off)
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
}

func TestOpenMissingFile(t *testing.T) {
	sockPath, stop := startTestServer(t, map[string][]byte{})
	defer stop()
	c, err := Dial(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.Open("/nope"); err == nil {
		t.Fatal("open of missing file must fail")
	}
}
