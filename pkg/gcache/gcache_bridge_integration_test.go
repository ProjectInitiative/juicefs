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

// End-to-end integration test for the pkg/chunk GcacheBridge (imported here
// because pkg/chunk's test binary links upstream's mockey dep, which fails
// to build on some platforms; pkg/gcache's test binary has no such dep).
// This test compiles ONLY with the chunk tag: it imports pkg/chunk, which
// would otherwise create a heavyweight test dependency for pkg/gcache users.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/object"
)

// TestGcacheBridgeIntegration builds a real cachedStore (uncompressed volume),
// bridges it, and drives the full server path: FillFromStorage → LoadCached →
// ServeConn serving a peer request.
func TestGcacheBridgeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	mem, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	// (cacheDir variable removed: memory-cache mode needs no temp dir)

	// mirrors cached_store_test.go defaultConf; Compress "none" exercises the
	// WireRaw path. CacheDir "memory" (not disk): upstream's diskCache spawns
	// checkFreeSpace which races with cache() (pre-existing upstream issue),
	// and -race would attribute it to this test.
	store := chunk.NewCachedStore(mem, chunk.Config{
		BlockSize:         1 << 20,
		CacheDir:          "memory",
		CacheMode:         0600,
		CacheSize:         10 << 20,
		CacheChecksum:     chunk.CsNone,
		CacheScanInterval: time.Second * 300,
		MaxUpload:         1,
		MaxDownload:       200,
		MaxRetries:        10,
		PutTimeout:        time.Second,
		GetTimeout:        time.Second * 2,
		AutoCreate:        true,
		Compress:          "none",
	}, nil)

	bridge, err := chunk.NewGcacheBridge(store)
	if err != nil {
		t.Fatalf("NewGcacheBridge: %v", err)
	}

	// Structural-typing check: *GcacheBridge satisfies ServerSource.
	var src ServerSource = bridge

	key := "1_0_1048576"
	payload := make([]byte, 1048576)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := mem.Put(context.Background(), key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("seed mem storage: %v", err)
	}

	// Missing key → os.ErrNotExist wrap.
	if _, err := bridge.LoadCached(key); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadCached(missing) = %v, want os.ErrNotExist wrap", err)
	}

	// FillFromStorage: fetch via load() (decompress+cache), return reader.
	r, err := bridge.FillFromStorage(context.Background(), key)
	if err != nil {
		t.Fatalf("FillFromStorage: %v", err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || len(got) != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("filled block mismatch: len=%d err=%v", len(got), err)
	}

	// ServeConn must answer a peer request from the local cache now.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		ServeConn(ctx, conn, src, time.Second)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Send one MsgBlockReq for key.
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = MsgBlockReq
	hdr[9], hdr[10] = byte(len(key)), byte(len(key)>>8) // klen little-endian
	hdr[12] = 0                                         // plen little-endian u64 = 0
	if _, err := conn.Write(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(key)); err != nil {
		t.Fatal(err)
	}
	resp := make([]byte, headerLen)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("read resp header: %v", err)
	}
	if resp[3] != MsgBlockResp {
		t.Fatalf("expected MsgBlockResp, got type 0x%x", resp[3])
	}
	if resp[4] != WireRaw {
		t.Fatalf("expected WireRaw for uncompressed volume, got %d", resp[4])
	}
	plen := uint64(resp[12]) | uint64(resp[13])<<8 | uint64(resp[14])<<16 | uint64(resp[15])<<24 |
		uint64(resp[16])<<32 | uint64(resp[17])<<40 | uint64(resp[18])<<48 | uint64(resp[19])<<56
	if plen != uint64(len(payload)) {
		t.Fatalf("plen = %d, want %d", plen, len(payload))
	}
	served := make([]byte, plen)
	if _, err := io.ReadFull(conn, served); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(served, payload) {
		t.Fatal("served bytes differ from seeded payload")
	}
}
