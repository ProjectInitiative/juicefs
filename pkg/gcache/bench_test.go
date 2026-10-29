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

// Protocol-stack microbenchmarks (loopback). For percentile-grade numbers use
// tools/latencybench; these exist for quick before/after sanity in CI.
package gcache

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func benchServer(b *testing.B, blocksize int) (addr string, stop func(), src *fakeSource) {
	b.Helper()
	src = newFakeSource("none")
	block := make([]byte, blocksize)
	for i := range block {
		block[i] = byte(i)
	}
	src.blocks["bench-key"] = block
	a, s := listenerWithSrc(b, src, 30*time.Second)
	return a, s, src
}

func benchRingClient(b *testing.B, addr string) (*RingClient, []Member) {
	b.Helper()
	rc := NewRingClient(5*time.Second, 31, "bench")
	return rc, []Member{{UUID: "peer", Addr: addr}}
}

func benchFetch(b *testing.B, reqLen int) {
	b.Helper()
	addr, stop, _ := benchServer(b, 4<<20)
	defer stop()
	rc, members := benchRingClient(b, addr)
	buf := make([]byte, reqLen)
	b.SetBytes(int64(reqLen))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := rc.Fetch(context.Background(), "g", "bench-key", members)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFetch4KViaFullBlock: a 4 KiB logical read as served by the
// pre-ranged-read protocol: a whole 4 MiB block per request, fresh conn each.
func BenchmarkFetch4KViaFullBlock(b *testing.B) { benchFetch(b, 4096) }

// BenchmarkFetch4M: the same wire path with a full 4 MiB read.
func BenchmarkFetch4M(b *testing.B) { benchFetch(b, 4<<20) }

// BenchmarkFetch4KParallel: 8 workers contending, one fresh conn per op.
func BenchmarkFetch4KParallel(b *testing.B) {
	addr, stop, _ := benchServer(b, 4<<20)
	defer stop()
	rc, members := benchRingClient(b, addr)
	b.SetBytes(4096)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 4096)
		for pb.Next() {
			r, err := rc.Fetch(context.Background(), "g", "bench-key", members)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.ReadFull(r, buf); err != nil {
				b.Fatal(err)
			}
			if err := r.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Round-trip through ServeConn on a raw net.Pipe (no TCP stack): isolates
// frame-loop cost from kernel networking.
func BenchmarkServeConnPipe4M(b *testing.B) {
	src := newFakeSource("none")
	block := make([]byte, 4<<20)
	src.blocks["k"] = block
	c1, c2 := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan struct{})
	go func() { defer close(served); ServeConn(ctx, c2, src, 30*time.Second) }()
	defer func() {
		cancel()
		_ = c1.Close()
		<-served
	}()

	rc := NewRingClient(5*time.Second, 31, "bench")
	buf := make([]byte, 4<<20)
	b.SetBytes(4 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rc.sendReq(context.Background(), c1, "k"); err != nil {
			b.Fatal(err)
		}
		algo, _, plen, isErr, err := rc.readRespHeader(context.Background(), c1)
		if err != nil || isErr {
			b.Fatalf("resp: %v (isErr=%v)", err, isErr)
		}
		if algo != WireRaw || plen != uint64(len(block)) {
			b.Fatalf("bad resp algo=%d plen=%d", algo, plen)
		}
		if err := readFull(c1, buf); err != nil {
			b.Fatal(err)
		}
	}
}
