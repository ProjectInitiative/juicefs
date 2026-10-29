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
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// Two sequential Fetches must reuse one connection (1 dial, 1 reuse).
func TestPoolReuseAcrossFetches(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = []byte("hello pooled world")
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	for i := 0; i < 3; i++ {
		r, err := rc.Fetch(context.Background(), "g", "k", members)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "hello pooled world" {
			t.Fatalf("payload mismatch: %q", b)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	dials, reuses, idle := rc.pool.stats()
	if dials != 1 {
		t.Fatalf("want 1 dial, got %d", dials)
	}
	if reuses != 2 {
		t.Fatalf("want 2 reuses, got %d", reuses)
	}
	if idle != 1 {
		t.Fatalf("want 1 idle conn, got %d", idle)
	}
}

// Abandoning a partially-read payload (close with big remainder) must NOT
// poison the pool: the conn is closed, the next fetch dials fresh.
func TestPoolDiscardOnPartialRead(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = make([]byte, 4<<20) // > maxDrain remainder when abandoned
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	r, err := rc.Fetch(context.Background(), "g", "k", members)
	if err != nil {
		t.Fatal(err)
	}
	// read 1 byte, then close with ~4MiB unconsumed
	var one [1]byte
	if _, err := r.Read(one[:]); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// next fetch must succeed (fresh conn)
	r2, err := rc.Fetch(context.Background(), "g", "k", members)
	if err != nil {
		t.Fatalf("fetch after discard: %v", err)
	}
	if _, err := io.ReadAll(r2); err != nil {
		t.Fatal(err)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	dials, _, _ := rc.pool.stats()
	if dials != 2 {
		t.Fatalf("want 2 dials (discard forced redial), got %d", dials)
	}
}

// An idle conn past the TTL is retired lazily and a fresh dial happens.
func TestPoolIdleTTL(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = []byte("x")
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(time.Second, 31, "self-test")
	rc.pool.ttl = time.Millisecond // shrink for the test
	members := []Member{{UUID: "peer-1", Addr: addr}}
	for i := 0; i < 2; i++ {
		r, err := rc.Fetch(context.Background(), "g", "k", members)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	dials, reuses, _ := rc.pool.stats()
	if dials != 2 || reuses != 0 {
		t.Fatalf("want ttl expiry to force redial (dials=2 reuses=0), got dials=%d reuses=%d", dials, reuses)
	}
}

// Concurrency: N parallel fetches over one peer share the pool without
// deadlock and all payloads are correct.
func TestPoolConcurrent(t *testing.T) {
	src := newFakeSource("none")
	src.blocks["k"] = make([]byte, 256<<10)
	addr, stop := listenerWithSrc(t, src, 5*time.Second)
	defer stop()

	rc := NewRingClient(2*time.Second, 31, "self-test")
	members := []Member{{UUID: "peer-1", Addr: addr}}
	var okCnt atomic.Int64
	done := make(chan struct{})
	for i := 0; i < 16; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 10; j++ {
				r, err := rc.Fetch(context.Background(), "g", "k", members)
				if err != nil {
					t.Error(err)
					return
				}
				b, err := io.ReadAll(r)
				if err != nil {
					t.Error(err)
					return
				}
				if err := r.Close(); err != nil {
					t.Error(err)
					return
				}
				if len(b) != 256<<10 {
					t.Errorf("short payload %d", len(b))
					return
				}
				okCnt.Add(1)
			}
		}()
	}
	for i := 0; i < 16; i++ {
		<-done
	}
	if okCnt.Load() != 160 {
		t.Fatalf("want 160 ok, got %d", okCnt.Load())
	}
}
