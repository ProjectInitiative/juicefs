package gcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Push stores the block on the owner (verified via fakeSource counters).
func TestPushStoresOnOwner(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	rc := NewRingClient(time.Second, 31, "self-uuid")
	rc.SetKick(func() {})
	members := []Member{{UUID: "owner-1", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	// find a key owned by owner-1 (not self)
	key := ""
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("chunks/0/0/%d_0_4194304", i)
		if Owners(k, members, 1)[0].UUID == "owner-1" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no owner-1 key found in 1000 tries")
	}
	rc.Push(context.Background(), "g", key, []byte("pushed-bytes"), members)
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.pushCnt[key] != 1 {
		t.Fatalf("push count = %d, want 1", src.pushCnt[key])
	}
	if string(src.blocks[key]) != "pushed-bytes" {
		t.Fatalf("stored bytes = %q", src.blocks[key])
	}
}

// Push to a self-owned key is a no-op (no network).
func TestPushSelfOwnedNoop(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	rc := NewRingClient(time.Second, 31, "owner-1")
	members := []Member{{UUID: "owner-1", Addr: addr}}
	// find a key whose owner is member 0 (self-uuid matches) — any key with
	// a single member is self-owned, so Push must skip dialing.
	rc.Push(context.Background(), "g", "chunks/0/0/1_0_4194304", []byte("x"), members)
	time.Sleep(50 * time.Millisecond)
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.pushCnt) != 0 {
		t.Fatal("self-owned push must not reach any server")
	}
}

// Drop broadcast evicts the key on every member.
func TestDropBroadcast(t *testing.T) {
	srcA := newFakeSource("none")
	srcB := newFakeSource("none")
	addrA, stopA := listenerWithSrc(t, srcA, time.Second)
	defer stopA()
	addrB, stopB := listenerWithSrc(t, srcB, time.Second)
	defer stopB()
	key := "chunks/0/0/2_0_4194304"
	srcA.blocks[key] = []byte("a")
	srcB.blocks[key] = []byte("b")
	rc := NewRingClient(time.Second, 31, "self-uuid")
	members := []Member{{UUID: "a", Addr: addrA}, {UUID: "b", Addr: addrB}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	rc.Drop(context.Background(), "g", key, members)
	srcA.mu.Lock()
	droppedA := srcA.dropCnt[key]
	_, existsA := srcA.blocks[key]
	srcA.mu.Unlock()
	srcB.mu.Lock()
	droppedB := srcB.dropCnt[key]
	_, existsB := srcB.blocks[key]
	srcB.mu.Unlock()
	if droppedA != 1 || existsA {
		t.Fatalf("A: dropCnt=%d exists=%v", droppedA, existsA)
	}
	if droppedB != 1 || existsB {
		t.Fatalf("B: dropCnt=%d exists=%v", droppedB, existsB)
	}
}

// Oversized push is rejected with an error frame.
func TestPushOversizedRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates maxBlockPayload")
	}
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	rc := NewRingClient(time.Second, 31, "self-uuid")
	members := []Member{{UUID: "owner-1", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	big := make([]byte, maxBlockPayload+1)
	key := ""
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("chunks/0/0/%d_0_4194304", i)
		if Owners(k, members, 1)[0].UUID == "owner-1" {
			key = k
			break
		}
	}
	rc.Push(context.Background(), "g", key, big, members)
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.pushCnt[key] != 0 {
		t.Fatal("oversized push must be rejected")
	}
}
