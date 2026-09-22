package gcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// recordingInner counts Put/Delete calls and records bytes written.
type recordingInner struct {
	object.ObjectStorage
	mu     sync.Mutex
	puts   map[string][]byte
	dels   map[string]int
	putErr error
	delErr error
}

func newRecordingInner() *recordingInner {
	return &recordingInner{puts: make(map[string][]byte), dels: make(map[string]int)}
}

func (r *recordingInner) Put(_ context.Context, key string, in io.Reader, _ ...object.AttrGetter) error {
	if r.putErr != nil {
		return r.putErr
	}
	b, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.puts[key] = b
	r.mu.Unlock()
	return nil
}

func (r *recordingInner) Delete(_ context.Context, key string, _ ...object.AttrGetter) error {
	if r.delErr != nil {
		return r.delErr
	}
	r.mu.Lock()
	r.dels[key]++
	r.mu.Unlock()
	return nil
}

func (r *recordingInner) Get(_ context.Context, key string, _, _ int64, _ ...object.AttrGetter) (io.ReadCloser, error) {
	r.mu.Lock()
	b, ok := r.puts[key]
	r.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

var errNoPush = errors.New("no push expected")

// findOwnedKey returns a key owned by wantUUID given members.
func findOwnedKey(members []Member, wantUUID string) string {
	for i := 0; i < 10000; i++ {
		k := "chunks/0/0/" + itoa(i) + "_0_4194304"
		if Owners(k, members, 1)[0].UUID == wantUUID {
			return k
		}
	}
	return ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// Put with fillOnUpload stores via inner and pushes to the owner.
func TestDecoratorPutPushesToOwner(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	members := []Member{{UUID: "peer", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	key := findOwnedKey(members, "peer")
	if key == "" {
		t.Fatal("no peer-owned key")
	}
	inner := newRecordingInner()
	rc := NewRingClient(time.Second, 31, "self-uuid")
	rs := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid").(*ringStorage)
	rs.fillOnUpload = true

	if err := rs.Put(context.Background(), key, bytes.NewReader([]byte("block-bytes"))); err != nil {
		t.Fatal(err)
	}
	inner.mu.Lock()
	written := string(inner.puts[key])
	inner.mu.Unlock()
	if written != "block-bytes" {
		t.Fatalf("inner got %q", written)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		src.mu.Lock()
		got, cnt := src.blocks[key], src.pushCnt[key]
		src.mu.Unlock()
		if cnt == 1 && string(got) == "block-bytes" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("push never arrived at owner")
}

// Put without fillOnUpload never buffers or pushes.
func TestDecoratorPutPassthrough(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	members := []Member{{UUID: "peer", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	key := findOwnedKey(members, "peer")
	inner := newRecordingInner()
	rc := NewRingClient(time.Second, 31, "self-uuid")
	rs := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid").(*ringStorage)

	if err := rs.Put(context.Background(), key, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.pushCnt) != 0 {
		t.Fatal("passthrough Put must not push")
	}
}

// inner.Put failure suppresses the push.
func TestDecoratorPutFailureNoPush(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	members := []Member{{UUID: "peer", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	key := findOwnedKey(members, "peer")
	inner := newRecordingInner()
	inner.putErr = errors.New("storage down")
	rc := NewRingClient(time.Second, 31, "self-uuid")
	rs := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid").(*ringStorage)
	rs.fillOnUpload = true

	if err := rs.Put(context.Background(), key, bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("want inner.Put error")
	}
	time.Sleep(50 * time.Millisecond)
	src.mu.Lock()
	defer src.mu.Unlock()
	if len(src.pushCnt) != 0 {
		t.Fatal("failed Put must not push")
	}
}

// Delete broadcasts a drop to members.
func TestDecoratorDeleteBroadcasts(t *testing.T) {
	src := newFakeSource("none")
	addr, stop := listenerWithSrc(t, src, time.Second)
	defer stop()
	members := []Member{{UUID: "peer", Addr: addr}, {UUID: "self-uuid", Addr: "127.0.0.1:1"}}
	key := "chunks/0/0/7_1_4194304"
	src.blocks[key] = []byte("stale")
	inner := newRecordingInner()
	rc := NewRingClient(time.Second, 31, "self-uuid")
	rs := NewRingStorage(inner, rc, func(string) []Member { return members }, "self-uuid").(*ringStorage)

	if err := rs.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		src.mu.Lock()
		cnt := src.dropCnt[key]
		_, exists := src.blocks[key]
		src.mu.Unlock()
		if cnt == 1 && !exists {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("drop never arrived")
}
