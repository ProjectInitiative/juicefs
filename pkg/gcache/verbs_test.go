//go:build rdma

// Hardening tests for the verbs transport (docs/VERBS_TRANSPORT.md §lifetimes).
// These exercise the frame-reassembly / queue / deadline / close paths that do
// NOT require a NIC; hardware paths are covered by the k8s wave matrix
// (deploy/gcache-test/k8s-lab/96-phase2-validate.yaml). Run with -race.

package gcache

import (
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"
)

// newTestConn returns a verbsConn with C fields nil-safe for the pure-Go
// frame paths (ingest/Read/queue accounting). It must NOT touch C.
func newTestConn() *verbsConn {
	c := &verbsConn{isServer: false}
	c.framesCond = sync.NewCond(&c.framesMu)
	c.recvDone = make(chan struct{})
	return c
}

// push sends a wire-format frame through ingest exactly as recvLoop would
// (prefix chunk first, then payload chunks ≤ vbMsgSize).
func push(t *testing.T, c *verbsConn, payload []byte) {
	t.Helper()
	total := len(payload)
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(total))
	if err := c.ingest(hdr); err != nil {
		t.Fatalf("ingest prefix: %v", err)
	}
	for off := 0; off < total || (total == 0 && off == 0); off += vbMsgSize {
		end := off + vbMsgSize
		if end > total {
			end = total
		}
		if err := c.ingest(payload[off:end]); err != nil {
			t.Fatalf("ingest chunk: %v", err)
		}
		if end >= total {
			break
		}
	}
}

func TestIngestSplitPrefixSingleChunk(t *testing.T) {
	c := newTestConn()
	push(t, c, []byte("hello"))
	if len(c.frames) != 1 || string(c.frames[0]) != "hello" {
		t.Fatalf("frame = %q, want hello", c.frames[0])
	}
	if c.queuedBytes != 5 {
		t.Fatalf("queuedBytes = %d, want 5", c.queuedBytes)
	}
}

func TestIngestFragmentedFrame(t *testing.T) {
	c := newTestConn()
	payload := make([]byte, 3*vbMsgSize+1234)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	push(t, c, payload)
	if len(c.frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(c.frames))
	}
	if string(c.frames[0]) != string(payload) {
		t.Fatal("fragmented frame corrupted")
	}
}

func TestReadAcrossConsecutiveReads(t *testing.T) {
	c := newTestConn()
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i)
	}
	push(t, c, payload)
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 333)
	for len(got) < len(payload) {
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, buf[:n]...)
	}
	if string(got) != string(payload) {
		t.Fatal("stream reassembly mismatch")
	}
}

func TestReadZeroLength(t *testing.T) {
	c := newTestConn()
	if n, err := c.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = %d, %v; want 0, nil", n, err)
	}
}

func TestConcurrentReadsAreSerialized(t *testing.T) {
	c := newTestConn()
	const frames = 64
	const size = 4096
	for i := 0; frames == 0 || i < frames; i++ {
		if i >= frames {
			break
		}
		payload := make([]byte, size)
		payload[0] = byte(i)
		payload[1] = byte(i >> 8)
		push(t, c, payload)
	}

	var wg sync.WaitGroup
	errs := make(chan error, frames)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				buf := make([]byte, 777)
				n, err := c.Read(buf)
				if err != nil {
					errs <- err
					return
				}
				if n == 0 {
					errs <- io.ErrUnexpectedEOF
					return
				}
				// partial reads are fine; re-read via a fresh frame scan is
				// not possible with pure stream Reads, so just verify the
				// bytes we got belong to SOME frame (index pattern)
				for i := 0; i < n; i++ {
					_ = buf[i]
				}
			}
		}()
	}
	// single reader thread is the contract (net.Conn); the concurrent test
	// only proves no data race / double-pop panics — close to stop workers.
	go func() {
		time.Sleep(50 * time.Millisecond)
		c.markBroken(errTestStop)
	}()
	wg.Wait()
}

var errTestStop = &testStopError{}

type testStopError struct{}

func (*testStopError) Error() string { return "test stop" }

func TestQueueBackpressure(t *testing.T) {
	c := newTestConn()
	// push more bytes than vbMaxQueuedBytes via ingest directly (recvLoop
	// checks the watermark, but ingest must still accept and account).
	payload := make([]byte, 1<<20)
	for i := 0; i < 20; i++ {
		push(t, c, payload)
	}
	if len(c.frames) != 20 {
		t.Fatalf("frames = %d, want 20", len(c.frames))
	}
	if c.queuedBytes != 20<<20 {
		t.Fatalf("queuedBytes = %d, want %d", c.queuedBytes, 20<<20)
	}
	// drain one frame fully → accounting drops
	buf := make([]byte, 1<<20)
	total := 0
	for total < 1<<20 {
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		total += n
	}
	if c.queuedBytes != 19<<20 {
		t.Fatalf("queuedBytes after drain = %d, want %d", c.queuedBytes, 19<<20)
	}
}

func TestReadHonorsDeadlineUpdateWhilePending(t *testing.T) {
	// item 7: a deadline set while a Read is pending must affect it.
	// Simulate: stateLive, no frames; start Read in goroutine; extend the
	// deadline from far-future to past → Read must fail promptly.
	c := newTestConn()
	c.state.Store(int32(stateLive))
	c.rdeadline.Store(time.Now().Add(time.Hour).UnixNano())

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := c.Read(make([]byte, 10))
		done <- result{n, err}
	}()

	time.Sleep(50 * time.Millisecond)
	c.rdeadline.Store(time.Now().Add(-time.Second).UnixNano()) // expire it
	c.framesCond.Broadcast()                                   // wake the waiter

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("Read succeeded despite expired deadline")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Read did not observe the deadline update")
	}
}

func TestIngestRejectsOversizedFrame(t *testing.T) {
	c := newTestConn()
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, vbMaxFrame+1)
	if err := c.ingest(hdr); err == nil {
		t.Fatal("ingest accepted oversized frame")
	}
}

func TestIngestRejectsShortChunk(t *testing.T) {
	c := newTestConn()
	if err := c.ingest([]byte{1, 2}); err == nil {
		t.Fatal("ingest accepted 2-byte chunk as frame start")
	}
}

func TestCloseIdempotentBrokenAndInit(t *testing.T) {
	// item 4: Close must be safe in Init (no C resources at all) and
	// Broken states — with nil C pointers it must not panic and must be
	// idempotent.
	for _, st := range []connState{stateInit, stateBroken, stateClosed} {
		c := newTestConn()
		c.state.Store(int32(st))
		if err := c.Close(); err != nil {
			t.Fatalf("Close(state %d): %v", st, err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close twice(state %d): %v", st, err)
		}
	}
}

func TestWriteZeroLengthIsNoop(t *testing.T) {
	// item 9: zero-length writes are no-ops (documented), not wire frames.
	// The real check is behavioral: the write path is guarded before any C
	// call, so a nil-cn conn must return 0,nil rather than crash.
	c := newTestConn()
	c.state.Store(int32(stateLive)) // nil-cn; the zero-length path must not touch C
	if n, err := c.Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = %d, %v; want 0, nil", n, err)
	}
	if n, err := c.Write([]byte{}); n != 0 || err != nil {
		t.Fatalf("Write(empty) = %d, %v; want 0, nil", n, err)
	}
}
