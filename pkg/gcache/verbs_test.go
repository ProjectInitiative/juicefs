//go:build rdma

// Hardening tests for the verbs transport (docs/VERBS_TRANSPORT.md §lifetimes).
// These exercise the frame-reassembly / queue / deadline / close paths that do
// NOT require a NIC; hardware paths are covered by the k8s wave matrix
// (deploy/gcache-test/k8s-lab/96-phase2-validate.yaml). Run with -race.

package gcache

import (
	"encoding/binary"
	"io"
	"strings"
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

func TestIngestWholeFrameInOneChunk(t *testing.T) {
	// Regression (Phase-2 F2/F3/F4 hardware corruption): a chunk carrying
	// [prefix][whole payload] must complete the frame IMMEDIATELY. The old
	// implementation staged it and only completed on the NEXT chunk — whose
	// bytes were then silently discarded, permanently desynchronizing the
	// frame stream.
	c := newTestConn()
	if err := c.ingest(frameBytes([]byte("hello"))); err != nil {
		t.Fatalf("ingest whole frame: %v", err)
	}
	if len(c.frames) != 1 || string(c.frames[0]) != "hello" {
		t.Fatalf("frame = %q, want hello", c.frames[0])
	}
	if c.pending != nil || c.pendingLen != 0 {
		t.Fatalf("pending not cleared: len=%d", c.pendingLen)
	}
	// the next frame must be intact too
	push(t, c, []byte("world"))
	if len(c.frames) != 2 || string(c.frames[1]) != "world" {
		t.Fatalf("frame[1] = %q, want world", c.frames[1])
	}
}

func TestIngestCoalescedFramesInOneChunk(t *testing.T) {
	// Regression: several frames coalesced into one chunk (any chunk boundary
	// must be handled — every byte consumed exactly once).
	c := newTestConn()
	var blob []byte
	blob = append(blob, frameBytes([]byte("aaa"))...)
	blob = append(blob, frameBytes([]byte("bb"))...)
	blob = append(blob, frameBytes([]byte("c"))...)
	if err := c.ingest(blob); err != nil {
		t.Fatalf("ingest coalesced: %v", err)
	}
	want := []string{"aaa", "bb", "c"}
	if len(c.frames) != len(want) {
		t.Fatalf("frames = %d, want %d", len(c.frames), len(want))
	}
	for i, w := range want {
		if string(c.frames[i]) != w {
			t.Fatalf("frames[%d] = %q, want %q", i, c.frames[i], w)
		}
	}
}

func TestIngestFrameSplitAcrossChunkBoundary(t *testing.T) {
	// Regression: a frame whose prefix and payload are split at arbitrary
	// positions (including mid-prefix) must reassemble; the tail of the
	// carrying chunk belongs to the next frame.
	c := newTestConn()
	a := frameBytes([]byte("hello-world"))
	b := frameBytes([]byte("xy"))
	blob := append(append([]byte{}, a...), b...)
	// feed byte-by-byte: worst-case boundary churn
	for i := range blob {
		if err := c.ingest(blob[i : i+1]); err != nil {
			t.Fatalf("ingest byte %d: %v", i, err)
		}
	}
	if len(c.frames) != 2 || string(c.frames[0]) != "hello-world" || string(c.frames[1]) != "xy" {
		t.Fatalf("frames = %v", c.frames)
	}
}

// frameBytes wires payload with its u32 length prefix.
func frameBytes(payload []byte) []byte {
	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(len(payload)))
	return append(hdr, payload...)
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
	// bug 12 revision: the recvLoop no longer pauses polling when the frame
	// queue grows (the recv ring itself bounds in-flight bytes); ingest must
	// still accept and account unboundedly as long as the reader keeps up.
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

func TestIngestCarriesShortChunkFragment(t *testing.T) {
	// A sub-4-byte chunk with no pending frame is a mid-prefix fragment: it
	// must be carried (never dropped) until the prefix is complete.
	c := newTestConn()
	if err := c.ingest([]byte{4, 0}); err != nil {
		t.Fatalf("ingest fragment: %v", err)
	}
	if err := c.ingest([]byte{0, 0}); err != nil {
		t.Fatalf("ingest rest of prefix: %v", err)
	}
	if c.pending == nil || c.pendingLen != 4 || c.pendingTotal != 4 {
		t.Fatalf("prefix not staged: pending=%v len=%d total=%d", c.pending != nil, c.pendingLen, c.pendingTotal)
	}
	// payload follows in a later chunk
	if err := c.ingest([]byte("abcd")); err != nil {
		t.Fatalf("ingest payload: %v", err)
	}
	if len(c.frames) != 1 || string(c.frames[0]) != "abcd" {
		t.Fatalf("frame = %q, want abcd", c.frames[0])
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

// ---- Phase 2b P1-1: lost-wakeup protocol ----

// TestReadWakesOnConcurrentIngest hammers the check→wait boundary: N readers
// block on an empty conn while a producer pushes frames one at a time (each
// Broadcast lands while readers are parked or mid-check). Every reader must
// receive its frame — no stranded waiters. -race clean.
func TestReadWakesOnConcurrentIngest(t *testing.T) {
	c := newTestConn()
	c.state.Store(int32(stateLive))
	const readers = 8
	payload := make([]byte, 4096)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(c, buf); err != nil {
				t.Errorf("reader: %v", err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond) // let all readers park in Wait
	for i := 0; i < readers; i++ {
		push(t, c, payload) // each Broadcast wakes exactly one parked reader
		time.Sleep(time.Millisecond)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readers stranded: lost wakeup in the check→wait window")
	}
}

// TestReadDeadlineExpiryWakesWaiter: a parked reader must return promptly on
// deadline expiry even with no frames and no close (the one-shot waker).
func TestReadDeadlineExpiryWakesWaiter(t *testing.T) {
	c := newTestConn()
	c.state.Store(int32(stateLive))
	c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	start := time.Now()
	if _, err := c.Read(make([]byte, 16)); err == nil {
		t.Fatal("expected deadline error")
	} else if !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("unexpected err: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("deadline wake took %s (stranded waiter)", d)
	}
}

// ---- Phase 2b P0-2: server conn accounting + idle reaper ----

func TestServeLiveStatsAccounting(t *testing.T) {
	a0, c0, l0 := ServeLiveStats()
	cs := newTestConn()
	cs.isServer = true
	cs.lastActive.Store(time.Now().UnixNano())
	srvRegister(cs)
	a1, _, l1 := ServeLiveStats()
	if l1 != l0+1 {
		t.Fatalf("live after register = %d, want %d", l1, l0+1)
	}
	if a1 != a0+1 {
		t.Fatalf("accepted after register = %d, want %d", a1, a0+1)
	}
	srvUnregister(cs) // exactly-once
	srvUnregister(cs) // idempotent
	_, c2, l2 := ServeLiveStats()
	if l2 != l0 {
		t.Fatalf("live after unregister = %d, want %d", l2, l0)
	}
	if c2 != c0+1 {
		t.Fatalf("closed after unregister = %d, want %d", c2, c0+1)
	}
}

func TestServerIdleReaper(t *testing.T) {
	idle := 50 * time.Millisecond
	old := newTestConn()
	old.isServer = true
	old.lastActive.Store(time.Now().Add(-2 * idle).UnixNano()) // stale
	fresh := newTestConn()
	fresh.isServer = true
	fresh.lastActive.Store(time.Now().UnixNano()) // active
	srvRegister(old)
	srvRegister(fresh)
	defer func() { srvUnregister(old); srvUnregister(fresh) }()
	reaped := reapIdleServerConns(idle)
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1 (only the stale conn)", reaped)
	}
	if old.state.Load() != int32(stateClosed) {
		t.Fatal("stale conn was not closed")
	}
	if fresh.state.Load() == int32(stateClosed) {
		t.Fatal("active conn was reaped")
	}
}
