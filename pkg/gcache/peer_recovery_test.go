package gcache

import (
	"errors"
	"io"
	"testing"
)

// brokenAfter returns a ReadCloser that yields n bytes then EOFs prematurely
// (simulating a peer stream aborted mid-payload).
func brokenAfter(b []byte, n int) io.ReadCloser {
	return &truncatedReader{data: b, limit: n}
}

type truncatedReader struct {
	data  []byte
	limit int
	off   int
}

func (t *truncatedReader) Read(p []byte) (int, error) {
	if t.off >= t.limit {
		return 0, io.EOF
	}
	n := copy(p, t.data[t.off:t.limit])
	t.off += n
	return n, nil
}
func (t *truncatedReader) Close() error { return nil }

// bytesReaderCloser is io.NopCloser with an exact-type helper for tests.
func bytesReaderCloser(b []byte) io.ReadCloser { return io.NopCloser(newSliceReader(b)) }

type sliceReader struct{ b []byte }

func newSliceReader(b []byte) *sliceReader { return &sliceReader{b: b} }

func (s *sliceReader) Read(p []byte) (int, error) {
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.b)
	s.b = s.b[n:]
	return n, nil
}
func (s *sliceReader) Close() error { return nil }

// TestPeerReaderHealsMidStreamEOF: a peer payload truncated before plen must
// be transparently refilled from object storage (skipping consumed bytes).
func TestPeerReaderHealsMidStreamEOF(t *testing.T) {
	full := []byte("0123456789ABCDEF") // 16-byte block payload
	refills := 0
	r := &peerReader{
		cur:   brokenAfter(full, 5), // stream dies after 5 bytes
		total: int64(len(full)),
		refill: func(consumed int64) (io.ReadCloser, error) {
			refills++
			if consumed != 5 {
				t.Fatalf("refill consumed = %d, want 5", consumed)
			}
			// Mirror the real refill: skip the already-consumed prefix.
			return bytesReaderCloser(full[consumed:]), nil
		},
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(out) != string(full) {
		t.Fatalf("healed read mismatch: got %q, want %q", out, full)
	}
	if refills != 1 {
		t.Fatalf("refills = %d, want 1", refills)
	}
}

// TestPeerReaderSecondBreakSurfaces: after the single refill, a second
// mid-stream breakage must surface as ErrUnexpectedEOF instead of looping or
// silently truncating.
func TestPeerReaderSecondBreakSurfaces(t *testing.T) {
	r := &peerReader{
		cur:   brokenAfter([]byte("AAAA"), 2),
		total: 16,
		refill: func(consumed int64) (io.ReadCloser, error) {
			return brokenAfter([]byte("BBBB"), 1), nil // dies again immediately
		},
	}
	_, err := io.ReadAll(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF after second break, got %v", err)
	}
}

// TestPeerReaderCleanEOF: full payload EOFs exactly at plen — no refill.
func TestPeerReaderCleanEOF(t *testing.T) {
	called := false
	r := &peerReader{
		cur:   bytesReaderCloser([]byte("xy")),
		total: 2,
		refill: func(int64) (io.ReadCloser, error) {
			called = true
			return nil, errors.New("should not refill")
		},
	}
	out, err := io.ReadAll(r)
	if err != nil || string(out) != "xy" {
		t.Fatalf("clean read failed: %v %q", err, out)
	}
	if called {
		t.Fatal("refill called on clean stream")
	}
}

// TestNoLocalCacheMarkers: all non-owner readers carry the structural marker
// pkg/chunk uses to skip persistent caching.
func TestNoLocalCacheMarkers(t *testing.T) {
	var _ interface{ NoLocalCache() } = &payloadReader{}
	var _ interface{ NoLocalCache() } = &peerReader{}
	var _ interface{ NoLocalCache() } = noCacheReader{}
}
