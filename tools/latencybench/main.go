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

// latencybench — reproducible low-overhead latency harness for the gcache
// fast-path work (docs/LOW_LATENCY_CACHE_REVIEW.md §3/§5).
//
// It never substitutes mocks for the real stack: `peer` mode drives the
// production RingClient (pooling, protocol, failover) against a real peer
// server; `serve` runs the production ServeConn frame loop over an explicit
// data source (RAM, or a file pread — optionally O_DIRECT, the owner-NVMe
// model). `fuse`/`nvme` modes measure pread() with warm/cold page-cache
// control via fadvise.
//
// Build:  go build -o latencybench ./tools/latencybench
// Serve:  ./latencybench serve --addr 127.0.0.1:0 --blocks 256 --blocksize 4194304
// Read:   ./latencybench read --backend peer --addr 127.0.0.1:PORT --len 4096 --qd 8 --secs 5
//
// Results: JSON (--out) + human table on stdout. Never label kernel
// page-cache results as remote-NVMe latency; see the README for the matrix.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/juicedata/juicefs/pkg/gcache"
	"github.com/juicedata/juicefs/pkg/readsvc"
)

// ---------------------------------------------------------------- results

type latencyStats struct {
	N      int64   `json:"n"`
	MeanUs float64 `json:"mean_us"`
	P50    float64 `json:"p50_us"`
	P90    float64 `json:"p90_us"`
	P95    float64 `json:"p95_us"`
	P99    float64 `json:"p99_us"`
	P999   float64 `json:"p999_us"`
	Max    float64 `json:"max_us"`
}

func summarize(us []float64) latencyStats {
	if len(us) == 0 {
		return latencyStats{}
	}
	s := append([]float64(nil), us...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	pick := func(q float64) float64 { return s[int(q*float64(len(s)-1))] }
	return latencyStats{
		N: int64(len(s)), MeanUs: sum / float64(len(s)),
		P50: pick(0.50), P90: pick(0.90), P95: pick(0.95),
		P99: pick(0.99), P999: pick(0.999), Max: s[len(s)-1],
	}
}

type runResult struct {
	Backend    string       `json:"backend"`
	Addr       string       `json:"addr,omitempty"`
	Path       string       `json:"path,omitempty"`
	Range      bool         `json:"range,omitempty"`
	Len        int          `json:"len_bytes"`
	QD         int          `json:"queue_depth"`
	Batch      int          `json:"batch"`
	Seconds    float64      `json:"seconds"`
	Direct     bool         `json:"o_direct,omitempty"`
	Dropped    bool         `json:"page_cache_dropped,omitempty"`
	UniqueKeys bool         `json:"unique_keys,omitempty"`
	Read       latencyStats `json:"read_latency"`
	BatchDone  latencyStats `json:"batch_completion,omitempty"`
	IOPS       float64      `json:"iops"`
	MiBS       float64      `json:"mibs_per_s"`
	Errors     int64        `json:"errors"`
	Transport  string       `json:"transport"`
}

var errRangeUnsupported = errors.New("--range needs a build with ranged peer reads (MsgRangeReq)")

var _ = errRangeUnsupported // referenced until wired (kept for build ordering)

func splitInts(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			fatalf("bad int %q: %v", f, err)
		}
		out = append(out, n)
	}
	return out
}

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "latencybench: "+f+"\n", args...)
	os.Exit(1)
}

// alignedBuf returns a 4096-aligned buffer of n bytes (O_DIRECT requirement).
func alignedBuf(n int) []byte {
	b := make([]byte, n+4096)
	base := int(uintptr(unsafe.Pointer(&b[0])))
	addr := (base + 4095) &^ 4095
	return b[addr-base : addr-base+n]
}

// ---------------------------------------------------------------- sources

// countSource wraps a ServerSource with warm/cold counters for periodic logs.
type countSource struct {
	inner gcache.ServerSource
	load  atomic.Int64 // LoadCached hits (warm)
	fill  atomic.Int64 // FillFromStorage (cold: owner had to fetch)
}

func (c *countSource) LoadCached(key string) (io.ReadCloser, error) {
	c.load.Add(1)
	return c.inner.LoadCached(key)
}
func (c *countSource) FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error) {
	c.fill.Add(1)
	return c.inner.FillFromStorage(ctx, key)
}
func (c *countSource) CompressAlgo() string                     { return c.inner.CompressAlgo() }
func (c *countSource) CompressBound(n int) int                  { return c.inner.CompressBound(n) }
func (c *countSource) CompressPayload(d, s []byte) (int, error) { return c.inner.CompressPayload(d, s) }
func (c *countSource) StorePushed(ctx context.Context, k string, d []byte) error {
	return c.inner.StorePushed(ctx, k, d)
}
func (c *countSource) DropCached(key string) error { return c.inner.DropCached(key) }

// LoadCachedRange forwards the optional RangeSource when the inner source
// implements it; otherwise the ranged request is answered "not cached" and
// the owner-side fill path applies.
func (c *countSource) LoadCachedRange(key string, off, limit int64) (io.ReadCloser, error) {
	rs, ok := c.inner.(interface {
		LoadCachedRange(string, int64, int64) (io.ReadCloser, error)
	})
	if !ok {
		return nil, os.ErrNotExist
	}
	return rs.LoadCachedRange(key, off, limit)
}

type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

// memSource serves preloaded blocks from RAM (pure protocol/transport cost).
// Cold fills are synthesized and kept under a cap (oldest evicted, preloaded
// keys exempt) so --unique cold-owner runs cannot grow without bound.
type memSource struct {
	mu        sync.Mutex
	blocks    map[string][]byte
	algo      string
	preloaded map[string]bool
	order     []string // fill order, for eviction
	cap       int
}

func newMemSource(blocks, blocksize int) *memSource {
	m := &memSource{
		blocks:    make(map[string][]byte, blocks),
		algo:      "none",
		preloaded: make(map[string]bool, blocks),
		cap:       blocks * 2,
	}
	for i := 0; i < blocks; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		b := make([]byte, blocksize)
		for j := range b {
			b[j] = byte(i + j) // non-degenerate content
		}
		m.blocks[key] = b
		m.preloaded[key] = true
	}
	return m
}

func (m *memSource) LoadCached(key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.blocks[key]; ok {
		return io.NopCloser(&byteReader{b: b}), nil
	}
	return nil, os.ErrNotExist
}
func (m *memSource) FillFromStorage(_ context.Context, key string) (io.ReadCloser, error) {
	// Model "object storage has it, owner didn't": synthesize deterministic
	// content on fill (the owner's cache then serves it warm). Holds m.mu
	// throughout; do NOT call the locking LoadCached from here (sync.Mutex
	// is not reentrant).
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.blocks[key]; !ok {
		b := make([]byte, 4<<20)
		h := uint64(14695981039346656037)
		for _, c := range []byte(key) {
			h = (h ^ uint64(c)) * 1099511628211
		}
		for i := range b {
			h = h*6364136223846793005 + 1442695040888963407
			b[i] = byte(h >> 33)
		}
		m.blocks[key] = b
		m.order = append(m.order, key)
		for len(m.order) > m.cap { // evict oldest fills
			old := m.order[0]
			m.order = m.order[1:]
			if !m.preloaded[old] {
				delete(m.blocks, old)
			}
		}
	}
	b := m.blocks[key]
	return io.NopCloser(&byteReader{b: b}), nil
}

// LoadCachedRange: positional window over the RAM block (RangeSource).
func (m *memSource) LoadCachedRange(key string, off, limit int64) (io.ReadCloser, error) {
	b, ok := m.blocks[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if off >= int64(len(b)) {
		return nil, fmt.Errorf("offset %d past end of %s (%d bytes)", off, key, len(b))
	}
	end := off + limit
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	return io.NopCloser(&byteReader{b: b[off:end]}), nil
}
func (m *memSource) CompressAlgo() string                     { return m.algo }
func (m *memSource) CompressBound(n int) int                  { return n }
func (m *memSource) CompressPayload(d, s []byte) (int, error) { return copy(d, s), nil }
func (m *memSource) StorePushed(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks[key] = append([]byte(nil), data...)
	return nil
}
func (m *memSource) DropCached(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blocks, key)
	return nil
}

// fileSource serves a real file's ranges via pread — models an owner's
// NVMe-resident block. With O_DIRECT it is genuine device IO; otherwise the
// kernel page cache serves (labeled in results).
type fileSource struct {
	f    *os.File
	size int64
}

func (s *fileSource) preadAt(p []byte, off int64) error {
	for len(p) > 0 {
		n, err := s.f.ReadAt(p, off)
		off += int64(n)
		p = p[n:]
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *fileSource) LoadCached(_ string) (io.ReadCloser, error) {
	b := make([]byte, s.size)
	if err := s.preadAt(b, 0); err != nil {
		return nil, err
	}
	return io.NopCloser(&byteReader{b: b}), nil
}
func (s *fileSource) FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.LoadCached(key)
}

// LoadCachedRange: positional pread — the owner-NVMe model (RangeSource).
func (s *fileSource) LoadCachedRange(key string, off, limit int64) (io.ReadCloser, error) {
	if off >= s.size {
		return nil, fmt.Errorf("offset %d past end of %s (%d bytes)", off, key, s.size)
	}
	n := limit
	if off+n > s.size {
		n = s.size - off
	}
	buf := make([]byte, n)
	if err := s.preadAt(buf, off); err != nil {
		return nil, err
	}
	return io.NopCloser(&byteReader{b: buf}), nil
}
func (s *fileSource) CompressAlgo() string                     { return "none" }
func (s *fileSource) CompressBound(n int) int                  { return n }
func (s *fileSource) CompressPayload(d, s2 []byte) (int, error) { return copy(d, s2), nil }
func (s *fileSource) StorePushed(_ context.Context, _ string, _ []byte) error { return nil }
func (s *fileSource) DropCached(_ string) error                { return nil }

// ---------------------------------------------------------------- serve

func cmdServe(args []string) {
	fs := newFlagSet("serve")
	addr := fs.String("addr", "127.0.0.1:0", "listen address")
	src := fs.String("src", "mem", "data source: mem | file")
	file := fs.String("file", "", "file path for --src file (served via pread)")
	direct := fs.Bool("direct", false, "open --file with O_DIRECT (owner-NVMe model)")
	blocks := fs.Int("blocks", 256, "number of preloaded blocks (mem)")
	blocksize := fs.Int("blocksize", 4<<20, "block size (mem)")
	_ = fs.Parse(args)

	var inner gcache.ServerSource
	switch *src {
	case "mem":
		inner = newMemSource(*blocks, *blocksize)
		fmt.Printf("serving %d RAM blocks of %d bytes\n", *blocks, *blocksize)
	case "file":
		if *file == "" {
			fatalf("--src file requires --file")
		}
		flag := os.O_RDONLY
		if *direct {
			flag |= unix.O_DIRECT
		}
		f, err := os.OpenFile(*file, flag, 0)
		if err != nil {
			fatalf("open %s: %v", *file, err)
		}
		st, err := f.Stat()
		if err != nil {
			fatalf("stat: %v", err)
		}
		inner = &fileSource{f: f, size: st.Size()}
		fmt.Printf("serving %s (%d bytes, o_direct=%v) via pread\n", *file, st.Size(), *direct)
	default:
		fatalf("unknown --src %q", *src)
	}

	cs := &countSource{inner: inner}
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			fmt.Printf("[serve] warm loads=%d cold fills=%d\n", cs.load.Load(), cs.fill.Load())
		}
	}()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fatalf("listen: %v", err)
	}
	fmt.Printf("gcache peer server on %s\n", ln.Addr())
	ctx := context.Background()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go gcache.ServeConn(ctx, conn, cs, 65*time.Second)
	}
}

// ---------------------------------------------------------------- peer client

type peerClient struct {
	rc       *gcache.RingClient
	members  []gcache.Member
	group    string
	keys     int
	bs       int
	useRange bool
	unique   bool
	uniqueN  atomic.Int64
}

func newPeer(addr string, keys, bs int, useRange, unique, useRDMA bool, timeout time.Duration) *peerClient {
	rc := gcache.NewRingClient(timeout, 31, "latencybench")
	if useRDMA {
		rt, err := gcache.DialRDMA()
		if err != nil {
			fatalf("--rdma unavailable: %v (build with -tags rdma and install rdma-core)", err)
		}
		rc.SetRDMA(rt)
	}
	return &peerClient{
		rc:       rc,
		members:  []gcache.Member{{UUID: "peer", Addr: addr}},
		group:    "bench",
		keys:     keys,
		bs:       bs,
		useRange: useRange,
		unique:   unique,
	}
}

func (p *peerClient) readAt(buf []byte, off int64, rnd *rand.Rand) error {
	keyIdx := rnd.Intn(p.keys)
	if p.unique { // --unique: cold owner (fill path every time)
		keyIdx = int(p.uniqueN.Add(1)) + p.keys
	}
	key := fmt.Sprintf("bench-key-%d", keyIdx)
	if p.useRange {
		return p.rangeAt(key, off, buf)
	}
	r, err := p.rc.Fetch(context.Background(), p.group, key, p.members)
	if err != nil {
		return err
	}
	defer r.Close()
	if _, err := io.CopyN(io.Discard, r, off); err != nil {
		return err
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return nil
}

func (p *peerClient) rangeAt(key string, off int64, buf []byte) error {
	_, err := p.rc.RangeRead(context.Background(), p.group, key, off, buf, p.members)
	return err
}

// ---------------------------------------------------------------- read cmd

func cmdRead(args []string) {
	fs := newFlagSet("read")
	backend := fs.String("backend", "peer", "peer | fuse | nvme | readsvc")
	sockPath := fs.String("sock", "", "readsvc unix socket (backend=readsvc)")
	openPath := fs.String("open-path", "", "file path to open in the readsvc backend")
	addr := fs.String("addr", "", "peer host:port (backend=peer)")
	path := fs.String("path", "", "file path (backend=fuse|nvme)")
	useRange := fs.Bool("range", false, "use ranged peer reads (MsgRangeReq) instead of full-block fetch")
	useRDMA := fs.Bool("rdma", false, "peer mode: dial the peer via rsockets (requires the -tags rdma build; TCP fallback stays active on failure)")
	lens := fs.String("len", "4096", "comma list of request sizes")
	qds := fs.String("qd", "1", "comma list of queue depths")
	batch := fs.Int("batch", 0, "if >0: batched rounds of this many concurrent reads instead of steady QD")
	secs := fs.Float64("secs", 5, "seconds per configuration")
	keys := fs.Int("keys", 256, "key space size (backend=peer)")
	bs := fs.Int("bs", 4<<20, "block size behind each key (backend=peer)")
	unique := fs.Bool("unique", false, "cold owner: never reuse a key (fill path)")
	direct := fs.Bool("direct", false, "O_DIRECT (backend=nvme)")
	drop := fs.Bool("drop-cache", false, "fadvise DONTNEED before each round (fuse/nvme buffered)")
	out := fs.String("out", "", "JSON results file (replaces; prior content is loaded then re-saved)")
	seed := fs.Int64("seed", 42, "rng seed")
	timeout := fs.Duration("timeout", 10*time.Second, "peer request timeout")
	_ = fs.Parse(args)

	switch *backend {
	case "peer":
		if *addr == "" {
			fatalf("--addr required for backend=peer")
		}
	case "readsvc":
		if *sockPath == "" || *openPath == "" {
			fatalf("--sock and --open-path required for backend=readsvc")
		}
	case "nvme", "fuse":
		if *path == "" {
			fatalf("--path required for backend=%s", *backend)
		}
	default:
		fatalf("unknown backend %q", *backend)
	}

	var results []runResult
	if *out != "" {
		if b, err := os.ReadFile(*out); err == nil {
			_ = json.Unmarshal(b, &results)
		}
	}

	for _, ln := range splitInts(*lens) {
		for _, qd := range splitInts(*qds) {
			res := runOnce(*backend, *addr, *path, *useRange, *useRDMA, ln, qd, *batch, *secs,
				*keys, *bs, *unique, *direct, *drop, *seed, *timeout, *sockPath, *openPath)
			results = append(results, res)
			printTable(res)
			if *out != "" {
				b, _ := json.MarshalIndent(results, "", "  ")
				if err := os.WriteFile(*out, b, 0o644); err != nil {
					fatalf("write %s: %v", *out, err)
				}
			}
		}
	}
}

func runOnce(backend, addr, path string, useRange, useRDMA bool, length, qd, batch int, secs float64,
	keys, bs int, unique, direct, drop bool, seed int64, timeout time.Duration,
	sockPath, openPath string) runResult {

	res := runResult{
		Backend: backend, Addr: addr, Path: path, Range: useRange, Len: length, QD: qd,
		Batch: batch, Seconds: secs, Direct: direct, Dropped: drop, UniqueKeys: unique,
	}

	var mu sync.Mutex
	lat := make([]float64, 0, 1<<20)
	batchLat := make([]float64, 0, 1024)
	var errs, ops, bytes atomic.Int64

	var fr *fileReader
	var pr *peerClient
	dataSize := int64(bs)
	// readWorker pairs a read function with per-worker cleanup; shared
	// backends return the same fn for every worker.
	type readWorker struct {
		read func(buf []byte, off int64, rnd *rand.Rand) error
		done func()
	}
	var setupWorker func(id int) *readWorker
	switch backend {
	case "nvme", "fuse":
		fr = openReader(path, direct && backend == "nvme", drop)
		dataSize = fr.size
		shared := &readWorker{read: func(buf []byte, off int64, _ *rand.Rand) error {
			if drop {
				fr.dropCache()
			}
			_, err := fr.preadAt(buf, off)
			return err
		}, done: func() {}}
		setupWorker = func(int) *readWorker { return shared }
	case "peer":
		pr = newPeer(addr, keys, bs, useRange, unique, useRDMA, timeout)
		shared := &readWorker{read: pr.readAt, done: func() {}}
		setupWorker = func(int) *readWorker { return shared }
	case "readsvc":
		if sockPath == "" {
			fatalf("--sock required for backend=readsvc")
		}
		setupWorker = func(id int) *readWorker {
			c, err := readsvc.Dial(sockPath)
			if err != nil {
				fatalf("dial %s: %v", sockPath, err)
			}
			h, err := c.OpenHandle(openPath)
			if err != nil {
				fatalf("open %s: %v", openPath, err)
			}
			return &readWorker{
				read: func(buf []byte, off int64, _ *rand.Rand) error {
					_, err := h.ReadAt(buf, off)
					return err
				},
				done: func() { _ = h.Close(); _ = c.Close() },
			}
		}
	default:
		fatalf("unknown backend %q", backend)
	}

	stop := make(chan struct{})
	deadline := time.Now().Add(time.Duration(secs * float64(time.Second)))

	// random 4KiB-aligned offset in [0, dataSize-length]
	pickOff := func(rnd *rand.Rand) int64 {
		maxOff := dataSize - int64(length)
		if maxOff < 0 {
			maxOff = 0
		}
		return (rnd.Int63n(maxOff+1) / 4096) * 4096
	}

	if batch > 0 {
		// inference workload: rounds of `batch` concurrent reads; round wall
		// time captures slowest-request (straggler) effects. Each worker owns
		// an RNG (rand.Rand is not goroutine-safe).
		bufs := make([][]byte, batch)
		rngs := make([]*rand.Rand, batch)
		slots := make([]*readWorker, batch)
		for i := range bufs {
			bufs[i] = alignedBuf(length)
			rngs[i] = rand.New(rand.NewSource(seed + int64(i)))
			slots[i] = setupWorker(i)
		}
		defer func() {
			for _, s := range slots {
				s.done()
			}
		}()
		for time.Now().Before(deadline) {
			offs := make([]int64, batch)
			for i := range offs {
				offs[i] = pickOff(rngs[i])
			}
			start := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < batch; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					s := time.Now()
					if err := slots[i].read(bufs[i], offs[i], rngs[i]); err != nil {
						errs.Add(1)
					} else {
						d := float64(time.Since(s).Microseconds())
						mu.Lock()
						lat = append(lat, d)
						mu.Unlock()
						ops.Add(1)
						bytes.Add(int64(length))
					}
				}(i)
			}
			wg.Wait()
			mu.Lock()
			batchLat = append(batchLat, float64(time.Since(start).Microseconds()))
			mu.Unlock()
			if drop {
				fr.dropCache()
			}
		}
		close(stop)
		res.Read = summarize(lat)
		res.BatchDone = summarize(batchLat)
	} else {
		var wg sync.WaitGroup
		workers := make([]*readWorker, qd)
		for i := 0; i < qd; i++ {
			workers[i] = setupWorker(i)
		}
		defer func() {
			for _, w := range workers {
				w.done()
			}
		}()
		for i := 0; i < qd; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(seed + int64(id)))
				buf := alignedBuf(length)
				for {
					select {
					case <-stop:
						return
					default:
					}
					off := pickOff(rnd)
					start := time.Now()
					err := workers[id].read(buf, off, rnd)
					d := float64(time.Since(start).Microseconds())
					if err != nil {
						errs.Add(1)
					} else {
						mu.Lock()
						lat = append(lat, d)
						mu.Unlock()
						ops.Add(1)
						bytes.Add(int64(length))
					}
				}
			}(i)
		}
		<-time.After(time.Until(deadline))
		close(stop)
		wg.Wait()
		res.Read = summarize(lat)
	}

	if res.Read.N > 0 {
		res.IOPS = float64(ops.Load()) / secs
		res.MiBS = float64(bytes.Load()) / (1 << 20) / secs
	}
	res.Errors = errs.Load()
	if backend == "peer" {
		res.Transport = map[bool]string{true: "rsocket", false: "tcp"}[useRDMA]
	}
	return res
}

func printTable(r runResult) {
	tag := ""
	if r.Range {
		tag += "+range"
	}
	if r.Direct {
		tag += " o_direct"
	}
	if r.Dropped {
		tag += " drop-cache"
	}
	if r.UniqueKeys {
		tag += " unique-keys"
	}
	fmt.Printf("\n=== %s%s len=%d qd=%d batch=%d ===\n", r.Backend, tag, r.Len, r.QD, r.Batch)
	fmt.Printf("  read  : p50=%8.1f p95=%8.1f p99=%8.1f us (n=%d, max=%.1f)\n",
		r.Read.P50, r.Read.P95, r.Read.P99, r.Read.N, r.Read.Max)
	if r.BatchDone.N > 0 {
		fmt.Printf("  batch : p50=%8.1f p95=%8.1f p99=%8.1f us (n=%d rounds)\n",
			r.BatchDone.P50, r.BatchDone.P95, r.BatchDone.P99, r.BatchDone.N)
	}
	fmt.Printf("  iops=%.0f mibs=%.1f errors=%d transport=%s\n", r.IOPS, r.MiBS, r.Errors, r.Transport)
}

// ---------------------------------------------------------------- pread

func syscallPread(f *os.File, p []byte, off int64) (int, error) {
	var n int
	var err error
	for len(p) > 0 {
		n, err = unix.Pread(int(f.Fd()), p, off)
		if n > 0 {
			off += int64(n)
			p = p[n:]
		}
		if err != nil || n == 0 {
			break
		}
	}
	return n, err
}

// fileReader wraps pread with warm/cold page-cache control.
type fileReader struct {
	f    *os.File
	size int64
	drop bool
}

func openReader(path string, direct, drop bool) *fileReader {
	flag := os.O_RDONLY
	if direct {
		flag |= unix.O_DIRECT
	}
	f, err := os.OpenFile(path, flag, 0)
	if err != nil {
		fatalf("open %s: %v", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		fatalf("stat: %v", err)
	}
	return &fileReader{f: f, size: st.Size(), drop: drop}
}

func (r *fileReader) dropCache() {
	_ = unix.Fadvise(int(r.f.Fd()), 0, 0, unix.FADV_DONTNEED)
}

func (r *fileReader) preadAt(p []byte, off int64) (int, error) {
	return syscallPread(r.f, p, off)
}

// ---------------------------------------------------------------- main

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: latencybench %s [flags]\n", name)
		fs.PrintDefaults()
	}
	return fs
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: latencybench serve|read ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "read":
		cmdRead(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		os.Exit(2)
	}
}
