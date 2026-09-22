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

package chunk

// gcache_bridge.go — fork addition: adapts a *cachedStore to the external
// gcache package WITHOUT pkg/chunk importing pkg/gcache. At the mount site,
// *GcacheBridge satisfies gcache.ServerSource via structural typing (the
// method sets match). See pkg/gcache/CONTRACT.md §4.
//
// This file is intentionally the ONLY touch point inside pkg/chunk: new file,
// no edits to upstream hot code, so upstream merges stay cheap.

import (
	"context"
	"fmt"
	"io"
	"os"
)

// GcacheBridge exposes cached-block access to the gcache peer server through
// function fields, so pkg/chunk carries no dependency on pkg/gcache.
type GcacheBridge struct {
	LoadCachedFn      func(key string) (io.ReadCloser, error)
	FillFromStorageFn func(ctx context.Context, key string) (io.ReadCloser, error)
	CompressAlgoFn    func() string
	CompressBoundFn   func(n int) int
	CompressPayloadFn func(dst, src []byte) (int, error)
	StorePushedFn     func(ctx context.Context, key string, data []byte) error
	DropCachedFn      func(key string) error
}

// NewGcacheBridge builds a GcacheBridge over a *cachedStore. It returns an
// error for store implementations that don't have a local cache (e.g. the
// S3-gateway path), which callers treat as "cache group unavailable".
func NewGcacheBridge(store ChunkStore) (*GcacheBridge, error) {
	cs, ok := store.(*cachedStore)
	if !ok {
		return nil, fmt.Errorf("gcache bridge requires a *cachedStore, got %T", store)
	}
	return &GcacheBridge{
		LoadCachedFn:      cs.loadCached,
		FillFromStorageFn: cs.fillFromStorage,
		CompressAlgoFn:    cs.compressAlgo,
		CompressBoundFn:   cs.compressBound,
		CompressPayloadFn: cs.compressPayload,
		StorePushedFn:     cs.storePushed,
		DropCachedFn:      cs.dropCached,
	}, nil
}

// Delegating methods — this is what makes *GcacheBridge satisfy
// gcache.ServerSource structurally.

func (b *GcacheBridge) LoadCached(key string) (io.ReadCloser, error) {
	return b.LoadCachedFn(key)
}

func (b *GcacheBridge) FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error) {
	return b.FillFromStorageFn(ctx, key)
}

func (b *GcacheBridge) CompressAlgo() string { return b.CompressAlgoFn() }

func (b *GcacheBridge) CompressBound(n int) int { return b.CompressBoundFn(n) }

func (b *GcacheBridge) CompressPayload(dst, src []byte) (int, error) {
	return b.CompressPayloadFn(dst, src)
}

func (b *GcacheBridge) StorePushed(ctx context.Context, key string, data []byte) error {
	return b.StorePushedFn(ctx, key, data)
}

func (b *GcacheBridge) DropCached(key string) error { return b.DropCachedFn(key) }

// loadCached returns a streaming reader over the locally cached (decompressed)
// block. chunk.ReadCloser is ReaderAt+Closer (no Read), so wrap it for
// io.ReadCloser consumers via the package's own pageReader when possible.
// Missing blocks map to an error wrapping os.ErrNotExist (CONTRACT.md §4).
func (cs *cachedStore) loadCached(key string) (io.ReadCloser, error) {
	r, err := cs.bcache.load(key)
	if err != nil {
		if err == errNotCached {
			return nil, fmt.Errorf("load %s: %w", key, os.ErrNotExist)
		}
		return nil, err
	}
	// ReadCloser (ReaderAt+Closer) → io.ReadCloser.
	return NewReadAtReader(r), nil
}

// fillFromStorage fetches the block from object storage through the normal
// load() path (which decompresses and caches it locally), then returns a
// reader over the cached bytes.
func (cs *cachedStore) fillFromStorage(ctx context.Context, key string) (io.ReadCloser, error) {
	size := parseObjOrigSize(key)
	if size <= 0 {
		return nil, fmt.Errorf("invalid block size for %s", key)
	}
	page := NewOffPage(size)
	defer page.Release()
	if err := cs.load(ctx, key, page, true, true); err != nil {
		return nil, fmt.Errorf("fill %s from storage: %w", key, err)
	}
	return cs.loadCached(key)
}

func (cs *cachedStore) compressAlgo() string { return cs.conf.Compress }

func (cs *cachedStore) compressBound(n int) int { return cs.compressor.CompressBound(n) }

func (cs *cachedStore) compressPayload(dst, src []byte) (int, error) {
	return cs.compressor.Compress(dst, src)
}

// storePushed caches a pushed block (--fill-group-cache). Pushed bytes are
// volume-format (what storage.Get returns); bcache stores decompressed.
func (cs *cachedStore) storePushed(ctx context.Context, key string, data []byte) error {
	if cs.conf.Compress == "none" {
		cs.bcache.cache(key, NewPage(data), true, !cs.conf.OSCache)
		return nil
	}
	size := parseObjOrigSize(key)
	if size <= 0 || size > cs.conf.BlockSize {
		return fmt.Errorf("invalid pushed block size for %s: %d", key, size)
	}
	p := NewOffPage(size)
	defer p.Release()
	n, err := cs.compressor.Decompress(p.Data, data)
	if err != nil {
		return fmt.Errorf("decompress pushed block %s: %w", key, err)
	}
	if n != size {
		return fmt.Errorf("decompress pushed block %s: got %d bytes, want %d", key, n, size)
	}
	cs.bcache.cache(key, p, true, !cs.conf.OSCache)
	return nil
}

func (cs *cachedStore) dropCached(key string) error {
	cs.bcache.remove(key, false)
	return nil
}

// ReadAtReader adapts chunk.ReadCloser (ReaderAt+Closer) to io.ReadCloser.
type ReadAtReader struct {
	r   ReadCloser
	off int64
	eof int64 // file length once known (-1: unknown); disk cache's ReadAt
	// panics when called at/past EOF (NewOffPage(0)), so after the
	// first short read we answer EOF ourselves without calling it.
}

// NewReadAtReader wraps a chunk ReadCloser for sequential reading.
func NewReadAtReader(r ReadCloser) *ReadAtReader { return &ReadAtReader{r: r, eof: -1} }

func (a *ReadAtReader) Read(p []byte) (int, error) {
	if a.eof >= 0 && a.off >= a.eof {
		return 0, io.EOF
	}
	n, err := a.r.ReadAt(p, a.off)
	a.off += int64(n)
	if n < len(p) && a.eof < 0 {
		a.eof = a.off // first short read pins the file length
	}
	if err == io.EOF && n > 0 {
		// ReadAt may return n>0 with io.EOF; io.Readers may report EOF on the
		// following call — surface data first, EOF later.
		err = nil
	}
	return n, err
}

func (a *ReadAtReader) Close() error { return a.r.Close() }
