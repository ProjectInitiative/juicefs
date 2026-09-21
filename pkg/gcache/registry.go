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
	"os"
)

// ServerSource is everything the peer server needs from a mount's chunk
// store. Satisfied by adapters in pkg/chunk/gcache_bridge.go via structural
// typing (no import cycle in either direction). See CONTRACT.md §4.
type ServerSource interface {
	// LoadCached returns a ReadCloser over the locally cached (decompressed)
	// block bytes, or an error wrapping os.ErrNotExist if absent.
	LoadCached(key string) (io.ReadCloser, error)
	// FillFromStorage downloads the block from object storage, caches it
	// locally (decompressed), and returns the cached reader.
	FillFromStorage(ctx context.Context, key string) (io.ReadCloser, error)
	// CompressAlgo returns the volume's compress string ("none", "lz4", "zstd").
	CompressAlgo() string
	// CompressBound returns the buffer size needed to compress n bytes.
	CompressBound(n int) int
	// CompressPayload compresses src (decompressed block) into dst, returns length.
	CompressPayload(dst, src []byte) (int, error)
}

// Registry maintains cache-group membership in the meta engine.
// One implementation per engine family: Redis, tkv, SQL (CONTRACT.md §5).
type Registry interface {
	// Register writes this member's record; called once after NewSession.
	Register(ctx context.Context, group string, self Member) error
	// Refresh rewrites the heartbeat (fresh TS / TTL); called on a ticker.
	Refresh(ctx context.Context, group string, self Member) error
	// List returns live members of the group (staleness-pruned).
	List(ctx context.Context, group string) ([]Member, error)
	// Close releases connections; membership expires via TTL/staleness.
	Close()
}

// ClientSource (removed): the client-side decorator needs nothing from the
// chunk layer — peer-fetched bytes are returned as an io.ReadCloser and
// handled by cachedStore.load() like any object-storage response.

// ensure os is used (os.ErrNotExist referenced in docs of ServerSource)
var _ = os.ErrNotExist
