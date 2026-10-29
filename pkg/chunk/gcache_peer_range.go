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

// gcache_peer_range.go — fork addition: opt-in fast path for small random
// reads through the distributed cache ring (pkg/gcache MsgRangeReq).
//
// Motivation (docs/LOW_LATENCY_CACHE_REVIEW.md §1.3): on a local cache miss,
// a 4 KiB read previously forced a whole-block fetch — up to 4–16 MiB across
// the network for 4 KiB of useful data (and non-owners don't persist it, so
// the next read in the same block repeated the transfer). With the loader
// wired, small reads are served positionally from the ring owner's cached
// block; any failure falls through silently to the pre-existing paths
// (seekable range read from storage, or the full-block load).
//
// Wiring (cmd/mount.go, gated by --peer-range-read):
//
//	chunk.SetPeerRangeLoaderOnStore(store, func(ctx, key, off, buf) (int, error) {
//	    return grc.RangeRead(ctx, group, key, off, buf, gmgr.AllMembers(group))
//	})

import "context"

// PeerRangeLoader serves one sub-block range from the cache-group owner.
// Implementations return (n, nil) only when the full window was served;
// any error means "fall back" (the caller proceeds exactly as if the
// loader were absent). off is the offset within the block named by key.
type PeerRangeLoader func(ctx context.Context, key string, off int64, buf []byte) (int, error)

// peerRangeThreshold: reads up to this fraction of a block may take the
// peer-range path. Matches the small-read heuristic of the seekable
// range-read branch (blockSize/4). 4 KiB random reads on 4 MiB blocks
// qualify by 1024x.
const peerRangeThreshold = 4

// setPeerRange installs the loader on a cachedStore (atomic: the loader is
// wired after NewCachedStore, before the VFS starts serving).
func (s *cachedStore) setPeerRange(fn PeerRangeLoader) { s.peerRange.Store(&fn) }

// SetPeerRangeLoaderOnStore installs the peer-range loader on a ChunkStore.
// No-op for stores without a local cache (S3 gateway etc.), mirroring
// gcache.SetBlockSizeOnStorage's tolerance pattern.
func SetPeerRangeLoaderOnStore(store ChunkStore, fn PeerRangeLoader) {
	if cs, ok := store.(*cachedStore); ok {
		cs.setPeerRange(fn)
	}
}

// peerRangeThresholdFor returns the max read length eligible for the peer
// range path on this store (0 when the loader is absent).
func (s *cachedStore) peerRangeLimit() int {
	if s.peerRange.Load() == nil {
		return 0
	}
	return int(s.conf.BlockSize / peerRangeThreshold)
}
