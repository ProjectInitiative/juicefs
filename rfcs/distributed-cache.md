# RFC: Distributed Cache for Community JuiceFS

* Status: Draft
* Author: kylepzak
* Target: ProjectInitiative fork of juicedata/juicefs @ `f654a80c` (go 1.25, upstream-synced)
* Target deployment: 3× DGX Spark nodes pooled over a private ConnectX-7 fabric

## Background

Distributed cache is a JuiceFS **Enterprise** feature, not present in the community edition.
Enterprise clients sharing a `--cache-group` form a consistent hashing ring; each block's
home node is computed by hashing the block key, and on a local miss the client fetches the
block from its home node instead of object storage. Membership is discovered via the
metadata service; failed peers are locally evicted after 31 consecutive errors; rebalancing
waits 10 minutes after membership changes. This RFC specifies a community implementation.

The codebase already provides all the seams this needs:

* `pkg/chunk/cached_store.go` — `cachedStore.load()` (line ~762) is the single choke point
  where object-storage fetches happen: local disk cache → `load()` → object storage. All
  distributed-cache logic hooks here. Nothing in `pkg/vfs` or the FUSE layer changes.
* `bcache` is a `CacheManager` interface (pkg/chunk/cache_manager.go:92) with
  `cache()`/`load()`/`exist()`/`remove()` — the peer server serves blocks straight out of
  `load()`, and peer fills land via `cache()`, same as local fills.
* `FillCache` (cached_store.go:1214) and warmup (`cmd/warmup.go`, `pkg/vfs/fill.go`) already
  drive full-block warming; warming on any member populates the ring.
* `NewSession` / `SessionInfo` (pkg/meta/interface.go:354) — the meta engine tracks live
  sessions; we add group membership records to the same engine.
* `singleflight` (`store.group.Execute`) already dedups concurrent fetches per key — peer
  fetches reuse it, so concurrent local readers of the same block share one in-flight fetch.

Reference behavior: [enterprise distributed cache docs](https://juicefs.com/docs/cloud/guide/distributed-cache/).

## Proposal

Every mount process is both a cache **server** (serves its locally cached blocks to peers
over the fabric) and a cache **client** (on a local miss for a block owned by a peer, RPCs
the peer; the owner serves from its disk, filling from object storage on its own miss).
Mounts with the same `--cache-group` form the ring.

Three core decisions:

**1. The meta engine is the membership registry.** Enterprise uses its metadata service for
discovery; our equivalent is writing membership records into the same meta engine the
filesystem already uses (Redis / kv / SQL). A member writes
`<prefix>gcache/<group>/<uuid>` with a TTL (or timestamp, for engines without TTL) and
refreshes it on a heartbeat. Listing members = a range scan / hash read of that prefix.
No new service to deploy; the registry inherits the meta engine's HA exactly.

**2. Rendezvous (highest-random-weight) hashing for block placement.** For a block key
`{vol}/{inode}/{blockIndex}_{size}`, every member computes
`score_i = hash(member_id || key)` and the owner is the argmax. The full ring view is
computed locally from the member list — there is no ring-reconfiguration protocol and no
virtual-node count to tune. Membership changes only move the fraction of keys whose top
member changed, and take effect immediately for new reads. This is why HRW was chosen over
a classic hash ring: it removes the entire class of membership-consistency problems
enterprise solves with a 10-minute migration delay.

**3. Block-level (4MiB) granularity.** Placement is per block, not per file. A hot file
spreads across all members' drives instead of pinning one node's bandwidth. This matches
enterprise behavior.

A peer fill is just `load()` from the ring instead of from object storage; the result is
cached into the local `bcache` identically. Failure at any point falls through to object
storage — the client always stays serviceable.

## Implementation

### 1. New package `pkg/gcache`

```
pkg/gcache/
├── config.go         # flags → Config{Groups, ListenAddr, AdvertiseAddr, Weight,
│                     #            HeartbeatInterval, RPCTimeout, MaxFailures,
│                     #            NoSharing, FillOnUpload}
├── ring.go           # rendezvous hashing
│   ├── Score(memberID, key string) uint64
│   └── Owners(key string, members []Member, n int) []Member   # top-n by score
├── registry.go       # Registry interface: Register / Refresh / List / Close
├── registry_redis.go # per-group HASH <prefix>gcache/<group>, field=uuid,
│                     # value=JSON{Addr,Weight,Version,ts}; EXPIRE on refresh;
│                     # HGETALL to list; ts-based staleness pruning as backstop
├── registry_tkv.go   # keys under <prefix>gcache/<group>/, range scan to list,
│                     # ts-based pruning (no native TTL)
├── registry_sql.go   # gcache_members(group, uuid, addr, weight, ts) table,
│                     # upsert heartbeat, DELETE WHERE ts stale
├── server.go         # TCP listener; per-conn frame loop; serves bcache.load()
├── client.go         # conn pool per peer, pipelined requests, failure counters
└── gcache.go         # Manager: Start/Stop, member list maintenance, Load() entry
```

Implementation order: Redis + tkv registries first (badger/etcd included), then SQL.
The ts-staleness check makes the design engine-agnostic — engines with native TTL use
them; the rest prune at list time.

### 2. Wire protocol (v1)

Length-prefixed frames over TCP, little-endian:

```
magic "GC1" | ver u8 | type u8 | xid u32 | klen u16 | plen u64 | [key] [payload]
- type 0x01 BLOCK_REQ, 0x02 BLOCK_RESP, 0x03 ERROR
- v1: client may pipeline requests per connection; xid matches responses
- ERROR: plen = ^uint64(0), followed by klen + error string
- server streams the payload with io.Copy from the bcache ReadCloser (1–2MB buffer)
```

Payload is the raw (already-compressed, as-stored) block — byte-identical to what object
storage holds, so checksums and the existing footer/format logic apply unchanged.
`sendfile()`/splice from the cache file to the socket is a v2 optimization noted in
Appendix A.

### 3. Flags (enterprise parity)

```
--cache-group strings        # join/create these groups (repeatable)
--group-weight int           # relative cache capacity of this node (default 1)
--group-listen string        # listen addr for peer RPC (default: fabric interface)
--group-advertise string     # advertised addr (default: derived from listen addr)
--group-heartbeat duration   # membership refresh interval (default 10s; TTL = 3x)
--remote-timeout duration    # per-peer RPC timeout (default 65s, enterprise parity)
--no-sharing                 # consumer-only: take from the ring, never serve
--fill-group-cache           # push blocks we upload to their ring owners (best-effort)
--group-backup               # (v2) forward misses to the 2nd owner too → dual replica
```

### 4. Read-path integration — ObjectStorage decorator (upstream-merge-safe)

We do NOT patch `load()` inside `cached_store.go` (upstream edits that file constantly).
Instead we wrap the `object.ObjectStorage` instance that `cmd/mount.go` passes **into**
`NewCachedStore`. Since `load()` fetches via `storage.Get(ctx, key, 0, -1, ...)` and
uploads/deletes via `Put`/`Delete`, a ring-aware decorator intercepts all of it:

* `Get(off==0, limit==-1)` on a ring-owned block → fetch from peer owner, return bytes;
  `load()` then caches the result into `bcache` exactly as it does for object-storage
  fetches (no extra code). Miss/peer-error/no-members → fall through to underlying Get.
  Small random reads (`off>0`, seekable fast path) and range warmups never match the
  full-block signature, preserving existing semantics.
* `Put` (only when `--fill-group-cache`) → buffer block, underlying Put, then best-effort
  push to the block's ring owner.
* `Delete` → underlying Delete, then best-effort drop-broadcast to ring members
  (enterprise 5.0 parity).

This runs inside the existing `store.group.Execute` singleflight in `rSlice.ReadAt`, so
concurrent readers of the same block share one in-flight fetch automatically.

**Try order in ReadAt becomes:** kernel page cache → local disk cache → **peer fetch (new,
via decorated Get)** → seekable direct read → full read from object storage.

**Import rules (enforced, this is what keeps upstream merges cheap):**

* `pkg/gcache` imports only `pkg/object`, `pkg/utils`, go-redis, murmur3. It never imports
  `pkg/chunk`, `pkg/meta`, or `pkg/vfs`.
* `pkg/chunk` gets ONE new file, `gcache_bridge.go` (new files never conflict on merge),
  with ~40 lines: adapters making `*cachedStore` satisfy gcache's `ServerSource` interface
  (Go structural typing — no import in either direction).
* `cmd/mount.go` gets flag definitions plus ~15 wiring lines (wrap blob before
  `NewCachedStore`, bind store after).
* All shared knowledge (key format, wire format, signatures) lives in
  `pkg/gcache/CONTRACT.md`; if upstream renames something, only the bridge or the wiring
  line needs a touch-up.

**Compression detail:** local cache files hold decompressed blocks, but `load()` expects
`Get()` to return volume-format (compressed) bytes and decompresses them itself. So the
peer wire carries compressed bytes for compressed volumes: the server re-compresses on
serve (per-payload flag in the response header). For AI/training volumes created with
`--compress none` (the common case) this cost is zero. Server-side re-compression
throughput ≈ zstd ~0.5 GB/s/core; at NDR200 fabric speeds only matters for compressed
volumes — prefer `none` for cache-heavy training volumes.

**Fallback semantics (enterprise parity):** one retry per request on transient peer errors;
`--remote-timeout` (65s default) bounds each attempt; N consecutive failures (31 in
enterprise) evict the peer from this consumer's local view with
`remove peer %s after %d failures in a row`; a successful reconnect re-adds it with
`add peer %s back after %s`. Eviction is per-consumer — members stay in the registry.

### 5. Writes: `--fill-group-cache`

In `wSlice.upload()` (cached_store.go:388) after a successful upload, fire-and-forget push
of each uploaded block to its ring owner. Pushes may fail silently (no guarantee, matching
enterprise docs). Off by default.

### 6. Deletes (parity with enterprise 5.0)

`cachedStore.Remove()` broadcasts a delete to the current owner of each removed key so
stale blocks don't survive on the ring after file deletion. Best-effort.

### 7. Metrics (enterprise parity names)

* `juicefs_remotecache_gets` / `juicefs_remotecache_bytes` — peer fetch hits
* `juicefs_remotecache_errors` — end-to-end peer errors (the alert-worthy one)
* `juicefs_remotecache_durations` — peer fetch latency histogram
* `juicefs_peer_failures{peer}` — consecutive-failure gauge per peer (eviction trips at 31)

### 8. Tests

* ring: uniform distribution, deterministic order, bounded key movement on join/leave
* registry: register/refresh/list round-trip per engine, TTL expiry, stale pruning
* server/client: loopback on 127.0.0.5:0 — round-trip, pipelining, error frames, 4MiB frames
* read path: hook sentinel fall-through, hook error propagation, singleflight dedup with hook
* integration: 3 mounts in one group; B reads a file A cached; assert B never touches object
  storage (counting stub ObjectStorage); kill A, assert B falls through to object storage

## Non-goals (v1)

* No peer-protocol encryption — the fabric is private; ACL the subnet (same assumption
  enterprise documents).
* No `--group-backup` dual replicas (the `Owners(key, members, 2)` primitive already
  supports it as a follow-up).
* No RDMA verbs data path, no cgo. TCP over the C7 fabric is not the bottleneck (below).
* No rebalancing daemon — HRW needs none; keys move lazily as they are re-read.

## Deployment notes (3-node DGX Spark pool)

```bash
# each Spark; same fs, same group; drives pooled across nodes via the ring
juicefs mount $META /jfs --cache-group=sparks \
    --cache-dir=/mnt/nvme/cache --cache-size=<~90% of pool> \
    --group-listen=<c7-ip>:0 --cache-eviction=2-random --cache-expire=0

# verify
grep peer /var/log/juicefs.log          # "Peer listen at <c7-ip>:port"
ss -4atnp | grep juicefs

# pre-warm from any member (same effect ring-wide)
juicefs warmup /jfs/train-data -c 80
```

Notes:

* Nodes should be homogeneous (same cache disk size) — HRW scoring can weight by
  `--group-weight`, but equal sizes avoid subtle imbalance (same guidance as enterprise).
* With 3 nodes, a single node going down moves ~1/3 of keys; reads fall through to object
  storage for those keys until re-warmed or the node returns.

## Bypassing FUSE for local cache reads

FUSE overhead is bounded and should not be attacked first:

* Page-cache hits never reach the daemon — zero fusedev traffic for warm data.
* Large reads amortize: a fusedev request carries up to `max_read` bytes (kernel default
  128KB); cost per byte is two copies + two context switches per request.
* The fabric (NDR200 ≈ 25 GB/s) already exceeds a single NVMe drive (~7 GB/s); the pooled
  cache ceiling is ~2× a single drive's bandwidth for a 3-node ring once fabric copies are
  counted. Local hits are where FUSE overhead matters.

Ranked plan for closing the local-hit gap:

1. **max_read negotiation at INIT** (pkg/fuse): request 1MB in the INIT reply. Kernel cap
   is 1MB (`FUSE_MAX_MAX_PAGES * PAGE_SIZE` on modern kernels). Pure client-side patch;
   halves per-byte copy/switch cost for large sequential reads. Also bump
   `max_background`/concurrency so the kernel keeps many requests in flight.
2. **O_DIRECT cache-file reads**: read cached blocks with O_DIRECT +
   `posix_fadvise(POSIX_FADV_DONTNEED)` after copy so cached blocks are not double-cached in
   page cache. Bounds page-cache thrash on very large caches; OSS already has the
   `dropOSCache` hook points (`JFS_DROP_OSCACHE` path) to build on.
3. **Cache-direct sidecar daemon** (`juicefs gcache serve`): standalone member that mounts
   nothing — binds the fabric port, serves block files straight off NVMe, fills from peers/
   object storage. Serving peers bypasses FUSE entirely today (server side reads disk
   directly); the sidecar extends that to the whole cache tier and isolates cache I/O from
   application mount restarts.
4. **FUSE over io_uring** (kernel ≥ 6.6 server-side support, `FUSE_IO_URING` INIT flag):
   removes request submission/retrieval latency. Requires kernel support + an INIT-flag
   patch in pkg/fuse; track upstream libfuse/kernel work before investing.

Not worth pursuing: FUSE DAX (needs virtiofs/fsdax plumbing and PMEM semantics), DAX for the
cache files themselves (cache files are regular files on ext4/xfs, not dax-capable), and
kernel-module filesystem clients (out of scope for a FUSE project).

## Locked decisions

1. **ObjectStorage decorator** as the single integration seam — `pkg/chunk` hot code untouched,
   one new bridge file, ~15 wiring lines in `cmd/mount.go`. Merge-friendly by construction.
2. Meta engine as membership registry; HRW (rendezvous) placement; block-level granularity.
3. Enterprise-parity flags, log lines, fallback timers, and metric names.
4. `--no-sharing` consumer-only mounts for the dedicated-cache-cluster pattern.
5. v1 is TCP over the fabric; verbs transport can slot under the same frames later.

## Upstream merge strategy

* All new code lives in `pkg/gcache/` + `pkg/chunk/gcache_bridge.go` (additive). The only
  files both sides edit are `cmd/mount.go` (flags + wiring, inserted as separate blocks),
  `go.mod` (no new deps — go-redis and murmur3 already present), and docs.
* `pkg/gcache/CONTRACT.md` documents the 5 facts gcache depends on (key format, Get
  full-block signature, ServerSource methods, compressor algo ids, SessionInfo registration
  point). A refactor-break shows up as a compile error or a contract test failure, not
  silent misbehavior.
* Keep `AGENTS.md` note in repo root: when syncing upstream, re-run the contract tests
  (`go test ./pkg/gcache/... -run TestContract`) before/after merge.
* Rebase discipline: rebase fork commits onto upstream/main at each sync; conflicts should
  be limited to the wiring block in mount.go, resolvable mechanically.
