# Ultra-Low-Latency Distributed Cache — Implementation & Benchmark Report

**Branch:** `low-latency-ranged-reads` (fork of `ProjectInitiative/juicefs`, based on `gcache-strict-sharding` / PR #3)
**Motivating workload:** DeepSeek V4.1 Flash 1017 NVMe-backed Engram tables — many small, effectively random 4 KiB reads, ~70 concurrent lookups per decoding round, on 3× DGX Spark nodes joined by a private ConnectX-7 fabric (~200 Gbps).

> Status legend: ✅ implemented & tested · 🔬 implemented, needs cluster hardware to measure · 📋 design/eval only.

---

## 1. Architecture review (baseline audit)

### 1.1 Components (as found, before changes)

| Component | File | Role |
|---|---|---|
| Ring placement | `pkg/gcache/ring.go` | Rendezvous (HRW) hashing; `Owners(key, members, n)` |
| Peer server | `pkg/gcache/server.go` | Frame loop `ServeConn`; serves blocks from the mount's cache via the bridge |
| Peer client | `pkg/gcache/client.go` | `RingClient.Fetch` — full-block fetch from ring owner; failure counters/eviction |
| Push/Drop | `pkg/gcache/pushdrop.go` | `--fill-group-cache` push + delete-broadcast |
| Membership | `pkg/gcache/manager.go`, `registry*.go` | Heartbeat into the meta engine; member views; multi-rail/C7 advertise |
| Transports | `pkg/gcache/transport.go`, `rdma.go` | TCP default; `dialFailover` (RDMA preferred → TCP backoff 5 m) |
| Wire protocol | `types.go` | 20-byte header, `BLOCK_REQ/RESP/ERROR/PUSH/DROP`, protoVer 1 |
| Bridge | `pkg/chunk/gcache_bridge.go` | Structural adapter `*cachedStore` → `gcache.ServerSource` (no import cycle) |
| Decorator | `pkg/gcache/server.go` (`ringStorage`) | Intercepts **full-block** `ObjectStorage.Get` only |
| Read path | `pkg/chunk/cached_store.go` (`rSlice.ReadAt`) | local cache → range read → full-block singleflight load |

### 1.2 The read path, exactly (traced)

A FUSE `pread(fd, buf, off, len)` reaches `vfs` → `internalNode.read` → chunk layer
`ChunkStore.Read` → `rSlice.ReadAt(ctx, page, off)`:

1. **Block decomposition.** `indx = off / BlockSize`, `boff = off % BlockSize` (`BlockSize` =
   chunk block size, default 4 MiB). Reads spanning a block boundary recurse per block.
2. **Local disk cache.** `bcache.load(key)` — on hit, `r.ReadAt(p, boff)` serves the sub-block
   range **positionally** from the local cache file. Fast path; no network. The disk cache
   stores **decompressed** block bytes (compression is undone before `bcache.cache`).
3. **On local miss** (`cacheMiss` counter):
   - **Uncompressed ("seekable") volumes only**, and only when `boff > 0 && len(p) <= blockSize/4`:
     `loadRange` → `storage.Get(ctx, key, boff, len(p))` — a **range GET to object storage**
     (S3). This path **bypasses the ring entirely**: the `ringStorage` decorator only
     intercepts full-block shapes. On success it also triggers a background prefetch
     (`fetcher.fetch`) that pulls the *whole* block.
   - **Otherwise:** singleflight `store.load()` → `storage.Get(ctx, key, 0, -1)` — the
     **full-block shape** — which `ringStorage.Get` intercepts:
     - self-owned → object storage (cached locally);
     - else → `RingClient.Fetch` from the ring owner → **the entire block (up to 4–16 MiB)
       crosses the network for a 4 KiB read**; owner-side miss → owner fills its whole block
       from object storage first (`serveBlock` does `io.ReadAll` of the full block and
       re-compresses it).
     - Strict one-copy sharding (PR #3): peer-served payloads carry `NoLocalCache`, so the
       **consumer does not persist the block** — the *next* 4 KiB read in the same block
       repeats the whole 4 MiB transfer.

### 1.3 Bottlenecks for small random reads (4 KiB @ ~70 concurrency)

| # | Bottleneck | Impact |
|---|---|---|
| B1 | **No sub-block peer reads.** A remote-cache hit costs the full block on the wire (`serveBlock` `io.ReadAll` + re-compress + stream). A 4 KiB read ⇒ 4 MiB (or more) transfer; at 70 reads/round spread over distinct blocks ⇒ ~280 MiB per decoding round. | dominates latency & bandwidth |
| B2 | **No connection reuse.** `fetchOnce` dials a **fresh TCP connection per request** (handshake + slow start each time); `Push`/`Drop` too. 70 concurrent reads ⇒ 70 handshakes per round. | ~1 RTT + per-conn allocs on every read |
| B3 | **Uncompressed volumes bypass the ring for small reads** (`loadRange` goes to S3). Small reads never touch peer caches on uncompressed volumes. | remote-cache miss even when owner has the block |
| B4 | **RDMA transport is a stub.** `RdmaTransport.Dial`/`ListenRDMA` return `ErrRdmaUnsupported` (no rsockets backend is compiled in). Every "RDMA" dial fails → TCP fallback + 5 min backoff. The advertised RDMA features are **not operational** in the current build. | claimed RDMA path silently = TCP |
| B5 | Client per-IO goroutine + channel pattern (`ctxSend`/`ctxRead`) allocates per send/recv; response header allocs per op. | overhead at high QPS |
| B6 | Owner fill (`FillFromStorage`) is whole-block: correct as a *cache-fill policy* (owner = designated copy) but currently it is the only way to serve any range of an uncached block. | acceptable once B1 is fixed (fill lands on owner NVMe; client gets 4 KiB) |

Non-issues (verified): correctness of offset translation (`rSlice.key`/`blockSize`), compression
round-trip on the full-block path (wire carries volume-format bytes; bridge compresses from the
decompressed cache), encrypted volumes (peer path serves post-decrypt, still-compressed bytes —
same trust domain as the full-block path), and per-chunk deadline liveness from PR #3.

### 1.4 Consequences for the design

- The priority fix is a **ranged peer read**: `MsgRangeReq(key, off, len)` answered by a
  positional read from the owner's cached block (≤ requested bytes on the wire), with the
  existing whole-block path preserved for sequential reads and cache population (B1).
- **Connection pooling** per peer (one request in flight per pooled connection — xid stays 0)
  removes the per-read handshake (B2).
- The small-read interception belongs in `rSlice.ReadAt` **before** the S3 `loadRange` and the
  full-block singleflight, gated by an explicit flag, falling back to both on any failure
  (B3) — *not* by intercepting every `ObjectStorage.Get` range request, whose byte
  representation differs by layer (encrypted/compressed) and whose callers have different
  semantics. The ranged payload is defined as **raw block bytes (decompressed, post-decrypt)**
  — exactly what the local-cache `ReadAt` path would have returned.
- Ranged serving requires the **owner to have the block cached**; on owner miss the existing
  fill policy applies (owner fills its designated copy from object storage, then serves the
  range). That is the explicit cache-fill trade: 4 MiB object-storage→owner once, then 4 KiB
  owner→client per read.
- Old peers reject `MsgRangeReq` with an error frame / conn close; the client memoizes
  range-unsupport per peer (TTL) and falls back — de-facto capability negotiation with no
  protocol version bump (existing messages stay byte-identical; protoVer remains 1).

---

## 2. Implementation summary

*(filled per commit — see §8 for the commit index)*

| Change | Files | Flag / default |
|---|---|---|
| Connection pool per peer | `pkg/gcache/pool.go`, `client.go`, `pushdrop.go` | always on (bounded: idle cap 16/peer, idle TTL 60 s, error ⇒ discard) |
| Ranged peer reads (`MsgRangeReq` 0x06) | `pkg/gcache/{types,server,client,registry,metrics}.go`, `pkg/chunk/{gcache_bridge.go,gcache_peer_range.go,cached_store.go}`, `cmd/{flags,mount}.go` | `--peer-range-read` (default **off**) |
| rsockets (RDMA) transport | `pkg/gcache/rdma.go`, `rdma_rsocket.go` (`-tags rdma`) | build tag `rdma` + `--rdma-network` |
| FUSE-bypass read service | `pkg/readsvc/…` | `--read-socket PATH` (default off) |

### Wire protocol extension (v1, backward compatible)

```
Existing frames unchanged. New request type:

  MsgRangeReq = 0x06
    header: magic 'G','C',1 | type=0x06 | algo=0 | xid u32 | klen u16 | plen u64 = 16
    body:   key (klen bytes) || off u64 LE || len u64 LE
  Response: MsgBlockResp, algo = WireRaw, plen = bytes served (== len, else client errors)
    payload = raw block bytes (decompressed, post-decrypt) at [off, off+len)
```

- `plen == 16` in the request header is the size of the trailing off/len pair.
- Old servers reject unknown type 0x06 (`sendError` + close); new clients memoize the
  rejection per peer (5 min TTL) and fall back to the standard paths. No version bump needed;
  all pre-existing frames are byte-identical.
- Server-side validation: `off >= 0 && 0 < len <= maxRangePayload (4 MiB)`; when the block
  size is derivable from the key (`<id>_<indx>_<size>`, plain mode) the range is additionally
  clamped to `[0, size)`; positional reads are panic-guarded (hash-prefix keys carry no size).

### Client eligibility & fallback

`rSlice.ReadAt` tries the peer ranged path only when `--peer-range-read` is set, the loader is
wired, and `len(p) <= BlockSize/4` (any `boff`, including 0). Any failure — no peers, self-owned
key, peer range-unsupported, timeout, short/mismatched payload — falls through silently to the
pre-existing paths (S3 range read or full-block load). Counters: `blockcache_peer_range_hits`,
`blockcache_peer_range_bytes` (chunk layer) and `remotecache_range_gets/_bytes/_errors` (gcache).

---

## 3. Benchmark report

**Environment (dev node):** NixOS, Go 1.25.x toolchain (repo pins `go 1.25.10`; `pkg/chunk`
tests require ≤1.25 due to a mockey/go1.26 incompatibility), loopback TCP for transport
benchmarks. **No RDMA hardware on this node** — RDMA numbers require the DGX cluster; see §6.

Harness: `tools/latencybench` (self-contained; uses the real `RingClient` stack in `peer` mode)
and `deploy/bench/` (fio job files + scripts). Modes: `nvme`, `fuse`, `peer`, `infer`, `serve`.

*(results inserted per run — see §5 for methodology and raw data locations)*

### 3.1 Transport microbenchmarks (dev node, loopback TCP)

| Scenario | p50 | p95 | p99 | notes |
|---|---|---|---|---|
| *(to be measured after pooling commit)* | | | | |

---

## 4. Correctness report

*(tests executed + results; filled per commit)*

---

## 5. Methodology

- **Warm vs cold:** `fuse` mode uses `posix_fadvise(POSIX_FADV_DONTNEED)` to drop page cache
  between rounds (cold) or prefaults the file (warm). Cold *remote-cache* ≠ cold *page cache*;
  `peer` mode distinguishes owner-cache hits (warm key reuse) from owner fills (unique keys).
- **Distinguish NVMe from page cache:** `nvme --direct` uses `O_DIRECT`; buffered modes are
  labeled as page-cache-inclusive.
- **Sizes × queue depths:** 4 KiB / 16 KiB / 64 KiB / 4 MiB × QD 1/8/32/64/128.
- **Inference workload:** ~70 concurrent 4 KiB reads per round over a key space larger than
  RAM cache; per-read p50/p95/p99, batch completion time (incl. slowest), IOPS, MiB/s,
  requested-vs-fetched bytes, hit/miss + fallback counts.
- JSON to `--out`, human table to stdout. Raw JSON committed under `docs/bench/`.

---

## 6. Deployment guide (DGX Spark cluster)

*(build flags, mount examples, RDMA bring-up checklist, verification commands — filled per commit)*

---

## 7. Remaining work / risks

*(filled at the end)*

---

## 8. Commit index

*(branch `low-latency-ranged-reads`; hashes recorded at the end)*

---

## 9. Handoff summary (for a second agent)

*(written last; 1 page: what landed, how to verify, what's open)*
