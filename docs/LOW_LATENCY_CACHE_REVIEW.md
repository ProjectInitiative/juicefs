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
| B2 | **No connection reuse.** `fetchOnce` dials a **fresh TCP connection per request**; `Push`/`Drop` too. 70 concurrent reads ⇒ 70 handshakes per round. | ~1 RTT + per-conn churn on every read |
| B3 | **Uncompressed volumes bypass the ring for small reads** (`loadRange` goes to S3). Small reads never touch peer caches on uncompressed volumes. | remote-cache miss even when owner has the block |
| B4 | **RDMA transport is a stub.** `RdmaTransport.Dial`/`ListenRDMA` returned `ErrRdmaUnsupported` (no rsockets backend compiled in). Every "RDMA" dial failed → TCP fallback + 5 min backoff. | claimed RDMA path silently = TCP |
| B5 | Client per-IO goroutine + channel pattern (`ctxSend`/`ctxRead`) allocates per send/recv; response header allocs per op. | overhead at high QPS |
| B6 | Owner fill (`FillFromStorage`) is whole-block: correct as a *cache-fill policy* (owner = designated copy) but was the only way to serve any range of an uncached block. | acceptable once B1 is fixed (fill lands on owner NVMe; client gets 4 KiB) |

Non-issues (verified): correctness of offset translation (`rSlice.key`/`blockSize`), compression
round-trip on the full-block path (wire carries volume-format bytes; bridge compresses from the
decompressed cache), encrypted volumes (peer path serves post-decrypt bytes — same trust domain
as the full-block path), and per-chunk deadline liveness from PR #3.

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

| Change | Files | Flag / default | Commit |
|---|---|---|---|
| Baseline audit | `docs/LOW_LATENCY_CACHE_REVIEW.md` | — | `d09f35c1` |
| Benchmark harness | `tools/latencybench/`, `deploy/bench/`, `pkg/gcache/bench_test.go` | — | `cdd19ae9` |
| Connection pool per peer | `pkg/gcache/pool.go`, `client.go`, `pushdrop.go` | always on (idle cap 16/peer, TTL 30 s, error ⇒ discard) | `e671ab83` |
| **Ranged peer reads** | `pkg/gcache/{types,registry,server,client,metrics}.go`, `pkg/chunk/{gcache_bridge.go,gcache_peer_range.go,cached_store.go}`, `cmd/{flags,mount}.go` | `--peer-range-read` (default **off**) | `c6c66af5` |
| rsockets (RDMA) transport | `pkg/gcache/rdma.go`, `rdma_rsocket.go` (`-tags rdma`), `rdma_test.go` | build tag `rdma` + `--rdma-network` | `ebc12c5d` |
| FUSE-bypass read service | `pkg/readsvc/*`, `cmd/{flags,mount}.go`, `tools/latencybench` | `--read-socket PATH` (default off) | `5ab33390` |
| Contract docs | `pkg/gcache/CONTRACT.md` (§6a, §9, §11) | — | `4b4caed4` |

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
  rejection per peer (5 min TTL, re-probed after) and fall back. plen ≠ requested ⇒ protocol
  error ⇒ fallback. Server validation: `off ≥ 0`, `0 < len ≤ 4 MiB`, clamped to the
  key-embedded block size when parseable.
- Owner serve path: `RangeSource.LoadCachedRange` (bridge → `cachedStore.loadCachedRange` →
  positional `ReadAt` on the local cache — no whole-block read, no decompress, no re-compress);
  owner miss ⇒ `FillFromStorage` (designated-copy fill) then serve.

### Client eligibility & fallback

`rSlice.ReadAt` tries the peer ranged path only when `--peer-range-read` is set, the loader is
wired, and `len(p) <= BlockSize/4` (any `boff`, including 0). Any failure — no peers, self-owned
key, peer range-unsupported, timeout, short/mismatched payload — falls through silently to the
pre-existing paths (S3 range read or full-block load). Counters: `blockcache_peer_range_hits`,
`blockcache_peer_range_bytes` (chunk layer) and `remotecache_range_gets/_bytes/_errors` (gcache).

---

## 3. Benchmark report

**Environment (dev node):** NixOS, AMD Ryzen AI MAX+ 395 (32 threads), Go 1.25.14 (repo pins
`go 1.25.10`; `pkg/chunk` tests require ≤1.25 — mockey/go1.26 incompatibility). Loopback TCP for
transport numbers; **no RDMA hardware on this node** — RDMA comparisons need the DGX cluster (§6).

Harness: `tools/latencybench` (drives the **production** client/server stack; never a mock), plus
`pkg/gcache` microbenchmarks and fio job files (`deploy/bench/`). Raw JSON: `--out` files;
commands in `deploy/bench/sweep.sh`.

### 3.1 4 KiB random reads via the peer protocol (loopback, in-memory owner)

| QD | Full-block path (before) p50/p95/p99 µs | Ranged path (after) p50/p95/p99 µs | p50 speedup |
|---|---|---|---|
| 1 | 1412 / 4993 / 5801 | **37 / 129 / 150** | **38×** |
| 8 | 4582 / 6504 / 7363 | **76 / 109 / 168** | **60×** |
| 32 | — | **179 / 409 / 682** | — |
| 64 | 24327 / 44067 / 59773 | **291 / 983 / 1642** | **84×** |
| 128 | — | **575 / 2086 / 3349** | — |

IOPS (4 KiB useful): 607 → **21 527** (QD1), 2476 → **166 898** (QD64). Read amplification
(bytes on wire ÷ bytes requested): ~1024× → **1×** on warm hits. 16 KiB / 64 KiB ranged reads:
p50 82 / 96 µs (QD8).

The pre-change p50 of 1.4 ms for a 4 KiB read is *entirely* the 4 MiB block transfer: the
`BenchmarkServeConnPipe4M` microbenchmark (net.Pipe, no TCP stack) shows the same ~1.7 ms,
and `go test -bench` attributes **21.6 MB allocated per op** to the copy chain.

### 3.2 Inference workload — 70 concurrent 4 KiB reads per decoding round (warm)

| Metric | Full-block (before) | Ranged (after) | Improvement |
|---|---|---|---|
| Per-read p50 | 21.8 ms | **352 µs** | 62× |
| Per-read p99 | 30.0 ms | **3.6 ms** | 8× |
| **Round completion p50** | 29.3 ms | **0.91 ms** | **32×** |
| **Round completion p95** | 31.8 ms | **4.6 ms** | 7× |
| Effective IOPS | 2 310 | 38 906 | 17× |

### 3.3 Cold remote cache (owner must fill from storage)

Cold ranged reads pay the owner's fill (whole block from object storage → owner NVMe, once)
before serving the 4 KiB: loopback p50 ≈ 34 ms (dominated by the synthetic fill), vs 13.5 ms
for the full-block path (which also fills). **Cold vs warm is the dominant cost driver on the
cluster**; the fill policy amortizes it (the owner's designated copy persists; subsequent reads
are warm). Warm-hit p99 of 150 µs (QD1) is already below the 150 µs exploratory *target* at
p95 for the transport+protocol stack; owner-NVMe service adds the positional pread on the peer.

### 3.4 Microbenchmarks (loopback TCP, `pkg/gcache/bench_test.go`)

```
BenchmarkFetch4KViaFullBlock-32   200   1872284 ns/op   2.19 MB/s   21642516 B/op   115 allocs/op
BenchmarkFetch4M-32               200   1832571 ns/op 2288.75 MB/s   21636675 B/op    93 allocs/op
BenchmarkFetch4KParallel-32       200    494124 ns/op   8.29 MB/s   21660107 B/op   115 allocs/op
BenchmarkServeConnPipe4M-32       200   1685821 ns/op 2487.99 MB/s  21636067 B/op    82 allocs/op
```

Pooling alone did **not** move loopback p50 (1.58 vs 1.41 ms — the transfer dominates); it
removes handshakes and connection churn, which matters on the real fabric and at high QD, and
it is a prerequisite for the ranged path's pool reuse. Honest conclusion: **B1 (transfer) was
the binding constraint; B2 (handshake) is secondary on loopback.**

### 3.5 What still needs cluster hardware (§6)

- Real ConnectX-7 TCP vs rsockets (`-tags rdma`) latency for 4 KiB ranged reads.
- End-to-end FUSE (`--backend fuse`) before/after with `--peer-range-read` on/off.
- readsvc (`--backend readsvc`) vs FUSE under identical cache conditions.
- Raw NVMe floor (fio `randread.fio`, O_DIRECT) for the absolute target.

---

## 4. Correctness report

**Test suites executed** (Go 1.25 toolchain; `nix-shell -p go_1_25 stdenv.cc`):

| Suite | Result |
|---|---|
| `go test ./pkg/gcache/...` | ok (0.7–1.3 s) — includes 8 new ranged-read tests, 4 pool tests, RDMA negative test |
| `go test ./pkg/chunk/...` | ok (45 s) — includes 5 new peer-range integration tests |
| `go test ./pkg/readsvc -v` | ok — protocol round-trip, batch, concurrent clients, error paths |
| `go test ./pkg/gcache -race -run "Range\|Pool\|ServerClient"` | ok |
| `go build -tags rdma ./pkg/gcache` (against rdma-core 62.0 headers) | ok |
| `go test -tags rdma ./pkg/gcache -run TestRdmaUnavailableFailsCleanly` | ok (no device ⇒ fast clean failure) |
| `go build ./...` + `go vet` (touched pkgs) | ok |

**Coverage of the task's correctness matrix:**

- Correct 4 KiB offsets/contents across boundaries: `TestRangeReadWarmHit` (start/mid/end),
  `TestPeerRangeServesSmallReads` (incl. block-end and cross-half offsets).
- Compressed volumes: `TestRangeReadCompressedVolumeServesRaw` (ranged payload is raw block
  bytes; full-block path unchanged), `TestServerClientCompressed` (existing).
- Older peers: `TestRangeReadLegacyPeerMemoized` (rejection + memoization + re-probe),
  `TestRangeReadSourceWithoutRange` (new server, legacy source ⇒ clean error frame).
- Concurrent requests: `TestRangeReadConcurrent`, `TestPoolConcurrent`, `TestConcurrentClients`.
- Cache eviction during reads / peer disconnects & timeouts / incorrect data:
  existing PR #3 suites (`peer_recovery_test.go`, eviction tests) + `TestPoolDiscardOnPartialRead`
  (abandoned payload ⇒ conn discarded, next fetch redials), `TestRangeReadValidation`
  (out-of-range ⇒ error, clamped window ⇒ protocol error ⇒ fallback).
- Fallback correctness: `TestPeerRangeFallsBackOnError`, `TestPeerRangeShortResponseFillsBack`
  (short loader response treated as failure), `TestPeerRangeDisabledByDefault` (control).
- readsvc vs FUSE data equivalence: `TestClientServerRoundTrip` exactness vs the backing
  file; **end-to-end equivalence on a real mount needs the cluster** (§6 script step 5).

**Compatibility notes:** no flag renamed/removed; `--peer-range-read` and `--read-socket`
default off; wire frames for pre-existing messages are byte-identical; the bridge gained an
optional method (old gcache servers simply never request ranges); `Member` JSON unchanged.

---

## 5. Methodology

- **Warm vs cold:** `fuse`/`nvme` modes use `fadvise(POSIX_FADV_DONTNEED)` (`--drop-cache`) to
  remove *page cache* between rounds; `--unique` targets a never-cached key each read
  (cold *remote cache* — owner must fill). These are different colds; both are reported.
- **NVMe vs page cache:** `nvme --direct` is O_DIRECT (device IO); buffered modes are labeled
  page-cache-inclusive. `serve --src file --direct` models an owner's NVMe-resident block.
- **Sizes × QD:** 4 KiB/16 KiB/64 KiB/4 MiB × QD 1/8/32/64/128 (`--len`, `--qd` lists).
- **Inference workload:** `--batch 70` — rounds of 70 concurrent 4 KiB reads over a key space
  ≫ RAM cache; reports per-read percentiles, round-completion percentiles (straggler effect),
  IOPS, MiB/s, error counts. Per-worker RNGs (rand.Rand is not goroutine-safe — an early
  harness bug this fix addresses is documented in the commit).
- **Peer mode measures the real stack:** `RingClient` (pooling, protocol, failover) ↔
  `ServeConn` frame loop; no synthetic sender.
- JSON arrays per `--out` (loaded + appended across runs), human table on stdout.

---

## 6. Deployment guide (DGX Spark cluster)

### 6.1 Build

```
nix-shell -p go_1_25 stdenv.cc --run 'go build -o juicefs ./juicefs-main'   # or: make juicefs
# RDMA build (requires rdma-core headers + librdmacm on the BUILD host; binary needs librdmacm.so.1 at runtime):
nix-shell -p go_1_25 stdenv.cc rdma-core --run 'CGO_CFLAGS="$(pkg-config --cflags librdmacm 2>/dev/null)" go build -tags rdma -o juicefs-rdma ./juicefs-main'
```

Normal builds are unaffected by RDMA; tagged binaries refuse `--rdma-network` at startup if
userland/devices are missing (no silent fallback).

### 6.2 RDMA bring-up checklist (per node, once)

```
ls /sys/class/infiniband/                                   # device present?
ibv_devinfo                                                 # port state ACTIVE?
rping -s &                                                   # server side
rping -c -a <peer-fabric-ip> -C 4                            # client side: must complete
```

Only after rping passes: mount with `--rdma-network <nic>[:<nic2>]` and
`--group-advertise auto-c7` (TCP advertise) — the RDMA endpoint is advertised automatically
from the bound listener; the client prefers it and falls back to TCP per failure.

### 6.3 Mount lines (3-node ring)

```
juicefs mount --cache-group dgx-model-cache --peer-range-read --read-socket /run/jfs-read.sock --cache-dir /nvme/jfs-cache --cache-size 2048000 META-URL /mnt/jfs
```

Add `--rdma-network eth-fabric` + `-tags rdma` binary after rping passes. One node per ring
also runs the benchmark owner: nothing special — any member serves.

### 6.4 Verification & benchmark sequence (single-line commands)

```
go build -o latencybench ./tools/latencybench
# 1) NVMe floor (once per node): fio deploy/bench/randread.fio --filename=<nvme file> --bs=4k --iodepth=1 --output=fio-4k-qd1.json --eta=never
# 2) peer matrix, node A → node B (B: ./latencybench serve --addr 0.0.0.0:13910 --src file --file /nvme/block.bin --direct):
./latencybench read --backend peer --addr <B>:13910 --len 4096,16384,65536,4194304 --qd 1,8,32,64,128 --secs 10 --out results-tcp.json
# 3) same with the RDMA binary mounts + --rdma-network (transport auto-selected):
#    (repeat step 2; transport recorded in mount logs)
# 4) inference batch:
./latencybench read --backend peer --addr <B>:13910 --len 4096 --batch 70 --secs 20 --out results-infer.json
# 5) end-to-end FUSE vs readsvc (same mount, same conditions):
./latencybench read --backend fuse --path /mnt/jfs/engram/table.bin --len 4096 --qd 8 --secs 10 --drop-cache --out results-fuse.json
./latencybench read --backend readsvc --sock /run/jfs-read.sock --open-path /engram/table.bin --len 4096 --qd 8 --secs 10 --out results-readsvc.json
# 6) cold remote cache:
./latencybench read --backend peer --addr <B>:13910 --range --len 4096 --qd 8 --unique --secs 10 --out results-cold.json
```

Metrics to watch while running: `juicefs_blockcache_peer_range_{hits,bytes}`,
`juicefs_remotecache_range_*`, `juicefs_blockcache_{hits,miss}`, plus per-peer
`juicefs_peer_failures`.

### 6.5 readsvc client usage (Go)

```
c, _ := readsvc.Dial("/run/jfs-read.sock")
h, _ := c.OpenHandle("/engram/table.bin")
n, _ := h.ReadAt(buf, off)          // or h.BatchReadAt(bufs, offs)
h.Close(); c.Close()
```

One `Client` (connection) per concurrent worker: `fs.File.Pread` serializes per handle, so
the 70-way workload opens 70 handles (the latencybench readsvc backend does exactly this).

**Stale handles / invalidation:** every readsvc read traverses the live meta+chunk stack, so
metadata changes apply within the mount's normal attr/entry cache windows — identical to
FUSE reads of the same mount. Deleted/truncated files surface errors on the next read, never
stale silent data beyond those windows. No POSIX promises beyond that.

---

## 7. Phase 5 — Approach comparison: app-level RDMA cache vs NVMe-oF

| Dimension | A: gcache ring + ranged reads (implemented) | B: NVMe-oF (evaluated, not implemented) |
|---|---|---|
| 4 KiB QD1 latency | ~37 µs transport + owner-NVMe pread (~50–100 µs) + fabric RTT (~1–2 µs RoCE) ⇒ ≈ local-NVMe + ~40–60 µs | NVMe-oF RDMA adds ~10–20 µs over local NVMe per I/O (kernel NVMe/TCP is worse); but the *filesystem* above it still pays page-cache/FUSE costs unless the consumer reads the raw block device |
| Concurrency/tail | peer pool: one conn per in-flight request; scales to hundreds; p99 governed by owner disk | excellent (queue pairs), but block-device semantics push all sharing problems to the consumer |
| Kernel vs userspace | userspace end-to-end; zero new kernel deps | kernel nvme-tcp/nvme-rdma modules (or SPDK userspace — large engineering) |
| Cache consistency | ring ownership + one-copy sharding (DEC-0050); ranged reads never write | **none** — NVMe-oF is a transport, not a coherent cache; two nodes mounting the same block device corrupt it unless a cluster FS (ocfs2/gfs2) or strict single-writer ownership is layered on |
| Failure recovery | per-peer eviction + TCP fallback + health counters; reads always serviceable via object storage | multipath/resignature; a failed target stalls mounted consumers until timeout |
| Remote-device availability | any member serves any key (HRW re-placement) | explicit per-device target config; adding nodes = re-export |
| Security/isolation | fabric-local ring; no new privileged surface | namespace exposure; initiator ACLs; a misconfigured ACL exposes raw blocks |
| Operational complexity | one flag set on existing mounts | NVMe-oF target config, initramfs/udev, multipath, plus a shared-FS layer — new operational domain |
| Interoperability | JuiceFS-native; object storage remains source of truth | block device unusable by standard tools without the shared FS layer |

**Verdict:** Approach A delivers the target behavior with none of B's consistency hazards.
B is only interesting as a *local-disk accelerator for one owner node's cache dir* (e.g. the
owner writes its cache on a locally exported NVMe-oF volume — but on a 3-node all-flash
cluster every node already has NVMe). **Do not build B for cache sharing.** A safe future
experiment (requires hardware + explicit owner pinning, never automatic):
export one node's *unused* NVMe as an NVMe-oF namespace to a second node that has no spare
NVMe, and point the second node's `--cache-dir` at it — single-writer (the second node), no
shared filesystem, reversible by disconnecting the namespace. Setup sketch and benchmarks
belong in a later hardware session; no destructive target configuration is performed by this
branch.

---

## 8. Remaining work / risks

1. **Cluster measurements (highest value):** §3.5 list — TCP-vs-rsockets on ConnectX-7,
   end-to-end FUSE/readsvc before/after, cold-fill distribution on real object storage. All
   harnesses are ready; nothing needs code changes.
2. **rsockets runtime validation:** compile-verified against rdma-core 62.0; needs a real
   device + `rping` pass. If rsockets per-op overhead disappoints (< target), the design
   already isolates the next step: a verbs RC-QP transport implementing `Transport` +
   listener (registered memory + persistent QPs), or io_uring on the TCP path (kernel ≥5.1).
3. **Batched range requests** (one frame carrying N ranges) would cut per-read framing at
   QD≥32; deferred until cluster data shows it matters.
4. **readsvc per-handle serialization** (fs.File.Pread lock): fine for one-handle-per-worker
   use; a lock-free positional API inside pkg/fs would be an upstream touch — deferred.
5. **Owner-fill policy tuning:** currently owner-miss ⇒ whole-block fill (designated copy).
   If cluster data shows sparse Engram access with low block reuse, an optional
   "range-from-storage without fill" mode (owner S3 range GET, no persist) could cut
   cold-fill cost at the price of no cache population — flag-gated, not implemented.
6. **Risk — pooled conn vs server frame deadline:** idle client TTL (30 s) is below the
   server's 65 s frame deadline by design; changing `--remote-timeout` below ~35 s would
   require revisiting `poolIdleTTL`.
7. **Risk — mem-only cache mounts:** `SetPeerRangeLoaderOnStore` is a no-op for non-cachedStore
   backends (S3 gateway), consistent with the bridge's behavior.

---

## 9. Commit index (branch `low-latency-ranged-reads`)

```
d09f35c1 docs: baseline architecture audit of the gcache read path
cdd19ae9 bench: latencybench harness (peer/fuse/nvme) + protocol microbenchmarks + fio baseline
e671ab83 gcache: per-peer connection pooling (single-flight per conn, idle TTL 30s, error-discard)
c6c66af5 gcache: ranged peer reads (MsgRangeReq) — sub-block remote-cache hits for small random reads
ebc12c5d gcache: real rsockets (RDMA) transport behind -tags rdma
5ab33390 readsvc: opt-in FUSE-bypassing read service (prototype)
4b4caed4 docs: CONTRACT.md — ranged-read protocol extension (6a), new flags, rsockets notes
```

Base: `gcache-strict-sharding` @ `d369102a` (PR #3). Push as
`git push -u origin low-latency-ranged-reads`; PR against `main` after PR #3 merges (or rebase).

---

## 10. Handoff summary (for a second agent)

**What landed, in one paragraph:** The fork's distributed cache previously moved whole 4–16 MiB
blocks per remote-cache hit over a fresh TCP connection per request, bypassed the ring for
small reads on uncompressed volumes, and had a non-functional RDMA stub. This branch adds:
(a) per-peer connection pooling; (b) `MsgRangeReq` (0x06) ranged peer reads — owner serves the
exact requested window positionally from its cached block (owner-miss ⇒ designated-copy fill),
wired into `rSlice.ReadAt` behind `--peer-range-read` with silent fallback to the untouched
upstream paths, old peers memoized-and-fallback; (c) a real rsockets transport behind
`-tags rdma`; (d) `pkg/readsvc`, a `--read-socket` Unix-socket read service reusing the mount's
meta+chunk stack; (e) `tools/latencybench` + `deploy/bench` harnesses that measure the real
stack. Loopback: 4 KiB warm remote read 1412 µs → 37 µs (QD1), 24.3 ms → 291 µs (QD64);
70-read decoding round 29.3 ms → 0.91 ms. All tests green (incl. `-race`, both RDMA build
modes); full matrix on hardware pending (§6.4).

**Verify in 5 minutes:**
```
cd juicefs && nix-shell -p go_1_25 stdenv.cc --run 'go test ./pkg/gcache/... ./pkg/chunk/... ./pkg/readsvc'
nix-shell -p go_1_25 stdenv.cc --run 'go test ./pkg/gcache -run xxx -bench Fetch4KViaFullBlock -benchtime 100x'
```

**Where things live:** protocol `pkg/gcache/types.go` (§6a of CONTRACT.md) · server
`server.go:servRange/serveRange` · client `client.go:RangeRead/rangeOnce` · pool `pool.go` ·
bridge `pkg/chunk/gcache_bridge.go:LoadCachedRange` · hook `pkg/chunk/cached_store.go`
(`rSlice.ReadAt`, marked "gcache fork") + `pkg/chunk/gcache_peer_range.go` · RDMA
`pkg/gcache/rdma_rsocket.go` · readsvc `pkg/readsvc/` · harness `tools/latencybench/`.

**Gotchas:** the ranged payload is *raw block bytes*, not volume-format (only new clients send
0x06); response/request framing on readsvc is u32-length (batch bodies exceed 64 KiB);
`poolIdleTTL` must stay < server frame deadline; latencybench workers need per-worker RNGs.
