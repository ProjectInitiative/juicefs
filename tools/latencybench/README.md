# latencybench + fio — Phase 1 benchmark harness

Low-overhead latency measurement for the gcache fast-path work. The guiding
rule: **never present kernel-page-cache results as remote-NVMe latency**, and
distinguish *cold remote-cache* (owner must fill) from *cold page cache*.

## Build

```
go build -o latencybench ./tools/latencybench
```

## Modes

### `serve` — production peer server over an explicit source

Runs the real `gcache.ServeConn` frame loop. Sources:

- `--src mem` — preloaded RAM blocks (pure protocol/transport cost; loopback
  on one node or cross-node for RDMA-vs-TCP). Cold fills are synthesized and
  evicted under a cap (safe for `--unique` runs).
- `--src file --file F [--direct] [--blocksize B]` — the file is treated as
  **fixed-size blocks**: `bench-key-%d` addresses block %d, so a `--keys N`
  read run samples the whole file (≥ N×B bytes), not just its first block.
  All reads use 4096-aligned windows/buffers/offsets — required for
  `--direct` (O_DIRECT), harmless otherwise. With `--direct` this is the
  **owner-NVMe model** (genuine device IO per range); without it, the owner's
  page cache serves.

```
./latencybench serve --addr 0.0.0.0:13910 --blocks 512 --blocksize 4194304
./latencybench serve --addr 0.0.0.0:13910 --src file --file /nvme/dataset.bin --blocksize 4194304 --direct
# strict RDMA benchmark: RDMA listener ONLY (no TCP listener exists to fall back to):
./latencybench serve --rdma --src file --file /nvme/dataset.bin --blocksize 4194304 --direct
```

### `read` — measured side

| Flag | Meaning |
|---|---|
| `--backend peer\|fuse\|nvme\|readsvc` | peer = production RingClient stack; fuse = pread through the mount; nvme = raw pread; readsvc = the mount's unix-socket read service |
| `--addr` | peer host:port (peer mode) |
| `--range` | use ranged reads (MsgRangeReq) — requires the ranged-read build |
| `--rdma` | peer/serve: use rsockets. On `read` this also enables **strict** mode — RDMA failure is an error, never silent TCP fallback; results are labeled `rsocket-strict` |
| `--len 4096,16384,...` | request sizes |
| `--qd 1,8,...` | queue depths (steady-state workers) |
| `--batch 70` | inference workload: rounds of N concurrent reads; reports round-completion percentiles (straggler effects) |
| `--unique` | cold owner: every read targets a never-cached key (fill path) |
| `--drop-cache` | `fadvise DONTNEED` per round (fuse/nvme): removes *page cache*, keeps remote/local disk caches |
| `--direct` | O_DIRECT (nvme only) — genuine device IO |
| `--secs`, `--out`, `--seed` | duration, JSON sink (loaded+appended), rng seed |

Output: per-read p50/p90/p95/p99/p999/max µs, batch-completion percentiles,
IOPS, MiB/s, error count. JSON array per `--out` file.

## What measures what

| Question | Recipe |
|---|---|
| Local NVMe floor | `nvme --direct` on a raw file/device |
| Local JuiceFS cached read | `fuse` on a warm mounted file (after one warm pass) |
| Page-cache excluded | add `--drop-cache` |
| Remote peer-cache hit, full-block path (baseline) | `peer` vs `serve --src mem` (or `--src file --direct` for owner-NVMe service) |
| Remote peer-cache hit, ranged path | same + `--range` (the whole file is the working set: `--keys` blocks × `--bs`) |
| Cold remote-cache miss | `peer --unique` (owner fill then serve) |
| Inference batch (~70×4 KiB/round) | `--batch 70` on any backend |

## Full matrix on a DGX node

See `deploy/bench/sweep.sh` (single-line commands, `OUT=`/`DEV=`/`MNT=`
overridable). It drives the fio job template (`randread.fio`) for the NVMe
floor and the latencybench matrix for everything else.

## Honesty notes

- RDMA results are only trustworthy with **both** `serve --rdma` and
  `read --rdma` (strict mode): strict disables TCP fallback, so an RDMA
  failure surfaces as errors, never as silently-mixed TCP samples. A
  `transport: "rsocket-strict"` label on a run with zero errors means every
  request used rsockets.
- `--src file` requires the file to be at least `--keys × --blocksize` bytes;
  keys beyond the block space fail loudly (they used to silently read the
  first block).

- `serve --src mem` measures protocol+transport only (no disk). Label results.
- `serve --src file` without `--direct` includes the owner's page cache.
- `fuse` without `--drop-cache` includes the *local* page cache — not a
  remote-cache measurement.
- The `peer` backend measures the true production client (pooling, protocol,
  failover), not a synthetic sender.
