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
  on one node or cross-node for RDMA-vs-TCP).
- `--src file --file F [--direct]` — positional preads from a real file. With
  `--direct` this is the **owner-NVMe model** (genuine device IO per range);
  without it, the owner's page cache serves.

```
./latencybench serve --addr 0.0.0.0:13910 --blocks 512 --blocksize 4194304
./latencybench serve --addr 0.0.0.0:13910 --src file --file /nvme/block.bin --direct
```

### `read` — measured side

| Flag | Meaning |
|---|---|
| `--backend peer\|fuse\|nvme` | peer = production RingClient stack; fuse = pread through the mount; nvme = raw pread |
| `--addr` | peer host:port (peer mode) |
| `--range` | use ranged reads (MsgRangeReq) — requires the ranged-read build |
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
| Remote peer-cache hit, ranged path | same + `--range` |
| Cold remote-cache miss | `peer --unique` (owner fill then serve) |
| Inference batch (~70×4 KiB/round) | `--batch 70` on any backend |

## Full matrix on a DGX node

See `deploy/bench/sweep.sh` (single-line commands, `OUT=`/`DEV=`/`MNT=`
overridable). It drives the fio job template (`randread.fio`) for the NVMe
floor and the latencybench matrix for everything else.

## Honesty notes

- `serve --src mem` measures protocol+transport only (no disk). Label results.
- `serve --src file` without `--direct` includes the owner's page cache.
- `fuse` without `--drop-cache` includes the *local* page cache — not a
  remote-cache measurement.
- The `peer` backend measures the true production client (pooling, protocol,
  failover), not a synthetic sender.
