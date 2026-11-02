# Capstan 10GbE results — sharded 2-peer ring over the bonded 2×10GbE k8s network

Run date: 2026-11-02 · branch `low-latency-ranged-reads` @ `e3fdff77`
Method: `latencybench` pods (hostNetwork) via k8s on the lab cluster;
capstan2 + capstan3 serve the 64 GiB patterned dataset (16384 × 4 MiB) from
their fresh NVMe (`/mnt/jfs-cache`, TEAM TM8FPD002T) with `--direct`
(genuine O_DIRECT); capstan1 is the client. Every payload in the
correctness run is verified against the deterministic pattern.

## Sharding proof (the "shared, sharded cache" validation)

Two peers, 16384 keys, consistent-hash owner selection:

| Server | distinct keys requested | share |
|---|---|---|
| capstan2 | 8,064 | 49.2% |
| capstan3 | 8,320 | 50.8% |
| **total** | **16,384** | exactly the keyspace, no overlap |

Each server only ever received requests for keys it owns. 53,200 ranged
4 KiB reads across the ring: **0 errors, 0 mismatches** — correct owner
routing + correct payloads end-to-end on real hardware.

## Ranged fast path vs whole-block baseline (4 KiB reads, TCP)

| QD | ranged p50 | whole-block p50 | improvement |
|---|---|---|---|
| 1 | 570 µs | 16,198 µs | **28×** |
| 8 | 469 µs | 17,565 µs | 37× |
| 32 | 1,243 µs | 45,352 µs | 36× |

(The whole-block path fetches a full 4 MiB block per 4 KiB read — the
exact bottleneck the ranged read eliminates.)

## Ranged throughput by size (aggregate, 2 servers)

| Read size | QD8 | QD32 | QD64 | QD128 |
|---|---|---|---|---|
| 4 KiB | 44 MiB/s | 83 | 87 | 85 |
| 16 KiB | 162 MiB/s | 304 | 302 | 316 |
| 64 KiB | 432 MiB/s | 904 | 938 | **966** |

64 KiB ranged reads at QD128 reach ~966 MiB/s ≈ 1.01 GB/s — near 10GbE
line rate (1.19 GiB/s). p99 tail at QD128 (~40 ms) is bond+queuing depth,
not the cache path; the 70-way inference workload runs at batch=70 ≈ QD70
with batch p99 6.5 ms (below).

## Batch-70 (the DeepSeek decoding-round metric)

| | value |
|---|---|
| per-read p50 / p99 | 2,324 µs / 4,859 µs |
| **batch completion p50 / p99** | **4,089 µs / 6,514 µs** |
| throughput | 16,338 reads/s, 63.8 MiB/s |
| errors / mismatches | 0 / 0 |

## Context vs the DGX CX7 rail

| 4 KiB ranged QD1 (mem owner) | p50 |
|---|---|
| capstan 10GbE TCP | 570 µs (this doc; 2-peer sharded) |
| DGX CX7 TCP | 271 µs (single peer) |
| DGX CX7 rsockets | **31 µs** |

The 10GbE bond + older NVMe + sharded-ring dial costs ~1.9× vs the CX7
rail; rsockets on CX7 remains the path to the 150 µs target.

## Not yet run on capstans

- rsockets (capstans' NICs are standard Ethernet — no RDMA device).
- fio NVMe floor on the TEAM drive (server O_DIRECT reads are already
  included in the numbers above).
