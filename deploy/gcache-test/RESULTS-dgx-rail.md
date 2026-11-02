# DGX Spark rail results — chronometer ⇄ sextant, ConnectX-7 (172.16.5.0/24)

Run date: 2026-11-02 · branch `low-latency-ranged-reads` @ `6034cfde`
Method: `latencybench` pods (hostNetwork) via k8s on the lab cluster; every
read payload **verified** against the deterministic pattern (`--verify`);
`rsocket-strict` = RDMA-only client, zero fallback allowed. 16 GiB patterned
dataset (4096 × 4 MiB) on sextant's root NVMe, served with `--direct`
(genuine O_DIRECT preads; page cache never answers).

## TCP over the CX7 rail (all 0 errors, 0 mismatches)

| Scenario | p50 | p95 | p99 | notes |
|---|---|---|---|---|
| 4 KiB ranged, mem owner, QD1 | 271 µs | 562 µs | 761 µs | transport+protocol floor |
| 4 KiB ranged, mem owner, QD8 | 276 µs | 482 µs | 632 µs | 27.9K IOPS |
| 4 KiB ranged, mem owner, QD64 | 429 µs | 1.49 ms | 2.5 ms | 110K IOPS, 428 MiB/s |
| 4 KiB ranged, **O_DIRECT NVMe**, QD1 | 657 µs | 1.03 ms | 1.33 ms | device read in path |
| 4 KiB ranged, O_DIRECT NVMe, QD8 | 563 µs | 1.26 ms | 1.57 ms | |
| 4 KiB ranged, O_DIRECT NVMe, QD64 | 1.98 ms | 8.47 ms | 12.8 ms | NVMe queue depth effects |
| **batch-70, O_DIRECT NVMe** | read 1.70 ms; **batch p50 3.65 ms, p99 5.96 ms** | | | 18.9K reads/s, 0 errors |
| **batch-70, mem owner** | read 449 µs; **batch p50 1.05 ms, p99 2.35 ms** | | | 61K reads/s |

## rsockets over the same rail (strict mode, verified)

| Scenario | p50 | p95 | p99 | notes |
|---|---|---|---|---|
| 4 KiB ranged, mem owner, QD1 | **31 µs** | 122 µs | 265 µs | **~10× TCP** |
| 4 KiB ranged, mem owner, QD8 | **45 µs** | 109 µs | 300 µs | 138K IOPS, 542 MiB/s |
| 4 KiB ranged, O_DIRECT NVMe, QD1 | **218 µs** | 626 µs | 905 µs | **~3× TCP** |

1.26 M+ verified rsocket reads with zero errors and zero mismatches.

## Findings

1. **rsockets delivers the target latency** where the fabric is the bottleneck:
   31 µs p50 per 4 KiB remote read (vs 271 µs TCP) — well inside the 150 µs
   target for the DeepSeek decoding loop.
2. **With real NVMe O_DIRECT in the path, rsockets still wins 3×** (218 µs vs
   657 µs p50) — the device read dominates, but the transport no longer adds
   milliseconds.
3. **TCP batch-70 is healthy** with a correctly sized pool: batch p99 2.3 ms
   (mem) / 5.9 ms (O_DIRECT NVMe), 0 errors across 1.2 M reads.
4. **KNOWN ISSUE — rsockets high concurrency**: at ≥~64 simultaneous
   connections the datapath stalls (client reads hang; server goes quiet), and
   the server needs a restart afterwards. Root cause is in the rsockets
   buffer/credit path or our poll loop at scale, NOT the wire protocol — the
   same code moves 138K IOPS at QD8. Tracked as follow-up work; until fixed,
   keep rsockets concurrency ≤8 connections per peer (or pipeline requests
   over fewer connections).
5. **Connection churn is fatal at 70-way concurrency**: with the default
   16-conn/peer pool, batch-70 dialed ~54 fresh connections per round →
   EADDRNOTAVAIL (ephemeral-port exhaustion) and "31 failures in a row"
   streaks against healthy servers. `SetPoolMaxIdle(workers)` fixed it —
   production mounts with high-concurrency readers need the same tuning
   (follow-up: a mount flag).

## Correctness

Every run above verified every payload byte against the deterministic
pattern — including all 70-way batch rounds and both transports. The sharded
ring, ranged reads, and owner routing returned correct data end-to-end on
real hardware.

## Not yet run

- Capstan 10GbE matrix: needs ports 13910/tcp open on capstan2/capstan3
  (NixOS firewall currently drops them; ssh/22 works).
- rsockets batch-70 (blocked by finding 4).
- fio NVMe floor on the Sparks' root NVMe.
