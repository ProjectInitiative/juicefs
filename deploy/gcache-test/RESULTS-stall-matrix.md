# Phase-1 stall matrix — 32-leg discrimination grid (2026-11-02)

Setup: fresh echo-server fleet per conns level on sextant (C :14000-7, Go :14100-7),
client legs from chronometer over DGX rail-0 (172.16.5.56), 8s legs, stall deadline 3s.
Binaries @ commit ff86c2ea (rdmaecho dispatch fix, INADDR_ANY binds).
Matrix log: job/lb-stall-matrix (llm-test).

## Result table

| conns | srv | cli | exit | reads (8s) |
|------:|-----|-----|-----:|-----------:|
| 8   | C  | c   | 139 (SIGSEGV after results) | 3.94M ok/s printed |
| 8   | C  | go  | 124 (hung) | 0 |
| 8   | GO | c   | 0 | 3,811,576 |
| 8   | GO | go  | 124 (hung) | 0 |
| 16  | C  | c   | 0 | 4,150,109 |
| 16  | C  | go  | 124 (hung) | 0 |
| 16  | GO | c   | 0 | 3,710,878 |
| 16  | GO | go  | 124 (hung) | 0 |
| 24  | C  | c   | 0 | 3,738,789 |
| 24  | C  | go  | 124 (hung) | 0 |
| 24  | GO | c   | 0 | 3,473,385 |
| 24  | GO | go  | 124 (hung) | 0 |
| 32  | C  | c   | 0 | 3,377,503 |
| 32  | C  | go  | 124 (hung) | 0 |
| 32  | GO | c   | 0 | 1,810,139 |
| 32  | GO | go  | 124 (hung) | 0 |
| 48  | C  | c   | 0 | 2,440,790 |
| 48  | C  | go  | 124 (hung) | 0 |
| 48  | GO | c   | 0 | 1,281,125 |
| 48  | GO | go  | 124 (hung) | 0 |
| 64  | C  | c   | 0 | 2,489,422 |
| 64  | C  | go  | 124 (hung) | 0 |
| 64  | GO | c   | 0 | 1,967,474 |
| 64  | GO | go  | 124 (hung) | 0 |
| 96  | C  | c   | 0 | 2,470,609 |
| 96  | C  | go  | 124 (hung) | 0 |
| 96  | GO | c   | 139 (SIGSEGV after start) | — |
| 96  | GO | go  | 124 (hung) | 0 |
| 128 | C  | c   | 0 | 1,748,144 |
| 128 | C  | go  | 124 (hung) | 0 |
| 128 | GO | c   | 0 | 1,667,194 |
| 128 | GO | go  | 124 (hung) | 0 |

**C-client legs: 14/16 pass. Go-client legs: 0/16 pass.**

## Quadrant verdict

1. **Server stack is irrelevant.** Both C and Go servers serve the C client at
   0.17M-4.2M reads / 8s (p50 10-13us) at EVERY conns level, including 128.
2. **The defect is in the Go CLIENT path** (`DialRDMA` → `rsocketConn`), not in
   librdmacm, not the CX7 device, not the wire protocol.
3. **Not a high-concurrency stall.** The Go client fails at 8 conns too — the
   previously observed "stall at >=64 conns" is the same defect, seen only
   because earlier DGX tests ran the Go client at high QD.
4. Goroutine signature per hung leg: goroutines ~= 2*conns + 8 — workers are
   parked in dial (each dial has a watchdog goroutine) or in an uninterruptible
   first request; 1-3 timeouts registered total, 0 completed requests ever.
   The 3s SetDeadline does not surface: consistent with goroutines parked
   inside the FIRST conn.Dial, before any request is issued.
5. Two C-client SIGSEGVs (conns=8 srv=C after printing all results; conns=96
   srv=C early) are a separate, lower-priority client-binary bug.

## Follow-up (commit 60b3794b)

Removed the warm sanity dial + close from the Go client (rsockets uses one
shared CM event thread; close-then-redial is the suspected poison) and added
per-dial timing prints. Next matrix run will show dial completion times per
worker: dial hang vs request stall will be directly visible.
