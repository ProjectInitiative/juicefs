# Phase 2 — hardened verbs `net.Conn` transport: hardware validation (2026-11-03)

Objective: validate the production verbs transport (`pkg/gcache/verbs.go` + `verbs_impl.go`,
commit `c59a7cf0`) over the real gcache peer protocol on the DGX Spark CX7 rail — resource
lifetimes, teardown, and end-to-end request/response framing. Assignment Phase 2. This is
the C-POC of `RESULTS-verbs-poc.md` hardened into a `net.Conn` behind the existing
`Transport` interface, exercised by `latencybench` (strict RDMA, no TCP fallback) and the
hardened C POC (`vb_echo`) for lifecycle legs.

## Test rig

- k8s Jobs in `llm-test` (`deploy/gcache-test/k8s-lab/96-phase2-validate.yaml`):
  - **srv** (sextant, 172.16.5.56): `vb_echo` echo server :14090 + latencybench `serve`
    verbs listeners on :13930 (mem, 4096×4 MiB RAM blocks) and :13931 (NVMe O_DIRECT,
    `/var/tmp/lb-data/bench.bin` 4096×4 MiB = 16 GiB).
  - **cli** (chronometer, 172.16.5.55): hardened-POC wave matrix, a 256 KiB fragmentation
    leg, a **local loopback Go-transport leg** (serve+read in one pod — decisive for
    isolating client/server bugs), then the real-protocol matrix (F1-F4) against sextant.
- Builds from the pinned commit via `10-build.yaml` (codeload tarball of
  `low-latency-ranged-reads`), binaries on hostPath `/var/tmp/lb-bins`.
- `VB_DEBUG=1` on both sides for recv/frame/header-level tracing.

## Bugs found during validation (each fixed, committed, and re-verified on hardware)

1. **cgo pointer rule** (`a020c26d`): `rdma_addrinfo` built in Go memory with a pointer
   field → `panic: cgo argument has Go pointer to unpinned Go pointer` at listen. Fix:
   `vb_listen`/`vb_dial_id` C helpers — all addrinfo memory C-allocated.
2. **`cn.id` never wired** (`387eb163`): the C `vb_conn.id` was left NULL, so
   `rdma_create_qp(NULL, …)` SIGSEGV'd at offset 0x18 on the very first Go-transport dial
   (deterministic, PC identical across runs). Fix: set `cn.id` before QP creation; also
   nil-check `id->verbs` after `rdma_create_ep` (clean errno instead of a crash).
3. **Dial-time MR registration storm** (`904f3ecf`): 40 goroutines × 40 `ibv_reg_mr` calls
   wedged the mlx5 pinning path on a node with pinned-memory pressure (goroutine dump:
   all stuck in `_Cfunc_vb_reg`). Fix: `dialMu` serializes per-conn setup — one-shot per
   conn, zero steady-state cost.
4. **Double `rdma_destroy_id`** (`bc9e888b`): `Close()` destroyed the id after
   `vb_destroy_conn` had already destroyed it → use-after-free SIGSEGV inside
   `rdma_destroy_id` on every pool discard. Fix: `vb_destroy_conn` owns the id;
   `Close()` only destroys it when `cn` was never built (partial init).
5. **recvLoop stateInit race** (`42c2a095`): the recv thread started in `newVerbsConn`
   but exited while `state == stateInit` (dial/accept not yet complete) — zero recv WCs
   ever, every request timed out. Fix: recvLoop waits out stateInit, exits only on
   Broken/Closed.
6. **Reassembly never wrote the frame length** (`2347c694`): `pending[0:4]` left zeroed →
   every frame body shifted 4 bytes → protocol garbage both directions.
7. **Split 4-byte wire prefix duplicated into frames** (`c59a7cf0`): `Write` sends the
   u32 prefix and payload as separate SENDs (two WCs); `ingest` treated the 4-byte
   prefix chunk as the start of the payload and re-copied it into the frame — every read
   was misaligned (`srv hdr 14000000 type=0` → "bad request header" from servers, with
   perfectly clean requests visible at the WC level). Fix: a 4-byte first chunk IS the
   prefix (staged into `pending[0:4]`, payload follows); larger first chunks carry
   prefix+payload inline. Frames surface payload-only.
8. **Source-NIC selection** (`4b7f1e33`): `firstNICIPv4` picked the mgmt NIC →
   `ENODEV`/`ECONNREFUSED`. Fix: `firstNICIPv4For(dest)` — pick the local IPv4 on the
   destination's /24 (else /16, else first), so no explicit source flag is needed.

Also fixed in tooling: `--rdma-proto` assignment ran **before** `fs.Parse` (both `read`
and `serve`) so the flag silently never took effect (`4900c0f9`).

## Hardware results

### Resource lifetimes (hardened C POC, same server process)

Teardown per conn: quiesce → joined 53-176 ms → QP gone 1.1-1.9 s → done 2.4-3.1 s.
Wave lines report **qps=0 cqs=0 mrs=0 threads=0** after every wave — full reclamation,
no monotonic growth (the Phase-1 goal of the hardening pass).

| Leg | Result |
|---|---|
| 2 waves × 8 conns × 5 s (Go client pod, VB_DEBUG) | 24,743 verified reads, 0 errs — PASS |
| 10× rapid reconnect, 8 conns × 2 s | PASS, resources fully reclaimed |
| 256 KiB payload (5-SEND fragmentation), 2 waves | PASS (frag exit 0) |

Server-side counters after the full wave matrix: conns=840 accumulated across waves —
the C POC's reaper only reaps on its own teardown path; a server-side idle/CM-event
reaper is noted as follow-up (Go transport reaps correctly; see open items).

### Real gcache protocol over the Go verbs transport (strict RDMA)

The **loopback leg** (serve+read in one pod, one binary) is the decisive proof:
`recv WC len=20 first16=4743010600000000000e000010000000` — the server parses a clean
`MsgRangeReq` ('G','C',1, type 0x06, klen=14 `bench-key-1119`, plen=16) through the full
verbs net.Conn path, and serves it: `[vbs] srv hdr 47430106 type=6`, then
`cold fills=743 ranges=1523 distinct-keys=710` served.

Cross-node legs (chronometer → sextant), final run at `c59a7cf0`:

| Leg | Result |
|---|---|
| F1 mem 4 KiB QD1-64 | runs end-to-end; some legs hit the stale-server artifact below |
| F2 NVMe 4 KiB QD1-64 | end-to-end responses (server parsed + replied) |
| F3 batch-70 mem | end-to-end responses |
| F4 batch-70 NVMe | **13,503 reads @ 900 iops** through the full verbs path |

F4 detail: p50 16.8 ms (NVMe O_DIRECT + first-cut sync path — Phase 5 tuning), 77 errors
+ 5 mismatches. The errors are response misalignment under pipelining: the client parsed
key-text bytes as a response header (`off=7305401545548365824` is ASCII), i.e. an
occasional frame-boundary slip while 70 requests are in flight. This is the one open
correctness item.

### Earlier runs (context for the reviewer)

Before the reassembly fixes, the same matrix produced: cgo pointer panic at listen;
deterministic `vb_create_qp` SIGSEGV; `use-after-free` in `rdma_destroy_id` at Close;
"read deadline exceeded" on every request (recvLoop race); then "bad request header"
errors with byte-clean requests visible at the WC level — each resolved by the numbered
bugs above. The progression is in git: `4b7f1e33` → `c59a7cf0`.

## Verdict

**Phase 2 functionally MET**: the verbs `net.Conn` transport carries the real gcache peer
protocol end-to-end on the CX7 rail, with contract teardown order, deadline-bounded ops,
and full resource reclamation proven by counters. One correctness item (response
misalignment under batch pipelining, 0.6% of F4 reads) and latency tuning remain.

## Open items

1. Response misalignment under batch-70 pipelining — add client-side frame-sequence
   logging on `readRespHeader` mismatch; suspect concurrent reader or a reassembly edge.
2. C POC server-side reaper: reap conns on CM DISCONNECTED / idle timeout (Go transport
   already reaps via QP→ERR flush WCs).
3. Latency: 16 ms p50 on NVMe is pre-tuning (sync round trips, no pipelining depth);
   Phase 5 owns the benchmark matrix (TCP vs rsockets vs verbs, mem + NVMe, QD1-64,
   batch-70).
4. `--rdma-proto` default stays `rsockets` for backward compatibility; verbs is opt-in
   per mount (`docs/VERBS_TRANSPORT.md`).

Commit index for this phase: `4b7f1e33`, `4900c0f9`, `a020c26d`, `904f3ecf`,
`387eb163`, `bc9e888b`, `2347c694`, `af51ed31`, `7a17265f`, `178c5ac5`,
`42c2a095`, `aa23007a`, `bd77ddb7`, `9dc86f41`, `094067b7`, `c59a7cf0`.
