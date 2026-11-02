# Native verbs RC-QP proof of concept — Phase 1 milestone (2026-11-03)

Objective: demonstrate a native verbs (libibverbs RC queue pairs) data path that survives
repeated connection waves against the SAME server process — the exact scenario that
ghost-serves under rsockets (`RESULTS-stall-matrix.md`). Assignment Phase 1.

## Reproducer

`deploy/gcache-test/verbstest/vb_echo.c` — libibverbs data path (SEND/RECV, explicit receive
posting, per-connection CQ pairs), librdmacm rdma_cm for connection establishment only
(`rdma_create_ep`/`rdma_create_qp`/`rdma_connect`/`rdma_get_request`/`rdma_accept`).
No rsockets anywhere. Framed messages `[u32 len][payload]` with bounded fragmentation
(MAX_MSG 64 KiB per SEND, receiver reassembles); every payload byte verified against an
LCG seeded from a per-request nonce. All buffers C-allocated + `ibv_reg_mr`; send buffers
reused only after their completion is reaped; per-CQ completion threads use bounded
non-blocking polling (100 µs slices) — no global poller, no blocking `ibv_get_cq_event`.

## Bugs found while writing the POC (each fixed and re-verified on hardware)

1. **QP/CQ ordering**: `rdma_create_ep(..., qp_init_attr)` with NULL `send_cq`/`recv_cq`
   lets librdmacm attach its own internal CQs — the app's `ibv_poll_cq` never sees any
   completion (posts succeed, zero WCs). Fix: create the endpoint without a QP, allocate
   PD/CQs, then explicit `rdma_create_qp`.
2. **Unsignaled sends**: with `sq_sig_all=0` a send produces no CQE; the completion-wait
   loop hung forever. Fix: `sq_sig_all = 1` + explicit `IBV_SEND_SIGNALED`.
3. **WC-stealing race**: a background send-completion thread consumed WCs that the poster's
   inline wait needed → starvation hang. Fix: no background send poller; sends are polled
   inline by the poster. The recv thread exclusively owns the recv CQ.
4. **Client echo loop**: sharing the server's echo path on the client caused an infinite
   ping-pong. Fix: separate client receive path (`frame_done` handshake).
5. **CM route/bind**: both sides must bind the rail IP (`VB_SRC_IP`/`VB_SRV_IP`) — the
   mgmt NIC has no RDMA device; a wildcard passive bind dies `EADDRNOTAVAIL` and an
   unbound active route hangs.

## Hardware results (chronometer → sextant, CX7 rail-0 172.16.5.0/24, commit 66bab9ea)

Same server process for ALL legs; every read payload byte-verified; 0 errors everywhere.

| Leg | Result |
|---|---|
| 3 waves × 8 conns × 5 s | 487,836 verified reads, 0 errs — PASS |
| 3 waves × 16 conns | 816,610 verified reads, 0 errs — PASS |
| 3 waves × 32 conns | 965,189 verified reads, 0 errs — PASS |
| 3 waves × 64 conns | 956,398 verified reads, 0 errs — PASS |
| 3 waves × 128 conns | 766,661 verified reads, 0 errs — PASS |
| Long-lived: 8 conns × 30 s | 313,844 verified reads, 0 errs — PASS |
| Rapid connect/disconnect: 10 waves × 8 conns × 2 s | 188,565 verified reads, 0 errs — PASS |
| 64 KiB payloads, 2 waves | 20,163 verified reads, 0 errs — PASS |
| 256 KiB payloads (5-SEND fragmentation), 2 waves | 6,652 verified reads, 0 errs — PASS |

Total: 4,517,030 verified reads across 32 connection waves against one server process —
including the rapid reconnect cycling that wedges rsockets. **No ghost-serve, no stalls,
no errors.**

## Milestone verdict

**Phase-1 milestone MET**: a native verbs RC-QP client/server survives repeated
connection waves (including rapid connect/disconnect cycles and long-lived connections)
against the same server process without ghost-serving, deadlocks, or hangs. Throughput
in this POC (~30-60K req/s per wave) is bounded by the naive synchronous request→echo
round trip (no pipelining, sched_yield polling); latency optimization is Phase 5 work on
the real latencybench stack, not a POC concern.

## Next

- Phase 2: wrap this data path as a `net.Conn` (framed, deadline-aware) behind the
  existing `Transport` interface; buffer-lifetime design doc.
- Phase 3: 8-conn cap experiment in the pool; Phase 5: latencybench TCP/rsockets/verbs
  comparison on the NVMe-backed dataset.
