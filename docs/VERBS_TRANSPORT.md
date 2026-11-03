# Native verbs transport — resource lifetimes & buffer ownership

Design doc for the Phase-2 native verbs transport (`pkg/gcache/verbs.go`,
`verbs_impl.go`, `verbs_stub.go`) and the standalone POC
(`deploy/gcache-test/verbstest/vb_echo.c`).

## 1. Resource inventory per connection

| Resource | Count | Lifetime |
|---|---|---|
| `rdma_cm_id` | 1 | dial/accept → `Close()` |
| `ibv_pd` | 1 (per conn; server children) or shared (listener) | setup → teardown |
| `ibv_cq` send | 1 | setup → teardown (after QP destroyed) |
| `ibv_cq` recv | 1 | setup → teardown (after QP destroyed) |
| `ibv_qp` | 1 | created after CQs; destroyed FIRST on teardown |
| registered send buffers | `vbSendDepth` (8) × `vbMsgSize` (64 KiB) | setup → teardown (after QP destroyed) |
| registered recv buffers | `vbRecvDepth` (32) × `vbMsgSize` (64 KiB) ≈ 2 MiB | same |

Per-connection CQ pairs are deliberate (Phase-1 lesson): a wedged or failed
QP can only ever block its own completion polling, never other connections.

## 2. Teardown order (contract)

```
1. mark state closed (wakes all waiters)
2. rdma_disconnect(id)          — QP→SQD: HW flushes outstanding WRs as
                                  IBV_WC_WR_FLUSH_ERR completions, so any
                                  poller in flight gets a WC and returns
3. join recv thread             — it observes the state and exits (bounded 2 s)
4. ibv_destroy_qp               — QP must die before its CQs/MRs are released
5. ibv_dereg_mr (all buffers)   — no WRs outstanding now (flushed in step 2)
6. ibv_destroy_cq (recv, send)
7. ibv_dealloc_pd
8. rdma_destroy_id
```

Every destruction return value is checked and logged. `Close()` is
idempotent (`sync.Once`). Partial initialization frees exactly what was
already allocated (`vconn_free_partial` in the POC; early returns + destroy
in `newVerbsConn`).

## 3. Buffer ownership rules

1. **All wire buffers are C-allocated and `ibv_reg_mr`-registered.** Go
   slices never back RDMA work requests; no Go pointer is retained by C
   across any asynchronous operation (cgo pointer-lifetime rule).
2. **Send slots** (`vbSendDepth` round-robin): a sender copies its data
   into a slot, posts one SEND, and polls the send CQ (deadline-bounded)
   until that slot's WR completes. The slot is released only after its
   completion — never while a WR references it. If the deadline expires
   with the WR still outstanding, the slot stays parked (it is reclaimed
   at teardown; the connection is broken anyway).
3. **Recv ring** (`vbRecvDepth` slots): posted at setup, re-posted as the
   recv thread consumes WCs. The thread owns the recv CQ exclusively —
   no competing poller (the WC-stealing race from Phase 1).
4. **Frame reassembly** happens into Go memory (16 MiB cap): wire chunks
   land in registered buffers, are copied out once, and the buffer is
   immediately re-posted. 4 MiB block responses are assembled from
   64 KiB chunks without blocking the ring.
5. **Framing**: `[u32 total-len LE][payload]`. Payloads > `vbMsgSize` are
   segmented into ≤`vbMsgSize` SENDs (the receiver is size-driven: it
   knows `total` from the header, so a short final chunk needs no flush).
   The 4-byte header travels with the first chunk.

## 4. Deadlines

- Every completion wait is bounded (`OP_TIMEOUT` default 5 s, or the
  connection's `SetDeadline`/`SetReadDeadline`/`SetWriteDeadline` value).
- A deadline miss marks the connection broken; subsequent ops fail with
  `os.ErrDeadlineExceeded` (or the recorded error). No C call can strand a
  Go goroutine: the cgo calls used are all non-blocking (`ibv_poll_cq`)
  or bounded (connect runs on a goroutine with a poll deadline).
- `Close()` interrupts pending operations via the QP flush (step 2).

## 5. Transport selection

| Layer | Mechanism |
|---|---|
| Server | `Config.RdmaProto`: `"rsockets"` (default) or `"verbs"`, explicit; anything else is a fatal config error |
| Client | `--rdma-proto` flag: same values; default `rsockets` so an existing deployment never changes behavior |
| Build | `-tags rdma` for either RDMA transport; plain builds return `ErrRdmaUnsupported`/`ErrVerbsUnsupported` cleanly |
| Fallback | TCP fallback semantics are unchanged (strict-RDMA mode still refuses TCP); verbs participates in the same `Member.RdmaAddrs` failover |

## 6. Verified on hardware (Phase-1/2, ConnectX-7 rail-0)

- 32 connection waves (8-128 conns), rapid connect/disconnect × 10,
  long-lived 30 s: 4,517,030 byte-verified reads, 0 errors, resource
  counters return to zero after every wave (no QP/CQ/MR/thread growth).
- The POC prints live QP/CQ/MR/thread counters per wave and the server
  logs them every 5 s — reclamation is observed, not assumed.
