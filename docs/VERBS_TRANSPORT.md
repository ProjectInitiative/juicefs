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
1. mark state Closed            — wakes all waiters (Read cond, recvLoop, accepts)
2. QP → IBV_QPS_ERR (if Live)   — HW flushes outstanding WRs as
                                  IBV_WC_WR_FLUSH_ERR completions, so any
                                  poller in flight gets a WC and returns
3. wait for the dial goroutine  — Close must never free the CM id (or anything
                                  else) while rdma_connect is inside C. Bounded
                                  by DialTimeout+2s; past that the id is LEAKED
                                  (reported) rather than freed under an active
                                  cgo call — a bounded, observable leak beats
                                  memory corruption. (DialTimeout is 5 s.)
4. join the recv thread         — unconditional; its polls are non-blocking
                                  (ibv_poll_cq) and it exits on the state flip,
                                  so this returns promptly. NO timeout-and-
                                  free: a wedged loop means leak (reported),
                                  never use-after-free.
5. rdma_destroy_qp              — the counterpart of rdma_create_qp (clears
                                  id->qp); the QP dies BEFORE its CQs and MRs,
                                  which it can still reference via posted WRs
6. ibv_dereg_mr + free (all bufs)
7. ibv_destroy_cq (recv, send)
8. ibv_dealloc_pd
9. rdma_destroy_id
```

Every destruction return value is checked; a failed step is returned from
`Close()` as a typed error and the C side returns a step bitmask
(`VB_TEARDOWN_*`) that `vbDestroyConn` maps to the specific failing call.
`Close()` is idempotent (`sync.Once`) and safe for Live, Broken, and Init
states (Init with no C resources allocated is a no-op).

### CM-id ownership

The `rdma_cm_id` is owned by whoever created it:

- **dial** (`verbsDialImpl`) creates it and destroys it if `newVerbsConn`
  fails; `newVerbsConn` never destroys it (its internal `destroy_id` flag is
  fixed false).
- **accept** (`verbsListener.Accept`) creates per-conn ids via
  `rdma_get_request` and destroys them only when `newVerbsConn` fails.
- a fully-built `verbsConn` owns its id through `Close()`.

This makes double-destroy impossible: exactly one owner destroys exactly once.

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

## 7. Hardening pass (post-Phase-2 review)

### Read/queue synchronization

The frame queue is produced by the recvLoop and consumed by `Read`:

- `framesMu` guards `frames` and `queuedBytes` (the producer/consumer
  boundary).
- `rmu` serializes ENTIRE `Read` calls — a frame is never copied by two
  concurrent readers and never double-popped (the pop happens under
  `framesMu` only after the frame is fully consumed, verified by identity
  against `frames[0]`).
- `framesCond` is broadcast by ingest, `markBroken`, and `Close`; `Read`
  waits on it instead of sleep-polling.

### Bounded receive queue & backpressure

Frames awaiting the reader are capped at `vbMaxQueuedFrames` (128) and
`vbMaxQueuedBytes` (16 MiB). When either watermark is hit the recvLoop stops
draining the recv CQ: the 32-slot ring fills, the RQ stops accepting, and the
peer's SENDs back up at transport level. Memory per connection is therefore
bounded at ~2 MiB (ring) + 16 MiB (queue) + 512 KiB (send pool) regardless of
reader behavior.

### Deadlines

- Sentinel `0` (never set) = wait indefinitely with periodic state re-checks.
- Any explicit deadline is re-loaded on every wait iteration, so
  `SetReadDeadline`/`SetWriteDeadline` issued while an operation is pending
  take effect immediately (verified by
  `TestReadHonorsDeadlineUpdateWhilePending`).
- `Write` re-loads the write deadline per chunk of a segmented frame.
- Zero-length `Write` is a no-op (an empty wire frame would otherwise be a
  framing ambiguity the gcache protocol never uses); zero-length `Read`
  returns `(0, nil)` without consuming.

### Listener shutdown

`verbsListener.Close()` flips `closed`, closes `wakeCh` (interrupting every
pending `Accept` immediately with `net.ErrClosed`) and destroys the listening
CM id exactly once (`closeOnce` + nil check). `Accept` never polls a dead id.

### Resource accounting

`vb_destroy_conn_impl` returns a bitmask of failed steps; `vbDestroyConn`
converts it to a typed error naming the failing call and errno. A clean
teardown and a failed teardown are distinguishable by the error path alone.

### Test coverage (hardware-independent, `-race` clean)

`pkg/gcache/verbs_test.go`: split-prefix reassembly, multi-SEND fragmentation,
stream reassembly across consecutive reads, zero-length ops, concurrent
readers (serialized, no double-pop), queue accounting under backpressure,
deadline update while a Read is pending, oversized/short-chunk rejection,
`Close` idempotence for Init/Broken/Closed. Hardware paths (wave matrix,
resource counters, real CM teardown) remain covered by
`deploy/gcache-test/k8s-lab/96-phase2-validate.yaml` on the CX7 rail.

### Frame-stream reassembly (bug 11: silent chunk-byte loss)

Hardware validation found a **frame-stream desync** under load: the server
parsed `off=0x06014347` (`'G','C',1,0x06` — the request magic) and
`len=0x1000000E00` (`[klen=14 wire prefix][0x10 pair wire prefix]`) as one
request's off/len pair — the classic 4-byte slip. Signature: "invalid range
off=..." errors with garbage values, plus correct-length payload mismatches
(`first-diff@0`), at low rate (F2: 36 errors/9 mismatches; F3: 4/2; F4: 4/1
of ~13.5k reads), always under the highest-concurrency legs. Both peers
logged perfectly well-formed recv WCs — the corruption was between the WC
copy and the frame the reader saw.

Root cause in `ingest` (both directions, client and server):

1. **Silent truncation** — `copy(c.pending[pendingLen:], chunk)` discards a
   chunk tail that exceeds the current frame's remaining capacity. A chunk
   carrying `hdr20 + keyPrefix4` completed the 20-byte header frame and
   dropped the key's 4-byte length prefix; every subsequent frame was then
   assembled 4 bytes off.
2. **Missing completion** — a chunk carrying a whole frame
   (`len(chunk) == 4+total`) was staged but never completed; completion fired
   on the *next* chunk, silently dropping its bytes.

Fix (commit `ca0724b1`): `ingest` is now a **strict stream consumer** — every
input byte is consumed exactly once, a frame completes the moment its last
byte arrives, and any chunk tail rolls into the next frame; sub-4-byte
fragments are carried (`head` buffer) instead of rejected. The reassembler is
now correct for ANY chunk boundary, including coalesced SENDs and byte-grain
splits. Regression tests: whole-frame chunk, three coalesced frames in one
chunk, byte-by-byte boundary churn, mid-prefix fragment carry (`-race` clean).

### Receive-path bugs 12 & 13: CQ overrun and torn frame-queue headers

Post-bug-11 hardware runs still corrupted the frame stream at low rate under
the NVMe qd=64 leg: the client's verify buffer held the *response header*
(`got16=47430102...`) while the server provably served correct bytes
(`srv range ... data16` == `exp16`). A leaked server-side error decoded the
request pair as `hdr[13:20]+key[0:9]` — completion entries were being lost
in **both** directions, between the NIC and the reader.

**Bug 12 — recv CQ overrun** (`3f231cb1`): the recvLoop slept *before*
polling the recv CQ whenever the Go frame queue hit its watermark, so WCs
piled up unpolled past the CQ depth (32). A verbs CQ overrun silently drops
completion entries — exactly the missing-chunk signature. Fix: `vbRecvDepth`
32→512 and the recvLoop now always polls + re-posts the buffer immediately
after the copy (before ingest). Backpressure is the recv ring itself: once
all buffers are outstanding, the peer's SENDs park on RNR rather than data
being lost. The old `vbMaxQueuedFrames/Bytes` watermarks are advisory.

**Bug 13 — torn frame-queue slice headers** (`3b5cea33`): `ingest` appended
to `c.frames` without `framesMu` while `Read()` re-sliced the same slice
under the lock. Concurrent append+pop tore the backing array; the hardware
faulted in `Read`'s memmove with a garbage data pointer. The bug-11-era
queue-full check had incidentally serialized ingest; fixing bug 12 exposed
the race. Fix: take `framesMu` around the append + byte accounting.

Shakeout: the C `vb_conn` struct's fixed `recvbufs[32]` array must track
`vbRecvDepth` (`e7de73b6` — dial-time panic otherwise).

Result: the F2 NVMe qd=64 leg (the case that failed every previous run)
passes with `errors=0 mismatches=0` and "verify: all payloads correct"
(iops=1907 probe / 1769 matrix).
