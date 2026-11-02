# Cache network topology — switchless rings, switched fabrics, and everything between

Status: implemented on `low-latency-ranged-reads`. Each capability is tagged
**[hardware]** (validated on the DGX Spark CX7 fabric / capstan 10GbE) or
**[simulation]** (unit tests with fake transports; not yet run on real
multihop hardware).

## 1. The invariant: logical ring ≠ physical topology

Cache ownership comes from the rendezvous hash over the member list. The
physical network — which rail, how many hops, whether a switch is involved —
changes only WHICH ENDPOINT a client dials, never WHO owns a key. Nothing in
the topology layer migrates ownership or re-hashes the ring.

## 2. Member endpoints

```go
type Member struct {
    Addr      string   // primary TCP endpoint
    Addrs     []string // all TCP endpoints (optional)
    RdmaAddr  string   // single RDMA endpoint (switched fabrics; legacy)
    RdmaAddrs []string // link-specific RDMA endpoints (switchless rings)
}
```

**[hardware]** On the DGX switchless ring a node's RDMA address is
**per-pair**: chronometer is `172.16.5.55` toward sextant and `172.16.7.55`
toward octant — the rails are isolated /24s. A client therefore needs the
endpoint that is reachable FROM ITSELF, tried in order, with per-ENDPOINT
failure backoff (one dead rail must not take down the pair's other rail).

Order tried: `RdmaAddr` first, then `RdmaAddrs`; TCP (`Addr`, then `Addrs`)
last. Strict mode (`SetStrictRDMA`) tries every RDMA endpoint and fails with
an aggregated error — never TCP. **[hardware]** verified.

## 3. The connectivity graph

`latencybench topo` (or `RingClient.ProbeTopology`) probes every endpoint and
classifies each edge:

| class | meaning | hops |
|---|---|---|
| `direct-rdma` | endpoint IP inside a local interface subnet (point-to-point rail / same L2) | 0 |
| `routed-rdma` | rsockets-reachable but outside local subnets (switched fabric, IP-routed RoCE) | 1+ |
| `tcp` | no RDMA endpoint; TCP is the path | — |
| `unavailable` | probe failed | — |

**[hardware]** verified on chronometer→sextant: two `direct-rdma` rails and
the control-plane endpoint, with per-endpoint dial latency.

Graph sources, composable:
1. `Member.RdmaAddrs` — explicit config (mount flags / registry).
2. **`MembersFromFabric`** — the DGX `/var/lib/dgx-spark/rdma-fabric.json`
   inventory (schemaVersion 1): builds per-pair rail endpoints + control-plane
   TCP addresses automatically. **[hardware]** parser tested against the live
   inventory.
3. **NCCL exports** (`NCCL_TOPO_FILE`, `NCCL_DEBUG_FILE` with
   `NCCL_DEBUG_SUBSYS=INIT,GRAPH`): an OPTIONAL input mapping GPU rails to
   cache endpoints. NCCL is never a transport for filesystem requests —
   its XML/diagnostics only suggest which interfaces exist. Not yet
   implemented beyond this contract note.

Benchmark results record `endpoint`, `class`, and `hops` per run — a
`rsocket-strict` label with `hops=0` is provably single-link RDMA; there is
no silent fallback anywhere in strict mode.

## 4. Topology support matrix

| Topology | Status |
|---|---|
| Direct 2-node link (1+ rails) | **[hardware]** — chronometer⇄sextant, 2 rails, failover tested via endpoint failure |
| 3-node fully connected ring (switchless) | **[hardware]** — 6 rails exist; graph + failover validated on 1 pair, direct-dial to octant's rail verified by probe |
| 4+ node physical ring (nonadjacent pairs) | **[simulation]** — endpoint lists + backoff are node-count-independent; nonadjacent access needs §5; no 4-node hardware available |
| Switched fabric | **[simulation]** — a single routed `RdmaAddr` per member is the expected shape; classification marks it `routed-rdma`; no switch on site |

## 5. Nonadjacent nodes: evaluated options

For nodes without a direct cable (4+ node rings), in order of preference:

1. **IP-routed RoCE** — if the rails' subnets are routed between nodes
   (requires host forwarding/routes — an ADMIN action, never automatic), the
   existing rsockets dial simply works; the graph classifies it
   `routed-rdma, hops>=1`. Nothing new to implement. NOT validated: the lab
   rails are isolated by policy (and `rdma-fabric.json` shows no gateway
   role); enabling forwarding is a documented admin step, not code.
2. **Application-level relay** — member B (adjacent to both A and C) answers
   a relayed RangeReq for C's key. Design: new message type with
   `originator UUID` + `hop count (max 2)`; a member forwards only if it is
   NOT the key's owner (ownership is never claimed by a relay) and only
   toward the owner; loop prevention = hop count + originator dedup. The
   relay adds one link of latency (~RTT) — acceptable for cold paths only.
   **Status: design only — NOT implemented.** It cannot be validated on the
   lab's fully-connected 3-node ring, and the requirement is to implement
   only what can be validated.

## 6. What runs concurrently with NCCL

The cache speaks rsockets/TCP on its own endpoints; NCCL uses the rails for
tensor traffic. There is no shared transport. The rails ARE shared physical
links, so contention is possible when NCCL saturates a rail the cache also
uses — the per-pair multi-endpoint lists let an operator point cache traffic
at the quieter rail, and the concurrent-load benchmark (below) measures the
effect directly.

## 7. Known limitation (blocking rsockets batch-70)

`rsocket()` in every rdma-core we could test (44, 62) rejects
`SOCK_NONBLOCK` (EOPNOTSUPP), so rsockets fds are blocking; all waits are
bounded by our poll-first loops and per-request deadlines, but under
sustained ≥8-connection load the datapath can deadlock inside librdmacm
(both sides freeze; the server needs a restart). Characterized on hardware;
see RESULTS-dgx-rail.md. Options: upstream investigation, or the planned
verbs RC-QP transport (the Transport interface already isolates it).
