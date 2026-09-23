# gcache-on-Sparks test design

Status: design (apply-only step left to user; agent kubectl is read-only)
Targets: chronometer (172.16.4.55) + sextant (172.16.4.56) — both arm64
DGX Sparks (label `gpu: dgx-spark`), k3s 1.35, containerd 2.2.3, NixOS.
Third Spark arriving → ring grows to 3; design accounts for it.

## What we learned from llm-test live configs

* Benchmarks live in `llm-test` ns with a controller-Job + child-Job
  pattern, pinned images (`registry.taildeab2.ts.net/homelab/...`), fio with
  O_DIRECT, TTL'd jobs, and a `juicefs-creds` VSO Secret already in
  `juicefs-platform`.
* The JuiceFS CSI driver (v0.33.0) is on every node with `juicedata/mount:ce-v1.4.1`.
* No Multus/SRIOV device plugins — RDMA in-pod would need the host network
  or Multus later; v1 tests run mounts in **hostNetwork** pods (RDMA userland
  sees the host's IB stack directly; this is also what enterprise-style
  `mount.rdma` deployments effectively do).
* No Mellanox driver present on capstan/astrolabe; ConnectX-7 driver status
  on the Sparks must be confirmed at first run (nvidia MLNX_OFED on DGX
  Spark is typically present or installable; `rdma link` decides).

## Phase 0 — reachability + baseline (no cluster changes needed)

Standalone job per Spark (privileged, hostNetwork), sequential:
1. `rdma link` / `ibv_devices` inventory (record in logs for the ring doc)
2. `rping -s` on chronometer, `rping -c` from sextant over the C7 fabric
   IPs (or soft-RoCE `rdma_rxe` fallback on the mgmnt NIC if CX7 driver is
   absent — records that fact and still exercises the full code path)
3. `juicefs objbench` against Garage direct + HAProxy endpoints (their
   existing matrix) — baseline object-storage throughput to subtract later

## Phase 1 — two-mount TCP cache-group over the C7 fabric (core test)

The centerpiece: identical to the local smoke test but across real nodes.

* Meta: reuse the existing `juicefs-platform` Postgres (CNPG) — create
  dedicated DB `gcache_sparks` via init job (creds from a new Secret we
  generate; no VSO dependency for the test ns).
* Bucket: dedicated Garage bucket `gcache-sparks` (their existing 3-node
  Garage, direct endpoint, not HAProxy, to remove proxy variance).
* member-chronometer + member-sextant: hostNetwork privileged pods,
  `--cache-group=sparks`, cache on the node's NVMe hostPath (their SC
  conventions: host-local-path-nvme → hostPath under /var/lib/rancher or
  dedicated dir), `--group-listen=<c7-ip>:3910` (explicit C7 IP — fabric
  isolation from mgmnt), pinned image from our local registry (arm64 build
  required — multi-arch or arm64-specific tag).
* **Images must be built for arm64.** Our current image is x86-only (built
  on astrolabe). The Dockerfile is multi-stage golang: build with
  `--platform linux/arm64` via buildx or build on a Spark node itself
  (docker is present on the host; simplest: build on chronometer).
* Success criteria (verify job on each member, then cross-node):
  - both members in `jfs_gcache_members` with their C7 IPs
  - 64MiB written on A reads identically on B
  - B's `juicefs_remotecache_gets` > 0 AND A's serve-side byte counter
    matches B's fetch bytes (end-to-end accounting)
  - failover drill: kill member A pod → B falls through to object storage
    (remotecache_errors spike then flat, reads still succeed); restart A →
    re-add (`add peer ... back`) and peer fetches resume
  - ring growth drill: apply member-c (3rd Spark when it arrives) → both
    existing members converge within ~3 heartbeats, ~1/3 of keys re-owned,
    no downtime
* Metrics: scrape `:9900/metrics` on both members; compare
  remotecache_gets/bytes/errors + peer_failures vs the objbench baseline.

## Phase 2 — TCP saturation + tuning (the "is TCP the bottleneck" answer)

Their existing tcp-tuning-matrix pattern, now cache-group flavored:
* parallel readers (their multi-reader bench) against a 8GiB dataset while
  capturing `remotecache_bytes` rate — this is the peer-path throughput.
* compare: direct Garage read (objbench numbers) vs peer-path read at same
  concurrency. If peer ≈ object-storage path → TCP not the bottleneck →
  RDMA is a CPU optimization, not throughput.
* measure CPU: containerd/cgroup CPU of both member pods at each load step
  (their controller pattern logs this) — the RDMA win is CPU/headroom.

## Phase 3 — RDMA (rsockets) over the C7s

Prereq: rsockets backend implemented behind the build tag (design + seam
landed; backend is a focused follow-up) + MLNX_OFED/rdma-core on both
Sparks + rping green over the fabric.
* rebuild image with `-tags rdma` (arm64), same manifests + `--rdma-network=<c7-nic>`
* dual-listener: same test as Phase 1 — same workload, same metrics
* compare per-load-step: remotecache_gets/bytes at equal concurrency TCP
  vs rsocket; CPU of both member pods; then flip `IORPC_RDMA_RETRY_DURATION`
  drill: `ip link set <c7> down` on A → failover to TCP (reads continue) →
  `up` → failback after backoff (log shows rsocket dials resume)
* acceptance: rsocket peer-path ≥ TCP peer-path throughput at equal CPU,
  or equal throughput at ≤50% of TCP's CPU (enterprise's documented win)

## Ring growth (3rd Spark)

Zero reconfig: new member joins with same `--cache-group=sparks`; HRW
moves only ~1/3 of keys. Watch: both old members converge in one settle
beat; verify `remotecache_gets` from all three; `jfs_gcache_members`
shows 3 rows. This is exactly the enterprise "scale out" property we get
from rendezvous hashing for free.

## Infra checklist for the user (when we get there)

1. Confirm C7 driver + IPoIB/RoCE IP on both Sparks (`rdma link`, `rping`)
2. Grant agent SA create/get-update rights in `gcache-test` ns (or apply
   the manifests; agent verifies via logs/metrics)
3. DB + bucket creation (init job handles it with the generated Secret)
4. arm64 image build (on a Spark or buildx) — deploy/gcache-test/Dockerfile
   is multi-stage and platform-agnostic already
5. Optional but nice: Multus + RDMA net-attach-def per C7 for in-pod RDMA
   without hostNetwork (phase 3+ refinement)
