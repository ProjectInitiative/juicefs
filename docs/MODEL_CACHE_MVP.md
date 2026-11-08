# model-cache distributed cache MVP: activation record

Status: **GitOps fix merged; live activation and peer-read proof pending** (2026-11-08 05:05 UTC). Do not mistake a custom image or a Ready PVC for a working ring.

## Identity and scope

- Target: `llm-test/model-cache`, Bound RWX 3 TiB, UID `65282e9c-4289-4e6d-bd05-906ba6629699`, StorageClass `juicefs-sc`.
- Original PV (unchanged at last observation): `pvc-afa2ecdd-04ae-4af9-9e08-e1ea1c6d1947`, UID `887efc24-4856-42c6-b17b-17fb4c22bd3d`, CSI driver `csi.juicefs.com`, same volume handle. The filesystem remains the existing PostgreSQL metadata / Garage S3 JuiceFS filesystem; neither backend nor PVC/PV was edited.
- `llm-test/model-cache-ephemeral` has separate backing and is **not** a ring member.
- Active consumer: `gate-inspector` on capstan3. Its existing stock Mount Pod `juicefs-capstan3-pvc-afa2ecdd-04ae-4af9-9e08-e1ea1c6d1947-kdjlwu` must not be replaced without checking the consumer; no mount Pod was manually replaced here.
- TensorFold v14 head and worker remain scaled to zero. Three temporary `gcache-ring-probe-{chronometer,sextant,octant}` pods completed; their DGX Mount Pods were subsequently removed by CSI. Do not recreate an existing live capstan3 mount merely to pick up new options.

## Root cause and scoped correction

Installed CSI driver: `juicedata/juicefs-csi-driver:v0.33.0`. Its [`GenMountPodPatch` implementation](https://github.com/juicedata/juicefs-csi-driver/blob/v0.33.0/pkg/config/config.go) merges **all** matching patches in list order. The `merge` function **replaces** `mountOptions` with the final matching list, rather than appending. The earlier PVC-specific ring patch matched, as shown by the custom mount image and host networking, but the later broad `juicefs-sc` capstan3 default replaced the ring option list. Previous fresh DGX Mount Pods therefore used the pinned custom image yet ran with `/mnt/jfs-cache` and **no** `cache-group` or peer flags. This is not a selector failure or a reason to upgrade CSI.

Homelab `clusters/mc.yaml`: move the exact `matchName: model-cache` / `matchStorageClassName: juicefs-sc` patch **to the end** of `mountPodPatch`. Keep all unrelated node-local and Argo PVC defaults. Remove `rdma-network`, `rdma-proto`, and `rdma-fallback` from this ring entry, leaving the fork's default TCP peer transport on port 13910. Custom Mount Pod image remains `local-registry.registry.svc.cluster.local:5000/homelab/juicefs-gcache-csi:gcache-strict-sharding-arm64@sha256:be6ece264ef6c40e7a8e69e6323df95bc5ba92ac0b9e0522679e858b8cff3440`. Intended flags include `cache-dir=/mnt/pool/juicefs-ring`, `cache-size=550000`, `cache-group=dgx-model-cache`, `group-listen=:13910`, `group-advertise=auto-c7`, `peer-range-read`, `group-heartbeat=5s`, and `remote-timeout=2s`.

- Homelab commit `c4beed9`, PR [#313](https://github.com/ProjectInitiative/homelab/pull/313), merged as `ddcdae759c015aebde51ac258239058efdcf4b52` to `main`; `dev` was fast-forwarded and pushed.
- `nix run .#generate-manifests` passed; rendered `.direnv/manifests/mc.k8s.yaml` shows target patch last and no RDMA flags. The Argo Application targets chart `0.33.0` with `globalConfig.manageByHelm: true`, `automated.prune/selfHeal: true` and the MC cluster.
- **Live ConfigMap had not yet reconciled at 05:05 UTC**: `kube-system/juicefs-csi-driver-config` was still resourceVersion `629875246`, with the old patch order and RDMA flags. The agent's MCP identity cannot patch/update ConfigMaps in kube-system or read Argo Applications on the control cluster. Do not launch another mount test until the live ConfigMap is confirmed updated.

## Acceptance checks still required

1. Verify the live ConfigMap has the ring patch **last** and no `rdma-*` entries. If Argo does not reconcile, inspect/sync the `argocd-mc/juicefs-csi-driver` Application using an identity authorized for the control cluster; do not bypass GitOps with a permanent drift patch.
2. On chronometer and sextant, start only read-only, bounded PVC consumers after checking for other consumers and node-local `/mnt/pool/juicefs-ring` paths. Inspect only **mount options**, not the full container startup command (which embeds credential-shaped material). Confirm both fresh Mount Pods use the digest above, TCP group flags, and persistent NVMe cache path. The capstan3 mount must remain untouched.
3. Confirm a two-member group in the existing PostgreSQL-backed membership registry; check peer listener/advertised ConnectX addresses and mount metrics without reading Secrets. Select a real model-weight shard already on the PVC and verify a bounded streamed read against an authoritative SHA-256. Capture before/after `juicefs_remotecache_*` and owner-side cache/S3 counters to distinguish an NVMe-served peer hit from object-store fill. No such proof has been observed yet.
4. Only after a proven peer hit, review `juicefs warmup` and `--fill-group-cache` owner semantics; warm one shard at bounded concurrency and verify per-node residency and memory. Do not initiate full-model or Engram warmup without identifying the actual files and ownership behavior.

## Rollback

Revert homelab commit `c4beed9` (via PR/Argo) to restore the prior patch ordering. It changes only future Mount Pod generation, not PVC/PV or backends. To restore a **live DGX** mount to previous flags, first stop only its test consumer and let CSI unpublish/delete that node's target Mount Pod, then recreate the test consumer after ConfigMap reconciliation. Never delete the capstan3 Mount Pod while `gate-inspector` uses it. Preserve `/mnt/pool/juicefs-ring`, existing node-local caches, PostgreSQL, and Garage data throughout.
