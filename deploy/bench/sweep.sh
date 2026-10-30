#!/bin/bash
# deploy/bench/sweep.sh — full Phase-1 matrix: sizes × queue depths, raw NVMe
# vs JuiceFS FUSE mount, plus the inference batch workload via latencybench.
# Run ON a DGX Spark node. Single-line commands only (Kyle's terminal rule).
set -euo pipefail

OUT=${OUT:-docs/bench/$(hostname)-$(date +%Y%m%d-%H%M%S)}
DEV=${DEV:-/mnt/nvme/baseline.bin}   # >= 64 GiB file on a local NVMe
MNT=${MNT:-/mnt/jfs}                 # JuiceFS mount point (with cache-group)
FILE=${FILE:-$MNT/engram/table-0.bin} # >= 64 GiB file inside the mount
LB=${LB:-./latencybench}

mkdir -p "$OUT"

echo "== 1) raw NVMe baseline (O_DIRECT) =="
for bs in 4k 16k 64k 4m; do
  for qd in 1 8 32 64 128; do
    fio deploy/bench/randread.fio --filename="$DEV" --bs=$bs --iodepth=$qd \
      --output="$OUT/fio-nvme-$bs-qd$qd.json" --eta=never
  done
done

echo "== 2) JuiceFS FUSE: warm (page cache) vs cold (fadvise-dropped) =="
for qd in 1 8 32 64 128; do
  $LB read --backend fuse --path "$FILE" --len 4096 --qd $qd --secs 10 --out "$OUT/lb-fuse.json"
  $LB read --backend fuse --path "$FILE" --len 4096 --qd $qd --secs 10 --drop-cache --out "$OUT/lb-fuse-cold.json"
done

echo "== 3) inference workload: 70 concurrent 4 KiB reads per round =="
$LB read --backend fuse --path "$FILE" --len 4096 --batch 70 --secs 20 --drop-cache --out "$OUT/lb-fuse-infer.json"

echo "== 4) peer transport (run latencybench serve on a peer node first) =="
# on peer (TCP): ./latencybench serve --addr 0.0.0.0:13910 --src file --file /nvme/dataset.bin --blocksize 4194304 --direct
# on peer (RDMA, strict — no TCP listener): ./latencybench serve --rdma --src file --file /nvme/dataset.bin --blocksize 4194304 --direct
#   (serve prints the actual bound rsockets port — rgetsockname discovery)
# dataset must be >= KEYS x 4MiB; --keys 16384 -> the full 64GiB working set
# full-block path (pre-change baseline):
for qd in 1 8 32 64 128; do
  $LB read --backend peer --addr PEER:13910 --keys 16384 --len 4096 --qd $qd --secs 10 --out "$OUT/lb-peer-tcp-fullblock.json"
done
$LB read --backend peer --addr PEER:13910 --keys 16384 --len 4096 --batch 70 --secs 20 --out "$OUT/lb-peer-infer-fullblock.json"
# ranged path (--range is REQUIRED for the fast-path protocol):
for qd in 1 8 32 64 128; do
  $LB read --backend peer --addr PEER:13910 --keys 16384 --range --len 4096 --qd $qd --secs 10 --out "$OUT/lb-peer-tcp-range.json"
done
$LB read --backend peer --addr PEER:13910 --keys 16384 --range --len 4096 --batch 70 --secs 20 --out "$OUT/lb-peer-infer-range.json"
# RDMA (build latencybench with: go build -tags rdma; rping must pass first):
for qd in 1 8 32 64 128; do
  $LB read --backend peer --addr PEER:13910 --keys 16384 --range --len 4096 --qd $qd --secs 10 --rdma --out "$OUT/lb-peer-rdma-range.json"
done

echo "Results in $OUT"
