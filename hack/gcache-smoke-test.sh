#!/usr/bin/env bash
# Smoke test: distributed cache group over two real JuiceFS mounts.
#
# Topology:
#   meta engine: sqlite3 (exercises the SQL gcache registry backend)
#   object storage: local file://
#   node A + node B: two juicefs mounts joined via --cache-group=smoke
#
# PASS criteria:
#   1. exactly 2 members in jfs_gcache_members (zombie-pollution guard)
#   2. data written through A reads back identical through B
#   3. B's juicefs_remotecache_gets > 0 — definitive proof B served blocks
#      from A over the peer protocol.
#
# Env: JFS=path-to-binary (default /tmp/juicefs-test, must be freshly built)
set -uo pipefail

JFS=${JFS:-/tmp/juicefs-test}
VOL=gcachesmoke
GROUP=smoke
SIZE_MB=${SIZE_MB:-64}   # 16 blocks at 4MiB: P(no peer-owned block) ~ 0.002%
EXTRA_ARGS=${EXTRA_ARGS:-}  # e.g. --log-level debug; default avoids set -u abort

# --- bulletproof cleanup: zombie mounts reconnect to the meta DB by path and
# poison the registry with dead-but-fresh-heartbeat members, so kill hard.
pkill -9 -f "juicefs-test mount" 2>/dev/null
sleep 1
LEFT=$(pgrep -fc "juicefs-test mount" || true)
if [ "${LEFT:-0}" != "0" ]; then echo "FAIL: $LEFT stale mounts still running"; exit 1; fi

BASE=$(mktemp -d /tmp/gcache-smoke.XXXXXX)
META="$BASE/meta.sqlite3"
DB="sqlite3://$META"
mkdir -p "$BASE/mnt-a" "$BASE/mnt-b" "$BASE/cache-a" "$BASE/cache-b"
PID_A=""; PID_B=""

cleanup() {
  [ -n "$PID_A" ] && kill "$PID_A" 2>/dev/null
  [ -n "$PID_B" ] && kill "$PID_B" 2>/dev/null
  wait 2>/dev/null
  fusermount3 -u "$BASE/mnt-a" 2>/dev/null
  fusermount3 -u "$BASE/mnt-b" 2>/dev/null
  chmod -R u+w "$HOME/.juicefs/local/$VOL" 2>/dev/null && rm -r "$HOME/.juicefs/local/$VOL" 2>/dev/null
  echo "logs in $BASE (kept for inspection)"
}
trap cleanup EXIT

chmod -R u+w "$HOME/.juicefs/local/$VOL" 2>/dev/null && rm -r "$HOME/.juicefs/local/$VOL" 2>/dev/null

echo "== format =="
$JFS format "$DB" "$VOL" --compress none >/dev/null 2>&1 || { echo "format FAILED"; exit 1; }

echo "== mount A =="
$JFS mount "$DB" "$BASE/mnt-a" \
  --cache-group "$GROUP" --cache-dir "$BASE/cache-a" --cache-size 1024 \
  --group-listen 127.0.0.1:0 --no-bgjob --foreground $EXTRA_ARGS \
  > "$BASE/log-a.log" 2>&1 &
PID_A=$!

echo "== mount B =="
$JFS mount "$DB" "$BASE/mnt-b" \
  --cache-group "$GROUP" --cache-dir "$BASE/cache-b" --cache-size 1024 \
  --group-listen 127.0.0.1:0 --no-bgjob --foreground $EXTRA_ARGS \
  > "$BASE/log-b.log" 2>&1 &
PID_B=$!

for i in $(seq 1 30); do
  A_OK=$(grep -c "is ready at" "$BASE/log-a.log" 2>/dev/null || true)
  B_OK=$(grep -c "is ready at" "$BASE/log-b.log" 2>/dev/null || true)
  MEMBERS=$(sqlite3 "$META" "SELECT COUNT(*) FROM jfs_gcache_members WHERE group_name='$GROUP';" 2>/dev/null || echo 0)
  [ "${A_OK:-0}" -ge 1 ] && [ "${B_OK:-0}" -ge 1 ] && [ "${MEMBERS:-0}" -eq 2 ] && break
  sleep 1
done
MEMBERS=$(sqlite3 "$META" "SELECT COUNT(*) FROM jfs_gcache_members WHERE group_name='$GROUP';" 2>/dev/null || echo 0)
echo "== registry members: $MEMBERS =="
sqlite3 "$META" "SELECT group_name, substr(uuid,1,8), addr FROM jfs_gcache_members;" 2>/dev/null
if [ "${MEMBERS:-0}" -ne 2 ]; then
  echo "FAIL: expected exactly 2 members (got $MEMBERS) — stale mounts polluting the registry?"
  exit 1
fi

echo "== write ${SIZE_MB}MiB through A =="
head -c $((SIZE_MB * 1024 * 1024)) /dev/urandom > "$BASE/mnt-a/big.bin"
A_SUM=$(md5sum "$BASE/mnt-a/big.bin" | cut -d' ' -f1)

echo "== read through B =="
B_SUM=$(md5sum "$BASE/mnt-b/big.bin" | cut -d' ' -f1)
echo "A md5: $A_SUM / B md5: $B_SUM"
[ "$A_SUM" = "$B_SUM" ] || { echo "FAIL: checksum mismatch"; exit 1; }

echo "== peer-fetch proof (B's metrics) =="
sleep 1
B_METRICS_PORT=$(grep -oP 'Prometheus metrics listening on "[^:]+:\K[0-9]+' "$BASE/log-b.log" | tail -1)
curl -s "http://127.0.0.1:$B_METRICS_PORT/metrics" > "$BASE/b.metrics"
RC_GETS=$(grep '^juicefs_remotecache_gets' "$BASE/b.metrics" | awk '{s+=$2} END {print s+0}')
RC_BYTES=$(grep '^juicefs_remotecache_bytes' "$BASE/b.metrics" | awk '{s+=$2} END {print s+0}')
RC_ERRS=$(grep '^juicefs_remotecache_errors' "$BASE/b.metrics" | awk '{s+=$2} END {print s+0}')
echo "B remotecache_gets=$RC_GETS bytes=$RC_BYTES errors=$RC_ERRS"

if [ "${RC_GETS:-0}" -gt 0 ]; then
  echo "PASS: 2 members, data consistent, $RC_GETS peer fetches ($RC_BYTES bytes) served by node A"
  exit 0
fi
echo "FAIL: no peer fetch observed despite $(wc -c < "$BASE/mnt-a/big.bin") bytes and 2 members"
grep -iE "gcache|cache group" "$BASE/log-b.log" | head -5
exit 1
