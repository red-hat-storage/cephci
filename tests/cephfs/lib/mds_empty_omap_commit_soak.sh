#!/bin/bash
# Multi-rank pinned metadata soak for empty _omap_commit_ops MDS assert stress.
#
# Env:
#   BASE, SOAK_SECONDS, SLEEP_BETWEEN, CREATES_PER_RANK, RENAMES_PER_RANK,
#   WORKERS_PER_RANK, APPLY_PINS, REQUIRE_PINS, SOAK_LOG

set -u

BASE="${BASE:-/mnt/cephfs/test_empty_omap}"
SOAK_SECONDS="${SOAK_SECONDS:-300}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-10}"
CREATES_PER_RANK="${CREATES_PER_RANK:-100}"
RENAMES_PER_RANK="${RENAMES_PER_RANK:-30}"
WORKERS_PER_RANK="${WORKERS_PER_RANK:-2}"
APPLY_PINS="${APPLY_PINS:-1}"
REQUIRE_PINS="${REQUIRE_PINS:-1}"

rank_work() {
  local r=$1
  local cycle=$2
  local worker=$3
  local creates=$4
  local renames=$5
  local d="$BASE/rank_$r/c_${cycle}_w${worker}"
  mkdir -p "$d" || return 1
  local i
  for i in $(seq 1 "$creates"); do
    echo y > "$d/f_$i" || return 1
  done
  for i in $(seq 1 "$renames"); do
    mv "$d/f_$i" "$d/n_$i" || return 1
  done
  rm -rf "$d" || return 1
  return 0
}

check_pins() {
  local r pin
  if ! command -v getfattr >/dev/null 2>&1; then
    echo "WARN: getfattr not installed — cannot verify ceph.dir.pin"
    return 0
  fi
  for r in 0 1; do
    pin=$(getfattr -n ceph.dir.pin --only-values "$BASE/rank_$r" 2>/dev/null || true)
    if [[ -z "$pin" ]]; then
      if [[ "$APPLY_PINS" == "1" ]]; then
        setfattr -n ceph.dir.pin -v "$r" "$BASE/rank_$r"
        pin=$(getfattr -n ceph.dir.pin --only-values "$BASE/rank_$r" 2>/dev/null || true)
        echo "Applied ceph.dir.pin=$pin on $BASE/rank_$r"
      else
        echo "ERROR: $BASE/rank_$r has no ceph.dir.pin (expected $r)"
        return 1
      fi
    elif [[ "$pin" != "$r" ]]; then
      if [[ "$APPLY_PINS" == "1" ]]; then
        setfattr -n ceph.dir.pin -v "$r" "$BASE/rank_$r"
        echo "Corrected ceph.dir.pin $pin -> $r on $BASE/rank_$r"
      else
        echo "ERROR: $BASE/rank_$r pin=$pin (expected $r)"
        return 1
      fi
    else
      echo "OK: $BASE/rank_$r ceph.dir.pin=$pin"
    fi
  done
  return 0
}

for r in 0 1; do
  if [[ ! -d "$BASE/rank_$r" ]]; then
    echo "ERROR: missing $BASE/rank_$r — create/pin dirs before soak"
    exit 1
  fi
done

if [[ "$REQUIRE_PINS" == "1" ]]; then
  check_pins || exit 1
else
  check_pins || echo "WARN: continuing without valid pins (REQUIRE_PINS=0)"
fi

if (( WORKERS_PER_RANK < 1 )); then
  WORKERS_PER_RANK=1
fi
creates_w=$(( (CREATES_PER_RANK + WORKERS_PER_RANK - 1) / WORKERS_PER_RANK ))
renames_w=$(( (RENAMES_PER_RANK + WORKERS_PER_RANK - 1) / WORKERS_PER_RANK ))
if (( renames_w > creates_w )); then
  renames_w=$creates_w
fi

STAMP=$(date +%Y%m%d_%H%M%S)
LOG="${SOAK_LOG:-./mds_empty_omap_commit_soak_${STAMP}.log}"
START_EPOCH=$(date +%s)
END_EPOCH=$((START_EPOCH + SOAK_SECONDS))
cycle=0
fail_cycles=0

{
  echo "=== empty omap commit soak start ==="
  echo "host=$(hostname) date=$(date -Is)"
  echo "BASE=$BASE SOAK_SECONDS=$SOAK_SECONDS SLEEP_BETWEEN=$SLEEP_BETWEEN"
  echo "CREATES_PER_RANK=$CREATES_PER_RANK RENAMES_PER_RANK=$RENAMES_PER_RANK"
  echo "WORKERS_PER_RANK=$WORKERS_PER_RANK (parallel ranks + workers)"
  echo "per_worker creates~$creates_w renames~$renames_w"
  echo "==========================="
} | tee -a "$LOG"

while (( $(date +%s) < END_EPOCH )); do
  cycle=$((cycle + 1))
  cycle_start=$(date +%s)
  pids=()
  fail=0

  for r in 0 1; do
    for w in $(seq 1 "$WORKERS_PER_RANK"); do
      rank_work "$r" "$cycle" "$w" "$creates_w" "$renames_w" &
      pids+=($!)
    done
  done

  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      fail=1
    fi
  done

  sync
  now=$(date +%s)
  elapsed=$((now - START_EPOCH))
  remaining=$((END_EPOCH - now))
  status="ok"
  if (( fail )); then
    status="FAIL"
    fail_cycles=$((fail_cycles + 1))
  fi
  echo "$(date -Is) cycle=$cycle status=$status elapsed_s=$elapsed remaining_s=$remaining cycle_duration_s=$((now - cycle_start)) workers=$((2 * WORKERS_PER_RANK))" | tee -a "$LOG"

  if (( remaining <= 0 )); then
    break
  fi
  if (( remaining < SLEEP_BETWEEN )); then
    sleep "$remaining"
  else
    sleep "$SLEEP_BETWEEN"
  fi
done

{
  echo "=== empty omap commit soak end ==="
  echo "host=$(hostname) date=$(date -Is)"
  echo "Completed cycles=$cycle fail_cycles=$fail_cycles"
  echo "total_elapsed_s=$(( $(date +%s) - START_EPOCH ))"
  echo "log=$LOG"
  echo "========================="
} | tee -a "$LOG"

echo "RESULT: Completed cycles=$cycle fail_cycles=$fail_cycles (log: $LOG)"
if (( cycle < 1 )); then
  exit 1
fi
if (( fail_cycles > 0 )); then
  exit 1
fi
exit 0
