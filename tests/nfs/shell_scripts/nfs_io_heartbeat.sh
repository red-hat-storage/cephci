#!/usr/bin/env bash
# NFS mount I/O heartbeat monitor — runs on each CephCI client node.
# Discovers NFS mounts, probes with touch+stat, logs timestamped results.
#
# Environment (set by nfs_io_health_monitor.py before launch):
#   NFS_IO_LOG_FILE          - append-only log path on the client
#   NFS_IO_CONTROL_FILE      - PAUSE / RUN / STOP control file
#   NFS_IO_PROBE_INTERVAL_S  - seconds between probe rounds (default 1)
#   NFS_IO_STALL_THRESHOLD_S - per-probe timeout / stall threshold (default 5)
#   NFS_IO_STALL_RECOVERY_S  - poll duration after stall before failure (default 100)
#   NFS_IO_MOUNT_PREFIX      - only probe mounts under this path (default /mnt)
#   NFS_IO_HEARTBEAT_FILE    - heartbeat filename under each mount (default .nfs_io_heartbeat)

set -u

PROBE_INTERVAL_S="${NFS_IO_PROBE_INTERVAL_S:-1}"
STALL_THRESHOLD_S="${NFS_IO_STALL_THRESHOLD_S:-5}"
STALL_RECOVERY_S="${NFS_IO_STALL_RECOVERY_S:-100}"
MOUNT_PREFIX="${NFS_IO_MOUNT_PREFIX:-/mnt}"
HEARTBEAT_FILE="${NFS_IO_HEARTBEAT_FILE:-.nfs_io_heartbeat}"
LOG_FILE="${NFS_IO_LOG_FILE:-/tmp/cephci_nfs_io_heartbeat.log}"
CONTROL_FILE="${NFS_IO_CONTROL_FILE:-/tmp/cephci_nfs_io_heartbeat.control}"

log_line() {
    # shellcheck disable=SC2034
    local level="$1"
    shift
    printf '%s %s %s\n' "$(date -Iseconds)" "$level" "$*" >>"$LOG_FILE"
}

mount_under_prefix() {
    local path="$1"
    local prefix="${MOUNT_PREFIX%/}"
    if [ -z "$prefix" ]; then
        return 0
    fi
    if [ "$path" = "$prefix" ] || [[ "$path" == "$prefix/"* ]]; then
        return 0
    fi
    return 1
}

discover_mounts() {
    local mounts=""
    local fstype
    for fstype in nfs nfs4; do
        while IFS= read -r line; do
            line="${line%/}"
            [ -n "$line" ] || continue
            if mount_under_prefix "$line"; then
                mounts="${mounts}${line}"$'\n'
            fi
        done < <(findmnt -rn -t "$fstype" -o TARGET 2>/dev/null || true)
    done
    printf '%s' "$mounts" | sort -u
}

is_stale_error() {
    local msg="$1"
    case "$msg" in
        *Stale\ file\ handle*) return 0 ;;
        *ESTALE*) return 0 ;;
        *stale\ file\ handle*) return 0 ;;
    esac
    return 1
}

probe_mount() {
    local mount="$1"
    local hb="${mount}/${HEARTBEAT_FILE}"
    local output
    local ec=0

    output=$(timeout "${STALL_THRESHOLD_S}" sh -c "touch '$hb' && stat -c '%Y' '$hb'" 2>&1) || ec=$?

    if [ "$ec" -ne 0 ]; then
        if is_stale_error "$output"; then
            log_line "STALE" "mount=$mount error=${output//$'\n'/ }"
            return 2
        fi
        if [ "$ec" -eq 124 ]; then
            log_line "STALL" "mount=$mount latency>=${STALL_THRESHOLD_S}s"
            return 1
        fi
        if is_stale_error "$output"; then
            log_line "STALE" "mount=$mount error=${output//$'\n'/ }"
            return 2
        fi
        log_line "ERROR" "mount=$mount exit=$ec error=${output//$'\n'/ }"
        return 3
    fi

    log_line "OK" "mount=$mount"
    return 0
}

poll_stall_recovery() {
    local mount="$1"
    local start_ts
    local now_ts
    local elapsed

    start_ts=$(date +%s)
    log_line "STALL_POLL" "mount=$mount recovery_timeout_s=${STALL_RECOVERY_S}"

    while true; do
        now_ts=$(date +%s)
        elapsed=$((now_ts - start_ts))
        if [ "$elapsed" -ge "$STALL_RECOVERY_S" ]; then
            log_line "STALL_FAILED" "mount=$mount recovery_timeout_s=${STALL_RECOVERY_S}"
            return 1
        fi

        if [ -f "$CONTROL_FILE" ]; then
            if grep -q '^STOP$' "$CONTROL_FILE" 2>/dev/null; then
                return 0
            fi
            if grep -q '^PAUSE$' "$CONTROL_FILE" 2>/dev/null; then
                sleep 1
                continue
            fi
        fi

        local output
        local ec=0
        output=$(timeout "${STALL_THRESHOLD_S}" sh -c "touch '${mount}/${HEARTBEAT_FILE}' && stat -c '%Y' '${mount}/${HEARTBEAT_FILE}'" 2>&1) || ec=$?

        if [ "$ec" -eq 0 ]; then
            log_line "STALL_RECOVERED" "mount=$mount elapsed_s=${elapsed}"
            return 0
        fi
        if is_stale_error "$output"; then
            log_line "STALE" "mount=$mount error=${output//$'\n'/ }"
            return 2
        fi

        sleep 1
    done
}

read_control() {
    if [ ! -f "$CONTROL_FILE" ]; then
        return 0
    fi
    if grep -q '^STOP$' "$CONTROL_FILE" 2>/dev/null; then
        return 2
    fi
    if grep -q '^PAUSE$' "$CONTROL_FILE" 2>/dev/null; then
        return 1
    fi
    return 0
}

: >"$LOG_FILE" || true
log_line "START" "probe_interval_s=${PROBE_INTERVAL_S} stall_threshold_s=${STALL_THRESHOLD_S} stall_recovery_s=${STALL_RECOVERY_S} mount_prefix=${MOUNT_PREFIX}"

while true; do
    ctrl=$(read_control)
    if [ "$ctrl" -eq 2 ]; then
        log_line "STOP" "heartbeat monitor exiting on STOP control"
        exit 0
    fi
    if [ "$ctrl" -eq 1 ]; then
        sleep "$PROBE_INTERVAL_S"
        continue
    fi

    mounts=$(discover_mounts)
    if [ -z "$mounts" ]; then
        log_line "INFO" "no NFS mounts under ${MOUNT_PREFIX}"
    else
        while IFS= read -r mount; do
            [ -n "$mount" ] || continue
            result=$(probe_mount "$mount")
            if [ "$result" -eq 2 ]; then
                exit 2
            fi
            if [ "$result" -eq 1 ]; then
                recovery=$(poll_stall_recovery "$mount")
                if [ "$recovery" -eq 2 ]; then
                    exit 2
                fi
                if [ "$recovery" -ne 0 ]; then
                    exit 3
                fi
            fi
            if [ "$result" -eq 3 ]; then
                exit 3
            fi
        done <<<"$mounts"
    fi

    sleep "$PROBE_INTERVAL_S"
done
