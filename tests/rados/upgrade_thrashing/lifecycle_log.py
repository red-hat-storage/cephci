"""Pure lifecycle log parsing helpers (no cluster/ceph imports)."""

import logging
import re
import shlex
from datetime import datetime, timedelta, timezone

_log = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Daemon lifecycle log patterns -- validated against live Squid 19.2.1 and
# Tentacle 20.2.x logs from the U3S5J1 upgrade run (2026-07-15).
#
# Two source types:
#   "logfile"      – parsed from /var/log/ceph/<fsid>/<daemon>.log on host
#   "cephadm_logs" – parsed from `cephadm logs --name <daemon>` (journalctl)
#
# Each event dict has:
#   event   – short stable identifier used as key in report data
#   pattern – regex applied with re.IGNORECASE to each log line
# ---------------------------------------------------------------------------
DAEMON_LOG_PATTERNS = {
    "mgr": {
        "source": "logfile",
        "log_glob": "ceph-mgr.*.log",
        "events": [
            # Validated: "mgr handle_mgr_map Activating!"
            {
                "event": "activating",
                "pattern": r"handle_mgr_map Activating!",
            },
            # Validated: "mgr handle_mgr_map I am now activating"
            {
                "event": "now_active",
                "pattern": r"handle_mgr_map I am now activating",
            },
            # Validated: "mgr handle_mgr_map I was active but no longer am"
            {
                "event": "deactivated",
                "pattern": r"handle_mgr_map I was active but no longer am",
            },
            # Validated: "mgr handle_mgr_map respawning because"
            {
                "event": "respawn",
                "pattern": r"handle_mgr_map respawning because",
            },
            # Catch-all: process banner at daemon startup (avoid cephadm
            # ContainerInspectInfo lines that also contain "ceph version").
            {
                "event": "started",
                "pattern": r", process ceph-mgr,",
            },
        ],
    },
    "mon": {
        "source": "logfile",
        "log_glob": "ceph-mon.*.log",
        "events": [
            # Validated: "mon.<name>@<rank>(<role>) e<epoch> shutdown"
            {
                "event": "shutdown",
                "pattern": r"mon\.\S+@\d+\(\w+\)\s+e\d+\s+shutdown",
            },
            # Validated: "calling monitor election"
            {
                "event": "election",
                "pattern": r"calling monitor election",
            },
            # Validated: "<name> is new leader, mons <list> in quorum"
            {
                "event": "new_leader",
                "pattern": r"is new leader, mons .* in quorum",
            },
            # Catch-all: process banner at daemon startup.
            {
                "event": "started",
                "pattern": r", process ceph-mon,",
            },
        ],
    },
    "osd": {
        "source": "logfile",
        "log_glob": "ceph-osd.*.log",
        "events": [
            # Validated: "osd.<N> <epoch> *** Immediate shutdown
            # (osd_fast_shutdown=true) ***"
            {
                "event": "shutdown",
                "pattern": (
                    r"osd\.\d+\s+\d+\s+\*{3}\s+Immediate shutdown"
                    r"|got_stop_ack starting shutdown"
                ),
            },
            # Validated: "osd.<N> <epoch> done with init, starting boot
            # process"
            {
                "event": "init_complete",
                "pattern": r"osd\.\d+\s+\d+\s+done with init, starting boot process",
            },
            # Validated: "osd.<N> <epoch> state: booting -> active"
            {
                "event": "activated",
                "pattern": r"osd\.\d+\s+\d+\s+state:\s+booting\s+->\s+active",
            },
            # Catch-all: process banner at daemon startup.
            {
                "event": "started",
                "pattern": r", process ceph-osd,",
            },
        ],
    },
    "mds": {
        "source": "logfile",
        "log_glob": "ceph-mds.*.log",
        "events": [
            # Validated: "handle_mds_map state change up:standby --> up:replay"
            {
                "event": "replay",
                "pattern": r"handle_mds_map state change.*-->\s+up:replay",
            },
            # Validated: "mds.<rank>.<inc> replay_start"
            {
                "event": "replay_start",
                "pattern": r"mds\.\d+\.\d+\s+replay_start",
            },
            # Validated: "handle_mds_map state change up:replay -->
            # up:reconnect"
            {
                "event": "reconnect",
                "pattern": r"handle_mds_map state change.*-->\s+up:reconnect",
            },
            # Validated: "mds.<rank>.<inc> reconnect_start"
            {
                "event": "reconnect_start",
                "pattern": r"mds\.\d+\.\d+\s+reconnect_start",
            },
            # Validated: "handle_mds_map state change up:reconnect -->
            # up:rejoin"
            {
                "event": "rejoin",
                "pattern": r"handle_mds_map state change.*-->\s+up:rejoin",
            },
            # Validated: "mds.<rank>.<inc> rejoin_start"
            {
                "event": "rejoin_start",
                "pattern": r"mds\.\d+\.\d+\s+rejoin_start",
            },
            # Validated: "handle_mds_map state change up:rejoin --> up:active"
            {
                "event": "active",
                "pattern": r"handle_mds_map state change.*-->\s+up:active",
            },
            # Validated: "handle_mds_map state change up:active --> up:stopping"
            {
                "event": "stopping",
                "pattern": r"handle_mds_map state change.*-->\s+up:stopping",
            },
            # Validated: "Restarting replay as standby-replay"
            {
                "event": "standby_replay",
                "pattern": r"Restarting replay as standby-replay",
            },
            # Validated: "mds is shutting down"
            {
                "event": "shutting_down",
                "pattern": r"mds is shutting down",
            },
            # Catch-all: process banner at daemon startup.
            {
                "event": "started",
                "pattern": r", process ceph-mds,",
            },
        ],
    },
    "rgw": {
        "source": "logfile",
        "log_glob": "ceph-client.rgw.*.log",
        "events": [
            # Validated: "starting handler: beast"
            {
                "event": "started",
                "pattern": r"starting handler:\s+beast",
            },
            # Best-effort: "shutting down" -- not yet captured in live logs
            # because RGW was not upgraded during validation window.
            # Keeping as reasonable pattern from prior Ceph releases.
            {
                "event": "shutdown",
                "pattern": r"shutting down",
            },
        ],
    },
    # NFS/SMB/NVMe-oF: logs live inside containers and are NOT on the host
    # filesystem.  `cephadm logs --name <daemon>` reads journalctl from the
    # container.  NFS-Ganesha writes to stdout/stderr (no ganesha.log file).
    "nfs": {
        "source": "cephadm_logs",
        "events": [
            # Validated: "ganesha.nfsd Starting: Ganesha Version 6.5"
            {
                "event": "started",
                "pattern": (
                    r"ganesha\.nfsd.*Starting:\s+Ganesha" r"\s+Version\s+[\d.]+"
                ),
            },
            # Validated (cali Tentacle 9.7): "NFS Server Now IN GRACE, duration 90"
            # Match without requiring duration (still parsed into configured_sec).
            # Keep grep -E safe: no (?...) groups (patterns are joined for remote grep).
            {
                "event": "grace_start",
                "pattern": r"NFS Server Now IN GRACE",
            },
            # Validated: "NFS Server Now NOT IN GRACE"
            {
                "event": "grace_end",
                "pattern": r"NFS Server Now NOT IN GRACE",
            },
            # Validated: "NFS EXIT: stopping NFS service"
            {
                "event": "shutdown",
                "pattern": r"NFS EXIT:\s+stopping NFS service",
            },
            # Validated: "NFS EXIT: do_shutdown done"
            {
                "event": "shutdown_done",
                "pattern": r"NFS EXIT:\s+do_shutdown done",
            },
            # Validated: "SIGHUP_HANDLER: Received SIGHUP.... initiating
            # export list reload"
            {
                "event": "export_reload",
                "pattern": r"SIGHUP_HANDLER:\s+Received SIGHUP",
            },
        ],
    },
    # SMB patterns are best-effort / unverified -- no SMB daemons were
    # deployed on the validation cluster. Refine once real samba container
    # logs are available.
    "smb": {
        "source": "cephadm_logs",
        "events": [
            {"event": "started", "pattern": r"smbd.*started|samba.*ready"},
            {"event": "shutdown", "pattern": r"shutting down|exit_daemon"},
        ],
    },
    # NVMe-oF patterns are best-effort / unverified -- no NVMe-oF daemons
    # were deployed on the validation cluster. Refine once real nvmeof
    # container logs are available.
    "nvmeof": {
        "source": "cephadm_logs",
        "events": [
            {"event": "started", "pattern": r"gateway.*started|listening"},
            {
                "event": "shutdown",
                "pattern": r"shutting down|gateway.*stopped",
            },
        ],
    },
}

_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})")

LOGFILE_SSH_TIMEOUT_SEC = 45
LOGFILE_SSH_TIMEOUT_LARGE_SEC = 90
# mds/osd logs on converged nodes can be multi-GB (tail scrape is bounded).
LOGFILE_SSH_TIMEOUT_LARGE_DTYPES = frozenset({"mds", "osd"})
LOGFILE_HEAD_LIMIT = 500
# ponytail: events older than this tail use cephadm logs fallback.
LOGFILE_TAIL_MAX_BYTES = 128 * 1024 * 1024
LOGFILE_PARALLEL_PER_HOST = 2
LIFECYCLE_WINDOW_BUFFER = timedelta(minutes=5)
LIFECYCLE_WINDOW_DEFAULT_TAIL = timedelta(minutes=10)


def _entry_lifecycle_window(
    entry: dict,
    type_window: tuple[datetime, datetime] | None,
) -> tuple[datetime, datetime] | None:
    """Per-daemon scrape window: redeploy_time ± buffer (+ duration tail)."""
    if not type_window:
        return None
    try:
        start = datetime.fromisoformat(entry["redeploy_time"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, KeyError):
        return type_window
    tail = float(entry.get("individual_duration_sec") or 0)
    tail_sec = max(tail, LIFECYCLE_WINDOW_DEFAULT_TAIL.total_seconds())
    buf = LIFECYCLE_WINDOW_BUFFER
    return (start - buf, start + timedelta(seconds=tail_sec) + buf)


def _daemon_logfile_path(fsid: str, dtype: str, daemon_name: str) -> str:
    """Build host log path for a redeployed daemon (matches core_workflows naming)."""
    prefix = f"{dtype}."
    if daemon_name.startswith(prefix):
        daemon_id = daemon_name[len(prefix) :]
    elif "." in daemon_name:
        daemon_id = daemon_name.split(".", 1)[1]
    else:
        daemon_id = daemon_name
    if dtype == "rgw":
        return f"/var/log/ceph/{fsid}/ceph-client.rgw.{daemon_id}.log"
    return f"/var/log/ceph/{fsid}/ceph-{dtype}.{daemon_id}.log"


def _grep_pattern_for_dtype(dtype: str) -> str:
    """Join validated event regexes for remote grep (same patterns as cephadm path)."""
    pat = DAEMON_LOG_PATTERNS.get(dtype, {})
    return "|".join(e["pattern"] for e in pat.get("events", []))


def _host_deploy_name_index(daemon_entries: dict) -> dict[str, list[str]]:
    """Map host -> full cephadm daemon names from all deploy entries."""
    index: dict[str, list[str]] = {}
    for entries in daemon_entries.values():
        for entry in entries:
            host = entry.get("host", "")
            name = entry.get("name", "")
            if host and name:
                index.setdefault(host, []).append(name)
    return index


def _normalize_log_daemon_name(
    log_name: str,
    host: str,
    host_index: dict[str, list[str]],
    normalize_warned: set[tuple[str, str]] | None = None,
) -> str:
    """Map log-file stem to full cephadm name on the same host."""
    if not log_name:
        return log_name
    names = host_index.get(host, [])
    if log_name in names:
        return log_name
    candidates = [n for n in names if n == log_name or n.startswith(log_name + ".")]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        chosen = max(candidates, key=len)
        if normalize_warned is not None:
            warn_key = (host, log_name)
            if warn_key not in normalize_warned:
                normalize_warned.add(warn_key)
                _log.warning(
                    "Lifecycle daemon name ambiguous on %s: log stem %r "
                    "matched %s; using %r",
                    host,
                    log_name,
                    candidates,
                    chosen,
                )
        return chosen
    return log_name


def remote_exec_failure_tag(
    exc: BaseException,
    timeout_sec: int | None = None,
) -> str:
    """Short label for Paramiko/CephCI remote exec failures (triage-friendly)."""
    cls = type(exc).__name__
    raw = str(exc).strip()
    detail = raw or cls
    if cls == "SocketTimeoutException":
        return f"ssh_transport_timeout:{timeout_sec or '?'}s:{detail}"
    if cls in ("CommandFailed", "TimeoutException"):
        if timeout_sec is not None:
            return f"ssh_exec_timeout:{timeout_sec}s:{detail}"
        return f"ssh_exec_failed:{detail}"
    if timeout_sec is not None and (
        not raw or "timeout" in raw.lower() or "Timeout" in cls
    ):
        return f"ssh_exec_timeout:{timeout_sec}s:{detail}"
    return detail


def _build_logfile_scrape_cmd(
    log_path: str,
    since_str: str,
    until_str: str,
    grep_pattern: str,
    head_limit: int = LOGFILE_HEAD_LIMIT,
    tail_bytes: int = LOGFILE_TAIL_MAX_BYTES,
) -> str:
    """Tail-bounded awk window + dtype grep; no-op when log file is absent."""
    inner = (
        f"log={shlex.quote(log_path)}; "
        f'test -f "$log" || exit 0; '
        f'tail -c {int(tail_bytes)} "$log" 2>/dev/null | '
        f"awk -v s={shlex.quote(since_str)} -v u={shlex.quote(until_str)} "
        f"'$0 >= s && $0 < u' 2>/dev/null | "
        f"grep -iE {shlex.quote(grep_pattern)} | head -n {head_limit}"
    )
    return f"bash -c {shlex.quote(inner)}"


def _parse_lifecycle_lines(
    text: str,
    dtype: str,
    hostname: str,
    daemon_name: str,
    window: tuple[datetime, datetime] | None,
    host_index: dict[str, list[str]],
    normalize_warned: set[tuple[str, str]] | None,
) -> list[dict]:
    """Parse grep output lines into lifecycle event dicts for one daemon."""
    normalized = _normalize_log_daemon_name(
        daemon_name, hostname, host_index, normalize_warned
    )
    events: list[dict] = []
    for line in text.strip().splitlines():
        if not line.strip():
            continue
        ts_match = _TS_RE.search(line)
        if not ts_match:
            continue
        ts_raw = ts_match.group(1).replace(" ", "T")
        try:
            ts_dt = datetime.fromisoformat(ts_raw)
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        if window and not (window[0] <= ts_dt <= window[1]):
            continue
        for evt_def in DAEMON_LOG_PATTERNS[dtype]["events"]:
            if re.search(evt_def["pattern"], line, re.IGNORECASE):
                evt_dict = {
                    "event": evt_def["event"],
                    "timestamp": ts_dt.isoformat(),
                    "host": hostname,
                    "daemon_name": normalized,
                }
                if evt_def["event"] == "grace_start":
                    dur = re.search(r"duration\s+(\d+)", line, re.IGNORECASE)
                    if dur:
                        evt_dict["configured_sec"] = int(dur.group(1))
                events.append(evt_dict)
                break
    return events


_GRACE_LIFECYCLE_EVENTS = frozenset({"grace_start", "grace_end"})


def _merge_daemon_lifecycle_events(
    log_events: list[dict],
    orch_events: list[dict],
) -> list[dict]:
    """Merge log and orch lifecycle events; keep first+last per event name.

    Grace events keep every occurrence in chronological order (including
    multiple enter/leave pairs that share a second-resolution timestamp).
    """
    # Index tie-break preserves start/end interleaving when timestamps tie.
    indexed = list(enumerate(log_events + orch_events))
    indexed.sort(key=lambda ie: ((ie[1].get("timestamp") or ""), ie[0]))

    grace_out: list[dict] = []
    by_name: dict[str, list[dict]] = {}
    order: list[str] = []
    for orig_i, evt in indexed:
        name = evt.get("event", "")
        if not name:
            continue
        if name in _GRACE_LIFECYCLE_EVENTS:
            item = {
                "event": name,
                "timestamp": evt.get("timestamp", ""),
                "_ord": orig_i,
            }
            if name == "grace_start" and evt.get("configured_sec") is not None:
                item["configured_sec"] = evt["configured_sec"]
            grace_out.append(item)
            continue
        ts_key = (evt.get("timestamp") or "")[:19]
        bucket = by_name.setdefault(name, [])
        if bucket and (bucket[-1].get("timestamp") or "")[:19] == ts_key:
            continue
        if name not in order:
            order.append(name)
        bucket.append(
            {
                "event": name,
                "timestamp": evt.get("timestamp", ""),
                "_ord": orig_i,
            }
        )
    out: list[dict] = []
    for name in order:
        occ = by_name[name]
        out.append(occ[0])
        if len(occ) > 1 and occ[-1] != occ[0]:
            out.append(occ[-1])
    out.extend(grace_out)
    out.sort(key=lambda x: (x.get("timestamp", ""), x.get("_ord", 0)))
    for item in out:
        item.pop("_ord", None)
    return out


def _median_timestamp(timestamps: list[str]) -> str:
    """Return median ISO timestamp string (empty if none)."""
    valid = sorted(t for t in timestamps if t)
    if not valid:
        return ""
    return valid[len(valid) // 2]


def summarize_orch_ps_running_counts(orch_ps: list) -> dict:
    """Aggregate ceph orch ps rows into {dtype: {running, count}}."""
    by_type: dict[str, dict] = {}
    if not isinstance(orch_ps, list):
        return by_type
    for row in orch_ps:
        dtype = row.get("daemon_type", "unknown")
        bucket = by_type.setdefault(dtype, {"running": 0, "count": 0})
        bucket["count"] += 1
        if row.get("status_desc") == "running":
            bucket["running"] += 1
    return by_type


def daemon_running_count_mismatches(pre: dict, current: dict) -> list[str]:
    """Return mismatch strings where post running count dropped vs pre."""
    mismatches: list[str] = []
    for dtype in sorted(set(pre.keys()) | set(current.keys())):
        pre_running = pre.get(dtype, {}).get("running", 0)
        cur_running = current.get(dtype, {}).get("running", 0)
        if cur_running < pre_running:
            mismatches.append(f"{dtype}: {pre_running} -> {cur_running}")
    return mismatches


def _build_type_lifecycle_summary(
    per_daemon_merged: list[list[dict]],
    max_events: int = 8,
) -> list[dict]:
    """Build representative type-level chain from merged per-daemon events.

    Uses median timestamp per event name across daemons so high-count types
    (e.g. 77 OSDs) show a representative shutdown→boot→active wave instead of
    only the first daemon's first event.
    """
    by_event: dict[str, list[str]] = {}
    for events in per_daemon_merged:
        seen_in_daemon: set[str] = set()
        for evt in events:
            name = evt.get("event")
            if not name or name in seen_in_daemon:
                continue
            seen_in_daemon.add(name)
            by_event.setdefault(name, []).append(evt.get("timestamp", ""))

    if not by_event:
        return []

    ordered = sorted(by_event.keys(), key=lambda n: _median_timestamp(by_event[n]))
    chain: list[dict] = []
    total_daemons = len(per_daemon_merged)
    for name in ordered[:max_events]:
        timestamps = by_event[name]
        entry = {
            "event": name,
            "timestamp": _median_timestamp(timestamps),
        }
        if total_daemons > 1:
            entry["daemon_count"] = len(timestamps)
        chain.append(entry)
    return chain


def compute_deploy_group_span(
    deploys: list[dict],
) -> tuple[float, str, str]:
    """Wall-clock span for deploys in one timeline group (same phase).

    Uses (last_redeploy - first_redeploy) + last individual duration so
    rolling types (OSD) keep fleet-wide duration within a phase.
    """
    if not deploys:
        return 0.0, "", ""

    daemons_sorted = sorted(deploys, key=lambda d: d.get("redeploy_time", ""))
    first_time = daemons_sorted[0]["redeploy_time"]
    last_time = daemons_sorted[-1]["redeploy_time"]
    last_individual = daemons_sorted[-1].get("individual_duration_sec", 0) or 0

    try:
        first_dt = datetime.fromisoformat(first_time)
        last_dt = datetime.fromisoformat(last_time)
        duration = (last_dt - first_dt).total_seconds() + float(last_individual)
        window_end = last_time
        if last_individual > 0:
            window_end = (
                last_dt + timedelta(seconds=float(last_individual))
            ).isoformat()
    except (ValueError, TypeError):
        duration = float(last_individual or 0)
        window_end = last_time

    return round(duration, 1), first_time, window_end


def group_deploy_events_for_timeline(
    deploy_events: list[dict],
) -> list[dict]:
    """Partition deploy events by (daemon_type, phase_idx, is_redeploy).

    Prevents cross-phase span inflation (e.g. MON in phase 1 and phase 3).
    """
    buckets: dict[tuple[str, int, bool], list[dict]] = {}
    for entry in deploy_events:
        key = (
            entry.get("daemon_type", ""),
            entry.get("phase_idx", -1),
            bool(entry.get("is_redeploy")),
        )
        buckets.setdefault(key, []).append(entry)

    groups: list[dict] = []
    for (dtype, phase_idx, is_redeploy), deploys in buckets.items():
        groups.append(
            {
                "daemon_type": dtype,
                "phase_idx": phase_idx,
                "is_redeploy": is_redeploy,
                "deploys": deploys,
            }
        )
    groups.sort(key=lambda g: min(d.get("redeploy_time", "") for d in g["deploys"]))
    return groups


def resolve_phase_window_from_boundaries(
    phase_name: str,
    boundary_map: dict[str, list[str]],
    phase_order: tuple[str, ...] = ("baseline", "upgrade", "post_upgrade"),
) -> tuple[str, str]:
    """Return (start_iso, end_iso) for a named test phase."""
    try:
        idx = phase_order.index(phase_name)
    except ValueError:
        return "", ""

    timestamps = boundary_map.get(phase_name, [])
    start = timestamps[0] if timestamps else ""
    end = timestamps[-1] if len(timestamps) > 1 else ""

    if phase_name == "upgrade" and not end:
        upgrade_end = boundary_map.get("upgrade_end", [])
        if upgrade_end:
            end = upgrade_end[-1]
        else:
            for np_name in phase_order[idx + 1 :]:
                np_ts = boundary_map.get(np_name, [])
                if np_ts:
                    end = np_ts[0]
                    break
    elif not end and start:
        for np_name in phase_order[idx + 1 :]:
            np_ts = boundary_map.get(np_name, [])
            if np_ts:
                end = np_ts[0]
                break

    return start, end


def count_daemons_by_type_from_timeline(timeline: list[dict]) -> dict[str, int]:
    """Per-type daemon counts from timeline entries.

    When individual_daemons include names, dedupe across split groups (e.g.
    MON in phase 0 + phase 2, or repeated redeploys of the same daemon).
    Otherwise sum entry counts (legacy timelines without per-daemon names).
    """
    names_by_type: dict[str, set[str]] = {}
    fallback_by_type: dict[str, int] = {}
    has_names: dict[str, bool] = {}

    for entry in timeline:
        dtype = entry.get("daemon_type", "")
        if not dtype:
            continue
        individuals = entry.get("individual_daemons") or []
        named = [d.get("name") for d in individuals if d.get("name")]
        if named:
            has_names[dtype] = True
            names_by_type.setdefault(dtype, set()).update(named)
        else:
            fallback_by_type[dtype] = fallback_by_type.get(dtype, 0) + int(
                entry.get("count", 0) or 0
            )

    counts: dict[str, int] = {}
    for dtype in set(names_by_type) | set(fallback_by_type):
        if has_names.get(dtype):
            unique = len(names_by_type.get(dtype, set()))
            counts[dtype] = unique or fallback_by_type.get(dtype, 0)
        else:
            counts[dtype] = fallback_by_type.get(dtype, 0)
    return counts


def _iso_delta_sec(start_iso: str, end_iso: str) -> float | None:
    """Return seconds between two ISO timestamps, or None on parse failure."""
    if not start_iso or not end_iso:
        return None
    try:
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return round((end - start).total_seconds(), 1)
    except (ValueError, TypeError):
        return None


def parse_grace_cycles(
    events: list[dict],
    daemon_name: str = "",
) -> list[dict]:
    """Pair grace_start with the next grace_end for one daemon's event list."""
    dname = daemon_name
    cycles: list[dict] = []
    pending: dict | None = None
    for evt in sorted(events, key=lambda e: e.get("timestamp", "")):
        name = evt.get("event", "")
        if name == "grace_start":
            pending = {
                "daemon": dname or evt.get("daemon_name", ""),
                "start": evt.get("timestamp", ""),
                "configured_sec": evt.get("configured_sec"),
            }
        elif name == "grace_end" and pending is not None:
            end = evt.get("timestamp", "")
            pending["end"] = end
            pending["observed_sec"] = _iso_delta_sec(pending["start"], end)
            cycles.append(pending)
            pending = None
    if pending is not None:
        pending["end"] = None
        pending["observed_sec"] = None
        cycles.append(pending)
    return cycles


def analyze_nfs_grace(daemon_timeline, config: dict | None = None) -> dict:
    """Summarize NFS grace cycles from daemon timeline data (report + a24)."""
    cfg = (config or {}).get("nfs_grace_validation") or {}
    mode = cfg.get("mode", "baseline")
    threshold = float(cfg.get("max_observed_grace_sec", 120))

    entries: list[dict] = []
    if isinstance(daemon_timeline, dict):
        entries = daemon_timeline.get("daemon_timeline") or []
    elif isinstance(daemon_timeline, list):
        entries = daemon_timeline

    # NFS may appear in multiple phase groups; merge per-daemon events.
    by_daemon: dict[str, list[dict]] = {}
    for entry in entries:
        if entry.get("daemon_type") != "nfs":
            continue
        for ind in entry.get("individual_daemons") or []:
            dname = ind.get("name", "")
            if not dname:
                continue
            by_daemon.setdefault(dname, []).extend(ind.get("lifecycle_events") or [])

    cycles_by_daemon: dict[str, list[dict]] = {}
    all_cycles: list[dict] = []
    for dname, events in by_daemon.items():
        merged = sorted(events, key=lambda e: e.get("timestamp", ""))
        cycles = parse_grace_cycles(merged, dname)
        if cycles:
            cycles_by_daemon[dname] = cycles
            all_cycles.extend(cycles)

    missing = [c for c in all_cycles if not c.get("end")]
    over_threshold = [
        c
        for c in all_cycles
        if c.get("observed_sec") is not None and c["observed_sec"] > threshold
    ]
    observed_vals = [
        c["observed_sec"] for c in all_cycles if c.get("observed_sec") is not None
    ]
    max_observed = max(observed_vals) if observed_vals else 0.0

    stagger_sec = None
    starts = sorted(c["start"] for c in all_cycles if c.get("start"))
    if len(starts) >= 2:
        gaps = []
        for i in range(1, len(starts)):
            gap = _iso_delta_sec(starts[i - 1], starts[i])
            if gap is not None:
                gaps.append(gap)
        if gaps:
            gaps.sort()
            stagger_sec = gaps[len(gaps) // 2]

    return {
        "mode": mode,
        "metrics": {
            "daemon_count": len(cycles_by_daemon),
            "cycle_count": len(all_cycles),
            "max_observed_sec": max_observed,
            "threshold_sec": threshold,
            "missing_grace_end": missing,
            "over_threshold": over_threshold,
        },
        "cycles_by_daemon": cycles_by_daemon,
        "stagger_sec": stagger_sec,
    }
