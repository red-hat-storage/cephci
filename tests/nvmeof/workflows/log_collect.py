"""NVMeoF log collection helpers for initiator dmesg and gateway journals.

Designed around how customers triage Ceph NVMeoF failures:
  Initiator: dmesg -T / journalctl -k for each connect→disconnect cycle (and FIO
            sub-windows as artifacts), then ``nvme list-subsys`` path state.
  Gateway:  systemd unit journal for keep-alive disconnects, SPDK abort/terminate,
            admin qpair / socket errors (often logged as NOTICE, not only WARNING).

Always-on soft checks: full dumps are written under ``<run_dir>/ceph_logs/``
(Cluster Logs). Collection runs on Pass and Fail.

Test log levels:
  - INFO  — collection ran successfully (including /tmp path note, warn-only archives)
  - ERROR — only when ERROR-level content is found in dmesg / journalctl
            (or collector hard-fails to write Cluster Logs)

Findings never fail the test.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

from ceph.utils import get_node_by_id, get_nodes_by_ids
from utility.log import Log
from utility.systemctl import SystemCtl

LOG = Log(__name__)

# ---------------------------------------------------------------------------
# Initiator patterns — customer greps in dmesg when IO stalls / fails.
# Avoid bare ``nvme-tcp:`` (matches routine info); require failure context.
# ---------------------------------------------------------------------------
INITIATOR_ISSUE_RE = re.compile(
    r"("
    r"I/O error|"
    r"\bOops\b|"
    r"\bBUG:|"
    r"Call Trace|"
    r"NULL pointer dereference|"
    r"general protection fault|"
    r"soft lockup|"
    r"blocked for more than \d+ seconds|"
    r"task .+ blocked for more than|"
    r"nvme[^:\s]*:.*(reset|reconnect|fail|error|timeout|offline|removing)|"
    r"nvme[-_]tcp:.*(fail|error|timeout|reset|reconnect|disconnect)|"
    r"failed to connect|"
    r"reconnect attempt|"
    r"reconnecting controller|"
    r"resetting controller|"
    r"controller is removed|"
    r"removing nsid|"
    r"Keep-alive timer expired|"
    r"keep.?alive.*(expir|timeout|fail)|"
    r"Admin queue is unavailable|"
    r"Property Set error|"
    r"Connect command failed|"
    r"I/O .* (cancelled|aborted|timeout)|"
    r"blk_update_request:.*(error|I/O error)"
    r")",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Gateway patterns — support asks customers to search these in GW journals.
# Keep tight: broad tokens like bare "Aborted" / "Assertion" cause noise.
# ---------------------------------------------------------------------------
GATEWAY_CUSTOMER_CRITICAL_RE = re.compile(
    r"("
    r"keep.?alive.*(timeout|expir)|"
    r"Disconnecting host .+ due to keep alive|"
    r"Host was disconnected from subsystem due to keep alive|"
    r"Inactive admin qpair|"
    r"Could not write IC_RESP|"
    r"Could not write .* to socket|"
    r"Terminating SPDK|"
    # Avoid matching benign "Total cores available" from spdk_app_start NOTICE.
    r"SPDK.*(crash|abort|terminated)|"
    r"spdk_get_version failed|"
    r"Got error \d+ while updating gateway state|"
    r"SystemExit: Got error|"
    # Monitor-client assert → abort → coredump → gateway SIGCHLD/SystemExit
    # (often not journalctl -p err; must match via any-priority critical scan).
    r"FAILED ceph_assert|"
    r"ceph_assert\(|"
    r"Caught signal \(Aborted\)|"
    r"SystemExit: Gateway subprocess terminated|"
    r"Gateway subprocess terminated|"
    r"GatewayServer: SIGCHLD|"
    r"GatewayServer exception|"
    r"beacon timeout|"
    # OMAP / state-lock failures (word-boundary OMAP avoids config keys like
    # omap_file_ignore_unlock_errors). Match fail-before-OMAP ("Failed to lock
    # OMAP") and the follow-on "No such lock" / "Failure while getting state".
    r"\bOMAP\b.+\b(fail|failed|error|errors|abort)\b|"
    r"\b(fail|failed|failure|error|errors|abort)\b.+\bOMAP\b|"
    r"Failed to lock OMAP|"
    r"Unable to lock OMAP|"
    r"No such lock|"
    r"Failure while getting state|"
    r"gRPC.*(UNAVAILABLE|DEADLINE_EXCEEDED|INTERNAL)|"
    r"Segmentation fault|"
    # systemd-coredump says "dumped core"; classic phrasing is "core dumped".
    r"(core dumped|dumped core)|" r"reactor .* failed|"
    # Real SPDK bdev failures (not NOTICE "Failed to get metadata reservation_key").
    r"bdev_rbd.*\*ERROR\*|"
    r"failed to create bdev|"
    r"Listener .* failed|"
    r"Failed to (start|stop|reload) (the )?(server|SPDK|gateway)"
    r")",
    re.IGNORECASE,
)

# Gateway logs INFO/DEBUG to stderr → journalctl -p err is polluted.
_GATEWAY_STDERR_INFO_RE = re.compile(r"\]\s+(INFO|DEBUG)\s+\S+:")
# SPDK dumps size/layout at *ERROR* with an XXX marker during successful create.
_GATEWAY_SPDK_XXX_NOISE_RE = re.compile(r"\*ERROR\*:\s*XXX\b", re.IGNORECASE)
# Content that counts as a real gateway error line (after noise filters).
_GATEWAY_CONTENT_ERROR_RE = re.compile(
    r"(?i)(\bERROR\b|\*ERROR\*|\bFATAL\b|\bCRITICAL\b|"
    r"Traceback \(most recent call last\)|Segmentation fault|"
    r"core dumped|dumped core|FAILED ceph_assert|Caught signal \(Aborted\))"
)

# HA failover/failback: initiator reconnect noise (often journalctl -p err).
# Archive in Cluster Logs, but do NOT LOG.error / publish index Comments.
HA_EXPECTED_INITIATOR_RE = re.compile(
    r"("
    r"failed to connect socket:\s*-111|"
    r"Failed reconnect attempt\s+\d+/\d+|"
    r"Reconnecting in\s+\d+\s+seconds|"
    r"Successfully reconnected\s*\(attempt|"
    r"starting error recovery|"
    r"Keep-alive timer expired|"
    r"resetting controller|"
    r"reconnect attempt|"
    r"reconnecting controller"
    r")",
    re.IGNORECASE,
)

# HA failover/failback: gateway host disconnect via keep-alive is the HA mechanism.
# Still archived in Cluster Logs; do not elevate to test-log ERROR / Comments.
HA_EXPECTED_GATEWAY_RE = re.compile(
    r"("
    r"Disconnecting host .+ due to keep alive|"
    r"Host was disconnected from subsystem due to keep alive|"
    r"keep.?alive.*(timeout|expir)"
    r")",
    re.IGNORECASE,
)

# Path states customers treat as unhealthy after IO (nvme list-subsys).
UNHEALTHY_PATH_STATES = ("connecting", "deleting", "unknown")

# Last-resort window when neither NVMeService.test_start_time nor config stamp exists.
# Keep short so suite N does not re-flag suite N-1 gateway noise.
DEFAULT_JOURNAL_SINCE = "30 minutes ago"
CONFIG_JOURNAL_SINCE_KEY = "_nvmeof_test_start_time"
CLUSTER_LOGS_SUBDIR = "ceph_logs"
DMESG_ARTIFACT_SUBDIR = os.path.join(CLUSTER_LOGS_SUBDIR, "nvmeof_initiator_dmesg")
JOURNAL_ARTIFACT_SUBDIR = os.path.join(CLUSTER_LOGS_SUBDIR, "nvmeof_gateway_journallog")

# index.html Comments: top unique signatures + a few sample lines.
COMMENT_TOP_N_SIGNATURES = 5
COMMENT_SAMPLE_LINES = 3

# dmesg -T wall-clock header, e.g. ``[Thu Nov  4 09:55:13 2026] ...``
_DMESG_T_TS_RE = re.compile(
    r"^\[([A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\]"
)

# Process-wide idempotency when callers pass no shared config dict.
_JOURNALS_COLLECTED_KEYS = set()
_JOURNALS_LOCK = threading.Lock()

# Bound to the active test config so findings can populate index.html Comments
# via run.py → config["artifacts"] → tc["comments"].
_ACTIVE_CONFIG = None
_ACTIVE_CONFIG_LOCK = threading.Lock()
_COMMENT_LINES = []
# Initiator Comments dedupe: (hostname, since, until) windows already published.
_PUBLISHED_INITIATOR_WINDOWS: List[Tuple[str, str, str]] = []

# Open connect→disconnect initiator windows (mirrors gateway arm/collect).
_OPEN_INITIATOR_WINDOWS = {}
_OPEN_INITIATOR_LOCK = threading.Lock()


def bind_test_config(config, *, force_reset: bool = False) -> None:
    """Bind the suite test config so Comments can be updated with findings.

    Resets comment/window state when the bound config object changes, or when
    ``force_reset=True`` (first arm of a test). Same-object re-binds (e.g. a
    second ``NVMeService`` in one test) do not wipe in-progress Comments.
    """
    global _ACTIVE_CONFIG
    with _ACTIVE_CONFIG_LOCK:
        new_cfg = config if isinstance(config, dict) else None
        if force_reset or new_cfg is not _ACTIVE_CONFIG:
            _COMMENT_LINES.clear()
            _PUBLISHED_INITIATOR_WINDOWS.clear()
            with _OPEN_INITIATOR_LOCK:
                _OPEN_INITIATOR_WINDOWS.clear()
            if isinstance(new_cfg, dict):
                new_cfg["artifacts"] = ""
                new_cfg.pop("_nvmeof_journals_collected", None)
        _ACTIVE_CONFIG = new_cfg


def _cluster_logs_relpath(artifact_path: Optional[str]) -> str:
    """Return path relative to run dir for Comments (under ./ceph_logs)."""
    if not artifact_path:
        return os.path.join(CLUSTER_LOGS_SUBDIR, "(artifact missing)")
    run_dir = resolve_run_log_dir()
    try:
        return os.path.relpath(artifact_path, run_dir)
    except Exception:  # noqa: BLE001
        if CLUSTER_LOGS_SUBDIR in artifact_path:
            return artifact_path[artifact_path.find(CLUSTER_LOGS_SUBDIR) :]
        return artifact_path


def publish_index_comment(line: str) -> None:
    """Append a short summary for index.html Comments (via config['artifacts']).

    Call only for:
      - real ERROR content (dmesg / journalctl -p err)
      - customer-critical markers (keep-alive disconnect, SPDK terminate, etc.)
      - hard failure to write Cluster Logs
    """
    text = (line or "").strip()
    if not text:
        return
    with _ACTIVE_CONFIG_LOCK:
        if text not in _COMMENT_LINES:
            _COMMENT_LINES.append(text)
        cfg = _ACTIVE_CONFIG
    if not isinstance(cfg, dict):
        return
    existing = cfg.get("artifacts") or ""
    if text in existing:
        return
    cfg["artifacts"] = f"{existing}\n{text}".strip() if existing else text


def flush_nvmeof_log_comments(config=None) -> None:
    """Ensure accumulated comment lines are on the given (or bound) test config."""
    with _ACTIVE_CONFIG_LOCK:
        lines = list(_COMMENT_LINES)
        cfg = config if isinstance(config, dict) else _ACTIVE_CONFIG
    if not isinstance(cfg, dict) or not lines:
        return
    existing = cfg.get("artifacts") or ""
    for line in lines:
        if line not in existing:
            existing = f"{existing}\n{line}".strip() if existing else line
    cfg["artifacts"] = existing


def _node_key(node) -> str:
    return getattr(node, "hostname", None) or getattr(node, "id", None) or str(id(node))


def _parse_window_ts(value: str) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def _window_contains(
    outer_since: str, outer_until: str, inner_since: str, inner_until: str
) -> bool:
    """True when outer [since, until] fully covers inner (string/parsed compare)."""
    os_, ou_, is_, iu_ = (
        _parse_window_ts(outer_since),
        _parse_window_ts(outer_until),
        _parse_window_ts(inner_since),
        _parse_window_ts(inner_until),
    )
    if os_ and ou_ and is_ and iu_:
        return os_ <= is_ and ou_ >= iu_
    # Fallback: lexicographic compare works for ``YYYY-mm-dd HH:MM:SS``.
    return outer_since <= inner_since and outer_until >= inner_until


def _initiator_window_already_covered(hostname: str, since: str, until: str) -> bool:
    with _ACTIVE_CONFIG_LOCK:
        published = list(_PUBLISHED_INITIATOR_WINDOWS)
    for host, p_since, p_until in published:
        if host != hostname:
            continue
        if _window_contains(p_since, p_until, since, until):
            return True
    return False


def _register_initiator_window(hostname: str, since: str, until: str) -> None:
    """Record a published initiator window; drop any fully-contained prior ones."""
    with _ACTIVE_CONFIG_LOCK:
        kept: List[Tuple[str, str, str]] = []
        for host, p_since, p_until in _PUBLISHED_INITIATOR_WINDOWS:
            if host == hostname and _window_contains(since, until, p_since, p_until):
                continue
            kept.append((host, p_since, p_until))
        kept.append((hostname, since, until))
        _PUBLISHED_INITIATOR_WINDOWS[:] = kept


def _error_signature(line: str) -> str:
    """Normalize a log line into a stable signature for Top-N Comments."""
    text = _raw_line_key(line)
    text = re.sub(
        r"^\d{4}-\d{2}-\d{2}T\S+\s+\S+\s+(kernel:\s*)?",
        "",
        text,
    )
    text = re.sub(r"^\[\s*[\d.]+\s*\]\s*", "", text)
    text = re.sub(
        r"^\[[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}\]\s*",
        "",
        text,
    )
    text = re.sub(
        r"reconnect attempt\s+\d+/\d+",
        "reconnect attempt N/M",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bnvme\d+\b", "nvmeN", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _top_error_signatures(
    lines: Iterable[str], top_n: int = COMMENT_TOP_N_SIGNATURES
) -> List[Tuple[str, int]]:
    counts = {}
    order = []
    for line in lines:
        sig = _error_signature(line)
        if not sig:
            continue
        if sig not in counts:
            counts[sig] = 0
            order.append(sig)
        counts[sig] += 1
    ranked = sorted(order, key=lambda s: (-counts[s], s))
    return [(sig, counts[sig]) for sig in ranked[:top_n]]


def _format_initiator_comment(
    hostname: str,
    since: str,
    until: str,
    error_lines: List[str],
    rel: Optional[str],
    window_label: str = "session",
) -> str:
    """One-line plain-text Comment (HTML collapses newlines in the Comments cell)."""
    top = _top_error_signatures(error_lines)
    top_txt = "; ".join(f"{sig} (x{cnt})" for sig, cnt in top) if top else "(none)"
    samples = []
    for line in error_lines:
        sample = _raw_line_key(line)
        if sample and sample not in samples:
            samples.append(sample)
        if len(samples) >= COMMENT_SAMPLE_LINES:
            break
    sample_txt = " | ".join(samples) if samples else "(none)"
    parts = [
        f"[Initiator/{window_label}] {hostname} [{since} -> {until}] "
        f"count={len(error_lines)}",
        f"top: {top_txt}",
        f"sample: {sample_txt}",
    ]
    if rel:
        parts.append(f"logs: {rel}")
    return " | ".join(parts)


def _format_gateway_comment(
    hostname: str,
    unit: str,
    since: str,
    until: str,
    err_lines: List[str],
    critical_hits: List[str],
    rel: Optional[str],
) -> str:
    """One-line plain-text Comment (HTML collapses newlines in the Comments cell)."""
    # Prefer customer-critical signatures (OMAP lock / state failures) in top:
    # so they are not buried under noisier -p err volume.
    crit_unique = _dedupe_prefixed(critical_hits)
    err_unique = _dedupe_prefixed(err_lines)
    top_crit = _top_error_signatures(crit_unique, top_n=COMMENT_TOP_N_SIGNATURES)
    remaining = max(0, COMMENT_TOP_N_SIGNATURES - len(top_crit))
    crit_sigs = {sig for sig, _ in top_crit}
    top_err = [
        (sig, cnt)
        for sig, cnt in _top_error_signatures(
            err_unique, top_n=COMMENT_TOP_N_SIGNATURES
        )
        if sig not in crit_sigs
    ][:remaining]
    top = top_crit + top_err
    top_txt = "; ".join(f"{sig} (x{cnt})" for sig, cnt in top) if top else "(none)"
    samples = []
    for line in list(crit_unique) + list(err_unique):
        sample = _raw_line_key(line)
        if sample and sample not in samples:
            samples.append(sample)
        if len(samples) >= COMMENT_SAMPLE_LINES:
            break
    sample_txt = " | ".join(samples) if samples else "(none)"
    parts = [
        f"[Gateway] {hostname} ({unit}) [{since} -> {until}] "
        f"err={len(err_lines)} critical={len(critical_hits)}",
        f"top: {top_txt}",
        f"sample: {sample_txt}",
    ]
    if rel:
        parts.append(f"logs: {rel}")
    return " | ".join(parts)


def publish_initiator_index_comment(
    hostname: str,
    since: str,
    until: str,
    error_lines: List[str],
    rel: Optional[str],
    window_label: str = "session",
) -> None:
    """Publish initiator Comments with host+time window containment dedupe."""
    if not error_lines:
        return
    if _initiator_window_already_covered(hostname, since, until):
        LOG.info(
            "Skipping initiator Comments for %s [%s -> %s]; "
            "already covered by a published window",
            hostname,
            since,
            until,
        )
        return
    text = _format_initiator_comment(
        hostname, since, until, error_lines, rel, window_label=window_label
    )
    _register_initiator_window(hostname, since, until)
    publish_index_comment(text)


def has_open_initiator_window(node) -> bool:
    """True when a connect→disconnect window is armed and not yet collected."""
    with _OPEN_INITIATOR_LOCK:
        entry = _OPEN_INITIATOR_WINDOWS.get(_node_key(node))
        return bool(entry and entry.get("since") and not entry.get("collected"))


def arm_initiator_connect_window(node) -> Optional[str]:
    """Stamp connect→disconnect dmesg window (mirrors gateway journal arming).

    Per connect/disconnect cycle: if a previous window is still open, collect it
    first, then arm a new since-stamp.
    """
    try:
        if has_open_initiator_window(node):
            collect_open_initiator_window(node)
        ts = get_node_timestamp(node)
        with _OPEN_INITIATOR_LOCK:
            _OPEN_INITIATOR_WINDOWS[_node_key(node)] = {
                "node": node,
                "since": ts,
                "collected": False,
                "in_progress": False,
            }
        LOG.info(
            "Armed initiator connect-window on %s since=%s",
            getattr(node, "hostname", "?"),
            ts,
        )
        return ts
    except Exception as exc:  # noqa: BLE001
        LOG.info(
            "Failed to arm initiator connect-window on %s: %s",
            getattr(node, "hostname", "?"),
            exc,
        )
        return None


def collect_open_initiator_window(
    node, until: Optional[str] = None, window_label: str = "connect_window"
) -> Optional[dict]:
    """Collect and close the armed connect→disconnect window for ``node``.

    Marks the window collected only after a successful collect so teardown can
    retry if SSH/collection fails.
    """
    key = _node_key(node)
    with _OPEN_INITIATOR_LOCK:
        entry = _OPEN_INITIATOR_WINDOWS.get(key)
        if not entry or entry.get("collected") or not entry.get("since"):
            return None
        if entry.get("in_progress"):
            return None
        since = entry["since"]
        entry["in_progress"] = True
        target = entry.get("node") or node
    try:
        result = collect_initiator_kernel_logs(
            target,
            since=since,
            until=until,
            window_label=window_label,
            publish_comments=True,
            path_check=True,
        )
        with _OPEN_INITIATOR_LOCK:
            cur = _OPEN_INITIATOR_WINDOWS.get(key)
            if cur is entry:
                cur["collected"] = True
                cur["in_progress"] = False
        return result
    except Exception as exc:  # noqa: BLE001
        with _OPEN_INITIATOR_LOCK:
            cur = _OPEN_INITIATOR_WINDOWS.get(key)
            if cur is entry:
                cur["in_progress"] = False
                # Leave collected=False so disconnect/teardown can retry.
        LOG.error(
            "Connect-window dmesg collection failed on %s: %s",
            getattr(node, "hostname", "?"),
            exc,
        )
        return None


def safe_collect_open_initiator_windows(
    nodes: Optional[Iterable] = None,
) -> List[dict]:
    """Collect any still-open initiator connect windows (cleanup fallback)."""
    results = []
    with _OPEN_INITIATOR_LOCK:
        if nodes is None:
            entries = [
                (key, dict(val))
                for key, val in _OPEN_INITIATOR_WINDOWS.items()
                if val and not val.get("collected") and val.get("since")
            ]
        else:
            wanted = {_node_key(n) for n in nodes}
            entries = [
                (key, dict(val))
                for key, val in _OPEN_INITIATOR_WINDOWS.items()
                if key in wanted
                and val
                and not val.get("collected")
                and val.get("since")
            ]
    for _, entry in entries:
        node = entry.get("node")
        if node is None:
            continue
        result = collect_open_initiator_window(node)
        if result:
            results.append(result)
    return results


def _parse_dmesg_t_timestamp(line: str) -> Optional[datetime]:
    match = _DMESG_T_TS_RE.match((line or "").strip())
    if not match:
        return None
    stamp = re.sub(r"\s+", " ", match.group(1).strip())
    try:
        return datetime.strptime(stamp, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        return None


def _filter_dmesg_t_by_window(text: str, since: str, until: str) -> str:
    """Keep dmesg -T lines inside [since, until] when headers parse."""
    start = _parse_window_ts(since)
    end = _parse_window_ts(until)
    if not start or not end:
        return text or ""
    kept = []
    for line in (text or "").splitlines():
        ts = _parse_dmesg_t_timestamp(line)
        if ts is None:
            # Keep unparsable lines only when we already accepted something
            # in-window (continuation); skip leading noise outside window.
            if kept:
                kept.append(line)
            continue
        if start <= ts <= end:
            kept.append(line)
    return "\n".join(kept)


def _collect_dmesg_timestamped(node, since: str, until: str) -> Tuple[str, str]:
    """Return (dmesg_text, label) preferring human-readable ``dmesg -T``."""
    labeled = _exec(
        node,
        f"dmesg -T --since '{since}' --level=err,crit,alert,emerg 2>/dev/null || true",
    )
    # Require a real dmesg -T wall-clock header. Bare ``[seconds]`` stamps also
    # contain ``[`` and must not be treated as human-readable -T output.
    if (
        (labeled or "").strip()
        and "unrecognized option" not in (labeled or "").lower()
        and _DMESG_T_TS_RE.search(labeled)
    ):
        return (
            labeled.strip(),
            f"dmesg -T --since '{since}' --level=err,crit,alert,emerg",
        )

    plain_since = _exec(
        node,
        f"dmesg --since '{since}' --level=err,crit,alert,emerg 2>/dev/null || true",
    )
    dmesg_t = _exec(
        node, "dmesg -T --level=err,crit,alert,emerg 2>/dev/null || dmesg -T || true"
    )
    filtered = _filter_dmesg_t_by_window(dmesg_t, since, until)
    if filtered.strip() and _DMESG_T_TS_RE.search(filtered):
        return (
            filtered.strip(),
            f"dmesg -T --level=err,crit,alert,emerg (filtered to {since} -> {until})",
        )
    if (plain_since or "").strip():
        # No usable -T window; keep level filter and rely on journalctl for stamps.
        return (
            plain_since.strip(),
            f"dmesg --since '{since}' --level=err,crit,alert,emerg "
            "(no dmesg -T window; journalctl is timestamp source of truth)",
        )
    return (
        (dmesg_t or "").strip(),
        "dmesg -T --level=err,crit,alert,emerg (unfiltered fallback)",
    )


def resolve_run_log_dir() -> str:
    """Resolve the cephci run directory that backs the results / Cluster Logs page.

    Prefer FileHandler directory (active run dir from run.py), then TestMetaData /
    Log.log_dir. Prefer non-/tmp when both exist (Jenkins --log-dir / magna).
    """
    candidates = []

    if getattr(LOG, "log_dir", None):
        candidates.append(LOG.log_dir)

    meta = LOG.config if hasattr(LOG, "config") else {}
    if meta is not None:
        for key in ("log_dir", "run_dir", "run_directory"):
            value = meta.get(key)
            if value:
                candidates.append(str(value))

    for handler in logging.getLogger("cephci").handlers:
        if isinstance(handler, logging.FileHandler):
            candidates.append(os.path.dirname(handler.baseFilename))

    for path in candidates:
        if path and os.path.isdir(path) and not str(path).startswith("/tmp/"):
            return path
    for path in candidates:
        if path and os.path.isdir(path):
            return path
    for path in candidates:
        if path:
            return path

    return os.getcwd()


def get_node_timestamp(node) -> str:
    """Return node-local wall clock suitable for journalctl --since/--until."""
    out, _ = node.exec_command(
        cmd="date '+%Y-%m-%d %H:%M:%S'", sudo=True, check_ec=False
    )
    ts = (out or "").strip()
    if not ts:
        # Fallback avoids empty --since which makes journalctl behave oddly.
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LOG.info(
            "Empty date from %s; using local fallback timestamp %s",
            getattr(node, "hostname", "?"),
            ts,
        )
    return ts


def arm_nvmeof_journal_window(config, node) -> Optional[str]:
    """Stamp config with node-local start time for pre-cleanup journal collect.

    Call once near test/gateway start for suites that do not use NVMeService.
    Idempotent: keeps the first stamp. Binds test config for Comments without
    clearing artifacts when the stamp already exists.
    """
    global _ACTIVE_CONFIG
    if not isinstance(config, dict):
        return None
    existing = config.get(CONFIG_JOURNAL_SINCE_KEY)
    if existing:
        with _ACTIVE_CONFIG_LOCK:
            _ACTIVE_CONFIG = config
        return existing
    try:
        ts = get_node_timestamp(node)
    except Exception as exc:  # noqa: BLE001
        LOG.info("Failed to arm NVMeoF journal window: %s", exc)
        return None
    config[CONFIG_JOURNAL_SINCE_KEY] = ts
    # First arm for this test — reset Comments / open-window isolation state.
    bind_test_config(config, force_reset=True)
    return ts


def _safe_unit_filename(unit: str) -> str:
    return re.sub(r"[^\w.\-]+", "_", unit or "unknown_unit")


def _artifact_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_artifact(subdir: str, filename: str, content: str) -> Optional[str]:
    """Write artifact under Cluster Logs (``<run_dir>/ceph_logs/...``).

    Details live only in Cluster Logs; the test log page is not used for content.
    """
    try:
        run_dir = resolve_run_log_dir()
        if str(run_dir).startswith("/tmp/"):
            LOG.info(
                "NVMeoF Cluster Logs under %s (pass --log-dir or use "
                "/ceph/cephci-jenkins for the shared log server)",
                run_dir,
            )
        base = os.path.join(run_dir, subdir)
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, filename)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content if content is not None else "")
        return path
    except Exception as exc:  # noqa: BLE001
        LOG.error(
            "Failed to write NVMeoF Cluster Logs artifact %s/%s: %s",
            subdir,
            filename,
            exc,
        )
        return None


def _exec(node, cmd: str) -> str:
    out, err = node.exec_command(cmd=cmd, sudo=True, check_ec=False)
    text = out if out is not None else ""
    if err and not text:
        return str(err)
    return text


def _is_journal_content_line(line: str) -> bool:
    stripped = (line or "").strip()
    return bool(stripped) and not stripped.startswith("--")


def _dedupe(lines: Iterable[str]) -> List[str]:
    seen = set()
    unique = []
    for line in lines:
        if line not in seen:
            seen.add(line)
            unique.append(line)
    return unique


def _raw_line_key(line: str) -> str:
    """Normalize a log line for cross-source dedupe (strip our prefixes)."""
    text = line or ""
    for prefix in (
        "[journalctl -p err] ",
        "[journalctl NVMe/IO] ",
        "[dmesg -T] ",
        "[dmesg] ",
        "[list-subsys] ",
    ):
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text.strip()


def _dedupe_prefixed(lines: Iterable[str]) -> List[str]:
    """Dedupe WARN lines that differ only by collection-source prefix."""
    seen_raw = set()
    unique = []
    for line in lines:
        key = _raw_line_key(line)
        if key in seen_raw:
            continue
        seen_raw.add(key)
        unique.append(line)
    return unique


def _match_initiator_issue_lines(text: str) -> List[str]:
    return [
        ln
        for ln in (text or "").splitlines()
        if INITIATOR_ISSUE_RE.search(ln) and _is_journal_content_line(ln)
    ]


def _is_ha_expected_initiator_line(line: str) -> bool:
    """True for HA path-flap reconnect noise (do not elevate to Comments/ERROR)."""
    return bool(HA_EXPECTED_INITIATOR_RE.search(line or ""))


def _filter_comment_errors(lines: Iterable[str]) -> List[str]:
    """Keep only non-HA-expected lines for LOG.error / index Comments."""
    return [
        ln
        for ln in lines
        if ln and not _is_ha_expected_initiator_line(_raw_line_key(ln))
    ]


def _filter_warn_priority_lines(text: str) -> List[str]:
    """Keep non-meta journal lines from a -p warning dump."""
    return [
        ln
        for ln in (text or "").splitlines()
        if _is_journal_content_line(ln) and "No entries" not in ln
    ]


def _is_gateway_journal_noise(line: str) -> bool:
    """True for stderr INFO dumps / SPDK XXX *ERROR* noise (not customer-facing)."""
    return bool(
        _GATEWAY_STDERR_INFO_RE.search(line or "")
        or _GATEWAY_SPDK_XXX_NOISE_RE.search(line or "")
    )


def _is_ha_expected_gateway_line(line: str) -> bool:
    """True for HA keep-alive host-disconnect noise on the gateway journal."""
    return bool(HA_EXPECTED_GATEWAY_RE.search(line or ""))


def _filter_gateway_err_content(lines: Iterable[str]) -> List[str]:
    """Keep real error content from journalctl -p err (drop INFO-on-stderr noise)."""
    out: List[str] = []
    for ln in lines:
        if not _is_journal_content_line(ln) or _is_gateway_journal_noise(ln):
            continue
        if _is_ha_expected_gateway_line(ln):
            continue
        if _GATEWAY_CONTENT_ERROR_RE.search(ln) or GATEWAY_CUSTOMER_CRITICAL_RE.search(
            ln
        ):
            out.append(ln)
    return out


def _filter_gateway_critical_hits(lines: Iterable[str]) -> List[str]:
    """Customer-critical hits minus config INFO / SPDK XXX / HA keep-alive noise."""
    return _dedupe(
        [
            ln
            for ln in lines
            if _is_journal_content_line(ln)
            and not _is_gateway_journal_noise(ln)
            and not _is_ha_expected_gateway_line(ln)
            and GATEWAY_CUSTOMER_CRITICAL_RE.search(ln)
        ]
    )


def check_initiator_nvme_paths(node) -> dict:
    """Customer first check after IO pain: ``nvme list-subsys`` path health.

    Results are returned for Cluster Logs artifacts; no test-log spam.
    """
    hostname = getattr(node, "hostname", "unknown-host")
    out = _exec(node, "nvme list-subsys 2>/dev/null || true")
    text = (out or "").strip()

    unhealthy = []
    # Controller lines look like: `` +- nvme1 tcp traddr=...,trsvcid=4420 live``
    for line in text.splitlines():
        lower = line.lower()
        if "traddr=" not in lower:
            continue
        flagged = False
        for state in UNHEALTHY_PATH_STATES:
            if re.search(rf"\b{state}\b", lower):
                unhealthy.append(line.strip())
                flagged = True
                break
        if not flagged and not re.search(r"\blive\b", lower):
            unhealthy.append(line.strip())

    return {
        "hostname": hostname,
        "list_subsys": text,
        "unhealthy_paths": unhealthy,
        "empty": not bool(text),
    }


def collect_initiator_kernel_logs(
    node,
    since: str,
    until: Optional[str] = None,
    window_label: str = "session",
    publish_comments: bool = True,
    path_check: bool = True,
) -> dict:
    """Collect journalctl -k + dmesg -T for an initiator time window.

    Full content goes to Cluster Logs. Test log ERROR + index Comments only for
    real err+ content (HA reconnect noise suppressed) or Cluster Logs write
    failure. Never fails the test.

    When ``publish_comments`` is False (FIO sub-window while a connect window is
    armed), artifacts and LOG.error still run; index Comments are left to the
    broader connect→disconnect collector (host+time dedupe).
    """
    hostname = getattr(node, "hostname", "unknown-host")
    if not since:
        since = get_node_timestamp(node)
        LOG.info("Missing %s since on %s; using %s", window_label, hostname, since)
    until = until or get_node_timestamp(node)
    header = (
        f"{window_label} kernel/dmesg check on {hostname}\n"
        f"Window: since='{since}' until='{until}'\n"
        f"{'=' * 72}\n"
    )
    sections = []
    archive_lines: List[str] = []
    # Lines that qualify for LOG.error + index.html Comments only.
    comment_error_lines: List[str] = []

    # journalctl -p err is authoritative for ERROR comments (wall-clock stamps).
    j_all = _exec(
        node,
        f"journalctl -k --since '{since}' --until '{until}' --no-pager "
        f"--output=short-iso",
    )
    j_err = _exec(
        node,
        f"journalctl -k -p err --since '{since}' --until '{until}' --no-pager "
        f"--output=short-iso",
    )
    sections.append(
        f"--- journalctl -k -p err --since/--until --output=short-iso ---\n"
        f"{(j_err or '').strip()}\n"
    )
    for line in (j_err or "").splitlines():
        if _is_journal_content_line(line):
            tagged = f"[journalctl -p err] {line}"
            archive_lines.append(tagged)
            comment_error_lines.append(tagged)

    pattern_hits = _match_initiator_issue_lines(j_all)
    sections.append(
        "--- journalctl -k (customer NVMe/IO/hang patterns) ---\n"
        + ("\n".join(pattern_hits) if pattern_hits else "(none)")
        + "\n"
    )
    err_raw = {_raw_line_key(ln) for ln in comment_error_lines}
    for line in pattern_hits:
        tagged = f"[journalctl NVMe/IO] {line}"
        archive_lines.append(tagged)
        # Critical markers only (keep-alive / fatal) — not every reconnect notice.
        if _raw_line_key(line) in err_raw:
            continue
        if GATEWAY_CUSTOMER_CRITICAL_RE.search(line) or re.search(
            r"(I/O error|Keep-alive timer expired|keep.?alive.*(expir|timeout)|"
            r"\bOops\b|\bBUG:|Call Trace|NULL pointer|soft lockup)",
            line,
            re.IGNORECASE,
        ):
            comment_error_lines.append(tagged)

    # Human-readable dmesg -T (fallback to journalctl stamps when -T/--since
    # unavailable on the kernel).
    dmesg_text, dmesg_label = _collect_dmesg_timestamped(node, since, until)
    sections.append(f"--- {dmesg_label} ---\n{(dmesg_text or '').strip()}\n")
    if dmesg_text.strip():
        unfiltered = "unfiltered fallback" in dmesg_label
        if unfiltered:
            source_lines = _match_initiator_issue_lines(dmesg_text)
        else:
            source_lines = [
                ln
                for ln in dmesg_text.splitlines()
                if ln.strip() and not ln.strip().startswith("--")
            ]
        tag_prefix = "[dmesg -T]" if "dmesg -T" in dmesg_label else "[dmesg]"
        for line in source_lines:
            if not line.strip():
                continue
            tagged = f"{tag_prefix} {line}"
            archive_lines.append(tagged)
            if unfiltered:
                if re.search(
                    r"(I/O error|Keep-alive timer expired|keep.?alive.*(expir|timeout)|"
                    r"\bOops\b|\bBUG:|Call Trace|NULL pointer|soft lockup)",
                    line,
                    re.IGNORECASE,
                ):
                    comment_error_lines.append(tagged)
            else:
                comment_error_lines.append(tagged)

    path_info = {
        "hostname": hostname,
        "list_subsys": "",
        "unhealthy_paths": [],
        "empty": True,
    }
    if path_check:
        path_info = check_initiator_nvme_paths(node)
        sections.append(
            f"--- nvme list-subsys (post-{window_label}) ---\n"
            f"{path_info.get('list_subsys') or '(empty)'}\n"
        )
        for line in path_info.get("unhealthy_paths") or []:
            archive_lines.append(f"[list-subsys] {line}")
        if path_info.get("empty"):
            archive_lines.append(f"[list-subsys] empty output after {window_label}")

    unique_archive = _dedupe_prefixed(archive_lines)
    # HA reconnect noise stays in artifacts only — not ERROR / Comments.
    unique_errors = _filter_comment_errors(_dedupe_prefixed(comment_error_lines))
    ha_noise_count = len(_dedupe_prefixed(comment_error_lines)) - len(unique_errors)

    artifact_body = header + "\n".join(sections)
    if unique_archive:
        artifact_body += (
            "\n--- archived findings ---\n" + "\n".join(unique_archive) + "\n"
        )
    if ha_noise_count > 0:
        artifact_body += (
            f"\n--- HA-expected reconnect noise suppressed from Comments "
            f"(count={ha_noise_count}) ---\n"
        )

    safe_label = re.sub(r"[^\w.\-]+", "_", window_label or "session")
    artifact = _write_artifact(
        DMESG_ARTIFACT_SUBDIR,
        f"{hostname}_{safe_label}_{_artifact_stamp()}.log",
        artifact_body,
    )
    rel = _cluster_logs_relpath(artifact) if artifact else None
    # Comments/ERROR only: real -p err / critical markers (not HA reconnect), or
    # Cluster Logs write failure.
    if not artifact:
        LOG.error(
            "%s dmesg collection on %s failed to write Cluster Logs (%s -> %s)",
            window_label,
            hostname,
            since,
            until,
        )
        if publish_comments:
            publish_index_comment(
                f"[Initiator] Cluster Logs write FAILED on {hostname} "
                f"[{since} -> {until}]"
            )
    elif unique_errors:
        LOG.error(
            "Initiator dmesg/journalctl ERRORS on %s from %s to %s "
            "(count=%d); see Cluster Logs: %s",
            hostname,
            since,
            until,
            len(unique_errors),
            rel,
        )
        if publish_comments:
            publish_initiator_index_comment(
                hostname,
                since,
                until,
                unique_errors,
                rel,
                window_label=window_label,
            )
    else:
        extra = (
            f"; HA reconnect noise archived={ha_noise_count}"
            if ha_noise_count > 0
            else ""
        )
        LOG.info(
            "Initiator %s dmesg/journalctl collected on %s from %s to %s "
            "(no ERRORS%s); Cluster Logs: %s",
            window_label,
            hostname,
            since,
            until,
            extra,
            rel,
        )

    return {
        "hostname": hostname,
        "since": since,
        "until": until,
        "warnings": unique_archive,
        "errors": unique_errors,
        "artifact": artifact,
        "list_subsys": path_info,
        "window_label": window_label,
    }


def collect_initiator_fio_dmesg(node, since: str, until: Optional[str] = None) -> dict:
    """Collect kernel/dmesg + path state for the FIO window on an initiator.

    Always archives under Cluster Logs. Index Comments are skipped when a
    connect→disconnect window is armed for the same host (that broader window
    owns Comments; host+time dedupe avoids duplicates).
    """
    publish_comments = not has_open_initiator_window(node)
    return collect_initiator_kernel_logs(
        node,
        since=since,
        until=until,
        window_label="fio_window",
        publish_comments=publish_comments,
        path_check=True,
    )


def _gateway_targets_from_service(nvme_service) -> List[Tuple]:
    targets = []
    for gw in getattr(nvme_service, "gateways", None) or []:
        try:
            unit = gw.system_unit_id
        except Exception as exc:  # noqa: BLE001
            LOG.info(
                "Could not resolve system_unit_id for gateway on %s: %s",
                getattr(getattr(gw, "node", None), "hostname", "?"),
                exc,
            )
            continue
        if not unit:
            LOG.info(
                "Empty system_unit_id for gateway on %s",
                getattr(getattr(gw, "node", None), "hostname", "?"),
            )
            continue
        targets.append((gw.node, unit, gw.node.hostname))
    return targets


def _gateway_targets_from_config(ceph_cluster, config) -> List[Tuple]:
    if not ceph_cluster or not config:
        return []
    gw_nodes = config.get("gw_nodes") or config.get("gw_node")
    if not gw_nodes:
        return []
    if not isinstance(gw_nodes, list):
        gw_nodes = [gw_nodes]

    nodes = []
    try:
        nodes = get_nodes_by_ids(ceph_cluster, gw_nodes)
    except Exception:  # noqa: BLE001
        for name in gw_nodes:
            try:
                nodes.append(get_node_by_id(ceph_cluster, name))
            except Exception as exc:  # noqa: BLE001
                LOG.info("Could not resolve gateway node %s: %s", name, exc)

    targets = []
    for node in nodes:
        try:
            # Note: get_service_unit returns the first *@nvmeof* match only.
            unit = SystemCtl(node).get_service_unit("*@nvmeof*")
            if unit:
                targets.append((node, unit, node.hostname))
        except Exception as exc:  # noqa: BLE001
            LOG.info(
                "Could not resolve nvmeof systemd unit on %s: %s", node.hostname, exc
            )
    return targets


def _mark_journals_collected(cfg: Optional[dict], nvme_service, key: str) -> None:
    if isinstance(cfg, dict):
        cfg["_nvmeof_journals_collected"] = True
    if nvme_service is not None:
        nvme_service._journals_collected = True
    with _JOURNALS_LOCK:
        _JOURNALS_COLLECTED_KEYS.add(key)


def _already_collected(cfg: Optional[dict], nvme_service, key: str) -> bool:
    """Idempotency for repeat teardown calls.

    Prefer the NVMeService instance flag when present so a mid-test
    delete→redeploy→new NVMeService can collect again for the same config.
    Config/process keys cover older helpers that pass only ``config``.
    """
    if nvme_service is not None:
        return bool(getattr(nvme_service, "_journals_collected", False))
    if isinstance(cfg, dict) and cfg.get("_nvmeof_journals_collected"):
        return True
    with _JOURNALS_LOCK:
        return key in _JOURNALS_COLLECTED_KEYS


def collect_nvmeof_service_journals(
    nvme_service=None,
    ceph_cluster=None,
    config=None,
    since: Optional[str] = None,
    gateways: Optional[Iterable] = None,
) -> dict:
    """Collect nvmeof unit journals before cleanup (customer triage view).

    1. ``journalctl -u <unit> -p warning`` — full WARNING+ dump (all lines logged).
    2. Same window scanned for customer-critical NOTICE-level markers.

    Soft check only. Full journal content goes to Cluster Logs artifacts.
    Test log ERROR + Comments for real err/critical content (HA keep-alive
    disconnect noise suppressed) or Cluster Logs write failure. Idempotent via
    config flag / NVMeService flag / process key after a successful collect.
    """
    cfg = config
    if cfg is None and nvme_service is not None:
        cfg = getattr(nvme_service, "config", None)

    if since is None and nvme_service is not None:
        since = getattr(nvme_service, "test_start_time", None)
    if since is None and isinstance(cfg, dict):
        since = cfg.get(CONFIG_JOURNAL_SINCE_KEY)
    if not since:
        since = DEFAULT_JOURNAL_SINCE

    cluster = ceph_cluster or getattr(nvme_service, "ceph_cluster", None)
    idem_key = f"{id(cluster)}|{since}|{id(cfg) if cfg is not None else 0}"
    if _already_collected(
        cfg if isinstance(cfg, dict) else None, nvme_service, idem_key
    ):
        return {"skipped": True, "results": []}

    targets: List[Tuple] = []
    if gateways:
        for gw in gateways:
            try:
                unit = gw.system_unit_id
                if unit:
                    targets.append((gw.node, unit, gw.node.hostname))
            except Exception as exc:  # noqa: BLE001
                LOG.info("Skipping gateway journal target: %s", exc)
    elif nvme_service is not None:
        targets = _gateway_targets_from_service(nvme_service)
        if not targets:
            targets = _gateway_targets_from_config(
                cluster, cfg if isinstance(cfg, dict) else {}
            )
    else:
        targets = _gateway_targets_from_config(
            cluster, cfg if isinstance(cfg, dict) else {}
        )

    if not targets:
        # Do not mark collected — unit lookup may succeed on a later teardown call.
        LOG.info(
            "No nvmeof gateway units found for journal collection (since=%s); "
            "will retry on a later collect if units appear",
            since,
        )
        return {"skipped": False, "results": [], "since": since}

    results = []
    for node, unit, hostname in targets:
        # Archive warning+ for Cluster Logs; ERROR in test log only for -p err /
        # customer-critical content.
        warn_cmd = (
            f"journalctl -u '{unit}' -p warning --since '{since}' --no-pager "
            f"--output=short-iso"
        )
        raw_warn = _exec(node, warn_cmd)
        body_lines = _filter_warn_priority_lines(raw_warn)

        err_cmd = (
            f"journalctl -u '{unit}' -p err --since '{since}' --no-pager "
            f"--output=short-iso"
        )
        raw_err = _exec(node, err_cmd)
        # -p err catches gateway INFO written to stderr; keep content ERROR only.
        err_lines = _filter_gateway_err_content(_filter_warn_priority_lines(raw_err))

        crit_pattern = (
            "keep.?alive|Inactive admin qpair|IC_RESP|Terminating SPDK|"
            "spdk_get_version failed|updating gateway state|beacon timeout|"
            "Segmentation fault|core dumped|dumped core|"
            "FAILED ceph_assert|ceph_assert|Caught signal|"
            "Gateway subprocess terminated|GatewayServer: SIGCHLD|"
            "GatewayServer exception|"
            "\\bOMAP\\b|No such lock|Failure while getting state|"
            "Failed to lock OMAP|Unable to lock OMAP|"
            "DEADLINE_EXCEEDED|UNAVAILABLE|"
            "failed to create bdev|Listener .* failed|reactor .* failed|"
            "\\*ERROR\\*|\\bERROR\\b"
        )
        crit_cmd = (
            f"journalctl -u '{unit}' --since '{since}' --no-pager --output=short-iso "
            f"| grep -iE '{crit_pattern}' || true"
        )
        raw_crit = _exec(node, crit_cmd)
        critical_hits = _filter_gateway_critical_hits((raw_crit or "").splitlines())

        until = get_node_timestamp(node)
        artifact_name = (
            f"{hostname}_{_safe_unit_filename(unit)}_{_artifact_stamp()}.log"
        )
        # Unfiltered critical scan for Cluster Logs triage; ERROR/Comments use
        # filtered err_lines / critical_hits (HA keep-alive suppressed).
        ha_crit_suppressed = _dedupe(
            [
                ln
                for ln in (raw_crit or "").splitlines()
                if _is_journal_content_line(ln)
                and _is_ha_expected_gateway_line(ln)
                and GATEWAY_CUSTOMER_CRITICAL_RE.search(ln)
            ]
        )
        artifact_body = (
            f"nvmeof journal on {hostname}\n"
            f"unit={unit}\n"
            f"since='{since}' until='{until}'\n"
            f"warn_cmd={warn_cmd}\n"
            f"err_cmd={err_cmd}\n"
            f"{'=' * 72}\n"
            f"--- journalctl -p err ---\n{raw_err or ''}\n"
            f"{'=' * 72}\n"
            f"--- journalctl -p warning ---\n{raw_warn or ''}\n"
            f"{'=' * 72}\n"
            f"--- customer-critical marker hits (any priority; HA keep-alive "
            f"suppressed from Comments) ---\n"
            + ("\n".join(critical_hits) if critical_hits else "(none)")
            + "\n"
        )
        if ha_crit_suppressed:
            artifact_body += (
                f"{'=' * 72}\n"
                f"--- HA-expected gateway keep-alive noise suppressed from "
                f"Comments (count={len(ha_crit_suppressed)}) ---\n"
                + "\n".join(ha_crit_suppressed)
                + "\n"
            )
        artifact = _write_artifact(
            JOURNAL_ARTIFACT_SUBDIR, artifact_name, artifact_body
        )
        rel = _cluster_logs_relpath(artifact) if artifact else None
        has_real_errors = bool(err_lines) or bool(critical_hits)

        if not artifact:
            LOG.error(
                "nvmeof journal collection on %s unit=%s failed to write Cluster Logs",
                hostname,
                unit,
            )
            publish_index_comment(
                f"[Gateway] Cluster Logs write FAILED on {hostname} ({unit}) "
                f"[{since} -> {until}]"
            )
        elif has_real_errors:
            # Comments only for -p err and/or critical markers (not warn-only).
            LOG.error(
                "nvmeof gateway journal ERRORS on %s unit=%s from %s to %s "
                "(err=%d critical=%d warn=%d); see Cluster Logs: %s",
                hostname,
                unit,
                since,
                until,
                len(err_lines),
                len(critical_hits),
                len(body_lines),
                rel,
            )
            publish_index_comment(
                _format_gateway_comment(
                    hostname,
                    unit,
                    since,
                    until,
                    err_lines,
                    critical_hits,
                    rel,
                )
            )
        else:
            LOG.info(
                "nvmeof gateway journal collected on %s unit=%s from %s to %s "
                "(no ERRORS; warn=%d archived); Cluster Logs: %s",
                hostname,
                unit,
                since,
                until,
                len(body_lines),
                rel,
            )

        results.append(
            {
                "hostname": hostname,
                "unit": unit,
                "since": since,
                "until": until,
                "has_entries": has_real_errors or bool(body_lines),
                "err_count": len(err_lines),
                "line_count": len(body_lines),
                "critical_count": len(critical_hits),
                "artifact": artifact,
            }
        )

    _mark_journals_collected(
        cfg if isinstance(cfg, dict) else None, nvme_service, idem_key
    )
    if isinstance(cfg, dict):
        flush_nvmeof_log_comments(cfg)
    return {"skipped": False, "results": results, "since": since}


def safe_collect_nvmeof_service_journals(**kwargs) -> dict:
    """Never-raise wrapper for teardown / delete paths.

    Also drains any still-open initiator connect→disconnect windows (tests that
    never disconnect before cleanup).
    """
    try:
        cfg = kwargs.get("config")
        nvme_service = kwargs.get("nvme_service")
        if cfg is None and nvme_service is not None:
            cfg = getattr(nvme_service, "config", None)
        if isinstance(cfg, dict):
            # Re-bind without wiping this test's artifacts. If the active config
            # object changed, drop leftover comment lines from a prior test so
            # flush cannot contaminate the current Comments cell.
            global _ACTIVE_CONFIG
            with _ACTIVE_CONFIG_LOCK:
                if cfg is not _ACTIVE_CONFIG:
                    _COMMENT_LINES.clear()
                    _PUBLISHED_INITIATOR_WINDOWS.clear()
                _ACTIVE_CONFIG = cfg
        # Fallback: collect initiator logs when no disconnect ran.
        try:
            safe_collect_open_initiator_windows()
        except Exception as init_exc:  # noqa: BLE001
            LOG.error("Open initiator-window collection failed: %s", init_exc)
        result = collect_nvmeof_service_journals(**kwargs)
        flush_nvmeof_log_comments(cfg)
        return result
    except Exception as exc:
        LOG.error("NVMeoF journal collection failed: %s", exc)
        publish_index_comment(f"[Gateway] journal collection FAILED: {exc}")
        return {"skipped": False, "error": str(exc), "results": []}
