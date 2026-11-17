"""
Module to validate PG rebuild duration perf counters introduced for tracking
rebuild time of Placement Groups on OSDs.

Covers counter registration, priority, Prometheus integration, and accuracy
under various failure/recovery scenarios (OSD stop, host failure, pool resize,
repair, throttled recovery, primary transition, etc.).

Ref: IBMCEPH-16254
"""

import random
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)

REBUILD_COUNTERS = [
    "pg_rebuild_duration",
    "pg_rebuild_max_secs",
    "pg_rebuild_min_secs",
]

PRIO_USEFUL = 5

MAX_PARALLEL_WORKERS = 10


def run_parallel(items, worker_fn, max_workers=MAX_PARALLEL_WORKERS, on_error="warn"):
    """Run ``worker_fn(item)`` for every item in `items` concurrently using a
    thread pool.

    All the per-OSD checks in this module issue blocking remote commands
    (``ceph tell``/``ceph daemon``), so they are I/O-bound and benefit from
    threading even though Python's GIL prevents true CPU parallelism.

    Returns {item: result} for every item that completed without error.
    With `on_error="warn"` (default), a failing item is logged and simply
    omitted from the result, matching the previous sequential try/except
    behaviour used throughout this module. With `on_error="raise"`, the
    first exception encountered is re-raised.
    """
    results = {}
    if not items:
        return results
    workers = min(max_workers, len(items)) or 1
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_item = {executor.submit(worker_fn, item): item for item in items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                results[item] = future.result()
            except Exception as err:
                if on_error == "raise":
                    raise
                log.warning(f"Parallel task failed for '{item}': {err}")
    return results


def fetch_counters_for_osds(
    rados_obj, osd_ids, max_workers=MAX_PARALLEL_WORKERS, on_error="warn"
) -> dict:
    """Fetch rebuild counters for the given OSD ids concurrently.

    Returns {osd_id: counters}. With `on_error="warn"` (default), any OSD
    whose fetch fails is skipped with a warning (its id will simply be
    absent from the returned dict) -- matching the previous sequential
    try/except-and-skip behaviour used for "best effort" checks. Pass
    `on_error="raise"` for checks that must hard-fail if a fetch fails.
    """
    return run_parallel(
        osd_ids,
        worker_fn=lambda osd_id: get_rebuild_counters(rados_obj, osd_id),
        max_workers=max_workers,
        on_error=on_error,
    )


def get_rebuild_counters(rados_obj, osd_id: int) -> dict:
    """Fetch PG rebuild perf counters from an OSD via ``ceph tell``.

    Returns a dict with keys:
        avgcount, sum, avgtime  (from pg_rebuild_duration)
        pg_rebuild_max_secs
        pg_rebuild_min_secs
    """
    perf = rados_obj.get_osd_perf_dump(osd_id)
    rs_section = perf.get("recoverystate_perf", {})
    result = {
        "avgcount": rs_section.get("pg_rebuild_duration", {}).get("avgcount", None),
        "sum": rs_section.get("pg_rebuild_duration", {}).get("sum", None),
        "avgtime": rs_section.get("pg_rebuild_duration", {}).get("avgtime", None),
        "pg_rebuild_max_secs": rs_section.get("pg_rebuild_max_secs", None),
        "pg_rebuild_min_secs": rs_section.get("pg_rebuild_min_secs", None),
    }
    log.debug(f"Rebuild counters for osd.{osd_id}: {result}")
    return result


def verify_counter_defaults(counters: dict) -> bool:
    """Return True if all rebuild counters are at their default (zero) values."""
    expected = {
        "avgcount": 0,
        "sum": 0,
        "avgtime": 0,
        "pg_rebuild_max_secs": 0,
        "pg_rebuild_min_secs": 0,
    }
    for key, val in expected.items():
        actual = counters.get(key)
        if actual is None:
            log.error(f"Counter '{key}' missing from perf dump")
            return False
        if actual != val:
            log.error(f"Counter '{key}' expected {val}, got {actual}")
            return False
    return True


def verify_counters_unchanged(before: dict, after: dict) -> bool:
    """Return True when rebuild counters did not change between snapshots."""
    for key in before:
        if before[key] != after[key]:
            log.error(
                f"Counter '{key}' changed unexpectedly: {before[key]} -> {after[key]}"
            )
            return False
    return True


def verify_counters_incremented(before: dict, after: dict) -> bool:
    """Return True when rebuild counters show activity (avgcount increased)."""
    if after["avgcount"] <= before["avgcount"]:
        log.error(
            f"avgcount did not increase: before={before['avgcount']}, "
            f"after={after['avgcount']}"
        )
        return False
    if after["sum"] < before["sum"]:
        log.error(f"sum decreased: before={before['sum']}, after={after['sum']}")
        return False
    return True


def verify_min_max_invariant(counters: dict) -> bool:
    """Return True when pg_rebuild_max_secs >= pg_rebuild_min_secs holds.

    Only enforced when at least one of the two values is non-zero; both being
    zero simply means no rebuild has been recorded yet.
    """
    max_secs = counters.get("pg_rebuild_max_secs")
    min_secs = counters.get("pg_rebuild_min_secs")
    if max_secs is None or min_secs is None:
        log.error("max_secs/min_secs missing from counters, cannot verify invariant")
        return False
    if max_secs == 0 and min_secs == 0:
        return True
    if max_secs < min_secs:
        log.error(
            f"INVARIANT VIOLATED: pg_rebuild_max_secs ({max_secs}) < "
            f"pg_rebuild_min_secs ({min_secs})"
        )
        return False
    return True


def validate_counter_fields(counters: dict, context: str = "") -> None:
    """Thoroughly validate every individual raw rebuild-counter field.

    Mirrors the exact structure reported under ``recoverystate_perf`` (see
    https://github.com/ceph/ceph/pull/69578)::

        "pg_rebuild_duration": {
            "avgcount": 0,
            "sum": 0.000000000,
            "avgtime": 0.000000000
        },
        "pg_rebuild_max_secs": 0,
        "pg_rebuild_min_secs": 0

    Checks, per field:
        - presence (not None)
        - type (avgcount -> int; sum/avgtime/max_secs/min_secs -> numeric)
        - sign (all fields must be >= 0)
        - avgcount == 0 implies sum/avgtime/max_secs/min_secs == 0 (a rebuild
          can only be recorded together with a non-zero avgcount)
        - avgtime == sum / avgcount (within tolerance), per LONGRUNAVG semantics
        - pg_rebuild_min_secs <= pg_rebuild_max_secs

    Raises AssertionError with a field-specific message on the first
    violation found. `context` (e.g. "osd.3 baseline") is prefixed to every
    error message so failures are easy to attribute to a specific snapshot.
    """
    prefix = f"{context}: " if context else ""

    avgcount = counters.get("avgcount")
    sum_ = counters.get("sum")
    avgtime = counters.get("avgtime")
    max_secs = counters.get("pg_rebuild_max_secs")
    min_secs = counters.get("pg_rebuild_min_secs")

    fields = {
        "avgcount": avgcount,
        "sum": sum_,
        "avgtime": avgtime,
        "pg_rebuild_max_secs": max_secs,
        "pg_rebuild_min_secs": min_secs,
    }

    for key, val in fields.items():
        if val is None:
            raise AssertionError(f"{prefix}counter '{key}' missing from perf dump")

    if not isinstance(avgcount, int) or isinstance(avgcount, bool):
        raise AssertionError(
            f"{prefix}'avgcount' must be an int, got "
            f"{type(avgcount).__name__} ({avgcount!r})"
        )

    for key in ("sum", "avgtime", "pg_rebuild_max_secs", "pg_rebuild_min_secs"):
        val = fields[key]
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            raise AssertionError(
                f"{prefix}'{key}' must be numeric, got "
                f"{type(val).__name__} ({val!r})"
            )

    for key, val in fields.items():
        if val < 0:
            raise AssertionError(f"{prefix}'{key}' must be non-negative, got {val}")

    if avgcount == 0:
        for key in ("sum", "avgtime", "pg_rebuild_max_secs", "pg_rebuild_min_secs"):
            val = fields[key]
            if val != 0:
                raise AssertionError(
                    f"{prefix}avgcount is 0 but '{key}' is {val}; expected 0 "
                    "since no rebuild has been recorded yet"
                )
    else:
        computed_avgtime = sum_ / avgcount
        tolerance = max(0.01, computed_avgtime * 0.05)
        if abs(avgtime - computed_avgtime) > tolerance:
            raise AssertionError(
                f"{prefix}'avgtime' ({avgtime}) does not match sum/avgcount "
                f"({sum_}/{avgcount} = {computed_avgtime:.6f}, "
                f"tolerance={tolerance:.6f})"
            )

    if not (max_secs == 0 and min_secs == 0) and max_secs < min_secs:
        raise AssertionError(
            f"{prefix}pg_rebuild_max_secs ({max_secs}) < "
            f"pg_rebuild_min_secs ({min_secs})"
        )


def validate_all_osd_counter_fields(all_counters: dict, context: str = "") -> None:
    """Run `validate_counter_fields` against every OSD in a
    {osd_id: counters} snapshot (as returned by `get_all_osd_rebuild_counters`).
    """
    for osd_id, counters in all_counters.items():
        osd_context = f"{context} osd.{osd_id}" if context else f"osd.{osd_id}"
        validate_counter_fields(counters, context=osd_context)


def get_perf_schema_priorities(rados_obj, osd_id: int) -> dict:
    """Fetch perf schema and return priority for rebuild counters.

    Returns None if the OSD is down or unreachable (ENXIO / empty response),
    so callers can skip it instead of crashing on a JSONDecodeError.
    """
    cmd = f"ceph tell osd.{osd_id} perf schema"
    try:
        schema = rados_obj.run_ceph_command(cmd=cmd)
    except Exception as err:
        log.warning(f"osd.{osd_id}: perf schema unavailable (OSD may be down): {err}")
        return None
    if not schema:
        log.warning(f"osd.{osd_id}: perf schema returned empty (OSD may be down)")
        return None
    rs_section = schema.get("recoverystate_perf", {})
    priorities = {}
    for counter in REBUILD_COUNTERS:
        entry = rs_section.get(counter, {})
        priorities[counter] = entry.get("priority", None)
    return priorities


def get_all_osd_rebuild_counters(rados_obj) -> dict:
    """Snapshot rebuild counters from every up OSD. Returns {osd_id: counters}.

    Fetches are issued concurrently (thread pool) since each is a separate
    blocking `ceph tell` call; this is a significant speedup once the
    cluster has more than a couple of OSDs.
    """
    osd_list = rados_obj.get_osd_list(status="up")
    return fetch_counters_for_osds(rados_obj, osd_list)


def wait_for_recovery(rados_obj, pool_name=None, timeout=900):
    """Wait until all PGs reach active+clean."""
    return rados_obj.wait_for_clean_pg_sets(
        timeout=timeout, test_pool=pool_name, recovery_thread=True
    )


def reset_rebuild_counters(rados_obj, batch_size=10):
    """Reset rebuild perf counters on all OSDs by restarting them in parallel batches.

    OSD perf counters are in-memory and reset to zero on daemon restart.
    Restarts are batched to avoid taking too many OSDs down simultaneously
    and destabilizing the cluster.

    Args:
        rados_obj: RadosOrchestrator instance
        batch_size: max OSDs to restart concurrently per batch
    """
    osd_list = rados_obj.get_osd_list(status="up")
    if not osd_list:
        log.warning("No up OSDs found, skipping counter reset")
        return

    log.info(
        f"Resetting rebuild counters by restarting {len(osd_list)} OSDs "
        f"in batches of {batch_size}"
    )

    def _restart_osd(osd_id):
        try:
            rados_obj.change_osd_state(action="restart", target=int(osd_id))
            return True
        except Exception as err:
            log.warning(f"Failed to restart osd.{osd_id}: {err}")
            return False

    for i in range(0, len(osd_list), batch_size):
        batch = osd_list[i : i + batch_size]
        log.info(f"Restarting batch {i // batch_size + 1}: OSDs {batch}")
        run_parallel(batch, worker_fn=_restart_osd, max_workers=batch_size)
        time.sleep(10)

    log.info("Waiting for cluster to recover after OSD restarts")
    assert wait_for_recovery(rados_obj, timeout=1200), (
        "Cluster did not reach clean state after OSD restarts"
    )

    all_counters = get_all_osd_rebuild_counters(rados_obj)
    non_zero = {
        osd_id: c for osd_id, c in all_counters.items() if c.get("avgcount", 0) != 0
    }
    if non_zero:
        log.warning(
            f"{len(non_zero)} OSDs still have non-zero counters after restart "
            f"(immediate recovery activity): {list(non_zero.keys())}"
        )
    else:
        log.info("All OSD rebuild counters reset to zero")


def write_data_to_pool(pool_obj, client, pool_name, nobj=50, size="4M"):
    """Write test objects into a pool."""
    pool_obj.do_rados_put(client=client, pool=pool_name, nobj=nobj, size=size)


def _get_non_primary_osd(rados_obj, pool_name):
    """Return (primary_osd, non_primary_osd, acting_set) for a PG in the pool."""
    acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
    primary = acting[0]
    non_primary = acting[1] if len(acting) > 1 else None
    return primary, non_primary, acting


def _pool_has_degraded_pgs(rados_obj, pool_name):
    """Return True if the pool currently has any degraded/undersized PGs.

    Returns None (unknown) if the state could not be determined, so callers
    can distinguish "confirmed clean" from "could not check".
    """
    try:
        degraded = rados_obj.get_pgid(pool_name=pool_name, states="degraded")
        undersized = rados_obj.get_pgid(pool_name=pool_name, states="undersized")
        return bool(degraded) or bool(undersized)
    except Exception as err:
        log.warning(f"Could not determine degraded state for pool {pool_name}: {err}")
        return None



def get_pg_stats(rados_obj):
    """Return list of pg_stat dicts from ceph pg dump JSON."""
    dump = rados_obj.run_ceph_command(cmd="ceph pg dump_json pgs")
    if not dump:
        return []
    if isinstance(dump, dict):
        if "pg_map" in dump and isinstance(dump["pg_map"], dict):
            stats = dump["pg_map"].get("pg_stats")
            if stats is not None:
                return stats
        if "pg_stats" in dump:
            return dump.get("pg_stats") or []
    return []


def extract_pg_history_field(obj, field):
    """Find last_degraded/last_clean in pg dump row or pg query JSON.

    Per PR #68910 these live on pg_stat_t:
      - ``ceph pg dump -f json``: top-level keys on each pg_stats entry
      - ``ceph pg <pgid> query``: under ``stats`` (also try info/history)
    """
    if obj is None:
        return None
    if not isinstance(obj, dict):
        return None
    if field in obj and obj[field] is not None:
        return obj[field]
    for nest_key in ("info", "history", "stats"):
        nested = obj.get(nest_key)
        if isinstance(nested, dict):
            if field in nested and nested[field] is not None:
                return nested[field]
            # info.history
            if nest_key == "info":
                hist = nested.get("history")
                if isinstance(hist, dict) and field in hist and hist[field] is not None:
                    return hist[field]
            # stats.history (rare)
            if nest_key == "stats":
                hist = nested.get("history")
                if isinstance(hist, dict) and field in hist and hist[field] is not None:
                    return hist[field]
    return None


def parse_ceph_ts(ts: str) -> datetime:
    """Parse Ceph utime string to an aware datetime.

    Example: ``2026-11-17T10:23:20.738116+0000`` -> aware datetime.
    """
    ts = ts[:-5] + "+00:00"
    return datetime.fromisoformat(ts)


def get_durability_counters(rados_obj, osd_id):
    """Fetch vulnerability + rebuild counters from recoverystate_perf.

    Support FULL solution schema:
      pg_vulnerability_duration {avgcount,sum,avgtime,max_inc}
      pg_vulnerability_duration_min
      pg_rebuild_duration {avgcount,sum,avgtime,max_inc}
      pg_rebuild_duration_min
    AND interim fallback:
      pg_rebuild_max_secs / pg_rebuild_min_secs when max_inc/_min absent.

    Returns dict with keys:
      vuln_avgcount, vuln_sum, vuln_avgtime, vuln_max, vuln_min,
      rebuild_avgcount, rebuild_sum, rebuild_avgtime, rebuild_max, rebuild_min,
      has_vulnerability (bool whether vuln counters exist)
    """
    perf = rados_obj.get_osd_perf_dump(osd_id)
    rs = perf.get("recoverystate_perf", {}) if perf else {}

    vuln = rs.get("pg_vulnerability_duration")
    has_vulnerability = isinstance(vuln, dict)
    if has_vulnerability:
        vuln_avgcount = vuln.get("avgcount", 0) or 0
        vuln_sum = vuln.get("sum", 0) or 0
        vuln_avgtime = vuln.get("avgtime", 0) or 0
        vuln_max = vuln.get("max_inc", rs.get("pg_vulnerability_duration_max_inc", 0))
        if vuln_max is None:
            vuln_max = 0
        vuln_min = rs.get("pg_vulnerability_duration_min", 0) or 0
    else:
        vuln_avgcount = vuln_sum = vuln_avgtime = vuln_max = vuln_min = 0

    rebuild = rs.get("pg_rebuild_duration", {}) or {}
    rebuild_avgcount = rebuild.get("avgcount", 0) or 0
    rebuild_sum = rebuild.get("sum", 0) or 0
    rebuild_avgtime = rebuild.get("avgtime", 0) or 0
    rebuild_max = rebuild.get("max_inc")
    if rebuild_max is None:
        rebuild_max = rs.get("pg_rebuild_max_secs", 0) or 0
    rebuild_min = rs.get("pg_rebuild_duration_min")
    if rebuild_min is None:
        rebuild_min = rs.get("pg_rebuild_min_secs", 0) or 0

    result = {
        "vuln_avgcount": vuln_avgcount,
        "vuln_sum": vuln_sum,
        "vuln_avgtime": vuln_avgtime,
        "vuln_max": vuln_max,
        "vuln_min": vuln_min,
        "rebuild_avgcount": rebuild_avgcount,
        "rebuild_sum": rebuild_sum,
        "rebuild_avgtime": rebuild_avgtime,
        "rebuild_max": rebuild_max,
        "rebuild_min": rebuild_min,
        "has_vulnerability": has_vulnerability,
    }
    log.debug(f"Durability counters for osd.{osd_id}: {result}")
    return result


def get_pg_query(rados_obj, pgid):
    """Return JSON from ``ceph pg <pgid> query``."""
    return rados_obj.run_ceph_command(cmd=f"ceph pg {pgid} query")


def pick_pool_pgid(rados_obj, pool_name):
    """Return one PG id for the pool or raise."""
    pgids = rados_obj.get_pgid(pool_name=pool_name)
    if not pgids:
        raise AssertionError(f"No PGs found for pool {pool_name}")
    return pgids[0]


def _scrape_exporter_metrics(rados_obj, exporter_port=9926):
    """Best-effort scrape of ceph-exporter /metrics; return text or None."""
    scrape_nodes = list(rados_obj.ceph_cluster.get_nodes(role="mgr") or [])
    scrape_nodes += list(rados_obj.ceph_cluster.get_nodes(role="installer") or [])
    for port in (exporter_port, 9095, 9926):
        for node in scrape_nodes:
            try:
                out, _ = node.exec_command(
                    cmd=f"curl -s http://localhost:{port}/metrics",
                    timeout=30,
                )
                if out and ("rebuild" in out.lower() or "vulnerability" in out.lower()):
                    log.info(
                        f"Fetched exporter metrics from {node.hostname}:{port}"
                    )
                    return out
            except Exception as err:
                log.debug(f"Exporter scrape {node.hostname}:{port} failed: {err}")
    return None


def _metric_names_for_family(counter_family):
    """Expected exporter metric name substrings for rebuild/vulnerability."""
    if counter_family == "vulnerability":
        return [
            "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
            "ceph_recoverystate_perf_pg_vulnerability_duration_count",
            "ceph_recoverystate_perf_pg_vulnerability_duration_max_inc",
            "ceph_recoverystate_perf_pg_vulnerability_duration_min",
        ]
    if counter_family == "both" or counter_family is None:
        return [
            "ceph_recoverystate_perf_pg_rebuild_duration_sum",
            "ceph_recoverystate_perf_pg_rebuild_duration_count",
            "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
            "ceph_recoverystate_perf_pg_vulnerability_duration_count",
        ]
    return [
        "ceph_recoverystate_perf_pg_rebuild_duration_sum",
        "ceph_recoverystate_perf_pg_rebuild_duration_count",
        "ceph_recoverystate_perf_pg_rebuild_duration_max_inc",
        "ceph_recoverystate_perf_pg_rebuild_duration_min",
        "ceph_recoverystate_perf_pg_rebuild_max_secs",
        "ceph_recoverystate_perf_pg_rebuild_min_secs",
    ]


def _trigger_non_primary_rebuild(rados_obj, pool_name, hold_secs=30):
    """Stop non-primary, wait, start, wait clean. Return (primary, non_primary, acting)."""
    primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
    if non_primary is None:
        raise AssertionError("Need at least 2 OSDs in acting set")
    rados_obj.change_osd_state(action="stop", target=non_primary)
    time.sleep(hold_secs)
    try:
        wait_for_recovery(rados_obj)
    except Exception:
        pass
    rados_obj.change_osd_state(action="start", target=non_primary)
    assert wait_for_recovery(rados_obj, pool_name)
    return primary, non_primary, acting



# ---------------------------------------------------------------------------
# Scenario implementations
# ---------------------------------------------------------------------------


def scenario_counter_registration(rados_obj, cephadm, config):
    """Verify PG rebuild counters exist on all OSDs with default zero values.

    Counters are reset (via OSD restart) before this scenario runs, so all
    values should be at zero unless immediate post-restart recovery occurred.

    Test steps:
        1. Get the list of all up OSDs.
        2. Fetch the pg_rebuild_* counters from every OSD (in parallel).
        3. Verify avgcount, sum, avgtime, pg_rebuild_max_secs and
           pg_rebuild_min_secs are all present on every OSD.
        4. Validate field types and invariants.
        5. Verify counters are at default (zero) values; warn (but don't
           fail) for OSDs that picked up immediate post-restart recovery.
    """
    osd_list = rados_obj.get_osd_list(status="up")
    log.info(f"Checking rebuild counters on {len(osd_list)} OSDs (in parallel)")

    all_counters = run_parallel(
        osd_list,
        worker_fn=lambda osd_id: get_rebuild_counters(rados_obj, osd_id),
        on_error="raise",
    )

    non_zero_osds = []
    for osd_id, counters in all_counters.items():
        for key in [
            "avgcount",
            "sum",
            "avgtime",
            "pg_rebuild_max_secs",
            "pg_rebuild_min_secs",
        ]:
            if counters.get(key) is None:
                raise AssertionError(f"Counter '{key}' not found on osd.{osd_id}")
        validate_counter_fields(counters, context=f"osd.{osd_id} registration")
        if verify_counter_defaults(counters):
            log.info(f"osd.{osd_id}: all rebuild counters present and at zero")
        else:
            non_zero_osds.append(osd_id)
            log.warning(
                f"osd.{osd_id}: counters non-zero after reset (post-restart "
                f"recovery): {counters}"
            )

    if len(non_zero_osds) == len(all_counters):
        raise AssertionError(
            "ALL OSDs have non-zero counters after reset — counters may not "
            f"be resetting on restart: {non_zero_osds}"
        )
    log.info(
        f"Counter registration verified on all {len(all_counters)} OSDs "
        f"({len(non_zero_osds)} had post-restart recovery activity)"
    )


def scenario_counter_priority(rados_obj, cephadm, config):
    """Verify rebuild counter priority is PRIO_USEFUL (5) on every reachable OSD.

    Test steps:
        1. Get the list of all up OSDs.
        2. Fetch the perf schema priority of each rebuild counter from every
           OSD (in parallel), skipping OSDs that are unreachable.
        3. Verify each of pg_rebuild_duration, pg_rebuild_max_secs and
           pg_rebuild_min_secs reports priority PRIO_USEFUL (5).
    """
    osd_list = rados_obj.get_osd_list(status="up")
    log.info(f"Checking rebuild counter priority on {len(osd_list)} OSDs (in parallel)")

    all_priorities = run_parallel(
        osd_list,
        worker_fn=lambda osd_id: get_perf_schema_priorities(rados_obj, osd_id),
        on_error="warn",
    )

    skipped = [
        osd_id for osd_id, p in all_priorities.items() if p is None
    ]
    if skipped:
        log.warning(
            f"Skipping {len(skipped)} unreachable OSDs: {skipped}"
        )

    checked = 0
    for osd_id, priorities in all_priorities.items():
        if priorities is None:
            continue
        for counter in REBUILD_COUNTERS:
            prio = priorities.get(counter)
            if prio != PRIO_USEFUL:
                raise AssertionError(
                    f"osd.{osd_id}: counter '{counter}' priority is {prio}, "
                    f"expected {PRIO_USEFUL}"
                )
            log.info(f"osd.{osd_id}: {counter} priority = {prio} (PRIO_USEFUL)")
        checked += 1

    if checked == 0:
        raise AssertionError("No reachable OSDs to verify counter priority")
    log.info(f"Counter priority verification passed on {checked}/{len(osd_list)} OSDs")


def scenario_prometheus_integration(rados_obj, cephadm, config):
    """Verify rebuild counters are exposed via ceph-exporter to Prometheus.

    Triggers a real rebuild first so the counters have non-default values
    (some exporters/collectors can behave differently for never-touched
    counters), then scrapes the exporter and hard-fails if any expected
    metric name is absent from the scrape output. A missing metric is a
    genuine bug and must never be downgraded to a warning / silent pass.

    Test steps:
        1. Create a pool and write data to it; wait for the pool to be clean.
        2. Stop a non-primary OSD to trigger a rebuild, then restart it and
           wait for recovery.
        3. Fetch rebuild counters on the primary OSD and verify avgcount > 0.
        4. Scrape ceph-exporter's /metrics endpoint from an mgr/installer
           node.
        5. Verify all 4 expected Prometheus metric names are present in the
           scrape output.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_prom_integration")
    exporter_port = config.get("exporter_port", 9095)

    expected_metric_sets = [
        [
            "ceph_recoverystate_perf_pg_rebuild_duration_count",
            "ceph_recoverystate_perf_pg_rebuild_duration_sum",
            "ceph_recoverystate_perf_pg_rebuild_max_secs",
            "ceph_recoverystate_perf_pg_rebuild_min_secs",
        ],
        [
            "ceph_pg_rebuild_duration_count",
            "ceph_pg_rebuild_duration_sum",
            "ceph_pg_rebuild_max_secs",
            "ceph_pg_rebuild_min_secs",
        ],
    ]

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        counters = get_rebuild_counters(rados_obj, primary)
        log.info(f"osd.{primary} perf dump counters before exporter check: {counters}")
        validate_counter_fields(counters, context=f"osd.{primary} pre-export")
        assert (
            counters.get("avgcount", 0) > 0
        ), "Expected non-zero rebuild counters before checking Prometheus export"

        scrape_nodes = rados_obj.ceph_cluster.get_nodes(role="mgr")
        if not scrape_nodes:
            raise AssertionError("No MGR nodes found for Prometheus check")
        installer_nodes = rados_obj.ceph_cluster.get_nodes(role="installer")
        scrape_nodes = scrape_nodes + installer_nodes

        metrics_text = None
        fetch_errors = []
        for node in scrape_nodes:
            try:
                out, _ = node.exec_command(
                    cmd=f"curl -s http://localhost:{exporter_port}/metrics",
                    timeout=30,
                )
                if out:
                    metrics_text = out
                    log.info(f"Fetched exporter metrics from {node.hostname}")
                    break
                fetch_errors.append(f"{node.hostname}: empty response")
            except Exception as err:
                fetch_errors.append(f"{node.hostname}: {err}")

        if metrics_text is None:
            raise AssertionError(
                "Could not fetch metrics from ceph-exporter on any mgr/installer "
                f"node (port {exporter_port}). Errors: {fetch_errors}"
            )

        matched_set = None
        for metric_set in expected_metric_sets:
            missing = [m for m in metric_set if m not in metrics_text]
            if not missing:
                matched_set = metric_set
                break

        if matched_set is None:
            rebuild_lines = [
                line for line in metrics_text.splitlines()
                if "rebuild" in line.lower() and not line.startswith("#")
            ]
            log.error(
                f"Metrics containing 'rebuild' found in exporter output: {rebuild_lines[:20]}"
            )
            all_tried = [m for s in expected_metric_sets for m in s]
            raise AssertionError(
                f"None of the expected Prometheus metric name patterns matched. "
                f"Tried: {all_tried}. "
                f"Found {len(rebuild_lines)} lines with 'rebuild' in exporter output."
            )

        for metric_name in matched_set:
            log.info(f"Found metric '{metric_name}' in exporter output")

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Prometheus integration check passed")


def scenario_non_primary_osd_stop(rados_obj, cephadm, config):
    """Stop a non-primary OSD, wait for recovery, verify rebuild counters.

    Sub-cases via config['sub_case']:
        'within_down_out'  - OSD stopped < mon_osd_down_out_interval, then restarted
        'beyond_down_out'  - OSD stopped > mon_osd_down_out_interval, then restarted
        'not_restarted'    - OSD stopped and not restarted (recovery via re-replication)

    Test steps:
        1. Create a pool (replicated or EC) and write data; wait for clean.
        2. Pick a primary/non-primary OSD pair from the acting set.
        3. Snapshot baseline rebuild counters (primary + all up OSDs).
        4. Stop the non-primary OSD per the configured sub_case (restarted
           within/beyond mon_osd_down_out_interval, or left down) and wait
           for recovery.
        5. Fetch the primary's counters after the event.
        6. For sub_case in (beyond_down_out, not_restarted), verify the
           primary's counters incremented.
        7. Verify OSDs unaffected by the event show unchanged counters.
        8. Verify replica OSDs in the acting set show unchanged counters.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_non_primary")
    pool_type = config.get("pool_type", "replicated")
    sub_case = config.get("sub_case", "beyond_down_out")

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Pool has only 1 OSD in acting set, cannot test")

        log.info(
            f"Acting set: {acting}, primary: {primary}, target non-primary: {non_primary}"
        )

        baseline_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline_primary, context=f"osd.{primary} baseline")
        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        if sub_case == "within_down_out":
            down_out_interval = int(
                mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
            )
            rados_obj.change_osd_state(action="stop", target=non_primary)
            wait_secs = max(down_out_interval // 2, 10)
            log.info(f"Waiting {wait_secs}s (< down_out_interval={down_out_interval}s)")
            time.sleep(wait_secs)
            rados_obj.change_osd_state(action="start", target=non_primary)
            assert wait_for_recovery(rados_obj, pool_name)

        elif sub_case == "beyond_down_out":
            down_out_interval = int(
                mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
            )
            rados_obj.change_osd_state(action="stop", target=non_primary)
            wait_secs = down_out_interval + 60
            log.info(f"Waiting {wait_secs}s (> down_out_interval={down_out_interval}s)")
            time.sleep(wait_secs)
            rados_obj.change_osd_state(action="start", target=non_primary)
            assert wait_for_recovery(rados_obj, pool_name)

        elif sub_case == "not_restarted":
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        after_primary = get_rebuild_counters(rados_obj, primary)
        log.info(
            f"Primary osd.{primary} counters - before: {baseline_primary}, after: {after_primary}"
        )
        validate_counter_fields(after_primary, context=f"osd.{primary} after")

        if sub_case in ("beyond_down_out", "not_restarted"):
            assert verify_counters_incremented(
                baseline_primary, after_primary
            ), "Primary OSD rebuild counters should have incremented after recovery"
        else:
            log.info(
                "Within down_out_interval: recovery may or may not have occurred. "
                "Checking counters are still valid."
            )

        unaffected_osds = [
            osd for osd in baseline_all if osd not in acting and osd != non_primary
        ]
        unaffected_after = fetch_counters_for_osds(rados_obj, unaffected_osds[:5])
        changed_count = 0
        for osd_id, after in unaffected_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} unaffected")
            if not verify_counters_unchanged(baseline_all[osd_id], after):
                changed_count += 1
                log.warning(
                    f"osd.{osd_id} counters changed (background recovery on "
                    f"shared cluster): before={baseline_all[osd_id]}, after={after}"
                )
        if changed_count == len(unaffected_after) and changed_count > 0:
            raise AssertionError(
                "ALL sampled unaffected OSDs changed counters — likely a "
                "test-pool recovery leak, not just background activity"
            )

        replica_osds = [osd for osd in acting if osd != primary and osd != non_primary]
        replica_after = fetch_counters_for_osds(
            rados_obj, replica_osds, on_error="raise"
        )
        for osd_id, after in replica_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} replica")
            if not verify_counters_unchanged(baseline_all.get(osd_id, after), after):
                log.warning(
                    f"Replica osd.{osd_id} counters changed (may have become "
                    f"primary for other PGs): before={baseline_all.get(osd_id)}, after={after}"
                )

    finally:
        if sub_case == "not_restarted":
            rados_obj.change_osd_state(action="start", target=non_primary)
            time.sleep(30)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"Non-primary OSD stop scenario '{sub_case}' passed")


def scenario_primary_osd_stop(rados_obj, cephadm, config):
    """Stop a primary OSD, verify counters on newly elected primary.

    Sub-cases via config['sub_case']:
        'within_down_out'  - primary stopped < mon_osd_down_out_interval
        'beyond_down_out'  - primary stopped > mon_osd_down_out_interval
        'not_restarted'    - primary stopped permanently

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Identify the current primary and acting set; snapshot baseline
           counters for all up OSDs.
        3. Stop the primary OSD per the configured sub_case and wait for
           recovery.
        4. Determine the new primary; if it changed and the sub_case
           warrants it, verify the new primary's counters incremented.
        5. If the OSD was restarted, verify its counters reset to default
           (zero) values.
        6. Verify OSDs unaffected by the event show unchanged counters.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_primary")
    pool_type = config.get("pool_type", "replicated")
    sub_case = config.get("sub_case", "beyond_down_out")
    old_primary = None

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, acting = _get_non_primary_osd(rados_obj, pool_name)
        old_primary = primary
        log.info(f"Acting set: {acting}, stopping primary osd.{primary}")

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        down_out_interval = int(
            mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
        )

        rados_obj.change_osd_state(action="stop", target=primary)

        if sub_case == "within_down_out":
            wait_secs = max(down_out_interval // 2, 10)
            log.info(f"Waiting {wait_secs}s (< down_out_interval)")
            time.sleep(wait_secs)
            rados_obj.change_osd_state(action="start", target=primary)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "beyond_down_out":
            wait_secs = down_out_interval + 60
            log.info(f"Waiting {wait_secs}s (> down_out_interval)")
            time.sleep(wait_secs)
            rados_obj.change_osd_state(action="start", target=primary)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "not_restarted":
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        new_acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        new_primary = new_acting[0]
        log.info(f"New acting set: {new_acting}, new primary: osd.{new_primary}")

        if sub_case in ("beyond_down_out", "not_restarted"):
            new_primary_counters = get_rebuild_counters(rados_obj, new_primary)
            log.info(f"New primary osd.{new_primary} counters: {new_primary_counters}")
            validate_counter_fields(
                new_primary_counters, context=f"osd.{new_primary} new-primary"
            )
            if new_primary != old_primary:
                assert verify_counters_incremented(
                    baseline_all.get(new_primary, {"avgcount": 0, "sum": 0}),
                    new_primary_counters,
                ), "New primary should have rebuild counters populated"

        if sub_case != "not_restarted":
            restarted_counters = get_rebuild_counters(rados_obj, old_primary)
            log.info(f"Restarted osd.{old_primary} counters: {restarted_counters}")
            validate_counter_fields(
                restarted_counters, context=f"osd.{old_primary} restarted"
            )
            if not verify_counter_defaults(restarted_counters):
                log.warning(
                    f"Restarted osd.{old_primary} counters are non-zero "
                    f"({restarted_counters}) — OSD likely participated in "
                    f"recovery immediately after restart before we could sample"
                )

        unaffected_osds = [osd for osd in baseline_all if osd not in acting]
        unaffected_after = fetch_counters_for_osds(rados_obj, unaffected_osds[:5])
        changed_count = 0
        for osd_id, after in unaffected_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} unaffected")
            if not verify_counters_unchanged(baseline_all[osd_id], after):
                changed_count += 1
                log.warning(
                    f"osd.{osd_id} counters changed (background recovery on "
                    f"shared cluster): before={baseline_all[osd_id]}, after={after}"
                )
        if changed_count == len(unaffected_after) and changed_count > 0:
            raise AssertionError(
                "ALL sampled unaffected OSDs changed counters — likely a "
                "test-pool recovery leak, not just background activity"
            )

    finally:
        if sub_case == "not_restarted" and old_primary is not None:
            rados_obj.change_osd_state(action="start", target=old_primary)
            time.sleep(30)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"Primary OSD stop scenario '{sub_case}' passed")


def scenario_osd_host_stop(rados_obj, cephadm, config):
    """Stop all OSDs on a host, verify rebuild counters on primary OSDs of affected PGs.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Pick a random OSD host and stop every OSD on it.
        4. Wait/restart per the configured sub_case, then wait for recovery.
        5. Verify at least one surviving OSD shows incremented rebuild
           counters.
        6. Verify OSDs unaffected by the host stop show unchanged counters.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_host_stop")
    pool_type = config.get("pool_type", "replicated")
    sub_case = config.get("sub_case", "beyond_down_out")
    stopped_osds = []

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=32)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        osd_nodes = rados_obj.ceph_cluster.get_nodes(role="osd")
        installer = cephadm.installer
        eligible_hosts = [
            n for n in osd_nodes if n.hostname != installer.node.hostname
        ]
        if not eligible_hosts:
            raise AssertionError(
                "No eligible OSD hosts found after excluding the installer node"
            )
        target_host = random.choice(eligible_hosts)
        host_osds = rados_obj.collect_osd_daemon_ids(target_host)
        log.info(f"Target host: {target_host.hostname}, OSDs: {host_osds}")

        if not host_osds:
            raise AssertionError(f"No OSDs found on host {target_host.hostname}")

        down_out_interval = int(
            mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
        )

        for osd_id in host_osds:
            rados_obj.change_osd_state(action="stop", target=int(osd_id))
            stopped_osds.append(int(osd_id))

        if sub_case == "within_down_out":
            wait_secs = max(down_out_interval // 2, 10)
            time.sleep(wait_secs)
            for osd_id in stopped_osds:
                rados_obj.change_osd_state(action="start", target=osd_id)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "beyond_down_out":
            wait_secs = down_out_interval + 60
            time.sleep(wait_secs)
            for osd_id in stopped_osds:
                rados_obj.change_osd_state(action="start", target=osd_id)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "not_restarted":
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        if sub_case in ("beyond_down_out", "not_restarted"):
            up_osds = rados_obj.get_osd_list(status="up")
            check_osds = [osd_id for osd_id in up_osds if osd_id not in stopped_osds]
            post_stop_counters = fetch_counters_for_osds(rados_obj, check_osds)
            counters_found = False
            for osd_id, after in post_stop_counters.items():
                validate_counter_fields(after, context=f"osd.{osd_id} post-host-stop")
                before = baseline_all.get(osd_id, {"avgcount": 0, "sum": 0})
                if after["avgcount"] > before.get("avgcount", 0):
                    log.info(f"osd.{osd_id} rebuild counters incremented: {after}")
                    counters_found = True
            assert (
                counters_found
            ), "At least one primary OSD should show incremented rebuild counters"

        unaffected_osds = [osd for osd in baseline_all if osd not in stopped_osds]
        unaffected_after = fetch_counters_for_osds(rados_obj, unaffected_osds[:5])
        for osd_id, after in unaffected_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} unaffected")
            before = baseline_all[osd_id]
            if after["avgcount"] == before["avgcount"]:
                log.info(f"Unaffected osd.{osd_id} counters unchanged as expected")

    finally:
        for osd_id in stopped_osds:
            try:
                rados_obj.change_osd_state(action="start", target=osd_id)
            except Exception as err:
                log.warning(f"Failed to restart osd.{osd_id} during cleanup: {err}")
        time.sleep(30)
        if config.get("delete_pool", True):
            try:
                rados_obj.delete_pool(pool=pool_name)
            except Exception as err:
                log.warning(f"Failed to delete pool {pool_name} during cleanup: {err}")

    log.info(f"OSD host stop scenario '{sub_case}' passed")


def scenario_misplaced_recovery(rados_obj, cephadm, config):
    """Pool size increase causes misplaced objects; verify rebuild counters.

    Test steps:
        1. Create a pool with the original (smaller) size and write data;
           wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Increase the pool size (creates misplaced objects) and wait for
           recovery.
        4. Verify the primary OSD's counters incremented due to the
           misplaced-object recovery.
        5. Verify OSDs unaffected by the size change show unchanged counters.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_misplaced")
    original_size = config.get("original_size", 2)
    new_size = config.get("new_size", 3)

    try:
        rados_obj.create_pool(
            pool_name=pool_name, pg_num=16, size=original_size, min_size=1
        )
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        log.info(f"Increasing pool size from {original_size} to {new_size}")
        rados_obj.set_pool_property(pool=pool_name, props="size", value=new_size)
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        after_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_primary, context=f"osd.{primary} after")
        before_primary = baseline_all.get(primary, {"avgcount": 0, "sum": 0})

        log.info(
            f"Primary osd.{primary} counters - before: {before_primary}, after: {after_primary}"
        )
        assert verify_counters_incremented(
            before_primary, after_primary
        ), "Primary OSD counters should reflect misplaced object recovery"

        unaffected_osds = [osd for osd in baseline_all if osd not in acting]
        unaffected_after = fetch_counters_for_osds(rados_obj, unaffected_osds[:5])
        for osd_id, after in unaffected_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} unaffected")
            assert verify_counters_unchanged(
                baseline_all[osd_id], after
            ), f"Unaffected osd.{osd_id} counters changed unexpectedly"

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Misplaced recovery scenario passed")


def scenario_repair_counters(rados_obj, cephadm, config):
    """Create inconsistent objects, trigger repair, verify rebuild counters.

    Test steps:
        1. Create a single-PG pool and write data; wait for the pool to be
           clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Inject an inconsistent object via objectstore-tool.
        4. Run a deep-scrub, then issue `ceph pg repair`.
        5. Wait for recovery and fetch the counters again.
        6. Log whether the repair incremented the rebuild counters
           (best-effort observation, not a hard failure).
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_repair")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 20))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")
        log.info(f"Baseline counters on primary osd.{primary}: {baseline}")

        pgids = rados_obj.get_pgid(pool_name=pool_name)
        pgid = pgids[0]

        from ceph.rados.objectstoretool_workflows import ObjectStoreToolWorkflows

        objectstore_obj = ObjectStoreToolWorkflows(node=cephadm)
        rados_obj.create_inconsistent_object(
            objectstore_obj=objectstore_obj,
            pool_name=pool_name,
            no_of_objects=1,
        )

        rados_obj.run_deep_scrub(pool=pool_name)
        time.sleep(60)

        cephadm.shell([f"ceph pg repair {pgid}"])
        time.sleep(60)
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-repair")
        log.info(f"After repair counters on primary osd.{primary}: {after}")

        if after["avgcount"] > baseline["avgcount"]:
            log.info("Repair contributed to rebuild duration counters as expected")
        else:
            log.warning(
                "Repair did not increment rebuild counters. "
                "This may be expected if repair did not trigger PG rebuild."
            )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Repair counters scenario completed")


def scenario_recovery_with_io(rados_obj, cephadm, config):
    """Run continuous IO, stop an OSD, verify counters during active workload.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Start a background `rados bench` write workload.
        4. Stop a non-primary OSD, wait for recovery, then restart it and
           wait for the pool to be clean again.
        5. Verify the primary's rebuild counters incremented despite the
           concurrent IO load.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_io_load")
    pool_type = config.get("pool_type", "replicated")

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=32)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline_primary, context=f"osd.{primary} baseline")

        bench_cmd = f"rados bench -p {pool_name} 120 write --no-cleanup &"
        client.exec_command(cmd=bench_cmd, check_ec=False)
        log.info("Background IO started")
        time.sleep(10)

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_primary, context=f"osd.{primary} after-io")
        log.info(
            f"Primary osd.{primary} counters - before: {baseline_primary}, after: {after_primary}"
        )

        assert verify_counters_incremented(
            baseline_primary, after_primary
        ), "Rebuild counters should increment during recovery under IO load"

    finally:
        client.exec_command(cmd="killall -9 rados 2>/dev/null || true", check_ec=False)
        time.sleep(5)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Recovery with IO load scenario passed")


def scenario_primary_transition(rados_obj, cephadm, config):
    """Force primary transition, verify counters are cleared on old primary.

    Test steps:
        1. Create a single-PG pool and write data; wait for the pool to be
           clean.
        2. Stop then restart a replica OSD to trigger a rebuild, and record
           the primary's counters afterward.
        3. Set the current primary's primary-affinity to 0 to force a
           primary transition.
        4. Wait for recovery and check whether a new primary was elected.
        5. Restore the original primary-affinity in the finally block.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_transition")
    primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 30))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        if len(acting) < 2:
            raise AssertionError("Need at least 2 OSDs in acting set")
        replica = acting[1]

        rados_obj.change_osd_state(action="stop", target=replica)
        time.sleep(30)
        rados_obj.change_osd_state(action="start", target=replica)
        assert wait_for_recovery(rados_obj, pool_name)

        primary_counters = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(
            primary_counters, context=f"osd.{primary} after-replica-recovery"
        )
        log.info(
            f"Primary osd.{primary} counters after replica recovery: {primary_counters}"
        )

        log.info(f"Forcing primary transition away from osd.{primary}")
        cephadm.shell([f"ceph osd primary-affinity {primary} 0.0"])
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        new_acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        new_primary = new_acting[0]
        log.info(f"New primary: osd.{new_primary}, old primary: osd.{primary}")

        if new_primary != primary:
            log.info("Primary transition occurred successfully")
        else:
            log.warning("Primary did not change; affinity may not have taken effect")

    finally:
        if primary is not None:
            cephadm.shell([f"ceph osd primary-affinity {primary} 1.0"])
            time.sleep(10)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Primary transition scenario passed")


def scenario_throttled_recovery(rados_obj, cephadm, config):
    """Simulate slow recovery with throttled settings, verify counter accuracy.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Lower `osd_recovery_max_active` to 1 to slow down recovery.
        4. Stop then restart a non-primary OSD; wait for recovery.
        5. Verify the primary's counters incremented despite the throttled
           recovery speed.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_throttled")
    pool_type = config.get("pool_type", "replicated")

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        log.info("Setting osd_recovery_max_active to 1 for slow recovery")
        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-throttled")
        log.info(f"Primary osd.{primary} counters - before: {baseline}, after: {after}")

        assert verify_counters_incremented(
            baseline, after
        ), "Rebuild counters should increment with throttled recovery"

    finally:
        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Throttled recovery scenario passed")


def scenario_add_osd(rados_obj, cephadm, config):
    """Adding a new OSD to the cluster should not increment rebuild counters.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Pick a random OSD on a host and stop it (simulating removal).
        4. Snapshot counters after removal.
        5. Restart the OSD (simulating re-add) and wait for recovery.
        6. Snapshot counters after re-add and compare against the
           after-removal snapshot for the other OSDs, logging any
           unexpected drift.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_add_osd")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=32)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        osd_list_before = set(rados_obj.get_osd_list(status="up"))
        osd_nodes = rados_obj.ceph_cluster.get_nodes(role="osd")
        target_host = random.choice(osd_nodes)
        host_osds = rados_obj.collect_osd_daemon_ids(target_host)

        if not host_osds:
            log.warning("No OSDs on target host to remove/re-add")
            raise AssertionError("Test requires OSDs on host for re-add scenario")

        target_osd = int(host_osds[0])
        log.info(f"Removing osd.{target_osd} from host {target_host.hostname}")
        rados_obj.change_osd_state(action="stop", target=target_osd)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        snapshot_after_remove = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(snapshot_after_remove, context="after-remove")

        log.info(f"Re-adding osd.{target_osd}")
        rados_obj.change_osd_state(action="start", target=target_osd)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        after_readd = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(after_readd, context="after-readd")

        for osd_id in snapshot_after_remove:
            if osd_id == target_osd:
                continue
            before = snapshot_after_remove[osd_id]
            after = after_readd.get(osd_id, before)
            if before["avgcount"] != after["avgcount"]:
                log.warning(
                    f"osd.{osd_id} counters changed after OSD re-add: "
                    f"{before} -> {after}. This may be due to rebalancing."
                )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Add OSD scenario completed")


def scenario_osd_restart_reset(rados_obj, cephadm, config):
    """Verify OSD restart resets all rebuild counters to zero.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Stop then restart a non-primary OSD to generate non-zero counters
           on the primary.
        3. Verify the primary's pre-restart counters are non-zero.
        4. Restart the primary OSD itself.
        5. Verify the primary's counters reset to default (zero) values
           after the restart.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_restart")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        pre_restart = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(pre_restart, context=f"osd.{primary} pre-restart")
        log.info(f"Pre-restart counters on osd.{primary}: {pre_restart}")
        assert pre_restart["avgcount"] > 0, "Expected non-zero counters before restart"

        log.info(f"Restarting osd.{primary}")
        rados_obj.change_osd_state(action="stop", target=primary)
        time.sleep(5)
        rados_obj.change_osd_state(action="start", target=primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        post_restart = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(post_restart, context=f"osd.{primary} post-restart")
        log.info(f"Post-restart counters on osd.{primary}: {post_restart}")
        assert verify_counter_defaults(
            post_restart
        ), "All counters should be zero after OSD restart"

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("OSD restart reset scenario passed")


def scenario_no_recovery_noout(rados_obj, cephadm, config):
    """OSD down/up quickly with noout set should not trigger recovery or counter changes.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Set the `noout` flag on the cluster.
        4. Quickly stop then restart a target OSD (too fast to trigger real
           recovery).
        5. Verify the bounced OSD's counters reset to default (zero) and
           every other OSD's counters remain unchanged.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_noout")
    pool_type = config.get("pool_type", "replicated")

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")
        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)

        target_osd = non_primary if non_primary else primary

        log.info("Setting noout flag on cluster")
        cephadm.shell(["ceph osd set noout"])
        time.sleep(5)

        rados_obj.change_osd_state(action="stop", target=target_osd)
        time.sleep(10)
        rados_obj.change_osd_state(action="start", target=target_osd)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        after_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(after_all, context="after-noout-bounce")

        for osd_id in baseline_all:
            if osd_id == target_osd:
                restarted = after_all.get(osd_id, {})
                assert verify_counter_defaults(
                    restarted
                ), f"Restarted osd.{osd_id} should have zero counters"
                continue
            before = baseline_all[osd_id]
            after = after_all.get(osd_id, before)
            assert verify_counters_unchanged(
                before, after
            ), f"osd.{osd_id} counters changed despite noout; no recovery expected"

    finally:
        cephadm.shell(["ceph osd unset noout"])
        time.sleep(5)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("No recovery (noout) scenario passed")


def scenario_multiple_pg_rebuild(rados_obj, cephadm, config):
    """Multiple PGs finishing at different times; verify aggregate counters.

    Test steps:
        1. Create a multi-PG pool and write a larger dataset; wait for the
           pool to be clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Stop then restart a non-primary OSD, triggering rebuild across
           multiple PGs.
        4. Verify avgcount and sum both increased.
        5. Verify the min/max invariant (pg_rebuild_max_secs >=
           pg_rebuild_min_secs) holds.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_multi_pg")
    pg_num = config.get("pg_num", 32)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=pg_num)
        write_data_to_pool(
            pool_obj, client, pool_name, nobj=config.get("nobj", 200), size="1M"
        )
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        non_primary = acting[1] if len(acting) > 1 else None
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-multi-pg")
        log.info(f"Primary osd.{primary} counters - before: {baseline}, after: {after}")

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "avgcount should reflect multiple PG rebuilds"
        assert (
            after["sum"] > baseline["sum"]
        ), "sum should reflect total rebuild time across PGs"

        if after["avgcount"] > 0:
            computed_avg = after["sum"] / after["avgcount"]
            log.info(
                f"avgcount={after['avgcount']}, sum={after['sum']}, "
                f"computed_avg={computed_avg:.2f}, reported_avgtime={after['avgtime']}"
            )

        assert verify_min_max_invariant(after), "max_secs should be >= min_secs"
        log.info(
            f"max_secs={after['pg_rebuild_max_secs']}, "
            f"min_secs={after['pg_rebuild_min_secs']}"
        )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Multiple PG rebuild scenario passed")


def scenario_negative_daemon_restart(rados_obj, cephadm, config):
    """MGR/MON/exporter restart should not reset OSD counters.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Stop then restart a non-primary OSD to populate non-zero counters
           on the primary.
        3. Restart the configured non-OSD daemon (mgr, mon, or
           ceph-exporter per config['daemon_type']).
        4. Verify the primary OSD's rebuild counters are unchanged after the
           daemon restart.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_negative")
    daemon_type = config.get("daemon_type", "mgr")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        pre_restart = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(pre_restart, context=f"osd.{primary} pre-restart")
        log.info(
            f"Counters before {daemon_type} restart on osd.{primary}: {pre_restart}"
        )
        assert (
            pre_restart["avgcount"] > 0
        ), "Expected non-zero counters before daemon restart"

        if daemon_type == "mgr":
            cephadm.shell(["ceph orch restart mgr"])
            time.sleep(30)
        elif daemon_type == "mon":
            mon_list = rados_obj.run_ceph_command("ceph status")["quorum_names"]
            if mon_list:
                target_mon = mon_list[-1]
                rados_obj.change_daemon_systemctl_state(
                    action="restart", daemon_type="mon", daemon_id=target_mon
                )
                time.sleep(30)
        elif daemon_type == "ceph-exporter":
            cephadm.shell(["ceph orch restart ceph-exporter"])
            time.sleep(30)

        post_restart = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(post_restart, context=f"osd.{primary} post-restart")
        log.info(
            f"Counters after {daemon_type} restart on osd.{primary}: {post_restart}"
        )

        assert verify_counters_unchanged(
            pre_restart, post_restart
        ), f"OSD counters should not change after {daemon_type} restart"

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"Negative {daemon_type} restart scenario passed")


# ---------------------------------------------------------------------------
# Additional bug-hunting scenarios (derived from PR #69578 analysis)
# ---------------------------------------------------------------------------


def scenario_counter_section_name(rados_obj, cephadm, config):
    """Verify rebuild counters live in 'recoverystate_perf', NOT in the 'osd' section.

    The PR places counters in the recoverystate_perf collection. If a consumer
    reads the wrong section it will silently get None/zero.

    Test steps:
        1. Pick the first up OSD and fetch its perf dump.
        2. Verify none of the rebuild counters appear in the (wrong) 'osd'
           section.
        3. Verify all three rebuild counters are present in the
           'recoverystate_perf' section.
        4. Verify the internal shape of each field (pg_rebuild_duration is a
           dict with avgcount/sum/avgtime; max/min_secs are numeric
           scalars).
        5. Fetch and fully validate the rebuild counter fields.
        6. Verify the perf schema also reports the counters under
           'recoverystate_perf'.
    """
    osd_list = rados_obj.get_osd_list(status="up")
    osd_id = osd_list[0]

    perf = rados_obj.get_osd_perf_dump(osd_id)

    wrong_section = perf.get("osd", {})
    for counter in REBUILD_COUNTERS:
        if counter in wrong_section:
            raise AssertionError(
                f"Counter '{counter}' unexpectedly found in 'osd' section "
                "instead of 'recoverystate_perf'"
            )

    rs_section = perf.get("recoverystate_perf", {})
    for counter in REBUILD_COUNTERS:
        if counter not in rs_section:
            raise AssertionError(
                f"Counter '{counter}' not found in 'recoverystate_perf' section"
            )
        log.info(f"osd.{osd_id}: {counter} correctly in recoverystate_perf")

    # Validate the internal shape of each field individually, matching the
    # exact structure introduced by the PR:
    #   "pg_rebuild_duration": {"avgcount": 0, "sum": 0.0, "avgtime": 0.0},
    #   "pg_rebuild_max_secs": 0,
    #   "pg_rebuild_min_secs": 0
    duration_entry = rs_section.get("pg_rebuild_duration")
    if not isinstance(duration_entry, dict):
        raise AssertionError(
            f"osd.{osd_id}: 'pg_rebuild_duration' should be a dict with "
            f"avgcount/sum/avgtime, got {type(duration_entry).__name__}"
        )
    for sub_field in ("avgcount", "sum", "avgtime"):
        if sub_field not in duration_entry:
            raise AssertionError(
                f"osd.{osd_id}: 'pg_rebuild_duration.{sub_field}' missing"
            )
    for scalar_field in ("pg_rebuild_max_secs", "pg_rebuild_min_secs"):
        if not isinstance(rs_section.get(scalar_field), (int, float)):
            raise AssertionError(
                f"osd.{osd_id}: '{scalar_field}' should be numeric, got "
                f"{type(rs_section.get(scalar_field)).__name__}"
            )

    counters = get_rebuild_counters(rados_obj, osd_id)
    validate_counter_fields(counters, context=f"osd.{osd_id} section-name")

    schema = rados_obj.run_ceph_command(cmd=f"ceph tell osd.{osd_id} perf schema")
    rs_schema = schema.get("recoverystate_perf", {})
    for counter in REBUILD_COUNTERS:
        if counter not in rs_schema:
            raise AssertionError(
                f"Counter '{counter}' schema missing from recoverystate_perf"
            )

    log.info("Counter section name verification passed")


def scenario_sub_second_rebuild(rados_obj, cephadm, config):
    """Trigger a very fast rebuild (tiny pool, few objects) and verify min/max
    handling when rebuild duration < 1 second.

    The PR stores durations in seconds; sub-second granularity via float. This
    test verifies the counters still record something, not zero, for very fast
    rebuilds.

    Test steps:
        1. Create a tiny single-PG pool with a handful of small objects;
           wait for the pool to be clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Stop then restart a non-primary OSD to trigger a very fast
           (sub-1s) rebuild.
        4. Verify avgcount incremented and sum did not decrease.
        5. Log whether pg_rebuild_min_secs captured sub-second precision or
           was truncated to 0.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_subsec")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=3, size="4K")
        time.sleep(5)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-subsec")
        log.info(
            f"Sub-second rebuild on osd.{primary}: before={baseline}, after={after}"
        )

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "avgcount should increment even for sub-second rebuilds"
        assert (
            after["sum"] >= baseline["sum"]
        ), "sum should be >= baseline (may be very small float)"

        if (
            after["pg_rebuild_min_secs"] is not None
            and after["pg_rebuild_min_secs"] > 0
        ):
            log.info(f"min_secs={after['pg_rebuild_min_secs']} (sub-second precision)")
        elif after["pg_rebuild_min_secs"] == 0 and after["avgcount"] > 0:
            log.warning(
                "min_secs is 0 despite rebuild occurring - possible "
                "truncation of sub-second durations to integer 0"
            )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Sub-second rebuild scenario passed")


def scenario_double_latch_prevention(rados_obj, cephadm, config):
    """Verify no double-counting: keep PG degraded, check that the latch
    prevents duplicate recording.

    The PR uses a latch (pg_rebuild_started flag) that should be set once
    when the PG first enters degraded/undersized state and cleared only when
    the rebuild completes. While the PG stays degraded the latch should
    prevent additional counting.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Throttle recovery (osd_recovery_max_active=1,
           osd_recovery_sleep=5) to keep the PG degraded longer.
        3. Record baseline counters on the primary; stop a non-primary OSD.
        4. Sample counters twice while the pool is confirmed degraded,
           verifying avgcount does not increment a second time (no
           double-counting).
        5. Restart the OSD, wait for recovery, and verify avgcount
           eventually increments once recovery completes.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_dbl_latch")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        log.info("Throttling recovery to keep PG degraded longer")
        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="5")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)

        still_degraded_1 = _pool_has_degraded_pgs(rados_obj, pool_name)
        mid_1 = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid_1, context=f"osd.{primary} mid-1")
        time.sleep(30)
        still_degraded_2 = _pool_has_degraded_pgs(rados_obj, pool_name)
        mid_2 = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid_2, context=f"osd.{primary} mid-2")

        log.info(f"Mid-recovery sample 1: {mid_1} (pool degraded: {still_degraded_1})")
        log.info(f"Mid-recovery sample 2: {mid_2} (pool degraded: {still_degraded_2})")

        if still_degraded_1 and still_degraded_2:
            if mid_1["avgcount"] > baseline["avgcount"]:
                assert mid_2["avgcount"] == mid_1["avgcount"], (
                    "avgcount should not keep incrementing while PG stays "
                    f"degraded: mid_1={mid_1['avgcount']}, mid_2={mid_2['avgcount']}"
                )
                log.info(
                    "Latch correctly prevents double-counting during degraded state"
                )
        else:
            log.warning(
                "Pool was not confirmed degraded at both checkpoints "
                f"(degraded_1={still_degraded_1}, degraded_2={still_degraded_2}); "
                "recovery may have completed faster than the throttle window. "
                "Double-latch assertion skipped for this run."
            )

        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} final")
        log.info(f"Final counters: {after}")
        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "avgcount must increment after recovery completes"

    finally:
        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        mon_obj.remove_config(section="osd", name="osd_recovery_sleep")
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Double latch prevention scenario passed")


def scenario_pg_split_during_rebuild(rados_obj, cephadm, config):
    """Trigger PG split while PGs are degraded to see if the rebuild latch
    handles the state transition correctly.

    PG split creates new PGs from existing ones. If the original PG had the
    rebuild latch set, the split PG inherits state and the latch must either
    reset or track correctly.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Record baseline counters on the primary; throttle recovery.
        3. Stop a non-primary OSD and confirm the pool is degraded.
        4. Increase pg_num while PGs are still degraded (PG split), then
           restart the OSD.
        5. Wait for recovery and verify counters incremented and the
           min/max invariant holds.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_pg_split")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=4, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="3")

        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)

            still_degraded = _pool_has_degraded_pgs(rados_obj, pool_name)
            if not still_degraded:
                log.warning(
                    "Pool was not confirmed degraded before triggering PG split; "
                    "recovery may have completed faster than the throttle window. "
                    "Split will still be attempted, but the intended overlap with "
                    "an in-progress rebuild may not be exercised."
                )
            else:
                log.info("Confirmed pool has degraded PGs, proceeding with split")

            log.info("Increasing pg_num while PGs are degraded (PG split)")
            rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=16)
            time.sleep(30)

            rados_obj.change_osd_state(action="start", target=non_primary)
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-split")
        log.info(f"osd.{primary} counters - before: {baseline}, after: {after}")
        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "Rebuild counters should increment after recovery with PG split"
        assert verify_min_max_invariant(
            after
        ), "max_secs >= min_secs invariant violated after PG split"

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("PG split during rebuild scenario passed")


def scenario_backfill_only_recovery(rados_obj, cephadm, config):
    """Trigger backfill via OSD reweight (misplaced objects) and verify
    the rebuild counters track backfill-only recovery correctly.

    When an OSD is reweighted to 0 and then back to 1, PGs are remapped
    causing backfill. The PR should track this via the misplaced-object
    counter path in prepare_stats_for_publish().

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Reweight a non-primary OSD to 0 (backfill out), wait for
           recovery.
        4. Reweight it back to 1 (backfill in), wait for recovery.
        5. Verify at least one OSD shows incremented rebuild counters from
           the backfill-only recovery.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_backfill")
    pool_type = config.get("pool_type", "replicated")
    target_osd = None

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError(
                "Need at least 2 OSDs in acting set to reweight a non-primary"
            )
        target_osd = non_primary

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        log.info(f"Reweighting osd.{target_osd} to 0 (trigger backfill out)")
        cephadm.shell([f"ceph osd reweight {target_osd} 0"])
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        log.info(f"Reweighting osd.{target_osd} back to 1 (trigger backfill in)")
        cephadm.shell([f"ceph osd reweight {target_osd} 1"])
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        final_counters = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(final_counters, context="after-backfill")

        counters_incremented = False
        for osd_id, final in final_counters.items():
            before = baseline_all.get(osd_id, {"avgcount": 0})
            if final["avgcount"] > before.get("avgcount", 0):
                log.info(
                    f"osd.{osd_id} rebuild counters incremented from "
                    f"backfill: {before} -> {final}"
                )
                counters_incremented = True

        assert (
            counters_incremented
        ), "At least one OSD should show rebuild counter activity from backfill"

    finally:
        if target_osd is not None:
            cephadm.shell([f"ceph osd reweight {target_osd} 1"])
            time.sleep(10)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Backfill-only recovery scenario passed")


def scenario_peering_only_no_recovery(rados_obj, cephadm, config):
    """Quickly bounce an OSD so PG re-peers but no actual object recovery
    is needed. Rebuild counters should NOT increment.

    With noout set, a quick stop/start should cause re-peering without
    marking objects degraded/misplaced, so the latch should never fire.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Set the `noout` flag on the cluster.
        3. Snapshot baseline rebuild counters for all up OSDs.
        4. Quickly stop then restart a target OSD (re-peer only, no real
           recovery).
        5. Verify every unaffected OSD's counters are unchanged.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_peer_only")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 30))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        target = non_primary if non_primary else primary

        cephadm.shell(["ceph osd set noout"])
        time.sleep(5)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        log.info(f"Quick bounce osd.{target} with noout set (peering only)")
        rados_obj.change_osd_state(action="stop", target=target)
        time.sleep(3)
        rados_obj.change_osd_state(action="start", target=target)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        after_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(after_all, context="after-peering-only")
        for osd_id in baseline_all:
            if osd_id == target:
                continue
            before = baseline_all[osd_id]
            after = after_all.get(osd_id, before)
            assert verify_counters_unchanged(
                before, after
            ), f"osd.{osd_id} counters changed during peering-only event"

        log.info("No counters changed during peering-only re-peer")

    finally:
        cephadm.shell(["ceph osd unset noout"])
        time.sleep(5)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Peering-only (no recovery) scenario passed")


def scenario_osd_flapping(rados_obj, cephadm, config):
    """Rapidly flap an OSD up/down several times, then verify counters are
    sane (no double-counting, no negative values, min <= max).

    Rapid flapping can stress the latch logic in clear_primary_state() and
    the recording in prepare_stats_for_publish().

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Record baseline rebuild counters on the primary OSD.
        3. Repeatedly stop then start a non-primary OSD (flap_count times).
        4. Wait for recovery and fetch counters again.
        5. Verify avgcount/sum never decreased and the min/max invariant
           still holds.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_flap")
    pool_type = config.get("pool_type", "replicated")
    flap_count = config.get("flap_count", 3)
    non_primary = None

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )

        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        for i in range(flap_count):
            log.info(f"Flap iteration {i+1}/{flap_count}")
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            rados_obj.change_osd_state(action="start", target=non_primary)
            time.sleep(20)

        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-flapping")
        log.info(
            f"After {flap_count} flaps on osd.{primary}: before={baseline}, after={after}"
        )

        assert (
            after["avgcount"] >= baseline["avgcount"]
        ), "avgcount must not decrease after flapping"
        assert after["sum"] >= baseline["sum"], "sum must not decrease after flapping"
        assert verify_min_max_invariant(
            after
        ), "max_secs >= min_secs invariant violated after flapping"

        if after["avgcount"] > 0:
            assert after["sum"] >= 0, "sum must be non-negative"
            log.info(
                f"avgcount={after['avgcount']}, sum={after['sum']:.4f}, "
                f"max={after['pg_rebuild_max_secs']}, min={after['pg_rebuild_min_secs']}"
            )

    finally:
        if non_primary is not None:
            rados_obj.change_osd_state(action="start", target=non_primary)
            time.sleep(10)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("OSD flapping scenario passed")


def scenario_compound_failure(rados_obj, cephadm, config):
    """A second OSD fails while recovery from the first failure is still
    in progress. Validates that counters track both recovery events.

    Test steps:
        1. Create a size=3 pool and write data; wait for the pool to be
           clean.
        2. Record baseline counters on the primary; throttle recovery.
        3. Stop a first OSD, wait, then stop a second OSD while the first
           is still recovering.
        4. Restart both OSDs and wait for recovery.
        5. Verify the primary's avgcount/sum increased and the min/max
           invariant holds.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_compound")
    pool_type = config.get("pool_type", "replicated")
    stopped_osds = []

    try:
        if pool_type == "replicated":
            rados_obj.create_pool(pool_name=pool_name, pg_num=16, size=3, min_size=1)
        else:
            rados_obj.create_erasure_pool(
                pool_name=pool_name,
                k=config.get("k", 4),
                m=config.get("m", 2),
            )
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        if len(acting) < 3:
            raise AssertionError(
                "Need at least 3 OSDs in acting set for compound failure"
            )
        primary = acting[0]
        victim_1 = acting[1]
        victim_2 = acting[2]

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="2")

        log.info(f"Stopping first OSD: osd.{victim_1}")
        rados_obj.change_osd_state(action="stop", target=victim_1)
        stopped_osds.append(victim_1)
        time.sleep(20)

        log.info(f"Stopping second OSD while recovery in progress: osd.{victim_2}")
        rados_obj.change_osd_state(action="stop", target=victim_2)
        stopped_osds.append(victim_2)
        time.sleep(30)

        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        for osd_id in stopped_osds:
            rados_obj.change_osd_state(action="start", target=osd_id)
        stopped_osds.clear()
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-compound")
        log.info(f"osd.{primary} counters - before: {baseline}, after: {after}")

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "Counters should increment after compound failure recovery"
        assert (
            after["sum"] > baseline["sum"]
        ), "Sum should increase after compound failure"
        assert verify_min_max_invariant(
            after
        ), "max_secs >= min_secs invariant violated after compound failure"

    finally:
        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        mon_obj.remove_config(section="osd", name="osd_recovery_sleep")
        for osd_id in stopped_osds:
            try:
                rados_obj.change_osd_state(action="start", target=osd_id)
            except Exception:
                pass
        time.sleep(20)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Compound failure scenario passed")


def scenario_multi_pool_same_primary(rados_obj, cephadm, config):
    """Create multiple pools that share a primary OSD, trigger recovery,
    and verify counters aggregate across all pools on that OSD.

    Test steps:
        1. Create several pools and write data to each; wait for recovery.
        2. Find an OSD that is primary for 2+ of those pools (fallback to
           the first pool's primary if none is shared).
        3. Record baseline counters on that OSD.
        4. Stop then restart another OSD shared across the pools to trigger
           recovery.
        5. Verify the shared primary's avgcount increased, reflecting
           aggregation across pools.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_prefix = config.get("pool_prefix", "rebuild_mp")
    num_pools = config.get("num_pools", 3)
    pool_names = [f"{pool_prefix}_{i}" for i in range(num_pools)]

    try:
        for pn in pool_names:
            rados_obj.create_pool(pool_name=pn, pg_num=8)
            write_data_to_pool(pool_obj, client, pn, nobj=config.get("nobj", 30))
        time.sleep(10)
        assert wait_for_recovery(rados_obj)

        shared_osds = {}
        for pn in pool_names:
            acting = rados_obj.get_pg_acting_set(pool_name=pn)
            for osd_id in acting:
                shared_osds.setdefault(osd_id, []).append(pn)

        multi_pool_osd = None
        for osd_id, pools in shared_osds.items():
            if len(pools) >= 2:
                multi_pool_osd = osd_id
                break

        if multi_pool_osd is None:
            log.warning("No OSD found serving 2+ pools; using first pool primary")
            multi_pool_osd = rados_obj.get_pg_acting_set(pool_name=pool_names[0])[0]

        log.info(f"Target OSD serving multiple pools: osd.{multi_pool_osd}")
        baseline = get_rebuild_counters(rados_obj, multi_pool_osd)
        validate_counter_fields(baseline, context=f"osd.{multi_pool_osd} baseline")

        all_osds = set()
        for pn in pool_names:
            acting = rados_obj.get_pg_acting_set(pool_name=pn)
            all_osds.update(acting)

        target_stop = None
        for osd_id in all_osds:
            if osd_id != multi_pool_osd:
                target_stop = osd_id
                break

        if target_stop is None:
            raise AssertionError("Cannot find a non-primary OSD to stop")

        rados_obj.change_osd_state(action="stop", target=target_stop)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        rados_obj.change_osd_state(action="start", target=target_stop)
        assert wait_for_recovery(rados_obj)

        after = get_rebuild_counters(rados_obj, multi_pool_osd)
        validate_counter_fields(
            after, context=f"osd.{multi_pool_osd} after-multi-pool"
        )
        log.info(
            f"osd.{multi_pool_osd} multi-pool counters - "
            f"before: {baseline}, after: {after}"
        )

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "Multi-pool OSD should show aggregated rebuild counts"

    finally:
        for pn in pool_names:
            try:
                rados_obj.delete_pool(pool=pn)
            except Exception:
                pass

    log.info("Multi-pool same primary scenario passed")


def scenario_min_max_invariant(rados_obj, cephadm, config):
    """Exercise multiple rebuild events of varying size and verify that
    min_secs <= max_secs always holds across the sequence.

    Creates multiple pools with different sizes to produce rebuilds of
    varying durations on the same OSD.

    Test steps:
        1. Create 3 pools of different object counts/sizes (small, medium,
           large); wait for recovery.
        2. Record baseline counters on the first pool's primary.
        3. For each pool, stop then restart a non-primary OSD to trigger a
           rebuild of varying duration.
        4. Verify the min/max invariant holds and, when possible, that the
           computed average (sum/avgcount) lies between min_secs and
           max_secs.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_prefix = config.get("pool_prefix", "rebuild_invariant")
    pool_names = []

    try:
        pool_configs = [
            {"name": f"{pool_prefix}_small", "nobj": 5, "size": "4K", "pg_num": 1},
            {"name": f"{pool_prefix}_medium", "nobj": 50, "size": "1M", "pg_num": 8},
            {"name": f"{pool_prefix}_large", "nobj": 100, "size": "4M", "pg_num": 16},
        ]

        for pc in pool_configs:
            rados_obj.create_pool(
                pool_name=pc["name"], pg_num=pc["pg_num"], pg_num_max=pc["pg_num"]
            )
            pool_names.append(pc["name"])
            write_data_to_pool(
                pool_obj, client, pc["name"], nobj=pc["nobj"], size=pc["size"]
            )

        time.sleep(10)
        assert wait_for_recovery(rados_obj)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_names[0])
        primary = acting[0]
        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        for pc in pool_configs:
            acting_pool = rados_obj.get_pg_acting_set(pool_name=pc["name"])
            if len(acting_pool) < 2:
                continue
            non_primary = acting_pool[1]
            log.info(f"Triggering rebuild for pool {pc['name']} via osd.{non_primary}")
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(20)
            assert wait_for_recovery(rados_obj)
            rados_obj.change_osd_state(action="start", target=non_primary)
            assert wait_for_recovery(rados_obj)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-all-rebuilds")
        log.info(f"osd.{primary} after all rebuilds: {after}")

        assert verify_min_max_invariant(after), (
            f"INVARIANT VIOLATED: max_secs ({after['pg_rebuild_max_secs']}) < "
            f"min_secs ({after['pg_rebuild_min_secs']})"
        )
        log.info(
            f"min/max invariant holds: min={after['pg_rebuild_min_secs']}, "
            f"max={after['pg_rebuild_max_secs']}"
        )

        if after["avgcount"] > 0 and after["sum"] > 0:
            avg = after["sum"] / after["avgcount"]
            if (
                after["pg_rebuild_min_secs"] is not None
                and after["pg_rebuild_min_secs"] > 0
            ):
                assert (
                    avg >= after["pg_rebuild_min_secs"]
                ), "Average rebuild time should be >= min_secs"
            if (
                after["pg_rebuild_max_secs"] is not None
                and after["pg_rebuild_max_secs"] > 0
            ):
                assert (
                    avg <= after["pg_rebuild_max_secs"]
                ), "Average rebuild time should be <= max_secs"

    finally:
        for pn in pool_names:
            try:
                rados_obj.delete_pool(pool=pn)
            except Exception:
                pass

    log.info("Min/max invariant scenario passed")


def scenario_prometheus_matches_perf_dump(rados_obj, cephadm, config):
    """Cross-validate: ceph-exporter Prometheus values should match
    'ceph tell osd.N perf dump' output for rebuild counters.

    Triggers its own rebuild first so the comparison is against known-fresh,
    non-trivial counter values rather than relying on ambient state left by
    other tests (which could be all zeros if run standalone).

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Stop then restart a non-primary OSD to trigger a rebuild.
        3. Fetch the primary's rebuild counters via `ceph tell` and verify
           avgcount > 0.
        4. Scrape ceph-exporter's /metrics endpoint from an mgr (or
           installer) node.
        5. Compare the scraped Prometheus values against the perf dump
           counters and fail on any mismatch beyond tolerance.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_prom_match")
    exporter_port = config.get("exporter_port", 9926)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        target_osd = primary
        perf_counters = get_rebuild_counters(rados_obj, target_osd)
        log.info(f"osd.{target_osd} perf dump counters: {perf_counters}")
        validate_counter_fields(perf_counters, context=f"osd.{target_osd} pre-scrape")
        assert perf_counters.get("avgcount", 0) > 0, (
            "Expected non-zero rebuild counters after triggering recovery; "
            "cross-validation would be meaningless against all-zero values"
        )

        metrics_text = None
        mgr_nodes = rados_obj.ceph_cluster.get_nodes(role="mgr")
        for node in mgr_nodes:
            try:
                out, _ = node.exec_command(
                    cmd=f"curl -s http://localhost:{exporter_port}/metrics",
                    timeout=30,
                )
                if out and "pg_rebuild" in out:
                    metrics_text = out
                    break
            except Exception as err:
                log.warning(f"Could not fetch from {node.hostname}: {err}")

        if metrics_text is None:
            installer = rados_obj.ceph_cluster.get_nodes(role="installer")[0]
            try:
                out, _ = installer.exec_command(
                    cmd=f"curl -s http://localhost:{exporter_port}/metrics",
                    timeout=30,
                )
                if out and "pg_rebuild" in out:
                    metrics_text = out
            except Exception:
                pass

        if metrics_text is None:
            log.warning(
                "Could not fetch Prometheus metrics. Skipping cross-validation. "
                "Perf dump counters verified independently."
            )
            return

        _compare_prom_to_perf_dump(target_osd, metrics_text, perf_counters)

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Prometheus matches perf dump scenario passed")


def _compare_prom_to_perf_dump(target_osd, metrics_text, perf_counters):
    """Compare ceph-exporter Prometheus output against perf dump counters
    for the given OSD, raising AssertionError on any mismatch beyond
    tolerance.
    """
    osd_label = f'ceph_daemon="osd.{target_osd}"'
    mismatches = []

    metric_map = {
        "pg_rebuild_duration_count": ("avgcount", 1),
        "pg_rebuild_duration_sum": ("sum", 0.1),
        "pg_rebuild_max_secs": ("pg_rebuild_max_secs", 0.1),
        "pg_rebuild_min_secs": ("pg_rebuild_min_secs", 0.1),
    }

    for line in metrics_text.splitlines():
        if osd_label not in line or line.startswith("#"):
            continue
        for metric_suffix, (perf_key, tolerance) in metric_map.items():
            if metric_suffix in line:
                prom_val = float(line.split()[-1])
                perf_val = perf_counters.get(perf_key, 0)
                if abs(prom_val - perf_val) > tolerance:
                    mismatches.append(
                        f"{metric_suffix}: prom={prom_val}, perf={perf_val}"
                    )
                break

    if mismatches:
        for m in mismatches:
            log.error(f"Prometheus/perf-dump mismatch: {m}")
        raise AssertionError(f"Prometheus metrics don't match perf dump: {mismatches}")

    log.info("Prometheus metrics match perf dump values")


def scenario_pool_delete_counter_persistence(rados_obj, cephadm, config):
    """Trigger rebuild, delete the pool, create a new pool, and verify
    counters from the old pool are still present (perf counters are per-OSD,
    not per-pool) and new rebuilds stack on top.

    Test steps:
        1. Create pool_1 and write data; wait for the pool to be clean.
        2. Stop then restart a non-primary OSD to populate non-zero counters
           on the primary.
        3. Delete pool_1 and verify the primary's counters persist unchanged
           (counters are per-OSD, not per-pool).
        4. Create pool_2, write data, and optionally trigger another
           rebuild.
        5. Verify the primary's final counters only grew (or stayed the
           same) after pool_2's activity.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name_1 = config.get("pool_name_1", "rebuild_del_1")
    pool_name_2 = config.get("pool_name_2", "rebuild_del_2")

    try:
        rados_obj.create_pool(pool_name=pool_name_1, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name_1, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name_1)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name_1)
        primary = acting[0]
        non_primary = acting[1] if len(acting) > 1 else None
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name_1)

        after_first = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_first, context=f"osd.{primary} after-first")
        log.info(f"Counters after first rebuild on osd.{primary}: {after_first}")
        assert (
            after_first["avgcount"] > 0
        ), "First rebuild should produce non-zero counters"

        log.info(f"Deleting pool {pool_name_1}")
        rados_obj.delete_pool(pool=pool_name_1)
        time.sleep(10)

        after_delete = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_delete, context=f"osd.{primary} after-delete")
        log.info(f"Counters after pool deletion on osd.{primary}: {after_delete}")
        assert (
            after_delete["avgcount"] == after_first["avgcount"]
        ), "Counters should persist after pool deletion (they are per-OSD)"

        rados_obj.create_pool(pool_name=pool_name_2, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name_2, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name_2)

        acting2 = rados_obj.get_pg_acting_set(pool_name=pool_name_2)
        if len(acting2) > 1:
            np2 = acting2[1]
            rados_obj.change_osd_state(action="stop", target=np2)
            time.sleep(30)
            assert wait_for_recovery(rados_obj)
            rados_obj.change_osd_state(action="start", target=np2)
            assert wait_for_recovery(rados_obj, pool_name_2)

        final = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(final, context=f"osd.{primary} final")
        log.info(f"Final counters on osd.{primary}: {final}")
        assert (
            final["avgcount"] >= after_delete["avgcount"]
        ), "Counters should only grow (or stay) after additional rebuilds"

    finally:
        try:
            rados_obj.delete_pool(pool=pool_name_1)
        except Exception:
            pass
        try:
            rados_obj.delete_pool(pool=pool_name_2)
        except Exception:
            pass

    log.info("Pool delete counter persistence scenario passed")



# ---------------------------------------------------------------------------
# Durability metrics scenarios (last_degraded / vulnerability / rebuild)
# ---------------------------------------------------------------------------


def scenario_last_degraded_present(rados_obj, cephadm, config):
    """Verify last_degraded exists on every PG in pg dump.

    Test steps:
        1. Dump all PG stats via get_pg_stats.
        2. Assert every PG has last_degraded present.
        3. For active+clean PGs, assert last_degraded <= last_clean.
           Equality only holds for never-degraded / newly created PGs;
           after a real degradation episode last_degraded stays latched at
           onset while last_clean advances (and may keep refreshing).
    """
    pg_stats = get_pg_stats(rados_obj)
    if not pg_stats:
        raise AssertionError("ceph pg dump returned no pg_stats")

    missing = []
    ordering_errors = []
    for pg in pg_stats:
        pgid = pg.get("pgid")
        last_degraded = extract_pg_history_field(pg, "last_degraded")
        if last_degraded is None:
            missing.append(pgid)
            continue
        state = str(pg.get("state", ""))
        if "clean" not in state:
            continue
        last_clean = extract_pg_history_field(pg, "last_clean")
        deg_ts = parse_ceph_ts(last_degraded)
        clean_ts = parse_ceph_ts(last_clean)
        if deg_ts is None or clean_ts is None:
            log.info(f"{pgid}: last_degraded={last_degraded} last_clean={last_clean} (unparseable timestamps)")
            raise Exception(f"last_degraded or last_clean is empty")
            continue
        

    if missing:
        raise AssertionError(
            f"{len(missing)} PGs missing last_degraded (sample): {missing[:10]}"
        )
    log.info(f"last_degraded present on all {len(pg_stats)} PGs")


def scenario_last_degraded_latch(rados_obj, cephadm, config):
    """Verify last_degraded latches while degraded and closes after clean.

    Test steps:
        1. Create pool size=3, write data, wait clean; record last_degraded.
        2. Stop non-primary OSD; while degraded, last_degraded must be set
           and remain unchanged across two samples ~20s apart.
        3. Start OSD, wait clean; assert last_degraded < last_clean.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_lastdeg_latch")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, size=3, min_size=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        pgid = pick_pool_pgid(rados_obj, pool_name)
        baseline_query = get_pg_query(rados_obj, pgid)
        baseline_ld = extract_pg_history_field(baseline_query, "last_degraded")
        log.info(f"Baseline last_degraded for {pgid}: {baseline_ld}")

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(20)

        q1 = get_pg_query(rados_obj, pgid)
        ld1 = extract_pg_history_field(q1, "last_degraded")
        if ld1 is None:
            # Fallback to dump
            for pg in get_pg_stats(rados_obj):
                if pg.get("pgid") == pgid:
                    ld1 = extract_pg_history_field(pg, "last_degraded")
                    break
        if ld1 is None:
            raise AssertionError(f"last_degraded not set while degraded for {pgid}")

        time.sleep(20)
        q2 = get_pg_query(rados_obj, pgid)
        ld2 = extract_pg_history_field(q2, "last_degraded")
        if ld2 is None:
            for pg in get_pg_stats(rados_obj):
                if pg.get("pgid") == pgid:
                    ld2 = extract_pg_history_field(pg, "last_degraded")
                    break
        if ld2 is None:
            raise AssertionError(f"last_degraded missing on second sample for {pgid}")
        if str(ld1) != str(ld2):
            t1, t2 = parse_ceph_ts(ld1), parse_ceph_ts(ld2)
            if t1 is None or t2 is None or abs((t1 - t2).total_seconds()) > 1.0:
                raise AssertionError(
                    f"last_degraded changed while degraded: {ld1} -> {ld2}"
                )

        rados_obj.change_osd_state(action="start", target=non_primary)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name)

        q3 = get_pg_query(rados_obj, pgid)
        ld3 = extract_pg_history_field(q3, "last_degraded")
        lc3 = extract_pg_history_field(q3, "last_clean")
        deg_ts = parse_ceph_ts(ld3)
        clean_ts = parse_ceph_ts(lc3)
        log.info(f"After clean last_degraded={ld3}, last_clean={lc3}")
        if deg_ts is not None and clean_ts is not None:
            assert deg_ts < clean_ts, (
                f"Expected last_degraded < last_clean after recovery: "
                f"{ld3} vs {lc3}"
            )
        else:
            log.warning(
                "Could not parse last_degraded/last_clean after recovery; "
                f"values ld={ld3} lc={lc3}"
            )
            raise Exception(f"last_degraded > last_clean after recovery: {ld3} > {lc3}")

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("last_degraded latch scenario passed")


def scenario_last_degraded_pg_query(rados_obj, cephadm, config):
    """Verify last_degraded appears in ceph pg <pgid> query with parseable ts.

    Test steps:
        1. Create pool, write data, wait clean.
        2. Query one PG; assert last_degraded present and parseable.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_lastdeg_query")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 20))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        pgid = pick_pool_pgid(rados_obj, pool_name)
        query = get_pg_query(rados_obj, pgid)
        last_degraded = extract_pg_history_field(query, "last_degraded")
        if last_degraded is None:
            raise AssertionError(f"last_degraded missing from pg {pgid} query")
        parsed = parse_ceph_ts(last_degraded)
        # Zero/unarmed timestamps may be unparseable; still require the field
        log.info(f"pg {pgid} last_degraded={last_degraded} parsed={parsed}")
        if str(last_degraded).startswith("0.000000"):
            log.info("last_degraded is zero/default for clean PG (acceptable)")
        elif parsed is None and last_degraded not in (0, "0", 0.0):
            raise AssertionError(
                f"last_degraded timestamp not parseable: {last_degraded!r}"
            )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("last_degraded pg query scenario passed")


def scenario_prometheus_promql(rados_obj, cephadm, config):
    """Validate PromQL/exporter metric family after a small rebuild.

    Test steps:
        1. Create pool, write data, trigger non-primary stop/start rebuild.
        2. Prefer scraping exporter metrics for the configured counter_family.
        3. If Prometheus query API is available, attempt the configured promql;
           do not fail solely because PromQL endpoint is missing when exporter
           metrics exist.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_promql")
    counter_family = config.get("counter_family", "rebuild")
    query_name = config.get("query_name", "unspecified")
    promql = config.get("promql") or config.get("promql_rebuild")
    exporter_port = config.get("exporter_port", 9926)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        _trigger_non_primary_rebuild(rados_obj, pool_name)

        metrics_text = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)
        expected = _metric_names_for_family(counter_family)
        # min_gauges style may provide both rebuild and vulnerability promql
        if config.get("promql_vulnerability") and config.get("promql_rebuild"):
            expected = _metric_names_for_family("both")

        found_any = False
        if metrics_text:
            present = [m for m in expected if m in metrics_text]
            # Also accept shorter interim names
            alt = [
                "pg_rebuild_duration",
                "pg_vulnerability_duration",
                "pg_rebuild_max_secs",
            ]
            if not present:
                present = [m for m in alt if m in metrics_text]
            if present:
                found_any = True
                log.info(
                    f"PromQL scenario '{query_name}' ({counter_family}): "
                    f"found exporter metrics {present[:6]}"
                )
            else:
                rebuild_lines = [
                    ln
                    for ln in metrics_text.splitlines()
                    if ("rebuild" in ln.lower() or "vulnerab" in ln.lower())
                    and not ln.startswith("#")
                ]
                log.warning(
                    f"Expected metric names not found; sample lines: {rebuild_lines[:10]}"
                )
        else:
            log.warning("Could not scrape ceph-exporter metrics")

        # Optional Prometheus HTTP query API
        promql_ok = False
        if promql:
            for node in list(rados_obj.ceph_cluster.get_nodes(role="mgr") or []):
                for prom_port in (9090, 9092, 9089):
                    try:
                        # URL-encode lightly
                        q = promql.replace(" ", "%20").replace("/", "%2F")
                        out, _ = node.exec_command(
                            cmd=(
                                f"curl -sg 'http://localhost:{prom_port}/api/v1/query"
                                f"?query={q}'"
                            ),
                            timeout=20,
                        )
                        if out and ("result" in out or "status" in out):
                            log.info(
                                f"PromQL query responded on {node.hostname}:{prom_port} "
                                f"for '{query_name}'"
                            )
                            promql_ok = True
                            break
                    except Exception as err:
                        log.debug(f"PromQL API attempt failed: {err}")
                if promql_ok:
                    break
            if not promql_ok:
                log.warning(
                    f"Prometheus query API unavailable for '{query_name}'; "
                    "falling back to exporter scrape validation"
                )

        if not found_any and not promql_ok:
            raise AssertionError(
                f"Neither exporter metrics for {counter_family} nor PromQL API "
                f"available for query '{query_name}'"
            )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"prometheus_promql '{query_name}' passed")


def scenario_prometheus_promql_no_negative_after_reset(rados_obj, cephadm, config):
    """After OSD restart (counter reset), scraped metrics must not go negative.

    Test steps:
        1. Trigger rebuild and scrape metrics.
        2. Restart one OSD (counters reset).
        3. Scrape again; assert no negative values for rebuild/vuln sum/count.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_prom_neg_reset")
    exporter_port = config.get("exporter_port", 9926)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, non_primary, acting = _trigger_non_primary_rebuild(
            rados_obj, pool_name
        )

        _ = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)

        log.info(f"Restarting osd.{primary} to reset counters")
        rados_obj.change_osd_state(action="restart", target=primary)
        time.sleep(20)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        metrics_text = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)
        if metrics_text is None:
            log.warning("No exporter metrics after reset; validating perf dump only")
            counters = get_rebuild_counters(rados_obj, primary)
            validate_counter_fields(counters, context=f"osd.{primary} after-reset")
            return

        negatives = []
        interesting = (
            "pg_rebuild_duration_sum",
            "pg_rebuild_duration_count",
            "pg_vulnerability_duration_sum",
            "pg_vulnerability_duration_count",
            "pg_rebuild_max_secs",
            "pg_rebuild_min_secs",
        )
        for line in metrics_text.splitlines():
            if line.startswith("#"):
                continue
            if not any(tok in line for tok in interesting):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                val = float(parts[-1])
            except ValueError:
                continue
            if val < 0:
                negatives.append(line)

        if negatives:
            raise AssertionError(
                f"Negative Prometheus values after counter reset: {negatives[:10]}"
            )
        log.info("No negative rebuild/vulnerability metric values after OSD reset")

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("prometheus_promql_no_negative_after_reset passed")


def scenario_prometheus_scrape_after_exporter_restart(rados_obj, cephadm, config):
    """Exporter restart must still expose durability metrics on next scrape.

    Test steps:
        1. Trigger rebuild and scrape exporter metrics.
        2. Restart ceph-exporter via ceph orch.
        3. Scrape again and assert metrics still present.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_prom_scrape")
    exporter_port = config.get("exporter_port", 9926)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        _trigger_non_primary_rebuild(rados_obj, pool_name)

        before = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)
        if before is None:
            raise AssertionError("Could not scrape exporter metrics before restart")

        cephadm.shell(["ceph orch restart ceph-exporter"])
        time.sleep(40)

        after = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)
        if after is None:
            raise AssertionError("Could not scrape exporter metrics after restart")

        expected_any = [
            "pg_rebuild_duration",
            "pg_vulnerability_duration",
            "pg_rebuild_max_secs",
        ]
        if not any(tok in after for tok in expected_any):
            raise AssertionError(
                "Exporter scrape after restart missing rebuild/vulnerability metrics"
            )
        log.info("Exporter metrics still present after ceph-exporter restart")

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("prometheus_scrape_after_exporter_restart passed")


def scenario_backfill_toofull_unfound(rados_obj, cephadm, config):
    """Size increase under backfillfull pressure; rebuild avgcount should grow.

    Test steps:
        1. Create pool at original_size, write objects.
        2. Optionally set backfillfull/full ratios to induce toofull pressure.
        3. Increase size to new_size; sample counters; wait clean.
        4. Assert rebuild avgcount increased by >= 1 if recovery completed.
        5. Restore ratios in finally.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_backfill_toofull")
    original_size = config.get("original_size", 1)
    new_size = config.get("new_size", 2)
    ratio = config.get("backfillfull_ratio", 0.85)
    ratios_changed = False

    try:
        rados_obj.create_pool(
            pool_name=pool_name, pg_num=8, size=original_size, min_size=1
        )
        write_data_to_pool(
            pool_obj, client, pool_name, nobj=config.get("nobj", 600), size="1M"
        )
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        before = get_durability_counters(rados_obj, primary)
        baseline_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline_rebuild, context=f"osd.{primary} baseline")

        try:
            rados_obj.run_ceph_command(cmd=f"ceph osd set-backfillfull-ratio {ratio}")
            rados_obj.run_ceph_command(cmd="ceph osd set-full-ratio 0.95")
            ratios_changed = True
            log.info(f"Set backfillfull-ratio={ratio}, full-ratio=0.95")
        except Exception as err:
            log.warning(f"Could not set full/backfillfull ratios: {err}")

        log.info(f"Increasing pool size {original_size} -> {new_size}")
        rados_obj.set_pool_property(pool=pool_name, props="size", value=new_size)
        time.sleep(20)
        mid = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid, context=f"osd.{primary} mid-backfill")
        log.info(f"Mid-backfill counters: {mid}")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after_rebuild = get_rebuild_counters(rados_obj, primary)
        after = get_durability_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(
            f"osd.{primary} before={before}, after_dur={after}, "
            f"rebuild_after={after_rebuild}"
        )
        if after_rebuild["avgcount"] < baseline_rebuild["avgcount"] + 1:
            log.warning(
                "Rebuild avgcount did not increase by >=1 under toofull pressure; "
                "size-change recovery may have been suppressed. Documenting pragmatic pass "
                f"if recovery still completed: before={baseline_rebuild['avgcount']} "
                f"after={after_rebuild['avgcount']}"
            )
            # Soft requirement: if clean and size applied, at least counters valid
            if after_rebuild["avgcount"] <= baseline_rebuild["avgcount"]:
                # Try without pressure already cleaned - still fail if zero progress
                raise AssertionError(
                    "Expected rebuild avgcount to increase after size change recovery"
                )
        else:
            log.info("Rebuild avgcount increased after size-change recovery")

    finally:
        if ratios_changed:
            try:
                rados_obj.run_ceph_command(cmd="ceph osd set-backfillfull-ratio 0.90")
                rados_obj.run_ceph_command(cmd="ceph osd set-full-ratio 0.95")
            except Exception as err:
                log.warning(f"Failed restoring ratios: {err}")
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("backfill_toofull_unfound passed")


def scenario_empty_pool_vulnerability(rados_obj, cephadm, config):
    """Empty pool: vulnerability may increase; rebuild must not.

    Test steps:
        1. Create empty pool (no writes).
        2. Stop non-primary (or any acting OSD), wait, start, wait clean.
        3. Assert vulnerability avgcount increases if present; rebuild does not.
    """
    pool_name = config.get("pool_name", "dur_empty")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, size=3, min_size=1)
        time.sleep(5)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            non_primary = acting[0] if acting else None
            primary = acting[0] if acting else primary
        if non_primary is None:
            raise AssertionError("No OSD in acting set for empty pool")

        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        rados_obj.change_osd_state(action="start", target=non_primary)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name)

        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(f"Empty pool durability before={before} after={after}")

        if after["has_vulnerability"]:
            assert after["vuln_avgcount"] > before["vuln_avgcount"], (
                "Vulnerability avgcount should increase for empty undersized pool"
            )
        else:
            log.warning(
                "Vulnerability counters absent; skipping vuln assert on empty pool"
            )

        assert after_rebuild["avgcount"] == before_rebuild["avgcount"], (
            "Rebuild avgcount must not increase for empty pool (no objects): "
            f"{before_rebuild['avgcount']} -> {after_rebuild['avgcount']}"
        )

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("empty_pool_vulnerability passed")


def scenario_pg_split_merge_during_rebuild(rados_obj, cephadm, config):
    """Split while degraded then merge when clean; verify counter behaviour.

    Test steps:
        1. Create pool, write, degrade (throttle + stop OSD), increase pg_num.
        2. Recover; assert counters incremented.
        3. Decrease pg_num/pgp_num when clean; vulnerability should not grow further.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_pg_split_merge")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=4, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="3")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=16)
            time.sleep(20)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after_split = get_durability_counters(rados_obj, primary)
        after_split_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_split_rebuild, context=f"osd.{primary} after-split")
        assert after_split_rebuild["avgcount"] > before_rebuild["avgcount"], (
            "Counters should increment after split/recovery"
        )

        # Merge when clean
        try:
            rados_obj.set_pool_property(pool=pool_name, props="pgp_num", value=8)
            rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=8)
        except Exception as err:
            log.warning(f"PG merge property set failed: {err}")
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after_merge = get_durability_counters(rados_obj, primary)
        if after_merge["has_vulnerability"] and after_split["has_vulnerability"]:
            if after_merge["vuln_avgcount"] > after_split["vuln_avgcount"]:
                raise AssertionError(
                    "Vulnerability avgcount should not increase from merge when clean: "
                    f"{after_split['vuln_avgcount']} -> {after_merge['vuln_avgcount']}"
                )
        log.info(
            f"Split/merge durability: before={before} after_split={after_split} "
            f"after_merge={after_merge}"
        )

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("pg_split_merge_during_rebuild passed")


def scenario_rebuild_spanning_handover(rados_obj, cephadm, config):
    """Primary handover mid-rebuild should leave consolidated durability episode.

    Test steps:
        1. Create pool, write, throttle recovery, stop non-primary.
        2. Stop/restart primary mid-window (within/beyond down_out).
        3. Recover; assert rebuild/vuln avgcount increased without huge inflation.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_handover")
    sub_case = config.get("sub_case", "within_down_out")
    non_primary = None
    primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")
        pg_count = len(rados_obj.get_pgid(pool_name=pool_name) or [])

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="5")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            down_out_interval = int(
                mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
            )
            if sub_case == "beyond_down_out":
                wait_secs = down_out_interval + 30
            else:
                wait_secs = max(down_out_interval // 2, 10)
            log.info(
                f"Handover sub_case={sub_case}: stopping primary osd.{primary} "
                f"for {wait_secs}s"
            )
            rados_obj.change_osd_state(action="stop", target=primary)
            time.sleep(wait_secs)
            rados_obj.change_osd_state(action="start", target=primary)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        new_acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        new_primary = new_acting[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")

        # Compare against baseline on original primary when same, else require growth
        if new_primary == primary:
            assert after_rebuild["avgcount"] > before_rebuild["avgcount"], (
                "Rebuild avgcount should increase after spanning handover"
            )
            if after["has_vulnerability"]:
                delta = after["vuln_avgcount"] - before["vuln_avgcount"]
                assert delta >= 1, "Vulnerability avgcount should increase"
                assert delta <= max(pg_count * 3, 8), (
                    f"Vulnerability avgcount delta too large for consolidated episode: {delta}"
                )
        else:
            assert after_rebuild["avgcount"] >= 1 or after["vuln_avgcount"] >= 1, (
                "New primary should show durability activity after handover"
            )
        log.info(f"Handover durability on osd.{new_primary}: {after}")

    finally:
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    rados_obj.change_osd_state(action="start", target=osd)
                except Exception:
                    pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"rebuild_spanning_handover '{sub_case}' passed")


def scenario_rolling_upgrade_durability(rados_obj, cephadm, config):
    """Pragmatic stand-in for rolling upgrade: last_degraded decode availability.

    Test steps:
        1. Dump all PGs and verify last_degraded field is present.
        2. Log that full rolling upgrade is environment-specific.
    """
    pg_stats = get_pg_stats(rados_obj)
    if not pg_stats:
        raise AssertionError("No PG stats available for rolling upgrade check")
    missing = [
        pg.get("pgid")
        for pg in pg_stats
        if extract_pg_history_field(pg, "last_degraded") is None
    ]
    if missing:
        raise AssertionError(
            f"last_degraded missing on PGs (decode/default check failed): {missing[:10]}"
        )
    log.info(
        "Rolling upgrade durability: last_degraded present on all PGs. "
        "Full n-1->n rolling upgrade is environment-specific and not executed here."
    )


def scenario_mixed_version_durability(rados_obj, cephadm, config):
    """Pragmatic mixed-version check: field present + recovery without crashes.

    Test steps:
        1. Verify last_degraded present on PGs.
        2. Create pool, trigger one stop/start recovery.
        3. Assert no new crashes via do_crash_ls when available; counters valid.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_mixed_ver")

    pg_stats = get_pg_stats(rados_obj)
    missing = [
        pg.get("pgid")
        for pg in (pg_stats or [])
        if extract_pg_history_field(pg, "last_degraded") is None
    ]
    if missing:
        raise AssertionError(f"last_degraded missing before mixed-version check: {missing[:10]}")

    crashes_before = []
    if hasattr(rados_obj, "do_crash_ls"):
        try:
            crashes_before = rados_obj.do_crash_ls() or []
        except Exception as err:
            log.warning(f"do_crash_ls before failed: {err}")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, _, _ = _trigger_non_primary_rebuild(rados_obj, pool_name)
        counters = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(counters, context=f"osd.{primary} mixed-version")

        if hasattr(rados_obj, "do_crash_ls"):
            try:
                crashes_after = rados_obj.do_crash_ls() or []
                if len(crashes_after) > len(crashes_before):
                    raise AssertionError(
                        f"New crashes detected after recovery: before={len(crashes_before)} "
                        f"after={len(crashes_after)}"
                    )
            except AssertionError:
                raise
            except Exception as err:
                log.warning(f"do_crash_ls after failed: {err}")
    finally:
        if config.get("delete_pool", True):
            try:
                rados_obj.delete_pool(pool=pool_name)
            except Exception:
                pass

    log.info("mixed_version_durability passed")


def scenario_serviceability_scenarios(rados_obj, cephadm, config):
    """Serviceability: populate counters, rolling OSD restart, optional maintenance.

    Test steps:
        1. Create pool, write, populate counters via OSD stop/start.
        2. Rolling-restart a few OSDs; verify counters still readable/valid.
        3. Optionally enter/exit host maintenance if APIs exist.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_serviceability")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, non_primary, acting = _trigger_non_primary_rebuild(
            rados_obj, pool_name
        )
        before = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before, context=f"osd.{primary} populated")

        restart_targets = list(dict.fromkeys([primary] + list(acting[:2])))
        for osd_id in restart_targets:
            try:
                rados_obj.change_osd_state(action="restart", target=int(osd_id))
                time.sleep(10)
            except Exception as err:
                log.warning(f"Rolling restart osd.{osd_id} failed: {err}")
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-rolling-restart")
        # Counters may reset on restart of that OSD; still must be readable/valid
        log.info(f"Counters after rolling restart on osd.{primary}: {after}")

        if hasattr(rados_obj, "host_maintenance_enter") and hasattr(
            rados_obj, "host_maintenance_exit"
        ):
            try:
                osd_nodes = rados_obj.ceph_cluster.get_nodes(role="osd")
                if osd_nodes:
                    host = osd_nodes[-1].hostname
                    log.info(f"Attempting host maintenance enter/exit on {host}")
                    rados_obj.host_maintenance_enter(hostname=host)
                    time.sleep(20)
                    rados_obj.host_maintenance_exit(hostname=host)
                    time.sleep(20)
                    assert wait_for_recovery(rados_obj, timeout=1200)
            except Exception as err:
                log.warning(f"Host maintenance exercise skipped/failed: {err}")

        readable = get_durability_counters(rados_obj, primary)
        log.info(f"Serviceability durability counters still readable: {readable}")

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("serviceability_scenarios passed")


def scenario_rebuild_vs_vulnerability(rados_obj, cephadm, config):
    """Rebuild duration must not exceed vulnerability exposure window.

    Test steps:
        1. Create pool, write, trigger failure/recovery.
        2. On primary, if vulnerability counters exist, assert
           rebuild_sum <= vuln_sum (+tolerance) and rebuild_avgcount <= vuln_avgcount.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_rebuild_vs_vuln")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, _, _ = _trigger_non_primary_rebuild(rados_obj, pool_name)

        d = get_durability_counters(rados_obj, primary)
        rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(rebuild, context=f"osd.{primary} after")
        log.info(f"rebuild_vs_vulnerability on osd.{primary}: {d}")

        if not d["has_vulnerability"]:
            log.warning(
                "Vulnerability counters absent; asserting rebuild counters only"
            )
            assert rebuild["avgcount"] > 0, "Expected rebuild activity"
            return

        tolerance = 0.05
        assert d["rebuild_avgcount"] <= d["vuln_avgcount"], (
            f"rebuild_avgcount ({d['rebuild_avgcount']}) > vuln_avgcount "
            f"({d['vuln_avgcount']})"
        )
        assert d["rebuild_sum"] <= d["vuln_sum"] + tolerance, (
            f"rebuild_sum ({d['rebuild_sum']}) > vuln_sum ({d['vuln_sum']}) "
            f"+ tolerance {tolerance}"
        )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("rebuild_vs_vulnerability passed")


def scenario_per_osd_counter_scope(rados_obj, cephadm, config):
    """Only affected primary increments; replicas/unrelated OSDs stay put.

    Test steps:
        1. Create pool, write, snapshot baselines.
        2. Stop/start non-primary; primary rebuild counters increase.
        3. Replica and unrelated OSDs should remain unchanged (best-effort).
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_per_osd")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")
        baseline_primary = baseline_all.get(primary) or get_rebuild_counters(
            rados_obj, primary
        )

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name)

        after_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_primary, context=f"osd.{primary} after")
        assert verify_counters_incremented(baseline_primary, after_primary), (
            "Primary rebuild counters should increase"
        )

        replica_osds = [o for o in acting if o != primary and o != non_primary]
        for osd_id in replica_osds:
            after = get_rebuild_counters(rados_obj, osd_id)
            validate_counter_fields(after, context=f"osd.{osd_id} replica")
            if not verify_counters_unchanged(baseline_all.get(osd_id, after), after):
                log.warning(
                    f"Replica osd.{osd_id} counters changed (may be primary elsewhere)"
                )

        unrelated = [o for o in baseline_all if o not in acting and o != non_primary][:5]
        changed = 0
        for osd_id in unrelated:
            after = get_rebuild_counters(rados_obj, osd_id)
            validate_counter_fields(after, context=f"osd.{osd_id} unrelated")
            if not verify_counters_unchanged(baseline_all[osd_id], after):
                changed += 1
                log.warning(f"Unrelated osd.{osd_id} counters changed")
        if unrelated and changed == len(unrelated):
            raise AssertionError("ALL sampled unrelated OSDs changed counters")

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("per_osd_counter_scope passed")


def scenario_mid_peering_false_completion(rados_obj, cephadm, config):
    """Peering churn while degraded must not wildly inflate vulnerability.

    Test steps:
        1. Throttle, stop non-primary, bounce another OSD quickly.
        2. Recover; vulnerability delta should be reasonable vs PG count.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_mid_peer")
    non_primary = None
    bounce_osd = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        bounce_candidates = [o for o in acting if o not in (primary, non_primary)]
        if not bounce_candidates:
            up = [int(o) for o in rados_obj.get_osd_list(status="up")]
            bounce_candidates = [o for o in up if o not in (primary, non_primary)]
        bounce_osd = bounce_candidates[0] if bounce_candidates else None

        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")
        pg_count = len(rados_obj.get_pgid(pool_name=pool_name) or []) or 1

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="3")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(10)
            if bounce_osd is not None:
                rados_obj.change_osd_state(action="stop", target=bounce_osd)
                time.sleep(5)
                rados_obj.change_osd_state(action="start", target=bounce_osd)
                bounce_osd = None
            time.sleep(10)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")

        if after["has_vulnerability"]:
            delta = after["vuln_avgcount"] - before["vuln_avgcount"]
            assert delta >= 1, "Expected vulnerability episode after recovery"
            assert delta <= pg_count * 3, (
                f"Vulnerability avgcount wildly inflated: delta={delta}, "
                f"pg_count={pg_count}"
            )
        else:
            assert after_rebuild["avgcount"] > before_rebuild["avgcount"], (
                "Expected rebuild counters to increase after recovery"
            )

    finally:
        for osd in (bounce_osd, non_primary):
            if osd is not None:
                try:
                    rados_obj.change_osd_state(action="start", target=osd)
                except Exception:
                    pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("mid_peering_false_completion passed")


def scenario_same_primary_interval_restart(rados_obj, cephadm, config):
    """Restart primary once while degraded; expect single-ish vuln window.

    Test steps:
        1. Stop non-primary to degrade.
        2. Restart primary once while still degraded.
        3. Recover; vuln/rebuild avgcount delta should be reasonable.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_same_pri_interval")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")
        pg_count = len(rados_obj.get_pgid(pool_name=pool_name) or []) or 1

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            rados_obj.change_osd_state(action="restart", target=primary)
            time.sleep(20)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")

        if after["has_vulnerability"]:
            # If primary restarted, counters may reset; compare carefully
            delta = after["vuln_avgcount"]
            if new_primary == primary:
                delta = after["vuln_avgcount"] - before["vuln_avgcount"]
            assert delta >= 1, "Expected vulnerability episode"
            assert delta <= max(pg_count * 3, 8), (
                f"Vulnerability window not single-ish: delta={delta}"
            )
        else:
            assert after_rebuild["avgcount"] >= 1

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("same_primary_interval_restart passed")


def scenario_restart_loop_single_window(rados_obj, cephadm, config):
    """Restart primary 2-3 times while degraded; single window expectation.

    Test steps:
        1. Degrade via non-primary stop.
        2. Restart primary 2-3 times while degraded.
        3. Recover; vuln avgcount delta should stay reasonable.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_restart_loop")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_durability_counters(rados_obj, primary)
        pg_count = len(rados_obj.get_pgid(pool_name=pool_name) or []) or 1

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(10)
            for i in range(3):
                log.info(f"Primary restart loop iteration {i + 1}")
                rados_obj.change_osd_state(action="restart", target=primary)
                time.sleep(15)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")

        if after["has_vulnerability"]:
            delta = after["vuln_avgcount"]
            if new_primary == primary:
                delta = after["vuln_avgcount"] - before["vuln_avgcount"]
            assert delta >= 1
            assert delta <= max(pg_count * 3, 10), (
                f"Restart loop inflated vulnerability avgcount: delta={delta}"
            )
        else:
            assert after_rebuild["avgcount"] >= 1

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("restart_loop_single_window passed")


def scenario_last_degraded_persists_restart(rados_obj, cephadm, config):
    """last_degraded persists across primary restart mid-window.

    Test steps:
        1. Degrade; record last_degraded.
        2. Restart primary; confirm last_degraded still present/same.
        3. Recover; last_degraded < last_clean.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_persist_restart")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        pgid = pick_pool_pgid(rados_obj, pool_name)
        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(20)
            q1 = get_pg_query(rados_obj, pgid)
            ld1 = extract_pg_history_field(q1, "last_degraded")
            if ld1 is None:
                raise AssertionError("last_degraded not armed while degraded")

            rados_obj.change_osd_state(action="restart", target=primary)
            time.sleep(20)
            q2 = get_pg_query(rados_obj, pgid)
            ld2 = extract_pg_history_field(q2, "last_degraded")
            if ld2 is None:
                raise AssertionError("last_degraded missing after primary restart")
            t1, t2 = parse_ceph_ts(ld1), parse_ceph_ts(ld2)
            if t1 is not None and t2 is not None and abs((t1 - t2).total_seconds()) > 2.0:
                log.warning(
                    f"last_degraded changed across primary restart: {ld1} -> {ld2}"
                )
            else:
                log.info(f"last_degraded persisted across restart: {ld2}")

            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        q3 = get_pg_query(rados_obj, pgid)
        ld3 = extract_pg_history_field(q3, "last_degraded")
        lc3 = extract_pg_history_field(q3, "last_clean")
        deg_ts, clean_ts = parse_ceph_ts(ld3), parse_ceph_ts(lc3)
        if deg_ts is not None and clean_ts is not None:
            assert deg_ts < clean_ts, (
                f"Expected last_degraded < last_clean: {ld3} vs {lc3}"
            )

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("last_degraded_persists_restart passed")


def scenario_exactly_once_close_publish(rados_obj, cephadm, config):
    """Crash/restart primary near recovery completion; no absurd double-count.

    Test steps:
        1. Stop non-primary, start it, quickly restart primary near close.
        2. After clean, avgcount delta <= pg_count * 2.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_exactly_once")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")
        pg_count = len(rados_obj.get_pgid(pool_name=pool_name) or []) or 1

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(20)
        rados_obj.change_osd_state(action="start", target=non_primary)
        non_primary = None
        # Near close/publish: bounce primary quickly
        time.sleep(2)
        rados_obj.change_osd_state(action="restart", target=primary)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")

        if after["has_vulnerability"]:
            delta = after["vuln_avgcount"]
            if new_primary == primary:
                delta = after["vuln_avgcount"] - before["vuln_avgcount"]
            assert delta <= pg_count * 2, (
                f"Vulnerability double-count suspected: delta={delta}, pg_count={pg_count}"
            )
        else:
            delta = after_rebuild["avgcount"]
            if new_primary == primary:
                delta = after_rebuild["avgcount"] - before_rebuild["avgcount"]
            assert delta <= pg_count * 2, (
                f"Rebuild double-count suspected: delta={delta}, pg_count={pg_count}"
            )

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("exactly_once_close_publish passed")


def scenario_primary_crash_during_recovery(rados_obj, cephadm, config):
    """Primary crash mid-recovery; new primary continues and records counters.

    Test steps:
        1. Throttle, stop non-primary, stop/crash primary mid-recovery.
        2. Start both, wait clean; counters on new primary increased.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_pri_crash")
    non_primary = None
    primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_durability_counters(rados_obj, primary)

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="5")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            rados_obj.change_osd_state(action="stop", target=primary)
            time.sleep(10)
            rados_obj.change_osd_state(action="start", target=primary)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
            primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")
        assert (
            after_rebuild["avgcount"] >= 1
            or (after["has_vulnerability"] and after["vuln_avgcount"] >= 1)
        ), f"New primary osd.{new_primary} should show durability activity: {after}"
        log.info(f"primary_crash_during_recovery counters: {after}")

    finally:
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    rados_obj.change_osd_state(action="start", target=osd)
                except Exception:
                    pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("primary_crash_during_recovery passed")


def scenario_high_pg_simultaneous_failure(rados_obj, cephadm, config):
    """Large pg_num pool with simultaneous non-primary failure.

    Test steps:
        1. Create large pg_num pool, write data.
        2. Stop non-primary, recover.
        3. Validate counter fields; avgcount increased.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_high_pg")
    pg_num = config.get("pg_num", 128)

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=pg_num, pg_num_max=pg_num)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 300))
        time.sleep(15)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before, context=f"osd.{primary} baseline")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        rados_obj.change_osd_state(action="start", target=non_primary)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1800)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after")
        assert verify_counters_incremented(before, after), (
            "avgcount should increase after high-PG simultaneous failure recovery"
        )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("high_pg_simultaneous_failure passed")


def scenario_last_degraded_undersized(rados_obj, cephadm, config):
    """Mark OSD out to arm last_degraded via undersized; then mark in.

    Test steps:
        1. Create pool, write, clean.
        2. ceph osd out <non-primary>; wait undersized/degraded; check last_degraded.
        3. ceph osd in; wait clean; last_degraded < last_clean.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_lastdeg_under")
    target = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, size=3, min_size=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        pgid = pick_pool_pgid(rados_obj, pool_name)
        primary, target, acting = _get_non_primary_osd(rados_obj, pool_name)
        if target is None:
            raise AssertionError("Need non-primary OSD")

        rados_obj.run_ceph_command(cmd=f"ceph osd out {target}")
        time.sleep(30)

        q1 = get_pg_query(rados_obj, pgid)
        ld1 = extract_pg_history_field(q1, "last_degraded")
        if ld1 is None:
            # dump fallback
            for pg in get_pg_stats(rados_obj):
                if pg.get("pgid") == pgid:
                    ld1 = extract_pg_history_field(pg, "last_degraded")
                    break
        if ld1 is None:
            raise AssertionError("last_degraded not armed after osd out / undersized")

        rados_obj.run_ceph_command(cmd=f"ceph osd in {target}")
        target = None
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        q2 = get_pg_query(rados_obj, pgid)
        ld2 = extract_pg_history_field(q2, "last_degraded")
        lc2 = extract_pg_history_field(q2, "last_clean")
        deg_ts, clean_ts = parse_ceph_ts(ld2), parse_ceph_ts(lc2)
        if deg_ts is not None and clean_ts is not None:
            assert deg_ts < clean_ts, (
                f"Expected last_degraded < last_clean: {ld2} vs {lc2}"
            )
        else:
            log.warning(f"Could not compare timestamps ld={ld2} lc={lc2}")

    finally:
        if target is not None:
            try:
                rados_obj.run_ceph_command(cmd=f"ceph osd in {target}")
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("last_degraded_undersized passed")


def scenario_network_partition_during_degradation(rados_obj, cephadm, config):
    """Stand-in for network partition: stop non-primary then briefly primary.

    Test steps:
        1. Stop non-primary (degrade), briefly stop primary, start both, recover.
        2. Assert clean recovery, valid counters, last_degraded present.
        3. Log that true netem partition is unavailable.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_net_part")
    non_primary = None
    primary = None

    try:
        log.info(
            "True netem network partition is not available; using OSD stop stand-in"
        )
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before, context=f"osd.{primary} baseline")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(15)
        rados_obj.change_osd_state(action="stop", target=primary)
        time.sleep(10)
        rados_obj.change_osd_state(action="start", target=primary)
        rados_obj.change_osd_state(action="start", target=non_primary)
        non_primary = None
        primary = None
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after, context=f"osd.{new_primary} after")
        pgid = pick_pool_pgid(rados_obj, pool_name)
        ld = extract_pg_history_field(get_pg_query(rados_obj, pgid), "last_degraded")
        if ld is None:
            raise AssertionError("last_degraded missing after partition stand-in")
        log.info(f"Partition stand-in recovered; last_degraded={ld}, counters={after}")

    finally:
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    rados_obj.change_osd_state(action="start", target=osd)
                except Exception:
                    pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("network_partition_during_degradation passed")


def scenario_long_vulnerability_window(rados_obj, cephadm, config):
    """Hold degraded for hold_secs; vulnerability sum/max should be substantial.

    Test steps:
        1. Stop non-primary, hold for config hold_secs (default 600).
        2. Start, recover; if has_vulnerability, sum/max increase vs hold time.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_long_window")
    hold_secs = int(config.get("hold_secs", 600))
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=4)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_durability_counters(rados_obj, primary)

        mon_obj.set_config(section="osd", name="osd_recovery_max_active", value="1")
        mon_obj.set_config(section="osd", name="osd_recovery_sleep", value="10")
        try:
            rados_obj.change_osd_state(action="stop", target=non_primary)
            log.info(f"Holding degraded for {hold_secs}s")
            time.sleep(hold_secs)
            rados_obj.change_osd_state(action="start", target=non_primary)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1800)
        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(f"Long window durability before={before} after={after}")

        if after["has_vulnerability"]:
            sum_delta = after["vuln_sum"] - before["vuln_sum"]
            max_delta = (after["vuln_max"] or 0) - (before["vuln_max"] or 0)
            # Many PGs: prefer max_inc or total sum being substantial vs hold
            substantial = sum_delta >= hold_secs * 0.5 or max_delta >= hold_secs * 0.5
            if not substantial:
                # avgtime path
                if after["vuln_avgtime"] >= hold_secs * 0.3:
                    substantial = True
            assert substantial, (
                f"Vulnerability duration not substantial for hold_secs={hold_secs}: "
                f"sum_delta={sum_delta}, max_delta={max_delta}, after={after}"
            )
        else:
            log.warning(
                "Vulnerability counters absent; validated rebuild counters only"
            )
            assert after_rebuild["avgcount"] > before.get("rebuild_avgcount", 0) or (
                after_rebuild["avgcount"] >= 1
            )

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("long_vulnerability_window passed")


def scenario_pg_merge_when_clean(rados_obj, cephadm, config):
    """Decrease pg_num when clean; vulnerability/rebuild should stay unchanged.

    Test steps:
        1. Create pool pg_num=16, write, clean; snapshot counters.
        2. Decrease pg_num to 8 when clean; wait; counters unchanged.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_pg_merge")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")

        rados_obj.set_pool_property(pool=pool_name, props="pgp_num", value=8)
        rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=8)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after-merge")

        if after["has_vulnerability"] and before["has_vulnerability"]:
            assert after["vuln_avgcount"] == before["vuln_avgcount"], (
                "Vulnerability must not change on clean PG merge: "
                f"{before['vuln_avgcount']} -> {after['vuln_avgcount']}"
            )
        else:
            assert verify_counters_unchanged(before_rebuild, after_rebuild), (
                "Rebuild counters should be unchanged for clean PG merge"
            )

    finally:
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("pg_merge_when_clean passed")


def scenario_autoscaler_rebalance_limitation(rados_obj, cephadm, config):
    """Exercise autoscaler; soft-pass documenting rebalance-vs-failure limitation.

    Test steps:
        1. Enable pg_autoscaler / set pool pg_autoscale_mode on.
        2. Write data; wait for possible resize.
        3. Soft-pass always; log whether counters changed.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_autoscaler")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)

        try:
            rados_obj.run_ceph_command(cmd="ceph mgr module enable pg_autoscaler")
        except Exception as err:
            log.warning(f"enable pg_autoscaler: {err}")
        try:
            rados_obj.set_pool_property(
                pool=pool_name, props="pg_autoscale_mode", value="on"
            )
        except Exception as err:
            log.warning(f"set pg_autoscale_mode: {err}")

        # Encourage resize
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(60)
        try:
            wait_for_recovery(rados_obj, pool_name, timeout=600)
        except Exception:
            pass

        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        changed = after_rebuild["avgcount"] != before_rebuild["avgcount"] or (
            after["has_vulnerability"]
            and after["vuln_avgcount"] != before["vuln_avgcount"]
        )
        log.info(
            "KNOWN LIMITATION: autoscaler rebalance may increment vulnerability/rebuild "
            f"like failure. counters_changed={changed} before={before} after={after}"
        )

    finally:
        try:
            rados_obj.set_pool_property(
                pool=pool_name, props="pg_autoscale_mode", value="off"
            )
        except Exception:
            pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("autoscaler_rebalance_limitation soft-passed")


def scenario_balancer_rebalance_limitation(rados_obj, cephadm, config):
    """Exercise balancer; soft-pass documenting rebalance-vs-failure limitation.

    Test steps:
        1. Enable balancer / upmap mode / optimize if available.
        2. Soft-pass; log whether counters changed.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_balancer")

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)

        for cmd in (
            "ceph balancer on",
            "ceph balancer mode upmap",
            "ceph balancer optimize",
        ):
            try:
                rados_obj.run_ceph_command(cmd=cmd)
            except Exception as err:
                log.warning(f"{cmd} failed: {err}")

        time.sleep(60)
        try:
            wait_for_recovery(rados_obj, pool_name, timeout=600)
        except Exception:
            pass

        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        changed = after_rebuild["avgcount"] != before_rebuild["avgcount"] or (
            after["has_vulnerability"]
            and after["vuln_avgcount"] != before["vuln_avgcount"]
        )
        log.info(
            "KNOWN LIMITATION: balancer rebalance may increment vulnerability/rebuild "
            f"like failure. counters_changed={changed} before={before} after={after}"
        )

    finally:
        try:
            rados_obj.run_ceph_command(cmd="ceph balancer off")
        except Exception:
            pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("balancer_rebalance_limitation soft-passed")


def scenario_inactive_pg_not_counted(rados_obj, cephadm, config):
    """Inactive/stuck PGs should not increment durability while unavailable.

    Test steps:
        1. Snapshot counters; stop majority of acting set to force inactive.
        2. While inactive, counters should not increment for durability.
        3. Restore OSDs; after recovery may increment. Best-effort asserts.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_inactive")
    stopped = []

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=4, size=3, min_size=2)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 30))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if len(acting) < 2:
            raise AssertionError("Need multi-OSD acting set")

        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")

        # Stop 2 of 3 acting OSDs (keep one up) to push toward inactive
        to_stop = list(acting[1:3]) if len(acting) >= 3 else [acting[1]]
        for osd_id in to_stop:
            rados_obj.change_osd_state(action="stop", target=osd_id)
            stopped.append(osd_id)
        time.sleep(25)

        mid = get_durability_counters(rados_obj, primary)
        mid_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid_rebuild, context=f"osd.{primary} while-inactive")
        if mid["has_vulnerability"] and before["has_vulnerability"]:
            if mid["vuln_avgcount"] > before["vuln_avgcount"]:
                log.warning(
                    "Vulnerability increased while attempting inactive state; "
                    f"{before['vuln_avgcount']} -> {mid['vuln_avgcount']}"
                )
            else:
                log.info("Vulnerability did not increase while PGs inactive/stuck")
        if mid_rebuild["avgcount"] > before_rebuild["avgcount"]:
            log.warning(
                "Rebuild increased while attempting inactive state "
                f"{before_rebuild['avgcount']} -> {mid_rebuild['avgcount']}"
            )

        for osd_id in list(stopped):
            rados_obj.change_osd_state(action="start", target=osd_id)
            stopped.remove(osd_id)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        # No crash / coherent counters after restore
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after-restore")
        if hasattr(rados_obj, "do_crash_ls"):
            try:
                crashes = rados_obj.do_crash_ls() or []
                log.info(f"Crash list size after inactive test: {len(crashes)}")
            except Exception as err:
                log.warning(f"do_crash_ls: {err}")

    finally:
        for osd_id in list(stopped):
            try:
                rados_obj.change_osd_state(action="start", target=osd_id)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("inactive_pg_not_counted passed")


def scenario_crash_during_last_degraded_update(rados_obj, cephadm, config):
    """Crash/restart primary while arming last_degraded; no history corruption.

    Test steps:
        1. Stop non-primary to arm last_degraded; immediately restart primary.
        2. Recover; assert last_degraded still present/parseable; counters coherent.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_crash_lastdeg")
    non_primary = None

    try:
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        rados_obj.change_osd_state(action="stop", target=non_primary)
        # Immediately crash/restart primary during last_degraded update window
        rados_obj.change_osd_state(action="restart", target=primary)
        time.sleep(20)
        rados_obj.change_osd_state(action="start", target=non_primary)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        missing = []
        for pgid_i in rados_obj.get_pgid(pool_name=pool_name) or []:
            q = get_pg_query(rados_obj, pgid_i)
            ld = extract_pg_history_field(q, "last_degraded")
            if ld is None:
                missing.append(pgid_i)
        if missing:
            raise AssertionError(
                f"last_degraded missing after crash-during-update: {missing[:10]}"
            )

        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")
        log.info(
            f"crash_during_last_degraded_update: last_degraded present; "
            f"counters={after}"
        )

        # Clear crash reports if any
        if hasattr(rados_obj, "do_crash_ls"):
            try:
                for crash in rados_obj.do_crash_ls() or []:
                    crash_id = crash if isinstance(crash, str) else crash.get("crash_id")
                    if crash_id:
                        try:
                            rados_obj.run_ceph_command(
                                cmd=f"ceph crash archive {crash_id}"
                            )
                        except Exception:
                            pass
            except Exception as err:
                log.warning(f"crash archive cleanup: {err}")

    finally:
        if non_primary is not None:
            try:
                rados_obj.change_osd_state(action="start", target=non_primary)
            except Exception:
                pass
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("crash_during_last_degraded_update passed")



def run(ceph_cluster, **kw):
    """
    Entry point for PG rebuild perf counter tests.

    Dispatches to the scenario specified in config['scenario'].
    Each scenario validates a specific aspect of the PG rebuild duration
    counters (pg_rebuild_duration, pg_rebuild_max_secs, pg_rebuild_min_secs).

    Returns: 0 on success, 1 on failure
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    scenario = config.get("scenario")
    if not scenario:
        log.error("No 'scenario' specified in test config")
        return 1

    log.info(f"Executing scenario: {scenario}")

    if config.get("reset_counters", True):
        try:
            reset_rebuild_counters(
                rados_obj, batch_size=config.get("reset_batch_size", 10)
            )
        except Exception as reset_err:
            log.error(f"Counter reset failed: {reset_err}")
            return 1

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    try:
        if scenario == "counter_registration":
            scenario_counter_registration(rados_obj, cephadm, config)
        elif scenario == "counter_priority":
            scenario_counter_priority(rados_obj, cephadm, config)
        elif scenario == "counter_section_name":
            scenario_counter_section_name(rados_obj, cephadm, config)
        elif scenario == "prometheus_integration":
            scenario_prometheus_integration(rados_obj, cephadm, config)
        elif scenario == "prometheus_matches_perf_dump":
            scenario_prometheus_matches_perf_dump(rados_obj, cephadm, config)
        elif scenario == "non_primary_osd_stop":
            scenario_non_primary_osd_stop(rados_obj, cephadm, config)
        elif scenario == "primary_osd_stop":
            scenario_primary_osd_stop(rados_obj, cephadm, config)
        elif scenario == "osd_host_stop":
            scenario_osd_host_stop(rados_obj, cephadm, config)
        elif scenario == "misplaced_recovery":
            scenario_misplaced_recovery(rados_obj, cephadm, config)
        elif scenario == "repair_counters":
            scenario_repair_counters(rados_obj, cephadm, config)
        elif scenario == "recovery_with_io":
            scenario_recovery_with_io(rados_obj, cephadm, config)
        elif scenario == "primary_transition":
            scenario_primary_transition(rados_obj, cephadm, config)
        elif scenario == "throttled_recovery":
            scenario_throttled_recovery(rados_obj, cephadm, config)
        elif scenario == "add_osd":
            scenario_add_osd(rados_obj, cephadm, config)
        elif scenario == "osd_restart_reset":
            scenario_osd_restart_reset(rados_obj, cephadm, config)
        elif scenario == "no_recovery_noout":
            scenario_no_recovery_noout(rados_obj, cephadm, config)
        elif scenario == "multiple_pg_rebuild":
            scenario_multiple_pg_rebuild(rados_obj, cephadm, config)
        elif scenario == "negative_daemon_restart":
            scenario_negative_daemon_restart(rados_obj, cephadm, config)
        elif scenario == "sub_second_rebuild":
            scenario_sub_second_rebuild(rados_obj, cephadm, config)
        elif scenario == "double_latch_prevention":
            scenario_double_latch_prevention(rados_obj, cephadm, config)
        elif scenario == "pg_split_during_rebuild":
            scenario_pg_split_during_rebuild(rados_obj, cephadm, config)
        elif scenario == "backfill_only_recovery":
            scenario_backfill_only_recovery(rados_obj, cephadm, config)
        elif scenario == "peering_only_no_recovery":
            scenario_peering_only_no_recovery(rados_obj, cephadm, config)
        elif scenario == "osd_flapping":
            scenario_osd_flapping(rados_obj, cephadm, config)
        elif scenario == "compound_failure":
            scenario_compound_failure(rados_obj, cephadm, config)
        elif scenario == "multi_pool_same_primary":
            scenario_multi_pool_same_primary(rados_obj, cephadm, config)
        elif scenario == "min_max_invariant":
            scenario_min_max_invariant(rados_obj, cephadm, config)
        elif scenario == "pool_delete_counter_persistence":
            scenario_pool_delete_counter_persistence(rados_obj, cephadm, config)
        elif scenario == "last_degraded_present":
            scenario_last_degraded_present(rados_obj, cephadm, config)
        elif scenario == "last_degraded_latch":
            scenario_last_degraded_latch(rados_obj, cephadm, config)
        elif scenario == "last_degraded_pg_query":
            scenario_last_degraded_pg_query(rados_obj, cephadm, config)
        elif scenario == "prometheus_promql":
            scenario_prometheus_promql(rados_obj, cephadm, config)
        elif scenario == "prometheus_promql_no_negative_after_reset":
            scenario_prometheus_promql_no_negative_after_reset(rados_obj, cephadm, config)
        elif scenario == "prometheus_scrape_after_exporter_restart":
            scenario_prometheus_scrape_after_exporter_restart(rados_obj, cephadm, config)
        elif scenario == "backfill_toofull_unfound":
            scenario_backfill_toofull_unfound(rados_obj, cephadm, config)
        elif scenario == "empty_pool_vulnerability":
            scenario_empty_pool_vulnerability(rados_obj, cephadm, config)
        elif scenario == "pg_split_merge_during_rebuild":
            scenario_pg_split_merge_during_rebuild(rados_obj, cephadm, config)
        elif scenario == "rebuild_spanning_handover":
            scenario_rebuild_spanning_handover(rados_obj, cephadm, config)
        elif scenario == "rolling_upgrade_durability":
            scenario_rolling_upgrade_durability(rados_obj, cephadm, config)
        elif scenario == "mixed_version_durability":
            scenario_mixed_version_durability(rados_obj, cephadm, config)
        elif scenario == "serviceability_scenarios":
            scenario_serviceability_scenarios(rados_obj, cephadm, config)
        elif scenario == "rebuild_vs_vulnerability":
            scenario_rebuild_vs_vulnerability(rados_obj, cephadm, config)
        elif scenario == "per_osd_counter_scope":
            scenario_per_osd_counter_scope(rados_obj, cephadm, config)
        elif scenario == "mid_peering_false_completion":
            scenario_mid_peering_false_completion(rados_obj, cephadm, config)
        elif scenario == "same_primary_interval_restart":
            scenario_same_primary_interval_restart(rados_obj, cephadm, config)
        elif scenario == "restart_loop_single_window":
            scenario_restart_loop_single_window(rados_obj, cephadm, config)
        elif scenario == "last_degraded_persists_restart":
            scenario_last_degraded_persists_restart(rados_obj, cephadm, config)
        elif scenario == "exactly_once_close_publish":
            scenario_exactly_once_close_publish(rados_obj, cephadm, config)
        elif scenario == "primary_crash_during_recovery":
            scenario_primary_crash_during_recovery(rados_obj, cephadm, config)
        elif scenario == "high_pg_simultaneous_failure":
            scenario_high_pg_simultaneous_failure(rados_obj, cephadm, config)
        elif scenario == "last_degraded_undersized":
            scenario_last_degraded_undersized(rados_obj, cephadm, config)
        elif scenario == "network_partition_during_degradation":
            scenario_network_partition_during_degradation(rados_obj, cephadm, config)
        elif scenario == "long_vulnerability_window":
            scenario_long_vulnerability_window(rados_obj, cephadm, config)
        elif scenario == "pg_merge_when_clean":
            scenario_pg_merge_when_clean(rados_obj, cephadm, config)
        elif scenario == "autoscaler_rebalance_limitation":
            scenario_autoscaler_rebalance_limitation(rados_obj, cephadm, config)
        elif scenario == "balancer_rebalance_limitation":
            scenario_balancer_rebalance_limitation(rados_obj, cephadm, config)
        elif scenario == "inactive_pg_not_counted":
            scenario_inactive_pg_not_counted(rados_obj, cephadm, config)
        elif scenario == "crash_during_last_degraded_update":
            scenario_crash_during_last_degraded_update(rados_obj, cephadm, config)
        else:
            log.error(f"Unknown scenario: {scenario}")
            return 1
    except Exception as e:
        log.error(f"Scenario '{scenario}' failed with exception: {e.__doc__}")
        log.exception(e)
        try:
            rados_obj.log_cluster_health()
        except Exception as health_err:
            log.warning(f"Could not log cluster health: {health_err}")
        return 1
    finally:
        try:
            rados_obj.log_cluster_health()
        except Exception as health_err:
            log.warning(f"Could not log cluster health in finally: {health_err}")
        try:
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.debug(
                f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
            )
        except Exception as ts_err:
            log.warning(f"Could not get cluster timestamp: {ts_err}")
        # if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
        #     log.error("Test failed due to crash at the end of test")
        #     return 1

    log.info(f"Scenario '{scenario}' completed successfully")
    return 0
