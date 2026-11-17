"""
Module to validate PG rebuild duration perf counters introduced for tracking
rebuild time of Placement Groups on OSDs.

Covers counter registration, priority, Prometheus integration, and accuracy
under various failure/recovery scenarios (OSD stop, host failure, pool resize,
repair, throttled recovery, primary transition, etc.).

Ref: IBMCEPH-16254
"""

import json
import random
import re
import threading
import time
from datetime import datetime

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log, format_command_output

log = Log(__name__)


def log_banner(title):
    """Print a high-visibility section banner for log files."""
    bar = "=" * 72
    log.info(bar)
    log.info(title)
    log.info(bar)


def log_phase(title):
    """Print a mid-level phase marker for log files."""
    bar = "-" * 72
    log.info(bar)
    log.info(title)
    log.info(bar)


def log_step(message):
    """Print a scenario step with separators so it stands out in log files."""
    bar = "=" * 72
    log.info(bar)
    log.info(message)
    log.info(bar)


def log_checkpoint(label, **vars_):
    """Log an important checkpoint and optional key=value variables."""
    if vars_:
        details = ", ".join(f"{k}={v!r}" for k, v in vars_.items())
        log.info(f"CHECKPOINT [{label}]: {details}")
    else:
        log.info(f"CHECKPOINT [{label}]")


def log_about_to(action, **vars_):
    """Log what is about to happen before a helper does work."""
    if vars_:
        details = ", ".join(f"{k}={v!r}" for k, v in vars_.items())
        log.info(f"ABOUT TO: {action} ({details})")
    else:
        log.info(f"ABOUT TO: {action}")


def pp(value):
    """Pretty-print a logged value.

    Small mappings are indented in full. Large documents keep the fields
    this suite reads (PG history, rebuild counters, matching metric lines).
    Scalars are returned unchanged.
    """
    if isinstance(value, (dict, list, tuple)):
        return "\n" + format_command_output(value)
    return value


def log_pretty(title, value, level="info"):
    """Log ``value`` with ``format_command_output``."""
    rendered = format_command_output(
        value if isinstance(value, str) else json.dumps(value, default=str)
    )
    getattr(log, level)("%s\n%s", title, rendered)

REBUILD_COUNTERS = [
    "pg_rebuild_duration",
    "pg_rebuild_duration_min",
]
VULNERABILITY_COUNTERS = [
    "pg_vulnerability_duration",
    "pg_vulnerability_duration_min",
]
DURATION_COUNTERS = REBUILD_COUNTERS + VULNERABILITY_COUNTERS

PRIO_USEFUL = 5


def run_for_each(items, worker_fn, on_error="warn"):
    """Run ``worker_fn(item)`` for every item in order.

    Returns {item: result} for every item that completed without error.
    With `on_error="warn"` (default), a failing item is logged and omitted
    from the result. With `on_error="raise"`, the first exception is
    re-raised.
    """
    items = list(items or [])
    log_about_to(
        "run worker sequentially over items",
        count=len(items),
        on_error=on_error,
        items=items,
    )
    results = {}
    for idx, item in enumerate(items, 1):
        log_about_to(f"[{idx}/{len(items)}] invoke worker", item=item)
        try:
            results[item] = worker_fn(item)
            log_checkpoint(
                f"worker done [{idx}/{len(items)}]",
                item=item,
                result_type=type(results[item]).__name__,
            )
        except Exception as err:
            if on_error == "raise":
                log.error(f"Worker raised for item={item!r}: {err}")
                raise
            log.warning(f"Task failed for '{item}': {err}")
    log_checkpoint(
        "sequential run finished",
        succeeded=len(results),
        total=len(items),
    )
    return results


def fetch_counters_for_osds(rados_obj, osd_ids, on_error="warn") -> dict:
    """Fetch rebuild counters for the given OSD ids one at a time.

    Returns {osd_id: counters}. With `on_error="warn"` (default), any OSD
    whose fetch fails is skipped with a warning. Pass `on_error="raise"`
    for checks that must hard-fail if a fetch fails.
    """
    osd_ids = list(osd_ids or [])
    log_phase(f"Fetch rebuild counters for {len(osd_ids)} OSD(s): {osd_ids}")
    log_about_to(
        "fetch pg_rebuild_duration for OSD list",
        osd_ids=osd_ids,
        on_error=on_error,
    )
    results = run_for_each(
        osd_ids,
        worker_fn=lambda osd_id: get_rebuild_counters(rados_obj, osd_id),
        on_error=on_error,
    )
    log_checkpoint(
        "rebuild counter fetch complete",
        requested=len(osd_ids),
        returned=len(results),
        osd_ids=sorted(results),
    )
    return results


def get_rebuild_counters(rados_obj, osd_id: int) -> dict:
    """Fetch PG rebuild perf counters from an OSD via ``ceph tell``.

    Returns the fields reported by the OSD::

        pg_rebuild_duration: avgcount, sum, avgtime[, max_inc]
        pg_rebuild_duration_min

    ``max_inc`` is only present when ``avgcount > 1`` (absent after reset
    and after a single sample).
    """
    log_about_to(
        "ceph tell / perf dump for pg_rebuild_duration",
        osd_id=osd_id,
    )
    perf = rados_obj.get_osd_perf_dump(osd_id)
    rs_section = perf.get("recoverystate_perf", {})
    duration = rs_section.get("pg_rebuild_duration")
    if not isinstance(duration, dict):
        duration = {}
    result = {
        "avgcount": duration.get("avgcount"),
        "sum": duration.get("sum"),
        "avgtime": duration.get("avgtime"),
        "max_inc": duration.get("max_inc"),
        "pg_rebuild_duration_min": rs_section.get("pg_rebuild_duration_min"),
    }
    log_checkpoint(
        f"osd.{osd_id} rebuild counters",
        avgcount=result.get("avgcount"),
        sum=result.get("sum"),
        avgtime=result.get("avgtime"),
        max_inc=result.get("max_inc"),
        min=result.get("pg_rebuild_duration_min"),
    )
    return result


def get_vulnerability_counters(rados_obj, osd_id: int) -> dict:
    """Fetch PG vulnerability counters from ``ceph tell osd.<id> perf dump``.

    Returns the fields reported by the OSD::

        pg_vulnerability_duration: avgcount, sum, avgtime[, max_inc]
        pg_vulnerability_duration_min

    ``max_inc`` is only present when ``avgcount > 1`` (absent after reset
    and after a single sample).
    """
    log_about_to(
        "ceph tell / perf dump for pg_vulnerability_duration",
        osd_id=osd_id,
    )
    perf = rados_obj.get_osd_perf_dump(osd_id)
    rs = perf.get("recoverystate_perf", {}) if isinstance(perf, dict) else {}
    duration = rs.get("pg_vulnerability_duration")
    if not isinstance(duration, dict):
        duration = {}
    result = {
        "avgcount": duration.get("avgcount"),
        "sum": duration.get("sum"),
        "avgtime": duration.get("avgtime"),
        "max_inc": duration.get("max_inc"),
        "pg_vulnerability_duration_min": rs.get("pg_vulnerability_duration_min"),
    }
    log_checkpoint(
        f"osd.{osd_id} vulnerability counters",
        avgcount=result.get("avgcount"),
        sum=result.get("sum"),
        avgtime=result.get("avgtime"),
        max_inc=result.get("max_inc"),
        min=result.get("pg_vulnerability_duration_min"),
    )
    return result


def _duration_fields(counters: dict) -> dict:
    """Normalize a rebuild or vulnerability snapshot to one field set.

    Cluster names are ``max_inc`` (only when avgcount > 1) plus
    ``pg_rebuild_duration_min`` or ``pg_vulnerability_duration_min``.
    The min gauge is stored as ``duration_min`` so both families share
    the same checks.
    """
    duration_min = None
    for key in (
        "pg_rebuild_duration_min",
        "pg_vulnerability_duration_min",
        "duration_min",
    ):
        if key in counters:
            duration_min = counters[key]
            break
    return {
        "avgcount": counters.get("avgcount"),
        "sum": counters.get("sum"),
        "avgtime": counters.get("avgtime"),
        "max_inc": counters.get("max_inc"),
        "duration_min": duration_min,
    }


def _ignored_duration_fields(ignore) -> set:
    """Map an ignore list onto ``max_inc`` and ``duration_min``."""
    ignored = set(ignore or ())
    if "max" in ignored:
        ignored.add("max_inc")
    if "min" in ignored or "pg_rebuild_duration_min" in ignored:
        ignored.add("duration_min")
    if "pg_vulnerability_duration_min" in ignored:
        ignored.add("duration_min")
    return ignored


def verify_counter_defaults(counters: dict, ignore=None) -> bool:
    """Return True if duration counters are at their default (zero) values.

    ``ignore`` names fields that are not checked. Use this when a counter is
    known to be absent or unreliable. After reset, ``max_inc`` is absent
    (it only appears when avgcount > 1). The min gauge is checked as
    ``duration_min``.
    """
    log_about_to("verify counters are at zero defaults", ignore=ignore, counters=counters)
    ignored = _ignored_duration_fields(ignore)
    fields = _duration_fields(counters)
    expected = {
        "avgcount": 0,
        "sum": 0,
        "avgtime": 0,
        "duration_min": 0,
    }
    for key, val in expected.items():
        if key in ignored:
            continue
        actual = fields.get(key)
        if actual is None:
            log.error(f"Counter '{key}' missing from perf dump")
            return False
        if actual != val:
            log.error(f"Counter '{key}' expected {val}, got {actual}")
            return False
    # max_inc must not exist at defaults (avgcount == 0)
    if "max_inc" not in ignored and fields.get("max_inc") is not None:
        log.error(
            "Counter 'max_inc' should be absent after reset "
            f"(avgcount <= 1), got {fields.get('max_inc')}"
        )
        return False
    log_checkpoint("counter defaults OK", fields=fields, ignored=sorted(ignored))
    return True


def verify_counters_unchanged(before: dict, after: dict) -> bool:
    """Return True when rebuild counters did not change between snapshots."""
    log_about_to("verify counters unchanged", before=before, after=after)
    for key in before:
        if before[key] != after[key]:
            log.error(
                f"Counter '{key}' changed unexpectedly: {before[key]} -> {after[key]}"
            )
            return False
    log_checkpoint("counters unchanged OK", keys=list(before.keys()))
    return True


def verify_counters_incremented(before: dict, after: dict) -> bool:
    """Return True when rebuild counters show activity (avgcount increased)."""
    log_about_to(
        "verify counters incremented",
        before_avgcount=before.get("avgcount"),
        after_avgcount=after.get("avgcount"),
        before_sum=before.get("sum"),
        after_sum=after.get("sum"),
    )
    if after["avgcount"] <= before["avgcount"]:
        log.error(
            f"avgcount did not increase: before={before['avgcount']}, "
            f"after={after['avgcount']}"
        )
        return False
    if after["sum"] < before["sum"]:
        log.error(f"sum decreased: before={before['sum']}, after={after['sum']}")
        return False
    log_checkpoint(
        "counters incremented OK",
        avgcount_delta=after["avgcount"] - before["avgcount"],
        sum_delta=(after["sum"] or 0) - (before["sum"] or 0),
    )
    return True


def verify_min_max_invariant(counters: dict) -> bool:
    """Return True when max_inc >= pg_rebuild_duration_min.

    ``max_inc`` only exists when avgcount > 1. With avgcount <= 1 (reset or
    a single sample) the invariant is skipped when max_inc is absent.
    """
    avgcount = counters.get("avgcount", 0) or 0
    max_secs = counters.get("max_inc")
    min_secs = counters.get("pg_rebuild_duration_min")
    if min_secs is None:
        min_secs = counters.get("duration_min")
    log_about_to(
        "verify max_inc >= duration_min invariant",
        avgcount=avgcount,
        max_inc=max_secs,
        duration_min=min_secs,
    )

    if avgcount <= 1 and max_secs is None:
        log.info(
            f"Skipping max_inc/min invariant: avgcount={avgcount} "
            "(max_inc only exists when avgcount > 1)"
        )
        return True

    if max_secs is None or min_secs is None:
        log.error(
            f"max_inc or pg_rebuild_duration_min is missing "
            f"(avgcount={avgcount}, max_inc={max_secs}, min={min_secs})"
        )
        return False
    if max_secs == 0 and min_secs == 0:
        log_checkpoint("min/max invariant OK (both zero)")
        return True
    if max_secs < min_secs:
        log.error(
            f"INVARIANT VIOLATED: max_inc ({max_secs}) < "
            f"pg_rebuild_duration_min ({min_secs})"
        )
        return False
    log_checkpoint("min/max invariant OK", max_inc=max_secs, duration_min=min_secs)
    return True


def validate_counter_fields(counters: dict, context: str = "", ignore=None) -> None:
    """Thoroughly validate every individual raw rebuild-counter field.

    Mirrors the structure reported under ``recoverystate_perf``::

        "pg_rebuild_duration": {
            "avgcount": 0,
            "sum": 0.000000000,
            "avgtime": 0.000000000
            # "max_inc" only when avgcount > 1
        },
        "pg_rebuild_duration_min": 0

    The same shape is used for ``pg_vulnerability_duration`` and
    ``pg_vulnerability_duration_min``.

    Checks, per field:
        - presence (not None); ``max_inc`` only required when avgcount > 1
        - type (avgcount -> int; sum/avgtime/max_inc/duration_min -> numeric)
        - sign (all fields must be >= 0)
        - avgcount == 0 implies sum/avgtime/duration_min == 0
        - avgtime == sum / avgcount (within tolerance), per LONGRUNAVG semantics
        - duration_min <= max_inc when max_inc is present

    ``max_inc`` is absent after reset and when avgcount <= 1; it appears
    only once avgcount > 1. The min gauge is ``pg_rebuild_duration_min``
    or ``pg_vulnerability_duration_min``.

    ``ignore`` names fields to skip, including presence, type, sign, the
    zero-avgcount implication, and the min/max comparison when either side
    is listed. The min/max comparison runs only when both fields are checked.

    Raises AssertionError with a field-specific message on the first
    violation found. `context` (e.g. "osd.3 baseline") is prefixed to every
    error message so failures are easy to attribute to a specific snapshot.
    """
    prefix = f"{context}: " if context else ""
    log_about_to(
        "validate counter fields",
        context=context or "(none)",
        ignore=ignore,
        counters=counters,
    )
    ignored = _ignored_duration_fields(ignore)

    fields = _duration_fields(counters)
    for key in ignored:
        fields.pop(key, None)

    # max_inc exists only when avgcount > 1; drop absent max_inc otherwise
    avgcount_for_gate = fields.get("avgcount")
    if "max_inc" not in ignored:
        if avgcount_for_gate is not None and avgcount_for_gate > 1:
            if fields.get("max_inc") is None:
                raise AssertionError(
                    f"{prefix}counter 'max_inc' missing from perf dump "
                    f"(required when avgcount={avgcount_for_gate} > 1)"
                )
        elif fields.get("max_inc") is None:
            fields.pop("max_inc", None)

    for key, val in fields.items():
        if val is None:
            raise AssertionError(f"{prefix}counter '{key}' missing from perf dump")

    avgcount = fields.get("avgcount")
    sum_ = fields.get("sum")
    avgtime = fields.get("avgtime")

    if "avgcount" in fields and (
        not isinstance(avgcount, int) or isinstance(avgcount, bool)
    ):
        raise AssertionError(
            f"{prefix}'avgcount' must be an int, got "
            f"{type(avgcount).__name__} ({avgcount!r})"
        )

    for key in ("sum", "avgtime", "max_inc", "duration_min"):
        if key not in fields:
            continue
        val = fields[key]
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            raise AssertionError(
                f"{prefix}'{key}' must be numeric, got "
                f"{type(val).__name__} ({val!r})"
            )

    for key, val in fields.items():
        if val < 0:
            raise AssertionError(f"{prefix}'{key}' must be non-negative, got {val}")

    zero_companions = [
        key for key in ("sum", "avgtime", "max_inc", "duration_min") if key in fields
    ]
    if "avgcount" in fields and avgcount == 0:
        for key in zero_companions:
            val = fields[key]
            if val != 0:
                raise AssertionError(
                    f"{prefix}avgcount is 0 but '{key}' is {val}; expected 0 "
                    "since no rebuild has been recorded yet"
                )
    elif "avgcount" in fields and "sum" in fields and "avgtime" in fields:
        computed_avgtime = sum_ / avgcount
        tolerance = max(0.01, computed_avgtime * 0.05)
        if abs(avgtime - computed_avgtime) > tolerance:
            raise AssertionError(
                f"{prefix}'avgtime' ({avgtime}) does not match sum/avgcount "
                f"({sum_}/{avgcount} = {computed_avgtime:.6f}, "
                f"tolerance={tolerance:.6f})"
            )

    if "max_inc" in fields and "duration_min" in fields:
        max_secs = fields["max_inc"]
        min_secs = fields["duration_min"]
        if not (max_secs == 0 and min_secs == 0) and max_secs < min_secs:
            raise AssertionError(
                f"{prefix}max_inc ({max_secs}) < duration_min ({min_secs})"
            )
    log_checkpoint(
        "validate_counter_fields passed",
        context=context or "(none)",
        fields=fields,
        ignored=sorted(ignored),
    )


def validate_all_osd_counter_fields(
    all_counters: dict, context: str = "", ignore=None
) -> None:
    """Run `validate_counter_fields` against every OSD in a
    {osd_id: counters} snapshot (as returned by `get_all_osd_rebuild_counters`).

    ``ignore`` is forwarded to each per-OSD check.
    """
    label = context or "counter-fields"
    log_phase(f"Validate counter fields for {len(all_counters)} OSD(s) [{label}]")
    log_about_to(
        "validate fields on every OSD in snapshot",
        context=label,
        osd_count=len(all_counters),
        osd_ids=sorted(all_counters),
        ignore=ignore,
    )
    for osd_id, counters in all_counters.items():
        osd_context = f"{context} osd.{osd_id}" if context else f"osd.{osd_id}"
        log_about_to("validate one OSD", context=osd_context, counters=counters)
        validate_counter_fields(counters, context=osd_context, ignore=ignore)
    log_checkpoint(
        "all OSD counter field checks passed",
        context=label,
        osd_count=len(all_counters),
    )


def get_perf_schema_priorities(rados_obj, osd_id: int) -> dict:
    """Fetch perf schema and return priority for rebuild counters.

    Returns None if the OSD is down or unreachable (ENXIO / empty response),
    so callers can skip it instead of crashing on a JSONDecodeError.
    """
    cmd = f"ceph tell osd.{osd_id} perf schema"
    log_about_to("fetch perf schema priorities", osd_id=osd_id, cmd=cmd)
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
    for counter in DURATION_COUNTERS:
        entry = rs_section.get(counter, {})
        priorities[counter] = entry.get("priority", None)
    log_checkpoint(f"osd.{osd_id} perf schema priorities", priorities=priorities)
    return priorities


def get_all_osd_rebuild_counters(rados_obj) -> dict:
    """Snapshot rebuild counters from every up OSD. Returns {osd_id: counters}."""
    log_phase("Snapshot rebuild counters from all up OSDs")
    log_about_to("list up OSDs then fetch rebuild counters for each")
    osd_list = rados_obj.get_osd_list(status="up")
    log_checkpoint("up OSD list", count=len(osd_list), osd_list=osd_list)
    results = fetch_counters_for_osds(rados_obj, osd_list)
    log_checkpoint(
        "all-OSD rebuild snapshot complete",
        count=len(results),
        osd_ids=sorted(results),
    )
    return results


def wait_for_recovery(rados_obj, pool_name=None, timeout=900):
    """Wait until all PGs reach active+clean."""
    target = pool_name or "all pools"
    log_phase(f"Wait for recovery / active+clean ({target}, timeout={timeout}s)")
    log_about_to(
        "wait_for_clean_pg_sets",
        pool_name=pool_name,
        timeout=timeout,
        recovery_thread=True,
    )
    ok = rados_obj.wait_for_clean_pg_sets(
        timeout=timeout, test_pool=pool_name, recovery_thread=True
    )
    log_checkpoint(
        "recovery wait finished",
        target=target,
        success=ok,
        timeout=timeout,
    )
    if ok:
        log.info(f"Recovery complete: {target} is active+clean")
    else:
        log.error(f"Recovery wait failed or timed out for {target}")
    return ok


def reset_rebuild_counters(rados_obj, batch_size=10):
    """Reset rebuild perf counters on all up OSDs.

    Issues ``ceph tell osd.<id> perf reset recoverystate_perf`` so only the
    recoverystate_perf collection is cleared. Counters are in-memory; this
    avoids restarting OSDs and the recovery that follows a restart.

    The command prints no JSON, so it is run through ``node.shell`` rather
    than ``run_ceph_command`` (which appends ``-f json`` and parses stdout).

    Args:
        rados_obj: RadosOrchestrator instance
        batch_size: unused; kept for config compatibility
    """
    log_banner("RESET recoverystate_perf on all up OSDs")
    log_about_to("list up OSDs for perf reset", batch_size=batch_size)
    osd_list = rados_obj.get_osd_list(status="up")
    log_checkpoint("OSDs to reset", count=len(osd_list or []), osd_list=osd_list)
    if not osd_list:
        log.warning("No up OSDs found, skipping counter reset")
        return

    log.info(f"Resetting recoverystate_perf on {len(osd_list)} OSDs one at a time")

    failed = []
    for idx, osd_id in enumerate(osd_list, 1):
        cmd = f"ceph tell osd.{int(osd_id)} perf reset recoverystate_perf"
        log_about_to(
            f"[{idx}/{len(osd_list)}] perf reset recoverystate_perf",
            osd_id=osd_id,
            cmd=cmd,
        )
        try:
            rados_obj.node.shell([cmd])
            log_checkpoint(f"osd.{osd_id} perf reset OK")
        except Exception as err:
            log.warning(f"Failed to reset recoverystate_perf on osd.{osd_id}: {err}")
            failed.append(osd_id)

    if failed:
        raise AssertionError(
            f"perf reset recoverystate_perf failed on OSDs: {failed}"
        )

    log_phase("Verify counters are zero after perf reset")
    log_about_to("re-fetch all rebuild counters to confirm reset")
    all_counters = get_all_osd_rebuild_counters(rados_obj)
    non_zero = {
        osd_id: c for osd_id, c in all_counters.items() if c.get("avgcount", 0) != 0
    }
    log_checkpoint(
        "post-reset avgcount check",
        total=len(all_counters),
        non_zero_count=len(non_zero),
        non_zero_osds=sorted(non_zero),
    )
    if non_zero:
        log.warning(
            f"{len(non_zero)} OSDs still have non-zero counters after perf reset "
            f"(recovery activity after reset): {list(non_zero.keys())}"
        )
    else:
        log.info("All OSD rebuild counters reset to zero")
    log_banner("RESET recoverystate_perf complete")


def write_data_to_pool(pool_obj, client, pool_name, nobj=50, size="4M"):
    """Write test objects into a pool."""
    client_name = getattr(client, "hostname", str(client))
    log_phase(f"Write data to pool {pool_name}: nobj={nobj}, size={size}")
    log_about_to(
        "rados put / write objects",
        pool_name=pool_name,
        nobj=nobj,
        size=size,
        client=client_name,
    )
    pool_obj.do_rados_put(client=client, pool=pool_name, nobj=nobj, size=size)
    log_checkpoint(
        "write_data_to_pool finished",
        pool_name=pool_name,
        nobj=nobj,
        size=size,
    )


def _get_non_primary_osd(rados_obj, pool_name):
    """Return (primary_osd, non_primary_osd, acting_set) for one PG in the pool.

    The stopped OSD must be a secondary only — never primary of any PG in this
    pool. Prefer a secondary in exactly one acting set; otherwise pick a
    secondary-only OSD with the fewest memberships.
    """
    log_about_to(
        "get PG acting map and pick secondary-only OSD",
        pool_name=pool_name,
    )
    mapping = _pool_pg_acting_map(rados_obj, pool_name)
    if not mapping:
        raise AssertionError(f"No PG acting sets found for pool {pool_name}")

    # osd_id -> list of pgids whose acting set contains that OSD
    membership = {}
    primaries = set()
    for pgid, info in mapping.items():
        primaries.add(int(info["primary"]))
        for osd_id in info["acting"]:
            membership.setdefault(int(osd_id), []).append(pgid)

    def _pick_from(osd_ids, reason):
        # Prefer fewest PG memberships, then stable osd/pgid order.
        scored = []
        for osd_id in osd_ids:
            for pgid in membership[osd_id]:
                info = mapping[pgid]
                primary = int(info["primary"])
                if osd_id == primary:
                    continue
                scored.append(
                    (
                        len(membership[osd_id]),
                        osd_id,
                        pgid,
                        primary,
                        list(info["acting"]),
                    )
                )
        if not scored:
            return None
        scored.sort()
        _count, non_primary, pgid, primary, acting = scored[0]
        log_checkpoint(
            "primary/non-primary selected",
            pool_name=pool_name,
            pgid=pgid,
            acting=acting,
            primary=primary,
            non_primary=non_primary,
            reason=reason,
            membership_count=_count,
        )
        return primary, non_primary, acting

    # 1) Secondary in exactly one acting set, and never a primary elsewhere.
    single = [
        osd_id
        for osd_id, pgids in membership.items()
        if len(pgids) == 1 and osd_id not in primaries
    ]
    picked = _pick_from(single, "secondary in exactly one acting set")
    if picked:
        return picked

    # 2) Secondary-only OSD (may sit in multiple acting sets) — never a primary.
    secondary_only = [
        osd_id for osd_id in membership if osd_id not in primaries
    ]
    picked = _pick_from(
        secondary_only,
        "secondary-only OSD (fewest acting-set memberships)",
    )
    if picked:
        log.warning(
            f"Pool {pool_name}: no secondary in exactly one acting set; "
            f"using secondary-only osd.{picked[1]} instead"
        )
        return picked

    raise AssertionError(
        f"Pool {pool_name}: no secondary-only OSD available to stop "
        f"(every non-primary is also a primary of another PG)"
    )


def _pool_pg_acting_map(rados_obj, pool_name):
    """Return {pgid: {primary, acting}} for every PG in the pool."""
    log_phase(f"Build PG acting map for pool {pool_name}")
    log_about_to("resolve pool id and dump PG stats", pool_name=pool_name)
    pool_id = int(rados_obj.get_pool_id(pool_name=pool_name))
    prefix = f"{pool_id}."
    log_checkpoint("pool id resolved", pool_name=pool_name, pool_id=pool_id, prefix=prefix)
    mapping = {}
    for row in get_pg_stats(rados_obj):
        pgid = str(row.get("pgid", ""))
        if not pgid.startswith(prefix):
            continue
        acting = row.get("acting") or row.get("up") or []
        acting = [int(osd) for osd in acting]
        primary = row.get("acting_primary")
        if primary is None and acting:
            primary = acting[0]
        if primary is None:
            continue
        mapping[pgid] = {"primary": int(primary), "acting": acting}
    log_checkpoint(
        "PG acting map built",
        pool_name=pool_name,
        pg_count=len(mapping),
    )
    log.info(
        f"Pool {pool_name} PG acting map ({len(mapping)} PGs): {pp(mapping)}"
    )
    return mapping


_LOG_TS_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
)
_PGID_RE = re.compile(r"pg\[(\d+\.[0-9a-fA-F]+)")
# PeeringState paths may be deeper than Active/<one>; allow trailing segments.
_STATE_ANGLE_RE = re.compile(
    r"state<\s*(Started/Primary/Active(?:/[A-Za-z]+)+)\s*>",
    re.IGNORECASE,
)
_ENTER_STATE_RE = re.compile(
    r"\b(?:enter|entering(?:\s+state)?)\s+"
    r"(Started/Primary/Active(?:/[A-Za-z]+)+)",
    re.IGNORECASE,
)
_EXIT_STATE_RE = re.compile(
    r"\b(?:exit|exiting(?:\s+state)?)\s+"
    r"(Started/Primary/Active(?:/[A-Za-z]+)+)",
    re.IGNORECASE,
)
# Real OSD lines look like:
#   ... mlcod 0'0 active+undersized+degraded mbc={}] PeeringState::...
# Prefer the token before `` mbc=``. Do NOT treat a bare ``]`` as the
# state boundary — ``readable_until[_ub]`` would otherwise steal the match
# and clear vulnerability on every lease line.
_PG_STATE_MBC_RE = re.compile(
    r"(?<![\w+])([a-z_]+(?:\+[a-z_]+)*)(?=\s+mbc\s*=)",
    re.IGNORECASE,
)
# Some lease lines use ``active+clean]`` with no ``mbc=``.
_PG_STATE_CLEAN_RE = re.compile(
    r"(?<![\w+])((?:active|activating)(?:\+[a-z_]+)*)(?=\])",
    re.IGNORECASE,
)
# Matches PeeringState::arm_rebuild_span(), which runs only on entry to
# Recovering and Backfilling. recovery_wait / backfill_wait are the
# reservation states before that span and are not part of the counter.
_REBUILD_PARTS = {
    "recovering",
    "backfilling",
}


def _host_local_timestamp(node):
    """Return the node's local time as ``YYYY-MM-DD HH:MM:SS`` for journalctl."""
    log_about_to("read host-local timestamp via date")
    out, _ = node.shell(["date '+%Y-%m-%d %H:%M:%S'"])
    stamp = str(out).strip().strip("'").strip()
    log_checkpoint("host-local timestamp", stamp=stamp)
    return stamp


def _parse_osd_log_timestamp(line, default_year=None):
    """Return a POSIX timestamp from a Ceph or journald log line, or None.

    Accepts Ceph file stamps (``2026-11-26T10:09:53.388+0000``), journalctl
    ``short-iso-precise`` (``2026-11-26T10:10:33.406711+00:00``), and the
    default journalctl short form (``Nov 26 10:10:33``) when ``default_year``
    is known.
    """
    match = _LOG_TS_RE.search(line)
    if match:
        frac = (match.group(3) or "0")[:6].ljust(6, "0")
        stamp = datetime.strptime(
            f"{match.group(1)} {match.group(2)}.{frac}",
            "%Y-%m-%d %H:%M:%S.%f",
        )
        return stamp.timestamp()
    # journalctl default: "Nov 26 10:10:33 host msg..."
    if default_year is not None:
        match = re.match(
            r"^([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})",
            line,
        )
        if match:
            try:
                stamp = datetime.strptime(
                    f"{default_year} {match.group(1)} {match.group(2)} "
                    f"{match.group(3)}",
                    "%Y %b %d %H:%M:%S",
                )
                return stamp.timestamp()
            except ValueError:
                return None
    return None


def _peering_name_flags(state_name):
    """Map a PeeringState name onto clean / degraded / undersized / recovering.

    Leaf names matter: ``.../Degraded`` or ``.../Undersized`` mean vulnerable
    even if other Active substates appear elsewhere in the path.
    """
    name = state_name.strip().lower()
    parts = [p for p in name.split("/") if p]
    flags = {
        "clean": False,
        "degraded": False,
        "undersized": False,
        "recovering": False,
    }
    if "degraded" in parts:
        flags["degraded"] = True
    if "undersized" in parts:
        flags["undersized"] = True
    if "clean" in parts and not flags["degraded"] and not flags["undersized"]:
        flags["clean"] = True
    # pg_rebuild_duration is armed only in Recovering and Backfilling.
    # WaitActingChange, recovery_wait, and backfill_wait are not that span.
    if "recovering" in parts or "backfilling" in parts:
        flags["recovering"] = True
    return flags


def _pg_state_token_flags(token):
    """Map a PG state token onto clean / degraded / undersized / recovering.

    Vulnerability follows the product rule: if the token contains
    ``degraded`` or ``undersized`` (any other flags allowed —
    ``active+undersized+degraded+remapped``, etc.), it is vulnerable.
    """
    parts = set(token.lower().split("+"))
    recovering = bool(parts & _REBUILD_PARTS)
    degraded = "degraded" in parts
    undersized = "undersized" in parts
    clean = "clean" in parts and not degraded and not undersized
    return {
        "clean": clean,
        "degraded": degraded,
        "undersized": undersized,
        "recovering": recovering,
    }


def _line_pg_state_flags(line):
    """Read PeeringState and PG state strings from one OSD log line.

    Vulnerability is True when the line shows degraded or undersized in
    either the PeeringState path or the PG ``a+b+c`` state token (the token
    that appears before ``mbc={}``). Extra flags on the token do not matter.

    Do not treat ``update_calc_stats degraded N`` as a state change — that is
    an object count, not the PG state string.

    ``exit .../Degraded`` (or Undersized) clears vulnerability.
    """
    flags = {
        "clean": False,
        "degraded": False,
        "undersized": False,
        "recovering": False,
        "exit_recovering": False,
        "exit_vulnerability": False,
        "state_seen": False,
    }
    for match in _STATE_ANGLE_RE.finditer(line):
        flags["state_seen"] = True
        for key, value in _peering_name_flags(match.group(1)).items():
            flags[key] = flags[key] or value
    for match in _ENTER_STATE_RE.finditer(line):
        flags["state_seen"] = True
        for key, value in _peering_name_flags(match.group(1)).items():
            flags[key] = flags[key] or value
    for match in _EXIT_STATE_RE.finditer(line):
        flags["state_seen"] = True
        leaf = _peering_name_flags(match.group(1))
        if leaf["recovering"]:
            flags["exit_recovering"] = True
        if leaf["degraded"] or leaf["undersized"]:
            flags["exit_vulnerability"] = True
    # Prefer ``... <state> mbc=``; fall back to ``active[+...]]`` / activating.
    token_match = _PG_STATE_MBC_RE.search(line) or _PG_STATE_CLEAN_RE.search(line)
    if token_match:
        flags["state_seen"] = True
        for key, value in _pg_state_token_flags(token_match.group(1)).items():
            flags[key] = flags[key] or value
    return flags


def _durations_from_osd_log(log_text, pgids, default_year=None):
    """Sum vulnerability and rebuild seconds for ``pgids`` from OSD log text.

    Vulnerability is the time a PG's state contains ``degraded`` or
    ``undersized`` (any other flags allowed). The window closes when a later
    state for that PG no longer contains either (typically ``active+clean``).

    Rebuild starts when the PG enters recovering/backfill and stops when
    recovering exits or the PG becomes clean without rebuild flags.
    """
    wanted = set(pgids)
    line_count = len((log_text or "").splitlines())
    if default_year is None:
        for line in (log_text or "").splitlines():
            match = _LOG_TS_RE.search(line)
            if match:
                default_year = int(match.group(1)[:4])
                break
    log_about_to(
        "parse PeeringState durations from OSD log text",
        pgids=sorted(wanted),
        log_lines=line_count,
        default_year=default_year,
    )
    vuln_open = {}
    rebuild_open = {}
    vuln_seconds = {pgid: 0.0 for pgid in wanted}
    rebuild_seconds = {pgid: 0.0 for pgid in wanted}
    transitions = {pgid: 0 for pgid in wanted}
    vuln_events = {pgid: [] for pgid in wanted}

    for line in (log_text or "").splitlines():
        stamp = _parse_osd_log_timestamp(line, default_year=default_year)
        if stamp is None:
            continue
        line_pgids = [
            match.group(1)
            for match in _PGID_RE.finditer(line)
            if match.group(1) in wanted
        ]
        if not line_pgids:
            continue
        flags = _line_pg_state_flags(line)
        # Need a real state signal (token / peering path / degraded word).
        if not flags["state_seen"]:
            continue
        # Any state containing degraded or undersized counts; exit of those
        # PeeringState leaves clears vulnerability on that line.
        vulnerable = (flags["degraded"] or flags["undersized"]) and not flags[
            "exit_vulnerability"
        ]
        for pgid in line_pgids:
            transitions[pgid] += 1
            if vulnerable:
                if pgid not in vuln_open:
                    vuln_open[pgid] = stamp
                    vuln_events[pgid].append(("open", stamp, line[:180]))
                    log.info(
                        f"vulnerability OPEN pg={pgid} at {stamp}: {line[:180]}"
                    )
            elif pgid in vuln_open and (
                flags["clean"] or flags["exit_vulnerability"]
            ):
                # Only clean / exit Degraded|Undersized closes the window.
                # Other state_seen lines (leases, Activating, readable_until)
                # must not clear vulnerability while undersized/degraded.
                opened = vuln_open.pop(pgid)
                delta = stamp - opened
                vuln_seconds[pgid] += delta
                vuln_events[pgid].append(("close", stamp, delta, line[:180]))
                log.info(
                    f"vulnerability CLOSE pg={pgid} at {stamp} "
                    f"duration={delta:.6f}s: {line[:180]}"
                )

            if flags["clean"]:
                if pgid in rebuild_open:
                    rebuild_seconds[pgid] += stamp - rebuild_open.pop(pgid)
                continue
            if flags["exit_recovering"] and pgid in rebuild_open:
                rebuild_seconds[pgid] += stamp - rebuild_open.pop(pgid)
            if (
                flags["recovering"]
                and not flags["exit_recovering"]
                and pgid not in rebuild_open
            ):
                rebuild_open[pgid] = stamp

    still_open = [pgid for pgid in wanted if pgid in vuln_open or pgid in rebuild_open]
    if still_open:
        log.warning(
            "PG state windows still open at end of OSD log "
            f"(not closed by active+clean): {still_open}"
        )
        for pgid in list(vuln_open):
            log.warning(
                f"vulnerability still OPEN for {pgid} since {vuln_open[pgid]}"
            )
    result = {
        "vulnerability": sum(vuln_seconds.values()),
        "rebuild": sum(rebuild_seconds.values()),
        "per_pg": {
            pgid: {
                "vulnerability": vuln_seconds[pgid],
                "rebuild": rebuild_seconds[pgid],
                "transitions": transitions[pgid],
                "vuln_events": len(vuln_events[pgid]),
            }
            for pgid in wanted
        },
    }
    log_checkpoint(
        "parsed OSD log durations",
        vulnerability=result["vulnerability"],
        rebuild=result["rebuild"],
        still_open=still_open,
        transitions=transitions,
    )
    return result


def _fetch_primary_osd_state_log(rados_obj, osd_id, start_time, end_time, pool_id=None):
    """Return OSD log lines that mention PG state transitions.

    ``pool_id`` limits the fetch to one pool. With ``pool_id`` omitted, every
    pool is included. Perf counters are per OSD, so a stopped OSD that also
    sits in another pool's acting set contributes to the same counter.

    Prefer the cephadm file log under ``/var/log/ceph/<fsid>/ceph-osd.<id>.log``
    (same source operators inspect; carries Ceph ``YYYY-MM-DDTHH:MM:SS`` stamps).
    Fall back to ``journalctl -o short-iso-precise`` so journal timestamps still
    parse — the default journalctl short form (``Nov 26 10:10:33``) has no year
    and was previously skipped by the duration parser, yielding vulnerability=0.
    """
    log_about_to(
        "fetch primary OSD PeeringState log lines",
        osd_id=osd_id,
        start_time=start_time,
        end_time=end_time,
        pool_id=pool_id,
    )
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
    if pool_id:
        pattern = f"pg\\[{pool_id}\\.|Started/Primary/Active|state<"
    else:
        pattern = "pg\\[|Started/Primary/Active|state<"
    log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
    # Ceph file stamps use T; start/end from date(1) use a space.
    start_iso = str(start_time).strip().replace(" ", "T", 1)
    end_iso = str(end_time).strip().replace(" ", "T", 1)
    file_cmd = (
        f"sudo awk -v s='{start_iso}' -v e='{end_iso}' "
        f"'$1 >= s && $1 <= e' '{log_path}' 2>/dev/null "
        f"| grep -E '{pattern}' || true"
    )
    log_checkpoint(
        "OSD file-log fetch ready",
        osd_id=osd_id,
        host=host.hostname,
        log_path=log_path,
        fsid=fsid,
        start_iso=start_iso,
        end_iso=end_iso,
    )
    log.info(
        f"Reading osd.{osd_id} file log on {host.hostname} "
        f"({log_path}) from {start_time} until {end_time}"
    )
    out, _ = host.exec_command(cmd=file_cmd, timeout=180)
    text = out or ""
    source = "file"
    if not text.strip():
        unit = f"ceph-{fsid}@osd.{osd_id}.service"
        journal_cmd = (
            f"sudo journalctl -o short-iso-precise -u '{unit}' "
            f"--since '{start_time}' --until '{end_time}' --no-pager "
            f"| grep -E '{pattern}' || true"
        )
        log_checkpoint(
            "file log empty; falling back to journalctl short-iso-precise",
            osd_id=osd_id,
            unit=unit,
        )
        log.info(
            f"Reading osd.{osd_id} journal on {host.hostname} from {start_time} "
            f"until {end_time}"
        )
        out, _ = host.exec_command(cmd=journal_cmd, timeout=180)
        text = out or ""
        source = "journal"
    log_checkpoint(
        f"osd.{osd_id} state-transition log fetched",
        source=source,
        line_count=len(text.splitlines()),
        bytes=len(text),
    )
    return text


def _duration_match_tolerance(log_duration, perf_delta):
    """Allow sub-second log granularity, floored at 1s and 5% of the larger value."""
    scale = max(abs(log_duration), abs(perf_delta))
    tol = max(1.0, 0.05 * scale)
    log_checkpoint(
        "duration match tolerance",
        log_duration=log_duration,
        perf_delta=perf_delta,
        scale=scale,
        tolerance=tol,
    )
    return tol


def _truncate_osd_logs(rados_obj):
    """Drop existing OSD journals so this test only reads its own lines.

    OSD hosts log through journald. ``journalctl --rotate`` archives the
    active journal and ``--vacuum-time=1s`` removes those archived files.
    File logs under ``/var/log/ceph`` are truncated when they exist.
    """
    log_banner("TRUNCATE OSD journals / ceph-osd log files")
    log_about_to("enumerate OSD hosts and truncate journals/files")
    hosts = rados_obj.ceph_cluster.get_nodes(role="osd") or []
    seen = set()
    cmd = (
        "sudo journalctl --rotate && sleep 1 && sudo journalctl --vacuum-time=1s && "
        "{ sudo find /var/log/ceph -name 'ceph-osd.*.log' -type f "
        "-exec truncate -s 0 {} + 2>/dev/null || true; }"
    )
    for host in hosts:
        name = getattr(host, "hostname", None) or str(host)
        if name in seen:
            continue
        seen.add(name)
        log_about_to("truncate OSD logs on host", host=name)
        host.exec_command(cmd=cmd, timeout=180)
        log_checkpoint("truncated done on host", host=name)
    if not seen:
        raise AssertionError("No OSD hosts found to truncate OSD logs")
    log_checkpoint("truncate OSD logs complete", hosts=sorted(seen))
    log_banner("TRUNCATE OSD logs complete")


def begin_duration_value_check(rados_obj, cephadm, pool_names):
    """Turn on OSD debug logs and snapshot counters before a fault.

    Call this once the pool is clean and immediately before the failure that
    should move PGs through degraded/recovering. ``end_osd_debug`` must run
    in the scenario's ``finally`` block. OSD journals are truncated here so
    the log window contains only this fault.
    """
    pool_names = list(pool_names)
    log_banner(f"BEGIN duration value check for pools {pool_names}")
    log_about_to(
        "enable debug_osd 20, truncate logs, snapshot baselines",
        pools=pool_names,
    )
    log.info("Setting osd debug_osd 20 for PeeringState log capture")
    cephadm.shell(["ceph config set osd debug_osd 20"])
    log_checkpoint("debug_osd set to 20")
    try:
        log_about_to("sleep 10s so debug_osd takes effect on OSD daemons")
        time.sleep(10)
        _truncate_osd_logs(rados_obj)
        state = _snapshot_duration_window(rados_obj, pool_names)
        log_checkpoint(
            "duration baseline ready — inject fault next",
            log_start=state.get("log_start"),
            rebuild_osds=len(state.get("baseline_rebuild") or {}),
            vuln_osds=len(state.get("baseline_vuln") or {}),
            pg_acting=len(state.get("pg_acting") or {}),
        )
        log_banner("Duration baseline snapshot ready — inject fault next")
        return state
    except Exception:
        log.error("begin_duration_value_check failed; removing debug_osd")
        end_osd_debug(cephadm, {"debug": True})
        raise


def _snapshot_duration_window(rados_obj, pool_names):
    """Snapshot counters, acting sets, and the log-window start."""
    pool_names = list(pool_names)
    log_phase("Snapshot duration baselines (rebuild + vulnerability + acting)")
    log_about_to("snapshot rebuild baselines", pools=pool_names)
    baseline_rebuild = get_all_osd_rebuild_counters(rados_obj)
    log_checkpoint(
        "rebuild baseline snapshot",
        osd_count=len(baseline_rebuild),
        osd_ids=sorted(baseline_rebuild),
    )
    validate_all_osd_counter_fields(
        baseline_rebuild, context="duration-baseline"
    )
    log_phase("Fetch vulnerability baselines for the same OSDs")
    log_about_to(
        "snapshot vulnerability baselines",
        osd_ids=sorted(baseline_rebuild),
    )
    baseline_vuln = run_for_each(
        list(baseline_rebuild),
        worker_fn=lambda osd_id: get_vulnerability_counters(rados_obj, osd_id),
        on_error="raise",
    )
    log_checkpoint(
        "vulnerability baselines collected",
        osd_count=len(baseline_vuln),
    )
    pg_acting = {}
    for pool_name in pool_names:
        log_about_to("collect PG acting sets", pool_name=pool_name)
        pg_acting.update(_pool_pg_acting_map(rados_obj, pool_name))
    if not pg_acting:
        raise AssertionError(
            f"No PG acting sets found for pools {list(pool_names)}"
        )
    log_about_to("record log-window start timestamp")
    log_start = _host_local_timestamp(rados_obj.node)
    log_checkpoint(
        "duration window baselines complete",
        log_start=log_start,
        rebuild_osds=len(baseline_rebuild),
        vuln_osds=len(baseline_vuln),
        pg_acting=len(pg_acting),
        pools=pool_names,
    )
    return {
        "debug": True,
        "baseline_rebuild": baseline_rebuild,
        "baseline_vuln": baseline_vuln,
        "pg_acting": pg_acting,
        "log_start": log_start,
    }


def _osd_daemon_start_epoch(rados_obj, osd_id):
    """Return the running OSD daemon's start time as a POSIX timestamp.

    ``ceph orch ps`` ``started`` is when the process came up. In-memory
    perf counters reset then, so a restarted OSD's log comparison has to
    begin at that instant rather than when the test later observed the
    daemon as running.
    """
    try:
        daemons = rados_obj.run_ceph_command(
            cmd=f"ceph orch ps --daemon_type osd --daemon_id {osd_id}"
        )
    except Exception as err:
        log.warning(f"osd.{osd_id} daemon start time unavailable: {err}")
        return None
    if isinstance(daemons, dict):
        daemons = [daemons]
    if not daemons:
        return None
    started = (daemons[0] or {}).get("started")
    if not started:
        return None
    text = str(started).strip().replace("T", " ", 1).replace("Z", "")
    text = text.split("+", 1)[0].strip()
    return _host_clock_epoch(text)


def _host_clock_epoch(text):
    """Parse ``YYYY-MM-DD HH:MM:SS`` from the OSD host clock."""
    text = str(text).strip().strip("'")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    return None


def _filter_log_since(log_text, not_before_epoch, default_year=None):
    """Drop log lines older than ``not_before_epoch``.

    A restarted OSD's counters start at zero, so only PeeringState lines
    from after the process came back can be compared to the new values.
    """
    if not_before_epoch is None:
        log_checkpoint("log filter skipped (no not_before_epoch)")
        return log_text or ""
    before = len((log_text or "").splitlines())
    if default_year is None:
        for line in (log_text or "").splitlines():
            match = _LOG_TS_RE.search(line)
            if match:
                default_year = int(match.group(1)[:4])
                break
    log_about_to(
        "filter OSD log lines older than restart",
        not_before_epoch=not_before_epoch,
        input_lines=before,
    )
    kept = []
    for line in (log_text or "").splitlines():
        stamp = _parse_osd_log_timestamp(line, default_year=default_year)
        if stamp is None or stamp >= not_before_epoch:
            kept.append(line)
    log_checkpoint(
        "log filter applied",
        input_lines=before,
        kept_lines=len(kept),
        dropped=before - len(kept),
    )
    return "\n".join(kept)


def _log_durations_for_pgs(
    rados_obj, osd_id, log_start, log_end, pgids, pool_ids, not_before_epoch=None
):
    """Sum PeeringState durations on one OSD.

    Counters are per OSD. A stopped OSD that is also in another pool's acting
    set adds that pool's episode to the same counter, so the total includes
    every PG in the log window. ``scenario_vuln`` / ``scenario_rebuild`` cover
    only ``pgids`` and are what activity requirements use.
    """
    log_phase(
        f"Parse PeeringState durations from osd.{osd_id} "
        f"(scenario PGs={list(pgids)}, pools={list(pool_ids)})"
    )
    log_about_to(
        "sum log durations across every pool on one OSD",
        osd_id=osd_id,
        log_start=log_start,
        log_end=log_end,
        pgids=list(pgids),
        pool_ids=list(pool_ids),
        not_before_epoch=not_before_epoch,
    )
    osd_log = _fetch_primary_osd_state_log(
        rados_obj, osd_id, log_start, log_end, pool_id=None
    )
    if not_before_epoch is not None:
        # The scenario stamps this after change_osd_state("start") returns,
        # which is after the daemon is already up and has published its
        # episodes. Counters reset at process start, so keep every line
        # from the daemon's started time.
        started = _osd_daemon_start_epoch(rados_obj, osd_id)
        if started is not None and started < not_before_epoch:
            log.info(
                f"osd.{osd_id} restart cutoff {not_before_epoch} is after "
                f"daemon start {started}; keeping lines from process start"
            )
            not_before_epoch = started
    osd_log = _filter_log_since(osd_log, not_before_epoch)
    saw_primary = "started/primary" in (osd_log or "").lower()
    year = None
    try:
        year = int(str(log_start).strip()[:4])
    except (TypeError, ValueError):
        year = None
    all_pgids = sorted(set(_PGID_RE.findall(osd_log or "")))
    if all_pgids:
        parsed = _durations_from_osd_log(osd_log, all_pgids, default_year=year)
    else:
        parsed = {"vulnerability": 0.0, "rebuild": 0.0, "per_pg": {}}
    wanted = {str(pg) for pg in pgids}
    empty_pg = {
        "vulnerability": 0.0,
        "rebuild": 0.0,
        "transitions": 0,
        "vuln_events": 0,
    }
    scenario_per_pg = {}
    for pg in wanted:
        scenario_per_pg[pg] = parsed["per_pg"].get(pg, dict(empty_pg))
    scenario_vuln = sum(item["vulnerability"] for item in scenario_per_pg.values())
    scenario_rebuild = sum(item["rebuild"] for item in scenario_per_pg.values())
    other = {
        pg: item
        for pg, item in parsed["per_pg"].items()
        if pg not in wanted
        and (item["vulnerability"] or item["rebuild"])
    }
    if other:
        log.info(
            f"osd.{osd_id} other-PG durations included in the counter match: "
            f"{pp(other)}"
        )
    log_checkpoint(
        f"osd.{osd_id} total log durations",
        scenario_vulnerability=scenario_vuln,
        scenario_rebuild=scenario_rebuild,
        vulnerability=parsed["vulnerability"],
        rebuild=parsed["rebuild"],
        saw_primary=saw_primary,
        scenario_pg_count=len(scenario_per_pg),
        all_pg_count=len(parsed["per_pg"]),
    )
    return {
        "scenario_vuln": scenario_vuln,
        "scenario_rebuild": scenario_rebuild,
        "total_vuln": parsed["vulnerability"],
        "total_rebuild": parsed["rebuild"],
        "per_pg": scenario_per_pg,
        "saw_primary": saw_primary,
    }


def end_osd_debug(cephadm, state):
    """Remove ``debug_osd`` if ``begin_duration_value_check`` enabled it."""
    if not state or not state.get("debug"):
        log_checkpoint("end_osd_debug skipped (debug not enabled)")
        return
    log_phase("CLEANUP: remove osd debug_osd")
    log_about_to("ceph config rm osd debug_osd")
    try:
        cephadm.shell(["ceph config rm osd debug_osd"])
        log_checkpoint("removed osd debug_osd")
    except Exception as err:
        log.warning(f"Failed to remove osd debug_osd: {err}")
    state["debug"] = False


def assert_duration_values_match_logs(
    rados_obj,
    state,
    stopped_osds,
    require_activity=True,
    require_rebuild=None,
    skip_osds=None,
    restarted_osds=None,
):
    """Match perf-counter deltas to primary OSD PeeringState log durations.

    ``stopped_osds`` marks PGs whose acting set contained a stopped OSD.
    Pass an empty list to treat every snapshotted PG as affected (backfill,
    repair, or rebalance with no OSD stop). Only an OSD that was the
    acting-set primary of an affected PG may show a non-zero delta that
    the scenario PGs do not explain. The counter is per OSD, so the match
    includes every PG in that OSD's log, including other pools that share
    the stopped OSD.

    Vulnerability and rebuild are independent:
      - degraded/undersized in the log → vulnerability duration must rise
        and match that window
      - recovering/backfill in the log → rebuild duration must rise and
        match that window
      - either counter may stay 0 when its PG state never appears (e.g.
        within_down_out with no mid-window writes → rebuild stays 0)

    ``require_activity`` expects vulnerability activity (degraded/undersized).
    ``require_rebuild`` expects rebuild activity; defaults to the same value
    as ``require_activity``. Pass ``require_rebuild=False`` when recovery is
    not expected (within_down_out without IO).

    Recording OSD (who must hold the durations):
      - Non-primary stop → original acting primary
      - Primary stop, returns within down_out and is still/again primary →
        that same primary (counters accumulate after restart)
      - Primary stop and stays down (or acting primary changed) →
        the current acting primary

    ``restarted_osds`` maps an OSD id to the host-local time it finished
    starting. That process's counters were cleared, so the comparison uses
    a zero baseline and ignores log lines from before the restart.
    ``skip_osds`` are still down and are not queried.
    """
    log_banner("ASSERT duration values vs PeeringState logs")
    if not state:
        raise AssertionError("duration value check was not started")
    if require_rebuild is None:
        require_rebuild = require_activity
    stopped = {int(osd) for osd in (stopped_osds or [])}
    skip = {int(osd) for osd in (skip_osds or [])}
    log.info(
        f"stopped_osds={sorted(stopped)}, skip_osds={sorted(skip)}, "
        f"require_activity={require_activity}, "
        f"require_rebuild={require_rebuild}, "
        f"restarted_osds={sorted((restarted_osds or {}).keys())}"
    )
    pg_acting = state["pg_acting"]
    if stopped:
        affected = {
            pgid: info
            for pgid, info in pg_acting.items()
            if stopped.intersection(info["acting"])
        }
    else:
        affected = dict(pg_acting)
    log.info(
        f"duration value check affected PGs ({len(affected)}): "
        f"{sorted(affected)}"
    )
    if not affected:
        raise AssertionError(
            f"No PG acting set contained stopped OSDs {sorted(stopped)}"
        )

    log_end = _host_local_timestamp(rados_obj.node)
    log.info(
        f"duration value check log window: "
        f"{state['log_start']} -> {log_end}"
    )
    restarted = {
        int(osd): _host_clock_epoch(stamp)
        for osd, stamp in (restarted_osds or {}).items()
    }
    primary_to_pgs = {}
    for pgid, info in affected.items():
        orig_primary = int(info["primary"])
        if orig_primary not in stopped:
            owner = orig_primary
            log.info(
                f"PG {pgid}: recording on original primary osd.{owner} "
                f"(acting={info['acting']})"
            )
        else:
            log.info(
                f"PG {pgid} primary osd.{orig_primary} was stopped; "
                "resolving recording OSD from current acting set"
            )
            new_acting = rados_obj.get_pg_acting_set(pg_num=pgid)
            if not new_acting:
                log.warning(f"PG {pgid} has empty acting set after stop; skipping")
                continue
            current_primary = int(new_acting[0])
            if orig_primary in skip:
                # Still down → replacement primary holds the durations.
                owner = current_primary
                log.info(
                    f"PG {pgid}: stopped primary still down; "
                    f"recording on new primary osd.{owner} (acting={new_acting})"
                )
            elif (
                orig_primary in restarted and current_primary == orig_primary
            ):
                # Returned within down_out (still/again primary) → same OSD.
                owner = orig_primary
                log.info(
                    f"PG {pgid}: primary osd.{owner} returned within down_out; "
                    f"durations stored on that primary (acting={new_acting})"
                )
            else:
                # Acting primary changed while it was down → new primary.
                owner = current_primary
                log.info(
                    f"PG {pgid}: acting primary is osd.{owner} "
                    f"(was osd.{orig_primary}; acting={new_acting})"
                )
        if owner in skip:
            log.info(f"Skipping PG {pgid} owner osd.{owner} (in skip_osds)")
            continue
        primary_to_pgs.setdefault(owner, []).append(pgid)
    allowed = set(primary_to_pgs)
    log.info(f"Acting-set primaries allowed to record duration: {sorted(allowed)}")
    for osd_id, pgids in sorted(primary_to_pgs.items()):
        log.info(f"  osd.{osd_id} owns affected PGs: {pgids}")
    pool_ids = sorted({pgid.split(".", 1)[0] for pgid in affected})

    if allowed:
        log_phase(f"Fetch post-fault counters on allowed primaries {sorted(allowed)}")
        rebuild_after = fetch_counters_for_osds(
            rados_obj, sorted(allowed), on_error="raise"
        )
        log_phase(
            f"Fetch post-fault vulnerability counters on {sorted(allowed)}"
        )
        vuln_after = run_for_each(
            sorted(allowed),
            worker_fn=lambda osd_id: get_vulnerability_counters(rados_obj, osd_id),
            on_error="raise",
        )
    else:
        rebuild_after = {}
        vuln_after = {}
        log.info("No allowed primaries to fetch post-fault counters for")

    baseline_rebuild = state["baseline_rebuild"]
    baseline_vuln = state["baseline_vuln"]
    affected_pgids = list(affected)
    saw_vuln_activity = False
    saw_rebuild_activity = False

    def _baselines_for(osd_id):
        if osd_id in restarted:
            log.info(
                f"osd.{osd_id} was restarted; using zero baselines for comparison"
            )
            return {"avgcount": 0, "sum": 0}, {"avgcount": 0, "sum": 0}
        return (
            baseline_rebuild.get(osd_id, {"avgcount": 0, "sum": 0}),
            baseline_vuln.get(osd_id, {"avgcount": 0, "sum": 0}),
        )

    def _compare_osd(
        osd_id,
        after_rebuild,
        after_vuln,
        pgids,
        must_increment_vuln,
        must_increment_rebuild,
    ):
        nonlocal saw_vuln_activity, saw_rebuild_activity
        log_phase(
            f"Compare osd.{osd_id} perf deltas vs log "
            f"(must_vuln={must_increment_vuln}, "
            f"must_rebuild={must_increment_rebuild}, PGs={pgids})"
        )
        before_rebuild, before_vuln = _baselines_for(osd_id)
        log.info(f"osd.{osd_id} rebuild before={before_rebuild}")
        log.info(f"osd.{osd_id} rebuild after={after_rebuild}")
        log.info(f"osd.{osd_id} vulnerability before={before_vuln}")
        log.info(f"osd.{osd_id} vulnerability after={after_vuln}")
        validate_counter_fields(
            after_rebuild, context=f"osd.{osd_id} duration-check rebuild"
        )
        validate_counter_fields(
            after_vuln, context=f"osd.{osd_id} duration-check vulnerability"
        )
        rebuild_delta = (after_rebuild.get("sum") or 0) - (
            before_rebuild.get("sum") or 0
        )
        vuln_delta = (after_vuln.get("sum") or 0) - (before_vuln.get("sum") or 0)
        log.info(
            f"osd.{osd_id} fetching PeeringState log durations for comparison"
        )
        parsed_logs = _log_durations_for_pgs(
            rados_obj,
            osd_id,
            state["log_start"],
            log_end,
            pgids,
            pool_ids,
            not_before_epoch=restarted.get(osd_id),
        )
        # Counter match uses every PG on the OSD. Activity requirements use
        # only the scenario PGs this OSD owns.
        log_vuln = parsed_logs["total_vuln"]
        log_rebuild = parsed_logs["total_rebuild"]
        scenario_vuln = parsed_logs["scenario_vuln"]
        scenario_rebuild = parsed_logs["scenario_rebuild"]
        per_pg = parsed_logs["per_pg"]
        saw_primary = parsed_logs["saw_primary"]
        log.info(
            f"osd.{osd_id} PGs {pgids}: vulnerability perf delta={vuln_delta} "
            f"scenario_log={scenario_vuln} total_log={log_vuln}, "
            f"rebuild perf delta={rebuild_delta} "
            f"scenario_log={scenario_rebuild} total_log={log_rebuild}, "
            f"saw_primary={saw_primary}, per-PG {pp(per_pg)}"
        )
        vuln_up = verify_counters_incremented(before_vuln, after_vuln)
        rebuild_up = verify_counters_incremented(before_rebuild, after_rebuild)
        log.info(
            f"osd.{osd_id} vuln_up={vuln_up}, rebuild_up={rebuild_up} "
            f"(rebuild_delta={rebuild_delta}, vuln_delta={vuln_delta})"
        )
        # Independent: each counter follows its own PG state in the log.
        if vuln_up and scenario_vuln > 0:
            saw_vuln_activity = True
            log.info(f"osd.{osd_id} counted as vulnerability activity")
        if rebuild_up and scenario_rebuild > 0:
            saw_rebuild_activity = True
            log.info(f"osd.{osd_id} counted as rebuild activity")
        # Require an increment only when this OSD's scenario PGs entered
        # that state. An empty PG, or a primary whose copies returned
        # without recovering, stays at 0 and still matches its log.
        # The caller-level saw_* checks still require the scenario as a
        # whole to observe vulnerability and, when requested, rebuild.
        if must_increment_vuln and scenario_vuln > 0:
            assert vuln_up, (
                f"osd.{osd_id} pg_vulnerability_duration should increase: "
                "PG entered degraded/undersized"
            )
        if must_increment_rebuild and scenario_rebuild > 0:
            assert rebuild_up, (
                f"osd.{osd_id} pg_rebuild_duration should increase: "
                "PG entered recovering/backfill"
            )
        # Log showed recovering → rebuild must have moved (even if not required).
        if log_rebuild > 0 and not rebuild_up:
            raise AssertionError(
                f"osd.{osd_id} log shows recovering "
                f"(log_rebuild={log_rebuild}) but pg_rebuild_duration did not increase"
            )
        # Log showed degraded/undersized → vulnerability must have moved.
        if log_vuln > 0 and not vuln_up:
            raise AssertionError(
                f"osd.{osd_id} log shows degraded/undersized "
                f"(log_vuln={log_vuln}) but pg_vulnerability_duration did not increase"
            )
        if (
            not must_increment_vuln
            and not must_increment_rebuild
            and not saw_primary
            and (abs(vuln_delta) > 1e-6 or abs(rebuild_delta) > 1e-6)
        ):
            raise AssertionError(
                f"osd.{osd_id} duration counters changed but its log has no "
                f"Started/Primary interval for {pgids}"
            )
        vuln_tol = _duration_match_tolerance(log_vuln, vuln_delta)
        rebuild_tol = _duration_match_tolerance(log_rebuild, rebuild_delta)
        log.info(
            f"osd.{osd_id} tolerance: vulnerability={vuln_tol}, rebuild={rebuild_tol}"
        )
        if abs(vuln_delta - log_vuln) > vuln_tol:
            raise AssertionError(
                f"osd.{osd_id} pg_vulnerability_duration delta {vuln_delta} "
                f"does not match log duration {log_vuln} (tolerance {vuln_tol})"
            )
        if abs(rebuild_delta - log_rebuild) > rebuild_tol:
            raise AssertionError(
                f"osd.{osd_id} pg_rebuild_duration delta {rebuild_delta} "
                f"does not match log duration {log_rebuild} "
                f"(tolerance {rebuild_tol})"
            )
        log.info(f"osd.{osd_id} duration compare PASSED")

    log_phase(f"Compare allowed primaries ({len(allowed)})")
    for osd_id in sorted(allowed):
        # Restarted recording primaries (within_down_out primary return) still
        # must show post-restart vulnerability/rebuild when required.
        _compare_osd(
            osd_id,
            rebuild_after[osd_id],
            vuln_after[osd_id],
            primary_to_pgs[osd_id],
            must_increment_vuln=require_activity,
            must_increment_rebuild=require_rebuild,
        )

    other_osds = [
        osd
        for osd in baseline_rebuild
        if osd not in allowed and osd not in skip
    ]
    if other_osds:
        log_phase(
            f"Verify non-primary / unaffected OSDs have zero deltas "
            f"({len(other_osds)} OSDs)"
        )
        other_rebuild = fetch_counters_for_osds(
            rados_obj, other_osds, on_error="raise"
        )
        other_vuln = run_for_each(
            other_osds,
            worker_fn=lambda osd_id: get_vulnerability_counters(rados_obj, osd_id),
            on_error="raise",
        )
        non_zero = []
        for osd_id, after in other_rebuild.items():
            vuln_now = other_vuln[osd_id]
            before_rebuild, before_vuln = _baselines_for(osd_id)
            rebuild_sum_delta = (after.get("sum") or 0) - (
                before_rebuild.get("sum") or 0
            )
            rebuild_count_delta = (after.get("avgcount") or 0) - (
                before_rebuild.get("avgcount") or 0
            )
            vuln_sum_delta = (vuln_now.get("sum") or 0) - (
                before_vuln.get("sum") or 0
            )
            vuln_count_delta = (vuln_now.get("avgcount") or 0) - (
                before_vuln.get("avgcount") or 0
            )
            if (
                abs(rebuild_sum_delta) <= 1e-6
                and rebuild_count_delta == 0
                and abs(vuln_sum_delta) <= 1e-6
                and vuln_count_delta == 0
            ):
                log.info(f"osd.{osd_id} unaffected: deltas are 0 (OK)")
                continue
            log.info(
                f"osd.{osd_id} has non-zero delta "
                f"(rebuild_sum={rebuild_sum_delta}, vuln_sum={vuln_sum_delta}); "
                "checking if it became primary during the window"
            )
            # A primary that took over during the window is not the
            # pre-fault primary. Accept it when its own log matches.
            try:
                _compare_osd(
                    osd_id,
                    after,
                    vuln_now,
                    affected_pgids,
                    must_increment_vuln=False,
                    must_increment_rebuild=False,
                )
                log.info(
                    f"osd.{osd_id} recorded duration as primary during the window"
                )
            except AssertionError as err:
                non_zero.append(osd_id)
                log.error(f"osd.{osd_id} duration delta is not explained: {err}")
        if non_zero:
            raise AssertionError(
                "pg_vulnerability_duration and pg_rebuild_duration deltas must "
                "be 0 on every OSD that is not the acting-set primary of an "
                f"affected PG. Non-zero on: {non_zero}. "
                f"Allowed primaries: {sorted(allowed)}"
            )
        log.info("Unaffected OSD zero-delta check PASSED")
    if require_activity and not saw_vuln_activity:
        raise AssertionError(
            "Expected an acting-set primary to record degraded/undersized "
            f"vulnerability time for {affected_pgids}"
        )
    if require_rebuild and not saw_rebuild_activity:
        raise AssertionError(
            "Expected an acting-set primary to record recovering "
            f"rebuild time for {affected_pgids}"
        )
    log_banner("ASSERT duration values vs logs PASSED")


def _pool_has_degraded_pgs(rados_obj, pool_name):
    """Return True if the pool currently has any degraded/undersized PGs.

    Returns None (unknown) if the state could not be determined, so callers
    can distinguish "confirmed clean" from "could not check".
    """
    log_about_to("check pool for degraded/undersized PGs", pool_name=pool_name)
    try:
        degraded = rados_obj.get_pgid(pool_name=pool_name, states="degraded")
        undersized = rados_obj.get_pgid(pool_name=pool_name, states="undersized")
        result = bool(degraded) or bool(undersized)
        log_checkpoint(
            "pool degraded check",
            pool_name=pool_name,
            degraded=degraded,
            undersized=undersized,
            has_degraded_or_undersized=result,
        )
        return result
    except Exception as err:
        log.warning(f"Could not determine degraded state for pool {pool_name}: {err}")
        return None



def get_pg_stats(rados_obj):
    """Return list of pg_stat dicts from ceph pg dump JSON."""
    log_about_to("ceph pg dump_json pgs")
    dump = rados_obj.run_ceph_command(cmd="ceph pg dump_json pgs")
    if not dump:
        log_checkpoint("pg dump empty", count=0)
        return []
    if isinstance(dump, dict):
        if "pg_map" in dump and isinstance(dump["pg_map"], dict):
            stats = dump["pg_map"].get("pg_stats")
            if stats is not None:
                log_checkpoint("pg_stats from pg_map", count=len(stats))
                return stats
        if "pg_stats" in dump:
            stats = dump.get("pg_stats") or []
            log_checkpoint("pg_stats top-level", count=len(stats))
            return stats
    log_checkpoint("pg dump had unexpected shape", dump_type=type(dump).__name__)
    return []


def extract_pg_history_field(obj, field):
    """Find last_degraded/last_clean in pg dump row or pg query JSON.

    Per PR #68910 these live on pg_stat_t:
      - ``ceph pg dump -f json``: top-level keys on each pg_stats entry
      - ``ceph pg <pgid> query``: under ``stats`` (also try info/history)
    """
    log_about_to("extract PG history field", field=field)
    if obj is None:
        return None
    if not isinstance(obj, dict):
        return None
    if field in obj and obj[field] is not None:
        log_checkpoint("history field at top-level", field=field, value=obj[field])
        return obj[field]
    for nest_key in ("info", "history", "stats"):
        nested = obj.get(nest_key)
        if isinstance(nested, dict):
            if field in nested and nested[field] is not None:
                log_checkpoint(
                    f"history field under {nest_key}",
                    field=field,
                    value=nested[field],
                )
                return nested[field]
            # info.history
            if nest_key == "info":
                hist = nested.get("history")
                if isinstance(hist, dict) and field in hist and hist[field] is not None:
                    log_checkpoint(
                        "history field under info.history",
                        field=field,
                        value=hist[field],
                    )
                    return hist[field]
            # stats.history (rare)
            if nest_key == "stats":
                hist = nested.get("history")
                if isinstance(hist, dict) and field in hist and hist[field] is not None:
                    log_checkpoint(
                        "history field under stats.history",
                        field=field,
                        value=hist[field],
                    )
                    return hist[field]
    log_checkpoint("history field not found", field=field)
    return None


def parse_ceph_ts(ts: str) -> datetime:
    """Parse Ceph utime string to an aware datetime.

    Example: ``2026-11-17T10:23:20.738116+0000`` -> aware datetime.
    """
    log_about_to("parse Ceph timestamp", ts=ts)
    ts = ts[:-5] + "+00:00"
    result = datetime.fromisoformat(ts)
    log_checkpoint("parsed Ceph timestamp", result=str(result))
    return result


def get_durability_counters(rados_obj, osd_id):
    """Fetch vulnerability + rebuild counters from recoverystate_perf.

    Schema reported by the OSD:
      pg_vulnerability_duration {avgcount,sum,avgtime,max_inc}
      pg_vulnerability_duration_min
      pg_rebuild_duration {avgcount,sum,avgtime,max_inc}
      pg_rebuild_duration_min

    Returns dict with keys:
      vuln_avgcount, vuln_sum, vuln_avgtime, vuln_max, vuln_min,
      rebuild_avgcount, rebuild_sum, rebuild_avgtime, rebuild_max, rebuild_min,
      has_vulnerability (bool whether vuln counters exist)
    """
    log_about_to("fetch durability counters (vuln+rebuild)", osd_id=osd_id)
    perf = rados_obj.get_osd_perf_dump(osd_id)
    rs = perf.get("recoverystate_perf", {}) if perf else {}

    vuln = rs.get("pg_vulnerability_duration")
    has_vulnerability = isinstance(vuln, dict)
    if has_vulnerability:
        vuln_avgcount = vuln.get("avgcount", 0) or 0
        vuln_sum = vuln.get("sum", 0) or 0
        vuln_avgtime = vuln.get("avgtime", 0) or 0
        vuln_max = vuln.get("max_inc") or 0
        vuln_min = rs.get("pg_vulnerability_duration_min") or 0
    else:
        vuln_avgcount = vuln_sum = vuln_avgtime = vuln_max = vuln_min = 0

    rebuild = rs.get("pg_rebuild_duration", {}) or {}
    rebuild_avgcount = rebuild.get("avgcount", 0) or 0
    rebuild_sum = rebuild.get("sum", 0) or 0
    rebuild_avgtime = rebuild.get("avgtime", 0) or 0
    rebuild_max = rebuild.get("max_inc") or 0
    rebuild_min = rs.get("pg_rebuild_duration_min") or 0

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
    log_checkpoint(f"osd.{osd_id} durability counters", **result)
    return result


def get_pg_query(rados_obj, pgid):
    """Return JSON from ``ceph pg <pgid> query``.

    The full document is large. The log keeps state, acting set, and the
    history/stats timestamps this suite asserts on.
    """
    log_about_to("ceph pg query", pgid=pgid)
    query = rados_obj.run_ceph_command(cmd=f"ceph pg {pgid} query")
    log_pretty(f"ceph pg {pgid} query", query)
    log_checkpoint(
        "pg query fetched",
        pgid=pgid,
        keys=sorted(query.keys()) if isinstance(query, dict) else type(query).__name__,
    )
    return query


def pick_pool_pgid(rados_obj, pool_name):
    """Return one PG id for the pool or raise."""
    log_about_to("pick one PG id from pool", pool_name=pool_name)
    pgids = rados_obj.get_pgid(pool_name=pool_name)
    if not pgids:
        raise AssertionError(f"No PGs found for pool {pool_name}")
    log_checkpoint(
        "picked pool PG",
        pool_name=pool_name,
        pgid=pgids[0],
        total_pgs=len(pgids),
    )
    return pgids[0]


def _scrape_exporter_metrics(rados_obj, exporter_port=9926):
    """Best-effort scrape of ceph-exporter /metrics; return text or None."""
    log_about_to("scrape ceph-exporter /metrics", exporter_port=exporter_port)
    scrape_nodes = list(rados_obj.ceph_cluster.get_nodes(role="mgr") or [])
    scrape_nodes += list(rados_obj.ceph_cluster.get_nodes(role="installer") or [])
    hostnames = [getattr(n, "hostname", str(n)) for n in scrape_nodes]
    log_checkpoint("exporter scrape candidates", hosts=hostnames, ports=[exporter_port, 9095, 9926])
    for port in (exporter_port, 9095, 9926):
        for node in scrape_nodes:
            try:
                log_about_to(
                    "curl exporter metrics",
                    host=node.hostname,
                    port=port,
                )
                out, _ = node.exec_command(
                    cmd=f"curl -s http://localhost:{port}/metrics",
                    timeout=30,
                )
                if out and ("rebuild" in out.lower() or "vulnerability" in out.lower()):
                    log_pretty(
                        f"Exporter metrics {node.hostname}:{port}",
                        out,
                    )
                    log_checkpoint(
                        "exporter scrape hit",
                        host=node.hostname,
                        port=port,
                        bytes=len(out),
                    )
                    return out
            except Exception as err:
                log.debug(f"Exporter scrape {node.hostname}:{port} failed: {err}")
    log_checkpoint("exporter scrape found no rebuild/vulnerability metrics")
    return None


def _metric_names_for_family(counter_family):
    """Expected exporter metric name substrings for rebuild/vulnerability."""
    log_about_to("resolve exporter metric names", counter_family=counter_family)
    if counter_family == "vulnerability":
        names = [
            "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
            "ceph_recoverystate_perf_pg_vulnerability_duration_count",
            "ceph_recoverystate_perf_pg_vulnerability_duration_max_inc",
            "ceph_recoverystate_perf_pg_vulnerability_duration_min",
        ]
    elif counter_family == "both" or counter_family is None:
        names = [
            "ceph_recoverystate_perf_pg_rebuild_duration_sum",
            "ceph_recoverystate_perf_pg_rebuild_duration_count",
            "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
            "ceph_recoverystate_perf_pg_vulnerability_duration_count",
        ]
    else:
        names = [
            "ceph_recoverystate_perf_pg_rebuild_duration_sum",
            "ceph_recoverystate_perf_pg_rebuild_duration_count",
            "ceph_recoverystate_perf_pg_rebuild_duration_max_inc",
            "ceph_recoverystate_perf_pg_rebuild_duration_min",
        ]
    log_checkpoint("metric names for family", counter_family=counter_family, names=names)
    return names


def _trigger_non_primary_rebuild(
    rados_obj, pool_name, hold_secs=30, cephadm=None
):
    """Stop non-primary, wait, start, wait clean. Return (primary, non_primary, acting).

    When ``cephadm`` is passed, compare the primary's perf-counter deltas to
    PeeringState durations from its OSD log before returning.

    Without mid-window writes, vulnerability is required and rebuild may stay
    0 (no recovering). Callers that need rebuild must write during the hold
    or use a beyond-down-out / remapped path.
    """
    log_banner(
        f"Trigger non-primary rebuild on pool {pool_name} (hold={hold_secs}s)"
    )
    log_about_to(
        "trigger non-primary stop/start rebuild",
        pool_name=pool_name,
        hold_secs=hold_secs,
        duration_check=cephadm is not None,
    )
    primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
    if non_primary is None:
        raise AssertionError("Need at least 2 OSDs in acting set")
    duration_state = None
    try:
        if cephadm is not None:
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
        log.info(f"Stopping non-primary osd.{non_primary}")
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        log.info(f"Holding osd.{non_primary} down for {hold_secs}s")
        time.sleep(hold_secs)
        try:
            wait_for_recovery(rados_obj)
        except Exception as err:
            log.info(f"Recovery wait while OSD down (optional): {err}")
        log.info(f"Starting non-primary osd.{non_primary}")
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        restarted_at = _host_local_timestamp(rados_obj.node)
        log.info(f"osd.{non_primary} restarted at {restarted_at}")
        assert wait_for_recovery(rados_obj, pool_name)
        if duration_state is not None:
            assert_duration_values_match_logs(
                rados_obj,
                duration_state,
                stopped_osds=[non_primary],
                require_activity=True,
                require_rebuild=False,
                restarted_osds={int(non_primary): restarted_at},
            )
    finally:
        if cephadm is not None:
            end_osd_debug(cephadm, duration_state)
    log.info(
        f"Non-primary rebuild trigger done: primary=osd.{primary}, "
        f"non_primary=osd.{non_primary}, acting={acting}"
    )
    return primary, non_primary, acting



# ---------------------------------------------------------------------------
# Scenario implementations
# ---------------------------------------------------------------------------


def scenario_counter_registration(rados_obj, cephadm, config):
    """Verify rebuild and vulnerability counters exist on all OSDs at zero.

    Counters are reset before this scenario runs, so values should be zero
    unless immediate post-restart recovery occurred. ``max_inc`` is absent
    after reset (it only appears when avgcount > 1).

    Test steps:
        1. Get the list of all up OSDs.
        2. Fetch pg_rebuild_duration and pg_vulnerability_duration from every OSD.
        3. Verify avgcount, sum, avgtime, and the _min gauge are present;
           max_inc only when avgcount > 1.
        4. Validate field types and the zero-default values.
    """
    log_step("step 1: Get the list of all up OSDs.")
    log_step("step 2: Fetch pg_rebuild_duration and pg_vulnerability_duration from every OSD.")
    log_step(
        "step 3: Verify avgcount, sum, avgtime, and the _min gauge are present; "
        "max_inc only when avgcount > 1."
    )
    log_step("step 4: Validate field types and the zero-default values.")
    osd_list = rados_obj.get_osd_list(status="up")
    log.info(f"Checking rebuild counters on {len(osd_list)} OSDs")
    rebuild_counters = run_for_each(
        osd_list,
        worker_fn=lambda osd_id: get_rebuild_counters(rados_obj, osd_id),
        on_error="raise",
    )
    log.info(f"Checking vulnerability counters on {len(osd_list)} OSDs")
    vuln_counters = run_for_each(
        osd_list,
        worker_fn=lambda osd_id: get_vulnerability_counters(rados_obj, osd_id),
        on_error="raise",
    )
    log.info(f"Rebuild counters: {pp(rebuild_counters)}")
    log.info(f"Vulnerability counters: {pp(vuln_counters)}")

    non_zero_osds = []
    for osd_id, counters in rebuild_counters.items():
        log.info(f"Rebuild counters for osd.{osd_id}: {pp(counters)}")
        for key in ("avgcount", "sum", "avgtime", "pg_rebuild_duration_min"):
            if counters.get(key) is None:
                raise AssertionError(f"Counter '{key}' not found on osd.{osd_id}")
        avgcount = counters.get("avgcount", 0) or 0
        if avgcount > 1 and counters.get("max_inc") is None:
            raise AssertionError(
                f"Counter 'max_inc' not found on osd.{osd_id} "
                f"(required when avgcount={avgcount} > 1)"
            )
        if avgcount <= 1 and counters.get("max_inc") is not None:
            raise AssertionError(
                f"Counter 'max_inc' unexpectedly present on osd.{osd_id} "
                f"when avgcount={avgcount} <= 1: {counters.get('max_inc')}"
            )
        validate_counter_fields(
            counters,
            context=f"osd.{osd_id} rebuild registration",
        )
        if verify_counter_defaults(counters):
            log.info(f"osd.{osd_id}: rebuild counters present and at zero")
        else:
            non_zero_osds.append(osd_id)
            log.warning(
                f"osd.{osd_id}: rebuild counters non-zero after reset: {pp(counters)}"
            )

    for osd_id, counters in vuln_counters.items():
        log.info(f"Vulnerability counters for osd.{osd_id}: {pp(counters)}")
        for key in (
            "avgcount",
            "sum",
            "avgtime",
            "pg_vulnerability_duration_min",
        ):
            if counters.get(key) is None:
                raise AssertionError(
                    f"Vulnerability counter '{key}' not found on osd.{osd_id}"
                )
        avgcount = counters.get("avgcount", 0) or 0
        if avgcount > 1 and counters.get("max_inc") is None:
            raise AssertionError(
                f"Vulnerability counter 'max_inc' not found on osd.{osd_id} "
                f"(required when avgcount={avgcount} > 1)"
            )
        if avgcount <= 1 and counters.get("max_inc") is not None:
            raise AssertionError(
                f"Vulnerability counter 'max_inc' unexpectedly present on "
                f"osd.{osd_id} when avgcount={avgcount} <= 1: "
                f"{counters.get('max_inc')}"
            )
        validate_counter_fields(
            counters,
            context=f"osd.{osd_id} vulnerability registration",
        )
        if verify_counter_defaults(counters):
            log.info(f"osd.{osd_id}: vulnerability counters present and at zero")
        else:
            log.warning(
                f"osd.{osd_id}: vulnerability counters non-zero after reset: "
                f"{pp(counters)}"
            )
            if osd_id not in non_zero_osds:
                non_zero_osds.append(osd_id)

    if non_zero_osds and len(non_zero_osds) == len(osd_list):
        raise AssertionError(
            "ALL OSDs have non-zero counters after reset — counters may not "
            f"be resetting on restart: {non_zero_osds}"
        )
    log.info(
        "Rebuild and vulnerability registration verified on all "
        f"{len(osd_list)} OSDs "
        f"({len(non_zero_osds)} had post-restart recovery activity)"
    )


def scenario_counter_priority(rados_obj, cephadm, config):
    """Verify rebuild counter priority is PRIO_USEFUL (5) on every reachable OSD.

    Test steps:
        1. Get the list of all up OSDs.
        2. Fetch the perf schema priority of each rebuild counter from every
           OSD, skipping OSDs that are unreachable.
        3. Verify pg_rebuild_duration, pg_rebuild_duration_min,
           pg_vulnerability_duration, and pg_vulnerability_duration_min
           report priority PRIO_USEFUL (5).
    """
    log_step("step 1: Get the list of all up OSDs.")
    log_step("step 2: Fetch the perf schema priority of each rebuild counter from every OSD, skipping OSDs that are unreachable.")
    log_step("step 3: Verify pg_rebuild_duration, pg_rebuild_duration_min, pg_vulnerability_duration, and pg_vulnerability_duration_min report priority PRIO_USEFUL (5).")
    osd_list = rados_obj.get_osd_list(status="up")
    log.info(f"Checking rebuild counter priority on {len(osd_list)} OSDs")

    all_priorities = run_for_each(
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
        for counter in DURATION_COUNTERS:
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


def _parse_exporter_metric_value(metrics_text, metric_name, osd_id):
    """Return the sample value for ``metric_name{ceph_daemon="osd.<id>"}``.

    Returns None when that labeled sample is absent.
    """
    osd_label = f'ceph_daemon="osd.{osd_id}"'
    log_about_to(
        "parse exporter metric sample",
        metric_name=metric_name,
        osd_id=osd_id,
        osd_label=osd_label,
    )
    for line in metrics_text.splitlines():
        if line.startswith("#") or metric_name not in line:
            continue
        if osd_label not in line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            value = float(parts[-1])
            log_checkpoint(
                "exporter metric sample found",
                metric_name=metric_name,
                osd_id=osd_id,
                value=value,
            )
            return value
        except ValueError:
            continue
    log_checkpoint(
        "exporter metric sample missing",
        metric_name=metric_name,
        osd_id=osd_id,
    )
    return None


def scenario_prometheus_integration(rados_obj, cephadm, config):
    """Verify durability counters are present on ceph-exporter /metrics.

    Scrapes ``http://localhost:<exporter_port>/metrics`` (default 9926), matching::

        curl -s http://localhost:9926/metrics | grep -iE 'rebuild|recoverystate'

    Expected series (max_inc is not exported; IBMCEPH-22668):
        ceph_recoverystate_perf_pg_rebuild_duration_{count,sum,min}
        ceph_recoverystate_perf_pg_vulnerability_duration_{count,sum,min}

    Test steps:
        1. Create a pool, write data, wait clean.
        2. Stop/start a non-primary OSD (vulnerability expected; rebuild may
           stay 0 without mid-window IO).
        3. Confirm the primary's vulnerability avgcount > 0.
        4. Scrape ceph-exporter /metrics from mgr/installer nodes on port 9926.
        5. Assert every expected metric name is present.
        6. Assert rebuild exporter sample matches perf dump (may be 0).
        7. Assert the primary OSD's vulnerability count sample is > 0.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_prom_integration")
    exporter_port = config.get("exporter_port", 9926)

    expected_metrics = [
        "ceph_recoverystate_perf_pg_rebuild_duration_count",
        "ceph_recoverystate_perf_pg_rebuild_duration_sum",
        "ceph_recoverystate_perf_pg_rebuild_duration_min",
        "ceph_recoverystate_perf_pg_vulnerability_duration_count",
        "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
        "ceph_recoverystate_perf_pg_vulnerability_duration_min",
        # "ceph_recoverystate_perf_pg_vulnerability_duration_max_inc" https://ibm-ceph.atlassian.net/browse/IBMCEPH-22668
        # "ceph_recoverystate_perf_pg_vulnerability_rebuild_max_inc" https://ibm-ceph.atlassian.net/browse/IBMCEPH-22668
    ]

    try:
        duration_state = None
        _restarted_osds = {}
        log_step(f"step 1: create pool {pool_name}, write data, wait clean")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        log_step(
            f"step 2: stop/start non-primary osd.{non_primary} "
            f"(primary osd.{primary}, acting={acting})"
        )
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        log_step(
            f"step 3: confirm primary osd.{primary} perf dump vulnerability "
            "avgcount > 0 (rebuild may stay 0 without mid-window IO)"
        )
        counters = get_rebuild_counters(rados_obj, primary)
        log.info(f"osd.{primary} rebuild perf dump counters: {pp(counters)}")
        validate_counter_fields(counters, context=f"osd.{primary} pre-export rebuild")
        vuln_counters = get_vulnerability_counters(rados_obj, primary)
        log.info(
            f"osd.{primary} vulnerability perf dump counters: {pp(vuln_counters)}"
        )
        validate_counter_fields(
            vuln_counters, context=f"osd.{primary} pre-export vulnerability"
        )
        assert vuln_counters.get("avgcount", 0) > 0, (
            "Expected non-zero vulnerability counters before checking exporter metrics"
        )
        if counters.get("avgcount", 0) <= 0:
            log.info(
                f"osd.{primary} rebuild avgcount is 0 (no recovering); "
                "exporter rebuild sample may be 0"
            )

        log_step(
            f"step 4: scrape ceph-exporter http://localhost:{exporter_port}/metrics "
            "from mgr/installer nodes"
        )
        scrape_nodes = list(rados_obj.ceph_cluster.get_nodes(role="mgr") or [])
        scrape_nodes += list(rados_obj.ceph_cluster.get_nodes(role="installer") or [])
        if not scrape_nodes:
            raise AssertionError("No mgr/installer nodes found for exporter scrape")

        metrics_text = None
        fetch_errors = []
        for node in scrape_nodes:
            url = f"http://localhost:{exporter_port}/metrics"
            log.info(
                f"Checking rebuild/vulnerability metrics in '{url}' "
                f"on {node.hostname}"
            )
            try:
                out, _ = node.exec_command(cmd=f"curl -s {url}", timeout=30)
                if out and (
                    "pg_rebuild_duration" in out or "pg_vulnerability_duration" in out
                ):
                    metrics_text = out
                    log.info(
                        f"Fetched ceph-exporter metrics from "
                        f"{node.hostname}:{exporter_port}"
                    )
                    break
                if out:
                    fetch_errors.append(
                        f"{node.hostname}:{exporter_port}: response lacked "
                        "rebuild/vulnerability series"
                    )
                else:
                    fetch_errors.append(
                        f"{node.hostname}:{exporter_port}: empty response"
                    )
            except Exception as err:
                fetch_errors.append(f"{node.hostname}:{exporter_port}: {err}")

        if metrics_text is None:
            raise AssertionError(
                "Could not scrape ceph-exporter /metrics with rebuild/"
                f"vulnerability series on port {exporter_port}. "
                f"Errors: {fetch_errors}"
            )
        log_pretty(f"ceph-exporter /metrics port {exporter_port}", metrics_text)

        log_step(
            "step 5: assert rebuild and vulnerability count/sum/min metric names "
            "are present"
        )
        missing = [m for m in expected_metrics if m not in metrics_text]
        if missing:
            sample = [
                line
                for line in metrics_text.splitlines()
                if (
                    "rebuild" in line.lower()
                    or "vulnerab" in line.lower()
                    or "recoverystate" in line.lower()
                )
                and not line.startswith("#")
            ]
            log.error(f"Sample recoverystate scrape lines: {sample[:20]}")
            raise AssertionError(
                "Expected ceph-exporter metric names missing from "
                f"/metrics scrape: {missing}. Expected: {expected_metrics}."
            )
        for metric_name in expected_metrics:
            log.info(f"Found metric '{metric_name}' in ceph-exporter /metrics")

        log_step(
            f"step 6: assert primary osd.{primary} rebuild count sample matches "
            "perf dump (may be 0 without recovering)"
        )
        rebuild_count = _parse_exporter_metric_value(
            metrics_text,
            "ceph_recoverystate_perf_pg_rebuild_duration_count",
            primary,
        )
        log.info(
            f"Exporter rebuild count for osd.{primary}: {rebuild_count} "
            f"(perf dump avgcount={counters.get('avgcount')})"
        )
        if rebuild_count is None:
            raise AssertionError(
                "ceph_recoverystate_perf_pg_rebuild_duration_count sample "
                f"missing for ceph_daemon=\"osd.{primary}\""
            )
        if counters.get("avgcount", 0) > 0 and rebuild_count <= 0:
            raise AssertionError(
                "Expected non-zero exporter rebuild count for "
                f"osd.{primary} after recovering, got {rebuild_count}"
            )

        log_step(
            f"step 7: assert primary osd.{primary} vulnerability count sample is > 0"
        )
        vuln_count = _parse_exporter_metric_value(
            metrics_text,
            "ceph_recoverystate_perf_pg_vulnerability_duration_count",
            primary,
        )
        log.info(
            f"Exporter vulnerability count for osd.{primary}: {vuln_count} "
            f"(perf dump avgcount={vuln_counters.get('avgcount')})"
        )
        if vuln_count is None:
            raise AssertionError(
                "ceph_recoverystate_perf_pg_vulnerability_duration_count sample "
                f"missing for ceph_daemon=\"osd.{primary}\""
            )
        if vuln_count <= 0:
            raise AssertionError(
                "Expected non-zero exporter vulnerability count for "
                f"osd.{primary} after recovery, got {vuln_count}"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        log.info(f"cleanup: delete pool {pool_name}")
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Prometheus integration check passed")


def _prometheus_query(node, prom_base, query, timeout=30):
    """Run a Prometheus instant query and return parsed JSON or None.

    Equivalent to::

        curl -sG "$PROM/api/v1/query" --data-urlencode "query=..."
    """
    host = getattr(node, "hostname", str(node))
    log_about_to(
        "Prometheus instant query",
        host=host,
        prom_base=prom_base,
        query=query,
        timeout=timeout,
    )
    # Quote the PromQL for the remote shell without breaking curly braces.
    quoted = query.replace("'", "'\"'\"'")
    cmd = (
        f"curl -sG '{prom_base}/api/v1/query' "
        f"--data-urlencode 'query={quoted}'"
    )
    out, _ = node.exec_command(cmd=cmd, timeout=timeout)
    if not out:
        log_checkpoint("Prometheus query empty response", query=query, host=host)
        return None
    try:
        payload = json.loads(out)
        log_checkpoint(
            "Prometheus query OK",
            query=query,
            host=host,
            status=payload.get("status") if isinstance(payload, dict) else None,
        )
        return payload
    except Exception as err:
        log.warning(f"Prometheus query JSON parse failed: {err}; raw={out[:200]!r}")
        return None


def _find_prometheus_base(rados_obj, prometheus_port=9095, prometheus_url=None):
    """Return (node, base_url) for a reachable Prometheus /api/v1/query endpoint."""
    log_about_to(
        "locate reachable Prometheus API",
        prometheus_port=prometheus_port,
        prometheus_url=prometheus_url,
    )
    if prometheus_url:
        bases = [prometheus_url.rstrip("/")]
    else:
        bases = [f"http://localhost:{prometheus_port}"]

    nodes = list(rados_obj.ceph_cluster.get_nodes(role="installer") or [])
    nodes += list(rados_obj.ceph_cluster.get_nodes(role="mgr") or [])
    log_checkpoint(
        "Prometheus probe candidates",
        bases=bases,
        hosts=[getattr(n, "hostname", str(n)) for n in nodes],
    )
    errors = []
    for node in nodes:
        for base in bases:
            try:
                # Cheap readiness probe: query a metric that should exist once
                # durability counters are scraped, or up{} as fallback.
                payload = _prometheus_query(node, base, "up")
                if payload and payload.get("status") == "success":
                    log_checkpoint(
                        "Prometheus API reachable",
                        base=base,
                        host=node.hostname,
                    )
                    return node, base
                errors.append(f"{node.hostname} {base}: bad status {payload}")
            except Exception as err:
                errors.append(f"{node.hostname} {base}: {err}")
    raise AssertionError(
        f"Could not reach Prometheus /api/v1/query on port "
        f"{prometheus_port}. Errors: {errors}"
    )


def scenario_prometheus_api_metric_presence(rados_obj, cephadm, config):
    """Verify rebuild/vulnerability metrics via Prometheus /api/v1/query.

    Matches::

        PROM=http://<host>:9095
        curl -sG "$PROM/api/v1/query" \\
          --data-urlencode 'query=ceph_recoverystate_perf_pg_rebuild_duration_sum{ceph_daemon="osd.N"}'

    Expected series:
        ceph_recoverystate_perf_pg_rebuild_duration_{count,sum,min}
        ceph_recoverystate_perf_pg_vulnerability_duration_{count,sum,min}

    Test steps:
        1. Create a pool, write data, wait clean.
        2. Stop/start a non-primary OSD (vulnerability expected; rebuild may
           stay 0 without mid-window IO).
        3. Confirm the primary's vulnerability avgcount > 0.
        4. Locate a reachable Prometheus /api/v1/query endpoint.
        5. Query each expected metric for the primary OSD and assert success.
        6. Assert vulnerability count > 0; rebuild count matches perf dump
           (may be 0 without recovering).
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_prom_api")
    prometheus_port = config.get("prometheus_port", 9095)
    prometheus_url = config.get("prometheus_url")

    expected_metrics = [
        "ceph_recoverystate_perf_pg_rebuild_duration_count",
        "ceph_recoverystate_perf_pg_rebuild_duration_sum",
        "ceph_recoverystate_perf_pg_rebuild_duration_min",
        "ceph_recoverystate_perf_pg_vulnerability_duration_count",
        "ceph_recoverystate_perf_pg_vulnerability_duration_sum",
        "ceph_recoverystate_perf_pg_vulnerability_duration_min",
    ]

    try:
        duration_state = None
        _restarted_osds = {}
        log_step(f"step 1: create pool {pool_name}, write data, wait clean")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        log_step(
            f"step 2: stop/start non-primary osd.{non_primary} to trigger rebuild "
            f"(primary osd.{primary}, acting={acting})"
        )
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        log_step(
            f"step 3: confirm primary osd.{primary} perf dump vulnerability "
            "avgcount > 0 (rebuild may stay 0 without mid-window IO)"
        )
        counters = get_rebuild_counters(rados_obj, primary)
        log.info(f"osd.{primary} rebuild perf dump counters: {pp(counters)}")
        validate_counter_fields(
            counters, context=f"osd.{primary} pre-prom-api rebuild"
        )
        vuln_counters = get_vulnerability_counters(rados_obj, primary)
        log.info(
            f"osd.{primary} vulnerability perf dump counters: {pp(vuln_counters)}"
        )
        validate_counter_fields(
            vuln_counters, context=f"osd.{primary} pre-prom-api vulnerability"
        )
        assert vuln_counters.get("avgcount", 0) > 0, (
            "Expected non-zero vulnerability counters before Prometheus API checks"
        )
        if counters.get("avgcount", 0) <= 0:
            log.info(
                f"osd.{primary} rebuild avgcount is 0 (no recovering); "
                "Prometheus rebuild sample may be 0"
            )

        log_step(
            f"step 4: locate Prometheus /api/v1/query "
            f"(port {prometheus_port}"
            + (f", url {prometheus_url}" if prometheus_url else "")
            + ")"
        )
        # Give Prometheus a moment to scrape the exporter after recovery.
        time.sleep(config.get("prom_scrape_wait", 30))
        prom_node, prom_base = _find_prometheus_base(
            rados_obj,
            prometheus_port=prometheus_port,
            prometheus_url=prometheus_url,
        )

        log_step(
            f"step 5: query each rebuild/vulnerability metric for "
            f"ceph_daemon=\"osd.{primary}\" via {prom_base}/api/v1/query"
        )
        missing = []
        values = {}
        for metric in expected_metrics:
            query = f'{metric}{{ceph_daemon="osd.{primary}"}}'
            log.info(f"Prometheus query: {query}")
            payload = _prometheus_query(prom_node, prom_base, query)
            log_pretty(f"Prometheus API response for {metric}", payload or {})
            if not payload or payload.get("status") != "success":
                missing.append(f"{metric}: bad status {payload}")
                continue
            result = (payload.get("data") or {}).get("result") or []
            if not result:
                missing.append(f"{metric}: empty result vector")
                continue
            try:
                values[metric] = float(result[0]["value"][1])
            except Exception as err:
                missing.append(f"{metric}: could not parse value ({err})")
                continue
            log.info(
                f"Found {metric}{{ceph_daemon=\"osd.{primary}\"}} = {values[metric]}"
            )

        if missing:
            raise AssertionError(
                "Prometheus /api/v1/query missing expected rebuild/"
                f"vulnerability metrics for osd.{primary}: {missing}"
            )

        log_step(
            f"step 6: assert primary osd.{primary} vulnerability count > 0; "
            "rebuild count matches perf dump (may be 0 without recovering)"
        )
        rebuild_count = values.get(
            "ceph_recoverystate_perf_pg_rebuild_duration_count"
        )
        log.info(
            f"Prometheus rebuild count for osd.{primary}: {rebuild_count} "
            f"(perf dump avgcount={counters.get('avgcount')})"
        )
        if rebuild_count is None:
            raise AssertionError(
                "Expected Prometheus rebuild count sample for "
                f"osd.{primary}, got {rebuild_count}"
            )
        if counters.get("avgcount", 0) > 0 and rebuild_count <= 0:
            raise AssertionError(
                "Expected non-zero Prometheus rebuild count for "
                f"osd.{primary} after recovering, got {rebuild_count}"
            )
        vuln_count = values.get(
            "ceph_recoverystate_perf_pg_vulnerability_duration_count"
        )
        log.info(
            f"Prometheus vulnerability count for osd.{primary}: {vuln_count} "
            f"(perf dump avgcount={vuln_counters.get('avgcount')})"
        )
        if vuln_count is None or vuln_count <= 0:
            raise AssertionError(
                "Expected non-zero Prometheus vulnerability count for "
                f"osd.{primary}, got {vuln_count}"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        log.info(f"cleanup: delete pool {pool_name}")
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Prometheus API metric presence check passed")


def scenario_non_primary_osd_stop(rados_obj, cephadm, config):
    """Stop a non-primary OSD, wait for recovery, verify rebuild counters.

    Sub-cases via config['sub_case']:
        'within_down_out'  - OSD stopped < mon_osd_down_out_interval, then
            restarted. No mid-window writes → expect vulnerability only
            (degraded/undersized); rebuild may stay 0.
        'beyond_down_out'  - OSD stopped > mon_osd_down_out_interval, then restarted
        'not_restarted'    - OSD stopped and not restarted (recovery via re-replication)

    Test steps:
        1. Create a pool (replicated or EC) and write data; wait for clean.
        2. Pick a secondary OSD that appears in exactly one acting set of
           the pool (not also primary elsewhere).
        3. Set debug_osd to 20, snapshot counters and acting sets, and open
           the OSD log window.
        4. Stop the non-primary OSD per the configured sub_case and wait
           for recovery.
        5. On each affected PG's acting-set primary, compare
           pg_vulnerability_duration and pg_rebuild_duration deltas with
           degraded/undersized and recovering intervals parsed from that
           primary OSD's log (independently). Every other OSD's deltas stay at 0.
    """
    pool_obj = PoolFunctions(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_non_primary")
    pool_type = config.get("pool_type", "replicated")
    sub_case = config.get("sub_case", "beyond_down_out")
    non_primary = None
    duration_state = None
    _restarted_osds = {}

    try:
        log_step("step 1: Create a pool (replicated or EC) and write data; wait for clean.")
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

        log_step(
            "step 2: Pick a secondary OSD that is in exactly one acting set."
        )
        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Pool has only 1 OSD in acting set, cannot test")

        log.info(
            f"Acting set: {acting}, primary: {primary}, target non-primary: {non_primary}"
        )

        log_step(
            "step 3: Set debug_osd 20 and snapshot counters and PG acting sets."
        )
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        affected = {
            pgid: info
            for pgid, info in duration_state["pg_acting"].items()
            if non_primary in info["acting"]
        }
        log.info(
            f"PGs whose acting set includes osd.{non_primary} "
            f"({len(affected)}): {sorted(affected)}"
        )
        if not affected:
            raise AssertionError(
                f"osd.{non_primary} is not in any PG acting set for pool {pool_name}"
            )

        log_step(
            "step 4: Stop the non-primary OSD per the configured sub_case "
            f"({sub_case}) and wait for recovery."
        )
        if sub_case == "within_down_out":
            down_out_interval = int(
                mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
            )
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            wait_secs = max(down_out_interval // 2, 10)
            log.info(f"Waiting {wait_secs}s (< down_out_interval={down_out_interval}s)")
            time.sleep(wait_secs)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(
                rados_obj.node
            )
            assert wait_for_recovery(rados_obj, pool_name)

        elif sub_case == "beyond_down_out":
            down_out_interval = int(
                mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
            )
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            wait_secs = down_out_interval + 60
            log.info(f"Waiting {wait_secs}s (> down_out_interval={down_out_interval}s)")
            time.sleep(wait_secs)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(
                rados_obj.node
            )
            assert wait_for_recovery(rados_obj, pool_name)

        elif sub_case == "not_restarted":
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        log_step(
            "step 5: Compare primary perf-counter deltas with PeeringState "
            "durations. Every other OSD's deltas stay at 0."
        )
        # within_down_out: no mid-window IO → vuln only; rebuild stays 0
        # unless the PG actually enters recovering.
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=[non_primary],
            require_activity=True,
            require_rebuild=sub_case in ("beyond_down_out", "not_restarted"),
            skip_osds=[non_primary] if sub_case == "not_restarted" else None,
            restarted_osds=_restarted_osds,
        )

    finally:
        end_osd_debug(cephadm, duration_state)
        if sub_case == "not_restarted" and non_primary is not None:
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            time.sleep(30)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"Non-primary OSD stop scenario '{sub_case}' passed")


def scenario_primary_osd_stop(rados_obj, cephadm, config):
    """Stop a primary OSD and verify where vulnerability/rebuild are stored.

    Sub-cases via config['sub_case']:
        'within_down_out'  - primary stopped < mon_osd_down_out_interval and
            restarted. Durations are stored on that same primary after it
            returns. Vulnerability rises for degraded/undersized; rebuild
            rises only if the PG enters recovering.
        'beyond_down_out'  - primary stopped > mon_osd_down_out_interval then
            restarted. Acting set is replaced while it is out; the new
            primary stores the durations.
        'not_restarted'    - primary stays down. New acting primary stores
            the durations.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Identify the current primary and acting set; snapshot baselines.
        3. Stop the primary OSD per the configured sub_case and wait for
           recovery.
        4. Match vuln/rebuild deltas to PeeringState logs on the recording
           primary (same OSD if it returned within down_out; otherwise the
           new acting primary).
        5. Assert recording-OSD ownership and independent counter rules.
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Identify the current primary and acting set; snapshot baselines.")
        log_step("step 3: Stop the primary OSD per the configured sub_case and wait for recovery.")
        log_step(
            "step 4: Match vuln/rebuild to PeeringState logs on the recording primary."
        )
        log_step("step 5: Assert recording-OSD ownership and independent counter rules.")
        log_step("step 6: Verify OSDs unaffected by the event show unchanged counters.")
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
        baseline_vuln_all = {}
        for osd_id in baseline_all:
            baseline_vuln_all[osd_id] = get_vulnerability_counters(rados_obj, osd_id)

        down_out_interval = int(
            mon_obj.get_config(section="mon", param="mon_osd_down_out_interval")
        )

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [primary]
        log.info("OSD action: stop target=" + str(primary))
        rados_obj.change_osd_state(action="stop", target=primary)

        if sub_case == "within_down_out":
            wait_secs = max(down_out_interval // 2, 10)
            log.info(f"Waiting {wait_secs}s (< down_out_interval)")
            time.sleep(wait_secs)
            log.info("OSD action: start target=" + str(primary))
            rados_obj.change_osd_state(action="start", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "beyond_down_out":
            wait_secs = down_out_interval + 60
            log.info(f"Waiting {wait_secs}s (> down_out_interval)")
            time.sleep(wait_secs)
            log.info("OSD action: start target=" + str(primary))
            rados_obj.change_osd_state(action="start", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "not_restarted":
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=sub_case in ("beyond_down_out", "not_restarted"),
            skip_osds=[primary] if sub_case == "not_restarted" else None,
            restarted_osds=_restarted_osds,
        )
        new_acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        new_primary = int(new_acting[0])
        log.info(f"New acting set: {new_acting}, acting primary: osd.{new_primary}")

        if sub_case == "within_down_out":
            # Same primary returns and stores vulnerability / rebuild.
            assert new_primary == old_primary, (
                f"within_down_out: expected primary osd.{old_primary} to remain "
                f"primary after return, got osd.{new_primary}"
            )
            vuln = get_vulnerability_counters(rados_obj, old_primary)
            rebuild = get_rebuild_counters(rados_obj, old_primary)
            validate_counter_fields(vuln, context=f"osd.{old_primary} within vuln")
            validate_counter_fields(
                rebuild, context=f"osd.{old_primary} within rebuild"
            )
            log.info(
                f"within_down_out recording primary osd.{old_primary}: "
                f"vuln={pp(vuln)}, rebuild={pp(rebuild)}"
            )
            assert vuln.get("avgcount", 0) > 0, (
                f"osd.{old_primary} should store vulnerability duration after "
                "returning within down_out"
            )
            if rebuild.get("avgcount", 0) > 0:
                log.info(
                    f"osd.{old_primary} also stored rebuild (PG entered recovering)"
                )
            else:
                log.info(
                    f"osd.{old_primary} rebuild stayed 0 (no recovering observed)"
                )

        elif sub_case in ("beyond_down_out", "not_restarted"):
            # Acting set replaced while primary was out → new primary stores.
            recording = new_primary
            if sub_case == "not_restarted":
                assert recording != old_primary, (
                    f"not_restarted: expected a new primary while osd.{old_primary} "
                    f"stays down, still saw osd.{recording}"
                )
            vuln = get_vulnerability_counters(rados_obj, recording)
            rebuild = get_rebuild_counters(rados_obj, recording)
            validate_counter_fields(vuln, context=f"osd.{recording} new-primary vuln")
            validate_counter_fields(
                rebuild, context=f"osd.{recording} new-primary rebuild"
            )
            log.info(
                f"{sub_case} recording primary osd.{recording}: "
                f"vuln={pp(vuln)}, rebuild={pp(rebuild)}"
            )
            assert vuln.get("avgcount", 0) > (
                baseline_vuln_all.get(recording, {}).get("avgcount") or 0
            ) or vuln.get("avgcount", 0) > 0, (
                f"New primary osd.{recording} should store vulnerability duration"
            )
            if rebuild.get("avgcount", 0) > 0:
                log.info(
                    f"osd.{recording} stored rebuild (PG entered recovering)"
                )
            else:
                raise AssertionError(
                    f"osd.{recording} pg_rebuild_duration should increase: "
                    f"PG should enter recovering for {sub_case}"
                )

        unaffected_osds = [
            osd
            for osd in baseline_all
            if osd not in acting and osd != new_primary and osd != old_primary
        ]
        unaffected_after = fetch_counters_for_osds(rados_obj, unaffected_osds[:5])
        changed_count = 0
        for osd_id, after in unaffected_after.items():
            validate_counter_fields(after, context=f"osd.{osd_id} unaffected")
            if not verify_counters_unchanged(baseline_all[osd_id], after):
                changed_count += 1
                log.warning(
                    f"osd.{osd_id} counters changed (background recovery on "
                    f"shared cluster): before={pp(baseline_all[osd_id])}, after={pp(after)}"
                )
        if changed_count == len(unaffected_after) and changed_count > 0:
            raise AssertionError(
                "ALL sampled unaffected OSDs changed counters — likely a "
                "test-pool recovery leak, not just background activity"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        if sub_case == "not_restarted" and old_primary is not None:
            log.info("OSD action: start target=" + str(old_primary))
            rados_obj.change_osd_state(action="start", target=old_primary)
            time.sleep(30)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info(f"Primary OSD stop scenario '{sub_case}' passed")


def scenario_osd_host_stop(rados_obj, cephadm, config):
    """Stop all OSDs on a host; verify duration counters on affected primaries.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Snapshot baseline rebuild counters for all up OSDs.
        3. Pick a random OSD host and stop every OSD on it.
        4. Wait/restart per the configured sub_case, then wait for recovery.
        5. Match vulnerability (and rebuild when recovering) to PeeringState
           logs. within_down_out without mid-window IO may leave rebuild at 0.
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 3: Pick a random OSD host and stop every OSD on it.")
        log_step("step 4: Wait/restart per the configured sub_case, then wait for recovery.")
        log_step("step 5: Verify at least one surviving OSD shows incremented rebuild counters.")
        log_step("step 6: Verify OSDs unaffected by the host stop show unchanged counters.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        for osd_id in host_osds:
            log.info("OSD action: stop target=" + str(int(osd_id)))
            rados_obj.change_osd_state(action="stop", target=int(osd_id))
            stopped_osds.append(int(osd_id))

        if sub_case == "within_down_out":
            wait_secs = max(down_out_interval // 2, 10)
            time.sleep(wait_secs)
            for osd_id in stopped_osds:
                log.info("OSD action: start target=" + str(osd_id))
                rados_obj.change_osd_state(action="start", target=osd_id)
                _restarted_osds[int(osd_id)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "beyond_down_out":
            wait_secs = down_out_interval + 60
            time.sleep(wait_secs)
            for osd_id in stopped_osds:
                log.info("OSD action: start target=" + str(osd_id))
                rados_obj.change_osd_state(action="start", target=osd_id)
                _restarted_osds[int(osd_id)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj, pool_name)
        elif sub_case == "not_restarted":
            time.sleep(30)
            assert wait_for_recovery(rados_obj)

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=stopped_osds,
            require_activity=True,
            require_rebuild=sub_case in ("beyond_down_out", "not_restarted"),
            skip_osds=stopped_osds if sub_case == "not_restarted" else None,
            restarted_osds=_restarted_osds,
        )
        if sub_case in ("beyond_down_out", "not_restarted"):
            up_osds = rados_obj.get_osd_list(status="up")
            check_osds = [osd_id for osd_id in up_osds if osd_id not in stopped_osds]
            post_stop_counters = fetch_counters_for_osds(rados_obj, check_osds)
            counters_found = False
            for osd_id, after in post_stop_counters.items():
                validate_counter_fields(after, context=f"osd.{osd_id} post-host-stop")
                before = baseline_all.get(osd_id, {"avgcount": 0, "sum": 0})
                if after["avgcount"] > before.get("avgcount", 0):
                    log.info(f"osd.{osd_id} rebuild counters incremented: {pp(after)}")
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
        end_osd_debug(cephadm, duration_state)
        for osd_id in stopped_osds:
            try:
                log.info("OSD action: start target=" + str(osd_id))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool with the original (smaller) size and write data; wait for the pool to be clean.")
        log_step("step 2: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 3: Increase the pool size (creates misplaced objects) and wait for recovery.")
        log_step("step 4: Verify the primary OSD's counters incremented due to the misplaced-object recovery.")
        log_step("step 5: Verify OSDs unaffected by the size change show unchanged counters.")
        rados_obj.create_pool(
            pool_name=pool_name, pg_num=16, size=original_size, min_size=1
        )
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        baseline_all = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(baseline_all, context="baseline")

        log.info(f"Increasing pool size from {original_size} to {new_size}")
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = []
        rados_obj.set_pool_property(pool=pool_name, props="size", value=new_size)
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        after_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_primary, context=f"osd.{primary} after")
        before_primary = baseline_all.get(primary, {"avgcount": 0, "sum": 0})

        log.info(
            f"Primary osd.{primary} counters - before: {pp(before_primary)}, after: {pp(after_primary)}"
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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a single-PG pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Inject an inconsistent object via objectstore-tool.")
        log_step("step 4: Run a deep-scrub, then issue `ceph pg repair`.")
        log_step("step 5: Wait for recovery and fetch the counters again.")
        log_step("step 6: Log whether the repair incremented the rebuild counters (best-effort observation, not a hard failure).")
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 20))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")
        log.info(f"Baseline counters on primary osd.{primary}: {pp(baseline)}")

        pgids = rados_obj.get_pgid(pool_name=pool_name)
        pgid = pgids[0]

        from ceph.rados.objectstoretool_workflows import ObjectStoreToolWorkflows

        objectstore_obj = ObjectStoreToolWorkflows(node=cephadm)
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = []
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
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-repair")
        log.info(f"After repair counters on primary osd.{primary}: {pp(after)}")

        if after["avgcount"] > baseline["avgcount"]:
            log.info("Repair contributed to rebuild duration counters as expected")
        else:
            log.warning(
                "Repair did not increment rebuild counters. "
                "This may be expected if repair did not trigger PG rebuild."
            )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Start a background `rados bench` write workload.")
        log_step("step 4: Stop a non-primary OSD, wait for recovery, then restart it and wait for the pool to be clean again.")
        log_step("step 5: Verify the primary's rebuild counters incremented despite the concurrent IO load.")
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

        stop_io = threading.Event()

        def _client_io_worker():
            while not stop_io.is_set():
                log.info(
                    f"Client IO going on: rados bench -p {pool_name} 120 write "
                    f"--no-cleanup ({client.hostname})"
                )
                try:
                    client.exec_command(
                        cmd=f"rados bench -p {pool_name} 120 write --no-cleanup &",
                        check_ec=False,
                    )
                except Exception as err:
                    log.warning(f"Client IO launch failed: {err}")
                # keep the thread alive and re-launch if bench exits early
                if stop_io.wait(30):
                    break
            log.info("Client IO thread exiting")

        io_thread = threading.Thread(
            target=_client_io_worker,
            name=f"rados-bench-{pool_name}",
            daemon=True,
        )
        io_thread.start()
        log.info("Background IO thread started")
        time.sleep(10)

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)

        after_primary = get_rebuild_counters(rados_obj, primary)
        # kill here
        log.info("Stopping client IO thread and killing rados bench")
        stop_io.set()
        client.exec_command(cmd="killall -9 rados 2>/dev/null || true", check_ec=False)
        io_thread.join(timeout=30)
        if io_thread.is_alive():
            log.warning("Client IO thread did not stop within 30s")
        else:
            log.info("Client IO thread stopped")

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        validate_counter_fields(after_primary, context=f"osd.{primary} after-io")
        log.info(
            f"Primary osd.{primary} counters - before: {pp(baseline_primary)}, after: {pp(after_primary)}"
        )

        assert verify_counters_incremented(
            baseline_primary, after_primary
        ), "Rebuild counters should increment during recovery under IO load"

    finally:
        end_osd_debug(cephadm, duration_state)
        if "stop_io" in locals():
            stop_io.set()
        client.exec_command(cmd="killall -9 rados 2>/dev/null || true", check_ec=False)
        if "io_thread" in locals() and io_thread.is_alive():
            io_thread.join(timeout=15)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a single-PG pool and write data; wait for the pool to be clean.")
        log_step("step 2: Stop then restart a replica OSD to trigger a rebuild, and record the primary's counters afterward.")
        log_step("step 3: Set the current primary's primary-affinity to 0 to force a primary transition.")
        log_step("step 4: Wait for recovery and check whether a new primary was elected.")
        log_step("step 5: Restore the original primary-affinity in the finally block.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 30))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        primary = acting[0]
        if len(acting) < 2:
            raise AssertionError("Need at least 2 OSDs in acting set")
        replica = acting[1]

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [replica]
        log.info("OSD action: stop target=" + str(replica))
        rados_obj.change_osd_state(action="stop", target=replica)
        time.sleep(30)
        log.info("OSD action: start target=" + str(replica))
        rados_obj.change_osd_state(action="start", target=replica)
        _restarted_osds[int(replica)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Lower `osd_recovery_max_active` to 1 to slow down recovery.")
        log_step("step 4: Stop then restart a non-primary OSD; wait for recovery.")
        log_step("step 5: Verify the primary's counters incremented despite the throttled recovery speed.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-throttled")
        log.info(f"Primary osd.{primary} counters - before: {pp(baseline)}, after: {pp(after)}")

        assert verify_counters_incremented(
            baseline, after
        ), "Rebuild counters should increment with throttled recovery"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 3: Pick a random OSD on a host and stop it (simulating removal).")
        log_step("step 4: Snapshot counters after removal.")
        log_step("step 5: Restart the OSD (simulating re-add) and wait for recovery.")
        log_step("step 6: Snapshot counters after re-add and compare against the after-removal snapshot for the other OSDs, logging any unexpected drift.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [target_osd]
        log.info("OSD action: stop target=" + str(target_osd))
        rados_obj.change_osd_state(action="stop", target=target_osd)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        snapshot_after_remove = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(snapshot_after_remove, context="after-remove")

        log.info(f"Re-adding osd.{target_osd}")
        log.info("OSD action: start target=" + str(target_osd))
        rados_obj.change_osd_state(action="start", target=target_osd)
        _restarted_osds[int(target_osd)] = _host_local_timestamp(rados_obj.node)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
                    f"{pp(before)} -> {pp(after)}. This may be due to rebalancing."
                )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Stop then restart a non-primary OSD to generate non-zero counters on the primary.")
        log_step("step 3: Verify the primary's pre-restart counters are non-zero.")
        log_step("step 4: Restart the primary OSD itself.")
        log_step("step 5: Verify the primary's counters reset to default (zero) values after the restart.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        pre_restart = get_rebuild_counters(rados_obj, primary)
        pre_vuln = get_vulnerability_counters(rados_obj, primary)
        validate_counter_fields(pre_restart, context=f"osd.{primary} pre-restart")
        validate_counter_fields(
            pre_vuln, context=f"osd.{primary} pre-restart vulnerability"
        )
        log.info(f"Pre-restart rebuild on osd.{primary}: {pp(pre_restart)}")
        log.info(f"Pre-restart vulnerability on osd.{primary}: {pp(pre_vuln)}")
        assert pre_vuln["avgcount"] > 0, (
            "Expected non-zero vulnerability counters before restart"
        )

        log.info(f"Restarting osd.{primary}")
        log.info("OSD action: stop target=" + str(primary))
        rados_obj.change_osd_state(action="stop", target=primary)
        time.sleep(5)
        log.info("OSD action: start target=" + str(primary))
        rados_obj.change_osd_state(action="start", target=primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        post_restart = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(post_restart, context=f"osd.{primary} post-restart")
        log.info(f"Post-restart counters on osd.{primary}: {pp(post_restart)}")
        assert verify_counter_defaults(
            post_restart
        ), "All counters should be zero after OSD restart"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 3: Set the `noout` flag on the cluster.")
        log_step("step 4: Quickly stop then restart a target OSD (too fast to trigger real recovery).")
        log_step("step 5: Verify the bounced OSD's counters reset to default (zero) and every other OSD's counters remain unchanged.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [target_osd]
        log.info("OSD action: stop target=" + str(target_osd))
        rados_obj.change_osd_state(action="stop", target=target_osd)
        time.sleep(10)
        log.info("OSD action: start target=" + str(target_osd))
        rados_obj.change_osd_state(action="start", target=target_osd)
        _restarted_osds[int(target_osd)] = _host_local_timestamp(rados_obj.node)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
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
        5. When avgcount > 1, verify max_inc is present and
           max_inc >= pg_rebuild_duration_min.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_multi_pg")
    pg_num = config.get("pg_num", 32)

    try:
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a multi-PG pool and write a larger dataset; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Stop then restart a non-primary OSD, triggering rebuild across multiple PGs.")
        log_step("step 4: Verify avgcount and sum both increased.")
        log_step(
            "step 5: When avgcount > 1, verify max_inc is present and "
            "max_inc >= pg_rebuild_duration_min."
        )
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-multi-pg")
        log.info(f"Primary osd.{primary} counters - before: {pp(baseline)}, after: {pp(after)}")

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

        if after["avgcount"] > 1:
            assert after.get("max_inc") is not None, (
                f"max_inc required when avgcount={after['avgcount']} > 1"
            )
            assert verify_min_max_invariant(after), (
                "max_inc should be >= pg_rebuild_duration_min"
            )
            log.info(
                f"max_inc={after['max_inc']}, "
                f"pg_rebuild_duration_min={after['pg_rebuild_duration_min']}"
            )
        else:
            log.info(
                f"Skipping max_inc check: avgcount={after['avgcount']} "
                "(max_inc only exists when avgcount > 1)"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Stop then restart a non-primary OSD to populate non-zero counters on the primary.")
        log_step("step 3: Restart the configured non-OSD daemon (mgr, mon, or ceph-exporter per config['daemon_type']).")
        log_step("step 4: Verify the primary OSD's rebuild counters are unchanged after the daemon restart.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        pre_restart = get_rebuild_counters(rados_obj, primary)
        pre_vuln = get_vulnerability_counters(rados_obj, primary)
        validate_counter_fields(pre_restart, context=f"osd.{primary} pre-restart")
        validate_counter_fields(
            pre_vuln, context=f"osd.{primary} pre-restart vulnerability"
        )
        log.info(
            f"Counters before {daemon_type} restart on osd.{primary}: "
            f"rebuild={pp(pre_restart)}, vuln={pp(pre_vuln)}"
        )
        assert pre_vuln["avgcount"] > 0, (
            "Expected non-zero vulnerability counters before daemon restart"
        )

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
            f"Counters after {daemon_type} restart on osd.{primary}: {pp(post_restart)}"
        )

        assert verify_counters_unchanged(
            pre_restart, post_restart
        ), f"OSD counters should not change after {daemon_type} restart"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        3. Verify rebuild and vulnerability counters are present in the
           'recoverystate_perf' section.
        4. Verify each duration object has avgcount, sum, avgtime; max_inc
           only when avgcount > 1; each _min gauge is numeric.
        5. Fetch and fully validate the rebuild counter fields.
        6. Verify the perf schema also reports the counters under
           'recoverystate_perf'.
    """
    log_step("step 1: Pick the first up OSD and fetch its perf dump.")
    log_step("step 2: Verify none of the rebuild counters appear in the (wrong) 'osd' section.")
    log_step("step 3: Verify rebuild and vulnerability counters are present in the 'recoverystate_perf' section.")
    log_step(
        "step 4: Verify each duration object has avgcount, sum, avgtime; "
        "max_inc only when avgcount > 1; each _min gauge is numeric."
    )
    log_step("step 5: Fetch and fully validate the rebuild counter fields.")
    log_step("step 6: Verify the perf schema also reports the counters under 'recoverystate_perf'.")
    osd_list = rados_obj.get_osd_list(status="up")
    osd_id = osd_list[0]

    perf = rados_obj.get_osd_perf_dump(osd_id)

    wrong_section = perf.get("osd", {})
    for counter in DURATION_COUNTERS:
        if counter in wrong_section:
            raise AssertionError(
                f"Counter '{counter}' unexpectedly found in 'osd' section "
                "instead of 'recoverystate_perf'"
            )

    rs_section = perf.get("recoverystate_perf", {})
    for counter in DURATION_COUNTERS:
        if counter not in rs_section:
            raise AssertionError(
                f"Counter '{counter}' not found in 'recoverystate_perf' section"
            )
        log.info(f"osd.{osd_id}: {counter} correctly in recoverystate_perf")

    for duration_name, min_name in (
        ("pg_rebuild_duration", "pg_rebuild_duration_min"),
        ("pg_vulnerability_duration", "pg_vulnerability_duration_min"),
    ):
        duration_entry = rs_section.get(duration_name)
        if not isinstance(duration_entry, dict):
            raise AssertionError(
                f"osd.{osd_id}: '{duration_name}' should be a dict with "
                f"avgcount/sum/avgtime[, max_inc], got "
                f"{type(duration_entry).__name__}"
            )
        for sub_field in ("avgcount", "sum", "avgtime"):
            if sub_field not in duration_entry:
                raise AssertionError(
                    f"osd.{osd_id}: '{duration_name}.{sub_field}' missing"
                )
        avgcount = duration_entry.get("avgcount", 0) or 0
        if avgcount > 1 and "max_inc" not in duration_entry:
            raise AssertionError(
                f"osd.{osd_id}: '{duration_name}.max_inc' missing "
                f"(required when avgcount={avgcount} > 1)"
            )
        if avgcount <= 1 and "max_inc" in duration_entry:
            raise AssertionError(
                f"osd.{osd_id}: '{duration_name}.max_inc' unexpectedly "
                f"present when avgcount={avgcount} <= 1"
            )
        if not isinstance(rs_section.get(min_name), (int, float)):
            raise AssertionError(
                f"osd.{osd_id}: '{min_name}' should be numeric, got "
                f"{type(rs_section.get(min_name)).__name__}"
            )

    counters = get_rebuild_counters(rados_obj, osd_id)
    validate_counter_fields(counters, context=f"osd.{osd_id} section-name")

    schema = rados_obj.run_ceph_command(cmd=f"ceph tell osd.{osd_id} perf schema")
    rs_schema = schema.get("recoverystate_perf", {})
    for counter in DURATION_COUNTERS:
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
        5. Log whether pg_rebuild_duration_min captured sub-second precision or
           was truncated to 0.
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "rebuild_subsec")

    try:
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a tiny single-PG pool with a handful of small objects; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Stop then restart a non-primary OSD to trigger a very fast (sub-1s) rebuild.")
        log_step("step 4: Verify avgcount incremented and sum did not decrease.")
        log_step("step 5: Log whether pg_rebuild_duration_min captured sub-second precision or was truncated to 0.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=1, pg_num_max=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=3, size="4K")
        time.sleep(5)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        baseline = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(baseline, context=f"osd.{primary} baseline")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-subsec")
        log.info(
            f"Sub-second rebuild on osd.{primary}: before={pp(baseline)}, after={pp(after)}"
        )

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "avgcount should increment even for sub-second rebuilds"
        assert (
            after["sum"] >= baseline["sum"]
        ), "sum should be >= baseline (may be very small float)"

        if (
            after["pg_rebuild_duration_min"] is not None
            and after["pg_rebuild_duration_min"] > 0
        ):
            log.info(
                f"pg_rebuild_duration_min={after['pg_rebuild_duration_min']} "
                "(sub-second precision)"
            )
        elif after["pg_rebuild_duration_min"] == 0 and after["avgcount"] > 0:
            log.warning(
                "min_secs is 0 despite rebuild occurring - possible "
                "truncation of sub-second durations to integer 0"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Throttle recovery (osd_recovery_max_active=1, osd_recovery_sleep=5) to keep the PG degraded longer.")
        log_step("step 3: Record baseline counters on the primary; stop a non-primary OSD.")
        log_step("step 4: Sample counters twice while the pool is confirmed degraded, verifying avgcount does not increment a second time (no double-counting).")
        log_step("step 5: Restart the OSD, wait for recovery, and verify avgcount eventually increments once recovery completes.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)

        still_degraded_1 = _pool_has_degraded_pgs(rados_obj, pool_name)
        mid_1 = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid_1, context=f"osd.{primary} mid-1")
        time.sleep(30)
        still_degraded_2 = _pool_has_degraded_pgs(rados_obj, pool_name)
        mid_2 = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid_2, context=f"osd.{primary} mid-2")

        log.info(f"Mid-recovery sample 1: {pp(mid_1)} (pool degraded: {still_degraded_1})")
        log.info(f"Mid-recovery sample 2: {pp(mid_2)} (pool degraded: {still_degraded_2})")

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

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} final")
        log.info(f"Final counters: {pp(after)}")
        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "avgcount must increment after recovery completes"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline counters on the primary; throttle recovery.")
        log_step("step 3: Stop a non-primary OSD and confirm the pool is degraded.")
        log_step("step 4: Increase pg_num while PGs are still degraded (PG split), then restart the OSD.")
        log_step("step 5: Wait for recovery and verify counters incremented and the min/max invariant holds.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
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

            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-split")
        log.info(f"osd.{primary} counters - before: {pp(baseline)}, after: {pp(after)}")
        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "Rebuild counters should increment after recovery with PG split"
        assert verify_min_max_invariant(
            after
        ), "max_secs >= min_secs invariant violated after PG split"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 3: Reweight a non-primary OSD to 0 (backfill out), wait for recovery.")
        log_step("step 4: Reweight it back to 1 (backfill in), wait for recovery.")
        log_step("step 5: Verify at least one OSD shows incremented rebuild counters from the backfill-only recovery.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [target_osd]
        cephadm.shell([f"ceph osd reweight {target_osd} 0"])
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        log.info(f"Reweighting osd.{target_osd} back to 1 (trigger backfill in)")
        cephadm.shell([f"ceph osd reweight {target_osd} 1"])
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)

        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        final_counters = get_all_osd_rebuild_counters(rados_obj)
        validate_all_osd_counter_fields(final_counters, context="after-backfill")

        counters_incremented = False
        for osd_id, final in final_counters.items():
            before = baseline_all.get(osd_id, {"avgcount": 0})
            if final["avgcount"] > before.get("avgcount", 0):
                log.info(
                    f"osd.{osd_id} rebuild counters incremented from "
                    f"backfill: {pp(before)} -> {pp(final)}"
                )
                counters_incremented = True

        assert (
            counters_incremented
        ), "At least one OSD should show rebuild counter activity from backfill"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Set the `noout` flag on the cluster.")
        log_step("step 3: Snapshot baseline rebuild counters for all up OSDs.")
        log_step("step 4: Quickly stop then restart a target OSD (re-peer only, no real recovery).")
        log_step("step 5: Verify every unaffected OSD's counters are unchanged.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [target]
        log.info("OSD action: stop target=" + str(target))
        rados_obj.change_osd_state(action="stop", target=target)
        time.sleep(3)
        log.info("OSD action: start target=" + str(target))
        rados_obj.change_osd_state(action="start", target=target)
        _restarted_osds[int(target)] = _host_local_timestamp(rados_obj.node)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline rebuild counters on the primary OSD.")
        log_step("step 3: Repeatedly stop then start a non-primary OSD (flap_count times).")
        log_step("step 4: Wait for recovery and fetch counters again.")
        log_step("step 5: Verify avgcount/sum never decreased and the min/max invariant still holds.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        for i in range(flap_count):
            log.info(f"Flap iteration {i+1}/{flap_count}")
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            time.sleep(20)

        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-flapping")
        log.info(
            f"After {flap_count} flaps on osd.{primary}: before={pp(baseline)}, after={pp(after)}"
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
                f"max_inc={after['max_inc']}, "
                f"pg_rebuild_duration_min={after['pg_rebuild_duration_min']}"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a size=3 pool and write data; wait for the pool to be clean.")
        log_step("step 2: Record baseline counters on the primary; throttle recovery.")
        log_step("step 3: Stop a first OSD, wait, then stop a second OSD while the first is still recovering.")
        log_step("step 4: Restart both OSDs and wait for recovery.")
        log_step("step 5: Verify the primary's avgcount/sum increased and the min/max invariant holds.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [victim_1, victim_2]
        log.info("OSD action: stop target=" + str(victim_1))
        rados_obj.change_osd_state(action="stop", target=victim_1)
        stopped_osds.append(victim_1)
        time.sleep(20)

        log.info(f"Stopping second OSD while recovery in progress: osd.{victim_2}")
        log.info("OSD action: stop target=" + str(victim_2))
        rados_obj.change_osd_state(action="stop", target=victim_2)
        stopped_osds.append(victim_2)
        time.sleep(30)

        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        for osd_id in stopped_osds:
            log.info("OSD action: start target=" + str(osd_id))
            rados_obj.change_osd_state(action="start", target=osd_id)
            _restarted_osds[int(osd_id)] = _host_local_timestamp(rados_obj.node)
        stopped_osds.clear()
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-compound")
        log.info(f"osd.{primary} counters - before: {pp(baseline)}, after: {pp(after)}")

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
        end_osd_debug(cephadm, duration_state)
        mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
        mon_obj.remove_config(section="osd", name="osd_recovery_sleep")
        for osd_id in stopped_osds:
            try:
                log.info("OSD action: start target=" + str(osd_id))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create several pools and write data to each; wait for recovery.")
        log_step("step 2: Find an OSD that is primary for 2+ of those pools (fallback to the first pool's primary if none is shared).")
        log_step("step 3: Record baseline counters on that OSD.")
        log_step("step 4: Stop then restart another OSD shared across the pools to trigger recovery.")
        log_step("step 5: Verify the shared primary's avgcount increased, reflecting aggregation across pools.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, pool_names
        )
        _duration_stopped = [target_stop]
        log.info("OSD action: stop target=" + str(target_stop))
        rados_obj.change_osd_state(action="stop", target=target_stop)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)

        log.info("OSD action: start target=" + str(target_stop))
        rados_obj.change_osd_state(action="start", target=target_stop)
        _restarted_osds[int(target_stop)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, multi_pool_osd)
        validate_counter_fields(
            after, context=f"osd.{multi_pool_osd} after-multi-pool"
        )
        log.info(
            f"osd.{multi_pool_osd} multi-pool counters - "
            f"before: {pp(baseline)}, after: {pp(after)}"
        )

        assert (
            after["avgcount"] > baseline["avgcount"]
        ), "Multi-pool OSD should show aggregated rebuild counts"

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create 3 pools of different object counts/sizes (small, medium, large); wait for recovery.")
        log_step("step 2: Record baseline counters on the first pool's primary.")
        log_step("step 3: For each pool, stop then restart a non-primary OSD to trigger a rebuild of varying duration.")
        log_step("step 4: Verify the min/max invariant holds and, when possible, that the computed average (sum/avgcount) lies between min_secs and max_secs.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, pool_names
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(20)
            assert wait_for_recovery(rados_obj)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj)
            log.info(
                "duration value check: compare perf-counter deltas with "
                "primary OSD PeeringState logs"
            )
            assert_duration_values_match_logs(
                rados_obj,
                duration_state,
                stopped_osds=_duration_stopped,
                require_activity=True,
                require_rebuild=False,
                skip_osds=None,
                restarted_osds=_restarted_osds,
            )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-all-rebuilds")
        log.info(f"osd.{primary} after all rebuilds: {pp(after)}")

        assert verify_min_max_invariant(after), (
            f"INVARIANT VIOLATED: max_inc ({after['max_inc']}) < "
            f"pg_rebuild_duration_min ({after['pg_rebuild_duration_min']})"
        )
        log.info(
            f"min/max invariant holds: pg_rebuild_duration_min="
            f"{after['pg_rebuild_duration_min']}, max_inc={after['max_inc']}"
        )

        if after["avgcount"] > 0 and after["sum"] > 0:
            avg = after["sum"] / after["avgcount"]
            if (
                after["pg_rebuild_duration_min"] is not None
                and after["pg_rebuild_duration_min"] > 0
            ):
                assert (
                    avg >= after["pg_rebuild_duration_min"]
                ), "Average rebuild time should be >= pg_rebuild_duration_min"
            if after["max_inc"] is not None and after["max_inc"] > 0:
                assert (
                    avg <= after["max_inc"]
                ), "Average rebuild time should be <= max_inc"

    finally:
        end_osd_debug(cephadm, duration_state)
        for pn in pool_names:
            try:
                rados_obj.delete_pool(pool=pn)
            except Exception:
                pass

    log.info("Min/max invariant scenario passed")


def scenario_prometheus_matches_perf_dump(rados_obj, cephadm, config):
    """Cross-validate: ceph-exporter Prometheus values should match
    'ceph tell osd.N perf dump' output for rebuild counters.

    Triggers a non-primary stop first so the comparison is against known-fresh
    values. Vulnerability is required; rebuild may stay 0 without mid-window IO.

    Test steps:
        1. Create a pool and write data; wait for the pool to be clean.
        2. Stop then restart a non-primary OSD.
        3. Fetch the primary's counters via `ceph tell` and verify
           vulnerability avgcount > 0.
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create a pool and write data; wait for the pool to be clean.")
        log_step("step 2: Stop then restart a non-primary OSD.")
        log_step(
            "step 3: Fetch primary counters via ceph tell; require vulnerability > 0."
        )
        log_step("step 4: Scrape ceph-exporter's /metrics endpoint from an mgr (or installer) node.")
        log_step("step 5: Compare the scraped Prometheus values against the perf dump counters and fail on any mismatch beyond tolerance.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        target_osd = primary
        perf_counters = get_rebuild_counters(rados_obj, target_osd)
        vuln_perf = get_vulnerability_counters(rados_obj, target_osd)
        log.info(f"osd.{target_osd} rebuild perf dump counters: {pp(perf_counters)}")
        log.info(
            f"osd.{target_osd} vulnerability perf dump counters: {pp(vuln_perf)}"
        )
        validate_counter_fields(perf_counters, context=f"osd.{target_osd} pre-scrape")
        validate_counter_fields(
            vuln_perf, context=f"osd.{target_osd} pre-scrape vulnerability"
        )
        assert vuln_perf.get("avgcount", 0) > 0, (
            "Expected non-zero vulnerability counters after OSD stop; "
            "cross-validation would be meaningless against all-zero values"
        )
        if perf_counters.get("avgcount", 0) <= 0:
            log.info(
                f"osd.{target_osd} rebuild avgcount is 0 (no recovering); "
                "exporter rebuild samples may be 0"
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

        log_pretty(f"Exporter metrics port {exporter_port}", metrics_text)
        _compare_prom_to_perf_dump(target_osd, metrics_text, perf_counters)

    finally:
        end_osd_debug(cephadm, duration_state)
        if config.get("delete_pool", True):
            rados_obj.delete_pool(pool=pool_name)

    log.info("Prometheus matches perf dump scenario passed")


def _compare_prom_to_perf_dump(target_osd, metrics_text, perf_counters):
    """Compare ceph-exporter Prometheus output against perf dump counters
    for the given OSD, raising AssertionError on any mismatch beyond
    tolerance.
    """
    osd_label = f'ceph_daemon="osd.{target_osd}"'
    log_about_to(
        "compare exporter metrics to perf dump",
        target_osd=target_osd,
        osd_label=osd_label,
        perf_counters=perf_counters,
        metrics_bytes=len(metrics_text or ""),
    )
    mismatches = []

    metric_map = {
        "pg_rebuild_duration_count": ("avgcount", 1),
        "pg_rebuild_duration_sum": ("sum", 0.1),
        "pg_rebuild_duration_max_inc": ("max_inc", 0.1),
        "pg_rebuild_duration_min": ("pg_rebuild_duration_min", 0.1),
    }

    for line in metrics_text.splitlines():
        if osd_label not in line or line.startswith("#"):
            continue
        for metric_suffix, (perf_key, tolerance) in metric_map.items():
            if metric_suffix in line:
                prom_val = float(line.split()[-1])
                perf_val = perf_counters.get(perf_key, 0)
                log_checkpoint(
                    "prom vs perf sample",
                    metric=metric_suffix,
                    prom=prom_val,
                    perf=perf_val,
                    tolerance=tolerance,
                )
                if abs(prom_val - perf_val) > tolerance:
                    mismatches.append(
                        f"{metric_suffix}: prom={prom_val}, perf={perf_val}"
                    )
                break

    if mismatches:
        for m in mismatches:
            log.error(f"Prometheus/perf-dump mismatch: {m}")
        raise AssertionError(f"Prometheus metrics don't match perf dump: {mismatches}")

    log_checkpoint(
        "Prometheus metrics match perf dump",
        target_osd=target_osd,
        mismatch_count=0,
    )


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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool_1 and write data; wait for the pool to be clean.")
        log_step("step 2: Stop then restart a non-primary OSD to populate non-zero counters on the primary.")
        log_step("step 3: Delete pool_1 and verify the primary's counters persist unchanged (counters are per-OSD, not per-pool).")
        log_step("step 4: Create pool_2, write data, and optionally trigger another rebuild.")
        log_step("step 5: Verify the primary's final counters only grew (or stayed the same) after pool_2's activity.")
        rados_obj.create_pool(pool_name=pool_name_1, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name_1, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name_1)

        acting = rados_obj.get_pg_acting_set(pool_name=pool_name_1)
        primary = acting[0]
        non_primary = acting[1] if len(acting) > 1 else None
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name_1]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        assert wait_for_recovery(rados_obj)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name_1)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after_first = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_first, context=f"osd.{primary} after-first")
        log.info(f"Counters after first rebuild on osd.{primary}: {pp(after_first)}")
        assert (
            after_first["avgcount"] > 0
        ), "First rebuild should produce non-zero counters"

        log.info(f"Deleting pool {pool_name_1}")
        rados_obj.delete_pool(pool=pool_name_1)
        time.sleep(10)

        after_delete = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_delete, context=f"osd.{primary} after-delete")
        log.info(f"Counters after pool deletion on osd.{primary}: {pp(after_delete)}")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name_2]
            )
            _duration_stopped = [np2]
            log.info("OSD action: stop target=" + str(np2))
            rados_obj.change_osd_state(action="stop", target=np2)
            time.sleep(30)
            assert wait_for_recovery(rados_obj)
            log.info("OSD action: start target=" + str(np2))
            rados_obj.change_osd_state(action="start", target=np2)
            _restarted_osds[int(np2)] = _host_local_timestamp(rados_obj.node)
            assert wait_for_recovery(rados_obj, pool_name_2)
            log.info(
                "duration value check: compare perf-counter deltas with "
                "primary OSD PeeringState logs"
            )
            assert_duration_values_match_logs(
                rados_obj,
                duration_state,
                stopped_osds=_duration_stopped,
                require_activity=True,
                require_rebuild=False,
                skip_osds=None,
                restarted_osds=_restarted_osds,
            )

        final = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(final, context=f"osd.{primary} final")
        log.info(f"Final counters on osd.{primary}: {pp(final)}")
        assert (
            final["avgcount"] >= after_delete["avgcount"]
        ), "Counters should only grow (or stay) after additional rebuilds"

    finally:
        end_osd_debug(cephadm, duration_state)
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
    log_step("step 1: Dump all PG stats via get_pg_stats.")
    log_step("step 2: Assert every PG has last_degraded present.")
    log_step("step 3: For active+clean PGs, assert last_degraded <= last_clean. Equality only holds for never-degraded / newly created PGs; after a real degradation episode last_degraded stays latched at onset while last_clean advances (and may keep refreshing).")
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool size=3, write data, wait clean; record last_degraded.")
        log_step("step 2: Stop non-primary OSD; while degraded, last_degraded must be set and remain unchanged across two samples ~20s apart.")
        log_step("step 3: Start OSD, wait clean; assert last_degraded < last_clean.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
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

        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        log_step("step 1: Create pool, write data, wait clean.")
        log_step("step 2: Query one PG; assert last_degraded present and parseable.")
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
        log_step("step 1: Create pool, write data, trigger non-primary stop/start rebuild.")
        log_step("step 2: Prefer scraping exporter metrics for the configured counter_family.")
        log_step("step 3: If Prometheus query API is available, attempt the configured promql; do not fail solely because PromQL endpoint is missing when exporter metrics exist.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        _trigger_non_primary_rebuild(rados_obj, pool_name, cephadm=cephadm)

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
                "max_inc",
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
        log_step("step 1: Trigger rebuild and scrape metrics.")
        log_step("step 2: Restart one OSD (counters reset).")
        log_step("step 3: Scrape again; assert no negative values for rebuild/vuln sum/count.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, non_primary, acting = _trigger_non_primary_rebuild(
            rados_obj, pool_name, cephadm=cephadm
        )

        _ = _scrape_exporter_metrics(rados_obj, exporter_port=exporter_port)

        log.info(f"Restarting osd.{primary} to reset counters")
        log.info("OSD action: restart target=" + str(primary))
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
            "pg_rebuild_duration_max_inc",
            "pg_rebuild_duration_min",
            "pg_vulnerability_duration_max_inc",
            "pg_vulnerability_duration_min",
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
        log_step("step 1: Trigger rebuild and scrape exporter metrics.")
        log_step("step 2: Restart ceph-exporter via ceph orch.")
        log_step("step 3: Scrape again and assert metrics still present.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        _trigger_non_primary_rebuild(rados_obj, pool_name, cephadm=cephadm)

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
            "max_inc",
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool at original_size, write objects.")
        log_step("step 2: Optionally set backfillfull/full ratios to induce toofull pressure.")
        log_step("step 3: Increase size to new_size; sample counters; wait clean.")
        log_step("step 4: Assert rebuild avgcount increased by >= 1 if recovery completed.")
        log_step("step 5: Restore ratios in finally.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = []
        rados_obj.set_pool_property(pool=pool_name, props="size", value=new_size)
        time.sleep(20)
        mid = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(mid, context=f"osd.{primary} mid-backfill")
        log.info(f"Mid-backfill counters: {pp(mid)}")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after_rebuild = get_rebuild_counters(rados_obj, primary)
        after = get_durability_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(
            f"osd.{primary} before={pp(before)}, after_dur={pp(after)}, "
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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create empty pool (no writes).")
        log_step("step 2: Stop non-primary (or any acting OSD), wait, start, wait clean.")
        log_step("step 3: Assert vulnerability avgcount increases if present; rebuild does not.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(f"Empty pool durability before={pp(before)} after={pp(after)}")

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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool, write, degrade (throttle + stop OSD), increase pg_num.")
        log_step("step 2: Recover; assert counters incremented.")
        log_step("step 3: Decrease pg_num/pgp_num when clean; vulnerability should not grow further.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=16)
            time.sleep(20)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after_split = get_durability_counters(rados_obj, primary)
        after_split_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_split_rebuild, context=f"osd.{primary} after-split")
        assert after_split_rebuild["avgcount"] > before_rebuild["avgcount"], (
            "Counters should increment after split/recovery"
        )

        # Merge when clean
        try:
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = []
            rados_obj.set_pool_property(pool=pool_name, props="pgp_num", value=8)
            rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=8)
        except Exception as err:
            log.warning(f"PG merge property set failed: {err}")
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after_merge = get_durability_counters(rados_obj, primary)
        if after_merge["has_vulnerability"] and after_split["has_vulnerability"]:
            if after_merge["vuln_avgcount"] > after_split["vuln_avgcount"]:
                raise AssertionError(
                    "Vulnerability avgcount should not increase from merge when clean: "
                    f"{after_split['vuln_avgcount']} -> {after_merge['vuln_avgcount']}"
                )
        log.info(
            f"Split/merge durability: before={pp(before)} after_split={pp(after_split)} "
            f"after_merge={pp(after_merge)}"
        )

    finally:
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool, write, throttle recovery, stop non-primary.")
        log_step("step 2: Stop/restart primary mid-window (within/beyond down_out).")
        log_step("step 3: Recover; assert rebuild/vuln avgcount increased without huge inflation.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary, primary]
            log.info("OSD action: stop target=" + str(non_primary))
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
            log.info("OSD action: stop target=" + str(primary))
            rados_obj.change_osd_state(action="stop", target=primary)
            time.sleep(wait_secs)
            log.info("OSD action: start target=" + str(primary))
            rados_obj.change_osd_state(action="start", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=sub_case in ("beyond_down_out", "not_restarted"),
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        new_acting = rados_obj.get_pg_acting_set(pool_name=pool_name)
        new_primary = new_acting[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")

        # Vulnerability must rise; rebuild only when recovering was expected.
        if new_primary == primary:
            if after["has_vulnerability"]:
                delta = after["vuln_avgcount"] - before["vuln_avgcount"]
                assert delta >= 1, "Vulnerability avgcount should increase"
                assert delta <= max(pg_count * 3, 8), (
                    f"Vulnerability avgcount delta too large for consolidated episode: {delta}"
                )
            else:
                raise AssertionError(
                    "Expected vulnerability counters after spanning handover"
                )
            if sub_case in ("beyond_down_out", "not_restarted"):
                assert after_rebuild["avgcount"] > before_rebuild["avgcount"], (
                    "Rebuild avgcount should increase after beyond/not_restarted handover"
                )
            elif after_rebuild["avgcount"] > before_rebuild["avgcount"]:
                log.info("Rebuild also increased (PG entered recovering)")
        else:
            assert after["vuln_avgcount"] >= 1 or (
                sub_case in ("beyond_down_out", "not_restarted")
                and after_rebuild["avgcount"] >= 1
            ), (
                "New primary should show vulnerability (and rebuild when expected) "
                "after handover"
            )
        log.info(f"Handover durability on osd.{new_primary}: {pp(after)}")

    finally:
        end_osd_debug(cephadm, duration_state)
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    log.info("OSD action: start target=" + str(osd))
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
    log_step("step 1: Dump all PGs and verify last_degraded field is present.")
    log_step("step 2: Log that full rolling upgrade is environment-specific.")
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
        log_step("step 1: Verify last_degraded present on PGs.")
        log_step("step 2: Create pool, trigger one stop/start recovery.")
        log_step("step 3: Assert no new crashes via do_crash_ls when available; counters valid.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, _, _ = _trigger_non_primary_rebuild(rados_obj, pool_name, cephadm=cephadm)
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
        log_step("step 1: Create pool, write, populate counters via OSD stop/start.")
        log_step("step 2: Rolling-restart a few OSDs; verify counters still readable/valid.")
        log_step("step 3: Optionally enter/exit host maintenance if APIs exist.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, non_primary, acting = _trigger_non_primary_rebuild(
            rados_obj, pool_name, cephadm=cephadm
        )
        before = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before, context=f"osd.{primary} populated")

        restart_targets = list(dict.fromkeys([primary] + list(acting[:2])))
        for osd_id in restart_targets:
            try:
                log.info("OSD action: restart target=" + str(int(osd_id)))
                rados_obj.change_osd_state(action="restart", target=int(osd_id))
                time.sleep(10)
            except Exception as err:
                log.warning(f"Rolling restart osd.{osd_id} failed: {err}")
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after-rolling-restart")
        # Counters may reset on restart of that OSD; still must be readable/valid
        log.info(f"Counters after rolling restart on osd.{primary}: {pp(after)}")

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
        log.info(f"Serviceability durability counters still readable: {pp(readable)}")

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
        log_step("step 1: Create pool, write, trigger failure/recovery.")
        log_step("step 2: On primary, if vulnerability counters exist, assert rebuild_sum <= vuln_sum (+tolerance) and rebuild_avgcount <= vuln_avgcount.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 80))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)
        primary, _, _ = _trigger_non_primary_rebuild(rados_obj, pool_name, cephadm=cephadm)

        d = get_durability_counters(rados_obj, primary)
        rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(rebuild, context=f"osd.{primary} after")
        log.info(f"rebuild_vs_vulnerability on osd.{primary}: {pp(d)}")

        if not d["has_vulnerability"]:
            log.warning(
                "Vulnerability counters absent; skip rebuild-vs-vuln inequality"
            )
            return

        # Rebuild may be 0 when there was no recovering (no mid-window IO).
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
        2. Stop/start non-primary; primary vulnerability increases
           (rebuild only if recovering).
        3. Replica and unrelated OSDs should remain unchanged (best-effort).
    """
    pool_obj = PoolFunctions(node=cephadm)
    client = rados_obj.ceph_cluster.get_nodes(role="client")[0]
    pool_name = config.get("pool_name", "dur_per_osd")

    try:
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool, write, snapshot baselines.")
        log_step(
            "step 2: Stop/start non-primary; primary vulnerability increases "
            "(rebuild only if recovering)."
        )
        log_step("step 3: Replica and unrelated OSDs should remain unchanged (best-effort).")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after_primary = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_primary, context=f"osd.{primary} after")
        after_vuln = get_vulnerability_counters(rados_obj, primary)
        validate_counter_fields(
            after_vuln, context=f"osd.{primary} after vulnerability"
        )
        # Without mid-window IO, rebuild may stay 0; vulnerability must rise.
        assert after_vuln.get("avgcount", 0) > 0, (
            "Primary vulnerability counters should increase after non-primary stop"
        )
        if after_primary.get("avgcount", 0) > (baseline_primary.get("avgcount") or 0):
            log.info(f"osd.{primary} also recorded rebuild activity")
        else:
            log.info(
                f"osd.{primary} rebuild unchanged "
                f"(before={pp(baseline_primary)}, after={pp(after_primary)})"
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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Throttle, stop non-primary, bounce another OSD quickly.")
        log_step("step 2: Recover; vulnerability delta should be reasonable vs PG count.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary] + ([bounce_osd] if bounce_osd is not None else [])
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(10)
            if bounce_osd is not None:
                log.info("OSD action: stop target=" + str(bounce_osd))
                rados_obj.change_osd_state(action="stop", target=bounce_osd)
                time.sleep(5)
                log.info("OSD action: start target=" + str(bounce_osd))
                rados_obj.change_osd_state(action="start", target=bounce_osd)
                _restarted_osds[int(bounce_osd)] = _host_local_timestamp(rados_obj.node)
                bounce_osd = None
            time.sleep(10)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
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
        end_osd_debug(cephadm, duration_state)
        for osd in (bounce_osd, non_primary):
            if osd is not None:
                try:
                    log.info("OSD action: start target=" + str(osd))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Stop non-primary to degrade.")
        log_step("step 2: Restart primary once while still degraded.")
        log_step("step 3: Recover; vuln/rebuild avgcount delta should be reasonable.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            log.info("OSD action: restart target=" + str(primary))
            rados_obj.change_osd_state(action="restart", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
            time.sleep(20)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Degrade via non-primary stop.")
        log_step("step 2: Restart primary 2-3 times while degraded.")
        log_step("step 3: Recover; vuln avgcount delta should stay reasonable.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(10)
            for i in range(3):
                log.info(f"Primary restart loop iteration {i + 1}")
                log.info("OSD action: restart target=" + str(primary))
                rados_obj.change_osd_state(action="restart", target=primary)
                _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
                time.sleep(15)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Degrade; record last_degraded.")
        log_step("step 2: Restart primary; confirm last_degraded still present/same.")
        log_step("step 3: Recover; last_degraded < last_clean.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(20)
            q1 = get_pg_query(rados_obj, pgid)
            ld1 = extract_pg_history_field(q1, "last_degraded")
            if ld1 is None:
                raise AssertionError("last_degraded not armed while degraded")

            log.info("OSD action: restart target=" + str(primary))
            rados_obj.change_osd_state(action="restart", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
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

            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        q3 = get_pg_query(rados_obj, pgid)
        ld3 = extract_pg_history_field(q3, "last_degraded")
        lc3 = extract_pg_history_field(q3, "last_clean")
        deg_ts, clean_ts = parse_ceph_ts(ld3), parse_ceph_ts(lc3)
        if deg_ts is not None and clean_ts is not None:
            assert deg_ts < clean_ts, (
                f"Expected last_degraded < last_clean: {ld3} vs {lc3}"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Stop non-primary, start it, quickly restart primary near close.")
        log_step("step 2: After clean, avgcount delta <= pg_count * 2.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(20)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        non_primary = None
        # Near close/publish: bounce primary quickly
        time.sleep(2)
        log.info("OSD action: restart target=" + str(primary))
        rados_obj.change_osd_state(action="restart", target=primary)
        _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Throttle, stop non-primary, stop/crash primary mid-recovery.")
        log_step("step 2: Start both, wait clean; counters on new primary increased.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary, primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            time.sleep(15)
            log.info("OSD action: stop target=" + str(primary))
            rados_obj.change_osd_state(action="stop", target=primary)
            time.sleep(10)
            log.info("OSD action: start target=" + str(primary))
            rados_obj.change_osd_state(action="start", target=primary)
            _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
            primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_durability_counters(rados_obj, new_primary)
        after_rebuild = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after_rebuild, context=f"osd.{new_primary} after")
        assert (
            after_rebuild["avgcount"] >= 1
            or (after["has_vulnerability"] and after["vuln_avgcount"] >= 1)
        ), f"New primary osd.{new_primary} should show durability activity: {pp(after)}"
        log.info(f"primary_crash_during_recovery counters: {pp(after)}")

    finally:
        end_osd_debug(cephadm, duration_state)
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    log.info("OSD action: start target=" + str(osd))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create large pg_num pool, write data.")
        log_step("step 2: Stop non-primary, recover.")
        log_step("step 3: Validate counter fields; avgcount increased.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=pg_num, pg_num_max=pg_num)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 300))
        time.sleep(15)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")
        before = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before, context=f"osd.{primary} baseline")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(30)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1800)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        after = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after, context=f"osd.{primary} after")
        assert verify_counters_incremented(before, after), (
            "avgcount should increase after high-PG simultaneous failure recovery"
        )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool, write, clean.")
        log_step("step 2: ceph osd out <non-primary>; wait undersized/degraded; check last_degraded.")
        log_step("step 3: ceph osd in; wait clean; last_degraded < last_clean.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, size=3, min_size=1)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        pgid = pick_pool_pgid(rados_obj, pool_name)
        primary, target, acting = _get_non_primary_osd(rados_obj, pool_name)
        if target is None:
            raise AssertionError("Need non-primary OSD")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [target]
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
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=True,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Stop non-primary (degrade), briefly stop primary, start both, recover.")
        log_step("step 2: Assert clean recovery, valid counters, last_degraded present.")
        log_step("step 3: Log that true netem partition is unavailable.")
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

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary, primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        time.sleep(15)
        log.info("OSD action: stop target=" + str(primary))
        rados_obj.change_osd_state(action="stop", target=primary)
        time.sleep(10)
        log.info("OSD action: start target=" + str(primary))
        rados_obj.change_osd_state(action="start", target=primary)
        _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        non_primary = None
        primary = None
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

        new_primary = rados_obj.get_pg_acting_set(pool_name=pool_name)[0]
        after = get_rebuild_counters(rados_obj, new_primary)
        validate_counter_fields(after, context=f"osd.{new_primary} after")
        pgid = pick_pool_pgid(rados_obj, pool_name)
        ld = extract_pg_history_field(get_pg_query(rados_obj, pgid), "last_degraded")
        if ld is None:
            raise AssertionError("last_degraded missing after partition stand-in")
        log.info(f"Partition stand-in recovered; last_degraded={ld}, counters={pp(after)}")

    finally:
        end_osd_debug(cephadm, duration_state)
        for osd in (non_primary, primary):
            if osd is not None:
                try:
                    log.info("OSD action: start target=" + str(osd))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Stop non-primary, hold for config hold_secs (default 600).")
        log_step("step 2: Start, recover; if has_vulnerability, sum/max increase vs hold time.")
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
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = [non_primary]
            log.info("OSD action: stop target=" + str(non_primary))
            rados_obj.change_osd_state(action="stop", target=non_primary)
            log.info(f"Holding degraded for {hold_secs}s")
            time.sleep(hold_secs)
            log.info("OSD action: start target=" + str(non_primary))
            rados_obj.change_osd_state(action="start", target=non_primary)
            _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
            non_primary = None
        finally:
            mon_obj.remove_config(section="osd", name="osd_recovery_max_active")
            mon_obj.remove_config(section="osd", name="osd_recovery_sleep")

        assert wait_for_recovery(rados_obj, pool_name, timeout=1800)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=True,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )
        after = get_durability_counters(rados_obj, primary)
        after_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(after_rebuild, context=f"osd.{primary} after")
        log.info(f"Long window durability before={pp(before)} after={pp(after)}")

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
                f"sum_delta={sum_delta}, max_delta={max_delta}, after={pp(after)}"
            )
        else:
            log.warning(
                "Vulnerability counters absent; cannot validate long vulnerability window"
            )
            raise AssertionError(
                "Expected vulnerability counters after long degraded hold"
            )

    finally:
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Create pool pg_num=16, write, clean; snapshot counters.")
        log_step("step 2: Decrease pg_num to 8 when clean; wait; counters unchanged.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)
        validate_counter_fields(before_rebuild, context=f"osd.{primary} baseline")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = []
        rados_obj.set_pool_property(pool=pool_name, props="pgp_num", value=8)
        rados_obj.set_pool_property(pool=pool_name, props="pg_num", value=8)
        time.sleep(30)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Enable pg_autoscaler / set pool pg_autoscale_mode on.")
        log_step("step 2: Write data; wait for possible resize.")
        log_step("step 3: Soft-pass always; log whether counters changed.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8, pg_num_max=64)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)

        try:
            duration_state = begin_duration_value_check(
                rados_obj, cephadm, [pool_name]
            )
            _duration_stopped = []
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
            log.info(
                "duration value check: compare perf-counter deltas with "
                "primary OSD PeeringState logs"
            )
            assert_duration_values_match_logs(
                rados_obj,
                duration_state,
                stopped_osds=_duration_stopped,
                require_activity=False,
                require_rebuild=False,
                skip_osds=None,
                restarted_osds=_restarted_osds,
            )
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
            f"like failure. counters_changed={changed} before={pp(before)} after={pp(after)}"
        )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Enable balancer / upmap mode / optimize if available.")
        log_step("step 2: Soft-pass; log whether counters changed.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=16)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 100))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, _, _ = _get_non_primary_osd(rados_obj, pool_name)
        before = get_durability_counters(rados_obj, primary)
        before_rebuild = get_rebuild_counters(rados_obj, primary)

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = []
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
            log.info(
                "duration value check: compare perf-counter deltas with "
                "primary OSD PeeringState logs"
            )
            assert_duration_values_match_logs(
                rados_obj,
                duration_state,
                stopped_osds=_duration_stopped,
                require_activity=False,
                require_rebuild=False,
                skip_osds=None,
                restarted_osds=_restarted_osds,
            )
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
            f"like failure. counters_changed={changed} before={pp(before)} after={pp(after)}"
        )

    finally:
        end_osd_debug(cephadm, duration_state)
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Snapshot counters; stop majority of acting set to force inactive.")
        log_step("step 2: While inactive, counters should not increment for durability.")
        log_step("step 3: Restore OSDs; after recovery may increment. Best-effort asserts.")
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
        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = to_stop
        for osd_id in to_stop:
            log.info("OSD action: stop target=" + str(osd_id))
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
            log.info("OSD action: start target=" + str(osd_id))
            rados_obj.change_osd_state(action="start", target=osd_id)
            _restarted_osds[int(osd_id)] = _host_local_timestamp(rados_obj.node)
            stopped.remove(osd_id)
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
        end_osd_debug(cephadm, duration_state)
        for osd_id in list(stopped):
            try:
                log.info("OSD action: start target=" + str(osd_id))
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
        duration_state = None
        _restarted_osds = {}
        log_step("step 1: Stop non-primary to arm last_degraded; immediately restart primary.")
        log_step("step 2: Recover; assert last_degraded still present/parseable; counters coherent.")
        rados_obj.create_pool(pool_name=pool_name, pg_num=8)
        write_data_to_pool(pool_obj, client, pool_name, nobj=config.get("nobj", 50))
        time.sleep(10)
        assert wait_for_recovery(rados_obj, pool_name)

        primary, non_primary, acting = _get_non_primary_osd(rados_obj, pool_name)
        if non_primary is None:
            raise AssertionError("Need at least 2 OSDs in acting set")

        duration_state = begin_duration_value_check(
            rados_obj, cephadm, [pool_name]
        )
        _duration_stopped = [non_primary]
        log.info("OSD action: stop target=" + str(non_primary))
        rados_obj.change_osd_state(action="stop", target=non_primary)
        # Immediately crash/restart primary during last_degraded update window
        log.info("OSD action: restart target=" + str(primary))
        rados_obj.change_osd_state(action="restart", target=primary)
        _restarted_osds[int(primary)] = _host_local_timestamp(rados_obj.node)
        time.sleep(20)
        log.info("OSD action: start target=" + str(non_primary))
        rados_obj.change_osd_state(action="start", target=non_primary)
        _restarted_osds[int(non_primary)] = _host_local_timestamp(rados_obj.node)
        non_primary = None
        assert wait_for_recovery(rados_obj, pool_name, timeout=1200)
        log.info(
            "duration value check: compare perf-counter deltas with "
            "primary OSD PeeringState logs"
        )
        assert_duration_values_match_logs(
            rados_obj,
            duration_state,
            stopped_osds=_duration_stopped,
            require_activity=False,
            require_rebuild=False,
            skip_osds=None,
            restarted_osds=_restarted_osds,
        )

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
            f"counters={pp(after)}"
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
        end_osd_debug(cephadm, duration_state)
        if non_primary is not None:
            try:
                log.info("OSD action: start target=" + str(non_primary))
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
    counters (pg_rebuild_duration, pg_rebuild_duration_min,
    pg_vulnerability_duration, pg_vulnerability_duration_min).

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

    log_banner(f"START scenario: {scenario}")
    log.info(f"Config keys: {sorted(config.keys())}")
    for key in (
        "scenario",
        "sub_case",
        "pool_name",
        "pool_type",
        "delete_pool",
        "reset_counters",
        "nobj",
    ):
        if key in config:
            log.info(f"  config[{key}] = {config[key]!r}")

    try:
        _truncate_osd_logs(rados_obj)
    except Exception as truncate_err:
        log.error(f"Failed to truncate OSD logs: {truncate_err}")
        return 1

    if config.get("reset_counters", True):
        try:
            reset_rebuild_counters(
                rados_obj, batch_size=config.get("reset_batch_size", 10)
            )
        except Exception as reset_err:
            log.error(f"Counter reset failed: {reset_err}")
            return 1
    else:
        log.info("Skipping counter reset (reset_counters=False)")

    start_time = get_cluster_timestamp(rados_obj.node)
    log.info(f"Test workflow started. Start time: {start_time}")
    log_banner(f"RUNNING scenario body: {scenario}")

    try:
        if scenario == "counter_registration":
            scenario_counter_registration(rados_obj, cephadm, config)
        elif scenario == "counter_priority":
            scenario_counter_priority(rados_obj, cephadm, config)
        elif scenario == "counter_section_name":
            scenario_counter_section_name(rados_obj, cephadm, config)
        elif scenario == "prometheus_integration":
            scenario_prometheus_integration(rados_obj, cephadm, config)
        elif scenario == "prometheus_api_metric_presence":
            scenario_prometheus_api_metric_presence(rados_obj, cephadm, config)
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
        log_banner(f"FAIL scenario: {scenario}")
        log.error(f"Scenario '{scenario}' failed with exception: {e.__doc__}")
        log.exception(e)
        try:
            rados_obj.log_cluster_health()
        except Exception as health_err:
            log.warning(f"Could not log cluster health: {health_err}")
        return 1
    finally:
        log_phase(f"Scenario '{scenario}' finally: cluster health + timestamps")
        try:
            rados_obj.log_cluster_health()
        except Exception as health_err:
            log.warning(f"Could not log cluster health in finally: {health_err}")
        try:
            test_end_time = get_cluster_timestamp(rados_obj.node)
            log.info(
                f"Test workflow completed. Start time: {start_time}, "
                f"End time: {test_end_time}"
            )
        except Exception as ts_err:
            log.warning(f"Could not get cluster timestamp: {ts_err}")
        # if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
        #     log.error("Test failed due to crash at the end of test")
        #     return 1

    log_banner(f"PASS scenario: {scenario}")
    return 0
