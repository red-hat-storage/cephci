"""
Module to verify Ceph config defaults on a cluster against stored reference values.

The test loads a pre-captured TSV of expected config defaults (per daemon type)
and compares them against the running cluster using:
  - ceph config show-with-defaults <type>.<id> -f json

Any mismatch between actual and stored reference values is flagged as a failure.
"""

import csv
import re
from collections import defaultdict

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)

RELEASE_MAP = {"7": "reef", "8": "squid", "9": "tentacle", "10": "umbrella"}
DAEMON_TYPES = ["mon", "osd", "mgr", "mds"]


def run(ceph_cluster, **kw) -> int:
    """
    Verify Ceph config defaults match stored reference values for this release.
    Returns:
        0 -> Pass, 1 -> Fail
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    rhbuild = config.get("rhbuild") or rados_obj.rhbuild
    release = _get_release_name(rhbuild)
    if not release:
        log.error(f"Unable to determine release from rhbuild: {rhbuild}")
        return 1

    log.info(f"Detected release: {release} (from rhbuild: {rhbuild})")

    ref_path = f"conf/{release}/rados/test-confs/ceph_config_defaults.tsv"
    reference, override_params = _load_reference_tsv(ref_path)
    if not reference:
        log.error(f"Failed to load reference TSV from: {ref_path}")
        return 1

    override_count = sum(len(v) for v in override_params.values())
    log.info(
        f"Loaded reference with {sum(len(v) for v in reference.values())} "
        f"default params + {override_count} override params "
        f"across {len(reference)} daemon types"
    )

    daemon_map = _get_daemon_ids(rados_obj)
    if not daemon_map:
        log.error("Failed to get daemon IDs from cluster")
        return 1

    log.info(f"Daemon map: {daemon_map}")

    # Fetch show-with-defaults for all daemon types in parallel
    show_results = {}
    with parallel(max_workers=len(daemon_map)) as p:
        for dtype, daemon_id in daemon_map.items():
            p.spawn(_fetch_config_show, rados_obj, dtype, daemon_id)
        for result in p:
            if isinstance(result, tuple) and len(result) == 2:
                dtype, params = result
                show_results[dtype] = params
            elif isinstance(result, Exception):
                log.error(f"Exception during config fetch: {result}")
            elif result is None:
                log.error("A daemon config fetch returned no data")

    log.info(
        f"Fetched config for {len(show_results)} daemon types "
        f"via 'ceph config show-with-defaults'"
    )

    # Compare actual values against stored reference
    mismatches = []
    missing_params = []
    missing_overrides = []
    new_params = []

    for dtype in DAEMON_TYPES:
        if dtype not in reference:
            log.warning(f"No reference data for daemon type: {dtype}")
            continue

        if dtype not in show_results:
            log.error(f"Could not fetch config for daemon type: {dtype}")
            missing_params.extend([(dtype, p) for p in reference[dtype].keys()])
            continue

        ref_params = reference[dtype]
        actual_params = show_results[dtype]

        # Check default params: value must match
        for param_name, expected_value in ref_params.items():
            if param_name not in actual_params:
                missing_params.append((dtype, param_name))
                continue

            actual_value = actual_params[param_name]
            if not _values_match(expected_value, actual_value):
                mismatches.append(
                    {
                        "daemon_type": dtype,
                        "param": param_name,
                        "expected": expected_value,
                        "actual": actual_value,
                    }
                )

        # Check override params: must exist (value not compared)
        dtype_overrides = override_params.get(dtype, set())
        for param_name in dtype_overrides:
            if param_name not in actual_params:
                missing_overrides.append((dtype, param_name))

        # Detect new params not in reference at all
        all_known = set(ref_params.keys()) | dtype_overrides
        for param_name in actual_params:
            if param_name not in all_known:
                new_params.append((dtype, param_name))

    # Collect matched params for reporting
    matched_params = []
    for dtype in DAEMON_TYPES:
        if dtype not in reference or dtype not in show_results:
            continue
        for param_name, expected_value in reference[dtype].items():
            if param_name in show_results[dtype]:
                actual_value = show_results[dtype][param_name]
                if _values_match(expected_value, actual_value):
                    matched_params.append(
                        (dtype, param_name, expected_value, actual_value)
                    )

    # Report: Errors first
    _log_report_header(
        mismatches, missing_params, missing_overrides, new_params, matched_params
    )

    if mismatches or missing_params or missing_overrides or new_params:
        return 1

    log.info("PASS: All config defaults match stored reference values")
    return 0


def _log_report_header(
    mismatches, missing_params, missing_overrides, new_params, matched_params
):
    """Log a structured report with error table first, then matched table."""
    sep = "-" * 100
    log.info(sep)
    log.info("CONFIG DEFAULTS VERIFICATION REPORT")
    log.info(sep)

    # Summary
    log.info(
        f"  Mismatches: {len(mismatches)} | "
        f"Missing defaults: {len(missing_params)} | "
        f"Missing overrides: {len(missing_overrides)} | "
        f"New params (fail): {len(new_params)} | "
        f"Matched: {len(matched_params)}"
    )
    log.info(sep)

    # Table 1: Mismatches
    if mismatches:
        log.error("")
        log.error("TABLE: MISMATCHED CONFIG VALUES")
        log.error(f"{'DAEMON':<6} | {'PARAM':<50} | {'STORED':<25} | {'ACTUAL':<25}")
        log.error(sep)
        for m in mismatches:
            log.error(
                f"{m['daemon_type']:<6} | {m['param']:<50} | "
                f"{m['expected']:<25} | {m['actual']:<25}"
            )

    # Table 2: Missing defaults
    if missing_params:
        log.error("")
        log.error("TABLE: MISSING DEFAULT PARAMS (not found on cluster)")
        log.error(f"{'DAEMON':<6} | {'PARAM':<50}")
        log.error(sep)
        for dtype, param in missing_params:
            log.error(f"{dtype:<6} | {param:<50}")

    # Table 3: Missing overrides
    if missing_overrides:
        log.error("")
        log.error("TABLE: MISSING OVERRIDE PARAMS (not found on cluster)")
        log.error(f"{'DAEMON':<6} | {'PARAM':<50}")
        log.error(sep)
        for dtype, param in missing_overrides:
            log.error(f"{dtype:<6} | {param:<50}")

    # Table 4: New params (not in reference - FAIL)
    if new_params:
        log.error("")
        log.error("TABLE: NEW PARAMS ON CLUSTER (not in stored reference)")
        log.error(f"{'DAEMON':<6} | {'PARAM':<50}")
        log.error(sep)
        for dtype, param in new_params:
            log.error(f"{dtype:<6} | {param:<50}")

    # Table 5: Matched params
    log.info("")
    log.info("TABLE: MATCHED CONFIG VALUES")
    log.info(
        f"{'DAEMON':<6} | {'PARAM':<45} | {'DEFAULT (stored)':<30} | {'RUNTIME (actual)':<30}"
    )
    log.info(sep)
    for dtype, param, stored, actual in matched_params:
        stored_val = str(stored)[:27] + "..." if len(str(stored)) > 30 else str(stored)
        actual_val = str(actual)[:27] + "..." if len(str(actual)) > 30 else str(actual)
        log.info(f"{dtype:<6} | {param:<45} | {stored_val:<30} | {actual_val:<30}")

    log.info(sep)
    log.info(f"Total matched: {len(matched_params)}")
    log.info(sep)


def _get_release_name(rhbuild: str) -> str:
    """Map rhbuild string to release name. E.g., '9.0-rhel-9' -> 'tentacle'."""
    if not rhbuild:
        return ""
    regex = r"(\d+)\."
    match = re.search(regex, rhbuild)
    if match:
        major = match.group(1)
        return RELEASE_MAP.get(major, "")
    return ""


def _load_reference_tsv(path: str) -> tuple:
    """
    Load reference TSV file into two nested dicts:
    - defaults: {daemon_type: {param_name: value}} -- source == "default"
    - overrides: {daemon_type: set(param_name)} -- source != "default" (existence check only)

    TSV format: daemon_type<TAB>name<TAB>value<TAB>source
    """
    defaults = defaultdict(dict)
    overrides = defaultdict(set)
    try:
        with open(path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                dtype = row.get("daemon_type")
                name = row.get("name")
                value = row.get("value", "")
                source = row.get("source", "default")
                if not dtype or not name:
                    continue
                if source == "default":
                    defaults[dtype][name] = value
                else:
                    overrides[dtype].add(name)
    except FileNotFoundError:
        log.error(f"Reference file not found: {path}")
        return {}, {}
    except Exception as e:
        log.error(f"Error reading reference file: {e}")
        return {}, {}

    return dict(defaults), dict(overrides)


def _get_daemon_ids(rados_obj: RadosOrchestrator) -> dict:
    """Get one running daemon ID per daemon type. Only picks daemons with status 'running'."""
    daemon_map = {}
    for dtype in DAEMON_TYPES:
        try:
            cmd = f"ceph orch ps --daemon-type {dtype}"
            out = rados_obj.run_ceph_command(cmd=cmd)
            if out and len(out) > 0:
                for daemon in out:
                    status = str(daemon.get("status_desc", "")).lower()
                    daemon_id = daemon.get("daemon_id")
                    if daemon_id and "running" in status:
                        daemon_map[dtype] = daemon_id
                        break
                if dtype not in daemon_map:
                    log.error(
                        f"No running daemon found for type: {dtype}. "
                        f"Available: {[d.get('daemon_id') for d in out]}"
                    )
        except Exception as e:
            log.error(f"Could not get daemon ID for {dtype}: {e}")
    return daemon_map


def _fetch_config_show(
    rados_obj: RadosOrchestrator, dtype: str, daemon_id: str
) -> tuple:
    """
    Run 'ceph config show-with-defaults <type>.<id>' and return
    (dtype, {param: value}).
    """
    try:
        cmd = f"ceph config show-with-defaults {dtype}.{daemon_id}"
        out = rados_obj.run_ceph_command(cmd=cmd)
        params = {}
        if isinstance(out, list):
            for entry in out:
                name = entry.get("name")
                if name:
                    params[name] = entry.get("value", "")
        elif isinstance(out, dict):
            params = out
        else:
            log.warning(
                f"Unexpected output type from config show-with-defaults "
                f"for {dtype}.{daemon_id}: {type(out)}"
            )
        return (dtype, params)
    except Exception as e:
        log.error(
            f"Failed to fetch config show-with-defaults "
            f"for {dtype}.{daemon_id}: {e}"
        )
        return None


def _values_match(expected: str, actual: str) -> bool:
    """
    Compare config values with tolerance for formatting differences.
    Handles: trailing zeros in floats, whitespace, empty vs dash,
    boolean case differences (true/True/TRUE).
    """
    if expected == actual:
        return True

    e = str(expected).strip()
    a = str(actual).strip()

    if e == a:
        return True

    if e.lower() == a.lower():
        return True

    if e in ("", "-") and a in ("", "-"):
        return True

    try:
        if float(e) == float(a):
            return True
    except (ValueError, TypeError):
        pass

    return False
