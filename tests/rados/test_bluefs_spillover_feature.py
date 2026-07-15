"""
Test module to verify BlueFS spillover health warning and spillover cleaner behavior.

This module validates BlueFS spillover detection, spillover cleaner operation, and
bluefs_spillover_idle_time enforcement on a Ceph cluster deployed with non-collocated
OSD devices (separate DB and data devices).

Test cases are selected via config['case_to_run']:

  case1 - Verify bluefs_debug_force_slow default, generate BlueFS spillover warning,
          verify slow files are not tracked by spillover cleaner before it is enabled,
          and verify bluefs_spillover_cleaner clears the warning.

  case2 - Toggle spillover cleaner on/off, verify migrated and pending files appear in
          spillover cleaner stats, re-enable the cleaner, and verify cluster reaches
          HEALTH_OK.

  case3 - Verify bluefs_spillover_idle_time by comparing migrated OSD log timestamps
          across two spillover cycles. Requires log_to_file enabled via rados_prep.

If case_to_run is not specified, case1 is executed.

Common config parameters:
    pool_name: Name of the test pool (default: test_pool)
    pool_config: Pool creation options (pg_num, etc.)
    rados_write_duration: rados bench write duration in seconds (default: 600)
    byte_size: rados bench object size in bytes (default: 4096)
    spillover_timeout: Timeout to wait for spillover warning (default: 900)
    cleaner_timeout: Timeout to wait for spillover cleanup (default: 900)
    poll_interval: Health/log polling interval in seconds (default: 10)

Case-specific config parameters:
    case1/case2: Uses common parameters above.
    case2: cleaner_stats_timeout, health_ok_timeout, cleaner_config_delay,
           cleaner_toggle_wait
    case3: bluefs_spillover_idle_time (default: 300), debug_bluefs (default: 10/10),
           post_action_wait (default: 10), min_osds_for_idle_time_verify (default: 1),
           health_ok_timeout
"""

import datetime
import re
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)

BLUEFS_SPILLOVER_MSG = "OSD(s) experiencing BlueFS spillover"
BLUEFS_DEBUG_FORCE_SLOW = "bluefs_debug_force_slow"
BLUEFS_SPILLOVER_CLEANER = "bluefs_spillover_cleaner"
BLUEFS_SPILLOVER_IDLE_TIME = "bluefs_spillover_idle_time"
DEBUG_BLUEFS = "debug_bluefs"
MIGRATED_LOG_PATTERN = re.compile(
    r"bluefs migrate_file done.*\bmigrated\b", re.IGNORECASE
)
LOG_TIMESTAMP_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d+|Z)?)"
)


def run(ceph_cluster, **kw):
    """
    Execute BlueFS spillover feature validation workflow.

    Initializes CephAdmin and RadosOrchestrator, runs selected test cases from
    config['case_to_run'], and performs cleanup of OSD configs and test pools in
    the finally block.

    Args:
        ceph_cluster: Ceph cluster object from the test suite.
        **kw: Keyword arguments containing suite config under kw['config'].

    Returns:
        0 on pass, 1 on failure.
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    case_to_run = config.get("case_to_run", ["case1"])
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug("Test workflow started. Start time: %s", start_time)

    try:
        if "case1" in case_to_run:
            case_config = config.get("case1", config)
            run_case1(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        if "case2" in case_to_run:
            case_config = config.get("case2", config)
            run_case2(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        if "case3" in case_to_run:
            case_config = config.get("case3", config)
            run_case3(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        log.info("BlueFS spillover feature validation completed successfully")
    except Exception as err:
        log.error("BlueFS spillover feature test failed: %s", err)
        log.error(traceback.format_exc())
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here "
            "*************** \n \n"
        )
        mon_obj.remove_config(section="osd", name=BLUEFS_DEBUG_FORCE_SLOW)
        mon_obj.remove_config(section="osd", name=BLUEFS_SPILLOVER_CLEANER)
        mon_obj.remove_config(section="osd", name=BLUEFS_SPILLOVER_IDLE_TIME)
        mon_obj.remove_config(section="osd", name=DEBUG_BLUEFS)
        rados_obj.rados_pool_cleanup()

        rados_obj.log_cluster_health()
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            "Test workflow completed. Start time: %s, End time: %s",
            start_time,
            test_end_time,
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def run_case1(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 1: Verify BlueFS spillover warning and spillover cleaner behavior.

    Steps:
        1. Verify that the default value of bluefs_debug_force_slow is false.
        2. Create a test pool.
        3. Set bluefs_debug_force_slow to true and run rados bench until the cluster
           reports "OSD(s) experiencing BlueFS spillover".
        4. Verify spillover OSDs, slow BlueFS files, and that cleaner stats do not
           yet track those files.
        5. Enable bluefs_spillover_cleaner and verify the spillover warning clears.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If any verification step fails.
    """
    log.info(run_case1.__doc__)
    verify_bluefs_debug_force_slow_default(mon_obj)

    pool_name = create_test_pool(rados_obj, config)

    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    verify_spillover_osds_and_bluefs_slow_files(rados_obj)

    verify_spillover_cleaner_removes_warning(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        config=config,
    )

    log.info("Case 1 completed successfully")


def run_case2(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 2: Toggle spillover cleaner and verify migration stats before HEALTH_OK.

    Steps:
        1. Create a test pool.
        2. Set bluefs_debug_force_slow to true and run rados bench until spillover
           warning appears.
        3. Verify spillover OSDs, slow BlueFS files, and cleaner stats state.
        4. Enable bluefs_spillover_cleaner and immediately disable it.
        5. Verify at least one OSD has files in both migrated and pending_files lists.
        6. Re-enable bluefs_spillover_cleaner.
        7. Verify cluster health reaches HEALTH_OK.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If cleaner stats or health verification fails.
    """
    log.info(run_case2.__doc__)

    pool_name = create_test_pool(rados_obj, config)

    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    verify_spillover_osds_and_bluefs_slow_files(rados_obj)

    toggle_spillover_cleaner_on_off(mon_obj, config)

    spillover_osds = get_spillover_osds_from_health_detail(rados_obj)
    verify_spillover_cleaner_stats_migrated_and_pending(
        rados_obj=rados_obj,
        osd_ids=spillover_osds,
        config=config,
    )

    log.info("Re-enabling %s on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    verify_cluster_health_ok(rados_obj, config)

    log.info("Case 2 completed successfully")


def run_case3(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 3: Verify bluefs_spillover_idle_time using migrated OSD log timestamps.

    Steps:
        1. Set debug_bluefs to 10/10 and bluefs_spillover_idle_time (default 300s).
        2. Truncate all OSD logs.
        3. Create a test pool and generate BLUEFS_SPILLOVER warning.
        4. Disable bluefs_debug_force_slow, enable bluefs_spillover_cleaner, and
           verify cluster health is HEALTH_OK.
        5. Store the last migrated log message per OSD (cycle 1).
        6. Truncate all OSD logs, regenerate spillover, disable force_slow, and wait
           for spillover cleanup (cycle 2).
        7. Store the first migrated log message per OSD (cycle 2).
        8. Log OSD-wise migrated log reference table.
        9. Verify at least min_osds_for_idle_time_verify OSDs have migrated log gap
           >= bluefs_spillover_idle_time between cycles.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If idle-time gap verification fails.
    """
    log.info(run_case3.__doc__)

    idle_time = int(config.get("bluefs_spillover_idle_time", 300))
    post_action_wait = config.get("post_action_wait", 10)
    min_osds_required = config.get("min_osds_for_idle_time_verify", 1)

    configure_case3_debug_settings(mon_obj, config)
    truncate_all_osd_logs(rados_obj)

    pool_name = create_test_pool(rados_obj, config)
    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    spillover_osds = get_spillover_osds_from_health_detail(rados_obj)
    all_osd_ids = [str(osd_id) for osd_id in rados_obj.get_osd_list(status="up")]

    disable_bluefs_debug_force_slow(mon_obj)
    log.info("Setting %s to true on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    verify_cluster_health_ok(rados_obj, config)
    time.sleep(post_action_wait)

    last_migrated_logs = get_last_migrated_logs_for_osds(rados_obj, all_osd_ids)

    truncate_all_osd_logs(rados_obj)
    time.sleep(post_action_wait)

    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )
    disable_bluefs_debug_force_slow(mon_obj)
    wait_for_bluefs_spillover_cleared(rados_obj, config)

    first_migrated_logs = get_first_migrated_logs_for_osds(rados_obj, all_osd_ids)

    log_migrated_logs_reference_table(
        spillover_osds=all_osd_ids,
        last_migrated_logs=last_migrated_logs,
        first_migrated_logs=first_migrated_logs,
    )

    verify_migrated_log_idle_time_gap(
        spillover_osds=spillover_osds,
        last_migrated_logs=last_migrated_logs,
        first_migrated_logs=first_migrated_logs,
        idle_time=idle_time,
        min_osds_required=min_osds_required,
    )

    log.info("Case 3 completed successfully")


def configure_case3_debug_settings(mon_obj: MonConfigMethods, config: dict) -> None:
    """
    Configure debug and spillover idle-time settings for case 3.

    Sets debug_bluefs and bluefs_spillover_idle_time on all OSD daemons.

    Args:
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If config set operations fail.
    """
    idle_time = config.get("bluefs_spillover_idle_time", 300)
    debug_bluefs = config.get("debug_bluefs", "10/10")

    assert mon_obj.set_config(
        section="osd", name=DEBUG_BLUEFS, value=debug_bluefs
    ), f"Failed to set {DEBUG_BLUEFS} to {debug_bluefs}"
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_IDLE_TIME, value=str(idle_time)
    ), f"Failed to set {BLUEFS_SPILLOVER_IDLE_TIME} to {idle_time}"


def get_all_osd_host_nodes(rados_obj: RadosOrchestrator) -> list:
    """
    Collect unique host nodes running OSD daemons.

    Args:
        rados_obj: RadosOrchestrator instance.

    Returns:
        List of unique CephNode host objects with running OSDs.
    """
    host_nodes = {}
    for osd_id in rados_obj.get_osd_list(status="up"):
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
        host_nodes[host.hostname] = host
    return list(host_nodes.values())


def truncate_all_osd_logs(rados_obj: RadosOrchestrator) -> None:
    """
    Truncate OSD log files on all OSD hosts.

    Uses remove_log_file_content to compress and clear existing OSD log files
    before log collection phases in case 3.

    Args:
        rados_obj: RadosOrchestrator instance.

    Raises:
        AssertionError: If log truncation fails on any OSD host.
    """
    osd_hosts = get_all_osd_host_nodes(rados_obj)
    log.info("Truncating OSD logs on %d host(s)", len(osd_hosts))
    assert rados_obj.remove_log_file_content(
        osd_hosts, daemon_type="osd"
    ), "Failed to truncate OSD logs"


def read_osd_log_lines(rados_obj: RadosOrchestrator, osd_id: str) -> list[str]:
    """
    Read OSD log file contents from the OSD host.

    Reads /var/log/ceph/{fsid}/ceph-osd.{id}.log. Falls back to grep for
    migrate_file log lines if the full file read returns empty output.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.

    Returns:
        List of log lines from the OSD log file.
    """
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
    log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
    out, _ = host.exec_command(sudo=True, cmd=f"cat {log_path}", check_ec=False)
    if out:
        return out.splitlines()

    grep_cmd = (
        f"grep -i 'bluefs migrate_file done' {log_path} "
        f"|| grep -i migrated {log_path} || true"
    )
    out, _ = host.exec_command(sudo=True, cmd=grep_cmd, check_ec=False)
    if not out:
        log.warning("No OSD log content found for osd.%s at %s", osd_id, log_path)
        return []
    return out.splitlines()


def extract_log_timestamp_string(log_line: str) -> str | None:
    """
    Extract raw timestamp string from a Ceph log line.

    Args:
        log_line: Single line from an OSD log file.

    Returns:
        Timestamp string (e.g. 2026-07-15T01:01:48.821+0000) or None.
    """
    match = LOG_TIMESTAMP_PATTERN.search(log_line)
    if not match:
        return None
    return match.group(1)


def normalize_timestamp_for_parse(timestamp: str) -> str:
    """
    Normalize Ceph log timestamps for datetime parsing.

    Converts formats such as +0000 and Z suffixes into ISO-8601 strings
    compatible with datetime.fromisoformat().

    Args:
        timestamp: Raw timestamp string from a log line.

    Returns:
        Normalized timestamp string.
    """
    if timestamp.endswith("Z"):
        return f"{timestamp[:-1]}+00:00"
    if timestamp.endswith("+0000"):
        return f"{timestamp[:-5]}+00:00"
    if timestamp.endswith("-0000"):
        return f"{timestamp[:-5]}-00:00"

    offset_match = re.search(r"([+-]\d{2})(\d{2})$", timestamp)
    if offset_match and ":" not in timestamp[-6:]:
        return f"{timestamp[:-5]}{offset_match.group(1)}:" f"{offset_match.group(2)}"
    return timestamp


def parse_log_line_timestamp(log_line: str) -> datetime.datetime | None:
    """
    Parse a Ceph log line timestamp into a datetime object.

    Args:
        log_line: Single line from an OSD log file.

    Returns:
        Parsed datetime object, or None if parsing fails.
    """
    timestamp_str = extract_log_timestamp_string(log_line)
    if not timestamp_str:
        return None

    try:
        return datetime.datetime.fromisoformat(
            normalize_timestamp_for_parse(timestamp_str)
        )
    except ValueError:
        log.warning("Unable to parse timestamp from log line: %s", log_line)
        return None


def build_migrated_log_entry(log_line: str) -> dict | None:
    """
    Build a migrated log entry from a single OSD log line.

    Matches lines containing 'bluefs migrate_file done' and 'migrated'.

    Args:
        log_line: Single line from an OSD log file.

    Returns:
        Dictionary with keys timestamp_str, timestamp, and log_line,
        or None if the line is not a valid migrated log entry.
    """
    if not MIGRATED_LOG_PATTERN.search(log_line):
        return None

    timestamp_str = extract_log_timestamp_string(log_line)
    timestamp = parse_log_line_timestamp(log_line)
    if not timestamp_str or timestamp is None:
        return None

    return {
        "timestamp_str": timestamp_str,
        "timestamp": timestamp,
        "log_line": log_line.strip(),
    }


def get_migrated_log_entries(log_lines: list[str]) -> list[dict]:
    """
    Collect and sort migrated log entries from OSD log lines.

    Args:
        log_lines: List of log lines read from an OSD log file.

    Returns:
        List of migrated log entry dictionaries sorted by timestamp.
    """
    migrated_entries = []
    for line in log_lines:
        entry = build_migrated_log_entry(line)
        if entry:
            migrated_entries.append(entry)

    migrated_entries.sort(key=lambda entry: entry["timestamp"])
    return migrated_entries


def get_last_migrated_logs_for_osds(
    rados_obj: RadosOrchestrator, osd_ids: list[str]
) -> dict[str, dict]:
    """
    Collect the last migrated log message for each OSD (cycle 1 reference).

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to scan.

    Returns:
        Dictionary mapping osd_id to migrated log entry dict with keys:
        timestamp_str, timestamp, and log_line.
    """
    last_migrated_logs = {}
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id)
        migrated_entries = get_migrated_log_entries(log_lines)
        if not migrated_entries:
            log.warning(
                "No migrated log entries found on osd.%s after scanning %d log lines",
                osd_id,
                len(log_lines),
            )
            continue

        last_entry = migrated_entries[-1]
        last_migrated_logs[osd_id] = last_entry
        log.info(
            "Last migrated log on osd.%s at %s: %s",
            osd_id,
            last_entry["timestamp_str"],
            last_entry["log_line"],
        )
    return last_migrated_logs


def get_first_migrated_logs_for_osds(
    rados_obj: RadosOrchestrator, osd_ids: list[str]
) -> dict[str, dict]:
    """
    Collect the first migrated log message for each OSD (cycle 2 reference).

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to scan.

    Returns:
        Dictionary mapping osd_id to migrated log entry dict with keys:
        timestamp_str, timestamp, and log_line.
    """
    first_migrated_logs = {}
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id)
        migrated_entries = get_migrated_log_entries(log_lines)
        if not migrated_entries:
            log.warning(
                "No migrated log entries found on osd.%s after scanning %d log lines",
                osd_id,
                len(log_lines),
            )
            continue

        first_entry = migrated_entries[0]
        first_migrated_logs[osd_id] = first_entry
        log.info(
            "First migrated log on osd.%s at %s: %s",
            osd_id,
            first_entry["timestamp_str"],
            first_entry["log_line"],
        )
    return first_migrated_logs


def log_migrated_logs_reference_table(
    spillover_osds: list[str],
    last_migrated_logs: dict[str, dict],
    first_migrated_logs: dict[str, dict],
) -> None:
    """
    Log OSD-wise last and first migrated log details in a table format.

    Prints a summary table with timestamps, log lines, time difference, and
    status (AVAILABLE, PARTIAL, or MISSING) for all OSDs in the union of
    spillover_osds, last_migrated_logs, and first_migrated_logs.

    Args:
        spillover_osds: List of OSD IDs to include in the reference table.
        last_migrated_logs: Last migrated log entries from cycle 1.
        first_migrated_logs: First migrated log entries from cycle 2.
    """
    osd_ids = sorted(
        set(spillover_osds)
        | set(last_migrated_logs.keys())
        | set(first_migrated_logs.keys()),
        key=int,
    )

    table_header = (
        f"{'OSD':<8}| {'Last Migrated Time':<32}| {'First Migrated Time':<32}| "
        f"{'Time Diff (s)':<14}| {'Status':<12}"
    )
    table_separator = "-" * len(table_header)
    log_column_header = (
        f"{'':<8}| {'Last Migrated Log':<115}| {'First Migrated Log':<115}"
    )

    log.info("Migrated logs reference table (cycle 1 last vs cycle 2 first):")
    log.info(table_separator)
    log.info(table_header)
    log.info(log_column_header)
    log.info(table_separator)

    for osd_id in osd_ids:
        last_entry = last_migrated_logs.get(osd_id)
        first_entry = first_migrated_logs.get(osd_id)
        last_time = last_entry["timestamp_str"] if last_entry else "N/A"
        first_time = first_entry["timestamp_str"] if first_entry else "N/A"
        last_log = last_entry["log_line"] if last_entry else "N/A"
        first_log = first_entry["log_line"] if first_entry else "N/A"

        if last_entry and first_entry:
            time_gap = (
                first_entry["timestamp"] - last_entry["timestamp"]
            ).total_seconds()
            time_diff = f"{time_gap:.3f}"
            status = "AVAILABLE"
        elif last_entry or first_entry:
            time_diff = "N/A"
            status = "PARTIAL"
        else:
            time_diff = "N/A"
            status = "MISSING"

        log.info(
            f"{'osd.' + osd_id:<8}| {last_time:<32}| {first_time:<32}| "
            f"{time_diff:<14}| {status:<12}"
        )
        log.info(f"{'':<8}| {last_log:<115}| {first_log:<115}")

    log.info(table_separator)
    log.info("Migrated logs reference details for all OSDs:")
    for osd_id in osd_ids:
        last_entry = last_migrated_logs.get(osd_id)
        first_entry = first_migrated_logs.get(osd_id)
        log.info("OSD osd.%s", osd_id)
        log.info(
            "  Last migrated time (cycle 1): %s",
            last_entry["timestamp_str"] if last_entry else "N/A",
        )
        log.info(
            "  Last migrated log (cycle 1) : %s",
            last_entry["log_line"] if last_entry else "N/A",
        )
        log.info(
            "  First migrated time (cycle 2): %s",
            first_entry["timestamp_str"] if first_entry else "N/A",
        )
        log.info(
            "  First migrated log (cycle 2): %s",
            first_entry["log_line"] if first_entry else "N/A",
        )
        if last_entry and first_entry:
            time_gap = (
                first_entry["timestamp"] - last_entry["timestamp"]
            ).total_seconds()
            log.info("  Time difference (seconds) : %.3f", time_gap)
        else:
            log.info("  Time difference (seconds) : N/A")


def verify_migrated_log_idle_time_gap(
    spillover_osds: list[str],
    last_migrated_logs: dict[str, dict],
    first_migrated_logs: dict[str, dict],
    idle_time: int,
    min_osds_required: int,
) -> None:
    """
    Verify migrated log timestamp gap meets bluefs_spillover_idle_time.

    Compares the first migrated log timestamp in cycle 2 against the last
    migrated log timestamp in cycle 1 for each spillover OSD. Passes when at
    least min_osds_required OSDs have a gap >= idle_time seconds.

    Args:
        spillover_osds: OSD IDs that reported BlueFS spillover in health detail.
        last_migrated_logs: Last migrated log entries from cycle 1.
        first_migrated_logs: First migrated log entries from cycle 2.
        idle_time: Expected minimum gap in seconds (bluefs_spillover_idle_time).
        min_osds_required: Minimum number of OSDs that must meet the gap.

    Raises:
        AssertionError: If fewer than min_osds_required OSDs pass verification.
    """
    passing_osds = []
    comparison_results = []

    log.info(
        "Comparing migrated log timestamps | required idle_time gap >= %s seconds",
        idle_time,
    )
    for osd_id in spillover_osds:
        if osd_id not in last_migrated_logs or osd_id not in first_migrated_logs:
            log.warning(
                "Skipping osd.%s due to missing migrated logs in one of the cycles",
                osd_id,
            )
            comparison_results.append(
                {
                    "osd_id": osd_id,
                    "last_migrated_time": None,
                    "first_migrated_time": None,
                    "last_migrated_log": None,
                    "first_migrated_log": None,
                    "time_difference_seconds": None,
                    "status": "MISSING_LOGS",
                }
            )
            continue

        last_entry = last_migrated_logs[osd_id]
        first_entry = first_migrated_logs[osd_id]
        last_ts = last_entry["timestamp"]
        first_ts = first_entry["timestamp"]
        time_gap = (first_ts - last_ts).total_seconds()
        status = "PASS" if time_gap >= idle_time else "FAIL"

        comparison_results.append(
            {
                "osd_id": osd_id,
                "last_migrated_time": last_entry["timestamp_str"],
                "first_migrated_time": first_entry["timestamp_str"],
                "last_migrated_log": last_entry["log_line"],
                "first_migrated_log": first_entry["log_line"],
                "time_difference_seconds": time_gap,
                "status": status,
            }
        )

        log.info(
            "osd.%s | last_migrated=%s | first_migrated=%s | "
            "time_difference=%.3f seconds | status=%s",
            osd_id,
            last_entry["timestamp_str"],
            first_entry["timestamp_str"],
            time_gap,
            status,
        )
        if time_gap >= idle_time:
            passing_osds.append(osd_id)

    log.info("Migrated log idle-time verification summary:")
    summary_header = (
        f"{'OSD':<8}| {'Last Migrated Time':<32}| {'First Migrated Time':<32}| "
        f"{'Time Diff (s)':<14}| {'Status':<8}"
    )
    log.info("-" * len(summary_header))
    log.info(summary_header)
    log.info("-" * len(summary_header))
    for result in comparison_results:
        time_diff = (
            f"{result['time_difference_seconds']:.3f}"
            if result["time_difference_seconds"] is not None
            else "N/A"
        )
        log.info(
            f"{'osd.' + result['osd_id']:<8}| "
            f"{result['last_migrated_time'] or 'N/A':<32}| "
            f"{result['first_migrated_time'] or 'N/A':<32}| "
            f"{time_diff:<14}| {result['status']:<8}"
        )
        if result.get("last_migrated_log"):
            log.info("  Last migrated log : %s", result["last_migrated_log"])
        if result.get("first_migrated_log"):
            log.info("  First migrated log: %s", result["first_migrated_log"])
    log.info("-" * len(summary_header))

    if len(passing_osds) < min_osds_required:
        raise AssertionError(
            f"Expected at least {min_osds_required} OSDs with migrated log gap "
            f">= {idle_time} seconds, but only {len(passing_osds)} passed: "
            f"{passing_osds}. Comparison results: {comparison_results}"
        )

    log.info(
        "Verified migrated log idle-time gap on OSDs: %s",
        passing_osds,
    )


def wait_for_bluefs_spillover_cleared(
    rados_obj: RadosOrchestrator, config: dict
) -> None:
    """
    Wait until BLUEFS_SPILLOVER warning is cleared from cluster health.

    Args:
        rados_obj: RadosOrchestrator instance.
        config: Test configuration with cleaner_timeout and poll_interval.

    Raises:
        AssertionError: If spillover warning is not cleared within timeout.
    """
    cleaner_timeout = config.get("cleaner_timeout", 900)
    poll_interval = config.get("poll_interval", 10)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=cleaner_timeout)

    log.info("Waiting for BLUEFS_SPILLOVER warning to be cleared")
    while datetime.datetime.now() < end_time:
        if not is_bluefs_spillover_present(rados_obj):
            log.info("BLUEFS_SPILLOVER warning cleared")
            return

        log.info(
            "BLUEFS_SPILLOVER warning still present, retrying in %s seconds",
            poll_interval,
        )
        time.sleep(poll_interval)

    raise AssertionError(
        f"BLUEFS_SPILLOVER warning was not cleared within {cleaner_timeout} seconds"
    )


def verify_bluefs_debug_force_slow_default(mon_obj: MonConfigMethods) -> None:
    """
    Verify that the default value of bluefs_debug_force_slow is false.

    Args:
        mon_obj: MonConfigMethods instance.

    Raises:
        AssertionError: If the default value is not false.
    """
    default_value = mon_obj.get_config(
        section="osd", param=BLUEFS_DEBUG_FORCE_SLOW
    ).lower()
    log.info("Default value of %s: %s", BLUEFS_DEBUG_FORCE_SLOW, default_value)
    if default_value != "false":
        raise AssertionError(
            f"Expected default {BLUEFS_DEBUG_FORCE_SLOW} to be false, "
            f"got {default_value}"
        )
    log.info("Verified default value of %s is false", BLUEFS_DEBUG_FORCE_SLOW)


def create_test_pool(rados_obj: RadosOrchestrator, config: dict) -> str:
    """
    Create the test pool used for BlueFS spillover validation.

    Args:
        rados_obj: RadosOrchestrator instance.
        config: Test configuration with pool_name and optional pool_config.

    Returns:
        Name of the created pool.
    """
    pool_name = config.get("pool_name", "test_pool")
    pool_config = config.get("pool_config", {})
    log.info("Creating pool %s for BlueFS spillover testing", pool_name)
    method_should_succeed(
        rados_obj.create_pool,
        pool_name=pool_name,
        **pool_config,
    )
    return pool_name


def is_bluefs_spillover_present(
    rados_obj: RadosOrchestrator,
    message: str = BLUEFS_SPILLOVER_MSG,
) -> bool:
    """
    Check whether the BlueFS spillover warning is present in cluster health.

    Searches both ceph health detail and ceph -s output for the spillover message.

    Args:
        rados_obj: RadosOrchestrator instance.
        message: Health warning text to search for.

    Returns:
        True if the spillover warning is present, False otherwise.
    """
    health_detail, _ = rados_obj.node.shell(args=["ceph health detail"])
    if message in health_detail:
        log.info("BlueFS spillover warning found in ceph health detail")
        return True

    ceph_status, _ = rados_obj.client.exec_command(cmd="ceph -s", sudo=True)
    if message in ceph_status:
        log.info("BlueFS spillover warning found in ceph status")
        return True
    return False


def get_spillover_osds_from_health_detail(
    rados_obj: RadosOrchestrator,
) -> list[str]:
    """
    Collect OSD IDs reporting BlueFS spillover from ceph health detail.

    Parses JSON health detail when available, falling back to text output.
    Matches OSDs with "spilled over" in their health message.

    Args:
        rados_obj: RadosOrchestrator instance.

    Returns:
        Sorted list of unique OSD IDs as strings (without the osd. prefix).

    Raises:
        AssertionError: If no spillover OSDs are found in health detail.
    """
    spillover_osds = []
    health_detail, _ = rados_obj.node.shell(args=["ceph health detail"])
    log.info("Parsing BlueFS spillover OSDs from ceph health detail")

    spillover_check = None
    try:
        health_json = rados_obj.run_ceph_command(
            cmd="ceph health detail", client_exec=True
        )
        spillover_check = health_json.get("checks", {}).get("BLUEFS_SPILLOVER")
    except Exception as err:
        log.warning(
            "Unable to parse ceph health detail as JSON, using text output: %s",
            err,
        )

    if spillover_check:
        for detail in spillover_check.get("detail", []):
            message = detail.get("message", "")
            osd_match = re.search(r"osd\.(\d+)", message)
            if osd_match and "spilled over" in message:
                spillover_osds.append(osd_match.group(1))

    if not spillover_osds:
        for line in health_detail.splitlines():
            if "spilled over" not in line:
                continue
            osd_match = re.search(r"osd\.(\d+)", line)
            if osd_match:
                spillover_osds.append(osd_match.group(1))

    spillover_osds = sorted(set(spillover_osds), key=int)
    if not spillover_osds:
        raise AssertionError(
            "No OSDs with BlueFS spillover found in ceph health detail"
        )

    log.info("OSDs with BlueFS spillover: %s", spillover_osds)
    return spillover_osds


def run_osd_bluefs_command(rados_obj: RadosOrchestrator, osd_id: str, command: str):
    """
    Run a BlueFS admin-socket command on an OSD.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.
        command: BlueFS command to run (e.g. 'bluefs files list').

    Returns:
        Parsed JSON output from the ceph tell command.
    """
    cmd = f"ceph tell osd.{osd_id} {command}"
    log.info("Running command: ceph daemon osd.%s %s", osd_id, command)
    return rados_obj.run_ceph_command(cmd=cmd, client_exec=True)


def get_bluefs_slow_files(rados_obj: RadosOrchestrator, osd_id: str) -> list[str]:
    """
    Collect BlueFS file names that have data allocated on the slow device.

    A file is considered a slow file when the bluefs files list JSON entry
    contains a non-zero "slow" field (device allocation), not based on the
    file path name.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.

    Returns:
        Sorted list of BlueFS file names with slow-device allocation.

    Raises:
        AssertionError: If no slow files are found on the OSD.
    """
    files_output = run_osd_bluefs_command(rados_obj, osd_id, "bluefs files list")
    file_entries = (
        files_output
        if isinstance(files_output, list)
        else files_output.get("files", [])
    )

    slow_files = []
    for file_entry in file_entries:
        if "slow" not in file_entry:
            continue
        slow_bytes = int(file_entry.get("slow", 0) or 0)
        if slow_bytes <= 0:
            continue
        slow_files.append(file_entry.get("name", ""))

    slow_files = sorted(set(filter(None, slow_files)))
    log.info("BlueFS slow files on osd.%s: %s", osd_id, slow_files)
    if not slow_files:
        raise AssertionError(
            f"No BlueFS files with slow allocation found on osd.{osd_id}"
        )
    return slow_files


def get_spillover_cleaner_stats_lists(
    cleaner_stats_output,
) -> tuple[list[str], list[str]]:
    """
    Parse bluefs spillover cleaner stats into migrated and pending file lists.

    Args:
        cleaner_stats_output: JSON output from 'bluefs spillover cleaner stats'.

    Returns:
        Tuple of (migrated_files, pending_files) as lists of file path strings.
    """
    stats = cleaner_stats_output.get("spillover_cleaner_stats", cleaner_stats_output)
    migrated_files = []
    pending_files = []

    for entry in stats.get("pending_files", []):
        if isinstance(entry, dict) and entry.get("file"):
            pending_files.append(entry["file"])
        elif isinstance(entry, str):
            pending_files.append(entry)

    for entry in stats.get("Files Migrated", []):
        if isinstance(entry, dict) and entry.get("File"):
            migrated_files.append(entry["File"].split(" size=")[0])
        elif isinstance(entry, str):
            migrated_files.append(entry.split(" size=")[0])

    return migrated_files, pending_files


def get_spillover_cleaner_tracked_files(cleaner_stats_output) -> set[str]:
    """
    Extract file paths tracked in bluefs spillover cleaner stats output.

    Combines both migrated and pending file lists into a single set.

    Args:
        cleaner_stats_output: JSON output from 'bluefs spillover cleaner stats'.

    Returns:
        Set of file path strings tracked by the spillover cleaner.
    """
    migrated_files, pending_files = get_spillover_cleaner_stats_lists(
        cleaner_stats_output
    )
    return set(migrated_files + pending_files)


def toggle_spillover_cleaner_on_off(mon_obj: MonConfigMethods, config: dict) -> None:
    """
    Enable bluefs_spillover_cleaner and immediately disable it.

    Used in case 2 to observe partial migration state before re-enabling the
    cleaner for full cleanup.

    Args:
        mon_obj: MonConfigMethods instance.
        config: Test configuration with cleaner_config_delay and
            cleaner_toggle_wait.

    Raises:
        AssertionError: If config set operations fail.
    """
    config_delay = config.get("cleaner_config_delay", 5)
    toggle_wait = config.get("cleaner_toggle_wait", 0)

    log.info("Setting %s to true on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_CLEANER,
        value="true",
        custom_delay=config_delay,
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    if toggle_wait:
        log.info(
            "Waiting %s seconds before disabling %s",
            toggle_wait,
            BLUEFS_SPILLOVER_CLEANER,
        )
        time.sleep(toggle_wait)

    log.info("Setting %s to false on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_CLEANER,
        value="false",
        custom_delay=config_delay,
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to false"


def verify_spillover_cleaner_stats_migrated_and_pending(
    rados_obj: RadosOrchestrator,
    osd_ids: list[str],
    config: dict,
) -> None:
    """
    Verify at least one OSD has files in both migrated and pending_files lists.

    Polls spillover cleaner stats on each spillover OSD until an OSD reports
    both migrated and pending files, or the timeout expires.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to check.
        config: Test configuration with cleaner_stats_timeout and poll_interval.

    Raises:
        AssertionError: If no OSD meets the criteria within the timeout.
    """
    stats_timeout = config.get("cleaner_stats_timeout", 300)
    poll_interval = config.get("poll_interval", 10)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=stats_timeout)

    log.info("Verifying spillover cleaner stats contain migrated and pending files")
    while datetime.datetime.now() < end_time:
        for osd_id in osd_ids:
            cleaner_stats_output = run_osd_bluefs_command(
                rados_obj, osd_id, "bluefs spillover cleaner stats"
            )
            migrated_files, pending_files = get_spillover_cleaner_stats_lists(
                cleaner_stats_output
            )
            log.info(
                "osd.%s spillover cleaner stats - migrated: %s, pending: %s",
                osd_id,
                migrated_files,
                pending_files,
            )
            if migrated_files and pending_files:
                log.info(
                    "Verified osd.%s has %d migrated and %d pending files",
                    osd_id,
                    len(migrated_files),
                    len(pending_files),
                )
                return

        log.info(
            "No OSD yet has both migrated and pending files, retrying in %s seconds",
            poll_interval,
        )
        time.sleep(poll_interval)

    raise AssertionError(
        "No OSD found with both migrated and pending spillover cleaner files "
        f"within {stats_timeout} seconds"
    )


def verify_cluster_health_ok(rados_obj: RadosOrchestrator, config: dict) -> None:
    """
    Verify cluster health reaches HEALTH_OK.

    Polls ceph health until status is HEALTH_OK or the timeout expires.

    Args:
        rados_obj: RadosOrchestrator instance.
        config: Test configuration with health_ok_timeout (or cleaner_timeout)
            and poll_interval.

    Raises:
        AssertionError: If HEALTH_OK is not reached within the timeout.
    """
    health_ok_timeout = config.get(
        "health_ok_timeout", config.get("cleaner_timeout", 900)
    )
    poll_interval = config.get("poll_interval", 10)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=health_ok_timeout)

    log.info("Waiting for cluster health to reach HEALTH_OK")
    while datetime.datetime.now() < end_time:
        health_status = rados_obj.run_ceph_command(cmd="ceph health", client_exec=True)
        status = health_status.get("status", "")
        log.info("Current cluster health status: %s", status)
        if status == "HEALTH_OK":
            log.info("Cluster health is HEALTH_OK")
            return

        log.info(
            "Cluster health is not HEALTH_OK yet, retrying in %s seconds",
            poll_interval,
        )
        time.sleep(poll_interval)

    raise AssertionError(
        f"Cluster health did not reach HEALTH_OK within {health_ok_timeout} seconds"
    )


def verify_slow_files_not_in_spillover_cleaner_stats(
    rados_obj: RadosOrchestrator,
    osd_id: str,
    slow_files: list[str],
) -> None:
    """
    Verify slow BlueFS files are not present in spillover cleaner stats.

    Checks both parsed tracked file lists and raw stats text for overlap with
    the provided slow file names.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.
        slow_files: List of BlueFS file names with slow-device allocation.

    Raises:
        AssertionError: If any slow file appears in spillover cleaner stats.
    """
    cleaner_stats_output = run_osd_bluefs_command(
        rados_obj, osd_id, "bluefs spillover cleaner stats"
    )
    tracked_files = get_spillover_cleaner_tracked_files(cleaner_stats_output)
    cleaner_stats_text = str(cleaner_stats_output)

    overlapping_files = []
    for slow_file in slow_files:
        if slow_file in tracked_files or slow_file in cleaner_stats_text:
            overlapping_files.append(slow_file)

    if overlapping_files:
        raise AssertionError(
            f"Slow BlueFS files found in spillover cleaner stats on osd.{osd_id}: "
            f"{overlapping_files}"
        )

    log.info(
        "Verified slow BlueFS files are not tracked in spillover cleaner stats "
        "on osd.%s",
        osd_id,
    )


def verify_spillover_osds_and_bluefs_slow_files(
    rados_obj: RadosOrchestrator,
) -> None:
    """
    Verify BlueFS spillover OSDs and slow files before enabling spillover cleaner.

    Steps:
        1. Collect OSDs with BlueFS spillover from ceph health detail.
        2. Collect BlueFS files with slow allocation from bluefs files list.
        3. Verify those files are not present in spillover cleaner stats.

    Args:
        rados_obj: RadosOrchestrator instance.

    Raises:
        AssertionError: If spillover OSDs, slow files, or cleaner stats
            verification fails.
    """
    log.info(verify_spillover_osds_and_bluefs_slow_files.__doc__)
    spillover_osds = get_spillover_osds_from_health_detail(rados_obj)

    for osd_id in spillover_osds:

        slow_files = get_bluefs_slow_files(rados_obj, osd_id)
        verify_slow_files_not_in_spillover_cleaner_stats(rados_obj, osd_id, slow_files)


def kill_rados_bench_write(rados_obj: RadosOrchestrator, pool_name: str = None) -> None:
    """
    Kill running rados bench write processes on the client node.

    Uses pgrep to find rados bench write processes, optionally scoped to a
    specific pool, and sends SIGKILL to each matching PID.

    Args:
        rados_obj: RadosOrchestrator instance.
        pool_name: Optional pool name to limit which bench processes are killed.
    """
    pgrep_cmd = (
        f'pgrep -f "rados.*-p {pool_name} bench.*write"'
        if pool_name
        else 'pgrep -f "rados bench.*write"'
    )
    pid, _ = rados_obj.client.exec_command(cmd=pgrep_cmd, sudo=True, check_ec=False)
    if not pid or not pid.strip():
        log.info("No running rados bench write process found to kill")
        return

    for pid_str in pid.strip().splitlines():
        rados_obj.client.exec_command(
            cmd=f"kill -9 {pid_str.strip()}", sudo=True, check_ec=False
        )
        log.info("Killed rados bench write process with pid %s", pid_str.strip())


def disable_bluefs_debug_force_slow(mon_obj: MonConfigMethods) -> None:
    """
    Disable bluefs_debug_force_slow on OSD daemons.

    Args:
        mon_obj: MonConfigMethods instance.

    Raises:
        AssertionError: If the config set operation fails.
    """
    log.info("Setting %s to false on OSD daemons", BLUEFS_DEBUG_FORCE_SLOW)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_DEBUG_FORCE_SLOW, value="false"
    ), f"Failed to set {BLUEFS_DEBUG_FORCE_SLOW} to false"


def trigger_bluefs_spillover_message(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    pool_name: str,
    config: dict,
) -> None:
    """
    Generate the "OSD(s) experiencing BlueFS spillover" health warning.

    Steps:
        1. Set bluefs_debug_force_slow to true on all OSD daemons.
        2. Run rados bench write in the background until the spillover warning
           appears in cluster health, then kill the bench process.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        pool_name: Name of the pool to run rados bench against.
        config: Test configuration with spillover_timeout, poll_interval,
            rados_write_duration, and byte_size.

    Raises:
        Exception: If the spillover warning does not appear within timeout.
    """
    log.info("Setting %s to true on OSD daemons", BLUEFS_DEBUG_FORCE_SLOW)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_DEBUG_FORCE_SLOW, value="true"
    ), f"Failed to set {BLUEFS_DEBUG_FORCE_SLOW} to true"

    spillover_timeout = config.get("spillover_timeout", 900)
    poll_interval = config.get("poll_interval", 10)
    bench_duration = config.get("rados_write_duration", 600)
    byte_size = config.get("byte_size", 4096)

    end_time = datetime.datetime.now() + datetime.timedelta(seconds=spillover_timeout)
    bench_started = False

    log.info(
        "Starting rados bench on pool %s until BlueFS spillover warning appears",
        pool_name,
    )
    while datetime.datetime.now() < end_time:
        if is_bluefs_spillover_present(rados_obj):
            if bench_started:
                kill_rados_bench_write(rados_obj, pool_name=pool_name)
            log.info("BlueFS spillover warning successfully generated")
            return

        if not bench_started:
            log.info(
                "Running rados bench -p %s %s write -b %s --no-cleanup",
                pool_name,
                bench_duration,
                byte_size,
            )
            rados_obj.bench_write(
                pool_name=pool_name,
                rados_write_duration=bench_duration,
                byte_size=byte_size,
                nocleanup=True,
                verify_stats=False,
                background=True,
            )
            bench_started = True

        log.info(
            "BlueFS spillover warning not yet present, retrying in %s seconds",
            poll_interval,
        )
        time.sleep(poll_interval)

    raise Exception(
        f"Expected BlueFS spillover warning did not appear within "
        f"{spillover_timeout} seconds"
    )


def verify_spillover_cleaner_removes_warning(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Verify that enabling bluefs_spillover_cleaner clears the spillover warning.

    Requires the BLUEFS_SPILLOVER warning to already be present. Enables the
    cleaner and polls cluster health until the warning is cleared.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test configuration with cleaner_timeout and poll_interval.

    Raises:
        Exception: If spillover warning is absent before verification, or is
            not cleared within the timeout.
    """
    if not is_bluefs_spillover_present(rados_obj):
        raise Exception(
            "BlueFS spillover warning must be present before cleaner verification"
        )

    log.info("Setting %s to true on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    cleaner_timeout = config.get("cleaner_timeout", 900)
    poll_interval = config.get("poll_interval", 10)
    end_time = datetime.datetime.now() + datetime.timedelta(seconds=cleaner_timeout)

    log.info("Waiting for BlueFS spillover warning to be cleared by spillover cleaner")
    while datetime.datetime.now() < end_time:
        if not is_bluefs_spillover_present(rados_obj):
            log.info(
                "BlueFS spillover warning cleared after enabling %s",
                BLUEFS_SPILLOVER_CLEANER,
            )
            return

        log.info(
            "BlueFS spillover warning still present, retrying in %s seconds",
            poll_interval,
        )
        time.sleep(poll_interval)

    raise Exception(
        f"BlueFS spillover warning was not cleared within {cleaner_timeout} seconds"
    )
