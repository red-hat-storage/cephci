"""
Test module to verify BlueFS spillover health warning and spillover cleaner behavior.

This module validates BlueFS spillover detection, spillover cleaner operation, and
bluefs_spillover_idle_time enforcement on a Ceph cluster deployed with non-collocated
OSD devices (separate DB and data devices).

Test cases are selected via config['case_to_run']:

  case1 - Verify bluefs_debug_force_slow default, generate BlueFS spillover warning,
          disable force_slow, verify slow files are not tracked by spillover cleaner
          before it is enabled, set bluefs_spillover_idle_time=3, and verify
          bluefs_spillover_cleaner clears the warning.

  case2 - Verify bluefs_spillover_idle_time (default 15s) by comparing migrated OSD
          log timestamps across two spillover cycles. OSDs with missing logs are
          skipped; at least one spillover OSD must have logs in both cycles with
          gap >= idle_time. Requires log_to_file enabled via rados_prep.

  case3 - Same as case2 but verifies bluefs_spillover_idle_time with default 100s.

  case4 - Create a single-PG pool, verify default work_ratio=0.1, then for default
          0.1 / 0.9: generate spillover, disable force_slow, truncate acting-set
          OSD logs, set debug_osd=20/20 then debug_bluefs=20/20 on acting OSDs, set
          bluefs_spillover_idle_time=10, enable cleaner, wait until BlueFS spillover
          is cleared from ceph status, remove per-OSD debug_osd/debug_bluefs, collect
          work_ratio/runtime_ms/sleep_ms from acting OSD logs, delete the pool, then
          recreate a single-PG pool and refresh the acting set for the next cycle.
          Finally verify overall runtime_ms_avg and sleep_ms_avg ordering:
          work_ratio=0.1 > 0.9.

  case5 - Generate spillover, change bluefs_spillover_idle_time (20s then 10s), and
          verify spillover cleaner stats remain unchanged (migrated files may exist
          but must be identical across Stats1-Stats4 with no new migrations).

  case6 - Create a single-PG pool, apply bluefs_debug_force_slow and
          bluefs_spillover_cleaner only on the pool acting set, verify spillover
          is limited to acting OSDs, confirm spillover persists across one acting
          OSD restart, then clear spillover after enabling cleaner and restarting
          an acting OSD.

  case7 - Generate BlueFS spillover, disable force_slow, enable spillover cleaner,
          wait for HEALTH_OK, verify cleaner stats have no "slow" entries (if any
          remain, spillover must still be reported in ceph -s), disable the cleaner,
          and verify any remaining slow cleaner-stat files are absent from
          bluefs files list. Runs continuously case7_iterations times (default: 3)
          with a unique pool name per iteration.

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
    case1: bluefs_spillover_idle_time (default: 3); uses common parameters above.
    case2: bluefs_spillover_idle_time (default: 15), debug_bluefs (default: 10/10),
           post_action_wait (default: 10), health_ok_timeout
    case3: Same as case2 with bluefs_spillover_idle_time default 100
    case4: single-PG pool_config, pool_settle_wait, debug_osd (default: 20/20),
           debug_bluefs (default: 20/20), bluefs_spillover_idle_time (default: 10),
           pre_config_rm_wait (default: 10),
           work_ratio_phases (default: [0.9]) after the default 0.1 phase,
           cleaner_timeout, post_action_wait (default: 10)
    case5: idle_time_phase1 (default: 20), idle_time_phase2 (default: 10)
    case6: pool_config with pg_num=1 (default), pool_name, pool_settle_wait,
           osd_restart_timeout (default: 300)
    case7: health_ok_timeout (default: cleaner_timeout), poll_interval,
           case7_iterations (default: 3)
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
BLUEFS_SPILLOVER_CLEANER_WORK_RATIO = "bluefs_spillover_cleaner_work_ratio"
DEBUG_BLUEFS = "debug_bluefs"
DEBUG_OSD = "debug_osd"
MIGRATED_LOG_PATTERN = re.compile(
    r"bluefs migrate_file done.*\bmigrated\b", re.IGNORECASE
)
LOG_TIMESTAMP_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d+|Z)?)"
)
WORK_RATIO_RUNTIME_SLEEP_PATTERN = re.compile(
    r"work_ratio=([0-9.]+)\s+runtime ms=(\d+)\s+sleep ms=(\d+)",
    re.IGNORECASE,
)


def _get_osd_label(osd_id: str) -> str:
    """Return a formatted OSD label."""
    return f"osd.{osd_id}"


def _strip_log_timestamp(log_line: str | None) -> str:
    """Return log message text without the leading timestamp."""
    if not log_line:
        return "N/A"
    match = LOG_TIMESTAMP_PATTERN.search(log_line)
    if match:
        return log_line[match.end() :].strip() or "N/A"
    return log_line.strip()


def _truncate_text(text: str, max_length: int) -> str:
    """Truncate text to max_length, appending ellipsis when needed."""
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


def log_migrated_log_comparison_table(
    title: str,
    rows: list[dict],
    *,
    required_idle_time: int | None = None,
    include_log_messages: bool = False,
) -> None:
    """
    Log a formatted table comparing cycle 1 and cycle 2 migrated log entries.

    Rows with no migrated-log data in either cycle (status MISSING / all N/A)
    are omitted from the printed table.

    Args:
        title: Table title printed above the header row.
        rows: List of row dicts with keys osd_id, last_migrated_time,
            first_migrated_time, time_difference_seconds, status, and optionally
            last_migrated_log and first_migrated_log.
        required_idle_time: Optional idle-time threshold to include in the title.
        include_log_messages: When True, add last/first migrated log columns to
            each table row instead of printing separate lines between rows.
    """
    rows = [
        row
        for row in rows
        if row.get("status") not in ("MISSING", "MISSING_LOGS")
        and (
            row.get("last_migrated_time") is not None
            or row.get("first_migrated_time") is not None
        )
    ]
    if not rows:
        log.info("%s: no OSD data to display", title)
        return

    osd_ids = [row["osd_id"] for row in rows]
    osd_width = max(len(_get_osd_label(osd_id)) for osd_id in osd_ids)
    osd_width = max(osd_width, len("OSD"))
    time_width = 32
    gap_width = 12
    status_width = max(
        len("Status"),
        max(len(row.get("status", "")) for row in rows),
    )
    log_col_width = 70

    if include_log_messages:
        header = (
            f"{'OSD':<{osd_width}} | "
            f"{'Last Migrated Time':<{time_width}} | "
            f"{'First Migrated Time':<{time_width}} | "
            f"{'Time Diff (s)':>{gap_width}} | "
            f"{'Status':<{status_width}} | "
            f"{'Last Migrated Log':<{log_col_width}} | "
            f"{'First Migrated Log':<{log_col_width}}"
        )
    else:
        header = (
            f"{'OSD':<{osd_width}} | "
            f"{'Last Migrated Time':<{time_width}} | "
            f"{'First Migrated Time':<{time_width}} | "
            f"{'Time Diff (s)':>{gap_width}} | "
            f"{'Status':<{status_width}}"
        )
    separator = "-" * len(header)

    if required_idle_time is not None:
        log.info(
            "%s (required gap >= %s seconds for OSDs with logs in both cycles):",
            title,
            required_idle_time,
        )
    else:
        log.info("%s:", title)

    log.info(separator)
    log.info(header)
    log.info(separator)

    for row in rows:
        osd_label = _get_osd_label(row["osd_id"])
        last_time = row.get("last_migrated_time") or "N/A"
        first_time = row.get("first_migrated_time") or "N/A"
        time_diff_value = row.get("time_difference_seconds")
        time_diff = f"{time_diff_value:.3f}" if time_diff_value is not None else "N/A"
        status = row.get("status", "N/A")

        if include_log_messages:
            last_log = _truncate_text(
                _strip_log_timestamp(row.get("last_migrated_log")), log_col_width
            )
            first_log = _truncate_text(
                _strip_log_timestamp(row.get("first_migrated_log")), log_col_width
            )
            log.info(
                f"{osd_label:<{osd_width}} | "
                f"{last_time:<{time_width}} | "
                f"{first_time:<{time_width}} | "
                f"{time_diff:>{gap_width}} | "
                f"{status:<{status_width}} | "
                f"{last_log:<{log_col_width}} | "
                f"{first_log:<{log_col_width}}"
            )
        else:
            log.info(
                f"{osd_label:<{osd_width}} | "
                f"{last_time:<{time_width}} | "
                f"{first_time:<{time_width}} | "
                f"{time_diff:>{gap_width}} | "
                f"{status:<{status_width}}"
            )

    log.info(separator)


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

        if "case4" in case_to_run:
            case_config = config.get("case4", config)
            run_case4(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        if "case5" in case_to_run:
            case_config = config.get("case5", config)
            run_case5(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        if "case6" in case_to_run:
            case_config = config.get("case6", config)
            run_case6(
                rados_obj=rados_obj,
                mon_obj=mon_obj,
                config=case_config,
            )

        if "case7" in case_to_run:
            case_config = config.get("case7", config)
            case7_iterations = case_config.get("case7_iterations", 3)
            base_pool_name = case_config.get("pool_name", "test_pool_case7")
            for iteration in range(1, case7_iterations + 1):
                iteration_config = dict(case_config)
                iteration_config["pool_name"] = f"{base_pool_name}_iter{iteration}"
                log.info(
                    "=" * 80
                    + f"\nCase 7 continuous run {iteration}/{case7_iterations} "
                    f"(pool={iteration_config['pool_name']})\n" + "=" * 80
                )
                run_case7(
                    rados_obj=rados_obj,
                    mon_obj=mon_obj,
                    config=iteration_config,
                )
                log.info(
                    "Case 7 continuous run %s/%s completed successfully",
                    iteration,
                    case7_iterations,
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
        for config_name in (
            BLUEFS_DEBUG_FORCE_SLOW,
            BLUEFS_SPILLOVER_CLEANER,
            BLUEFS_SPILLOVER_IDLE_TIME,
            BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
            DEBUG_BLUEFS,
        ):
            try:
                mon_obj.remove_config(section="osd", name=config_name)
            except Exception as cleanup_err:
                log.warning(
                    "Failed to remove config %s during cleanup: %s",
                    config_name,
                    cleanup_err,
                )
        try:
            rados_obj.rados_pool_cleanup()
        except Exception as cleanup_err:
            log.warning("Failed during rados pool cleanup: %s", cleanup_err)

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
        4. Kill the background rados bench process.
        5. Set bluefs_debug_force_slow to false.
        6. Verify spillover OSDs, slow BlueFS files, and that cleaner stats do not
           yet track those files.
        7. Set bluefs_spillover_idle_time to 3 seconds.
        8. Enable bluefs_spillover_cleaner and verify the spillover warning clears.

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

    disable_bluefs_debug_force_slow(mon_obj)

    verify_spillover_osds_and_bluefs_slow_files(rados_obj)

    idle_time = int(config.get("bluefs_spillover_idle_time", 3))
    log.info(
        "Setting %s to %s seconds before enabling cleaner",
        BLUEFS_SPILLOVER_IDLE_TIME,
        idle_time,
    )
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_IDLE_TIME,
        value=str(idle_time),
    ), f"Failed to set {BLUEFS_SPILLOVER_IDLE_TIME} to {idle_time}"

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
    Case 2: Verify bluefs_spillover_idle_time (default 15s) using migrated logs.

    Steps:
        1. Set debug_bluefs to 10/10 and bluefs_spillover_idle_time (default 15s).
        2. Truncate all OSD logs.
        3. Create a test pool and generate BLUEFS_SPILLOVER warning.
        4. Disable bluefs_debug_force_slow, enable bluefs_spillover_cleaner, and
           verify cluster health is HEALTH_OK.
        5. Store the last migrated log message per OSD (cycle 1).
        6. Truncate all OSD logs, regenerate spillover, disable force_slow, wait
           for spillover cleanup, then wait post_action_wait seconds for migrated
           logs to be generated (cycle 2).
        7. Store the first migrated log message per OSD (cycle 2).
        8. Log OSD-wise migrated log reference table.
        9. Verify at least one spillover OSD has migrated logs in both cycles and
           every comparable OSD has gap >= bluefs_spillover_idle_time between cycles.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If idle-time gap verification fails.
    """
    log.info(run_case2.__doc__)
    run_idle_time_verification_case(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        config=config,
        default_idle_time=15,
        case_number=2,
    )


def run_case3(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 3: Verify bluefs_spillover_idle_time (default 100s) using migrated logs.

    Same workflow as case 2, but sets bluefs_spillover_idle_time to 100 seconds
    and verifies at least one spillover OSD has comparable migrated logs with
    gap >= that threshold.

    Steps:
        1. Set debug_bluefs to 10/10 and bluefs_spillover_idle_time (default 100s).
        2. Truncate all OSD logs.
        3. Create a test pool and generate BLUEFS_SPILLOVER warning.
        4. Disable bluefs_debug_force_slow, enable bluefs_spillover_cleaner, and
           verify cluster health is HEALTH_OK.
        5. Store the last migrated log message per OSD (cycle 1).
        6. Truncate all OSD logs, regenerate spillover, disable force_slow, wait
           for spillover cleanup, then wait post_action_wait seconds for migrated
           logs to be generated (cycle 2).
        7. Store the first migrated log message per OSD (cycle 2).
        8. Log OSD-wise migrated log reference table.
        9. Verify at least one spillover OSD has migrated logs in both cycles and
           every comparable OSD has gap >= bluefs_spillover_idle_time between cycles.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If idle-time gap verification fails.
    """
    log.info(run_case3.__doc__)
    run_idle_time_verification_case(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        config=config,
        default_idle_time=100,
        case_number=3,
    )


def run_case4(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 4: Verify bluefs_spillover_cleaner_work_ratio via OSD log metrics.

    Steps:
        1. Verify default bluefs_spillover_cleaner_work_ratio is 0.1.
        2. Create a pool with a single PG.
        3. Get the acting set of the PG.
        4. Ensure bluefs_spillover_cleaner is false, then generate BlueFS
           spillover (rados bench) and stop the bench.
        5. Set bluefs_debug_force_slow to false.
        6. Truncate OSD logs for the acting set only.
        7. Set debug_osd to 20/20 on acting OSDs.
        8. Set debug_bluefs to 20/20 on acting OSDs.
        9. Set bluefs_spillover_idle_time to 10.
        10. Set bluefs_spillover_cleaner to true.
        11. Wait until BlueFS spillover messages are cleared from ceph status.
        12. Sleep 10 seconds, then remove per-OSD debug_osd and debug_bluefs
            from acting OSDs.
        13. From OSD logs collect work_ratio; average all runtime_ms values and
            average all sleep_ms values per acting OSD.
        14. Prepare per-acting-OSD table data (work_ratio, runtime_ms_avg,
            sleep_ms_avg).
        15. Delete the pool.
        16. Set bluefs_spillover_cleaner_work_ratio to 0.9, create a new single-PG
            pool, fetch the acting set, and repeat steps 4-15.
        17. Log per-phase work_ratio / runtime_ms_avg / sleep_ms_avg tables.
        18. For each phase, average runtime_ms_avg and sleep_ms_avg across acting
            OSDs, then verify ordering:
            runtime_ms_avg(0.1) > runtime_ms_avg(0.9) and
            sleep_ms_avg(0.1) > sleep_ms_avg(0.9).

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If default work_ratio verification fails, acting set is
            empty, pool delete fails, work_ratio/runtime/sleep log values cannot
            be collected, or phase metric ordering is incorrect.
    """
    log.info(run_case4.__doc__)
    log.info("=" * 80)
    log.info("Starting Case 4: bluefs_spillover_cleaner_work_ratio log validation")
    log.info("=" * 80)

    case_config = dict(config)
    pool_config = dict(case_config.get("pool_config", {}))
    pool_config.setdefault("pg_num", 1)
    pool_config.setdefault("pg_num_min", 1)
    pool_config.setdefault("disable_pg_autoscale", True)
    case_config["pool_config"] = pool_config
    pool_settle_wait = case_config.get("pool_settle_wait", 15)
    debug_bluefs = case_config.get("debug_bluefs", "20/20")
    debug_osd = case_config.get("debug_osd", "20/20")
    idle_time = int(case_config.get("bluefs_spillover_idle_time", 10))
    base_pool_name = case_config.get("pool_name", "test_pool_case4")
    # First phase uses the verified default (0.1); then configured phases.
    work_ratio_phases = case_config.get("work_ratio_phases", [0.9])
    phase_results = {}

    log.info(
        "Step 1: Verifying default %s is 0.1",
        BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
    )
    verify_bluefs_spillover_cleaner_work_ratio_default(mon_obj, expected=0.1)

    # Phase at default work_ratio 0.1, then each configured phase.
    all_phases = [0.1] + [float(ratio) for ratio in work_ratio_phases]
    for phase_index, work_ratio in enumerate(all_phases, start=1):
        phase_label = f"work_ratio={work_ratio}"
        # Unique pool per cycle so recreate does not collide with a stale name.
        phase_pool_name = (
            f"{base_pool_name}_wr{str(work_ratio).replace('.', '')}_p{phase_index}"
        )
        phase_case_config = dict(case_config)
        phase_case_config["pool_name"] = phase_pool_name

        log.info("=" * 80)
        log.info(
            "Case 4 phase %s/%s: %s",
            phase_index,
            len(all_phases),
            phase_label,
        )
        log.info("=" * 80)

        if phase_index > 1:
            log.info(
                "Setting %s to %s before next spillover cycle",
                BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
                work_ratio,
            )
            set_bluefs_spillover_cleaner_work_ratio(mon_obj, work_ratio)

        log.info(
            "[%s] Creating single-PG pool %s",
            phase_label,
            phase_pool_name,
        )
        log.info(
            "Pool create options: pg_num=%s, pg_num_min=%s, disable_pg_autoscale=%s",
            pool_config.get("pg_num"),
            pool_config.get("pg_num_min"),
            pool_config.get("disable_pg_autoscale"),
        )
        pool_name = create_test_pool(rados_obj, phase_case_config)
        log.info(
            "Waiting %s seconds for pool %s PGs to settle before fetching acting set",
            pool_settle_wait,
            pool_name,
        )
        time.sleep(pool_settle_wait)

        log.info("[%s] Fetching acting set for pool %s", phase_label, pool_name)
        acting_osds = [
            str(osd_id) for osd_id in rados_obj.get_pg_acting_set(pool_name=pool_name)
        ]
        log.info(
            "Acting set for pool %s contains %d OSD(s): %s",
            pool_name,
            len(acting_osds),
            acting_osds,
        )
        if not acting_osds:
            raise AssertionError(f"Empty acting set returned for pool {pool_name}")

        phase_data = run_case4_work_ratio_collection_phase(
            rados_obj=rados_obj,
            mon_obj=mon_obj,
            pool_name=pool_name,
            acting_osds=acting_osds,
            config=phase_case_config,
            debug_bluefs=debug_bluefs,
            debug_osd=debug_osd,
            idle_time=idle_time,
            expected_work_ratio=work_ratio,
            phase_label=phase_label,
        )
        phase_results[phase_label] = phase_data

        log.info("[%s] Deleting pool %s after data collection", phase_label, pool_name)
        assert rados_obj.delete_pool(
            pool=pool_name
        ), f"Failed to delete pool {pool_name} after {phase_label}"
        log.info("[%s] Successfully deleted pool %s", phase_label, pool_name)

    log.info("Logging all collected work_ratio / runtime / sleep data")
    log_work_ratio_runtime_sleep_phase_results(phase_results)
    log.info(
        "Verifying overall runtime_ms_avg and sleep_ms_avg ordering "
        "across work_ratio phases 0.1 > 0.9"
    )
    verify_work_ratio_phase_metric_ordering(phase_results)
    log.info("Case 4 completed successfully")


def run_case4_work_ratio_collection_phase(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    pool_name: str,
    acting_osds: list[str],
    config: dict,
    debug_bluefs: str,
    debug_osd: str,
    idle_time: int,
    expected_work_ratio: float,
    phase_label: str,
) -> dict[str, dict]:
    """
    Run one Case 4 collection phase (spillover -> spillover clear -> parse OSD logs).

    Implements the repeating Case 4 steps: disable spillover cleaner, generate
    spillover, disable force_slow, truncate acting-set OSD logs, set debug_osd
    then debug_bluefs on acting OSDs, set bluefs_spillover_idle_time, enable
    cleaner, wait until BlueFS spillover is cleared from ceph status, sleep
    before removing per-OSD debug_osd/debug_bluefs, and collect work_ratio plus
    average runtime_ms/sleep_ms values.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        pool_name: Test pool name.
        acting_osds: Acting-set OSD IDs.
        config: Case configuration.
        debug_bluefs: debug_bluefs value to set on acting OSDs (e.g. 20/20).
        debug_osd: debug_osd value to set on acting OSDs (e.g. 20/20).
        idle_time: bluefs_spillover_idle_time value in seconds.
        expected_work_ratio: Work ratio configured for this phase.
        phase_label: Label used in logs for this phase.

    Returns:
        Mapping of osd_id to dict with work_ratio, averaged runtime_ms/sleep_ms,
        sample_count, and log_line.

    Raises:
        AssertionError: If no acting OSD yields parseable work_ratio log values.
    """
    log.info(
        "[%s] Ensuring %s is false before generating spillover so cleaner "
        "migration (and work_ratio logs) occur only after debug is enabled",
        phase_label,
        BLUEFS_SPILLOVER_CLEANER,
    )
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="false"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to false"

    log.info(
        "[%s] Steps 4-5: Generating BlueFS spillover and stopping rados bench",
        phase_label,
    )
    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    log.info(
        "[%s] Step 6: Setting %s to false",
        phase_label,
        BLUEFS_DEBUG_FORCE_SLOW,
    )
    disable_bluefs_debug_force_slow(mon_obj)

    log.info(
        "[%s] Step 7: Truncating OSD logs for acting set %s",
        phase_label,
        acting_osds,
    )
    truncate_osd_logs_for_osds(rados_obj, acting_osds)

    log.info(
        "[%s] Step 7b: Setting %s=%s on acting OSDs %s",
        phase_label,
        DEBUG_OSD,
        debug_osd,
        acting_osds,
    )
    set_osd_daemon_config_on_osds(
        mon_obj=mon_obj,
        osd_ids=acting_osds,
        name=DEBUG_OSD,
        value=str(debug_osd),
    )

    log.info(
        "[%s] Step 8: Setting %s=%s on acting OSDs %s",
        phase_label,
        DEBUG_BLUEFS,
        debug_bluefs,
        acting_osds,
    )
    set_osd_daemon_config_on_osds(
        mon_obj=mon_obj,
        osd_ids=acting_osds,
        name=DEBUG_BLUEFS,
        value=str(debug_bluefs),
    )

    log.info(
        "[%s] Step 9: Setting %s to %s",
        phase_label,
        BLUEFS_SPILLOVER_IDLE_TIME,
        idle_time,
    )
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_IDLE_TIME,
        value=str(idle_time),
    ), f"Failed to set {BLUEFS_SPILLOVER_IDLE_TIME} to {idle_time}"

    log.info(
        "[%s] Step 10: Setting %s to true",
        phase_label,
        BLUEFS_SPILLOVER_CLEANER,
    )
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    log.info(
        "[%s] Step 11: Waiting until BlueFS spillover message is cleared from "
        "ceph status",
        phase_label,
    )
    wait_for_bluefs_spillover_cleared(rados_obj, config)

    pre_config_rm_wait = int(config.get("pre_config_rm_wait", 10))
    log.info(
        "[%s] Waiting %s seconds before removing %s and %s from acting OSDs",
        phase_label,
        pre_config_rm_wait,
        DEBUG_OSD,
        DEBUG_BLUEFS,
    )
    time.sleep(pre_config_rm_wait)

    log.info(
        "[%s] Step 12: Removing %s and %s from acting OSDs %s",
        phase_label,
        DEBUG_OSD,
        DEBUG_BLUEFS,
        acting_osds,
    )
    remove_osd_daemon_config_on_osds(
        mon_obj=mon_obj,
        osd_ids=acting_osds,
        name=DEBUG_OSD,
    )
    remove_osd_daemon_config_on_osds(
        mon_obj=mon_obj,
        osd_ids=acting_osds,
        name=DEBUG_BLUEFS,
    )

    log.info(
        "[%s] Steps 13-14: Collecting work_ratio and average runtime_ms/sleep_ms "
        "from acting OSD logs (expected work_ratio=%s)",
        phase_label,
        expected_work_ratio,
    )
    phase_data = collect_work_ratio_runtime_sleep_for_osds(
        rados_obj=rados_obj,
        osd_ids=acting_osds,
    )
    log_work_ratio_runtime_sleep_table(phase_label, phase_data, acting_osds)

    osds_with_data = [osd_id for osd_id, entry in phase_data.items() if entry]
    if not osds_with_data:
        raise AssertionError(
            f"[{phase_label}] No acting OSD logs contained "
            f"'work_ratio=... runtime ms=... sleep ms=...' values. "
            f"Acting OSDs={acting_osds}"
        )

    log.info(
        "[%s] Collected work_ratio metrics from %d/%d acting OSD(s)",
        phase_label,
        len(osds_with_data),
        len(acting_osds),
    )
    return phase_data


def run_case5(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 5: Verify cleaner stats stay unchanged when idle_time is modified.

    Steps:
        1. Create a test pool.
        2. Generate BLUEFS_SPILLOVER warning.
        3. Collect spillover cleaner stats from all OSDs (Stats1).
        4. Set bluefs_spillover_idle_time to 20 seconds.
        5. Wait for 20 seconds.
        6. Collect spillover cleaner stats from all OSDs (Stats2).
        7. Wait 20 seconds, then collect spillover cleaner stats again (Stats3).
        8. Set bluefs_spillover_idle_time to 10 seconds.
        9. Wait 15 seconds, then collect spillover cleaner stats (Stats4).
        10. Print Stats1-Stats4 in logs.
        11. Verify Stats1-Stats4 have the same migrated/pending files (migrated
            files may exist; no new migrated files between consecutive snapshots).
        12. Whether step 11 passes or fails, enable bluefs_spillover_cleaner and
            wait for BLUEFS_SPILLOVER to clear.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If migrated/pending files differ across snapshots, new
            migrated files appear between consecutive snapshots, or spillover
            warning is not cleared.
    """
    log.info(run_case5.__doc__)

    idle_time_phase1 = int(config.get("idle_time_phase1", 20))
    idle_time_phase2 = int(config.get("idle_time_phase2", 10))
    all_osd_ids = [str(osd_id) for osd_id in rados_obj.get_osd_list(status="up")]

    pool_name = create_test_pool(rados_obj, config)
    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    stats1 = collect_spillover_cleaner_stats_for_osds(
        rados_obj, all_osd_ids, label="Stats1"
    )

    log.info("Setting %s to %s seconds", BLUEFS_SPILLOVER_IDLE_TIME, idle_time_phase1)
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_IDLE_TIME,
        value=str(idle_time_phase1),
    ), f"Failed to set {BLUEFS_SPILLOVER_IDLE_TIME} to {idle_time_phase1}"

    log.info("Waiting %s seconds after setting idle_time", idle_time_phase1)
    time.sleep(idle_time_phase1)

    stats2 = collect_spillover_cleaner_stats_for_osds(
        rados_obj, all_osd_ids, label="Stats2"
    )

    log.info("Waiting 20 seconds before collecting Stats3")
    time.sleep(20)

    stats3 = collect_spillover_cleaner_stats_for_osds(
        rados_obj, all_osd_ids, label="Stats3"
    )

    log.info("Setting %s to %s seconds", BLUEFS_SPILLOVER_IDLE_TIME, idle_time_phase2)
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_IDLE_TIME,
        value=str(idle_time_phase2),
    ), f"Failed to set {BLUEFS_SPILLOVER_IDLE_TIME} to {idle_time_phase2}"

    log.info("Waiting 15 seconds before collecting Stats4")
    time.sleep(15)

    stats4 = collect_spillover_cleaner_stats_for_osds(
        rados_obj, all_osd_ids, label="Stats4"
    )

    log_spillover_cleaner_stats_snapshots(
        {
            "Stats1": stats1,
            "Stats2": stats2,
            "Stats3": stats3,
            "Stats4": stats4,
        }
    )

    stats_verify_error = None
    try:
        verify_spillover_cleaner_stats_unchanged_no_migration(
            {
                "Stats1": stats1,
                "Stats2": stats2,
                "Stats3": stats3,
                "Stats4": stats4,
            }
        )
    except Exception as err:
        stats_verify_error = err
        log.error(
            "Case 5 stats verification failed; continuing to enable cleaner and "
            "wait for BLUEFS_SPILLOVER cleanup: %s",
            err,
        )
    finally:
        log.info("Setting %s to true on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
        assert mon_obj.set_config(
            section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
        ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"
        disable_bluefs_debug_force_slow(mon_obj)
        wait_for_bluefs_spillover_cleared(rados_obj, config)

    if stats_verify_error:
        raise stats_verify_error

    log.info("Case 5 completed successfully")


def run_case6(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 6: Scope BlueFS spillover force_slow/cleaner to the pool acting set.

    Validates that spillover can be triggered and cleaned using per-OSD configs
    on the acting set only, and that spillover health remains correct across
    acting OSD restarts.

    Steps:
        1. Create a pool with a single PG (pg_num=1, pg_num_min=1, autoscaler off).
        2. Fetch the acting set for the pool PG (pool_id.0).
        3. Set bluefs_debug_force_slow=true only on acting OSDs using:
           ceph config set osd.<id> bluefs_debug_force_slow true
        4. Generate BlueFS spillover with rados bench and verify spillover OSDs
           are a non-empty subset of the acting set only.
        5. Restart any one acting OSD; after it is up and running, verify the
           BlueFS spillover warning still exists and remains limited to the
           acting set.
        6. Set bluefs_spillover_cleaner=true only on acting OSDs, and set
           bluefs_debug_force_slow=false on those OSDs so cleanup can proceed.
        7. Immediately restart any one acting OSD.
        8. Wait until the BlueFS spillover warning is cleared from cluster health.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary. Supported keys include:
            pool_name, pool_config, pool_settle_wait, osd_restart_timeout,
            spillover_timeout, cleaner_timeout, poll_interval,
            rados_write_duration, byte_size.

    Raises:
        AssertionError: If the acting set is empty, spillover appears on
            non-acting OSDs, spillover is missing after the first restart,
            OSD restart fails, or spillover is not cleared after cleaner
            enable and the second restart.
    """
    log.info(run_case6.__doc__)
    log.info("=" * 80)
    log.info("Starting Case 6: BlueFS spillover scoped to pool acting set")
    log.info("=" * 80)

    case_config = dict(config)
    pool_config = dict(case_config.get("pool_config", {}))
    pool_config.setdefault("pg_num", 1)
    pool_config.setdefault("pg_num_min", 1)
    pool_config.setdefault("disable_pg_autoscale", True)
    case_config["pool_config"] = pool_config
    pool_settle_wait = case_config.get("pool_settle_wait", 15)

    acting_osds: list[str] = []
    try:
        log.info("Step 1/8: Creating single-PG pool for acting-set spillover test")
        log.info(
            "Pool create options: pg_num=%s, pg_num_min=%s, disable_pg_autoscale=%s",
            pool_config.get("pg_num"),
            pool_config.get("pg_num_min"),
            pool_config.get("disable_pg_autoscale"),
        )
        pool_name = create_test_pool(rados_obj, case_config)
        log.info(
            "Waiting %s seconds for pool %s PGs to settle before fetching acting set",
            pool_settle_wait,
            pool_name,
        )
        time.sleep(pool_settle_wait)

        log.info("Step 2/8: Fetching acting set for pool %s", pool_name)
        acting_osds = [
            str(osd_id) for osd_id in rados_obj.get_pg_acting_set(pool_name=pool_name)
        ]
        log.info(
            "Acting set for pool %s contains %d OSD(s): %s",
            pool_name,
            len(acting_osds),
            acting_osds,
        )
        if not acting_osds:
            raise AssertionError(f"Empty acting set returned for pool {pool_name}")

        log.info(
            "Step 3/8: Setting %s=true only on acting OSDs %s",
            BLUEFS_DEBUG_FORCE_SLOW,
            acting_osds,
        )
        set_osd_daemon_config_on_osds(
            mon_obj=mon_obj,
            osd_ids=acting_osds,
            name=BLUEFS_DEBUG_FORCE_SLOW,
            value="true",
        )

        log.info(
            "Step 4/8: Generating BlueFS spillover on pool %s and verifying "
            "spillover OSDs are limited to the acting set",
            pool_name,
        )
        trigger_bluefs_spillover_message(
            rados_obj=rados_obj,
            mon_obj=mon_obj,
            pool_name=pool_name,
            config=case_config,
            set_force_slow=False,
        )
        spillover_osds = get_spillover_osds_from_health_detail(rados_obj)
        log.info(
            "Collected BLUEFS_SPILLOVER OSDs after generation: %s",
            spillover_osds,
        )
        verify_spillover_osds_only_on_acting_set(
            spillover_osds=spillover_osds,
            acting_osds=acting_osds,
        )

        log.info(
            "Step 5/8: Restarting one acting OSD, then verifying spillover warning "
            "still exists and remains limited to the acting set"
        )
        restart_osd_id = pick_acting_osd_to_restart(acting_osds)
        log.info(
            "Case 6 first restart target selected from acting set: osd.%s",
            restart_osd_id,
        )
        restart_acting_osd_and_wait_up(
            rados_obj=rados_obj,
            osd_id=restart_osd_id,
            config=case_config,
        )
        log.info(
            "Checking BlueFS spillover health after osd.%s came back up",
            restart_osd_id,
        )
        if not is_bluefs_spillover_present(rados_obj):
            raise AssertionError(
                "BlueFS spillover message does not exist after restarting "
                f"acting OSD osd.{restart_osd_id}. Expected spillover to persist."
            )
        log.info(
            "BlueFS spillover warning is still present after osd.%s restart",
            restart_osd_id,
        )
        spillover_osds_after_restart = get_spillover_osds_from_health_detail(rados_obj)
        log.info(
            "Collected BLUEFS_SPILLOVER OSDs after first restart: %s",
            spillover_osds_after_restart,
        )
        verify_spillover_osds_only_on_acting_set(
            spillover_osds=spillover_osds_after_restart,
            acting_osds=acting_osds,
        )
        log.info(
            "Verified spillover message is proper after osd.%s restart | "
            "spillover OSDs=%s | acting set=%s",
            restart_osd_id,
            spillover_osds_after_restart,
            acting_osds,
        )

        log.info(
            "Step 6/8: Enabling %s=true only on acting OSDs %s",
            BLUEFS_SPILLOVER_CLEANER,
            acting_osds,
        )
        set_osd_daemon_config_on_osds(
            mon_obj=mon_obj,
            osd_ids=acting_osds,
            name=BLUEFS_SPILLOVER_CLEANER,
            value="true",
        )
        log.info(
            "Setting %s=false on acting OSDs so spillover cleaner can clear the "
            "warning",
            BLUEFS_DEBUG_FORCE_SLOW,
        )
        set_osd_daemon_config_on_osds(
            mon_obj=mon_obj,
            osd_ids=acting_osds,
            name=BLUEFS_DEBUG_FORCE_SLOW,
            value="false",
        )

        log.info(
            "Step 7/8: Immediately restarting one acting OSD after enabling "
            "spillover cleaner"
        )
        restart_osd_id = pick_acting_osd_to_restart(
            acting_osds, exclude_osd_id=restart_osd_id
        )
        log.info(
            "Case 6 second restart target selected from acting set: osd.%s",
            restart_osd_id,
        )
        restart_acting_osd_and_wait_up(
            rados_obj=rados_obj,
            osd_id=restart_osd_id,
            config=case_config,
        )

        log.info(
            "Step 8/8: Waiting for BlueFS spillover warning to clear after cleaner "
            "enable and osd.%s restart",
            restart_osd_id,
        )
        wait_for_bluefs_spillover_cleared(rados_obj, case_config)
        log.info(
            "BlueFS spillover warning cleared successfully after acting-set cleaner "
            "enable and OSD restart"
        )

        log.info("=" * 80)
        log.info("Case 6 completed successfully")
        log.info("=" * 80)
    finally:
        if acting_osds:
            log.info(
                "Case 6 cleanup: removing per-OSD %s and %s from acting OSDs %s",
                BLUEFS_DEBUG_FORCE_SLOW,
                BLUEFS_SPILLOVER_CLEANER,
                acting_osds,
            )
            try:
                remove_osd_daemon_config_on_osds(
                    mon_obj=mon_obj,
                    osd_ids=acting_osds,
                    name=BLUEFS_DEBUG_FORCE_SLOW,
                )
            except Exception as cleanup_err:
                log.warning(
                    "Failed to remove per-OSD %s during case 7 cleanup: %s",
                    BLUEFS_DEBUG_FORCE_SLOW,
                    cleanup_err,
                )
            try:
                remove_osd_daemon_config_on_osds(
                    mon_obj=mon_obj,
                    osd_ids=acting_osds,
                    name=BLUEFS_SPILLOVER_CLEANER,
                )
            except Exception as cleanup_err:
                log.warning(
                    "Failed to remove per-OSD %s during case 7 cleanup: %s",
                    BLUEFS_SPILLOVER_CLEANER,
                    cleanup_err,
                )
            log.info("Case 6 cleanup completed for acting-set OSD configs")
        else:
            log.info(
                "Case 6 cleanup: no acting OSDs were collected; skipping per-OSD "
                "config removal"
            )


def run_case7(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Case 7: Verify cleaner stats have no residual slow entries after HEALTH_OK.

    Steps:
        1. Set bluefs_debug_force_slow to true.
        2. Run rados bench to push data.
        3. Generate the BlueFS spillover health warning.
        4. Stop the background rados bench.
        5. Set bluefs_debug_force_slow to false.
        6. Set bluefs_spillover_cleaner to true.
        7. Wait for cluster health to reach HEALTH_OK.
        8. Verify bluefs spillover cleaner stats contain no "slow" entries.
        9. If any "slow" entries still exist, require the BlueFS spillover
           message in ceph -s; fail if the message is absent.
        10. Set bluefs_spillover_cleaner to false.
        11. If "slow" entries exist in cleaner stats, verify those file names
            are not present in bluefs files list.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.

    Raises:
        AssertionError: If any verification step fails.
    """
    log.info(run_case7.__doc__)

    pool_name = create_test_pool(rados_obj, config)

    log.info("Step 1-4: Generate BlueFS spillover via force_slow and rados bench")
    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )

    log.info("Step 5: Setting %s to false on OSD daemons", BLUEFS_DEBUG_FORCE_SLOW)
    disable_bluefs_debug_force_slow(mon_obj)

    log.info("Step 6: Setting %s to true on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="true"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to true"

    log.info("Step 7: Waiting for cluster health to reach HEALTH_OK")
    verify_cluster_health_ok(rados_obj, config)

    all_osd_ids = [str(osd_id) for osd_id in rados_obj.get_osd_list(status="up")]
    log.info(
        "Step 8-9: Verifying spillover cleaner stats have no 'slow' entries on "
        "%d OSD(s); if any remain, BlueFS spillover must be present in ceph -s",
        len(all_osd_ids),
    )
    verify_no_slow_entries_in_cleaner_stats_or_spillover_reported(
        rados_obj=rados_obj,
        osd_ids=all_osd_ids,
    )

    log.info("Step 10: Setting %s to false on OSD daemons", BLUEFS_SPILLOVER_CLEANER)
    assert mon_obj.set_config(
        section="osd", name=BLUEFS_SPILLOVER_CLEANER, value="false"
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER} to false"

    log.info(
        "Step 11: Verifying any 'slow' cleaner-stat files are absent from "
        "bluefs files list"
    )
    verify_slow_cleaner_stat_files_absent_from_files_list(
        rados_obj=rados_obj,
        osd_ids=all_osd_ids,
    )

    log.info("Case 7 completed successfully")


def run_idle_time_verification_case(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
    default_idle_time: int,
    case_number: int,
) -> None:
    """
    Run idle-time verification workflow for case 2 or case 3.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.
        default_idle_time: Default bluefs_spillover_idle_time when not in config.
        case_number: Case number (2 or 3) for logging.

    Raises:
        AssertionError: If idle-time gap verification fails.
    """
    log.info(
        "Running case %d idle-time verification with default idle_time=%s seconds",
        case_number,
        default_idle_time,
    )

    idle_time = int(config.get("bluefs_spillover_idle_time", default_idle_time))
    post_action_wait = config.get("post_action_wait", 10)

    configure_idle_time_debug_settings(
        mon_obj, config, default_idle_time=default_idle_time
    )
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
    log.info(
        "Waiting %s seconds after spillover cleanup for migrated logs to be generated",
        post_action_wait,
    )
    time.sleep(post_action_wait)

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
    )

    log.info("Case %d completed successfully", case_number)


def configure_idle_time_debug_settings(
    mon_obj: MonConfigMethods, config: dict, default_idle_time: int = 300
) -> None:
    """
    Configure debug and spillover idle-time settings for idle-time verification cases.

    Sets debug_bluefs and bluefs_spillover_idle_time on all OSD daemons.

    Args:
        mon_obj: MonConfigMethods instance.
        config: Test case configuration dictionary.
        default_idle_time: Default bluefs_spillover_idle_time when not in config.

    Raises:
        AssertionError: If config set operations fail.
    """
    idle_time = config.get("bluefs_spillover_idle_time", default_idle_time)
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


def truncate_osd_logs_for_osds(
    rados_obj: RadosOrchestrator, osd_ids: list[str]
) -> None:
    """
    Truncate OSD log files only for the given OSD IDs.

    Compresses each matching ceph-osd.<id>.log and then truncates it to zero,
    without clearing logs for OSDs outside the provided list.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: OSD IDs whose log files should be truncated.

    Raises:
        AssertionError: If truncation fails for any requested OSD log.
    """
    if not osd_ids:
        raise AssertionError("No OSD IDs provided for log truncation")

    fsid = get_cluster_fsid(rados_obj)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log.info(
        "Truncating OSD logs for %d OSD(s): %s",
        len(osd_ids),
        sorted(osd_ids, key=int),
    )

    for osd_id in osd_ids:
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=str(osd_id))
        log_path = f"/var/log/ceph/{fsid}/ceph-osd.{osd_id}.log"
        truncate_cmd = (
            f"if [ -f {log_path} ]; then "
            f"gzip -c {log_path} > {log_path}.{timestamp}.gz && "
            f"truncate -s 0 {log_path}; "
            f"else echo 'Log file not found: {log_path}'; fi"
        )
        log.info(
            "Truncating osd.%s log on host %s at %s",
            osd_id,
            host.hostname,
            log_path,
        )
        try:
            out, _ = host.exec_command(sudo=True, cmd=truncate_cmd, check_ec=False)
            if out and "Log file not found" in out:
                log.warning(
                    "OSD log file not found while truncating osd.%s at %s",
                    osd_id,
                    log_path,
                )
            else:
                log.info("Successfully truncated log for osd.%s", osd_id)
        except Exception as err:
            raise AssertionError(
                f"Failed to truncate log for osd.{osd_id} at {log_path}: {err}"
            ) from err


def get_cluster_fsid(rados_obj: RadosOrchestrator) -> str:
    """
    Return the cluster FSID, fetching once and caching on rados_obj.

    Args:
        rados_obj: RadosOrchestrator instance.

    Returns:
        Cluster FSID string.
    """
    cached_fsid = getattr(rados_obj, "_cached_cluster_fsid", None)
    if cached_fsid:
        return cached_fsid

    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    rados_obj._cached_cluster_fsid = fsid
    log.info("Resolved and cached cluster FSID: %s", fsid)
    return fsid


def read_osd_log_lines(
    rados_obj: RadosOrchestrator, osd_id: str, fsid: str | None = None
) -> list[str]:
    """
    Read OSD log file contents from the OSD host.

    Reads /var/log/ceph/{fsid}/ceph-osd.{id}.log. Falls back to grep for
    migrate_file log lines if the full file read returns empty output.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.
        fsid: Optional cluster FSID. When omitted, resolved once via
            get_cluster_fsid() and reused for subsequent calls.

    Returns:
        List of log lines from the OSD log file.
    """
    if not fsid:
        fsid = get_cluster_fsid(rados_obj)
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
    fsid = get_cluster_fsid(rados_obj)
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id, fsid=fsid)
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
    fsid = get_cluster_fsid(rados_obj)
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id, fsid=fsid)
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

    table_rows = []
    for osd_id in osd_ids:
        last_entry = last_migrated_logs.get(osd_id)
        first_entry = first_migrated_logs.get(osd_id)

        if last_entry and first_entry:
            time_gap = (
                first_entry["timestamp"] - last_entry["timestamp"]
            ).total_seconds()
            status = "AVAILABLE"
        elif last_entry or first_entry:
            time_gap = None
            status = "PARTIAL"
        else:
            time_gap = None
            status = "MISSING"

        table_rows.append(
            {
                "osd_id": osd_id,
                "last_migrated_time": (
                    last_entry["timestamp_str"] if last_entry else None
                ),
                "first_migrated_time": (
                    first_entry["timestamp_str"] if first_entry else None
                ),
                "time_difference_seconds": time_gap,
                "status": status,
                "last_migrated_log": (last_entry["log_line"] if last_entry else None),
                "first_migrated_log": (
                    first_entry["log_line"] if first_entry else None
                ),
            }
        )

    log_migrated_log_comparison_table(
        "Migrated logs reference table (cycle 1 last vs cycle 2 first)",
        table_rows,
        include_log_messages=True,
    )


def verify_migrated_log_idle_time_gap(
    spillover_osds: list[str],
    last_migrated_logs: dict[str, dict],
    first_migrated_logs: dict[str, dict],
    idle_time: int,
) -> None:
    """
    Verify migrated log idle-time gap for spillover OSDs with logs in both cycles.

    Compares the first migrated log timestamp in cycle 2 against the last
    migrated log timestamp in cycle 1 for each spillover OSD. OSDs missing
    migrated logs in either cycle are skipped. At least one spillover OSD must
    have logs in both cycles, and every such OSD must have gap >= idle_time
    seconds.

    Args:
        spillover_osds: OSD IDs that reported BlueFS spillover in health detail.
        last_migrated_logs: Last migrated log entries from cycle 1.
        first_migrated_logs: First migrated log entries from cycle 2.
        idle_time: Expected minimum gap in seconds (bluefs_spillover_idle_time).

    Raises:
        AssertionError: If no comparable OSD logs exist, or any comparable OSD
            has a gap less than idle_time.
    """
    failing_osds = []
    comparable_osds = []
    comparison_results = []

    for osd_id in spillover_osds:
        if osd_id not in last_migrated_logs or osd_id not in first_migrated_logs:
            log.warning(
                "Skipping osd.%s idle-time verification due to missing migrated "
                "logs in one of the cycles",
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
        comparable_osds.append(osd_id)

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

        if time_gap < idle_time:
            failing_osds.append(osd_id)

    log_migrated_log_comparison_table(
        "Migrated log idle-time verification summary",
        sorted(comparison_results, key=lambda row: int(row["osd_id"])),
        required_idle_time=idle_time,
        include_log_messages=False,
    )

    if not comparable_osds:
        raise AssertionError(
            "Expected at least one spillover OSD with migrated logs in both cycles, "
            f"but none were found among spillover OSDs: {spillover_osds}"
        )

    if failing_osds:
        raise AssertionError(
            f"Expected all comparable spillover OSDs to have migrated log gap "
            f">= {idle_time} seconds, but {len(failing_osds)} failed: "
            f"{failing_osds}. Comparison results: {comparison_results}"
        )

    log.info(
        "Verified migrated log idle-time gap on comparable spillover OSDs: %s",
        comparable_osds,
    )


def verify_bluefs_spillover_cleaner_work_ratio_default(
    mon_obj: MonConfigMethods, expected: float = 0.1
) -> None:
    """
    Verify that the default value of bluefs_spillover_cleaner_work_ratio is 0.1.

    Args:
        mon_obj: MonConfigMethods instance.
        expected: Expected default work ratio value.

    Raises:
        AssertionError: If the default value does not match expected.
    """
    default_value = mon_obj.get_config(
        section="osd", param=BLUEFS_SPILLOVER_CLEANER_WORK_RATIO
    )
    log.info(
        "Default value of %s: %s",
        BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
        default_value,
    )
    if abs(float(default_value) - expected) > 0.0001:
        raise AssertionError(
            f"Expected default {BLUEFS_SPILLOVER_CLEANER_WORK_RATIO} to be "
            f"{expected}, got {default_value}"
        )
    log.info(
        "Verified default value of %s is %s",
        BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
        expected,
    )


def set_bluefs_spillover_cleaner_work_ratio(
    mon_obj: MonConfigMethods, work_ratio
) -> None:
    """
    Set bluefs_spillover_cleaner_work_ratio on OSD daemons.

    Runs the equivalent of:
        ceph config set osd bluefs_spillover_cleaner_work_ratio <value>

    For work_ratio 0 the value is set as integer 0 so the command matches:
        ceph config set osd bluefs_spillover_cleaner_work_ratio 0

    Args:
        mon_obj: MonConfigMethods instance.
        work_ratio: Work ratio value to set (e.g. 0, 0.9, 0.5, 0.1).

    Raises:
        AssertionError: If the config set operation fails.
    """
    ratio = float(work_ratio)
    # Use int 0 so CLI becomes "... work_ratio 0" and verify uses float compare.
    value = 0 if ratio == 0 else ratio

    log.info(
        "Running: ceph config set osd %s %s",
        BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
        value,
    )
    assert mon_obj.set_config(
        section="osd",
        name=BLUEFS_SPILLOVER_CLEANER_WORK_RATIO,
        value=value,
    ), f"Failed to set {BLUEFS_SPILLOVER_CLEANER_WORK_RATIO} to {value}"


def parse_work_ratio_runtime_sleep_from_log_lines(log_lines: list[str]) -> list[dict]:
    """
    Parse work_ratio / runtime ms / sleep ms values from OSD log lines.

    Matches BlueFS spillover cleaner dout lines such as:
        entering wait work_ratio=0.1 runtime ms=12 sleep ms=108

    Args:
        log_lines: OSD log lines to scan.

    Returns:
        List of dicts with keys work_ratio, runtime_ms, sleep_ms, and log_line.
    """
    entries = []
    for line in log_lines:
        match = WORK_RATIO_RUNTIME_SLEEP_PATTERN.search(line)
        if not match:
            continue
        entries.append(
            {
                "work_ratio": float(match.group(1)),
                "runtime_ms": int(match.group(2)),
                "sleep_ms": int(match.group(3)),
                "log_line": line.strip(),
            }
        )
    return entries


def collect_work_ratio_runtime_sleep_for_osds(
    rados_obj: RadosOrchestrator,
    osd_ids: list[str],
) -> dict[str, dict]:
    """
    Collect work_ratio and average runtime_ms/sleep_ms from each OSD log.

    For each OSD, parses all matching spillover-cleaner wait log lines, then:
      - work_ratio: taken from the last matching entry
      - runtime_ms: average of all runtime ms values
      - sleep_ms: average of all sleep ms values

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: OSD IDs to scan.

    Returns:
        Mapping of osd_id to metrics dict with work_ratio, runtime_ms, sleep_ms,
        sample_count, and log_line; or {} when no matches are found.
    """
    fsid = get_cluster_fsid(rados_obj)
    results = {}
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id, fsid=fsid)
        entries = parse_work_ratio_runtime_sleep_from_log_lines(log_lines)
        if not entries:
            log.warning(
                "No work_ratio/runtime_ms/sleep_ms log entries found on osd.%s "
                "after scanning %d log lines",
                osd_id,
                len(log_lines),
            )
            continue

        runtime_values = [entry["runtime_ms"] for entry in entries]
        sleep_values = [entry["sleep_ms"] for entry in entries]
        avg_runtime_ms = sum(runtime_values) / float(len(runtime_values))
        avg_sleep_ms = sum(sleep_values) / float(len(sleep_values))
        last_entry = entries[-1]

        results[osd_id] = {
            "work_ratio": last_entry["work_ratio"],
            "runtime_ms": avg_runtime_ms,
            "sleep_ms": avg_sleep_ms,
            "sample_count": len(entries),
            "runtime_ms_values": runtime_values,
            "sleep_ms_values": sleep_values,
            "log_line": last_entry["log_line"],
        }
        log.info(
            "osd.%s work_ratio metrics | work_ratio=%s | runtime_ms_avg=%.3f "
            "(from %s samples: %s) | sleep_ms_avg=%.3f (from %s samples: %s) | "
            "last_log=%s",
            osd_id,
            last_entry["work_ratio"],
            avg_runtime_ms,
            len(runtime_values),
            runtime_values,
            avg_sleep_ms,
            len(sleep_values),
            sleep_values,
            last_entry["log_line"],
        )
    return results


def log_work_ratio_runtime_sleep_table(
    phase_label: str,
    phase_data: dict[str, dict],
    osd_ids: list[str],
) -> None:
    """
    Log a per-OSD table of work_ratio and average runtime_ms/sleep_ms for one phase.

    OSDs without collected metrics are omitted so the table does not show N/A rows.

    Args:
        phase_label: Phase label (e.g. work_ratio=0.5).
        phase_data: Mapping of osd_id to metrics dict.
        osd_ids: Ordered OSD IDs to include as rows.
    """
    rows = []
    for osd_id in osd_ids:
        entry = phase_data.get(osd_id) or {}
        if entry.get("work_ratio") is None and entry.get("runtime_ms") is None:
            continue
        rows.append(
            (
                _get_osd_label(osd_id),
                (
                    f"{entry['work_ratio']:.4g}"
                    if entry.get("work_ratio") is not None
                    else "N/A"
                ),
                (
                    f"{entry['runtime_ms']:.3f}"
                    if entry.get("runtime_ms") is not None
                    else "N/A"
                ),
                (
                    f"{entry['sleep_ms']:.3f}"
                    if entry.get("sleep_ms") is not None
                    else "N/A"
                ),
                str(entry.get("sample_count", "N/A")),
            )
        )

    if not rows:
        log.info(
            "[%s] No acting OSDs with work_ratio metrics to display",
            phase_label,
        )
        return

    headers = ("OSD", "work_ratio", "runtime_ms_avg", "sleep_ms_avg", "samples")
    widths = [
        max(len(headers[idx]), max(len(row[idx]) for row in rows))
        for idx in range(len(headers))
    ]
    separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    header_line = (
        "| "
        + " | ".join(headers[idx].ljust(widths[idx]) for idx in range(len(headers)))
        + " |"
    )

    log.info(
        "[%s] Acting OSD work_ratio / runtime_ms_avg / sleep_ms_avg table:",
        phase_label,
    )
    log.info(separator)
    log.info(header_line)
    log.info(separator)
    for row in rows:
        log.info(
            "| "
            + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(headers)))
            + " |"
        )
    log.info(separator)


def log_work_ratio_runtime_sleep_phase_results(
    phase_results: dict[str, dict[str, dict]],
) -> None:
    """
    Log per-phase work_ratio / runtime_ms_avg / sleep_ms_avg tables.

    Only OSDs with collected metrics are included; rows that would be all N/A
    are omitted.

    Args:
        phase_results: Mapping of phase_label to per-OSD metrics dicts.
    """
    if not phase_results:
        log.info("Case 4 work_ratio phase results: no data collected")
        return

    log.info("=" * 80)
    log.info("Case 4 consolidated work_ratio / runtime_ms_avg / sleep_ms_avg results")
    log.info("=" * 80)
    for phase_label, phase_data in phase_results.items():
        phase_osd_ids = sorted(
            (
                osd_id
                for osd_id, entry in phase_data.items()
                if entry
                and (
                    entry.get("work_ratio") is not None
                    or entry.get("runtime_ms") is not None
                )
            ),
            key=int,
        )
        log_work_ratio_runtime_sleep_table(phase_label, phase_data, phase_osd_ids)


def compute_phase_overall_runtime_sleep_averages(
    phase_data: dict[str, dict],
) -> tuple[float, float, int]:
    """
    Average runtime_ms_avg and sleep_ms_avg across OSDs for one work_ratio phase.

    Args:
        phase_data: Mapping of osd_id to per-OSD metrics dict.

    Returns:
        Tuple of (overall_runtime_ms_avg, overall_sleep_ms_avg, osd_count).

    Raises:
        AssertionError: If no OSD metrics are available for the phase.
    """
    runtime_values = []
    sleep_values = []
    for osd_id, entry in phase_data.items():
        if not entry:
            continue
        if entry.get("runtime_ms") is None or entry.get("sleep_ms") is None:
            continue
        runtime_values.append(float(entry["runtime_ms"]))
        sleep_values.append(float(entry["sleep_ms"]))
        log.info(
            "Including osd.%s in phase overall average | runtime_ms_avg=%.3f | "
            "sleep_ms_avg=%.3f",
            osd_id,
            entry["runtime_ms"],
            entry["sleep_ms"],
        )

    if not runtime_values or not sleep_values:
        raise AssertionError(
            "Cannot compute phase overall averages: no OSD runtime_ms_avg / "
            "sleep_ms_avg values available"
        )

    overall_runtime = sum(runtime_values) / float(len(runtime_values))
    overall_sleep = sum(sleep_values) / float(len(sleep_values))
    return overall_runtime, overall_sleep, len(runtime_values)


def verify_work_ratio_phase_metric_ordering(
    phase_results: dict[str, dict[str, dict]],
) -> None:
    """
    Verify overall runtime/sleep averages decrease as work_ratio increases.

    For work_ratio=0.1 and 0.9, averages runtime_ms_avg and sleep_ms_avg across
    the phase OSDs, then asserts:
      runtime_ms_avg(0.1) > runtime_ms_avg(0.9)
      sleep_ms_avg(0.1) > sleep_ms_avg(0.9)

    Args:
        phase_results: Mapping of phase_label to per-OSD metrics dicts.

    Raises:
        AssertionError: If a required phase is missing, has no OSD metrics, or
            the ordering checks fail.
    """
    required_phases = ("work_ratio=0.1", "work_ratio=0.9")
    missing_phases = [label for label in required_phases if label not in phase_results]
    if missing_phases:
        raise AssertionError(
            "Cannot verify work_ratio metric ordering; missing phase results for: "
            f"{missing_phases}. Available phases={list(phase_results.keys())}"
        )

    phase_averages = {}
    log.info("=" * 80)
    log.info("Case 4 overall phase averages for ordering verification")
    log.info("=" * 80)
    for phase_label in required_phases:
        overall_runtime, overall_sleep, osd_count = (
            compute_phase_overall_runtime_sleep_averages(phase_results[phase_label])
        )
        phase_averages[phase_label] = {
            "runtime_ms_avg": overall_runtime,
            "sleep_ms_avg": overall_sleep,
            "osd_count": osd_count,
        }
        log.info(
            "%s | overall_runtime_ms_avg=%.3f | overall_sleep_ms_avg=%.3f | "
            "osd_count=%d",
            phase_label,
            overall_runtime,
            overall_sleep,
            osd_count,
        )

    runtime_0_1 = phase_averages["work_ratio=0.1"]["runtime_ms_avg"]
    runtime_0_9 = phase_averages["work_ratio=0.9"]["runtime_ms_avg"]
    sleep_0_1 = phase_averages["work_ratio=0.1"]["sleep_ms_avg"]
    sleep_0_9 = phase_averages["work_ratio=0.9"]["sleep_ms_avg"]

    log.info(
        "Comparing overall runtime_ms_avg ordering: "
        "work_ratio=0.1 (%.3f) > work_ratio=0.9 (%.3f)",
        runtime_0_1,
        runtime_0_9,
    )
    if not (runtime_0_1 > runtime_0_9):
        raise AssertionError(
            "Expected overall runtime_ms_avg ordering "
            "work_ratio=0.1 > work_ratio=0.9, but got "
            f"0.1={runtime_0_1:.3f}, 0.9={runtime_0_9:.3f}"
        )

    log.info(
        "Comparing overall sleep_ms_avg ordering: "
        "work_ratio=0.1 (%.3f) > work_ratio=0.9 (%.3f)",
        sleep_0_1,
        sleep_0_9,
    )
    if not (sleep_0_1 > sleep_0_9):
        raise AssertionError(
            "Expected overall sleep_ms_avg ordering "
            "work_ratio=0.1 > work_ratio=0.9, but got "
            f"0.1={sleep_0_1:.3f}, 0.9={sleep_0_9:.3f}"
        )

    log.info(
        "Verified overall metric ordering: runtime_ms_avg and sleep_ms_avg both "
        "satisfy work_ratio=0.1 > work_ratio=0.9"
    )


def verify_spillover_osd_lists_unchanged(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    osd_list_before: list[str],
    duration: int,
) -> None:
    """
    Verify BLUEFS_SPILLOVER OSD list is unchanged after waiting with work_ratio=0.

    Sets bluefs_debug_force_slow to false, waits for the given duration, collects
    OSD_list_after from BLUEFS_SPILLOVER health messages, and compares it with
    OSD_list_before. Fails if the lists differ and reports deviation percentage.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        osd_list_before: Spillover OSD IDs collected before the wait.
        duration: Wait duration in seconds before collecting OSD_list_after.

    Raises:
        AssertionError: If OSD_list_before and OSD_list_after are not the same.
    """
    log.info(
        "Setting %s to false before verifying spillover OSD list remains unchanged",
        BLUEFS_DEBUG_FORCE_SLOW,
    )
    disable_bluefs_debug_force_slow(mon_obj)

    log.info(
        "Waiting %s seconds before collecting OSD_list_after for BLUEFS_SPILLOVER",
        duration,
    )
    time.sleep(duration)

    osd_list_after = get_spillover_osds_from_health_detail(
        rados_obj, require_osds=False
    )
    log.info("OSD_list_before (BLUEFS_SPILLOVER OSDs): %s", osd_list_before)
    log.info("OSD_list_after  (BLUEFS_SPILLOVER OSDs): %s", osd_list_after)

    before_set = set(osd_list_before)
    after_set = set(osd_list_after)
    osds_cleared = sorted(before_set - after_set, key=int)
    osds_new = sorted(after_set - before_set, key=int)
    changed_osds = sorted(before_set.symmetric_difference(after_set), key=int)

    if before_set:
        deviation_pct = (len(changed_osds) / len(before_set)) * 100
    elif after_set:
        deviation_pct = 100.0
    else:
        deviation_pct = 0.0

    log.info(
        "Spillover OSD list comparison | before=%d | after=%d | changed=%d | "
        "deviation=%.2f%% | cleared=%s | newly_added=%s",
        len(osd_list_before),
        len(osd_list_after),
        len(changed_osds),
        deviation_pct,
        osds_cleared,
        osds_new,
    )

    if before_set != after_set:
        raise AssertionError(
            "OSD_list_before and OSD_list_after are not the same for "
            f"BLUEFS_SPILLOVER. Deviation: {deviation_pct:.2f}%. "
            f"OSD_list_before={osd_list_before}, OSD_list_after={osd_list_after}, "
            f"cleared_osds={osds_cleared}, newly_added_osds={osds_new}"
        )

    log.info(
        "Verified OSD_list_before and OSD_list_after are the same "
        "(deviation=0.00%%) after waiting %s seconds with work_ratio=0",
        duration,
    )


def generate_spillover_and_wait_for_cleanup(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    pool_name: str,
    config: dict,
) -> None:
    """
    Generate spillover warning, disable force_slow, and wait for cleanup.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        pool_name: Pool name for rados bench.
        config: Test configuration dictionary.
    """
    post_action_wait = config.get("post_action_wait", 10)

    trigger_bluefs_spillover_message(
        rados_obj=rados_obj,
        mon_obj=mon_obj,
        pool_name=pool_name,
        config=config,
    )
    disable_bluefs_debug_force_slow(mon_obj)
    wait_for_bluefs_spillover_cleared(rados_obj, config)
    log.info(
        "Waiting %s seconds after spillover cleanup for migrated logs to be generated",
        post_action_wait,
    )
    time.sleep(post_action_wait)


def collect_all_migration_timestamps(
    rados_obj: RadosOrchestrator, osd_ids: list[str]
) -> list[datetime.datetime]:
    """
    Collect migration timestamps from migrated log entries on all OSDs.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to scan.

    Returns:
        Sorted list of migration datetime objects from all OSD logs.
    """
    migration_timestamps = []
    fsid = get_cluster_fsid(rados_obj)
    for osd_id in osd_ids:
        log_lines = read_osd_log_lines(rados_obj, osd_id, fsid=fsid)
        migrated_entries = get_migrated_log_entries(log_lines)
        migration_timestamps.extend(entry["timestamp"] for entry in migrated_entries)
        log.info(
            "Collected %d migrated log entries from osd.%s",
            len(migrated_entries),
            osd_id,
        )

    migration_timestamps.sort()
    log.info(
        "Collected %d total migration timestamps from %d OSD(s)",
        len(migration_timestamps),
        len(osd_ids),
    )
    return migration_timestamps


def calculate_average_migration_interval(
    migration_timestamps: list[datetime.datetime], label: str
) -> float:
    """
    Calculate average time difference between consecutive migration timestamps.

    Args:
        migration_timestamps: Sorted list of migration datetime objects.
        label: Label for logging (e.g. TIME_AVG1).

    Returns:
        Average interval in seconds between consecutive migrations.

    Raises:
        AssertionError: If fewer than two migration timestamps are available.
    """
    if len(migration_timestamps) < 2:
        raise AssertionError(
            f"{label} requires at least 2 migration timestamps, "
            f"but found {len(migration_timestamps)}"
        )

    intervals = [
        (migration_timestamps[index + 1] - migration_timestamps[index]).total_seconds()
        for index in range(len(migration_timestamps) - 1)
    ]
    average_interval = sum(intervals) / len(intervals)

    log.info("%s migration intervals (seconds): %s", label, intervals)
    log.info("%s average migration interval: %.3f seconds", label, average_interval)
    return average_interval


def collect_average_migration_interval(
    rados_obj: RadosOrchestrator, osd_ids: list[str], label: str
) -> float:
    """
    Collect migration timestamps from all OSDs and calculate average interval.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to scan.
        label: Label for logging and error messages.

    Returns:
        Average interval in seconds between consecutive migrations.
    """
    migration_timestamps = collect_all_migration_timestamps(rados_obj, osd_ids)
    return calculate_average_migration_interval(migration_timestamps, label=label)


def verify_migration_interval_averages(
    time_avg1: float, time_avg2: float, time_avg3: float
) -> None:
    """
    Verify TIME_AVG1 > TIME_AVG2 > TIME_AVG3 for work ratio phases.

    Args:
        time_avg1: Average interval with work_ratio 0.9.
        time_avg2: Average interval with work_ratio 0.5.
        time_avg3: Average interval with work_ratio 0.1.

    Raises:
        AssertionError: If the expected ordering is not satisfied.
    """
    log.info(
        "Migration interval averages | TIME_AVG1=%.3f | TIME_AVG2=%.3f | "
        "TIME_AVG3=%.3f seconds",
        time_avg1,
        time_avg2,
        time_avg3,
    )
    if not (time_avg1 > time_avg2 > time_avg3):
        raise AssertionError(
            "Expected TIME_AVG1 > TIME_AVG2 > TIME_AVG3, but got "
            f"TIME_AVG1={time_avg1:.3f}, TIME_AVG2={time_avg2:.3f}, "
            f"TIME_AVG3={time_avg3:.3f}"
        )

    log.info("Verified migration interval ordering: TIME_AVG1 > TIME_AVG2 > TIME_AVG3")


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
    require_osds: bool = True,
) -> list[str]:
    """
    Collect OSD IDs reporting BlueFS spillover from ceph health detail.

    Parses JSON health detail when available, falling back to text output.
    Matches OSDs with "spilled over" in their health message.

    Args:
        rados_obj: RadosOrchestrator instance.
        require_osds: When True, raise if no spillover OSDs are found.

    Returns:
        Sorted list of unique OSD IDs as strings (without the osd. prefix).

    Raises:
        AssertionError: If require_osds is True and no spillover OSDs are found.
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
        if require_osds:
            raise AssertionError(
                "No OSDs with BlueFS spillover found in ceph health detail"
            )
        log.warning("No OSDs with BlueFS spillover found in ceph health detail")
        return []

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


def collect_spillover_cleaner_stats_for_osds(
    rados_obj: RadosOrchestrator, osd_ids: list[str], label: str
) -> dict[str, dict]:
    """
    Collect bluefs spillover cleaner stats from all given OSDs.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: List of OSD IDs to query.
        label: Snapshot label used in logs (e.g. Stats1).

    Returns:
        Dictionary mapping osd_id to cleaner stats output.
    """
    stats_by_osd = {}
    log.info(
        "Collecting spillover cleaner stats for %s from %d OSD(s)", label, len(osd_ids)
    )
    for osd_id in osd_ids:
        cleaner_stats_output = run_osd_bluefs_command(
            rados_obj, osd_id, "bluefs spillover cleaner stats"
        )
        if not isinstance(cleaner_stats_output, dict):
            cleaner_stats_output = {"raw_output": cleaner_stats_output}
        stats_by_osd[osd_id] = cleaner_stats_output
        migrated_files, pending_files = get_spillover_cleaner_stats_lists(
            cleaner_stats_output
        )
        log.info(
            "%s | osd.%s | migrated=%s | pending=%s",
            label,
            osd_id,
            migrated_files,
            pending_files,
        )
    return stats_by_osd


def normalize_spillover_cleaner_stats_snapshot(
    stats_by_osd: dict[str, dict],
) -> dict[str, dict]:
    """
    Normalize cleaner stats for comparison across snapshots.

    Args:
        stats_by_osd: Mapping of osd_id to cleaner stats output.

    Returns:
        Mapping of osd_id to sorted migrated and pending file lists.
    """
    normalized = {}
    for osd_id, cleaner_stats_output in stats_by_osd.items():
        migrated_files, pending_files = get_spillover_cleaner_stats_lists(
            cleaner_stats_output
        )
        normalized[osd_id] = {
            "migrated_files": sorted(migrated_files),
            "pending_files": sorted(pending_files),
        }
    return normalized


def log_spillover_cleaner_stats_snapshots(
    stats_snapshots: dict[str, dict[str, dict]],
) -> None:
    """
    Log all collected spillover cleaner stats snapshots in table format.

    Prints a summary count table across snapshots and a detailed per-OSD table
    with migrated/pending file lists for each snapshot.

    Args:
        stats_snapshots: Mapping of snapshot label to per-OSD stats.
    """
    if not stats_snapshots:
        log.info("Spillover cleaner stats snapshots: no data to display")
        return

    labels = list(stats_snapshots.keys())
    osd_ids = sorted(
        {
            osd_id
            for stats_by_osd in stats_snapshots.values()
            for osd_id in stats_by_osd.keys()
        },
        key=int,
    )
    if not osd_ids:
        log.info("Spillover cleaner stats snapshots: no OSD stats to display")
        return

    snapshot_data = {}
    for label, stats_by_osd in stats_snapshots.items():
        snapshot_data[label] = {}
        for osd_id in osd_ids:
            migrated_files, pending_files = get_spillover_cleaner_stats_lists(
                stats_by_osd.get(osd_id, {})
            )
            snapshot_data[label][osd_id] = {
                "migrated_files": sorted(migrated_files),
                "pending_files": sorted(pending_files),
                "migrated_count": len(migrated_files),
                "pending_count": len(pending_files),
            }

    osd_width = max(len(_get_osd_label(osd_id)) for osd_id in osd_ids)
    osd_width = max(osd_width, len("OSD"))
    count_width = 14
    status_width = 8

    # Summary table: one row per OSD with mig/pend counts per snapshot
    summary_parts = [f"{'OSD':<{osd_width}}"]
    for label in labels:
        summary_parts.append(f"{label + ' Mig':>{count_width}}")
        summary_parts.append(f"{label + ' Pend':>{count_width}}")
    summary_parts.append(f"{'Status':<{status_width}}")
    summary_header = " | ".join(summary_parts)
    summary_separator = "-" * len(summary_header)

    log.info("Spillover cleaner stats summary table:")
    log.info(summary_separator)
    log.info(summary_header)
    log.info(summary_separator)

    for osd_id in osd_ids:
        baseline = snapshot_data[labels[0]][osd_id]
        status = "SAME"
        for label in labels[1:]:
            current = snapshot_data[label][osd_id]
            if (
                current["migrated_files"] != baseline["migrated_files"]
                or current["pending_files"] != baseline["pending_files"]
            ):
                status = "DIFF"
                break

        row_parts = [f"{_get_osd_label(osd_id):<{osd_width}}"]
        for label in labels:
            data = snapshot_data[label][osd_id]
            row_parts.append(f"{data['migrated_count']:>{count_width}}")
            row_parts.append(f"{data['pending_count']:>{count_width}}")
        row_parts.append(f"{status:<{status_width}}")
        log.info(" | ".join(row_parts))

    log.info(summary_separator)

    # Detail table: one row per OSD per snapshot with file lists
    snapshot_width = max(len("Snapshot"), max(len(label) for label in labels))
    files_width = 60
    detail_header = (
        f"{'OSD':<{osd_width}} | "
        f"{'Snapshot':<{snapshot_width}} | "
        f"{'#Migrated':>9} | "
        f"{'#Pending':>8} | "
        f"{'Migrated Files':<{files_width}} | "
        f"{'Pending Files':<{files_width}}"
    )
    detail_separator = "-" * len(detail_header)

    log.info("Spillover cleaner stats detail table:")
    log.info(detail_separator)
    log.info(detail_header)
    log.info(detail_separator)

    for osd_id in osd_ids:
        for label in labels:
            data = snapshot_data[label][osd_id]
            migrated_text = _truncate_text(str(data["migrated_files"]), files_width)
            pending_text = _truncate_text(str(data["pending_files"]), files_width)
            log.info(
                f"{_get_osd_label(osd_id):<{osd_width}} | "
                f"{label:<{snapshot_width}} | "
                f"{data['migrated_count']:>9} | "
                f"{data['pending_count']:>8} | "
                f"{migrated_text:<{files_width}} | "
                f"{pending_text:<{files_width}}"
            )
        log.info(detail_separator)


def verify_spillover_cleaner_stats_unchanged_no_migration(
    stats_snapshots: dict[str, dict[str, dict]],
) -> None:
    """
    Verify migrated files stay the same across Stats1-Stats4.

    Migrated files are allowed to exist. Verification fails only when:
      - migrated files differ across snapshots, or
      - new migrated files appear between consecutive snapshots
        (Stats1->Stats2, Stats2->Stats3, Stats3->Stats4).

    Pending files must also remain unchanged across all snapshots.

    Args:
        stats_snapshots: Mapping of snapshot label (Stats1-Stats4) to per-OSD stats.

    Raises:
        AssertionError: If migrated/pending files differ across snapshots or new
            migrated files appear between consecutive snapshots.
    """
    if not stats_snapshots:
        raise AssertionError("No spillover cleaner stats snapshots provided")

    labels = list(stats_snapshots.keys())
    if len(labels) < 2:
        raise AssertionError(
            f"Expected at least 2 stats snapshots for comparison, got: {labels}"
        )

    normalized_snapshots = {
        label: normalize_spillover_cleaner_stats_snapshot(stats_by_osd)
        for label, stats_by_osd in stats_snapshots.items()
    }

    osd_ids = sorted(
        {
            osd_id
            for snapshot in normalized_snapshots.values()
            for osd_id in snapshot.keys()
        },
        key=int,
    )
    if not osd_ids:
        raise AssertionError("No OSD cleaner stats found in any snapshot")

    for label, snapshot in normalized_snapshots.items():
        missing_osds = [osd_id for osd_id in osd_ids if osd_id not in snapshot]
        if missing_osds:
            raise AssertionError(
                f"{label} is missing cleaner stats for OSDs: {missing_osds}"
            )

    differences = []
    new_migrated_failures = []

    log.info(
        "Comparing spillover cleaner stats across %s for %d OSD(s). "
        "Migrated files may exist but must be the same in all snapshots, with no "
        "new migrated files between consecutive snapshots.",
        labels,
        len(osd_ids),
    )

    for osd_id in osd_ids:
        baseline_label = labels[0]
        baseline = normalized_snapshots[baseline_label][osd_id]
        baseline_migrated = set(baseline["migrated_files"])

        for label in labels:
            osd_stats = normalized_snapshots[label][osd_id]
            log.info(
                "osd.%s | %s | migrated=%s | pending=%s",
                osd_id,
                label,
                osd_stats["migrated_files"],
                osd_stats["pending_files"],
            )
            if osd_stats != baseline:
                differences.append(
                    {
                        "osd_id": osd_id,
                        "baseline_snapshot": baseline_label,
                        "compared_snapshot": label,
                        "baseline": baseline,
                        "compared": osd_stats,
                    }
                )
                log.error(
                    "Stats difference on osd.%s | %s=%s | %s=%s",
                    osd_id,
                    baseline_label,
                    baseline,
                    label,
                    osd_stats,
                )

        # Consecutive comparisons: Stats1->Stats2, Stats2->Stats3, Stats3->Stats4
        for index in range(len(labels) - 1):
            current_label = labels[index]
            next_label = labels[index + 1]
            current_migrated = set(
                normalized_snapshots[current_label][osd_id]["migrated_files"]
            )
            next_migrated = set(
                normalized_snapshots[next_label][osd_id]["migrated_files"]
            )
            new_migrated = sorted(next_migrated - current_migrated)
            removed_migrated = sorted(current_migrated - next_migrated)

            if new_migrated or removed_migrated:
                new_migrated_failures.append(
                    {
                        "osd_id": osd_id,
                        "comparison": f"{current_label} -> {next_label}",
                        "new_migrated_files": new_migrated,
                        "removed_migrated_files": removed_migrated,
                        "current_migrated": sorted(current_migrated),
                        "next_migrated": sorted(next_migrated),
                    }
                )
                log.error(
                    "Migrated files changed on osd.%s between %s and %s | "
                    "new=%s | removed=%s",
                    osd_id,
                    current_label,
                    next_label,
                    new_migrated,
                    removed_migrated,
                )
            else:
                log.info(
                    "osd.%s | %s -> %s | migrated files unchanged: %s",
                    osd_id,
                    current_label,
                    next_label,
                    sorted(current_migrated),
                )

        if baseline_migrated:
            log.info(
                "osd.%s has migrated files present in all snapshots (allowed): %s",
                osd_id,
                sorted(baseline_migrated),
            )

    if new_migrated_failures:
        raise AssertionError(
            "Expected no new migrated files between consecutive snapshots "
            f"(Stats1->Stats2, Stats2->Stats3, Stats3->Stats4), but found changes: "
            f"{new_migrated_failures}"
        )

    if differences:
        raise AssertionError(
            "Expected migrated and pending files to be the same across "
            f"Stats1, Stats2, Stats3, and Stats4, but found {len(differences)} "
            f"mismatch(es): {differences}"
        )

    log.info(
        "Verified Stats1-Stats4 have identical migrated/pending files for all OSDs "
        "(migrated files may exist; no new migrated files between consecutive "
        "snapshots)"
    )


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


def _collect_slow_entries_from_object(obj, found=None) -> list:
    """
    Recursively collect objects that include a non-zero "slow" field.

    Args:
        obj: Nested dict/list structure from cleaner stats JSON.
        found: Accumulator list used during recursion.

    Returns:
        List of dicts (or scalar wrappers) that contain a "slow" field > 0.
    """
    if found is None:
        found = []

    if isinstance(obj, dict):
        if "slow" in obj:
            slow_raw = obj.get("slow")
            include = False
            try:
                include = int(slow_raw or 0) > 0
            except (TypeError, ValueError):
                include = bool(slow_raw)
            if include:
                found.append(obj)
        for value in obj.values():
            _collect_slow_entries_from_object(value, found)
    elif isinstance(obj, list):
        for item in obj:
            _collect_slow_entries_from_object(item, found)
    return found


def get_slow_entries_from_cleaner_stats(cleaner_stats_output) -> list:
    """
    Extract entries that contain a "slow" field from cleaner stats output.

    Also treats a raw stats dump containing the '"slow":' token as evidence of
    slow entries when structured parsing finds none.

    Args:
        cleaner_stats_output: JSON/text output from spillover cleaner stats.

    Returns:
        List of slow entries (dicts) found in the stats output.
    """
    if cleaner_stats_output is None:
        return []

    if isinstance(cleaner_stats_output, dict):
        stats = cleaner_stats_output.get(
            "spillover_cleaner_stats", cleaner_stats_output
        )
        slow_entries = _collect_slow_entries_from_object(stats)
        if slow_entries:
            return slow_entries
        # Fallback: literal "slow": token in serialized output
        if '"slow":' in str(cleaner_stats_output):
            return [{"raw_slow_token": True, "raw": cleaner_stats_output}]
        return []

    text = str(cleaner_stats_output)
    if '"slow":' in text or "slow:" in text:
        return [{"raw_slow_token": True, "raw": text}]
    return []


def get_file_names_from_slow_entries(slow_entries: list) -> set[str]:
    """
    Extract BlueFS file names referenced by slow cleaner-stat entries.

    Args:
        slow_entries: Entries returned by get_slow_entries_from_cleaner_stats.

    Returns:
        Set of file path/name strings.
    """
    file_names = set()
    for entry in slow_entries:
        if not isinstance(entry, dict):
            continue
        for key in ("file", "File", "name", "path"):
            value = entry.get(key)
            if isinstance(value, str) and value.strip():
                file_names.add(value.split(" size=")[0].strip())
                break
    return file_names


def get_bluefs_file_names(rados_obj: RadosOrchestrator, osd_id: str) -> set[str]:
    """
    Collect BlueFS file names from 'bluefs files list' on an OSD.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID as a string.

    Returns:
        Set of BlueFS file name strings.
    """
    files_output = run_osd_bluefs_command(rados_obj, osd_id, "bluefs files list")
    file_entries = (
        files_output
        if isinstance(files_output, list)
        else files_output.get("files", []) if isinstance(files_output, dict) else []
    )

    file_names = set()
    for file_entry in file_entries:
        if isinstance(file_entry, dict):
            name = file_entry.get("name", "")
            if name:
                file_names.add(name)
        elif isinstance(file_entry, str) and file_entry.strip():
            file_names.add(file_entry.strip())
    return file_names


def verify_no_slow_entries_in_cleaner_stats_or_spillover_reported(
    rados_obj: RadosOrchestrator,
    osd_ids: list[str],
) -> None:
    """
    Verify cleaner stats have no "slow" entries after HEALTH_OK (case 8 steps 8-9).

    If any OSD still reports "slow" entries in spillover cleaner stats, the BlueFS
    spillover warning must still be present in cluster status. Absence of that
    warning while slow entries remain is a test failure. Presence of slow entries
    after HEALTH_OK is also treated as a failure of the no-slow-entries check.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: OSD IDs to query.

    Raises:
        AssertionError: If slow entries exist without spillover warning, or if
            slow entries remain after HEALTH_OK.
    """
    osds_with_slow = {}
    for osd_id in osd_ids:
        cleaner_stats_output = run_osd_bluefs_command(
            rados_obj, osd_id, "bluefs spillover cleaner stats"
        )
        if not isinstance(cleaner_stats_output, dict):
            cleaner_stats_output = {"raw_output": cleaner_stats_output}
        slow_entries = get_slow_entries_from_cleaner_stats(cleaner_stats_output)
        if slow_entries:
            osds_with_slow[osd_id] = slow_entries
            log.warning(
                "Found %d 'slow' entr(y/ies) in spillover cleaner stats on osd.%s: %s",
                len(slow_entries),
                osd_id,
                slow_entries,
            )
        else:
            log.info(
                "Verified spillover cleaner stats on osd.%s contain no 'slow' entries",
                osd_id,
            )

    if not osds_with_slow:
        log.info(
            "Verified spillover cleaner stats have no 'slow' entries on all %d OSD(s)",
            len(osd_ids),
        )
        return

    # Step 9: slow entries exist → BlueFS spillover message must be present
    if not is_bluefs_spillover_present(rados_obj):
        raise AssertionError(
            "Found 'slow' entries in bluefs spillover cleaner stats but BlueFS "
            "spillover message is not present in ceph -s / health detail. "
            f"OSDs with slow entries: {sorted(osds_with_slow.keys(), key=int)}"
        )

    raise AssertionError(
        "Expected no 'slow' entries in bluefs spillover cleaner stats after "
        "HEALTH_OK, but found slow entries on OSDs: "
        f"{sorted(osds_with_slow.keys(), key=int)}"
    )


def verify_slow_cleaner_stat_files_absent_from_files_list(
    rados_obj: RadosOrchestrator,
    osd_ids: list[str],
) -> None:
    """
    Verify slow cleaner-stat files are absent from bluefs files list (case 8 step 11).

    For each OSD, if spillover cleaner stats still list "slow" entries, those file
    names must not appear in 'bluefs files list'.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_ids: OSD IDs to query.

    Raises:
        AssertionError: If any slow cleaner-stat file is still present in the
            bluefs files list.
    """
    failures = []
    checked_any_slow = False

    for osd_id in osd_ids:
        cleaner_stats_output = run_osd_bluefs_command(
            rados_obj, osd_id, "bluefs spillover cleaner stats"
        )
        if not isinstance(cleaner_stats_output, dict):
            cleaner_stats_output = {"raw_output": cleaner_stats_output}

        slow_entries = get_slow_entries_from_cleaner_stats(cleaner_stats_output)
        slow_file_names = get_file_names_from_slow_entries(slow_entries)
        if not slow_file_names:
            log.info(
                "osd.%s has no named 'slow' cleaner-stat files to cross-check "
                "against bluefs files list",
                osd_id,
            )
            continue

        checked_any_slow = True
        files_list_names = get_bluefs_file_names(rados_obj, osd_id)
        overlapping = sorted(slow_file_names.intersection(files_list_names))
        log.info(
            "osd.%s slow cleaner-stat files=%s | bluefs files list count=%d | "
            "overlap=%s",
            osd_id,
            sorted(slow_file_names),
            len(files_list_names),
            overlapping,
        )
        if overlapping:
            failures.append(
                f"osd.{osd_id}: slow cleaner-stat files still present in "
                f"bluefs files list: {overlapping}"
            )

    if failures:
        raise AssertionError(
            "Slow entries from spillover cleaner stats must not appear in "
            f"bluefs files list. Failures: {failures}"
        )

    if checked_any_slow:
        log.info(
            "Verified all named 'slow' cleaner-stat files are absent from "
            "bluefs files list"
        )
    else:
        log.info(
            "No named 'slow' cleaner-stat files found after disabling cleaner; "
            "step 11 has nothing to cross-check"
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


def set_osd_daemon_config_on_osds(
    mon_obj: MonConfigMethods,
    osd_ids: list[str],
    name: str,
    value: str,
) -> None:
    """
    Set an OSD daemon config on specific OSDs only.

    Applies config at the individual OSD level instead of the global osd section.
    Equivalent CLI for each OSD:
        ceph config set osd.<id> <name> <value>

    Args:
        mon_obj: MonConfigMethods instance.
        osd_ids: List of OSD IDs to target.
        name: Config parameter name.
        value: Config value to set.

    Raises:
        AssertionError: If any per-OSD config set operation fails.
    """
    log.info(
        "Setting %s=%s on %d OSD(s): %s",
        name,
        value,
        len(osd_ids),
        osd_ids,
    )
    for osd_id in osd_ids:
        log.info("Running: ceph config set osd.%s %s %s", osd_id, name, value)
        assert mon_obj.set_config(
            section=f"osd.{osd_id}", name=name, value=value
        ), f"Failed to set {name}={value} on osd.{osd_id}"
        log.info("Successfully set %s=%s on osd.%s", name, value, osd_id)


def remove_osd_daemon_config_on_osds(
    mon_obj: MonConfigMethods,
    osd_ids: list[str],
    name: str,
) -> None:
    """
    Remove an OSD daemon config from specific OSDs.

    Equivalent CLI for each OSD:
        ceph config rm osd.<id> <name>

    Args:
        mon_obj: MonConfigMethods instance.
        osd_ids: List of OSD IDs to target.
        name: Config parameter name to remove.
    """
    log.info(
        "Removing config %s from %d OSD(s): %s",
        name,
        len(osd_ids),
        osd_ids,
    )
    for osd_id in osd_ids:
        log.info("Removing config %s from osd.%s", name, osd_id)
        try:
            mon_obj.remove_config(section=f"osd.{osd_id}", name=name)
            log.info("Removed config %s from osd.%s", name, osd_id)
        except Exception as cleanup_err:
            log.warning(
                "Failed to remove config %s from osd.%s during cleanup: %s",
                name,
                osd_id,
                cleanup_err,
            )


def verify_spillover_osds_only_on_acting_set(
    spillover_osds: list[str],
    acting_osds: list[str],
) -> None:
    """
    Verify BLUEFS_SPILLOVER OSDs are limited to the pool acting set.

    Ensures spillover health detail reports at least one OSD and that every
    reported spillover OSD belongs to the acting set. Non-acting OSDs must not
    appear in the spillover warning.

    Args:
        spillover_osds: OSD IDs reporting BlueFS spillover.
        acting_osds: OSD IDs from the pool acting set.

    Raises:
        AssertionError: If no spillover OSDs are found, or any spillover OSD is
            outside the acting set.
    """
    acting_set = set(str(osd_id) for osd_id in acting_osds)
    spillover_set = set(str(osd_id) for osd_id in spillover_osds)
    acting_sorted = sorted(acting_set, key=int)
    spillover_sorted = sorted(spillover_set, key=int)

    log.info(
        "Comparing BLUEFS_SPILLOVER OSDs against acting set | acting=%s | "
        "spillover=%s",
        acting_sorted,
        spillover_sorted,
    )

    if not spillover_set:
        raise AssertionError(
            "Expected BlueFS spillover on acting set OSDs, but no spillover OSDs "
            f"were found. Acting set={acting_sorted}"
        )

    non_acting_spillover = sorted(spillover_set - acting_set, key=int)
    if non_acting_spillover:
        raise AssertionError(
            "BlueFS spillover was reported on OSDs outside the acting set: "
            f"{non_acting_spillover}. Acting set={acting_sorted}, "
            f"spillover OSDs={spillover_sorted}"
        )

    acting_with_spillover = sorted(spillover_set & acting_set, key=int)
    log.info(
        "Verified BlueFS spillover message is proper: spillover OSDs %s are all "
        "within acting set %s",
        acting_with_spillover,
        acting_sorted,
    )


def pick_acting_osd_to_restart(
    acting_osds: list[str], exclude_osd_id: str | None = None
) -> str:
    """
    Pick one OSD from the acting set to restart.

    Prefers an OSD other than exclude_osd_id when multiple acting OSDs exist so
    consecutive restart steps can exercise different daemons.

    Args:
        acting_osds: OSD IDs from the pool acting set.
        exclude_osd_id: Optional OSD ID to avoid if another acting OSD is available.

    Returns:
        OSD ID selected for restart.

    Raises:
        AssertionError: If acting_osds is empty.
    """
    if not acting_osds:
        raise AssertionError("Cannot pick OSD to restart from empty acting set")

    candidates = [str(osd_id) for osd_id in acting_osds]
    if exclude_osd_id is not None:
        filtered = [osd_id for osd_id in candidates if osd_id != str(exclude_osd_id)]
        if filtered:
            log.info(
                "Excluding previously restarted osd.%s from candidates %s",
                exclude_osd_id,
                candidates,
            )
            candidates = filtered
        else:
            log.info(
                "Only one acting OSD available; reusing osd.%s for restart",
                exclude_osd_id,
            )

    selected_osd = candidates[0]
    log.info(
        "Selected acting OSD osd.%s for restart from candidates %s",
        selected_osd,
        candidates,
    )
    return selected_osd


def restart_acting_osd_and_wait_up(
    rados_obj: RadosOrchestrator,
    osd_id: str,
    config: dict,
) -> None:
    """
    Restart an acting-set OSD and wait until it is up and running.

    Uses RadosOrchestrator.change_osd_state(action='restart') and then confirms
    daemon status is running before returning.

    Args:
        rados_obj: RadosOrchestrator instance.
        osd_id: OSD ID to restart.
        config: Test configuration with optional osd_restart_timeout (default 300).

    Raises:
        AssertionError: If OSD restart fails or OSD is not running afterward.
    """
    restart_timeout = config.get("osd_restart_timeout", 300)
    log.info(
        "Restarting acting OSD osd.%s and waiting up to %s seconds for it to become "
        "running",
        osd_id,
        restart_timeout,
    )
    assert rados_obj.change_osd_state(
        action="restart", target=int(osd_id), timeout=restart_timeout
    ), f"Failed to restart osd.{osd_id}"

    osd_status, status_desc = rados_obj.get_daemon_status(
        daemon_type="osd", daemon_id=int(osd_id)
    )
    log.info(
        "Post-restart daemon status for osd.%s: status=%s, description=%s",
        osd_id,
        osd_status,
        status_desc,
    )
    if not (osd_status == 1 or status_desc == "running"):
        raise AssertionError(
            f"osd.{osd_id} is not up and running after restart "
            f"(status={osd_status}, description={status_desc})"
        )

    log.info("Confirmed osd.%s is up and running after restart", osd_id)


def trigger_bluefs_spillover_message(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    pool_name: str,
    config: dict,
    set_force_slow: bool = True,
) -> None:
    """
    Generate the "OSD(s) experiencing BlueFS spillover" health warning.

    Steps:
        1. Optionally set bluefs_debug_force_slow to true on all OSD daemons.
        2. Run rados bench write in the background until the spillover warning
           appears in cluster health. The bench process is always killed before
           returning (success, timeout, or error) so later steps are not affected.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        pool_name: Name of the pool to run rados bench against.
        config: Test configuration with spillover_timeout, poll_interval,
            rados_write_duration, and byte_size.
        set_force_slow: When True, set bluefs_debug_force_slow=true globally.
            Set False when force_slow was already configured on selected OSDs.

    Raises:
        AssertionError: If the spillover warning does not appear within timeout.
    """
    if set_force_slow:
        log.info("Setting %s to true on OSD daemons", BLUEFS_DEBUG_FORCE_SLOW)
        assert mon_obj.set_config(
            section="osd", name=BLUEFS_DEBUG_FORCE_SLOW, value="true"
        ), f"Failed to set {BLUEFS_DEBUG_FORCE_SLOW} to true"
    else:
        log.info(
            "Skipping global %s set; expecting it already configured on target OSDs",
            BLUEFS_DEBUG_FORCE_SLOW,
        )

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
    try:
        while datetime.datetime.now() < end_time:
            if is_bluefs_spillover_present(rados_obj):
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

        raise AssertionError(
            f"Expected BlueFS spillover warning did not appear within "
            f"{spillover_timeout} seconds"
        )
    finally:
        # Always stop background bench before returning to the next test step
        # (success, timeout, or unexpected error).
        if bench_started:
            kill_rados_bench_write(rados_obj, pool_name=pool_name)


def verify_spillover_cleaner_removes_warning(
    rados_obj: RadosOrchestrator,
    mon_obj: MonConfigMethods,
    config: dict,
) -> None:
    """
    Verify that enabling bluefs_spillover_cleaner clears the spillover warning.

    Enables the cleaner and polls cluster health until the warning is cleared.

    Args:
        rados_obj: RadosOrchestrator instance.
        mon_obj: MonConfigMethods instance.
        config: Test configuration with cleaner_timeout and poll_interval.

    Raises:
        AssertionError: If spillover warning is not cleared within the timeout.
    """
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

    raise AssertionError(
        f"BlueFS spillover warning was not cleared within {cleaner_timeout} seconds"
    )
