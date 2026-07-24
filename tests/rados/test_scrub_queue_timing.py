"""
Test module for verifying Ceph scrub queue timing functionality.

This module contains tests to validate that Ceph's scrub queue mechanism correctly
schedules and executes periodic scrub operations on placement groups (PGs). The test
verifies:

1. Scrub scheduling: Ensures that scrub operations are properly scheduled based on
   configured intervals and time windows.

2. Scrub queue patterns: Validates that scrub operations follow the expected sequence:
   - Periodic scrub scheduled events
   - Scrub activity (queued/scrubbing states)
   - Proper timestamp progression

3. Configuration management: Tests the ability to set and remove scrub-related OSD
   configuration parameters.

The test creates a pool, generates test objects, configures scrub parameters, and
monitors scrub operations to ensure they are executed according to the configured
schedule and follow the expected queue patterns.

Key Features:
- Creates a replicated pool for testing
- Generates 10,000 objects using rados bench
- Configures scrub intervals and time windows
- Monitors scrub stamps and schedule details
- Verifies scrub sequence patterns across PGs
- Cleans up configuration and test resources
"""

import datetime
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Main test function to verify scrub queue timing functionality.

    This test performs the following operations:
    1. Creates a replicated pool using the provided configuration
    2. Generates 10,000 test objects using rados bench
    3. Waits for PGs to reach active+clean state
    4. Extracts initial scrub stamps and schedule details for all PGs
    5. Configures scheduled scrub parameters (intervals and time windows)
    6. Monitors scrub operations for up to 900 seconds, tracking:
       - Changes in scrub stamps (last_deep_scrub_stamp)
       - Scrub schedule details for each PG
       - Unique scrub scheduled messages
    7. Verifies that scrub operations follow the expected sequence pattern:
       - Periodic scrub scheduled events
       - Scrub activity between periodic events
       - Proper timestamp progression
    8. Cleans up test resources (pool deletion and parameter removal)

    Args:
        ceph_cluster: Ceph cluster object containing cluster configuration
        **kw: Keyword arguments containing:
            - config: Dictionary with test configuration including:
                - pool_name: Name of the pool to create for testing
                - Other pool configuration parameters (replicated_config, etc.)

    Returns:
        int: 0 if test passes, 1 if test fails

    Test Flow:
        1. Pool Creation -> Object Generation -> PG Stabilization
        2. Baseline Data Collection (scrub stamps, schedule details)
        3. Scrub Parameter Configuration
        4. Monitoring Loop (scrub execution tracking)
        5. Pattern Verification
        6. Cleanup

    Raises:
        Exception: Any unhandled exception during test execution is caught,
                   logged, and returns 1 (test failure)
    """
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    scrub_object = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    pool_name = config.get("pool_name")
    operation = config.get("operation")

    try:

        # Create pool using replicated_config
        if not rados_object.create_pool(**config):
            log.error("Failed to create the replicated Pool")
            return 1

        log.info("Successfully created pool: %s", pool_name)
        method_should_succeed(wait_for_clean_pg_sets, rados_object, timeout=300)

        # Get PG list from the pool and select a random PG
        log.info("Getting PG list from pool: %s", pool_name)
        pg_list = rados_object.get_pgid(pool_name=pool_name)
        if not pg_list:
            log.error("No PGs found in pool %s", pool_name)
            return 1
        log.info(
            "Retrieved %d PG IDs from pool %s: %s", len(pg_list), pool_name, pg_list
        )

        # Get the ceph pg <pg_id> query
        log.info(
            "Extracting last %s operation details before scrub operations", operation
        )
        before_test_last_ops = get_last_scrub_ops_detail(
            rados_object, pg_list, operation
        )
        log.info(
            "Successfully extracted last %s operation details for %d PGs before scrub operations",
            operation,
            len([v for v in before_test_last_ops.values() if v is not None]),
        )

        # Generate 10000 objects using rados bench
        log.info("Generating 10000 objects using rados bench")
        if not rados_object.bench_write(pool_name=pool_name, max_objs=10000):
            log.error("Failed to generate objects using rados bench")
            return 1
        log.info("Successfully generated 10000 objects using rados bench")

        # Wait for PGs to become stable (active+clean)
        log.info(
            "Waiting for PGs in pool %s to become stable (active+clean)", pool_name
        )
        if not rados_object.wait_for_clean_pg_sets(test_pool=pool_name, timeout=1800):
            log.error(
                "PGs in pool %s did not reach active+clean state within timeout",
                pool_name,
            )
            return 1
        log.info("PGs in pool %s are now stable (active+clean)", pool_name)

        log.info("Fetching initial scrub stamps from pool before scrub operations")
        active_pool_before_scrub_stamps = rados_object.get_scrub_stamps(
            pool_name=pool_name
        )
        log.info(
            "Retrieved initial scrub stamps for %d PGs",
            len(active_pool_before_scrub_stamps),
        )

        # Extract scrub_schedule details for all PGs in pg_list
        log.info(
            "Extracting initial scrub_schedule details for %d PGs in pg_list",
            len(pg_list),
        )

        log.info("Setting scheduled scrub parameters")
        set_scheduled_scrub_parameters(scrub_object, operation)
        log.info("Successfully set scheduled scrub parameters")

        before_scrub_schedule_details = extract_scrub_schedule_details(
            rados_object, pg_list
        )
        if operation == "scrub":
            scrub_wait_time = 900
        else:
            scrub_wait_time = 1200
        active_pool_after_scrub_stamps = {}
        if operation == "scrub":
            scrub_type = "last_scrub_stamp"
        else:
            scrub_type = "last_deep_scrub_stamp"
        unique_scrub_scheduled_dict = {}
        scrub_check_flag = False
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=scrub_wait_time)
        log.info(
            "Starting scrub monitoring loop. Will monitor for up to %d seconds",
            scrub_wait_time,
        )
        log.info("Monitoring end time: %s", endtime)
        iteration_count = 0
        while datetime.datetime.now() < endtime:
            iteration_count += 1
            remaining_time = (endtime - datetime.datetime.now()).total_seconds()
            log.debug(
                "Monitoring iteration %d: %.1f seconds remaining",
                iteration_count,
                remaining_time,
            )

            log.debug("Fetching current scrub stamps from pool")
            active_pool_after_scrub_stamps = rados_object.get_scrub_stamps(
                pool_name=pool_name
            )

            log.debug("Checking if periodic scrub scheduled has changed for all PGs")
            result, new_pgid_list = check_periodic_scrub_scheduled_changed(
                active_pool_before_scrub_stamps,
                active_pool_after_scrub_stamps,
                scrub_type,
            )

            log.debug("Extracting current scrub schedule details for all PGs")
            after_scrub_schedule_details = extract_scrub_schedule_details(
                rados_object, pg_list
            )

            # Create a dictionary that collects every PG unique scrub_scheduled
            # Dictionary structure: {pg_id: [scrub_scheduled, ...], ...}
            # Only adds entries where scrub_scheduled value is unique in PG_id list
            log.debug("Updating unique scrub scheduled dictionary with new messages")
            new_messages_count = 0
            for pg_id, pg_details in after_scrub_schedule_details.items():
                if pg_details and pg_details.get("scrub_schedule") is not None:
                    scrub_scheduled = pg_details.get("scrub_schedule")
                    # Only add if scrub_scheduled is unique among existing values of the pg_id
                    if pg_id not in unique_scrub_scheduled_dict:
                        unique_scrub_scheduled_dict[pg_id] = []
                    if scrub_scheduled not in unique_scrub_scheduled_dict[pg_id]:
                        unique_scrub_scheduled_dict[pg_id].append(scrub_scheduled)
                        new_messages_count += 1
                        log.debug(
                            "Added new unique scrub scheduled message for PG %s: %s",
                            pg_id,
                            scrub_scheduled,
                        )

            if new_messages_count > 0:
                log.info(
                    "Found %d new unique scrub scheduled message(s) in this iteration",
                    new_messages_count,
                )
            log_scrub_scheduled_dict(unique_scrub_scheduled_dict)

            if result:
                log.info(
                    "Scrub operations detected! All PGs have changed %s", scrub_type
                )
                scrub_check_flag = True
                break
            else:
                if iteration_count % 30 == 0:  # Log every 30 iterations (60 seconds)
                    log.info(
                        "Still monitoring... %.1f seconds remaining. PGs with unchanged stamps: %d",
                        remaining_time,
                        len(new_pgid_list),
                    )
            time.sleep(2)
        if not scrub_check_flag:
            log.error(
                "Scrub operations were not performed within the %d second timeout period",
                scrub_wait_time,
            )
            log.error("Total monitoring iterations completed: %d", iteration_count)
            unchanged_count = (
                len(new_pgid_list) if "new_pgid_list" in locals() else "N/A"
            )
            log.error("PGs with unchanged %s: %s", scrub_type, unchanged_count)
            return 1

        log.info(
            "Scrub operations detected successfully. Proceeding to verify scrub sequence pattern"
        )
        total_messages = sum(len(msgs) for msgs in unique_scrub_scheduled_dict.values())
        log.info("Total unique scrub scheduled messages collected: %d", total_messages)
        log.info(
            "Number of PGs with scrub scheduled messages: %d",
            len(unique_scrub_scheduled_dict),
        )

        if not verify_scrub_sequence_pattern(unique_scrub_scheduled_dict):
            log.error("Scrub queue pattern verification failed")
            log.error(
                "Expected pattern: periodic scheduled -> queued/scrubbing -> scrubbing -> periodic scheduled"
            )
            return 1
        log.info("Scrub sequence pattern verification passed successfully")

        # Verification 2:
        # Get the ceph pg <pg_id> query
        log.info(
            "Extracting last %s operation details after scrub operations", operation
        )
        after_test_last_ops = get_last_scrub_ops_detail(
            rados_object, pg_list, operation
        )
        log.info(
            "Successfully extracted last %s operation details for %d PGs after scrub operations",
            operation,
            len([v for v in after_test_last_ops.values() if v is not None]),
        )

        log.info(
            "Verifying that last %s operation details changed for all PGs", operation
        )
        result = verify_last_ops_changed(
            before_test_last_ops, after_test_last_ops, operation
        )

        if not result:
            log.error(
                "The last operation for %s operation did not update for some PGs",
                operation,
            )
            return 1
        log.info(
            "Successfully verified that last %s operation details changed for all PGs",
            operation,
        )

        log.info(
            "=================Verification of scrub queue feature is completed ===================="
        )
        return 0

    except Exception as e:
        log.error("Test failed with exception: %s", str(e))
        log.exception("Exception details:")
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Remove parameter configuration
        log.info("Starting cleanup: removing test pool and parameter configurations")
        try:
            log.info("Deleting test pool: %s", pool_name)
            method_should_succeed(rados_object.delete_pool, pool_name)
            log.info("Successfully deleted pool: %s", pool_name)

            log.info("Removing OSD scrub parameter configurations")
            remove_parameter_configuration(mon_obj)
            log.info(
                "Cleanup completed successfully: pool deleted and parameters removed"
            )
        except Exception as e:
            log.error("Cleanup failed: %s", str(e))
            log.exception("Exception details during cleanup:")


def check_periodic_scrub_scheduled_changed(
    before_scrub_schedule_details,
    after_scrub_schedule_details,
    scrub_type="last_scrub_stamp",
):
    """
    Check if all pg_ids have their scrub stamp changed.

    Supported scrub_type values:
    - last_scrub_stamp (default)
    - last_deep_scrub_stamp
    """
    log.debug("Checking if %s has changed for all PGs", scrub_type)
    unchanged_pg_list = []

    # Validate scrub_type
    if scrub_type not in ["last_scrub_stamp", "last_deep_scrub_stamp"]:
        log.error(
            "Invalid scrub_type: %s. Must be 'last_scrub_stamp' or 'last_deep_scrub_stamp'",
            scrub_type,
        )
        return False, list(before_scrub_schedule_details.keys())

    log.debug("Comparing %s for %d PGs", scrub_type, len(before_scrub_schedule_details))
    changed_count = 0

    for pg_id, before_details in before_scrub_schedule_details.items():

        if pg_id not in after_scrub_schedule_details:
            log.warning("PG %s not found in after_scrub_schedule_details", pg_id)
            unchanged_pg_list.append(pg_id)
            continue

        after_details = after_scrub_schedule_details.get(pg_id)

        if before_details is None or after_details is None:
            before_exists = before_details is not None
            after_exists = after_details is not None
            log.warning(
                "PG %s has None details (before=%s, after=%s)",
                pg_id,
                before_exists,
                after_exists,
            )
            unchanged_pg_list.append(pg_id)
            continue

        before_stamp = before_details.get(scrub_type)
        after_stamp = after_details.get(scrub_type)

        if before_stamp is None or after_stamp is None:
            log.warning(
                "PG %s has None value for %s (before=%s, after=%s)",
                pg_id,
                scrub_type,
                before_stamp,
                after_stamp,
            )
            unchanged_pg_list.append(pg_id)
            continue

        log.debug(
            "PG %s: Before %s=%s, After %s=%s",
            pg_id,
            scrub_type,
            before_stamp,
            scrub_type,
            after_stamp,
        )

        if before_stamp == after_stamp:
            log.debug(
                "PG %s: %s has not changed (still %s)", pg_id, scrub_type, before_stamp
            )
            unchanged_pg_list.append(pg_id)
        else:
            changed_count += 1
            log.debug(
                "PG %s: %s has changed from %s to %s",
                pg_id,
                scrub_type,
                before_stamp,
                after_stamp,
            )

    if unchanged_pg_list:
        log.debug(
            "Found %d PGs with unchanged %s and %d PGs with changed %s",
            len(unchanged_pg_list),
            scrub_type,
            changed_count,
            scrub_type,
        )
        return False, unchanged_pg_list

    log.info(
        "All %d PGs have changed %s", len(before_scrub_schedule_details), scrub_type
    )
    return True, unchanged_pg_list


def set_scheduled_scrub_parameters(scrub_object, operation="scrub"):
    """
    Method to set the scheduled parameters
    Args:
        scrub_object: Scrub object
        operation: scrub or deep-scrub.
    Returns: None

    """
    try:
        log.info("Setting scheduled scrub parameters for operation: %s", operation)
        osd_scrub_min_interval = 240
        osd_scrub_max_interval = 900
        osd_deep_scrub_interval = 900
        log.info(
            "Configuration values: min_interval=%ds, max_interval=%ds, deep_scrub_interval=%ds",
            osd_scrub_min_interval,
            osd_scrub_max_interval,
            osd_deep_scrub_interval,
        )

        log.debug("Calculating scrub begin and end hours")
        (
            scrub_begin_hour,
            scrub_begin_weekday,
            scrub_end_hour,
            scrub_end_weekday,
        ) = scrub_object.add_begin_end_hours(0, 1)
        log.info(
            "Scrub time window: begin_hour=%s, begin_weekday=%s, end_hour=%s, end_weekday=%s",
            scrub_begin_hour,
            scrub_begin_weekday,
            scrub_end_hour,
            scrub_end_weekday,
        )

        log.debug("Setting osd_scrub_begin_hour configuration")
        scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
        log.info("Successfully set osd_scrub_begin_hour to %s", scrub_begin_hour)

        log.debug("Setting osd_scrub_begin_week_day configuration")
        scrub_object.set_osd_configuration(
            "osd_scrub_begin_week_day", scrub_begin_weekday
        )
        log.info("Successfully set osd_scrub_begin_week_day to %s", scrub_begin_weekday)

        log.debug("Setting osd_scrub_end_hour configuration")
        scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
        log.info("Successfully set osd_scrub_end_hour to %s", scrub_end_hour)

        log.debug("Setting osd_scrub_end_week_day configuration")
        scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
        log.info("Successfully set osd_scrub_end_week_day to %s", scrub_end_weekday)

        log.debug("Setting osd_scrub_min_interval configuration")
        scrub_object.set_osd_configuration(
            "osd_scrub_min_interval", osd_scrub_min_interval
        )
        log.info(
            "Successfully set osd_scrub_min_interval to %d", osd_scrub_min_interval
        )
        if operation == "scrub":
            log.debug("Setting osd_scrub_max_interval configuration")
            scrub_object.set_osd_configuration(
                "osd_scrub_max_interval", osd_scrub_max_interval
            )
            log.info(
                "Successfully set osd_scrub_max_interval to %d", osd_scrub_max_interval
            )

        if operation == "deep-scrub":
            log.debug("Setting osd_deep_scrub_interval configuration")
            scrub_object.set_osd_configuration(
                "osd_deep_scrub_interval", osd_deep_scrub_interval
            )
            log.info(
                "Successfully set osd_deep_scrub_interval to %d",
                osd_deep_scrub_interval,
            )

        log.info("All scheduled scrub parameters have been set successfully")
    except Exception as e:
        log.error("Failed to set scheduled scrub parameters: %s", str(e))
        log.exception("Exception details:")
        raise


def remove_parameter_configuration(mon_obj):
    """
    Used to set the default osd scrub parameter value
    Args:
        mon_obj: monitor object
    Returns : None
    """
    try:
        log.info("Starting removal of OSD scrub parameter configurations")
        parameters_to_remove = [
            "osd_scrub_min_interval",
            "osd_scrub_max_interval",
            "osd_deep_scrub_interval",
            "osd_scrub_begin_week_day",
            "osd_scrub_end_week_day",
            "osd_scrub_begin_hour",
            "osd_scrub_end_hour",
        ]

        for param_name in parameters_to_remove:
            log.debug("Removing configuration parameter: %s", param_name)
            try:
                mon_obj.remove_config(section="osd", name=param_name)
                log.info("Successfully removed %s", param_name)
            except Exception as e:
                log.warning(
                    "Failed to remove %s: %s. Continuing with other parameters.",
                    param_name,
                    str(e),
                )

        log.info("Completed removal of OSD scrub parameter configurations")
    except Exception as e:
        log.error("Failed to remove parameter configuration: %s", str(e))
        log.exception("Exception details:")
        raise


def extract_scrub_schedule_details(rados_object, pg_list):
    """
    Extract scrub_schedule details for all PGs in pg_list using threads.

    Args:
        rados_object: RadosOrchestrator object
        pg_list: List of PG IDs

    Returns:
        dict: {pg_id: {'scrub_schedule': value} or None}
    """
    try:
        log.info("Extracting scrub_schedule details for all PGs (threaded)")

        # Get ceph pg dump once (shared, read-only)
        pg_dump = rados_object.run_ceph_command(cmd="ceph pg dump pgs")
        pg_stats = pg_dump.get("pg_stats", [])
        log.info("PG dump retrieved successfully. Found %d PGs", len(pg_stats))

        # Build quick lookup map: pgid -> scrub_schedule
        pg_stat_map = {
            pg_stat.get("pgid"): pg_stat.get("scrub_schedule") for pg_stat in pg_stats
        }

        scrub_schedule_details = {}

        def fetch_pg_schedule(pg_id):
            """
            Worker function to fetch scrub_schedule for a single PG
            """
            if pg_id in pg_stat_map:
                return pg_id, {"scrub_schedule": pg_stat_map[pg_id]}
            else:
                log.warning("PG %s not found in pg_dump output", pg_id)
                return pg_id, None

        # Use threads for PG processing
        max_workers = min(10, len(pg_list)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_pg_schedule, pg_id) for pg_id in pg_list]

            for future in as_completed(futures):
                pg_id, result = future.result()
                scrub_schedule_details[pg_id] = result

        extracted_count = len(
            [v for v in scrub_schedule_details.values() if v is not None]
        )
        log.info(
            "Successfully extracted scrub_schedule details for %d out of %d PGs",
            extracted_count,
            len(pg_list),
        )

        log.debug("Scrub schedule details: %s", scrub_schedule_details)
        return scrub_schedule_details

    except Exception as e:
        log.error("Failed to extract scrub_schedule details: %s", str(e))
        log.exception("Exception details:")
        raise


def check_scrub_queue_jobs(rados_object, osd_id, pg_id):
    """
    Check for scrub-queue jobs with pattern <pgid>/sh in the primary OSD log
    and extract init time and end time.

    Args:
        rados_object: RadosOrchestrator object
        osd_id: Primary OSD ID
        pg_id: Placement group ID

    Returns:
        dict: Dictionary containing scrub-queue job information with init_time and end_time
              Returns None if no scrub-queue jobs found
    """
    try:
        # Get the host node for the primary OSD
        osd_host = rados_object.fetch_host_node(
            daemon_type="osd", daemon_id=str(osd_id)
        )
        if not osd_host:
            log.error("Failed to fetch host node for OSD %s", osd_id)
            return None

        # Get cluster FSID
        fsid = rados_object.run_ceph_command(cmd="ceph fsid")["fsid"]
        log_path = "/var/log/ceph/%s/ceph-osd.%s.log" % (fsid, osd_id)

        # Search for scrub-queue jobs with pattern <pgid>/sh
        # Pattern: scrub-queue.*<pgid>/sh or similar patterns
        # Escape special characters in pg_id for regex
        pg_id_escaped = re.escape(str(pg_id))
        search_pattern = "scrub-queue.*%s/sh" % pg_id_escaped
        log.info("Searching for scrub-queue jobs with pattern: %s", search_pattern)

        # Use grep to find lines containing the pattern
        cmd = "grep -E '%s' %s 2>/dev/null || true" % (search_pattern, log_path)
        output, _ = osd_host.exec_command(cmd=cmd, sudo=True, check_ec=False)

        if not output or not output.strip():
            log.warning(
                "No scrub-queue jobs found for %s/sh in OSD %s log", pg_id, osd_id
            )
            return None

        # Parse the log lines to extract timestamps
        log_lines = output.strip().split("\n")
        log.info("Found %d log line(s) matching scrub-queue pattern", len(log_lines))

        # Extract timestamps from log lines
        # Log format typically: YYYY-MM-DD HH:MM:SS.mmm ... scrub-queue ...
        init_time = None
        end_time = None

        for line in log_lines:
            if not line.strip():
                continue

            log.debug("Processing log line: %s...", line[:100])

            # Extract timestamp from log line (first field is usually timestamp)
            # Format can vary: "2024-01-01 12:00:00.123" or similar
            timestamp_match = re.search(
                r"(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}[.\d]*[+\d]*Z?)", line
            )
            if timestamp_match:
                timestamp = timestamp_match.group(1)

                # Check if this is an init/start or end/complete message
                if "scrub-queue" in line.lower():
                    if init_time is None:
                        init_time = timestamp
                        log.info("Found init time: %s", init_time)
                    # Update end_time as we find more entries (last one is the end)
                    end_time = timestamp
                    log.debug("Updated end time: %s", end_time)

        result = {}
        if init_time:
            result["init_time"] = init_time
        if end_time:
            result["end_time"] = end_time
        if result:
            result["pg_id"] = pg_id
            result["osd_id"] = osd_id
            result["log_lines_found"] = len(log_lines)

        return result if result else None

    except Exception as e:
        log.error("Error checking scrub-queue jobs: %s", str(e))
        log.exception("Exception details:")
        return None


def log_scrub_scheduled_dict(unique_scrub_scheduled_dict):
    """
    Log the unique_scrub_scheduled_dict in a readable table format.

    Args:
        unique_scrub_scheduled_dict: Dictionary with structure {pg_id: [scrub_scheduled, ...], ...}
    """
    if not unique_scrub_scheduled_dict:
        log.info("unique_scrub_scheduled_dict is empty")
        return

    # Calculate column widths
    max_pg_id_len = (
        max(len(str(pg_id)) for pg_id in unique_scrub_scheduled_dict.keys())
        if unique_scrub_scheduled_dict
        else 0
    )
    max_pg_id_len = max(max_pg_id_len, len("PG_id"))

    # Create header
    header = "=" * (max_pg_id_len + 50)  # 50 for spacing and separators
    log.info(header)
    log.info("%-*s  %s", max_pg_id_len, "PG_id", "Messages")
    log.info(header)

    # Create rows - each message on a new line
    for pg_id, messages in unique_scrub_scheduled_dict.items():
        if messages:
            # First message on the same line as PG_id
            first_msg = str(messages[0])
            if not first_msg.endswith(";"):
                first_msg += ";"
            log.info("%-*s  %s", max_pg_id_len, str(pg_id), first_msg)

            # Remaining messages on new lines with indentation
            for msg in messages[1:]:
                msg_str = str(msg)
                if not msg_str.endswith(";"):
                    msg_str += ";"
                # Indent with spaces equal to PG_id column width + spacing
                indent = " " * (max_pg_id_len + 2)
                log.info("%s%s", indent, msg_str)
        else:
            log.info("%-*s  ", max_pg_id_len, str(pg_id))

    log.info(header)


def verify_scrub_sequence_pattern(unique_scrub_scheduled_dict):
    """
    Verify that scrub operations follow the expected sequence pattern across PGs.

    This method validates that at least one PG (Placement Group) has a complete
    scrub sequence pattern in its scrub_scheduled messages. The expected pattern
    demonstrates the scrub queue timing mechanism working correctly.

    Expected Sequence Pattern:
    -------------------------
    The method looks for the following sequence in chronological order:
    1. Periodic scrub scheduled (initial scheduling event)
    2. Queued for scrub/deep scrub (or scrubbing if queued is missing)
    3. Scrubbing/deep scrubbing (actual scrub execution)
    4. Periodic scrub scheduled (next scheduling event after scrub completion)

    Logic Used:
    -----------
    For each PG in the dictionary:
    1. Main Index Identification:
       - First, searches forward through messages to find "queued for scrub" or
         "queued for deep scrub" pattern. If found, uses this index as main_index.
       - If queued pattern is not found, searches for "scrubbing for" or
         "deep scrubbing for" pattern and uses that as main_index instead.
       - If neither pattern is found, skips this PG.

    2. Backward Search for Periodic Pattern:
       - Searches backwards from (main_index - 1) to index 0 to find a
         "periodic scrub scheduled" or "periodic deep scrub scheduled" pattern.
       - This represents the initial scheduling event that triggered the scrub.
       - If not found, skips this PG (pattern incomplete).

    3. Forward Search for Scrubbing Pattern:
       - If main_index was based on "queued" pattern (not scrubbing):
         * Searches forward from (main_index + 1) to find "scrubbing for" or
           "deep scrubbing for" pattern.
         * If not found, skips this PG.
       - If main_index was already based on "scrubbing" pattern:
         * Uses main_index as scrubbing_index directly.

    4. Forward Search for Next Periodic Pattern:
       - Searches forward from (scrubbing_index + 1) to find another
         "periodic scrub scheduled" or "periodic deep scrub scheduled" pattern.
       - This represents the next scheduling event after scrub completion.
       - If found, marks is_pattern_found = True and breaks (one complete
         pattern is sufficient).

    The method returns True if at least one PG has the complete sequence pattern,
    indicating that the scrub queue timing mechanism is working correctly.

    Args:
        unique_scrub_scheduled_dict (dict): Dictionary with structure
            {pg_id: [scrub_scheduled_message1, scrub_scheduled_message2, ...], ...}
            where each scrub_scheduled_message is a string containing scrub-related
            log messages for that PG. Messages are in chronological order.

    Returns:
        bool: True if at least one PG has the complete scrub sequence pattern,
              False otherwise (including when dictionary is empty).

    Example:
        A valid sequence for a PG might be:
        [
            "periodic scrub scheduled @ 2024-01-01T10:00:00",
            "queued for scrub",
            "scrubbing for pg 1.2",
            "periodic scrub scheduled @ 2024-01-01T10:05:00"
        ]

        or
        A valid sequence for a PG might be:
        [
            "periodic scrub scheduled @ 2024-01-01T10:00:00",
            "scrubbing for pg 1.2",
            "periodic scrub scheduled @ 2024-01-01T10:05:00"
        ]
        This would match the expected pattern.
    """
    log.info("Starting verification of scrub sequence pattern")
    if not unique_scrub_scheduled_dict:
        log.warning("unique_scrub_scheduled_dict is empty - cannot verify pattern")
        return False

    log.info(
        "Verifying scrub sequence pattern across %d PGs",
        len(unique_scrub_scheduled_dict),
    )
    search_terms = ("queued for scrub", "queued for deep scrub")
    periodic_patterns = (
        "periodic deep scrub scheduled",
        "periodic scrub scheduled",
    )
    scrubbing_terms = ("scrubbing for", "deep scrubbing for")
    is_pattern_found = False
    pg_checked_count = 0

    for key, values in unique_scrub_scheduled_dict.items():
        pg_checked_count += 1
        log.debug(
            "Checking PG %s (%d/%d): %d messages",
            key,
            pg_checked_count,
            len(unique_scrub_scheduled_dict),
            len(values),
        )
        main_index = None
        used_scrubbing_as_main = False

        # Step 1: Find queued pattern
        log.debug(
            "PG %s: Searching for queued pattern in %d messages", key, len(values)
        )
        for idx, message in enumerate(values):
            if any(term in message for term in search_terms):
                main_index = idx
                log.info(
                    "PG %s: Queued pattern found at index %d: %s", key, idx, message
                )
                break

        # If queued not found → use scrubbing index as main
        if main_index is None:
            log.debug(
                "PG %s: No queued pattern found, searching for scrubbing pattern", key
            )
            for idx, message in enumerate(values):
                if any(term in message for term in scrubbing_terms):
                    main_index = idx
                    used_scrubbing_as_main = True
                    log.info(
                        "PG %s: Scrubbing pattern used as main_index at index %d: %s",
                        key,
                        idx,
                        message,
                    )
                    break

        if main_index is None:
            log.debug(
                "PG %s: No queued or scrubbing pattern found, skipping this PG", key
            )
            continue

        # Step 2: Search backwards from main_index - 1 to 0 for periodic pattern
        log.debug(
            "PG %s: Searching backwards from index %d for periodic pattern",
            key,
            main_index - 1,
        )
        periodic_index = None
        for back_idx in range(main_index - 1, -1, -1):
            if any(pat in values[back_idx] for pat in periodic_patterns):
                periodic_index = back_idx
                log.debug(
                    "PG %s: Found periodic pattern at index %d: %s",
                    key,
                    back_idx,
                    values[back_idx],
                )
                break

        if periodic_index is None:
            log.debug(
                "PG %s: No periodic pattern found before main_index, skipping this PG",
                key,
            )
            continue

        # Step 3: Forward check: scrubbing / deep scrubbing
        if not used_scrubbing_as_main:
            log.debug(
                "PG %s: Searching forward from index %d for scrubbing pattern",
                key,
                main_index + 1,
            )
            scrubbing_index = None
            for fwd_idx in range(main_index + 1, len(values)):
                if any(term in values[fwd_idx] for term in scrubbing_terms):
                    scrubbing_index = fwd_idx
                    log.debug(
                        "PG %s: Found scrubbing pattern at index %d: %s",
                        key,
                        fwd_idx,
                        values[fwd_idx],
                    )
                    break

            if scrubbing_index is None:
                log.debug(
                    "PG %s: No scrubbing pattern found after queued pattern, skipping this PG",
                    key,
                )
                continue
        else:
            scrubbing_index = main_index
            log.debug("PG %s: Using main_index %d as scrubbing_index", key, main_index)

        # Step 4: Search forward for next periodic pattern
        log.debug(
            "PG %s: Searching forward from index %d for next periodic pattern",
            key,
            scrubbing_index + 1,
        )
        periodic_after_index = None
        for post_idx in range(scrubbing_index + 1, len(values)):
            if any(pat in values[post_idx] for pat in periodic_patterns):
                periodic_after_index = post_idx
                is_pattern_found = True
                log.info("PG %s: Found complete scrub sequence pattern!", key)
                log.info(
                    "PG %s: Pattern indices - periodic_before=%d, main=%d, scrubbing=%d, periodic_after=%d",
                    key,
                    periodic_index,
                    main_index,
                    scrubbing_index,
                    post_idx,
                )
                log.info("PG %s: Complete sequence verified successfully", key)
                break

        if periodic_after_index is None:
            log.debug(
                "PG %s: No periodic pattern found after scrubbing, pattern incomplete",
                key,
            )
            continue

        if is_pattern_found:
            break

    if not is_pattern_found:
        log.error(
            "Scrub sequence pattern verification failed for all %d PGs checked",
            pg_checked_count,
        )
        log.error(
            "Expected pattern: periodic scheduled -> queued/scrubbing -> scrubbing -> periodic scheduled"
        )
        return False

    log.info(
        "Scrub sequence pattern verification passed! Found complete pattern in PG %s",
        key,
    )
    return True


def get_last_scrub_ops_detail(rados_object, pg_list, operation):
    """
    Execute ceph pg query command for all PGs in parallel and store
    last_scrub / last_deep_scrub details.

    Args:
        rados_object: RadosOrchestrator object
        pg_list: List of PG IDs to query
        operation: Type of operation ("scrub" or "deep-scrub")

    Returns:
        dict: {pg_id: {'last_scrub': value} or {'last_deep_scrub': value}}
    """
    try:
        log.info(
            "Extracting last %s operation details for %d PGs (threaded)",
            operation,
            len(pg_list),
        )

        if operation == "scrub":
            last_ops_key = "last_scrub"
        else:
            last_ops_key = "last_deep_scrub"

        pg_details_dict = {}
        successful_count = 0
        failed_count = 0

        def query_pg(pg_id):
            """
            Worker function to query a single PG
            """
            try:
                cmd_pg_query = f"ceph pg {pg_id} query"
                log.debug("Executing command for PG %s: %s", pg_id, cmd_pg_query)

                pg_query_output = rados_object.run_ceph_command(
                    cmd=cmd_pg_query, client_exec=True
                )
                last_ops_value = pg_query_output["info"]["stats"][last_ops_key]

                return pg_id, {last_ops_key: last_ops_value}, None

            except Exception as e:
                return pg_id, None, str(e)

        # Limit threads to avoid overwhelming the cluster
        max_workers = min(5, len(pg_list)) or 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(query_pg, pg_id) for pg_id in pg_list]

            for future in as_completed(futures):
                pg_id, result, error = future.result()

                if error:
                    failed_count += 1
                    log.error(
                        "Failed to query PG %s: %s. Continuing with other PGs.",
                        pg_id,
                        error,
                    )
                    pg_details_dict[pg_id] = None
                else:
                    successful_count += 1
                    pg_details_dict[pg_id] = result
                    log.debug(
                        "PG %s: Extracted %s successfully (value: %s)",
                        pg_id,
                        last_ops_key,
                        result[last_ops_key],
                    )

        log.info(
            "Completed extraction: %d successful, %d failed out of %d total PGs",
            successful_count,
            failed_count,
            len(pg_list),
        )

        return pg_details_dict

    except Exception as e:
        log.error("Failed to extract PG details: %s", str(e))
        log.exception("Exception details:")
        raise


def verify_last_ops_changed(
    before_test_last_ops, after_test_last_ops, operation="scrub"
):
    """
    Verify that last scrub operation details changed for at least half of the PGs.

    Args:
        before_test_last_ops (dict): {pg_id: {'last_scrub': value} or {'last_deep_scrub': value}}
        after_test_last_ops (dict): {pg_id: {'last_scrub': value} or {'last_deep_scrub': value}}
        operation: Type of operation ("scrub" or "deep-scrub") for logging purposes

    Returns:
        bool: True if at least half of PG values changed, False otherwise
    """
    log.info(
        "Verifying last %s operation changes for %d PGs",
        operation,
        len(before_test_last_ops),
    )
    unchanged_pgs = []
    changed_count = 0
    missing_count = 0
    total_valid_pgs = 0

    if operation == "scrub":
        last_ops_key = "last_scrub"
    else:
        last_ops_key = "last_deep_scrub"

    for pg_id, before_dict in before_test_last_ops.items():
        if before_dict is None:
            log.warning("PG %s has None value in before_test_last_ops, skipping", pg_id)
            unchanged_pgs.append(pg_id)
            continue

        before_val = before_dict.get(last_ops_key)
        after_dict = after_test_last_ops.get(pg_id)

        # If PG missing in after data, treat as failure
        if after_dict is None:
            log.error("PG %s missing in after_test_last_ops", pg_id)
            unchanged_pgs.append(pg_id)
            missing_count += 1
            continue

        after_val = after_dict.get(last_ops_key)

        if before_val is None or after_val is None:
            log.warning(
                "PG %s has None value (before=%s, after=%s), skipping",
                pg_id,
                before_val,
                after_val,
            )
            unchanged_pgs.append(pg_id)
            continue

        total_valid_pgs += 1

        if before_val == after_val:
            log.warning(
                "PG %s value did NOT change: %s (operation: %s)",
                pg_id,
                before_val,
                operation,
            )
            unchanged_pgs.append(pg_id)
        else:
            changed_count += 1
            log.debug(
                "PG %s value changed: %s → %s (operation: %s)",
                pg_id,
                before_val,
                after_val,
                operation,
            )

    # Check if at least half of the pg_id values changed
    if total_valid_pgs == 0:
        log.error("No valid PGs found to check (operation: %s)", operation)
        return False

    half_threshold = total_valid_pgs / 2
    if changed_count >= half_threshold:
        log.info(
            "At least half of PGs changed: %d/%d changed (operation: %s)",
            changed_count,
            total_valid_pgs,
            operation,
        )
        if unchanged_pgs:
            log.warning(
                "Some PGs did not change: %d PGs (operation: %s)",
                len(unchanged_pgs),
                operation,
            )
            log.warning("PGs with unchanged values: %s", unchanged_pgs)
        return True
    else:
        log.error(
            "Less than half of PGs changed: %d/%d changed (operation: %s)",
            changed_count,
            total_valid_pgs,
            operation,
        )
        log.error(
            "Unchanged PGs detected: %d PGs did not update (operation: %s)",
            len(unchanged_pgs),
            operation,
        )
        log.error("PGs with unchanged values: %s", unchanged_pgs)
        if missing_count > 0:
            log.error("%d PGs were missing in after_test_last_ops", missing_count)
        return False
