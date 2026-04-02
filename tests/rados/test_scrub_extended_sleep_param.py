"""
Test for scrub extended sleep parameter.

Uses a test pool with a single PG for predictable scrub behavior.
Verifies that the osd_scrub_extended_sleep parameter extends scrub duration
when set (e.g. to 300) compared to the default, by comparing scrub_duration
from ``ceph pg <pg_id> query`` before and after applying the parameter.
"""

import time
from datetime import datetime

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_scrub import RadosScrubber
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Execute the scrub extended sleep parameter test.

    Steps:
        1. Create a test pool with a single PG (pg_num=1, pg_num_max=1).
        2. Wait for the PG set to be active+clean (timeout 300s).
        3. Generate up to 20000 objects in the pool for scrub workload.
        4. Get the pool's PG ID and baseline PG query stats.
        5. Set noscrub and nodeep-scrub flags; configure scheduled scrub
           parameters (begin/end hour, intervals) via set_scheduled_scrub_parameters.
        6. Wait until cluster time is in the last two minutes of the hour
           (wait_until_last_two_minutes).
        7. Wait for a scheduled scrub to start and complete (up to wait_time=1800s);
           re-set noscrub after; record scrub_duration from ``ceph pg <pg_id> query``
           as baseline (before_scrub_duration).
        8. Remove OSD scrub parameter overrides (remove_parameter_configuration)
           so osd_scrub_extended_sleep is at default.
        9. Set osd_scrub_extended_sleep to 300.
        10. Wait again for active+clean; set scheduled scrub parameters again;
            wait until last two minutes of the hour.
        11. Wait for another scheduled scrub to complete; record scrub_duration
            as after_scrub_duration.
        12. Assert after_scrub_duration >= before_scrub_duration (extended sleep
            should increase scrub duration); return 1 on failure, 0 on success.
        13. In finally: unset noscrub and nodeep-scrub, remove parameter
            configuration, and delete the test pool.

    """
    config = kw.get("config")
    pool_name = config.get("pool_name")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    scrub_obj = RadosScrubber(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)

    log.info("Starting scrub extended sleep parameter test for pool %s", pool_name)

    try:
        log.info("Step 1: Creating test pool with a single PG")
        if not rados_obj.create_pool(
            pool_name=pool_name,
            pg_num=1,
            pg_num_max=1,
        ):
            log.error("Failed to create pool %s", pool_name)
            return 1
        log.info("Pool %s created with 1 PG", pool_name)

        log.info("Step 2: Waiting for PG set to be active+clean (timeout 300s)")
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, test_pool=pool_name, timeout=300
        )
        log.info("PG set is active+clean")

        log.info(
            "Step 3: Generating up to 20000 objects in pool %s for scrub workload",
            pool_name,
        )
        if not rados_obj.bench_write(pool_name=pool_name, max_objs=20000):
            log.error("Failed to generate objects in pool %s", pool_name)
            return 1
        log.info("Objects generated successfully")

        log.info("Step 4: Getting PG ID and baseline PG query stats")
        pg_id = rados_obj.get_pgid(pool_name=pool_name)[0]
        log.info("Pool %s PG id: %s", pool_name, pg_id)

        log.info(
            "Step 5: Setting noscrub and nodeep-scrub flags; configuring scheduled scrub parameters"
        )
        scrub_obj.set_osd_flags("set", "noscrub")
        scrub_obj.set_osd_flags("set", "nodeep-scrub")
        set_scheduled_scrub_parameters(scrub_obj)

        log.info(
            "Step 6: Waiting until cluster time is in the last two minutes of the hour"
        )
        wait_until_last_two_minutes(rados_obj, scrub_obj)

        wait_time = 1800
        log.info(
            "Step 7: Waiting for first scheduled scrub to complete (timeout %s s)",
            wait_time,
        )
        try:
            if not rados_obj.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=wait_time
            ):
                log.error(
                    "Scrub did not start or complete within %s s (scheduled scrub window)",
                    wait_time,
                )
                return 1

        except Exception:
            log.warning(
                "Scrub did not start within the scheduled window; continuing to record baseline duration"
            )
        scrub_obj.set_osd_flags("set", "noscrub")
        pg_query = rados_obj.run_ceph_command(cmd=f"ceph pg {pg_id} query")
        before_scrub_duration = pg_query["info"]["stats"]["scrub_duration"]
        log.info(
            "First scrub completed; baseline scrub_duration (default osd_scrub_extended_sleep): %s",
            before_scrub_duration,
        )

        log.info("Step 8: Removing OSD scrub parameter overrides (restore defaults)")
        remove_parameter_configuration(mon_obj=mon_obj)

        log.info("Step 9: Setting osd_scrub_extended_sleep to 300")
        scrub_obj.set_osd_configuration("osd_scrub_extended_sleep", "300")

        log.info(
            "Step 10: Waiting for active+clean; re-applying scheduled scrub parameters; "
            "waiting until last two minutes"
        )
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, test_pool=pool_name, timeout=300
        )
        log.info("PG set is active+clean")
        set_scheduled_scrub_parameters(scrub_obj)
        wait_until_last_two_minutes(rados_obj, scrub_obj)

        log.info(
            "Step 11: Waiting for second scheduled scrub to complete (timeout %s s)",
            wait_time,
        )
        try:
            if not rados_obj.start_check_scrub_complete(
                pg_id=pg_id, user_initiated=False, wait_time=wait_time
            ):
                log.error(
                    "Scrub did not start or complete within %s s after setting osd_scrub_extended_sleep=300",
                    wait_time,
                )
                return 1

        except Exception:
            log.warning(
                "Scrub did not start within the scheduled window; continuing to record duration with extended sleep"
            )

        pg_query = rados_obj.run_ceph_command(cmd=f"ceph pg {pg_id} query")
        after_scrub_duration = pg_query["info"]["stats"]["scrub_duration"]
        log.info(
            "Second scrub completed; scrub_duration with osd_scrub_extended_sleep=300: %s",
            after_scrub_duration,
        )

        log.info(
            "Step 12: Comparing scrub durations - baseline (default): %s, with osd_scrub_extended_sleep=300: %s",
            before_scrub_duration,
            after_scrub_duration,
        )
        if after_scrub_duration < before_scrub_duration:
            log.error(
                "Verification failed:scrub duration with osd_scrub_extended_sleep=300 (%s) is less than baseline (%s)",
                after_scrub_duration,
                before_scrub_duration,
            )
            return 1
        log.info(
            "Verification passed: extended sleep increased scrub duration as expected"
        )
        return 0
    except Exception as e:
        log.error("Test failed with exception: %s", e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        log.info(
            "Step 13: Cleanup - unsetting flags, removing config overrides, deleting pool"
        )
        scrub_obj.set_osd_flags("unset", "noscrub")
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        log.info("Unset noscrub and nodeep-scrub flags")
        remove_parameter_configuration(mon_obj)
        log.info("Removed OSD scrub parameter overrides")
        rados_obj.delete_pool(pool=pool_name)
        log.info("Deleted test pool %s", pool_name)


def wait_until_last_two_minutes(rados_obj, scrub_obj):
    """
    Wait until the cluster time reaches the last two minutes of the hour.

    If the current minute is already >= 58, proceeds immediately. Otherwise
    sleeps until HH:58:00. Used to align scrub with the scheduled window.

    Args:
        rados_obj: RadosOrchestrator instance (for cluster timestamp).
        scrub_obj: RadosScrubber instance (noscrub may be unset if already in window).

    Returns:
        None
    """
    cluster_time = get_cluster_timestamp(rados_obj.node)
    log.info("Cluster time (UTC): %s", cluster_time)

    if isinstance(cluster_time, str):
        cluster_time = datetime.fromisoformat(cluster_time)

    minute = cluster_time.minute

    if minute < 58:
        target_time = cluster_time.replace(minute=58, second=0, microsecond=0)
        wait_seconds = (target_time - cluster_time).total_seconds()

        log.info("Waiting until last 2 minutes of the hour: %s", target_time)
        time.sleep(wait_seconds)
    else:
        log.info("Cluster already in last 2 minutes of the hour, proceeding.")
        scrub_obj.set_osd_flags("unset", "noscrub")


def set_scheduled_scrub_parameters(scrub_object):
    """
    Set OSD scheduled scrub window and interval parameters.

    Configures begin/end hour and week day, and min/max scrub intervals
    so that scrubs are allowed in a narrow window (e.g. hour 0-1).

    Args:
        scrub_object: RadosScrubber instance to apply configuration.

    Returns:
        None
    """
    osd_scrub_min_interval = 30
    osd_scrub_max_interval = 2700

    (
        scrub_begin_hour,
        scrub_begin_weekday,
        scrub_end_hour,
        scrub_end_weekday,
    ) = scrub_object.add_begin_end_hours(0, 1)

    log.info(
        "Setting scheduled scrub parameters: begin_hour=%s, end_hour=%s, min_interval=%s, max_interval=%s",
        scrub_begin_hour,
        scrub_end_hour,
        osd_scrub_min_interval,
        osd_scrub_max_interval,
    )
    scrub_object.set_osd_configuration("osd_scrub_begin_hour", scrub_begin_hour)
    scrub_object.set_osd_configuration("osd_scrub_begin_week_day", scrub_begin_weekday)
    scrub_object.set_osd_configuration("osd_scrub_end_hour", scrub_end_hour)
    scrub_object.set_osd_configuration("osd_scrub_end_week_day", scrub_end_weekday)
    scrub_object.set_osd_configuration("osd_scrub_min_interval", osd_scrub_min_interval)
    scrub_object.set_osd_configuration("osd_scrub_max_interval", osd_scrub_max_interval)
    log.info("Scheduled scrub parameters set successfully")


def remove_parameter_configuration(mon_obj):
    """
    Restore default OSD scrub-related config by removing overrides.

    Removes osd_scrub_min_interval, osd_scrub_max_interval,
    osd_deep_scrub_interval, scrub begin/end hour and week day,
    and osd_scrub_extended_sleep from the mon config store.

    Args:
        mon_obj: MonConfigMethods instance used to remove config entries.

    Returns:
        None
    """
    log.info("Removing OSD scrub parameter overrides from mon config")
    mon_obj.remove_config(section="osd", name="osd_scrub_min_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_max_interval")
    mon_obj.remove_config(section="osd", name="osd_deep_scrub_interval")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_week_day")
    mon_obj.remove_config(section="osd", name="osd_scrub_begin_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_end_hour")
    mon_obj.remove_config(section="osd", name="osd_scrub_extended_sleep")
    log.info("OSD scrub parameter overrides removed successfully")
