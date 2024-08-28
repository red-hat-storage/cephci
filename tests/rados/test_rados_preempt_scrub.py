"""
This file contains the  methods to verify the preempt messages in the OSD logs.
"""

import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83572916 - Verify that the preempted messages are generated at OSD logs during scrubbing
    1.Create pool and write data in to the pool
    2.set the osd_shallow_scrub_chunk_max and osd_scrub_chunk_max values to 250
    3.Start writing the data into pool and start scrub parallely
    4.Once the scrub started check the OSD logs contain the-
       - head preempted
       - WaitReplicas::react(const GotReplicas&) PREEMPTED
    5. Once the tests are complete delete the pool and remove the set values of the parameter
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    ceph_nodes = kw.get("ceph_nodes")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    pool_target_configs = config["verify_osd_preempt"]["configurations"]

    try:
        scrub_status_flag = False
        # Before starting the tests check that scrubbing is not progress in the cluster
        time_end = time.time() + 60 * 15
        while time.time() < time_end:
            scrub_status = check_scrub_status(rados_object)
            if scrub_status is False:
                scrub_status_flag = True
                log.info("Scrubbing is not in progress.Continuing the test execution")
                break
            log.info("Waiting for the scrub to complete")
            time.sleep(30)

        if scrub_status_flag is False:
            log.error(
                "Scrubbing is in progress cannot continue the further execution of test"
            )
            return 1

        for entry in pool_target_configs.values():
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            method_should_succeed(
                rados_object.create_pool,
                **entry,
            )
            log.info(f"Created the pool {entry['pool_name']}")
            rados_object.bench_write(
                pool_name=entry["pool_name"], rados_write_duration=30
            )

            mon_obj.set_config(
                section="osd", name="osd_shallow_scrub_chunk_max", value="250"
            )
            mon_obj.set_config(section="osd", name="osd_scrub_chunk_max", value="250")
            mon_obj.set_config(section="osd", name="debug_osd", value="10/10")

            rados_object.bench_write(
                pool_name=entry["pool_name"], rados_write_duration=60, background=True
            )

            log_lines = [
                "head preempted",
                "WaitReplicas::react(const GotReplicas&) PREEMPTED",
            ]

            init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
            rados_object.run_scrub(pool=entry["pool_name"])
            scrub_status_flag = False

            # Wait for the scrub to start for 15 minutes
            time_end = time.time() + 60 * 15
            while time.time() < time_end:
                scrub_status = check_scrub_status(rados_object)
                if scrub_status is True:
                    scrub_status_flag = True
                    log.info("Scrubbing is in progress")
                    break
                log.info("Waiting for the scrub to start")
                time.sleep(30)

            # check for the scrub is initiated or it is timeout
            if scrub_status_flag is False:
                log.error("Scrub is not initiated")
                return 1
            time.sleep(10)
            end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
            osd_list = []
            for node in ceph_nodes:
                if node.role == "osd":
                    node_osds = rados_object.collect_osd_daemon_ids(node)
                    osd_list = osd_list + node_osds
            log.info(f"The number of OSDs in the cluster are-{len(osd_list)}")

            log_osd_count = 0
            for osd_id in osd_list:
                if verify_preempt_log(
                    rados_obj=rados_object,
                    osd=osd_id,
                    start_time=init_time,
                    end_time=end_time,
                    lines=log_lines,
                ):
                    log.info(f"The preempted lines found at {osd_id}")
                    log_osd_count = log_osd_count + 1
                if log_osd_count == 2:
                    break
            if log_osd_count == 0:
                log.error("Log lines not found in any of the OSDs")
                return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Execution of finally block")
        if config.get("delete_pool"):
            method_should_succeed(rados_object.delete_pool, entry["pool_name"])
            log.info("deleted the pool successfully")
        mon_obj.remove_config(section="osd", name="osd_shallow_scrub_chunk_max")
        mon_obj.remove_config(section="osd", name="osd_scrub_chunk_max")
        mon_obj.remove_config(section="osd", name="debug_osd")
        # intentional wait for 5 seconds
        time.sleep(5)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    return 0


def verify_preempt_log(
    rados_obj: RadosOrchestrator, osd, start_time, end_time, lines
) -> bool:
    """
    Retrieve the preempt log using journalctl command
    Args:
        rados_obj: Rados object
        osd: osd id
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        lines: Log lines to search in the journalctl logs
    Returns:  True-> if the lines are exist in the journalctl logs
              False -> if the lines are not exist in the journalctl logs
    """

    log.info("Checking for the preempt messages in the OSD logs")
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time,
        end_time=end_time,
        daemon_type="osd",
        daemon_id=osd,
    )
    log.debug(f"Journalctl logs are : {log_lines}")
    for line in lines:
        if line not in log_lines:
            log.error(f" Did not find logging on OSD : {osd}")
            log.error(f"Journalctl logs lines: {log_lines}")
            log.error(f"Expected logs lines are: {lines}")
            return False
    log.info(f"Found the log lines on OSD : {osd}")
    return True


def check_scrub_status(osd_object):
    """
    Retrieve the preempt log using journalctl command
    Args:
        osd_obj: Rados object

    Returns:  True-> if the scrubbing is in progress
              False -> if the scrubbing is not in progress
    """
    status_report = osd_object.run_ceph_command(cmd="ceph report")
    for entry in status_report["num_pg_by_state"]:
        if "scrubbing" in entry["state"]:
            log.info("Scrubbing is in progress")
            return True
        log.info("Scrubbing is not in-progress")
    return False
