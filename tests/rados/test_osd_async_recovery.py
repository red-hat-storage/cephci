"""
This file contains the  methods to verify the async messages in the OSD logs.
"""
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574487 - Verification of Async recovery performance with fault injection.
    1.Create pool and write data in to the pool
    2.set the osd_async_recovery_min_cost to 10  and debug_osd values to 20
    3.Start writing the data into pool and start scrub parallely
    4.Once the scrub started check the OSD logs contain the-
       - choose_async_recovery_replicated candidates by cost are
       - choose_async_recovery_replicated
    5. Once the tests are complete delete the pool and remove the set values of the parameter
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    ceph_nodes = kw.get("ceph_nodes")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    try:
        target_configs = config["async_recovery"]["configurations"]
        mon_obj.set_config(
            section="osd", name="osd_async_recovery_min_cost", value="10"
        )
        mon_obj.set_config(section="osd", name="debug_osd", value="20/20")

        # Creating pools and starting the test
        for entry in target_configs.values():
            log.debug(
                f"Creating {entry['pool_type']} pool on the cluster with name {entry['pool_name']}"
            )
            method_should_succeed(rados_object.create_pool, **entry)
            pool_name = entry["pool_name"]
            rados_object.bench_write(
                pool_name=pool_name, rados_write_duration=900, background=True
            )
            log_lines = [
                "choose_async_recovery_replicated candidates by cost are",
                "choose_async_recovery_replicated",
            ]
            osd_list = []
            for node in ceph_nodes:
                if node.role == "osd":
                    node_osds = rados_object.collect_osd_daemon_ids(node)
                    osd_list = osd_list + node_osds
            log.info(f"The number of OSDs in the cluster are-{len(osd_list)}")

            log_osd_count = 0
            found_osd_list = []
            for osd_id in osd_list:
                log.info(f"Performing the tests on the-{osd_id}")
                init_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
                )
                if not rados_object.change_osd_state(action="restart", target=osd_id):
                    log.error(f" Failed to restart OSD : {osd_id}")
                    return 1
                method_should_succeed(
                    wait_for_clean_pg_sets, rados_object, test_pool=pool_name
                )
                end_time, _ = installer.exec_command(
                    cmd="sudo date '+%Y-%m-%d %H:%M:%S'"
                )
                if verify_async_recovery_log(
                    rados_obj=rados_object,
                    osd=osd_id,
                    start_time=init_time,
                    end_time=end_time,
                    lines=log_lines,
                ):
                    log.info(f"found the log lines in the osd id -{osd_id}")
                    log_osd_count = log_osd_count + 1
                    found_osd_list.append(osd_id)
                if log_osd_count == 2:
                    log.info(f"The messages found on the {log_osd_count} osds")
                    log.info(f"The log messages found at -{found_osd_list} OSD logs")
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
            method_should_succeed(rados_object.detete_pool, entry["pool_name"])
            log.info("deleted the pool successfully")
        mon_obj.remove_config(section="osd", name="osd_async_recovery_min_cost")
        mon_obj.remove_config(section="osd", name="debug_osd")
        # intentional wait for 5 seconds
        time.sleep(5)
    return 0


def verify_async_recovery_log(
    rados_obj: RadosOrchestrator, osd, start_time, end_time, lines
) -> bool:
    """
    Retrieve the async recovery log using journalctl command
    Args:
        rados_obj: Rados object
        osd: osd id
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        lines: Log lines to search in the journalctl logs
    Returns:  True-> if the lines are exist in the journalctl logs
              False -> if the lines are not exist in the journalctl logs
    """
    log.info("Checking for the async messages in the OSD logs")
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time,
        end_time=end_time,
        daemon_type="osd",
        daemon_id=osd,
    )
    log.debug(f"Journalctl logs : {log_lines}")
    for line in lines:
        if line not in log_lines:
            log.error(f" did not find logging on OSD : {osd}")
            log.error(f"Journalctl logs lines: {log_lines}")
            log.error(f"expected logs lines: {lines}")
            return False
    log.info(f"Found the log lines on OSD : {osd}")
    return True
