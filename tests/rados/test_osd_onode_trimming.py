"""
Method to verify the onode trimming
Bugzilla:
  https://bugzilla.redhat.com/show_bug.cgi?id=1947215
"""

import datetime
import re
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    ceph_nodes = kw.get("ceph_nodes")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    pool_name = "test-osdonode"
    log.info("Running test case to verify trimming of onodes")

    osd_list = []
    onode_values = {}

    try:
        regex = r"\s*(\d.\d)-rhel-\d"
        build = (re.search(regex, config.get("build", config.get("rhbuild")))).groups()[
            0
        ]

        if float(build) >= 6.0:
            onode_key = "onodes_pinned"
        else:
            onode_key = "bluestore_pinned_onodes"

        log_line = [
            "trim maximum skip pinned reached",
        ]
        bluestore_cache_trim_value = mon_obj.get_config(
            section="osd", param="bluestore_cache_trim_max_skip_pinned"
        )
        log.info(
            f"The bluestore_cache_trim_value value is -{bluestore_cache_trim_value}"
        )
        if int(bluestore_cache_trim_value) != 1000:
            log.error(
                f"The default value of bluestore_cache_trim_max_skip_pinned is {bluestore_cache_trim_value} "
                f"and not equal to the 1000 which is default"
            )
            return 1

        mon_obj.set_config(
            section="osd", name="bluestore_cache_trim_max_skip_pinned", value="20"
        )

        for node in ceph_nodes:
            if node.role == "osd":
                node_osds = rados_object.collect_osd_daemon_ids(node)
                osd_list = osd_list + node_osds
        # create pool with given config
        log.info("Creating a replicated pool with default config")
        assert rados_object.create_pool(pool_name=pool_name)

        for id in osd_list:
            if not rados_object.change_osd_state(action="restart", target=id):
                log.error(f"Unable to restart the OSD : {id}")
                return 1
        mon_obj.set_config(section="osd", name="debug_bluestore", value="20/20")
        init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        bench_cfg = {
            "pool_name": pool_name,
            "byte_size": "1KB",
            "rados_write_duration": 300,
        }
        rados_object.bench_write(**bench_cfg)
        rados_object.bench_read(**bench_cfg)

        time_end = time.time() + 30 * 10
        while time.time() < time_end:
            for id in osd_list:
                try:
                    perf_dump_output = rados_object.get_osd_perf_dump(id)
                    onode_pinned_obj = perf_dump_output["bluestore"][onode_key]
                except Exception:
                    log.info("Fetching OSD perf dump failed, retrying after 15 secs")
                    time.sleep(15)
                    continue
                if int(onode_pinned_obj) != 0:
                    log.info(
                        f"The inodes pinned in the osd {id} are {onode_pinned_obj} "
                    )
                    onode_values[id] = onode_pinned_obj
            break
        log.info(f"The total inode list is-{onode_values}")
        for osd_id, old_inodes in onode_values.items():
            log.info(f"starting scrub on the osd-{osd_id}")
            onode_flag = False
            time_end = time.time() + 60 * 10
            while time.time() < time_end:
                try:
                    rados_object.run_scrub(osd=osd_id)
                    perf_dump_output = rados_object.get_osd_perf_dump(id)
                    current_onode_value = perf_dump_output["bluestore"][onode_key]
                except Exception:
                    log.info("Fetching OSD perf dump failed, retrying after 15 secs")
                    time.sleep(15)
                    continue
                if current_onode_value < old_inodes:
                    log.info(
                        f"The inodes are reduced from the {old_inodes} to {current_onode_value}"
                    )
                    onode_flag = True
                    break
                log.info(
                    f"onode values are not decreased.After scrubbing the inode values are - {current_onode_value}"
                )
                log.info(" Checking the inode values after 10 seconds")
                time.sleep(10)
            if onode_flag is False:
                log.error(f"onodes are not reduced after scrub on OSD-{osd_id}")
                return 1
        # Checking the memory growth in the OSDs for 10 minutes
        time_execution = datetime.datetime.now() + datetime.timedelta(minutes=10)
        while datetime.datetime.now() < time_execution:
            # Get the memory usage of the OSDs
            for node in ceph_nodes:
                if node.role == "osd":
                    node_osds = rados_object.collect_osd_daemon_ids(node)
                    for osd_id in node_osds:
                        # Get the OSD memory
                        osd_memory_usage = rados_object.get_osd_memory_usage(
                            node, osd_id
                        )
                        time.sleep(3)
                        if osd_memory_usage > 70:
                            log.error(
                                f"The memory usage on the {node.hostname} -{osd_id} is {osd_memory_usage} "
                            )
                            return 1
                        log.info(
                            f"The memory usage on the {node.hostname} -{osd_id} is {osd_memory_usage} "
                        )
        # check the OSD logs
        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        for osd_id in osd_list:
            if not verify_osd_log(
                rados_obj=rados_object,
                osd=osd_id,
                start_time=init_time,
                end_time=end_time,
                lines=log_line,
            ):
                log.error(f"found the log lines in the osd id -{osd_id}")
                return 1
    finally:
        log.info("Execution of finally block")
        method_should_succeed(rados_object.delete_pool, pool_name)
        log.info("deleted the pool successfully")
        mon_obj.remove_config(section="osd", name="debug_bluestore")
        mon_obj.remove_config(
            section="osd", name="bluestore_cache_trim_max_skip_pinned"
        )
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        if rados_object.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def verify_osd_log(
    rados_obj: RadosOrchestrator, osd, start_time, end_time, lines
) -> bool:
    """
    Checking the trim maximum  log using journalctl command
    Args:
        rados_obj: Rados object
        osd: osd id
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        lines: Log lines to search in the journalctl logs
    Returns:  True-> if the lines are exist in the journalctl logs
              False -> if the lines are not exist in the journalctl logs
    """
    log.info("Checking for the trim maximum  message in the OSD logs")
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time,
        end_time=end_time,
        daemon_type="osd",
        daemon_id=osd,
    )
    log.debug(f"Journalctl logs : {log_lines}")
    for line in lines:
        if line in log_lines:
            log.error(f" The {line} found on the OSD : {osd}")
            log.error(f"Journalctl logs lines: {log_lines}")
            return False
    log.info(f"Not found the log lines on OSD : {osd}")
    return True
