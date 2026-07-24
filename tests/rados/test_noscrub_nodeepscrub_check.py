"""
The file contains the script that verify the customer scenario of scrub and deep scrub functionality.
Previously, in some cases, using the `noscrub` or `nodeep-scrub` flags resulted in incorrect handling of these flags.
As a result, placement groups (PGs) were not scrubbed until the primary OSD was restarted.
In addition, until the OSD was restarted, the number of concurrent scrubs that could be performed by any of the
OSDs that are storing the PG was reduced. These OSD limitations were on both the primary OSDs and OSDs
serving as replicas.  If the maximum configuration was 1, which was the default, the affected OSDs would not be able
to participate in any new scrub actions, for any PG. With this fix, the 'noscrub' flag now works as expected.
CEPH-83594004 - Verification of Scrub and Deep-Scrub Operations on Placement Groups (PGs)
"""

import datetime
import time
import traceback

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
    CEPH-83594004
    Bug id - https://bugzilla.redhat.com/show_bug.cgi?id=2241025
    1. Set the nodeep-scrub and noscrub flags
    2. Check that the cluster PGs are in active+clean state.
    3. Create a pool and push the data
    4. Modify the osd_scrub_min_interval to 60 and  osd_deep_scrub_interval to 420
    5. Check that the scrub or deep-scrub is in progress or not
    6. Unset the deep-scrub flag
    7. Check that the deep-scrub is in progress or not
    8. Set the osd_deep_scrub_interval and  osd_scrub_min_interval to the default values
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_object = MonConfigMethods(rados_obj=rados_object)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    scrub_obj = RadosScrubber(node=cephadm)

    if not rados_object.enable_file_logging():
        log.error("Error while setting config to enable logging into file")
        return 1
    # Start with a cluster that has nodeep-scrub and noscrub flags set
    scrub_obj.set_osd_flags("set", "noscrub")
    scrub_obj.set_osd_flags("set", "nodeep-scrub")
    if not wait_for_clean_pg_sets(rados_object, timeout=300):
        log.error("Cluster is not in active+clean state")
        raise Exception(
            "Cluster is not in actve+clean state.Not executing the further tests"
        )
    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'")
    init_time = init_time.strip()
    # Created pool and pushed the data.
    pool_name = config["pool_name"]
    start_time = get_cluster_timestamp(rados_object.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:

        method_should_succeed(rados_object.create_pool, **config)
        msg_pool_name = f"The {pool_name} pool is created"
        log.info(msg_pool_name)
        rados_object.bench_write(pool_name=pool_name, max_objs=5000)
        msg_data_create = f"The data is pushed into the {pool_name} pool"
        log.info(msg_data_create)
        pg_id_list = rados_object.get_pgid(pool_name=pool_name)
        msg_pg_id_list = f"The pg list of the pool {pool_name} is-- {pg_id_list}"
        log.info(msg_pg_id_list)

        # Modified the scrub parameters
        mon_object.set_config(section="osd", name="osd_scrub_min_interval", value=60)
        mon_object.set_config(section="osd", name="osd_deep_scrub_interval", value=60)
        mon_object.set_config(section="osd", name="debug_osd", value="20/20")

        # Check for log
        exec_end_time = datetime.datetime.now() + datetime.timedelta(minutes=10)

        for pg_id in pg_id_list:
            if datetime.datetime.now() >= exec_end_time:
                break
            end_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            end_time = end_time.strip()
            log_lines = [
                rf"'{pg_id} scrub starts'",
                rf"'{pg_id} deep-scrub starts'",
            ]
            if verify_scrub_log(rados_object, init_time, end_time, pg_id, log_lines):
                msg_err_pgid = f"The scrub started on the {pg_id} after setting the no_scrub and no_deepscrub flag"
                log.error(msg_err_pgid)
                return 1
            time.sleep(30)
        log.info(
            "===Verification of the scrub operation by setting the no-scrub and nodeep-scrub completed==="
        )

        log.info(
            "===Verify that the deep-scrub starts after unsetting the nodeep-scrub flag==="
        )
        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        log.info("The nodeep-scrub flag is unset")
        time.sleep(20)
        deep_scrub_falg = False

        exec_end_time = datetime.datetime.now() + datetime.timedelta(minutes=10)
        for pg_id in pg_id_list:
            if datetime.datetime.now() >= exec_end_time:
                break
            end_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            end_time = end_time.strip()
            log_lines = [
                rf"'{pg_id} deep-scrub starts'",
                rf"'{pg_id} deep-scrub ok'",
            ]
            if verify_scrub_log(rados_object, init_time, end_time, pg_id, log_lines):
                msg_scrub_start = f"The deep-scrub started on the {pg_id} after unsetting no_deepscrub flag"
                log.info(msg_scrub_start)
                deep_scrub_falg = True
            time.sleep(30)

        if not deep_scrub_falg:
            log.error(
                "The deep-scrub not started after unsetting the nodeep-scrub flag"
            )
            return 1
        log.info(
            "===Verification of the deep-scrub starts after unsetting the nodeep-scrub flag completed==="
        )
        log.info(
            "===Verification of pg scheduled time after removing the scrub intervals==="
        )
        before_test_scrub_schedule_time = {}
        # unset the parameters
        mon_object.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_object.remove_config(section="osd", name="osd_deep_scrub_interval")
        log.info("Wait for the cluster into active+clean state")
        method_should_succeed(wait_for_clean_pg_sets, rados_object, timeout=3600)

        # Get the scrub time before removing the parameter values
        for pg_id in pg_id_list:
            acting_set = rados_object.get_pg_acting_set(pg_num=pg_id)
            pg_dump_scrub = scrub_obj.get_pg_dump_scrub(acting_set[0], pg_id)
            before_test_scrub_schedule_time[pg_id] = pg_dump_scrub["sched_time"]

        # Deep-scrub should not start immediately
        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()
        exec_end_time = datetime.datetime.now() + datetime.timedelta(minutes=10)
        # Define the format for parsing
        fmt = "%Y-%m-%dT%H:%M:%S.%f%z"
        for pg_id in pg_id_list:
            if datetime.datetime.now() >= exec_end_time:
                break
            end_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            end_time = end_time.strip()
            log_lines = [
                rf"'{pg_id} scrub starts'",
                rf"'{pg_id} deep-scrub starts'",
            ]
            pg_id_time = before_test_scrub_schedule_time[pg_id]
            scrub_log_result = verify_scrub_log(
                rados_object, init_time, end_time, pg_id, log_lines
            )
            pg_id_dt = datetime.datetime.strptime(pg_id_time, fmt)
            end_dt = datetime.datetime.strptime(end_time, fmt)
            if scrub_log_result and pg_id_dt >= end_dt:
                msg_error = (
                    f"The scrub started on the {pg_id} after setting the no_scrub and no_deepscrub flag."
                    f"The scheduled time- {pg_id_dt} is greater than current time-{end_dt}"
                )
                log.error(msg_error)
                return 1
            time.sleep(30)
        log.info(
            "===Verification of pg scheduled time after removing the scrub intervals completed==="
        )

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("============Execution of finally block==================")
        mon_object.remove_config(section="osd", name="debug_osd")
        method_should_succeed(rados_object.delete_pool, pool_name)
        scrub_obj.set_osd_flags("unset", "nodeep-scrub")
        scrub_obj.set_osd_flags("unset", "noscrub")
        mon_object.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_object.remove_config(section="osd", name="osd_deep_scrub_interval")
        time.sleep(10)
        # log cluster health
        rados_object.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_object.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_object.check_crash_status(
            start_time=start_time, end_time=test_end_time
        ):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def verify_scrub_log(rados_object, init_time, end_time, pgid, log_lines):
    """
    Method to check the scrub and deep-scrub logs exists or not
    Args:
        rados_object: Rados object
        init_time:  initial time
        end_time:  End time
        pgid: pg id
        log_lines: log lines to check in th logs

    Returns: True -> If log lines are present in the logs
             False ->  Log lines not exists in the logs
    """
    msg_log_lines = f"The log lies to check - {log_lines}"
    log.info(msg_log_lines)
    acting_set = rados_object.get_pg_acting_set(pg_num=pgid)
    msg_acting_set = f"The acting osd set of the pg {pgid} is {acting_set}"
    log.info(msg_acting_set)

    scrub_flag = False
    for osd_id in acting_set:
        for log_line in log_lines:
            chk_log_msg = rados_object.lookup_log_message(
                init_time=init_time,
                end_time=end_time,
                daemon_type="osd",
                daemon_id=osd_id,
                search_string=log_line,
            )
            if chk_log_msg:
                msg_found_logline = f" Found the log lines on OSD : {osd_id} and log line is {chk_log_msg}"
                log.info(msg_found_logline)
                scrub_flag = True
                break
    if scrub_flag:
        return True
    log.info("The line not appeared in the log files")
    return False
