"""
This file contains the  methods to verify the preempt messages in the OSD logs.
"""

import re
import subprocess
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
    1.Create pool with 1 PG with the pg_autoscaler off
    2.Create  objects
    3. Set the following paramters-
        3.1  osd_scrub_sleep ->0
        3.2  osd_deep_scrub_keys ->1
        3.3  osd_scrub_interval_randomize_ratio ->0
        3.4  osd_deep_scrub_randomize_ratio -> 0
        3.5  osd_scrub_min_interval -> 10
        3.6  osd_scrub_max_interval -> 2000
        3.7  osd_deep_scrub_interval -> 600
        3.8  min_size -> 2
        3.9  debug_osd -> 20/20
    4. Rewrite the data into the four objects
    5. Search the "preempted" string in the active OSDs
    6. Once the tests are complete delete the pool and remove the set values of the parameter
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_object = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_object)
    installer = ceph_cluster.get_nodes(role="installer")[0]

    log_lines = ["preempted"]
    pool_name = config["pool_name"]
    try:
        # enable the file logging
        if not rados_object.enable_file_logging():
            log.error("Error while setting config to enable logging into file")
            return 1

        init_time, _ = installer.exec_command(
            cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
        )
        init_time = init_time.strip()
        rados_object.configure_pg_autoscaler(**{"default_mode": "off"})
        method_should_succeed(rados_object.create_pool, **config)

        if not rados_object.bench_write(
            pool_name=pool_name, rados_write_duration=20, max_objs=15, num_threads=1
        ):
            log.error(f"Failed to write objects into {pool_name}")
            return 1
        acting_set = rados_object.get_pg_acting_set(pool_name=pool_name)
        log.info(f"The PG acting set is -{acting_set}")
        set_preempt_parameter_value(mon_obj, rados_object, pool_name)

        object_list = rados_object.get_object_list(pool_name)
        rados_object.run_deep_scrub(pool=pool_name)
        time.sleep(5)
        for _ in range(30):
            tmp_date = subprocess.check_output("date", text=True).strip()
            for object in object_list:
                if re.search(r".*object.*", object):
                    cmd_rewrite = (
                        f"echo {tmp_date} | rados -p {pool_name} put {object} -"
                    )
                    rados_object.client.exec_command(cmd=cmd_rewrite, sudo=True)
        get_preempt_parameter_value(mon_obj, rados_object, pool_name)
        # Wait time for starting the scheduled scrubbing
        time.sleep(180)

        # Check for 30 minutes
        time_end = time.time() + 60 * 30
        found_flag = False
        while time.time() < time_end:
            time.sleep(10)
            end_time, _ = installer.exec_command(
                cmd="sudo date '+%Y-%m-%dT%H:%M:%S.%3N+0000'"
            )
            end_time = end_time.strip()
            for osd_id in acting_set:
                time.sleep(5)
                log.info(f"Checking the logs at: {osd_id}")
                if verify_preempt_log(
                    init_time=init_time,
                    end_time=end_time,
                    rados_object=rados_object,
                    osd_id=osd_id,
                    lines=log_lines,
                ):
                    log.info(f"The preempted lines found at {osd_id}")
                    found_flag = True
                if found_flag:
                    log.info(f"Logs messages are noticed at - {osd_id} osd")
                    break
            if found_flag:
                break
        if not found_flag:
            log.error("The preempted messages are not appeared")
            return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:

        log.info("Execution of finally block")
        if config.get("delete_pool"):
            method_should_succeed(rados_object.delete_pool, pool_name)
            log.info("deleted the pool successfully")
        unset_preempt_parameter_value(mon_obj, rados_object, pool_name)
        rados_object.disable_file_logging()
        # log cluster health
        rados_object.log_cluster_health()

    return 0


def verify_preempt_log(init_time, end_time, rados_object, osd_id, lines) -> bool:
    """
    Retrieve the preempt log using journalctl command
    Args:
        init_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        rados_object: Rados object
        osd_id : osd id
        lines: Log lines to search in the osd logs
    Returns:  True-> if the lines are exist in the osd logs
              False -> if the lines are not exist in the osd logs
    """

    log.info("Checking for the preempt messages in the OSD logs")
    fsid = rados_object.run_ceph_command(cmd="ceph fsid")["fsid"]
    host = rados_object.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
    cmd_get_log_lines = f'awk \'$1 >= "{init_time}" && $1 <= "{end_time}"\' /var/log/ceph/{fsid}/ceph-osd.{osd_id}.log'
    log_lines, err = host.exec_command(
        sudo=True,
        cmd=cmd_get_log_lines,
    )
    for line in lines:
        if line in log_lines:
            log.info(f" Found the log lines at OSD : {osd_id}")
            return True
    log.info(f"Did not found the  log lines on OSD : {osd_id}")
    return False


def set_preempt_parameter_value(mon_object, rados_object, pool_name):
    """
    Method is used to configure the  parameter values:
    mon_object : Monitor object
    rados_object : Rados Object
    pool_name: pool name
    Return -> none
    """
    try:
        mon_object.set_config(section="osd", name="osd_scrub_sleep", value=0)
        mon_object.set_config(section="osd", name="osd_deep_scrub_keys", value=1)
        mon_object.set_config(
            section="osd", name="osd_scrub_interval_randomize_ratio", value="0.0"
        )
        mon_object.set_config(
            section="osd", name="osd_deep_scrub_randomize_ratio", value="0.0"
        )
        mon_object.set_config(section="osd", name="osd_scrub_min_interval", value=10)
        mon_object.set_config(section="osd", name="osd_deep_scrub_interval", value=600)
        rados_object.set_pool_property(pool=pool_name, props="min_size", value=2)
        mon_object.set_config(section="osd", name="debug_osd", value="20/20")
        time.sleep(10)
    except Exception as error:
        log.error(f"Configuration of parameter is failed. Error : {error}")
        raise Exception("Error: Setting configuration failed")
    return None


def unset_preempt_parameter_value(mon_object, rados_object, pool_name):
    """
    Method is used to set the default  parameter values :
    mon_object : Monitor object
    rados_object : Rados Object
    pool_name: pool name
    Return -> None
    """
    try:
        mon_object.remove_config(section="osd", name="osd_scrub_sleep")
        mon_object.remove_config(section="osd", name="osd_deep_scrub_keys")
        mon_object.remove_config(
            section="osd", name="osd_scrub_interval_randomize_ratio"
        )
        mon_object.remove_config(section="osd", name="osd_deep_scrub_randomize_ratio")
        mon_object.remove_config(section="osd", name="osd_scrub_min_interval")
        mon_object.remove_config(section="osd", name="osd_scrub_max_interval")
        mon_object.remove_config(section="osd", name="osd_deep_scrub_interval")
        rados_object.set_pool_property(pool=pool_name, props="min_size", value=2)
        mon_object.remove_config(section="osd", name="debug_osd")
    except Exception as error:
        log.error(f"Configuration of parameter to default is failed. Error : {error}")
        raise Exception("Error: Unsetting configuration failed")
    return None


def get_preempt_parameter_value(mon_object, rados_object, pool_name):
    """
    Method is used to display the  parameter values :
    mon_object : Monitor object
    rados_object : Rados Object
    pool_name: pool name
    Return -> None
    """
    try:
        log.info("The scrub parameter values are:  ")
        out_put = mon_object.get_config(section="osd", param="osd_scrub_sleep")
        log.info(f"The osd_scrub_sleep parameter value is - {out_put}")
        out_put = mon_object.get_config(section="osd", param="osd_deep_scrub_keys")
        log.info(f"The osd_deep_scrub_keys parameter value is - {out_put}")
        out_put = mon_object.get_config(
            section="osd", param="osd_scrub_interval_randomize_ratio"
        )
        log.info(
            f"The osd_scrub_interval_randomize_ratio parameter value is - {out_put}"
        )
        out_put = mon_object.get_config(
            section="osd", param="osd_deep_scrub_randomize_ratio"
        )
        log.info(f"The osd_deep_scrub_randomize_ratio parameter value is - {out_put}")
        out_put = mon_object.get_config(section="osd", param="osd_scrub_min_interval")
        log.info(f"The osd_scrub_min_interval parameter value is - {out_put}")
        out_put = mon_object.get_config(section="osd", param="osd_scrub_max_interval")
        log.info(f"The osd_scrub_max_interval parameter value is - {out_put}")
        out_put = mon_object.get_config(section="osd", param="osd_deep_scrub_interval")
        log.info(f"The osd_deep_scrub_interval parameter value is - {out_put}")
        out_put = mon_object.get_config(section="osd", param="osd_deep_scrub_interval")
        log.info(f"The osd_deep_scrub_interval parameter value is - {out_put}")
        out_put = rados_object.get_pool_property(pool=pool_name, props="min_size")
        log.info(f"The min_size parameter value is - {out_put}")
        out_put = rados_object.get_pool_property(pool=pool_name, props="size")
        log.info(f"The size parameter value is - {out_put}")
    except Exception as error:
        log.error(f"Display the parameter values is failed. Error : {error}")
        raise Exception("Error: Display configuration failed")
    return None
