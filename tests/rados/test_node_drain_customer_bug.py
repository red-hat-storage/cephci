"""
The file contain the method to check the customer issue-
 CEPH-83593996 - Check that the Ceph cluster logs are being generated appropriately according to the log level
"""

import random
import re
import time
from threading import Thread

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83593996
    Bug id - https://bugzilla.redhat.com/show_bug.cgi?id=2305677
    1. Configure a cluster that have more than four OSD nodes
    2. Select an OSD node and drain the node
    3. Parallely execute the ceph mgr fail command
    4. Check that exception occurs on the cluster
    5. Perform the following workaround  steps-
          5.1 ceph config-key rm mgr/cephadm/osd_remove_queue
          5.2 ceph mgr fail
    6. If exception not occured then check for the Traceback logs
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    ceph_nodes = kw.get("ceph_nodes")

    mgr_daemon = Thread(
        target=background_mgr_task, kwargs={"mgr_object": mgr_obj}, daemon=True
    )
    osd_list = []

    for node in ceph_nodes:
        cmd_host_chk = f"ceph orch host ls --host_pattern {node.hostname}"
        out = rados_obj.run_ceph_command(cmd=cmd_host_chk)
        if not out:
            log.info(f"The {node.hostname} is not in the cluster")
            continue
        if node.role == "osd":
            node_osds = rados_obj.collect_osd_daemon_ids(node)
            osd_list = osd_list + node_osds
    osd_weight_chk = check_set_reweight(rados_obj, osd_list)
    if not osd_weight_chk:
        log.error(
            "The osd weights are zero for ew nodes and weights of the OSD are not unique.Set the weights "
            "manually and re-run the tests"
        )
        return 1

    log_lines = [
        "mgr load Traceback",
        "TypeError: __init__() got an unexpected keyword argument 'original_weight'",
    ]
    ceph_version = rados_obj.run_ceph_command(cmd="ceph version")
    log.info(f"Current version on the cluster : {ceph_version}")
    match_str = re.match(
        r"ceph version (\d+)\.(\d+)\.(\d+)-(\d+)", ceph_version["version"]
    )
    major, minor, patch, build = match_str.groups()
    bug_exists = False
    exceptional_flag = False
    if int(major) < 18:
        bug_exists = True
    elif int(major) == 18 and int(minor) < 2:
        bug_exists = True
    elif int(major) == 18 and int(minor) == 2 and int(patch) < 1:
        bug_exists = True
    elif int(major) == 18 and int(minor) == 2 and int(patch) == 1 and int(build) <= 194:
        bug_exists = True

    osd_hosts = rados_obj.get_osd_hosts()
    log.info(f"The osd node slist are-{osd_hosts}")
    drain_host = random.choice(osd_hosts)
    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    mgr_dump = rados_obj.run_ceph_command(cmd="ceph mgr dump", client_exec=True)
    active_mgr = mgr_dump["active_name"]
    try:
        mgr_daemon.start()
        service_obj.remove_custom_host(host_node_name=drain_host)
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        exceptional_flag = True
    finally:
        log.info("=======In the bug reproduce finally block===========")
        time.sleep(300)
        if bug_exists:
            log.info("Performing the workaround on the cluster")
            cmd_remove_key = "ceph config-key rm mgr/cephadm/osd_remove_queue"
            rados_obj.run_ceph_command(cmd=cmd_remove_key, client_exec=True)
            mgr_obj.set_mgr_fail()
            log.info(
                f"This is an existing issue.The current version of ceph is {ceph_version}.The bug exists at "
                f"< 18.2.1-194 ceph version"
            )
            log.info("For more details refer the Bug#2305677")
            return 0
        elif not bug_exists and exceptional_flag:
            log.error(
                f"The verification failed.The current version of ceph is {ceph_version}."
                f"The bug fixed ceph-18.2.1-235.Find more details at-Bug#2305677 "
            )
            return 1
        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        if not verify_mgr_traceback_log(
            rados_obj=rados_obj,
            start_time=init_time,
            end_time=end_time,
            mgr_type=active_mgr,
            lines=log_lines,
        ):
            log.error("Traceback messages are noticed in logs")
            return 1
        log.info("Verification completed and not noticed any traceback messages")
        return 0


def verify_mgr_traceback_log(
    rados_obj: RadosOrchestrator, start_time, end_time, mgr_type, lines
) -> bool:
    """
    Retrieve the preempt log using journalctl command
    Args:
        rados_obj: Rados object
        osd: osd id
        start_time: time to start reading the journalctl logs - format ('2022-07-20 09:40:10')
        end_time: time to stop reading the journalctl logs - format ('2022-07-20 10:58:49')
        lines: Log lines to search in the journalctl logs
    Returns:  True-> if the lines are not exist in the journalctl logs
              False -> if the lines are  exist in the journalctl logs
    """

    log.info("Checking log lines")
    log_lines = rados_obj.get_journalctl_log(
        start_time=start_time, end_time=end_time, daemon_type="mgr", daemon_id=mgr_type
    )
    log.debug(f"Journalctl logs are : {log_lines}")
    for line in lines:
        if line in log_lines:
            log.error(f" Found the {line} in the mgr logs")
            return False
    return True


def background_mgr_task(mgr_object):
    """
    Method is used to execute the mgr fail command to execute parallel with other commands
    Args:
        mgr_object: mgr object
    Returns: None
    """
    for _ in range(10):
        mgr_object.set_mgr_fail()
        time.sleep(2)


def check_set_reweight(rados_obj, osd_list):
    """
    Method is used to check the OSD weights and assigned weight if the OSD weight value is 0
    Args:
        rados_obj: Rados object
        osd_list: osd lists. For example [0,1,2,3,4,5]

    Returns:
        True-> If none of the OSD weights are 0 or weights are reassigned
        False-> The cluster has more than one weight value in the cluster

    """
    osd_zero_weight_list = []
    osd_weights = []
    for osd_id in osd_list:
        selected_osd_details = rados_obj.get_osd_details(osd_id=osd_id)
        if selected_osd_details["crush_weight"] == 0:
            osd_zero_weight_list = osd_zero_weight_list + [osd_id]
        osd_weights = osd_weights + [selected_osd_details["crush_weight"]]
    if osd_zero_weight_list is None:
        return True
    unique_weight_list = list(set(osd_weights) - {0})
    if len(unique_weight_list) == 1:
        osd_weight = unique_weight_list[0]
    else:
        log.info(
            f"The osd weights are assigned more than 1 value in the cluster. The weights are-{osd_weights}"
        )
        return False
    if len(osd_zero_weight_list) != 0:
        for osd_id in osd_zero_weight_list:
            rados_obj.reweight_crush_items(name=f"osd.{osd_id}", weight=osd_weight)
    time.sleep(30)  # blind sleep to let stats get updated crush re-weight
    assert wait_for_clean_pg_sets(rados_obj, timeout=900)
    return True
