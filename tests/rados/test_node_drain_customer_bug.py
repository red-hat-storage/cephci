"""
The file contain the method to check the customer issue-
 CEPH-83593996 - Check that the Ceph cluster logs are being generated appropriately according to the log level
"""

import datetime
import random
import re
import time
from threading import Thread

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.serviceability_workflows import ServiceabilityMethods
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
    6. If exception occured then check for the Traceback logs and perform the workaround
    7. After the verification add the drained node back to the cluster
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)
    ceph_nodes = kw.get("ceph_nodes")
    config = kw["config"]

    replicated_config = config.get("replicated_pool")
    pool_name = replicated_config["pool_name"]
    active_osd_list = rados_obj.get_active_osd_list()
    log.info(f"The active OSDs list before starting the test-{active_osd_list}")
    if not rados_obj.create_pool(pool_name=pool_name):
        log.error("Failed to create the  Pool")
        return 1

    rados_obj.bench_write(pool_name=pool_name, byte_size="5M", rados_write_duration=90)
    mgr_daemon = Thread(
        target=background_mgr_task, kwargs={"mgr_object": mgr_obj}, daemon=True
    )
    # Printing the hosts in cluster
    cmd_host_ls = "ceph orch host ls"
    out = rados_obj.run_ceph_command(cmd=cmd_host_ls)
    log.debug(f"The hosts in the cluster before starting the test are - {out}")

    mgr_host_object_list = []
    for node in ceph_nodes:
        if node.role == "mgr":
            mgr_host_object_list.append(node)
            log.debug(f"The mgr host node is{node.hostname}")

    mgr_daemon_list = mgr_obj.get_mgr_daemon_list()
    log.debug(f"The MGR daemons list are -{mgr_daemon_list}")

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
    elif int(major) == 18 and int(minor) == 2 and int(patch) == 1 and int(build) < 234:
        bug_exists = True

    if bug_exists:
        log.info(f"The bug exists and  the ceph version is - {ceph_version}")
    else:
        log.info(f"The bug not exists and the ceph version is - {ceph_version}")

    log.info(
        "The logic developed to select the drain host is-"
        "1. Select the cluster node that has the _no_schedule label.This check is included because in few "
        "   scenarios(7.1z0) first the issue is reproducing and upgrading to the latest version and again "
        "   checking the bug"
        "2. Select the node with OSD weight/reweight are 0 if none of the hosts have the _no_schedule label"
        "3. If both 1&2 failed then select a random OSD node"
    )
    cmd_host_ls = "ceph orch host ls"
    out = rados_obj.run_ceph_command(cmd=cmd_host_ls)
    log.info(f"The node details in the cluster -{out} ")
    drain_host = None
    # Check any nodes that has _no_schedule label
    for node in out:
        if "_no_schedule" in node["labels"]:
            drain_host = node["hostname"]
            log.info(f"The {drain_host} is identified to drain from the cluster")
            break
    # Select the node with OSD nodes weight/re-weights are 0 if none of the hosts have the _no_schedule label.
    if drain_host is None:
        osd_down_node_list = get_zero_osd_weight_list(rados_obj)
        if len(osd_down_node_list) == 0:
            log.info("None of the OSD weight and reweight are 0")
        else:
            drain_host, _ = list(osd_down_node_list.items())[0]
            log.info(f"The drain operation is performing on {drain_host} node")
    # Pick any random OSD node if none of hosts have the _no_schedule label and all OSDs are up.
    if drain_host is None:
        osd_hosts = rados_obj.get_osd_hosts()
        log.info(f"The osd nodes list are-{osd_hosts}")
        drain_host = random.choice(osd_hosts)

    init_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
    log.info(f"The test execution  started at - {init_time} ")
    try:
        osd_count_before_test = get_node_osd_list(rados_obj, ceph_nodes, drain_host)
        log.info(
            f"The OSDs in the drain node before starting the test - {osd_count_before_test} "
        )
        mgr_daemon.start()
        service_obj.remove_custom_host(host_node_name=drain_host)
        time.sleep(300)
        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        log.info(f"The test execution  ends at - {end_time}")
        if not verify_mgr_traceback_log(
            rados_obj=rados_obj,
            start_time=init_time,
            end_time=end_time,
            mgr_daemon_list=mgr_daemon_list,
            mgr_host_object_list=mgr_host_object_list,
            lines=log_lines,
        ):
            log.error(
                "The traceback messages are noticed in logs.The error snippets are noticed in the MGR logs"
            )
            return 1
        log.info(
            "Adding the node by providing the deploy_osd as False, because the script is not setting the "
            "--unmanaged=true.Once the node is added back to the cluster the OSDs get configured automatically"
        )
        service_obj.add_new_hosts(add_nodes=[drain_host], deploy_osd=False)
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
        osd_add_chk_flag = False
        while datetime.datetime.now() <= end_time:
            osd_count_after_test = get_node_osd_list(rados_obj, ceph_nodes, drain_host)
            log.info(
                f"The OSDs after adding the drain node are- {osd_count_after_test} "
            )
            if len(osd_count_after_test) == len(osd_count_before_test):
                log.info("All the OSDs are added to the node")
                osd_add_chk_flag = True
                break
            time.sleep(30)
        if not osd_add_chk_flag:
            log.error("All the OSDs are not added to the node")
            return 1
    except CommandFailed as err:
        log.error(f"Failed with exception: {err.__doc__}")
        log.exception(err)
        exceptional_flag = True

        time.sleep(400)
        end_time, _ = installer.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")
        if verify_mgr_traceback_log(
            rados_obj=rados_obj,
            start_time=init_time,
            end_time=end_time,
            mgr_daemon_list=mgr_daemon_list,
            mgr_host_object_list=mgr_host_object_list,
            lines=log_lines,
        ):
            log.error(
                "The traceback messages are not noticed in logs.The error snippets are not noticed in the "
                "MGR logs"
            )
            return 1

        if bug_exists:
            active_osd_list = rados_obj.get_active_osd_list()
            log.info(
                f"The active OSDs list after reproducing the issue is-{active_osd_list}"
            )
            log.info("Performing the workaround on the cluster")
            cmd_get_key = "ceph config-key get mgr/cephadm/osd_remove_queue"
            key_output = rados_obj.run_ceph_command(cmd=cmd_get_key, client_exec=True)
            log.info(f"The config key output is- {key_output}")
            cmd_remove_key = "ceph config-key rm mgr/cephadm/osd_remove_queue"
            key_output = rados_obj.run_ceph_command(
                cmd=cmd_remove_key, client_exec=True
            )
            if key_output:
                log.error(
                    f"The config key is not removed.The existing config key is- {key_output}."
                )
                return 1
            mgr_obj.set_mgr_fail()
            out = rados_obj.run_ceph_command(cmd=cmd_host_ls)
            log.info(
                f"The node details in the cluster after draining the node {drain_host} is  -{out} "
            )
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
    except Exception as err:
        log.error(
            f"Could not add host : {drain_host} into the cluster and deploy OSDs. Error : {err}"
        )
        raise Exception("Host not added error")
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        if replicated_config.get("delete_pool"):
            rados_obj.delete_pool(pool=pool_name)
        time.sleep(5)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0


def verify_mgr_traceback_log(
    rados_obj: RadosOrchestrator,
    start_time,
    end_time,
    mgr_daemon_list,
    mgr_host_object_list,
    lines,
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

    host = None
    log.info("Checking log lines")
    fsid = rados_obj.run_ceph_command(cmd="ceph fsid")["fsid"]
    for mgr_daemon in mgr_daemon_list:
        systemctl_name = f"ceph-{fsid}@mgr.{mgr_daemon}.service"
        host_name = mgr_daemon.split(".")[0]
        for mgr_obj in mgr_host_object_list:
            if mgr_obj.hostname == host_name:
                host = mgr_obj
                break
        log_lines, err = host.exec_command(
            cmd=f"sudo journalctl -u {systemctl_name} --since '{start_time.strip()}' --until '{end_time.strip()}'"
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
    time.sleep(20)
    for _ in range(10):
        active_mgr_before_fail = mgr_object.get_active_mgr()
        mgr_object.set_mgr_fail()
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=3)
        while datetime.datetime.now() <= end_time:
            active_mgr_after_fail = mgr_object.get_active_mgr()
            if active_mgr_before_fail != active_mgr_after_fail:
                break
            time.sleep(1)


def get_node_osd_list(rados_object, ceph_nodes, drain_host):
    """
    Method is used to return the osd of a node
    Args:
        rados_object: Rados object
        ceph_nodes: ceph node object
        drain_host: The host name

    Returns: the list of OSDs that are available in the node

    """
    for node in ceph_nodes:
        if node.role == "osd":
            if node.hostname == drain_host:
                node_osds = rados_object.collect_osd_daemon_ids(node)
                return node_osds


def get_zero_osd_weight_list(rados_obj):
    """
    Method return the nodes that contain OSD weight or reweight 0
    Args:
        rados_obj: Rados object

    Returns: Dictionary of hosts that has weight or reweight 0

    """
    osd_hosts = rados_obj.get_osd_hosts()
    zero_osd_weight_nodes = {}
    for host in osd_hosts:
        zero_osd_weight_nodes[host] = []
        df_output = rados_obj.get_osd_df_stats(
            tree=False, filter_by="name", filter=f"{host}"
        )
        for osd in df_output["nodes"]:
            if osd["crush_weight"] == 0 or osd["reweight"] == 0:
                zero_osd_weight_nodes[host].append(osd["id"])
    for key, value in list(zero_osd_weight_nodes.items()):
        if not value:
            del zero_osd_weight_nodes[key]
    return zero_osd_weight_nodes
