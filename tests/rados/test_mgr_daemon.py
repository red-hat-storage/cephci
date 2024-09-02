"""
Module to test MGR functionalities

"""

import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH manager daemon functional testing
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)

    mgr_daemons = mgr_obj.get_mgr_daemon_list()
    log.info(f"The list of mgr daemons in the cluster is-{mgr_daemons} ")

    # Check the cluster is having more the one mgr daemons
    mgr_nodes = rados_obj.get_daemon_list_fromCluster(daemon_type="mgr")
    if len(mgr_nodes) <= 1:
        log.error(
            f" Minimum two mgr daemons required to perform the following tests.The mgr count is-{len(mgr_nodes)}"
        )
        return 1

    log.info("Scenario 1: Verification of MGR fail scenario")
    # Get the active MGR name
    mgr_stats = mgr_obj.get_mgr_stats()
    log.info(f"The manager stats is -{mgr_stats}")
    active_mgr = mgr_stats["active_name"]
    log.info(f"The active manager is -{active_mgr} ")
    # Make active mgr fail
    mgr_obj.set_mgr_fail(active_mgr)
    duration = 300  # 300 seconds
    mgr_chk_flag = False
    start_time = time.time()
    while time.time() - start_time < duration:
        mgr_stats = mgr_obj.get_mgr_stats()
        log.info(f"The manager stats is -{mgr_stats}")
        new_active_mgr = mgr_stats["active_name"]
        log.info(f"The new active manager is -{new_active_mgr}")
        if new_active_mgr is not active_mgr:
            log.info(
                f"Scenario-1:After the MGR fail the new active manager selected -{new_active_mgr} "
            )
            mgr_chk_flag = True
            break
        time.sleep(30)

    if not mgr_chk_flag:
        log.error(
            "Scenario-1:After the mgr fail the new active manager is not selected"
        )
        return 1
    mgr_daemons_after_fail = mgr_obj.get_mgr_daemon_list()
    status = len(mgr_daemons_after_fail) == len(mgr_daemons)
    if not status:
        log.error(
            f" Scenario:1-The mgr list is not same-"
            f"mgr daemons before failure are - {mgr_daemons}"
            f"and mgr daemons after fail are -{mgr_daemons_after_fail}"
        )
        return 1
    log.info(
        f"Scenario-1:All the mgrs are up and running.The mgr daemons after fail are -{mgr_daemons_after_fail}"
    )
    # Check the ceph status
    rados_obj.log_cluster_health()
    # check for crashes
    if rados_obj.check_crash_status():
        log.error("Test failed due to crash")
        return 1
    log.info("End of Scenario-1: Verification of MGR fail scenario passed")

    log.info("Scenario 2: Verification of MGR restart scenario")
    # Restart MGR and check
    if not rados_obj.restart_daemon_services(daemon="mgr"):
        log.error("Scenario-2:Manager orch service did not restart as expected")
        raise Exception("Manager orch service did not restart as expected")

    mgr_daemons_after_restart = mgr_obj.get_mgr_daemon_list()
    status = len(mgr_daemons_after_restart) == len(mgr_daemons)
    if not status:
        log.error(
            f"Scenario-2: The mgr list is not same-"
            f"mgr daemons before failure are - {mgr_daemons}"
            f"and mgr daemons after restart are -{mgr_daemons_after_restart}"
        )
        return 1
    log.info(
        f"All the mgrs are up and running.The mgr daemons after restart are -{mgr_daemons_after_restart}"
    )
    rados_obj.log_cluster_health()
    # check for crashes
    if rados_obj.check_crash_status():
        log.error("Test failed due to crash")
        return 1
    time.sleep(10)
    log.info("End of Scenario 2: Verification of MGR restart scenario passed")
    log.info(
        "Scenario 3: Verification of  a scenario of adding and removing mgr daemon"
    )
    mgr_daemons_before_tests = mgr_obj.get_mgr_daemon_list()
    log.info(
        f" Scenario-3: The mgr daemons before starting tests are-{mgr_daemons_before_tests} "
    )
    # Adding a new MGR
    new_node = fetch_node_without_mgr(ceph_cluster)
    if new_node is None:
        log.error("Scenario:3-No new node to add the MGR daemon")
        return 1
    # result = mgr_obj.add_mgr_service(host=new_node.hostname)
    result = mgr_obj.add_mgr_with_label(hostname=new_node.hostname)
    if not result:
        log.error("Scenario:3-Not able to add the mgr daemon to the cluster")
        return 1
    mgr_daemons = mgr_obj.get_mgr_daemon_list()
    log.info(f"Scenario:3- The mgr nodes after adding a node are-{mgr_daemons}")
    log.info(f"Scenario:3-Removing the {new_node.hostname} from the mgr daemons list")
    result = mgr_obj.remove_mgr_with_label(hostname=new_node.hostname)
    if not result:
        log.error("Not able to remove the mgr daemon to the cluster")
        return 1

    mgr_daemons_after_add_remove = mgr_obj.get_mgr_daemon_list()
    log.info(
        f" Scenario-3: The mgr daemons after completing the tests are-{mgr_daemons_after_add_remove} "
    )
    status = len(mgr_daemons_after_add_remove) == len(mgr_daemons_before_tests)
    if not status:
        log.error(
            f" Scenario-3:The mgr list is not same-"
            f"mgr daemons before add remove tests are - {mgr_daemons_before_tests}"
            f"and mgr daemons after add remove test are -{mgr_daemons_after_add_remove}"
        )
        return 1
    log.info(
        f"Scenario:3-All the mgrs are up and running.The mgr daemons after restart are -{mgr_daemons_after_add_remove}"
    )

    log.info(
        "End of Scenario 3: Verification of  a scenario of adding and removing mgr daemon passed"
    )

    log.info(
        "Scenario 4: Verification of  a scenario of scaleup and down of mgr daemon"
    )
    # Get the MGR count in the cluster
    mgr_nodes = rados_obj.get_daemon_list_fromCluster(daemon_type="mgr")
    log.info(f" Scenario-4- The mgr daemons before starting the test-{mgr_nodes}")
    mgr_count = len(mgr_nodes)
    new_node = fetch_node_without_mgr(ceph_cluster)
    mgr_nodes.append(new_node.hostname)
    log.info(f"The mgr count before scaleup is - {mgr_count}")

    result = mgr_obj.mgr_scale_up_down_byLabel(mgr_nodes)
    if not result:
        log.error("Scenario:4-Not able to scale-up the mgr daemon to the cluster")
        return 1
    mgr_nodes = rados_obj.get_daemon_list_fromCluster(daemon_type="mgr")
    mgr_count = len(mgr_nodes)
    log.info(f"The mgr count after scaleup is - {mgr_count}")
    mgr_nodes.pop()
    # scale down scenario
    mgr_obj.mgr_scale_up_down_byLabel(mgr_nodes)
    mgr_nodes = rados_obj.get_daemon_list_fromCluster(daemon_type="mgr")
    mgr_count = len(mgr_nodes)
    if not result:
        log.error("Scenario:4-Not able to scale-down the mgr daemon to the cluster")
        return 1
    log.info(f"Scenario:4-The mgr count after scale-down is - {mgr_count}")
    log.info(
        "End of Scenario 4: Verification of  a scenario of scaleup and down of mgr daemon passed"
    )
    return 0


def fetch_node_without_mgr(cluster):
    """
    Method to get a node that is not an MGR
    Args:
        cluster: ceph cluster object
    return: node that is not a mgr node

    """
    # Get any random node for scale-up the mgr daemons
    mgr_nodes = cluster.get_nodes(role="mgr")
    cluster_nodes = cluster.get_nodes()
    # Get a new node
    for node in cluster_nodes:
        if node not in mgr_nodes:
            return node
