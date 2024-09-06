"""
Module to test addition and removal of mon daemons

"""

import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to test Removal and addition of Mon daemons on RHCS clusters
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonitorWorkflows(node=cephadm)

    log.info("Beginning tests to replace mon daemons on RHCS cluster")

    mon_nodes = ceph_cluster.get_nodes(role="mon")
    mon_host = random.choice(mon_nodes)

    log.info(
        f"Selected host : {mon_host.hostname} with IP {mon_host.ip_address} to test replacement"
    )

    try:
        # Checking if the selected mon is part of Mon Quorum
        quorum = mon_obj.get_mon_quorum_hosts()
        if mon_host.hostname not in quorum:
            log.error(
                f"selected host : {mon_host.hostname} does not have mon as part of Quorum"
            )
            raise Exception("Mon not in quorum error")

        # Setting the mon service as unmanaged
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        # Removing mon service
        if not mon_obj.remove_mon_service(host=mon_host.hostname):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not removed error")

        quorum = mon_obj.get_mon_quorum_hosts()
        if mon_host.hostname in quorum:
            log.error(
                f"selected host : {mon_host.hostname} is present as part of Quorum"
            )
            raise Exception("Mon in quorum error post removal")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post mon daemons Removal. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon removal")

        # Adding the mon back to the cluster
        if not mon_obj.add_mon_service(host=mon_host):
            log.error("Could not add mon service")
            raise Exception("mon service not added error")

        quorum = mon_obj.get_mon_quorum_hosts()
        if mon_host.hostname not in quorum:
            log.error(
                f"selected host : {mon_host.hostname} is not present as part of Quorum post addition"
            )
            raise Exception("Mon not in quorum error post addition")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post mon daemons Addition. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon Addition")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # Adding the mon back to the cluster
        if not mon_obj.add_mon_service(host=mon_host):
            log.error("Could not add mon service")
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # Setting the mon service as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            return 1

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post mon removal and addition")
            log.error("Post execution checks failed on the Stretch cluster")
            return 1

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        f"Successfully removed and added back the mon on host : {mon_host.hostname}"
    )
    return 0
