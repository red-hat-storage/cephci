"""
This test will exercise the basic mon related operations like mon add, remove and some failure scenarios.
MON operations and failure scenarios
Resizing the monitor cluster
Adding a monitor
Removing a monitor from a healthy cluster
Mon daemon failure - restart and kill

"""

import random

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Module to test Mon daemons on RHCS clusters
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonitorWorkflows(node=cephadm)
    cluster_nodes = ceph_cluster.get_nodes()
    init_mon_nodes = ceph_cluster.get_nodes(role="mon")

    pool_name = "mon_service_test_pool"
    try:
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Could not create pool {pool_name} for mon tests")
            raise Exception("Pool not created error")

        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=50
        )

        # Increasing backfill & recovery rate
        rados_obj.change_recovery_threads(config={}, action="set")

        log.info("---------- starting workflow 1 - Addition of new mons --------")
        # Setting the mon service as unmanaged by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        found = False
        # Identifying test host without any mons to deploy new mons
        for item in cluster_nodes:
            if item.hostname not in [mon.hostname for mon in init_mon_nodes]:
                alt_mon_host = item
                found = True
                break
        if not found:
            log.error("No alternate mon host was found... fail")
            raise Exception("No free mon host found error")

        log.debug(
            f"Selected host: {alt_mon_host.hostname} with "
            f"IP : {alt_mon_host.ip_address} to deploy new mon daemon"
        )

        # Adding the new mon to the cluster
        if not mon_obj.add_mon_service(host=alt_mon_host):
            log.error(f"Could not add mon service on host {alt_mon_host.hostname} ")
            raise Exception("mon service not added error")

        # Setting the mon service as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

        # Checking if the new mon added is part of Mon Quorum
        quorum = mon_obj.get_mon_quorum_hosts()
        if alt_mon_host.hostname not in quorum:
            log.error(
                f"selected host : {alt_mon_host.hostname} does not have mon as part of Quorum post addition"
            )
            raise Exception("Mon not in quorum error")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post addition of new mon daemons on cluster. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon addition")
        log.info("Completed workflow 1. Addition of new mon daemon")

        log.info("---------- starting workflow 2 - Removal of new mons --------")
        # Setting the mon service as unmanaged
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        # Removing mon service
        if not mon_obj.remove_mon_service(host=alt_mon_host.hostname):
            log.error("Could not remove the new mon added previously")
            raise Exception("mon service not removed error")

        # Setting the mon service as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

        quorum = mon_obj.get_mon_quorum_hosts()
        if alt_mon_host.hostname in quorum:
            log.error(
                f"selected host : {alt_mon_host.hostname} is present as part of Quorum post removal"
            )
            raise Exception("Mon in quorum error post removal")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post removal of mon daemons on cluster. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon removal")

        log.info("Completed Scenario 2")

        log.info(
            "---------- starting workflow 3 - Removal of mons during OSD down --------"
        )

        # fetching all the OSDs on the cluster and selecting 1 OSD at random to be shut down.
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")

        osd_id = int(random.choice(osd_list))
        log.debug(f"Selected OSD : {osd_id} to be shut down")
        if not rados_obj.change_osd_state(action="stop", target=osd_id):
            log.error(f"Could not stop OSD : {osd_id}")
            raise Exception("OSD not stopped error")

        # Selecting one mon host to be removed at random during OSD down
        test_mon_host = random.choice(init_mon_nodes)

        log.debug(f"Selected mon : {test_mon_host.hostname} to be removed from cluster")
        # Setting the mon service as unmanaged
        if not mon_obj.set_mon_service_managed_type(unmanaged=True):
            log.error("Could not set the mon service to unmanaged")
            raise Exception("mon service not unmanaged error")

        # Removing mon service
        if not mon_obj.remove_mon_service(host=test_mon_host.hostname):
            log.error("Could not remove the new mon added previously")
            raise Exception("mon service not removed error")

        quorum = mon_obj.get_mon_quorum_hosts()
        if test_mon_host.hostname in quorum:
            log.error(
                f"selected host : {test_mon_host.hostname} has a mon daemon present as part of Quorum post Removal"
            )
            raise Exception("Mon in quorum error post removal error")

        # Writing Objects on pool post mon removal and OSD down
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=50, check_ec=False
        )

        if not rados_obj.change_osd_state(action="start", target=osd_id):
            log.error(f"Could not start OSD : {osd_id}")
            raise Exception("OSD not started error")

        # Now that the OSD is restarted, waiting for the recovery while the mon is removed
        log.debug(
            " waiting for PGs to settle down and recover post OSD stop and start during mon removal"
        )
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=5000, test_pool=pool_name
        )

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post mon removal and OSD rebalance")
            raise Exception("Secnario 3 of Mon tests failed")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post removal of mon daemons during OSD down Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon removal with OSD failures")
        log.info("Completed scenario 3. Pass")

        log.info(
            "---------- starting workflow 4 - Addition of mons during OSD down --------"
        )

        # fetching all the OSDs on the cluster and selecting 1 OSD at random to be shut down.
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")

        osd_id = int(random.choice(osd_list))
        log.debug(f"Selected OSD : {osd_id} to be shut down")
        if not rados_obj.change_osd_state(action="stop", target=osd_id):
            log.error(f"Could not stop OSD : {osd_id}")
            raise Exception("OSD not stopped error")

        # Adding the mon back to the cluster
        if not mon_obj.add_mon_service(host=test_mon_host):
            log.error(f"Could not add mon service on host {test_mon_host}")
            raise Exception("mon service not added error")

        # Setting the mon service as managed by cephadm
        if not mon_obj.set_mon_service_managed_type(unmanaged=False):
            log.error("Could not set the mon service to managed")
            raise Exception("mon service not managed error")

        # Checking if the mon added is part of Mon Quorum
        quorum = mon_obj.get_mon_quorum_hosts()
        if test_mon_host.hostname not in quorum:
            log.error(
                f"selected host : {test_mon_host.hostname} does not have mon as part of Quorum post addition"
            )
            raise Exception("Mon not in quorum post addition error")

        # Writing Objects on pool post mon Addition and OSD down
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=50, check_ec=False
        )

        if not rados_obj.change_osd_state(action="start", target=osd_id):
            log.error(f"Could not start OSD : {osd_id}")
            raise Exception("OSD not started error")

        # Now that the OSD is restarted, waiting for the recovery while the mon is Added
        log.debug(
            " waiting for PGs to settle down and recover post OSD stop and start during mon Addition"
        )
        method_should_succeed(
            wait_for_clean_pg_sets, rados_obj, timeout=5000, test_pool=pool_name
        )

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post mon addition and OSD rebalance")
            raise Exception("Secnario 4 of Mon tests failed")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post addition of new mon daemons with OSD Failures. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon removal during OSD failures")
        log.info("Completed scenario 4. Pass")

        log.info(
            "---------- starting workflow 5 - Start and Stop of mon daemons with IOs --------"
        )
        test_mon_host = random.choice(init_mon_nodes)
        log.debug(f"Selected mon : {test_mon_host.hostname} to be stopped")
        if not rados_obj.change_daemon_systemctl_state(
            action="stop", daemon_type="mon", daemon_id=test_mon_host.hostname
        ):
            log.error(f"Failed to stop mon on host {test_mon_host.hostname}")
            raise Exception("Mon stop failure error")

        # Writing Objects on pool post mon down
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=50, check_ec=False
        )

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post mon daemons shut down on cluster. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon shut down")

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post mon down")
            raise Exception("Sanity check failed post mon down")

        if not rados_obj.change_daemon_systemctl_state(
            action="start", daemon_type="mon", daemon_id=test_mon_host.hostname
        ):
            log.error(f"Failed to start mon on host {test_mon_host.hostname}")
            raise Exception("Mon start failure error")

        # Writing Objects on pool post mon Up
        rados_obj.bench_write(
            pool_name=pool_name, byte_size=1024, rados_write_duration=50, check_ec=False
        )

        if not rados_obj.run_pool_sanity_check():
            log.error("Checks failed post mon Up")
            raise Exception("Sanity check failed post mon start")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post mon daemons restart on cluster. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon reboots")

        log.info("Completed scenario 5. Pass")

        log.info(
            "---------- starting workflow 6 - rolling reboot of all mon daemons with IOs --------"
        )

        for mon_node in init_mon_nodes:
            if not rados_obj.change_daemon_systemctl_state(
                action="restart", daemon_type="mon", daemon_id=mon_node.hostname
            ):
                log.error(f"Failed to reboot mon on host {test_mon_host.hostname}")
                raise Exception("Mon restart failure error")

            # Writing Objects on pool post mon reboots
            rados_obj.bench_write(
                pool_name=pool_name,
                byte_size=1024,
                rados_write_duration=50,
                check_ec=False,
            )

            if not rados_obj.run_pool_sanity_check():
                log.error("Checks failed post mon reboots")
                raise Exception("Sanity check failed post mon reboots")
            log.debug(f"Successfully restarted mon : {mon_node.hostname}")

        # Checking for crashes in the cluster
        crash = rados_obj.do_crash_ls()
        if crash:
            log.error(
                f"Crashes seen on cluster post mon daemons rolling reboots on cluster. Crash : {crash}"
            )
            raise Exception("Service crash on cluster error")
        log.debug("No crashes seen on cluster post mon rolling reboots")

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # setting the backfill & recovery rate to default
        rados_obj.change_recovery_threads(config={}, action="rm")
        mon_obj.set_mon_service_managed_type(unmanaged=False)

        if "alt_mon_host" in locals() or "alt_mon_host" in globals():
            mon_obj.remove_mon_service(host=alt_mon_host.hostname)

        if "test_mon_host" in locals() or "test_mon_host" in globals():
            mon_obj.add_mon_service(host=test_mon_host)

        if "osd_id" in locals() or "osd_id" in globals():
            rados_obj.change_osd_state(action="restart", target=osd_id)

        # removal of rados pools
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        "Completed rolling reboot of all mon daemons and sanity check."
        " All scenarios Passed"
    )
    return 0
