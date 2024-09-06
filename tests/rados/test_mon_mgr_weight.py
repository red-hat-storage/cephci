"""
Module to ensure Mgr daemon does not crash when Monitor weights are out of place
"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonElectionStrategies
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83588304
    Covers:
        - BZ-2236226
        - BZ-2269541
        - BZ-2269542
    Should cover:
        - BZ-2269543
    Test to verify Mgr daemon does not crash when Monitor weights are out of place
    # Steps
    1. Deploy a ceph cluster
    2. Capture ceph monitor dump before modifying weights
    3. Modify the mon weight for the active mon daemon
    4. Check the modified weight is showing up correctly in ceph mon dump
    5. Restart MGR orch services
    6. All the MGR daemons should start without fail, no crashes should be reported
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    log.info("Running test case to verify Mgr stability when Mon weights are modified")

    try:
        if rhbuild.startswith("5"):
            log.info("Test is not valid for Pacific, BZ yet to be back-ported")
            return 0

        # fetch active MGR for the cluster
        active_mgr = rados_obj.run_ceph_command(cmd="ceph mgr stat", client_exec=True)[
            "active_name"
        ]
        log.info(f"Active Mgr for the cluster: {active_mgr}")

        out = rados_obj.run_ceph_command(cmd="ceph mon dump", client_exec=True)
        mon_list = out["mons"]
        current_leader = mon_obj.get_mon_quorum_leader()

        for mon in mon_list:
            if current_leader == mon["name"]:
                active_mon = mon["name"]
                break

        log.info(f"Active Mon for the cluster: {active_mon}")
        log.info(f"Monitor for whom weight is going to be modified: {active_mon}")
        log.info(f"Monitor details \n: {mon}")

        if mon["weight"] != 0:
            log.error(f"Default weight of Monitor {active_mon} is not 0")
            raise Exception(f"Default weight of Monitor {active_mon} is not 0")

        # change the weight to 10, 0, and 5 one after the other and restart MGRs,
        # ensure no crash is detected
        for weight in [10, 0, 5]:
            log.info(
                f"\n\n\n ***** Executing workflow for mon weight {weight} ******* \n\n\n"
            )
            set_weight_cmd = f"ceph mon set-weight {active_mon} {weight}"
            rados_obj.client.exec_command(cmd=set_weight_cmd, client_exec=True)
            time.sleep(10)

            # check if the value was set correctly
            out = rados_obj.run_ceph_command(cmd="ceph mon dump", client_exec=True)
            mon_list = out["mons"]

            for mon in mon_list:
                if current_leader == mon["name"]:
                    active_mon = mon["name"]
                    break

            log.info(f"Monitor details \n: {mon}")
            if not mon["weight"] == weight:
                log.error(f"Mon weight for active Mon {active_mon} not set properly")
                raise Exception(
                    f"Mon weight for active Mon {active_mon} not set properly"
                )

            log.info(
                f"Mon weight for Active mon {active_mon} set to {weight} successfully"
            )

            # restart Mgr services and monitor cluster for 120 secs for any crash
            if not rados_obj.restart_daemon_services(daemon="mgr"):
                log.error("Manager orch service did not restart as expected")
                raise Exception("Manager orch service did not restart as expected")

            mgr_status, mgr_status_desc = rados_obj.get_daemon_status(
                daemon_type="mgr", daemon_id=active_mgr
            )
            if mgr_status != 1 or mgr_status_desc != "running":
                log.error(
                    "Manager for whom mon-weight was modified is not in running state post "
                    "restart"
                )
                raise Exception("Manager with modified weight is not in running state")

            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=120)
            while datetime.datetime.now() < timeout_time:
                # list any reported crashes on the cluster, should not have any
                crash_out = rados_obj.run_ceph_command(
                    cmd="ceph crash ls-new", client_exec=True
                )
                if len(crash_out) != 0:
                    log.error(
                        f"Verification failed, crash observed on the cluster : \n {crash_out}"
                    )
                    raise Exception(
                        "Verification failed, crash observed on the cluster"
                    )
                log.info(
                    "No crashes reported on the cluster, proceeding to check health status"
                )

                # fetch ceph health detail
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                assert "no active mgr" not in health_detail
                time.sleep(30)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # reset monitor weight to 0
        if "active_mon" in locals() or "active_mon" in globals():
            set_weight_cmd = f"ceph mon set-weight {active_mon} 0"
            rados_obj.client.exec_command(cmd=set_weight_cmd, client_exec=True)

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Mgr stability with varying Mon weight verified successfully")
    return 0
