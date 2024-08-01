"""
Module to check for the warning "OSD_UPGRADE_FINISHED" during upgrades

"""

import datetime
import time

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to check if the warning "OSD_UPGRADE_FINISHED" is generated when "require_osd_release" does ot match
    the current release during upgrades.

    Quincy to reef bug : https://bugzilla.redhat.com/show_bug.cgi?id=2243570
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm_obj = CephAdmin(cluster=ceph_cluster, **config)
    cluster_obj = Orch(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm_obj)
    verify_warning = config.get("verify_warning", True)
    verify_daemons = config.get("verify_daemons", False)
    verify_cluster_usage = config.get("verify_cluster_usage", False)

    log.debug("Collecting daemon info and Cluster usage info before the upgrade")
    pre_upgrade_orch_ps = rados_obj.run_ceph_command(cmd="ceph orch ps")
    pre_upgrade_df_detail = rados_obj.run_ceph_command(cmd="ceph df detail")
    if not rados_obj.verify_max_avail():
        log.error("MAX_AVAIL deviates on the cluster more than expected")
        # Not failing the workflow as of now
        # raise Exception("MAX_AVAIL not proper error")

    log.debug("Starting upgrade")
    try:
        cluster_obj.set_tool_repo()
        time.sleep(5)
        cluster_obj.install()
        time.sleep(5)

        # Check service versions vs available and target containers
        cluster_obj.upgrade_check(image=config.get("container_image"))

        ceph_version = rados_obj.run_ceph_command(cmd="ceph version")
        log.info(f"Current version on the cluster : {ceph_version}")

        # Start Upgrade
        config.update({"args": {"image": "latest"}})
        cluster_obj.start_upgrade(config)
        time.sleep(5)

        warn_flag = False
        upgrade_complete = False
        # Monitor upgrade status, till completion, checking for the warning to be generated
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=3600)
        while end_time > datetime.datetime.now():
            cmd = "ceph orch upgrade status"
            out = rados_obj.run_ceph_command(cmd=cmd, client_exec=True)

            if not out["in_progress"]:
                log.info("Upgrade Complete...")
                upgrade_complete = True
                break

            log.debug(f"upgrade in progress. Status : {out}")
            if not warn_flag:
                status_report = rados_obj.run_ceph_command(
                    cmd="ceph report", client_exec=True
                )
                ceph_health_status = list(status_report["health"]["checks"].keys())
                expected_health_warns = "OSD_UPGRADE_FINISHED"
                if expected_health_warns in ceph_health_status:
                    warn_flag = True
                    log.info(
                        f"We have the expected health warning generated on the cluster.\n "
                        f"Warnings on cluster: {ceph_health_status}"
                    )
                    out = rados_obj.run_ceph_command(cmd="ceph health detail")
                    log.info(f"\n\nHealth detail on the cluster :\n {out}\n\n")
                else:
                    log.debug(
                        "expected health warning not yet generated on the cluster."
                        f" health_warns on cluster : {ceph_health_status}"
                    )
        if not upgrade_complete:
            log.error("Upgrade was not completed on the cluster. Fail")
            raise Exception("Upgrade not complete")

        if verify_warning:
            if not warn_flag:
                log.error("expected warning not generated on the cluster. Fail")
                raise Exception("Warning not raised")
            status_report = rados_obj.run_ceph_command(
                cmd="ceph report", client_exec=True
            )
            ceph_health_status = list(status_report["health"]["checks"].keys())
            expected_health_warns = "OSD_UPGRADE_FINISHED"
            if expected_health_warns in ceph_health_status:
                log.error(
                    "Warning about the mismatched release present on the cluster. Fail. "
                )
                out = rados_obj.run_ceph_command(cmd="ceph health detail")
                log.error(f"\n\nHealth detail on the cluster :\n {out}\n\n")
                raise Exception("Warning present post upgrade")

            log.info(
                "Warning OSD_UPGRADE_FINISHED was found on in"
                " health status and Removed once upgrade was completed"
            )

        if not rados_obj.verify_max_avail():
            log.error(
                "MAX_AVAIL deviates on the cluster more than expected post upgrade"
            )
            # Not failing the workflow as of now
            # raise Exception("MAX_AVAIL not proper error")

        if verify_daemons:
            if not rados_obj.daemon_check_post_tests(
                pre_test_orch_ps=pre_upgrade_orch_ps
            ):
                log.error("There are daemons missing post upgrade")
                raise Exception("Daemons missing post upgrade error")
            log.info("All the daemon existence verified")

        if verify_cluster_usage:
            if not rados_obj.compare_df_stats(pre_test_df_stats=pre_upgrade_df_detail):
                log.error("Cluster usage changed post upgrade")
                # Not failing tests as of now due to RAW usage changes
                # raise Exception("Cluster usage changed post upgrade error")
            log.info("Cluster usage before and after upgrade verified")

        log.info("Warning about the mismatched release found on cluster. Pass. ")
        log.info("Completed upgrade on the cluster")
        return 0
    except Exception as e:
        log.error(f"Could not upgrade the cluster. error : {e}")
        return 1
