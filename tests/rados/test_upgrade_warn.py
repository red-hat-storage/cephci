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
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm_obj = CephAdmin(cluster=ceph_cluster, **config)
    cluster_obj = Orch(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm_obj)

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
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=14400)
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
        if not warn_flag:
            log.error("expected warning not generated on the cluster. Fail")
            raise Exception("Warning not raised")

        if not upgrade_complete:
            log.error("Upgrade was not completed on the cluster. Fail")
            raise Exception("Upgrade not complete")

        status_report = rados_obj.run_ceph_command(cmd="ceph report", client_exec=True)
        ceph_health_status = list(status_report["health"]["checks"].keys())
        expected_health_warns = "OSD_UPGRADE_FINISHED"
        if expected_health_warns in ceph_health_status:
            log.error(
                "Warning about the mismatched release present on the cluster. Fail. "
            )
            out = rados_obj.run_ceph_command(cmd="ceph health detail")
            log.error(f"\n\nHealth detail on the cluster :\n {out}\n\n")
            raise Exception("Warning present post upgrade")

        log.info("Warning about the mismatched release found on cluster. Pass. ")
        log.info("Completed upgrade on the cluster")
        return 0
    except Exception as e:
        log.error(f"Could not upgrade the cluster. error : {e}")
        return 1
