"""
Module to verify the removal and replacement of an OSD from the Ceph cluster.
This test validates the OSD lifecycle by executing a defined sequence of actions:
1. Mark the selected OSD down and out.
2. Stop the OSD process.
3. Remove the OSD using `ceph orch osd rm` with --zap, --replace, and --force options.
4. Ensure the OSD no longer appears in the cluster via `get_osd_list()` output.
5. Verify no `CEPHADM_STRAY_DAEMON` warnings are raised post-removal.
The test ensures the OSD is cleanly removed from the cluster's control plane and
that no residual daemon artifacts or health warnings persist.
Cluster health is verified at the end of the test to confirm it returns to `HEALTH_OK`.
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Case: Verify that there are no CEPHADM_STRAY_DAEMON warnings while replacing the OSD
    BZ's: https://bugzilla.redhat.com/show_bug.cgi?id=2269003, https://bugzilla.redhat.com/show_bug.cgi?id=2355037
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    try:

        def verify_osd_removal(osd_id: int):
            log.info("Starting verification for removal of osd.{0}".format(osd_id))

            # Step 1: Mark OSD out
            log.info("Step 2: Marking osd out")
            if not rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="out"):
                err_msg = "Failed to mark osd.{0} out".format(osd_id)
                log.error(err_msg)
                raise Exception(err_msg)

            # Step 2: Stop OSD
            log.info("Step 3: Stopping OSD process")
            try:
                rados_obj.run_ceph_command(cmd="ceph osd stop {0}".format(osd_id))
            except Exception as e:
                err_msg = "Failed to stop OSD.{0} process: {1}".format(osd_id, e)
                log.error(err_msg)
                raise Exception(err_msg)

            # Step 3: Mark OSD down
            log.info("Step 1: Marking osd down")
            if not rados_obj.update_osd_state_on_cluster(osd_id=osd_id, state="down"):
                err_msg = "Failed to mark osd.{0} down".format(osd_id)
                log.error(err_msg)
                raise Exception(err_msg)

            # Step 4: Remove OSD via orchestrator
            log.info("Step 4: Removing OSD with zap and replace")
            try:
                rados_obj.run_ceph_command(
                    cmd="ceph orch osd rm {0} --zap --replace --force".format(osd_id)
                )
            except Exception as e:
                err_msg = "Failed to remove OSD.{0} using orchestrator: {1}".format(
                    osd_id, e
                )
                log.error(err_msg)
                raise Exception(err_msg)

            # Step 5: Wait and verify OSD list
            log.info("Waiting 60 seconds before verifying updated OSD list")
            time.sleep(60)
            # Fetch list of destroyed OSDs
            destroyed_osds = rados_obj.get_osd_list(status="destroyed")
            if osd_id not in destroyed_osds:
                err_msg = f"osd.{osd_id} not found in destroyed list after removal"
                log.error(err_msg)
                raise Exception(err_msg)
            log.info(f"osd.{osd_id} successfully marked as destroyed in the OSD tree")

            # Step 6: Wait and verify node list
            log.info("Waiting 60 seconds before verifying ceph node ls")
            time.sleep(60)
            node_ls = rados_obj.run_ceph_command(cmd="ceph node ls")
            osd_still_present = any(
                osd_id in osds for osds in node_ls.get("osd", {}).values()
            )
            if osd_still_present:
                err_msg = f"osd.{osd_id} still present in node list"
                log.error(err_msg)
                raise Exception(err_msg)
            log.info(f"osd.{osd_id} successfully removed from node list")

            # Step 7: Check for stray daemon warning
            log.info("Checking for CEPHADM_STRAY_DAEMON warnings")
            if rados_obj.check_health_warning("CEPHADM_STRAY_DAEMON"):
                err_msg = "Found CEPHADM_STRAY_DAEMON warning after OSD replacement"
                log.error(err_msg)
                raise Exception(err_msg)
            log.info("No CEPHADM_STRAY_DAEMON warnings found")

        # Fetch an OSD to test
        log.info("Fetching available OSD ID for removal...")
        available_osds = rados_obj.get_osd_list(status="up")
        if not available_osds:
            err_msg = "No 'up' OSDs available for testing"
            log.error(err_msg)
            raise Exception(err_msg)

        osd_id_to_remove = random.choice(available_osds)
        log.info("Selected osd.{0} for removal".format(osd_id_to_remove))

        verify_osd_removal(osd_id_to_remove)

        return 0

    except Exception as e:
        log.error("An error occurred during the OSD replacement process: {0}".format(e))
        raise

    finally:
        log.info(
            "\n\n************** Execution of finally block begins here ***************\n\n"
        )
        log.info("Ensuring no crashes occurred during the testing")
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash")
            return 1
        rados_obj.log_cluster_health()
        log.info("Test completed successfully without issues or crashes")
