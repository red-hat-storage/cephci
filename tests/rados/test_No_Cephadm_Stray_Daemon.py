"""
Module to verify the removal and replacement of an OSD from the Ceph cluster.
This test ensures the proper lifecycle of OSD, by executing a sequence
of actions that include marking the OSD down and out, stopping the OSD service, removing
it from the CRUSH map, deleting its authentication, and finally removing or replacing
the OSD using the Ceph orchestrator with zap, replace, and force options.
The module also validates that the removed OSD no longer appears in the CRUSH map or the
output of the `ceph node ls` command. Post-removal, verifies that no `CEPHADM_STRAY_DAEMON`
warnings are present after the OSD is replaced, ensuring a clean and complete OSD removal
and replacement process.
The cluster health is checked to ensure it returns to a `HEALTH_OK` state.
"""

import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Case: Verify that there are no CEPHADM_STRAY_DAEMON warnings while replacing the OSD

    This test case contains the following steps:
        1. Mark the OSD as down and out.
        2. Remove the OSD from the CRUSH map and Ceph auth.
        3. Remove the OSD using ceph orch with --zap and --replace.
        4. Wait and verify OSD is removed from ceph osd tree and ceph node ls.
        5. Confirm the cluster is in HEALTH_OK state.
        6. Assert no 'CEPHADM_STRAY_DAEMON' warnings are present.
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    try:

        def verify_osd_removal(osd_id: int):
            log.info("Starting verification for removal of osd.{}".format(osd_id))

            try:
                # Step 1: Check initial OSD tree
                log.info("Step 1: Checking initial OSD tree")
                initial_hosts = rados_obj.get_osd_hosts()
                log.info("Initial OSD Hosts: {}".format(initial_hosts))

                # Step 2: Mark OSD down
                log.info("Step 2: Marking osd down")
                cephadm.shell(args=["ceph", "osd", "down", "osd.{}".format(osd_id)])

                # Step 3: Mark OSD out
                log.info("Step 3: Marking osd out")
                cephadm.shell(args=["ceph", "osd", "out", "osd.{}".format(osd_id)])

                # Step 4: Stop OSD
                log.info("Step 4: Stopping osd process")
                cephadm.shell(args=["ceph", "osd", "stop", "osd.{}".format(osd_id)])

                # Step 5: Remove from CRUSH map
                log.info("Step 5: Removing from CRUSH map")
                cephadm.shell(
                    args=["ceph", "osd", "crush", "remove", "osd.{}".format(osd_id)]
                )

                # Step 6: Delete OSD auth
                log.info("Step 6: Deleting OSD auth")
                cephadm.shell(args=["ceph", "auth", "del", "osd.{}".format(osd_id)])

                # Step 7: Remove/replace via orchestrator
                log.info("Step 7: Removing via ceph orch")
                cephadm.shell(
                    args=[
                        "ceph",
                        "orch",
                        "osd",
                        "rm",
                        str(osd_id),
                        "--zap",
                        "--replace",
                        "--force",
                    ]
                )

                # Step 8: Check updated OSD tree
                log.info("Step 8: Waiting 60 seconds before checking updated OSD tree")
                time.sleep(60)

                osd_tree_output, _ = cephadm.shell(args=["ceph", "osd", "tree"])
                log.info("Step 8: Updated ceph osd tree:\n{}".format(osd_tree_output))

                updated_hosts = rados_obj.get_osd_hosts()
                log.info("Updated OSD Hosts: {}".format(updated_hosts))

                # Step 9: Wait for node state sync before checking node ls
                log.info(
                    "Step 9: Waiting 60 seconds before verifying OSD status in node list"
                )
                time.sleep(60)

                log.info("Step 9: Verifying osd not in ceph node ls")
                node_ls_out, _ = cephadm.shell(
                    args=["ceph", "node", "ls", "-f", "json"]
                )
                node_ls_json = json.loads(node_ls_out)

                osd_still_present = False
                for host, osds in node_ls_json.get("osd", {}).items():
                    if osd_id in osds:
                        osd_still_present = True
                        log.debug(
                            "osd.{} still present on host {}".format(osd_id, host)
                        )
                        break

                if osd_still_present:
                    err_msg = (
                        "osd.{} still present in node list after 60 seconds".format(
                            osd_id
                        )
                    )
                    log.error(err_msg)
                    raise Exception(err_msg)
                else:
                    log.info(
                        "osd.{} successfully removed from node list".format(osd_id)
                    )

                # Step 10: Check for CEPHADM_STRAY_DAEMON warning
                log.info("Step 10: Checking for CEPHADM_STRAY_DAEMON warnings")
                stray_check_output, _ = cephadm.shell(args=["ceph", "health", "detail"])
                if "CEPHADM_STRAY_DAEMON" in stray_check_output:
                    err_msg = "Found CEPHADM_STRAY_DAEMON warning after OSD replacement"
                    log.error(err_msg)
                    raise Exception(err_msg)
                log.info("No CEPHADM_STRAY_DAEMON warnings found")

                # Step 11: Cluster health check
                log.info("Step 11: Verifying cluster is in HEALTH_OK state")
                health = rados_obj.log_cluster_health()
                if "HEALTH_OK" not in health:
                    err_msg = "Cluster is not in HEALTH_OK state after OSD removal"
                    log.error(err_msg)
                    raise Exception(err_msg)

                log.info(
                    "Verification completed successfully for osd.{}".format(osd_id)
                )

            except Exception as e:
                log.error("Error during OSD removal for osd.{}: {}".format(osd_id, e))
                raise

        # Step: Fetch OSD ID from cluster
        log.info("Fetching available OSD ID for removal...")
        osd_tree_raw, _ = cephadm.shell(args=["ceph", "osd", "tree", "-f", "json"])
        osd_tree = json.loads(osd_tree_raw)
        available_osds = [
            entry["id"] for entry in osd_tree["nodes"] if entry["type"] == "osd"
        ]

        if available_osds:
            osd_id_to_remove = available_osds[0]
            log.info("Selected osd.{} for removal".format(osd_id_to_remove))
            try:
                verify_osd_removal(osd_id_to_remove)
            except Exception as e:
                log.error(
                    "Error occurred during OSD removal verification: {}".format(e)
                )
                raise
        else:
            raise Exception("No OSDs found in the cluster!")

    except Exception as e:
        log.error("An error occurred during the OSD replacement process: {}".format(e))
        raise Exception("Test failed due to an error: {}".format(e))

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
        return 0
