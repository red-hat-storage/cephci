"""
Module to verify Ceph Manager Failover functionality and ensure system stability
during continuous failover cycles. This test also checks for defunct SSH processes
after each failover and applies a workaround if needed (for Pacific release).
The test is designed to run continuously through multiple failover cycles and ensure
the robustness of the Ceph Manager failover process, as well as system stability with
respect to SSH process management.
This test is particularly useful for validating that Ceph Manager failover does not
leave lingering issues such as defunct SSH processes or abnormal system behavior.
"""

import subprocess
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to verify Manager Failover and check for Defunct SSH Processes
    Test Steps:
        1. Failover of Active Manager Continuously
        2. Check for Defunct SSH Processes
        3. Ensure system stability after failover
        4. Application Workaround (if applicable) for defunct SSH
        5. Ensure No Crashes During Testing
    """

    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        # Step 1: Ensure the cluster is healthy
        log.info("Checking cluster health...")
        rados_obj.log_cluster_health()

        # Initialize active manager before the failover cycle loop
        active_mgr = mgr_obj.get_active_mgr()
        log.info("Current active manager is: {}".format(active_mgr))

        # Repeat to simulate continuous failovers
        failover_cycles = 6
        for i in range(failover_cycles):
            active_mgr = perform_failover_cycle(mgr_obj, active_mgr, i + 1)
            if active_mgr == 1:
                return 1

            # Step 2: Check for defunct SSH processes
            log.info("Step 2: Checking for defunct SSH processes")
            try:
                defunct_processes = (
                    subprocess.check_output(
                        "ps aux | grep defunct | grep -v grep", shell=True
                    )
                    .decode()
                    .strip()
                )
            except subprocess.CalledProcessError as e:
                log.error(
                    "Error occurred while checking defunct processes: {}".format(str(e))
                )
                defunct_processes = ""

            log.info(
                "Output of grep for defunct processes: {}".format(defunct_processes)
            )

            if "defunct" in defunct_processes:
                log.error("Defunct processes found: {}".format(defunct_processes))

                # Step 3: Get Ceph Version using run_ceph_command
                ceph_version_output = rados_obj.run_ceph_command(cmd="ceph version")
                if ceph_version_output is None:
                    log.error("Failed to retrieve Ceph version.")
                    return 1

                # If the output is a string, we need to check it directly.
                if isinstance(ceph_version_output, str):
                    ceph_version = ceph_version_output
                else:
                    ceph_version = ceph_version_output.get("ceph")

                # Log the Ceph version
                log.info("Ceph Version being tested: {}".format(ceph_version))

                # Determine the Ceph release name
                ceph_version_name = get_ceph_version_name(ceph_version)
                log.info("Detected Ceph version name: {}".format(ceph_version_name))

                # Check if the Ceph version is from the Pacific release (version 16.x.x)
                if ceph_version.startswith(
                    "ceph version 16"
                ):  # Pacific release (e.g., ceph version 16.x.x)
                    log.info(
                        "Detected Pacific release. Applying workaround for defunct processes."
                    )
                    return apply_workaround_for_defunct_processes(mgr_obj, active_mgr)
                else:
                    log.error(
                        "Defunct processes should not be present in versions other than Pacific."
                    )
                    return 1

        # Step 4: Run pool sanity check
        log.info("Step 4: No defunct SSH processes found, verifying system health")
        if not rados_obj.run_pool_sanity_check():
            log.error("Health warnings detected in the cluster after failover.")
            return 1

    except Exception as e:
        log.error("An error occurred during the test: {}".format(str(e)))
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        # Step 5: Ensure No Crashes Occur During Testing and Check Health
        log.info("Step 5: Ensuring no crashes occurred during the testing")
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

        # Log cluster health again after testing to ensure no lingering issues
        rados_obj.log_cluster_health()

        log.info("Test completed successfully without issues or crashes")
        return 0


def apply_workaround_for_defunct_processes(mgr_obj, active_mgr):
    """
    Apply workaround to remove defunct SSH processes (only for Pacific release).
    """
    log.info("Applying workaround (WA) to remove defunct processes")
    result = mgr_obj.remove_mgr_service(host=active_mgr)  # Remove the active MGR
    if not result:
        log.error("Failed to remove MGR {}".format(active_mgr))
        return 1
    result = mgr_obj.add_mgr_service(host=active_mgr)  # Add it back
    if not result:
        log.error("Failed to add back MGR {}".format(active_mgr))
        return 1
    log.info("Workaround applied, verifying defunct processes again")
    try:
        defunct_processes_after_wa = (
            subprocess.check_output("ps aux | grep defunct | grep -v grep", shell=True)
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError as e:
        log.error(
            "Error occurred while checking defunct processes after workaround: {}".format(
                str(e)
            )
        )
        defunct_processes_after_wa = ""
    log.debug(
        "Output of grep for defunct processes after WA: {}".format(
            defunct_processes_after_wa
        )
    )
    if "defunct" in defunct_processes_after_wa:
        log.error(
            "Defunct processes still found after WA: {}".format(
                defunct_processes_after_wa
            )
        )
        return 1
    return 0


def perform_failover_cycle(mgr_obj, active_mgr, failover_cycle_num):
    """
    Perform a single failover cycle, which includes failing over the manager
    and verifying if the failover is successful.
    """
    log.info("Cycle {}/5: Initiating manager failover".format(failover_cycle_num))
    mgr_obj.set_mgr_fail(host=active_mgr)  # Fail the current active manager
    time.sleep(10)  # Ensure enough time for failover
    log.info("Cycle {}/5: Verifying the failover".format(failover_cycle_num))

    # Fetch the new active manager directly using get_active_mgr() method
    new_active_mgr = mgr_obj.get_active_mgr()
    log.info("New active manager is: {}".format(new_active_mgr))

    if active_mgr == new_active_mgr:
        log.error(
            "Cycle {}: Failover did not occur as expected".format(failover_cycle_num)
        )
        return 1

    return new_active_mgr


def get_ceph_version_name(ceph_version):
    """
    Determine the Ceph version name based on the version number.
    """
    if ceph_version.startswith("ceph version 16"):
        return "Pacific"
    elif ceph_version.startswith("ceph version 17"):
        return "Quincy"
    elif ceph_version.startswith("ceph version 18"):
        return "Reef"
    elif ceph_version.startswith("ceph version 19"):
        return "Squid"
    else:
        return "Unknown"
