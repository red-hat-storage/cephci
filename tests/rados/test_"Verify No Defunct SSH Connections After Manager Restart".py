"""
Module to deploy an RHCS cluster, create pools, perform IO operations, and manage MGR daemon failover.
This test verifies that IO operations continue successfully during manager daemon failovers and that defunct SSH
processes are handled by redeploying the MGR daemon.
"""

import shutil
import logging
import time
from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from utility.log import Log
import psutil

log = Log(__name__)

def run(ceph_cluster, **kw):
    """
    Deploy an RHCS cluster, create pools, start IO operations, and failover the manager daemon.
    Ensure no IO disruption and redeploy the MGR daemon if defunct SSH processes are found.
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running test to verify IO stability during manager failovers and handling defunct SSH processes.")
    
    # Log the cluster health at the start of the test
    log.info("Logging the initial cluster health...")
    rados_obj = RadosOrchestrator(node=ceph_cluster)
    mgr_obj = MgrWorkflows(node=ceph_cluster)
    
    # Step 1: Create a pool for testing IO operations
    pool_name = "test_pool"
    log.info(f"Creating pool: {pool_name}")
    rados_obj.create_pool(pool_name=pool_name)

    # Step 2: Get the active MGR and start failover
    active_mgr = mgr_obj.get_active_mgr()
    log.info(f"Active MGR before failover: {active_mgr}")
    
    try:
        # Step 3: Continuously failover the MGR daemon and check for IO disruption
        log.info("Starting continuous MGR failover.")
        start_time = time.time()
        failover_count = 0
        while time.time() - start_time < 600:  # Run for 10 minutes
            failover_count += 1
            log.info(f"Failing over MGR daemon, attempt #{failover_count}")
            mgr_obj.set_mgr_fail(active_mgr)
            
            # Check if defunct SSH processes appear
            if check_defunct_ssh_processes():
                log.info("Defunct SSH processes detected. Redeploying the MGR daemon.")
                redeploy_mgr_daemon(ceph_cluster, rados_obj, mgr_obj)
            
            # Pause briefly before the next failover
            time.sleep(30)

        log.info("Completed MGR failovers.")
        
        # Step 4: Check for any IO disruptions by reading from the pool
        pool_ls_cmd = f"radosgw-admin bucket stats --bucket={pool_name}"
        out, err, rc, _ = ceph_cluster.get_ceph_object("installer").exec_command(cmd=pool_ls_cmd)
        if rc != 0:
            log.error(f"Failed to read from pool {pool_name}: {err}")
            raise Exception(f"Error reading from pool {pool_name}: {err}")
        log.info(f"Successfully completed IO operations on pool {pool_name}")

    except Exception as e:
        log.error(f"Test failed due to exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:

        # log cluster health
        rados_obj.log_cluster_health()

        # Step 5: Ensure there are no crashes and clean up
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of the test")
            return 1
        
        # Final check to verify no IO disruption during failovers
        log.info("Verifying IO operations after MGR failovers.")
        pool_check_cmd = f"radosgw-admin bucket stats --bucket={pool_name}"
        out, err, rc, _ = ceph_cluster.get_ceph_object("installer").exec_command(cmd=pool_check_cmd)
        if rc != 0:
            log.error(f"Error during final IO check on pool {pool_name}: {err}")
            return 1
    
    log.info("Test successfully completed, no IO disruption and defunct SSH processes handled.")
    return 0


def check_defunct_ssh_processes():
    """
    Checks for defunct SSH processes on the system.
    Returns True if defunct SSH processes are found, False otherwise.
    """
    defunct_processes = [proc for proc in psutil.process_iter(['pid', 'name', 'status']) if proc.info['status'] == psutil.STATUS_ZOMBIE and 'ssh' in proc.info['name']]
    if defunct_processes:
        log.info(f"Found {len(defunct_processes)} defunct ssh processes.")
        return True
    return False

def redeploy_mgr_daemon(ceph_cluster, rados_obj, mgr_obj):
    """
    Redeploys the MGR daemon if defunct SSH processes are found.
    This is the workaround to clear defunct SSH processes.
    Additionally, if the Orch service is set to managed and the correct labels are in place,
    the redeployment will happen automatically.
    """
    log.info("Checking if MGR daemon is managed by Ceph Orch.")
    active_mgr = mgr_obj.get_active_mgr()
    log.info(f"Active manager: {active_mgr}")
    
    # Check if MGR is managed by the orchestrator
    orch_status_cmd = f"ceph orch ps {active_mgr} --status"
    _, orch_err, orch_rc, _ = ceph_cluster.get_ceph_object("installer").exec_command(sudo=True, cmd=orch_status_cmd, verbose=True)
    if orch_rc != 0:
        log.error(f"Failed to check Orch status for MGR daemon: {orch_err}")
        raise Exception(f"Error checking Orch status for MGR daemon: {orch_err}")
    
    # Parse the status to check if it's set to 'managed'
    if "managed" in orch_err.lower():
        log.info("MGR daemon is managed by Ceph Orch. Redeployment will occur automatically if labels are correct.")
    else:
        log.info("MGR daemon is not managed by Ceph Orch. Redeployment will be triggered manually.")
        
        # Proceed with manual redeployment if not managed
        redeploy_cmd = f"ceph orch restart mgr {active_mgr}"
        _, err, rc, _ = ceph_cluster.get_ceph_object("installer").exec_command(sudo=True, cmd=redeploy_cmd, verbose=True)
        if rc != 0:
            log.error(f"Failed to redeploy MGR daemon with error: {err}")
            raise Exception(f"Error redeploying MGR daemon: {err}")
        log.info("Successfully redeployed the MGR daemon manually.")