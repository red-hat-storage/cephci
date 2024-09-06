"""
Module to verify libcephsqlite's ability to reopen a database connection if
current connection is down/blocklisted.
"""

import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83583664
    Covers:
        - BZ-2130867
        - BZ-2248719
    Test to verify reopening of database connection in case of database blocklisting
    MGR module - libcephsqlite and devicehealth
    Steps
    1. Deploy a ceph cluster
    2. Find the active client address and nonce of libcephsqlite from ceph mgr dump
    3. Choose an OSD at random and get the device id using osd metadata
    4. Run a ceph device command to invoke device health module and utilize libcephsqlite
    5. Capture the value of libcephsqlite address and nonce, should be same as before
    6. Enable logging to file and set debug level for mgr and debug_cephsqlite
    7. Add the MGR address/nonce to OSD blocklist
    8. Run the ceph device command again, should NOT result in an error as a new connection
     should automatically be established with libcephsqlite realizes that current active connection was down.
    9. Capture the value of libcephsqlite address and nonce, nonce should NOT be same as before
    10. Check MGR health status and cluster health
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    client = ceph_cluster.get_nodes(role="client")[0]
    active_mgr = ""
    reconnect_log_list = []
    log.info(
        "Running test case to verify reopening of database connection in case of database blocklisting"
    )

    def get_libcephsqlite_addr():
        mgr_dump = rados_obj.run_ceph_command(cmd="ceph mgr dump", client_exec=True)
        active_clients = mgr_dump["active_clients"]
        nonlocal active_mgr
        active_mgr = mgr_dump["active_name"]
        for client in active_clients:
            if client["name"] == "libcephsqlite":
                addr = client["addrvec"][0]["addr"]
                nonce = client["addrvec"][0]["nonce"]
                return addr, nonce
        raise Exception("libcephsqlite address and nonce could not be listed")

    try:
        if rhbuild.startswith("5"):
            log.info("Test is not valid for Pacific, BZ yet to be fixed/raised")
            return 0

        # fetch the current value of address and nonce of libcephsqlite from ceph mgr dump
        init_cephsqlite_addr, init_cephsqlite_nonce = get_libcephsqlite_addr()
        log.info(
            f"Value of libcephsqlite address and nonce is {init_cephsqlite_addr} "
            f"and {init_cephsqlite_nonce} respectively"
        )

        # Fetch list of OSDs in the cluster
        out, _ = cephadm.shell(args=["ceph osd ls"])
        osd_list = out.strip().split("\n")
        log.debug(f"List of OSDs: \n{osd_list}")

        # Choose an OSD at random
        osd_id = random.choice(osd_list)
        osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        log.info(f"Chosen OSD: {osd_id} on host {osd_host}")

        # From the osd metadata, get the device id
        osd_metadata = ceph_cluster.get_osd_metadata(osd_id=int(osd_id), client=client)
        log.debug(f"OSD.{osd_id} metadata: \n {osd_metadata}")
        device_id = osd_metadata["device_ids"].split("=")[-1]

        # Execute the device module command
        device_module_cmd = f"ceph device get-health-metrics {device_id}"
        out, _ = cephadm.shell(args=[device_module_cmd])
        log.info(out)

        # fetch the current value of address and nonce of libcephsqlite from ceph mgr dump
        cephsqlite_addr, cephsqlite_nonce = get_libcephsqlite_addr()
        log.info(
            f"Value of libcephsqlite address and nonce is {cephsqlite_addr} "
            f"and {cephsqlite_nonce} respectively"
        )
        assert cephsqlite_addr == init_cephsqlite_addr
        assert cephsqlite_nonce == init_cephsqlite_nonce

        # Enable granular logging for mgr and cephsqlite
        assert mon_obj.set_config(section="mgr", name="debug_cephsqlite", value="10/10")
        assert mon_obj.set_config(section="mgr", name="debug_mgr", value="10/10")
        log.info(
            "Logging level for debug_cephsqlite and debug_mgr set to 10/10 successfully"
        )

        # log capturing start time
        mgr_node = rados_obj.fetch_host_node(daemon_type="mgr", daemon_id=active_mgr)
        start_time, _ = mgr_node.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")

        # blocklist the current active libcephsqlite address
        log.info(
            f"Blocklisting libcephsqlite address {cephsqlite_addr}/{cephsqlite_nonce}"
        )
        rados_obj.add_client_blocklisting(ip=f"{cephsqlite_addr}/{cephsqlite_nonce}")

        log.info(
            "With libcephsqlite's address already added to blocklist, execute device health command again"
        )
        device_module_cmd = f"ceph device get-health-metrics {device_id}"
        out, _ = cephadm.shell(args=[device_module_cmd])
        log.info(out)

        assert "Error" not in out or "sqlite3.OperationalError" not in out
        log.info(
            "device command executed successfully even after blocklisting (expected)"
        )

        time.sleep(10)
        # get the new updated address for libcephsqlite, nonce should get updated
        final_cephsqlite_addr, final_cephsqlite_nonce = get_libcephsqlite_addr()
        log.info(
            f"Value of libcephsqlite address and nonce after client blocklisting and reopening of database "
            f"is {final_cephsqlite_addr} "
            f"and {final_cephsqlite_nonce} respectively"
        )
        assert final_cephsqlite_addr == init_cephsqlite_addr
        assert final_cephsqlite_nonce != init_cephsqlite_nonce
        log.info(
            f"libcephsqlite's nonce has changed from {init_cephsqlite_nonce} to "
            f"{final_cephsqlite_nonce} which indicates reopening of database connection"
        )

        # log capturing stop time
        stop_time, _ = mgr_node.exec_command(cmd="sudo date '+%Y-%m-%d %H:%M:%S'")

        mgr_log = rados_obj.get_journalctl_log(
            start_time=start_time,
            end_time=stop_time,
            daemon_type="mgr",
            daemon_id=active_mgr,
        )

        log_entires = [
            "maybe_reconnect: reconnecting to RADOS",
            "completed connection to RADOS with address",
            "attempting reopen of database",
            "ms_handle_authentication new session",
            "OPEN_CREATE",
            "create: main.db:",
            "open: main.db:",
        ]

        for log_entry in log_entires:
            for line in mgr_log.splitlines():
                if log_entry in line:
                    reconnect_log_list.append(line)
                    break

        reconnect_log = "\n".join(reconnect_log_list)
        log.info(
            f"\n ==========================================================================="
            f"\n MGR log lines describing database reconnection: \n {reconnect_log}"
            f"\n ==========================================================================="
        )

        # Ensure cluster is in healthy state
        cluster_health, _ = cephadm.shell(args=["ceph health detail"])
        log.info(f"Cluster health: {cluster_health}")
        assert "HEALTH_ERR" not in cluster_health
        assert "devicehealth" not in cluster_health

        log.info("Database reconnection verification complete")
    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        mon_obj.remove_config(section="mgr", name="debug_mgr")
        mon_obj.remove_config(section="mgr", name="debug_cephsqlite")
        rados_obj.rm_client_blocklisting(ip=f"{cephsqlite_addr}/{cephsqlite_nonce}")
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Verification of Database reconnection for libcephsqlite completed")
    return 0
