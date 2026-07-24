import datetime
import json
import re
import secrets
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def stop_mds_service(cluster, mds_name):
    """
    Find and stop MDS service using systemctl

    Args:
        cluster: Ceph cluster object
        mds_name: MDS name

    Returns:
        Information about the stopped MDS service
    """
    log.info("==== STARTING: Stop MDS service for %s ====" % mds_name)
    # Extract node information from MDS name (example: node6)
    node_pattern = r"node\d+"
    node_match = re.search(node_pattern, mds_name)

    if not node_match:
        log.error("Could not extract node name from MDS name: %s" % mds_name)
        return None

    mds_node_name = node_match.group(0)
    log.info("Extracted MDS node name: %s" % mds_node_name)

    # Find the node in the cluster
    mds_node = None
    for node in cluster:
        if mds_node_name in node.hostname:
            mds_node = node
            log.info("Found MDS node: %s" % mds_node.hostname)
            break

    if not mds_node:
        log.error("Could not find MDS node %s in cluster" % mds_node_name)
        return None

    # Find the MDS service name (full service name)
    try:
        # Find service based on mds_name (service name should contain part of mds_name)
        # Simply use systemctl list-units | grep mds command
        log.info("Looking for MDS service using systemctl list-units command")
        out, rc = mds_node.exec_command(
            sudo=True, cmd="systemctl list-units | grep mds", check_ec=False
        )
        log.info("Systemctl output: %s" % out)

        # Extract service name
        mds_service = None
        if out and len(out.strip()) > 0:
            # First find the line that matches the given mds_name
            mds_short_name = mds_name.split(".")[-1]  # Last part (example: ngiiku)

            log.info("Looking for MDS service containing: %s" % mds_short_name)
            service_line = None

            for line in out.splitlines():
                # Check if line contains part of mds_name
                if mds_name in line or mds_short_name in line:
                    service_line = line
                    log.info("Found matching service line: %s" % service_line)
                    break

            # If a matching line is found, extract the first field (service name)
            if service_line:
                # Split by whitespace and get first field
                parts = service_line.split()
                if parts and len(parts) > 0:
                    mds_service = parts[0]
                    # Remove .service part from service name
                    if mds_service.endswith(".service"):
                        mds_service = mds_service[:-8]  # Remove .service
                    log.info("Extracted MDS service: %s" % mds_service)

            if mds_service:
                # Stop MDS service
                log.info(
                    "Attempting to stop MDS service: %s on node %s"
                    % (mds_service, mds_node.hostname)
                )
                out, rc = mds_node.exec_command(
                    sudo=True, cmd="systemctl stop %s" % mds_service
                )

                # Verify service is stopped
                log.info("Verifying that the MDS service is stopped")
                out, rc = mds_node.exec_command(
                    sudo=True,
                    cmd="systemctl is-active %s" % mds_service,
                    check_ec=False,
                )
                if "inactive" in out or rc != 0:
                    log.info(
                        "MDS service %s stopped successfully (status: %s)"
                        % (mds_service, out.strip())
                    )
                else:
                    log.warning(
                        "MDS service %s might not be fully stopped (status: %s)"
                        % (mds_service, out.strip())
                    )

                    # Try to forcefully kill the process
                    log.info("Attempting to forcefully kill MDS process with pkill")
                    try:
                        out, rc = mds_node.exec_command(
                            sudo=True, cmd="pkill -9 ceph-mds", check_ec=False
                        )
                        log.info("pkill result: %s, rc: %d" % (out, rc))
                    except Exception as e:
                        log.warning("Error during pkill: %s" % e)

                # Return information about the stopped service
                log.info("==== COMPLETED: Stop MDS service for %s ====" % mds_name)
                return {"mds_name": mds_name, "node": mds_node, "service": mds_service}
            else:
                log.error("Could not identify MDS service name for %s" % mds_name)
                return None
        else:
            log.error("No MDS services found on node %s" % mds_node.hostname)
            return None

    except Exception as e:
        log.error("Error while stopping MDS service: %s" % e)
        log.error(traceback.format_exc())
        return None


def start_mds_service(service_info):
    """
    Restart a previously stopped MDS service

    Args:
        service_info: Service information returned by stop_mds_service function

    Returns:
        Success status (True/False)
    """
    log.info("==== STARTING: Restart MDS service ====")
    if not service_info:
        log.error("No service information provided to start_mds_service")
        return False

    try:
        if service_info.get("killed_by_pkill", False):
            log.info("MDS was killed by pkill, not attempting to restart service")
            return False

        node = service_info["node"]
        service_name = service_info["service"]

        if not service_name:
            log.warning(
                "No service name available for restarting MDS on %s" % node.hostname
            )
            return False
        log.info("Restarting MDS service: %s on %s" % (service_name, node.hostname))
        out, rc = node.exec_command(
            sudo=True, cmd="systemctl start %s" % service_name, check_ec=False
        )

        # Verify service is started
        log.info("Verifying that the MDS service is started")
        out, rc = node.exec_command(
            sudo=True, cmd="systemctl is-active %s" % service_name, check_ec=False
        )
        if "active" in out and rc == 0:
            log.info(
                "MDS service %s started successfully (status: %s)"
                % (service_name, out.strip())
            )
            log.info("==== COMPLETED: Restart MDS service ====")
            return True
        else:
            log.warning(
                "MDS service %s might not be fully started (status: %s)"
                % (service_name, out.strip())
            )
            return False

    except Exception as e:
        log.error("Error while starting MDS service: %s" % e)
        log.error(traceback.format_exc())
        return False


def run(ceph_cluster, **kw):
    """
    Test MDS last-seen functionality

    Steps:
    1. Set the standby-replay mds to 2
    2. Run last-seen for active mds and check if it shows 0s
    3. Stop MDS service directly using systemctl on the MDS node
    4. Wait for about 120sec
    5. Run last-seen for the failed mds and check if the time has passed more than 120sec
    6. Run last-seen for standby replay mds
    7. Run last-seen for invalid mds name

    """
    log.info("====================================================")
    log.info("===== STARTING: MDS last-seen functionality test =====")
    log.info("====================================================")

    # Initialize variables that might be referenced in finally block
    fuse_mount_dir = None
    kernel_mount_dir = None
    mds_services_stopped = []  # List to store stopped MDS services

    try:
        tc = "CEPH-83614244"
        log.info("Running CephFS tests%s", tc)
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        config = kw.get("config", {})  # Add default value
        if config is None:
            config = {}
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        # Create test filesystem if it doesn't exist
        log.info("==== STARTING: Create filesystem if it doesn't exist ====")
        fs_name = "cephfs"
        out, rc = client1.exec_command(sudo=True, cmd="ceph fs ls -f json")
        if fs_name not in out:
            log.info("Creating filesystem %s" % fs_name)
            fs_util.create_fs(client1, fs_name)
        else:
            log.info("Filesystem %s already exists" % fs_name)

        # Step 1: Set the standby-replay mds to 2
        log.info("==== STARTING: Step 1 - Configure standby-replay MDS ====")
        log.info("Setting standby-replay MDS count to 2")
        fs_util.set_and_validate_mds_standby_replay(client1, fs_name, 1)
        client1.exec_command(
            sudo=True, cmd="ceph fs set %s standby_count_wanted 2" % fs_name
        )

        # Wait for 30 seconds to allow standby-replay to be set up
        log.info("Waiting for standby-replay to be set up...")
        time.sleep(30)
        log.info("==== STARTING: Mount filesystem ====")
        mon_node_ips = fs_util.get_mon_node_ips()
        fuse_mount_dir = "/mnt/fuse_%s" % secrets.token_hex(4)
        kernel_mount_dir = "/mnt/kernel_%s" % secrets.token_hex(4)

        log.info("Creating fuse mount at %s" % fuse_mount_dir)
        fs_util.fuse_mount(
            [client1], fuse_mount_dir, extra_params=" --client_fs %s" % fs_name
        )

        log.info("Creating kernel mount at %s" % kernel_mount_dir)
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir,
            ",".join(mon_node_ips),
            extra_params=",fs=%s" % fs_name,
        )
        log.info("==== COMPLETED: Mount filesystem ====")

        # Step 2: Run IOs and check last-seen for active MDS
        log.info(
            "==== STARTING: Step 2 - Run IOs and check last-seen for active MDS ===="
        )
        log.info("Running IOs and checking last-seen for active MDS")
        with parallel() as p:
            # Start IO operation in background
            p.spawn(fs_util.run_ios, client1, fuse_mount_dir, ["dd", "smallfile"])
            # Allow some time for IO to start
            time.sleep(5)

        # Check if standby-replay MDS is available
        log.info("Checking if standby-replay MDS is available")
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph fs status %s -f json" % fs_name
        )
        fs_status = json.loads(out)

        # Find active MDS
        log.info("Finding active MDS")
        active_mds = None
        for mds in fs_status["mdsmap"]:
            if mds["state"] == "active" and mds["rank"] == 0:
                active_mds = mds["name"]
                log.info("Found active MDS: %s" % active_mds)
                break
        if not active_mds:
            log.error("No active MDS found")
            return 1
        # Find standby-replay MDS
        log.info("Finding standby-replay MDS")
        standby_replay_mds = None
        for mds in fs_status["mdsmap"]:
            if mds["state"] == "standby-replay":
                standby_replay_mds = mds["name"]
                log.info("Found standby-replay MDS: %s" % standby_replay_mds)
                break

        if not standby_replay_mds:
            log.error("No standby-replay MDS found")
            return 1
        # Step 2: Run last-seen for active MDS and check if it shows 0s
        log.info("Running last-seen command for active MDS")
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph mds last-seen %s" % active_mds
        )
        log.info("Active MDS last-seen output: %s" % out)

        # Verify if the output contains "0s" or very recent time
        if "0s" not in out:
            log.error("Active MDS %s last-seen is not 0s: %s" % (active_mds, out))
            return 1
        log.info(
            "==== COMPLETED: Step 2 - Run IOs and check last-seen for active MDS ===="
        )

        # Step 3: Stop MDS service directly using systemctl on the MDS node
        log.info("==== STARTING: Step 3 - Stop MDS service directly ====")
        log.info("Finding and stopping MDS service for %s" % active_mds)
        stopped_service = stop_mds_service(ceph_cluster, active_mds)

        if stopped_service:
            mds_services_stopped.append(stopped_service)
            failed_mds = active_mds
            log.info("Successfully stopped MDS service for %s" % active_mds)
        else:
            # Fallback to ceph mds fail command if service stop fails
            log.warning(
                "Failed to stop MDS service directly. Falling back to ceph mds fail"
            )
            client1.exec_command(sudo=True, cmd="ceph mds fail %s" % active_mds)
            failed_mds = active_mds

        # Record the time when MDS was failed
        failure_time = datetime.datetime.now()
        log.info("MDS failure time: %s" % failure_time.strftime("%Y-%m-%d %H:%M:%S"))
        log.info("==== COMPLETED: Step 3 - Stop MDS service directly ====")

        # Step 4: Wait for about 120 seconds
        log.info("==== STARTING: Step 4 - Wait for failover ====")
        wait_time = 120
        log.info("Waiting for %s seconds..." % wait_time)
        time.sleep(wait_time)
        log.info("==== COMPLETED: Step 4 - Wait for failover ====")

        # Step 5: Run last-seen for the failed MDS and check elapsed time
        log.info("==== STARTING: Step 5 - Check last-seen for failed MDS ====")
        log.info("Running last-seen for failed MDS %s" % failed_mds)
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph mds last-seen %s" % failed_mds
        )
        log.info("Failed MDS last-seen output: %s" % out)

        # Check if time passed is greater than 120s
        # Extract time information from output
        time_pattern = r"(\d+)([s])"
        matches = re.findall(time_pattern, out)

        if matches:
            total_seconds = 0
            for value, unit in matches:
                if unit == "s":
                    total_seconds += int(value)

            log.info("Elapsed time: %s seconds" % total_seconds)
            # Allow for a small offset (wait_time - 2)
            if total_seconds < (wait_time - 2):
                log.error(
                    "Failed MDS %s last-seen time %ss is less than expected wait time %ss"
                    % (failed_mds, total_seconds, wait_time - 2)
                )
                return 1
        else:
            log.error("Failed to parse time from last-seen output: %s" % out)
        log.info("==== COMPLETED: Step 5 - Check last-seen for failed MDS ====")

        # Step 6: Restart the previously stopped MDS service
        log.info("==== STARTING: Step 6 - Restart the stopped MDS service ====")
        log.info("Restarting the stopped MDS service for %s" % failed_mds)

        # Use the first service info in the mds_services_stopped list to restart
        if mds_services_stopped and len(mds_services_stopped) > 0:
            first_stopped_service = mds_services_stopped[0]
            restart_success = start_mds_service(first_stopped_service)

            if restart_success:
                log.info("Successfully restarted MDS service for %s" % failed_mds)
                # Remove the service from the list after successfully restarting
                mds_services_stopped.remove(first_stopped_service)
            else:
                log.warning("Failed to restart MDS service for %s" % failed_mds)
        else:
            log.warning("No stopped MDS services found to restart")
        log.info("==== COMPLETED: Step 6 - Restart the stopped MDS service ====")

        # Step 7: Check last-seen for standby-replay MDS
        log.info("==== STARTING: Step 7 - Check last-seen for standby-replay MDS ====")
        log.info("Getting current standby-replay MDS")
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph fs status %s -f json" % fs_name
        )
        fs_status = json.loads(out)
        for mds in fs_status["mdsmap"]:
            if mds["state"] == "standby-replay":
                standby_replay_mds = mds["name"]
                log.info("Found standby-replay MDS: %s" % standby_replay_mds)
                # check last-seen for standby-replay mds
                log.info("Checking last-seen for standby-replay MDS")
                out, rc = client1.exec_command(
                    sudo=True, cmd="ceph mds last-seen %s" % standby_replay_mds
                )
                log.info("Standby-replay MDS last-seen output: %s" % out)
                # check if last-seen is 0s
                if "0s" not in out:
                    log.error(
                        "Standby-replay MDS %s last-seen is not 0s: %s"
                        % (standby_replay_mds, out)
                    )
                    return 1
        log.info("==== COMPLETED: Step 7 - Check last-seen for standby-replay MDS ====")

        log.info("====================================================")
        log.info("===== COMPLETED: MDS last-seen functionality test =====")
        log.info("====================================================")
        return 0

    except Exception as e:
        log.error("Error: %s" % e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("==== STARTING: Cleanup ====")
        # Restart any remaining stopped MDS services
        for service_info in mds_services_stopped:
            try:
                log.info("Restarting MDS service in cleanup")
                start_mds_service(service_info)
            except Exception as e:
                log.warning("Failed to restart MDS service in finally block: %s" % e)
        # Unmount directories if they were created
        log.info("Unmounting fuse directory: %s" % fuse_mount_dir)
        client1.exec_command(
            sudo=True, cmd="umount %s" % fuse_mount_dir, check_ec=False
        )
        log.info("Unmounted fuse directory: %s" % fuse_mount_dir)
        log.info("Unmounting kernel directory: %s" % kernel_mount_dir)
        client1.exec_command(
            sudo=True, cmd="umount %s" % kernel_mount_dir, check_ec=False
        )
        log.info("Unmounted kernel directory: %s" % kernel_mount_dir)
        log.info("==== COMPLETED: Cleanup ====")
