import json
import random
import secrets
import signal
import string
import time
import traceback
from threading import Thread

from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def run_mds_signal_tests(
    fs_util_v1,
    active_mds_objects,
    filesystem_name,
    test_case,
):
    """
    Execute SIGHUP, SIGTERM, and SIGKILL signal tests on MDS daemons.

    This function sends signals to active MDS daemons and handles service
    restart for SIGTERM. It can be used for both source and target cluster MDS.

    Args:
        fs_util_v1: FsUtilsV1 object for the cluster
        active_mds_objects: List of active MDS node objects
        filesystem_name: Name of the filesystem (e.g., "cephfs" or "cephfs-ec")
        test_case: Dictionary containing test case information with:
            - "signal": signal.SIGHUP, signal.SIGTERM, or signal.SIGKILL
            - "name": Test name (e.g., "SIGHUP", "SIGTERM", "SIGKILL")
            - "expect_exit": Boolean indicating if the signal should cause process exit

    Returns:
        int: 0 if successful, 1 if there's an error
    """
    try:
        sig = test_case["signal"]
        test_name = test_case["name"]
        expect_exit = test_case["expect_exit"]

        log.info("Sending %s to MDS daemons", test_name)

        # Send signal to active MDS daemons
        for mds in active_mds_objects:
            log.info("Sending %s to MDS on host: %s", test_name, mds.hostname)
            fs_util_v1.pid_signal(mds, "mds", sig=sig, expect_exit=expect_exit, wait=10)
            # If SIGTERM was sent and expect_exit is True, restart the service
            if sig == signal.SIGTERM and expect_exit:
                log.info(
                    "Restarting MDS service after SIGTERM on host: %s", mds.hostname
                )
                service_name = fs_util_v1.deamon_name(mds, rf"mds\.{filesystem_name}\.")
                log.info("Starting MDS service: %s", service_name)
                mds.exec_command(
                    sudo=True,
                    cmd=f"systemctl start {service_name}",
                    check_ec=False,
                )
                # Verify service is started
                out, rc = mds.exec_command(
                    sudo=True,
                    cmd=f"systemctl is-active {service_name}",
                    check_ec=False,
                )
                if "active" in out:
                    log.info("MDS service %s started successfully", service_name)
                else:
                    log.warning(
                        "MDS service %s might not be active: %s", service_name, out
                    )

        # Wait for MDS to recover
        log.info("Waiting for MDS to recover after %s", test_name)
        time.sleep(30)

        return 0
    except Exception as e:
        log.error("Error during %s test: %s", test_name, e)
        log.error(traceback.format_exc())
        return 1


def run_mds_systemctl_restart_test(
    fs_util_v1,
    active_mds_objects,
    filesystem_name,
):
    """
    Execute systemctl restart test on MDS daemons.

    This function restarts MDS daemons using systemctl restart and can be used
    for both source and target cluster MDS.

    Args:
        fs_util_v1: FsUtilsV1 object for the cluster
        active_mds_objects: List of active MDS node objects
        filesystem_name: Name of the filesystem (e.g., "cephfs" or "cephfs-ec")

    Returns:
        int: 0 if successful, 1 if there's an error
    """
    try:
        log.info("Restarting MDS daemons using systemctl restart")

        # Restart MDS using systemctl restart
        for mds in active_mds_objects:
            log.info("Restarting MDS using systemctl on host: %s", mds.hostname)
            # Get MDS service name using deamon_name method
            service_name = fs_util_v1.deamon_name(mds, rf"mds\.{filesystem_name}\.")
            log.info("Restarting MDS service: %s", service_name)
            mds.exec_command(
                sudo=True,
                cmd=f"systemctl restart {service_name}",
            )

        # Wait for MDS to recover
        log.info("Waiting for MDS to recover after systemctl restart")
        time.sleep(30)

        return 0
    except Exception as e:
        log.error("Error during systemctl restart test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_mds_node_reboot_test(
    fs_util_v1,
    active_mds_objects,
):
    """
    Execute node reboot test on MDS nodes.

    This function reboots MDS nodes and can be used for both source and target cluster MDS.

    Args:
        fs_util_v1: FsUtilsV1 object for the cluster
        active_mds_objects: List of active MDS node objects

    Returns:
        int: 0 if successful, 1 if there's an error
    """
    try:
        log.info("Rebooting MDS nodes")
        for mds in active_mds_objects:
            log.info("Rebooting MDS node: %s", mds.hostname)
            fs_util_v1.reboot_node_v1(mds, timeout=300)
        # Wait for MDS to recover after reboot
        log.info("Waiting for MDS to recover after node reboot")
        time.sleep(60)

        return 0
    except Exception as e:
        log.error("Error during node reboot test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_mds_container_restart_test(
    active_mds_objects,
    filesystem_name,
):
    """
    Execute container restart test on MDS daemons using podman restart.

    This function restarts MDS containers using podman restart and can be used
    for both source and target cluster MDS.

    Args:
        active_mds_objects: List of active MDS node objects
        filesystem_name: Name of the filesystem (e.g., "cephfs" or "cephfs-ec")

    Returns:
        int: 0 if successful, 1 if there's an error
    """
    try:
        log.info("Restarting MDS containers using podman restart")
        for mds in active_mds_objects:
            log.info("Restarting MDS container on host: %s", mds.hostname)
            try:
                # Get MDS container ID using podman ps | grep mds
                out, _ = mds.exec_command(
                    sudo=True,
                    cmd="podman ps | grep mds",
                )
                # Extract container ID (first column)
                lines = out.strip().split("\n")
                for line in lines:
                    if "mds" in line and filesystem_name in line:
                        container_id = line.split()[0]
                        log.info(
                            "Restarting MDS container: %s on host: %s",
                            container_id,
                            mds.hostname,
                        )
                        mds.exec_command(
                            sudo=True,
                            cmd=f"podman restart {container_id}",
                        )
                        break
            except Exception as e:
                log.warning(
                    "Failed to restart MDS container on %s: %s", mds.hostname, e
                )
        # Wait for MDS to recover
        log.info("Waiting for MDS to recover after container restart")
        time.sleep(30)

        return 0
    except Exception as e:
        log.error("Error during container restart test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_mds_redeploy_test(
    client,
    filesystem_name,
    ceph_cluster_dict,
    cluster_key,
):
    """
    Execute MDS redeploy test by deploying MDS on different nodes.

    Args:
        client: Client node to execute commands on
        filesystem_name: Name of the filesystem (e.g., "cephfs" or "cephfs-ec")
        ceph_cluster_dict: Dictionary containing cluster objects
        cluster_key: Key for the cluster in ceph_cluster_dict (e.g., "ceph1" or "ceph2")

    Returns:
        int: 0 if successful, 1 if there's an error
    """
    # Get current MDS daemon hostnames
    log.info("Getting current MDS daemon hostnames")
    out, rc = client.exec_command(
        sudo=True,
        cmd="ceph orch ps --daemon_type=mds --format json",
        check_ec=False,
    )

    current_mds_hostnames = set()
    try:
        mds_data = json.loads(out)
        for daemon in mds_data:
            hostname = daemon.get("hostname")
            if hostname:
                current_mds_hostnames.add(hostname)
        log.info(
            "Current MDS daemons running on hosts: %s", list(current_mds_hostnames)
        )
    except json.JSONDecodeError as e:
        log.error("Failed to parse MDS daemon information: %s", e)
        return 1

    # Get all available MDS nodes
    mds_hosts = ceph_cluster_dict.get(cluster_key).get_nodes(role="mds")
    all_mds_hostnames = {node.hostname for node in mds_hosts}
    log.info("All available MDS nodes: %s", list(all_mds_hostnames))

    # Find nodes that are NOT currently running MDS
    available_for_deploy = all_mds_hostnames - current_mds_hostnames

    if not available_for_deploy:
        log.warning(
            "No available nodes for MDS redeploy. All MDS nodes are currently running MDS."
        )
        log.info("Skipping MDS redeploy test")
    else:
        log.info("Nodes available for MDS redeploy: %s", list(available_for_deploy))

        # Select nodes for redeploy (use first available node, or all if needed)
        nodes_to_deploy = list(available_for_deploy)[-len(current_mds_hostnames) :]
        log.info("Deploying MDS on nodes: %s", nodes_to_deploy)

        # Deploy MDS on selected nodes
        hosts_str = " ".join(nodes_to_deploy)
        deploy_cmd = f"ceph orch apply mds {filesystem_name} --placement='{len(current_mds_hostnames)} {hosts_str}'"
        log.info("Executing: %s", deploy_cmd)

        out, rc = client.exec_command(
            sudo=True,
            cmd=deploy_cmd,
            check_ec=False,
        )

        log.info("MDS deployment command executed successfully")

        # Wait for MDS to be deployed and become active
        log.info("Waiting for MDS to be deployed on new nodes")
        mds_process_name = f"mds.{filesystem_name}"
        if wait_for_process(
            client=client,
            process_name=mds_process_name,
            timeout=120,
            interval=10,
            ispresent=True,
        ):
            log.info("MDS redeploy completed successfully")
        else:
            log.warning("MDS deployment verification timeout")

    # Wait for MDS to stabilize
    log.info("Waiting for MDS to stabilize after redeploy")
    time.sleep(30)

    return 0


def run(ceph_cluster, **kw):
    """
    Test MDS failure operations during active mirroring to ensure IOs and snapshot sync continue.

    This test verifies that:
    1. Background IOs continue running during MDS operations
    2. Snapshot sync is not affected by MDS operations
    3. Cluster remains healthy after each operation
    4. Clients reconnect properly after MDS operations

    Test operations performed:
    - SIGTERM: Graceful termination signal
    - SIGHUP: Reload configuration signal
    - SIGKILL: Forceful kill signal
    - Systemctl Restart: Restart MDS using systemctl restart ceph-mds@<id>
    - Node Reboot: Reboot MDS nodes

    Args:
        ceph_cluster: The Ceph cluster to perform the mirroring tests on.
        **kw: Additional keyword arguments.

    Returns:
        int: 0 if the test is successful, 1 if there's an error.

    Raises:
        Exception: Any unexpected exceptions that might occur during the test.
    """

    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        snap_util = SnapUtils(ceph_cluster)
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"))
        fs_util_v1_ceph1 = FsUtilsV1(
            ceph_cluster_dict.get("ceph1"), test_data=test_data
        )
        fs_util_v1_ceph2 = FsUtilsV1(
            ceph_cluster_dict.get("ceph2"), test_data=test_data
        )
        fs_util_v1_ceph2 = FsUtilsV1(
            ceph_cluster_dict.get("ceph2"), test_data=test_data
        )
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        mds_hosts = ceph_cluster_dict.get("ceph1").get_nodes(role="mds")
        if not mds_nodes:
            log.error("No MDS nodes found in ceph1 cluster")
            return 1
        log.info("Found %s MDS node(s)", len(mds_nodes))

        target_mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        target_mds_hosts = ceph_cluster_dict.get("ceph2").get_nodes(role="mds")
        if not target_mds_nodes:
            log.error("No MDS nodes found in ceph2 cluster")
            return 1
        log.info("Found %s target MDS node(s)", len(target_mds_nodes))
        build = config.get("build", config.get("rhbuild"))
        num_of_mons = len(fs_util_v1_ceph1.get_mon_node_ips())
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        fs_util_v1_ceph1.setup_ssh_root_keys(clients=source_clients + target_clients)
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )
        num_of_osds = int(fs_util_ceph1.get_osd_count(source_clients[0]))
        # Test configuration
        io_runtime = config.get(
            "io_runtime", 40
        )  # Runtime for background IOs in minutes
        signal_tests = [
            {
                "type": "signal",
                "signal": signal.SIGHUP,
                "name": "SIGHUP",
                "expect_exit": False,
            },
            {
                "type": "signal",
                "signal": signal.SIGKILL,
                "name": "SIGKILL",
                "expect_exit": True,
            },
            {
                "type": "signal",
                "signal": signal.SIGTERM,
                "name": "SIGTERM",
                "expect_exit": True,
            },
            {"type": "systemctl_restart", "name": "MDS Systemctl Restart"},
            {"type": "container_restart", "name": "MDS Container Restart"},
            {"type": "node_reboot", "name": "MDS Node Reboot"},
            {"type": "mds_redeploy", "name": "MDS Redeploy"},
        ]

        log.info("Checking Pre-requisites")
        if not source_clients or not target_clients:
            log.error(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing Clients...")
        fs_util_v1_ceph1.prepare_clients(source_clients, build)
        fs_util_v1_ceph2.prepare_clients(target_clients, build)
        fs_util_v1_ceph1.auth_list(source_clients)
        fs_util_v1_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"

        log.info("Erasure coded pools enabled: %s", erasure)
        log.info("Source filesystem: %s, Target filesystem: %s", source_fs, target_fs)

        fs_details_source = fs_util_v1_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            log.info("Creating source filesystem: %s", source_fs)
            # Use FsUtilsV1 for erasure coded pool support
            fs_util_v1_ceph1.create_fs(source_clients[0], source_fs, validate=True)
        else:
            log.info("Source filesystem %s already exists", source_fs)

        fs_details_target = fs_util_v1_ceph1.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            log.info("Creating target filesystem: %s", target_fs)
            # Use FsUtilsV1 for erasure coded pool support
            fs_util_v1_ceph2.create_fs(target_clients[0], target_fs, validate=True)
        else:
            log.info("Target filesystem %s already exists", target_fs)

        target_user = "mirror_remote"
        target_site_name = "remote_site"
        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        log.info("Create Subvolumes for adding data")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_v1_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_kernel_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_kernel_2",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_fuse_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_fuse_2",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_nfs_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_nfs_2",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
        ]

        for subvolume in subvolume_list:
            fs_util_v1_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{mounting_dir}_1/"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel_{mounting_dir}_2/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{mounting_dir}_2/"
        nfs_mounting_dir_1 = f"/mnt/cephfs_nfs_{mounting_dir}_1/"
        nfs_mounting_dir_2 = f"/mnt/cephfs_nfs_{mounting_dir}_2/"
        mon_node_ips = fs_util_v1_ceph1.get_mon_node_ips()
        subvol_list = [
            d.get("subvol_name")
            for d in subvolume_list
            if d.get("subvol_name") is not None
        ]
        subvolume_paths = []
        for subvol in subvol_list:
            log.info(
                "Get the path of %s on filesystem and add them for mirroring to remote location",
                subvol,
            )
            subvol_path, rc = source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {source_fs} {subvol} subvolgroup_1",
            )
            index = subvol_path.find(f"{subvol}/")
            if index != -1:
                subvol_path = subvol_path[: index + len(f"{subvol}/")]
            else:
                subvol_path = subvol_path
            log.info(subvol_path)
            log.info("Add subvolumes for mirroring to remote location")
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )
            subvolume_paths.append(f"{subvol_path}")

        mounting_dirs = [
            kernel_mounting_dir_1,
            kernel_mounting_dir_2,
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
            nfs_mounting_dir_1,
            nfs_mounting_dir_2,
        ]

        full_subvolume_path = []
        for mount_dir, subvol in zip(mounting_dirs, subvolume_paths):
            if "nfs" in mount_dir:
                full_subvolume_path.append(f"{mount_dir}")
            else:
                full_subvolume_path.append(f"{mount_dir}{subvol}")

        source_nfs_servers = ceph_cluster_dict.get("ceph1").get_ceph_objects("nfs")
        source_nfs_server = source_nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        log.info("Create NFS cluster and NFS export on source cluster")

        log.info("Enable NFS module on source cluster")
        source_clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")

        log.info("Create nfs cluster on source cluster")
        log.info("NFS cluster name: %s", nfs_name)
        log.info("NFS server: %s", source_nfs_server)

        # Create NFS cluster using Ceph API from cli/ceph/nfs/cluster/cluster.py
        log.info(
            "Creating NFS cluster using Ceph API: %s on '%s'",
            nfs_name,
            source_nfs_server,
        )

        fs_util_v1_ceph1.create_nfs(
            source_clients[0],
            nfs_cluster_name=nfs_name,
            nfs_server_name=source_nfs_server,
        )
        log.info("Checking if NFS cluster process is running...")
        if wait_for_process(
            client=source_clients[0], process_name=nfs_name, ispresent=True
        ):
            log.info("ceph nfs cluster created successfully on source cluster")
        else:
            log.error("NFS cluster process not found after creation")
        source_nfs_export_name_1 = "/export_source_nfs_1_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        source_nfs_export_name_2 = "/export_source_nfs_2_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        log.info("Create nfs exports on source cluster")
        for export_name, subvol_path in zip(
            [source_nfs_export_name_1, source_nfs_export_name_2], subvolume_paths[-2:]
        ):
            fs_util_v1_ceph1.create_nfs_export(
                source_clients[0],
                nfs_name,
                export_name,
                source_fs,
                path=subvol_path,
            )
            log.info("Export %s on source cluster created successfully.", export_name)
        log.info("Mounting FS as kernel, fuse and NFS mounts")
        for mount_dir in mounting_dirs:
            if "kernel" in mount_dir:
                fs_util_v1_ceph1.kernel_mount(
                    [source_clients[0]],
                    mount_dir,
                    ",".join(mon_node_ips),
                    extra_params=f",fs={source_fs}",
                )
            elif "fuse" in mount_dir:
                fs_util_v1_ceph1.fuse_mount(
                    [source_clients[0]],
                    mount_dir,
                    extra_params=f" --client_fs {source_fs}",
                )
            elif mount_dir == nfs_mounting_dir_1:
                rc = fs_util_v1_ceph1.cephfs_nfs_mount(
                    source_clients[0],
                    source_nfs_server,
                    source_nfs_export_name_1,
                    mount_dir,
                )
                if not rc:
                    log.error(
                        "cephfs nfs export %s mount failed", source_nfs_export_name_1
                    )
                    return 1
            else:
                rc = fs_util_v1_ceph1.cephfs_nfs_mount(
                    source_clients[0],
                    source_nfs_server,
                    source_nfs_export_name_2,
                    mount_dir,
                )
                if not rc:
                    log.error(
                        "cephfs nfs export %s mount failed", source_nfs_export_name_2
                    )
                    return 1

        log.info("Enable snap schedule on primary cluster")
        snap_util.enable_snap_schedule(source_clients[0])
        time.sleep(10)
        log.info("Allow minutely Snap Schedule on primary cluster")
        snap_util.allow_minutely_schedule(source_clients[0], allow=True)
        time.sleep(10)

        log.info(
            "Creating snapshot schedules for subvolume paths with a 1-minute interval"
        )
        interval = "1m"
        for path in subvolume_paths:
            snap_params = {
                "client": source_clients[0],
                "path": path,
                "sched": interval,
                "fs_name": source_fs,
                "validate": True,
            }
            result_1 = snap_util.create_snap_schedule(snap_params)
            if result_1 != 0:
                log.error("Failed to create snapshot schedule for %s", snap_params)
                return 1
            log.info(
                "Snapshot schedules for subvolume path %s created successfully.", path
            )

            sched_list1 = snap_util.get_snap_schedule_list(
                source_clients[0], path, source_fs
            )
            log.info("Snap Schedule list for %s : %s", path, sched_list1)

        time.sleep(60)
        log.info("Wait for 60 secs for snapshot creation via snap schedule")

        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info("fsid on ceph cluster : %s", fsid)
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        log.info("Name of the cephfs-mirror daemon : %s", daemon_name)
        asok_file = fs_mirroring_utils.get_asok_file_with_connectivity_check(
            cephfs_mirror_node, fsid, daemon_name
        )
        if not asok_file:
            return 1
        log.info("Admin Socket file of cephfs-mirror daemon : %s", asok_file)
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info("filesystem id of %s is : %s", source_fs, filesystem_id)
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info("peer uuid of %s is : %s", source_fs, peer_uuid)

        log.info(f"Starting background IOs for {io_runtime} minutes")
        io_threads = start_background_ios(
            fs_util_v1_ceph1, source_clients[0], mounting_dirs, io_runtime
        )

        # Give IO a moment to actually start
        time.sleep(15)
        log.info("Verifying IO threads are running")
        for idx, t in enumerate(io_threads):
            log.info("IO thread %s alive: %s", idx, t.is_alive())

        # Get initial snapshot sync counts
        snap_sync_counts_initial = []
        for path in subvolume_paths:
            snaps_synced = fs_mirroring_utils.get_snaps_synced(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_counts_initial.append(snaps_synced)
            log.info("Initial snap sync count for %s: %s", path, snaps_synced)

        # Run all tests (signals, mds redeploy, node reboot)
        for test_case in signal_tests:
            test_type = test_case["type"]
            test_name = test_case["name"]

            log.info("=== Starting %s test ===", test_name)

            # Get baseline snapshot sync counts before test
            snap_sync_counts_before = []
            for path in subvolume_paths:
                snaps_synced = fs_mirroring_utils.get_snaps_synced(
                    source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
                )
                snap_sync_counts_before.append(snaps_synced)

            # Check cluster health before test
            cluster_health_before = check_ceph_healthly(
                source_clients[0],
                num_of_osds,
                num_of_mons,
                build,
                None,
                30,
            )
            log.info("Cluster health before %s: %s", test_name, cluster_health_before)

            # Verify IOs are still running
            log.info("Verifying IO threads are still running before %s", test_name)
            for idx, t in enumerate(io_threads):
                if not t.is_alive():
                    log.error("IO thread %s died before %s test", idx, test_name)
                    return 1
                log.info("IO thread %s alive: %s", idx, t.is_alive())

            try:
                # Get active MDS nodes for source cluster
                active_mds = fs_util_v1_ceph1.get_active_mdss(
                    source_clients[0], source_fs
                )
                active_mds_objects = [
                    mds
                    for mds in mds_hosts
                    if any(mds.hostname in active for active in active_mds)
                ]
                log.info(
                    "Active MDS Nodes are: %s",
                    [mds.hostname for mds in active_mds_objects],
                )

                if not active_mds_objects:
                    log.error("No active MDS nodes found")
                    return 1

                # Get active MDS nodes for target cluster
                target_active_mds = fs_util_v1_ceph2.get_active_mdss(
                    target_clients[0], target_fs
                )
                target_active_mds_objects = [
                    mds
                    for mds in target_mds_hosts
                    if any(mds.hostname in active for active in target_active_mds)
                ]
                log.info(
                    "Active target MDS Nodes are: %s",
                    [mds.hostname for mds in target_active_mds_objects],
                )

                if not target_active_mds_objects:
                    log.error("No active target MDS nodes found")
                    return 1

                # Perform the test operation based on test type
                if test_type == "signal":
                    # Run signal tests on source cluster MDS
                    log.info("Running signal test on source cluster MDS")
                    result = run_mds_signal_tests(
                        fs_util_v1_ceph1,
                        active_mds_objects,
                        source_fs,
                        test_case,
                    )
                    if result != 0:
                        return 1

                    # Run signal tests on target cluster MDS
                    log.info("Running signal test on target cluster MDS")
                    result = run_mds_signal_tests(
                        fs_util_v1_ceph2,
                        target_active_mds_objects,
                        target_fs,
                        test_case,
                    )
                    if result != 0:
                        return 1

                elif test_type == "systemctl_restart":
                    # Run systemctl restart test on source cluster MDS
                    log.info("Running systemctl restart test on source cluster MDS")
                    result = run_mds_systemctl_restart_test(
                        fs_util_v1_ceph1,
                        active_mds_objects,
                        source_fs,
                    )
                    if result != 0:
                        return 1

                    # Run systemctl restart test on target cluster MDS
                    log.info("Running systemctl restart test on target cluster MDS")
                    result = run_mds_systemctl_restart_test(
                        fs_util_v1_ceph2,
                        target_active_mds_objects,
                        target_fs,
                    )
                    if result != 0:
                        return 1

                elif test_type == "container_restart":
                    # Run container restart test on source cluster MDS
                    log.info("Running container restart test on source cluster MDS")
                    result = run_mds_container_restart_test(
                        active_mds_objects,
                        source_fs,
                    )
                    if result != 0:
                        return 1

                    # Run container restart test on target cluster MDS
                    log.info("Running container restart test on target cluster MDS")
                    result = run_mds_container_restart_test(
                        target_active_mds_objects,
                        target_fs,
                    )
                    if result != 0:
                        return 1

                elif test_type == "node_reboot":
                    # Run node reboot test on source cluster MDS
                    log.info("Running node reboot test on source cluster MDS")
                    result = run_mds_node_reboot_test(
                        fs_util_v1_ceph1,
                        active_mds_objects,
                    )
                    if result != 0:
                        return 1

                    # Run node reboot test on target cluster MDS
                    log.info("Running node reboot test on target cluster MDS")
                    result = run_mds_node_reboot_test(
                        fs_util_v1_ceph2,
                        target_active_mds_objects,
                    )
                    if result != 0:
                        return 1

                elif test_type == "mds_redeploy":
                    # Run MDS redeploy test on source cluster
                    log.info("Running MDS redeploy test on source cluster")
                    result = run_mds_redeploy_test(
                        source_clients[0],
                        source_fs,
                        ceph_cluster_dict,
                        "ceph1",
                    )
                    if result != 0:
                        return 1

                    # Run MDS redeploy test on target cluster
                    log.info("Running MDS redeploy test on target cluster")
                    result = run_mds_redeploy_test(
                        target_clients[0],
                        target_fs,
                        ceph_cluster_dict,
                        "ceph2",
                    )
                    if result != 0:
                        return 1

                else:
                    log.error("Unknown test type: %s", test_type)
                    return 1

            except Exception as e:
                log.error("Error during %s test: %s", test_name, e)
                log.error(traceback.format_exc())
                return 1

            # Check cluster health after test
            # Ensure mds_nodes is not empty to avoid index out of range errors
            mds_count = len(mds_nodes) if mds_nodes else 0
            if mds_count == 0:
                log.warning("No MDS nodes available, skipping cluster health check")
                cluster_health_after = "unknown"
            else:
                cluster_health_after = check_ceph_healthly(
                    source_clients[0],
                    num_of_osds,
                    num_of_mons,
                    build,
                    None,
                    30,
                )
            log.info("Cluster health after %s: %s", test_name, cluster_health_after)

            if cluster_health_after == cluster_health_before:
                log.info("Cluster is healthy after %s", test_name)
            else:
                log.error("Cluster is not healthy after %s", test_name)
                log.error(
                    "Health before: %s, Health after: %s",
                    cluster_health_before,
                    cluster_health_after,
                )
                return 1

            # Verify IOs are still running after test
            log.info("Verifying IO threads are still running after %s", test_name)
            for idx, t in enumerate(io_threads):
                if not t.is_alive():
                    log.error("IO thread %s died after %s", idx, test_name)
                    return 1
                log.info("IO thread %s alive: %s", idx, t.is_alive())

            # Wait for snapshot sync to progress
            log.info("Waiting for snapshot sync to progress after %s", test_name)
            time.sleep(60)

            # Verify snapshot sync continued
            snap_sync_counts_after = []
            for path, snap_synced_before in zip(
                subvolume_paths, snap_sync_counts_before
            ):
                snaps_synced_after = fs_mirroring_utils.get_snaps_synced(
                    source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
                )
                snap_sync_counts_after.append(snaps_synced_after)

                if snaps_synced_after > snap_synced_before:
                    log.info(
                        "Snapshot sync continued for %s after %s: before=%s, after=%s",
                        path,
                        test_name,
                        snap_synced_before,
                        snaps_synced_after,
                    )
                else:
                    log.error(
                        "Snapshot sync count not continued for %s after %s: before=%s, after=%s",
                        path,
                        test_name,
                        snap_synced_before,
                        snaps_synced_after,
                    )
                    return 1

            log.info("=== %s test completed successfully ===", test_name)

            # Wait a bit before next test to ensure stability
            if test_case != signal_tests[-1]:
                log.info("Waiting for cluster to stabilize before next test")
                time.sleep(30)

        # Final verification: Wait for IOs to complete or check they're still running
        log.info("Final verification: Checking IO threads status")
        for idx, t in enumerate(io_threads):
            if t.is_alive():
                log.info("IO thread %s still running (as expected)", idx)
            else:
                log.info("IO thread %s completed", idx)

        # Final snapshot sync verification
        log.info("Final snapshot sync verification")
        snap_sync_counts_final = []
        for path, snap_synced_initial in zip(subvolume_paths, snap_sync_counts_initial):
            snaps_synced_final = fs_mirroring_utils.get_snaps_synced(
                source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
            )
            snap_sync_counts_final.append(snaps_synced_final)

            if snaps_synced_final > snap_synced_initial:
                log.info(
                    "Final snapshot sync count for %s: initial=%s, final=%s",
                    path,
                    snap_synced_initial,
                    snaps_synced_final,
                )
            else:
                log.warning(
                    "Snapshot sync count did not increase for %s: initial=%s, final=%s",
                    path,
                    snap_synced_initial,
                    snaps_synced_final,
                )

        # Final cluster health check
        cluster_health_final = check_ceph_healthly(
            source_clients[0],
            num_of_osds,
            num_of_mons,
            build,
            None,
            30,
        )
        log.info("Final cluster health: %s", cluster_health_final)

        # Wait for all IO threads to complete
        log.info("Waiting for all IO threads to complete")
        for t in io_threads:
            t.join()
        log.info("All IO threads have completed")

        log.info(
            "Test Completed Successfully. All signal tests passed. "
            "Snapshot sync continued during all MDS signal operations. "
            "All snapshots are synced to target cluster."
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):
            log.info("Remove snapshot schedules")
            for path in subvolume_paths:
                snap_util.remove_snap_schedule(
                    source_clients[0], path, fs_name=source_fs
                )
            log.info("Delete the snapshots")
            snapshots_to_delete = []
            for path in full_subvolume_path[:-2]:
                snapshots_to_delete.append(f"{path}.snap/*")

            for snapshot_path in snapshots_to_delete:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
                )
            for subvol in subvol_list[-2:]:
                out, rc = source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume snapshot ls {source_fs} {subvol} subvolgroup_1 --format json",
                )
                # Parse JSON output to get snapshot list
                snapshots = []
                if out and out.strip():
                    try:
                        data = json.loads(out)
                        snapshots = [snap["name"] for snap in data]
                    except (json.JSONDecodeError, ValueError) as e:
                        log.error(
                            "Failed to parse JSON output for subvolume %s: %s (rc=%s, output=%s)",
                            subvol,
                            e,
                            rc,
                            out[:200] if out else "empty",
                        )
                elif rc != 0:
                    log.error(
                        "Failed to list snapshots for subvolume %s: rc=%s, output=%s",
                        subvol,
                        rc,
                        out,
                    )
                log.info(
                    "NFS Snapshots found for subvol path: %s, %s", snapshots, subvol
                )
                for snapshot_name in snapshots:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"ceph fs subvolume snapshot rm {source_fs} {subvol} {snapshot_name} subvolgroup_1",
                        check_ec=False,
                    )

            for mounting_dir in mounting_dirs:
                log.info("Unmount the paths")
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {mounting_dir}", check_ec=False
                )
            for path in subvolume_paths:
                log.info("Remove paths used for mirroring")
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, path
                )
            log.info("Destroy CephFS Mirroring setup.")
            try:
                fs_mirroring_utils.destroy_cephfs_mirroring(
                    source_fs,
                    source_clients[0],
                    target_fs,
                    target_clients[0],
                    target_user,
                    peer_uuid,
                )
            except Exception as e:
                log.error("Error during mirroring cleanup: %s", e)

            log.info("Remove Subvolumes")
            for subvolume in subvolume_list:
                try:
                    fs_util_v1_ceph1.remove_subvolume(
                        source_clients[0],
                        **subvolume,
                    )
                except Exception as e:
                    log.error("Error removing subvolume %s: %s", subvolume, e)

            log.info("Remove Subvolume Group")
            for subvolumegroup in subvolumegroup_list:
                try:
                    fs_util_v1_ceph1.remove_subvolumegroup(
                        source_clients[0], **subvolumegroup
                    )
                except Exception as e:
                    log.error("Error removing subvolumegroup %s: %s", subvolumegroup, e)

            log.info("Delete the mounted paths")
            for mounting_dir in mounting_dirs:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {mounting_dir}", check_ec=False
                )


def start_background_ios(fs_util, client, mounting_dirs, runtime):
    """
    Start background IO threads on all mount points.

    Args:
        fs_util: Filesystem utility object
        client: Client node to run IOs on
        mounting_dirs: List of mount directories
        runtime: Runtime for IOs in minutes

    Returns:
        List of Thread objects
    """
    threads = []

    for mounting_dir in mounting_dirs:
        t = Thread(
            target=fs_util.run_ios_V1,
            args=(client, mounting_dir),
            kwargs={
                "io_tools": ["smallfile"],
                "run_time": runtime,
            },
            daemon=True,  # important: don't block test exit
        )
        t.start()
        threads.append(t)
        log.info("Started IO thread for %s", mounting_dir)

    return threads
