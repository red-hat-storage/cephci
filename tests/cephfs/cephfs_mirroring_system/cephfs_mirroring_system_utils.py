"""
Shared utilities for CephFS mirroring system tests.

Contains generic daemon operation helpers (signal, restart, reboot,
container restart, redeploy) and IO utilities used by both the MDS
and mirror-daemon test suites.
"""

import json
import random
import secrets
import signal
import string
import time
import traceback
from threading import Thread

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


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
            kwargs={"io_tools": ["smallfile"], "run_time": runtime},
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Started IO thread for %s", mounting_dir)
    return threads


def run_signal_tests(
    fs_util_v1, nodes, daemon_process_name, daemon_service_pattern, test_case
):
    """
    Send a signal to daemon processes on the given nodes.

    After SIGTERM with expect_exit=True, restarts the service via systemctl.
    Recovery after the operation is the caller's responsibility.

    Args:
        fs_util_v1: FsUtilsV1 object for pid_signal and deamon_name
        nodes: List of resolved node objects to send signals to
        daemon_process_name: Process name for pid_signal (e.g., "mds", "cephfs-mirror")
        daemon_service_pattern: Regex pattern for deamon_name lookup
        test_case: Dict with "signal", "name", "expect_exit" keys

    Returns:
        int: 0 if successful, 1 on error
    """
    try:
        sig = test_case["signal"]
        test_name = test_case["name"]
        expect_exit = test_case["expect_exit"]

        log.info("Sending %s to %s daemons", test_name, daemon_process_name)

        for node in nodes:
            log.info(
                "Sending %s to %s on host: %s",
                test_name,
                daemon_process_name,
                node.hostname,
            )
            fs_util_v1.pid_signal(
                node, daemon_process_name, sig=sig, expect_exit=expect_exit, wait=10
            )
            if sig == signal.SIGTERM and expect_exit:
                log.info(
                    "Restarting %s service after SIGTERM on host: %s",
                    daemon_process_name,
                    node.hostname,
                )
                service_name = fs_util_v1.deamon_name(node, daemon_service_pattern)
                log.info("Starting %s service: %s", daemon_process_name, service_name)
                node.exec_command(
                    sudo=True,
                    cmd=f"systemctl start {service_name}",
                    check_ec=False,
                )
                out, rc = node.exec_command(
                    sudo=True,
                    cmd=f"systemctl is-active {service_name}",
                    check_ec=False,
                )
                if "active" in out:
                    log.info(
                        "%s service %s started successfully",
                        daemon_process_name,
                        service_name,
                    )
                else:
                    log.warning(
                        "%s service %s might not be active: %s",
                        daemon_process_name,
                        service_name,
                        out,
                    )
        return 0
    except Exception as e:
        log.error("Error during %s test: %s", test_name, e)
        log.error(traceback.format_exc())
        return 1


def run_systemctl_restart(
    fs_util_v1, nodes, daemon_process_name, daemon_service_pattern
):
    """
    Restart daemons via systemctl restart.

    Args:
        fs_util_v1: FsUtilsV1 object for deamon_name
        nodes: List of resolved node objects
        daemon_process_name: Daemon name for logging
        daemon_service_pattern: Regex pattern for deamon_name lookup

    Returns:
        int: 0 if successful, 1 on error
    """
    try:
        log.info("Restarting %s daemons using systemctl restart", daemon_process_name)

        for node in nodes:
            log.info(
                "Restarting %s using systemctl on host: %s",
                daemon_process_name,
                node.hostname,
            )
            service_name = fs_util_v1.deamon_name(node, daemon_service_pattern)
            log.info("Restarting %s service: %s", daemon_process_name, service_name)
            node.exec_command(
                sudo=True,
                cmd=f"systemctl restart {service_name}",
            )
        return 0
    except Exception as e:
        log.error("Error during systemctl restart test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_node_reboot(fs_util_v1, nodes, timeout=300):
    """
    Reboot daemon nodes.

    Args:
        fs_util_v1: FsUtilsV1 object for reboot_node_v1
        nodes: List of resolved node objects
        timeout: Reboot timeout in seconds

    Returns:
        int: 0 if successful, 1 on error
    """
    try:
        log.info("Rebooting daemon nodes")
        for node in nodes:
            log.info("Rebooting node: %s", node.hostname)
            fs_util_v1.reboot_node_v1(node, timeout=timeout)
        return 0
    except Exception as e:
        log.error("Error during node reboot test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_container_restart(nodes, container_filter_keywords):
    """
    Restart daemon containers via podman restart.

    Args:
        nodes: List of resolved node objects
        container_filter_keywords: List of strings; all must be present in the
            podman ps output line to identify the target container.
            The first keyword is used as the grep pattern.

    Returns:
        int: 0 if successful, 1 on error
    """
    grep_pattern = container_filter_keywords[0]
    try:
        log.info(
            "Restarting containers matching '%s' using podman restart", grep_pattern
        )

        for node in nodes:
            log.info("Restarting container on host: %s", node.hostname)
            try:
                out, _ = node.exec_command(
                    sudo=True,
                    cmd=f"podman ps | grep {grep_pattern}",
                )
                lines = out.strip().split("\n")
                for line in lines:
                    if all(kw in line for kw in container_filter_keywords):
                        container_id = line.split()[0]
                        log.info(
                            "Restarting container: %s on host: %s",
                            container_id,
                            node.hostname,
                        )
                        node.exec_command(
                            sudo=True,
                            cmd=f"podman restart {container_id}",
                        )
                        break
            except Exception as e:
                log.warning("Failed to restart container on %s: %s", node.hostname, e)
        return 0
    except Exception as e:
        log.error("Error during container restart test: %s", e)
        log.error(traceback.format_exc())
        return 1


def run_daemon_redeploy(
    client,
    daemon_type,
    ceph_cluster_dict,
    cluster_key,
    deploy_service_name,
    node_role="mds",
    process_name=None,
):
    """
    Redeploy daemons to different nodes in the cluster.

    Finds nodes not currently running the daemon and deploys there
    using ``ceph orch apply``.

    Args:
        client: Client node to execute commands on
        daemon_type: Daemon type for 'ceph orch ps' (e.g., "mds", "cephfs-mirror")
        ceph_cluster_dict: Dictionary containing cluster objects
        cluster_key: Key for the cluster (e.g., "ceph1")
        deploy_service_name: Service name for 'ceph orch apply'
            (e.g., "mds cephfs" or "cephfs-mirror")
        node_role: Cluster role to find available nodes (default: "mds")
        process_name: If set, verify the process with wait_for_process after deploy

    Returns:
        int: 0 if successful, 1 on error
    """
    log.info("Getting current %s daemon hostnames", daemon_type)
    out, rc = client.exec_command(
        sudo=True,
        cmd=f"ceph orch ps --daemon_type={daemon_type} --format json",
        check_ec=False,
    )

    current_hostnames = set()
    try:
        daemon_data = json.loads(out)
        for daemon in daemon_data:
            hostname = daemon.get("hostname")
            if hostname:
                current_hostnames.add(hostname)
        log.info(
            "Current %s daemons running on hosts: %s",
            daemon_type,
            list(current_hostnames),
        )
    except json.JSONDecodeError as e:
        log.error("Failed to parse %s daemon information: %s", daemon_type, e)
        return 1

    available_hosts = ceph_cluster_dict.get(cluster_key).get_nodes(role=node_role)
    if not available_hosts:
        log.warning("No %s nodes found in cluster", node_role)
        return 0

    all_hostnames = {node.hostname for node in available_hosts}
    log.info("All available %s nodes: %s", node_role, list(all_hostnames))

    available_for_deploy = all_hostnames - current_hostnames

    if not available_for_deploy:
        log.warning(
            "No available nodes for %s redeploy. " "All nodes are already running %s.",
            daemon_type,
            daemon_type,
        )
        log.info("Skipping %s redeploy test", daemon_type)
    else:
        log.info(
            "Nodes available for %s redeploy: %s",
            daemon_type,
            list(available_for_deploy),
        )

        nodes_to_deploy = list(available_for_deploy)[: len(current_hostnames)]
        log.info("Deploying %s on nodes: %s", daemon_type, nodes_to_deploy)

        hosts_str = " ".join(nodes_to_deploy)
        deploy_cmd = (
            f"ceph orch apply {deploy_service_name} "
            f"--placement='{len(current_hostnames)} {hosts_str}'"
        )
        log.info("Executing: %s", deploy_cmd)

        client.exec_command(sudo=True, cmd=deploy_cmd, check_ec=False)
        log.info("%s deployment command executed successfully", daemon_type)

        if process_name:
            if wait_for_process(
                client=client,
                process_name=process_name,
                timeout=120,
                interval=10,
                ispresent=True,
            ):
                log.info("%s redeploy completed successfully", daemon_type)
            else:
                log.warning("%s deployment verification timeout", daemon_type)

    return 0


@retry(Exception, tries=5, delay=15)
def wait_for_snaps_synced_increase(
    fs_mirroring_utils,
    source_fs,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
    path,
    count_before,
    test_name,
):
    """
    Wait for snaps_synced count to increase after a daemon operation.

    Args:
        fs_mirroring_utils: CephfsMirroringUtils instance
        source_fs: Source filesystem name
        fsid: Cluster FSID
        asok_file: Admin socket file path
        filesystem_id: Filesystem ID
        peer_uuid: Mirroring peer UUID
        path: Subvolume path to check
        count_before: snaps_synced value captured before the operation
        test_name: Name of the test (for logging)

    Returns:
        The new snaps_synced count

    Raises:
        Exception: If count did not increase (triggers retry)
    """
    count_after = fs_mirroring_utils.get_snaps_synced(
        source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
    )
    if (
        count_after is not None
        and count_before is not None
        and count_after > count_before
    ):
        log.info(
            "snaps_synced increased for %s after %s: before=%s, after=%s",
            path,
            test_name,
            count_before,
            count_after,
        )
        return count_after
    raise Exception(
        f"snaps_synced not progressed for {path} after {test_name}: "
        f"before={count_before}, after={count_after}"
    )


@retry(Exception, tries=5, delay=15)
def wait_for_sync_id_increase(
    fs_mirroring_utils,
    source_fs,
    fsid,
    asok_file,
    filesystem_id,
    peer_uuid,
    path,
    sync_id_before,
    test_name,
):
    """
    Wait for last_synced_snap.id to increase after a daemon operation.

    Args:
        fs_mirroring_utils: CephfsMirroringUtils instance
        source_fs: Source filesystem name
        fsid: Cluster FSID
        asok_file: Admin socket file path
        filesystem_id: Filesystem ID
        peer_uuid: Mirroring peer UUID
        path: Subvolume path to check
        sync_id_before: last_synced_snap.id value captured before the operation
        test_name: Name of the test (for logging)

    Returns:
        The new last_synced_snap.id value

    Raises:
        Exception: If id did not increase (triggers retry)
    """
    sync_id_after = fs_mirroring_utils.get_last_synced_snap_id(
        source_fs, fsid, asok_file, filesystem_id, peer_uuid, path
    )
    if (
        sync_id_after is not None
        and sync_id_before is not None
        and sync_id_after > sync_id_before
    ):
        log.info(
            "last_synced_snap.id increased for %s after %s: before=%s, after=%s",
            path,
            test_name,
            sync_id_before,
            sync_id_after,
        )
        return sync_id_after
    raise Exception(
        f"last_synced_snap.id not progressed for {path} after {test_name}: "
        f"before={sync_id_before}, after={sync_id_after}"
    )


def cleanup_io_dirs(client, mounting_dirs):
    """
    Remove smallfile IO directories from mount points.

    Should be called while mounts are still active (before unmount).

    Args:
        client: Client node to execute cleanup commands on
        mounting_dirs: List of mount directory paths
    """
    for mounting_dir in mounting_dirs:
        log.info("Cleaning up IO directories in %s", mounting_dir)
        client.exec_command(
            sudo=True,
            cmd=f"rm -rf {mounting_dir}/smallfile_io_dir_*",
            check_ec=False,
        )


def setup_mirroring_test_environment(
    ceph_cluster,
    ceph_cluster_dict,
    config,
    test_data=None,
):
    """
    Set up the common mirroring system test environment.

    Creates all utility objects, validates pre-requisites, prepares
    clients, creates filesystems if needed, deploys mirroring, creates
    subvolume groups, subvolumes, mount points (kernel, FUSE, NFS),
    configures snapshot schedules, and fetches mirroring metadata.

    Args:
        ceph_cluster: The Ceph cluster object
        ceph_cluster_dict: Dictionary of cluster objects
        config: Test configuration dict
        test_data: Optional test data for erasure/custom config

    Returns:
        dict with all environment state including utility objects,
        client lists, filesystem names, subvolume details, mount dirs,
        and mirroring metadata

    Raises:
        Exception: On any setup failure
    """
    erasure = (
        FsUtilsV1.get_custom_config_value(test_data, "erasure") if test_data else False
    )
    build = config.get("build", config.get("rhbuild"))

    snap_util = SnapUtils(ceph_cluster)
    cephfs_common_utils = CephFSCommonUtils(ceph_cluster_dict.get("ceph1"))
    fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"))
    fs_util_v1_ceph1 = FsUtilsV1(ceph_cluster_dict.get("ceph1"), test_data=test_data)
    fs_util_v1_ceph2 = FsUtilsV1(ceph_cluster_dict.get("ceph2"), test_data=test_data)
    fs_mirroring_utils = CephfsMirroringUtils(
        ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
    )

    source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
    target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")

    log.info("Checking Pre-requisites")
    if not source_clients or not target_clients:
        raise Exception(
            "This test requires a minimum of 1 client node on both ceph1 and ceph2."
        )

    fs_util_v1_ceph1.setup_ssh_root_keys(clients=source_clients + target_clients)

    cephfs_mirror_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects(
        "cephfs-mirror"
    )
    num_of_mons = len(fs_util_v1_ceph1.get_mon_node_ips())
    num_of_osds = int(fs_util_ceph1.get_osd_count(source_clients[0]))

    log.info("Preparing Clients...")
    fs_util_v1_ceph1.prepare_clients(source_clients, build)
    fs_util_v1_ceph2.prepare_clients(target_clients, build)
    fs_util_v1_ceph1.auth_list(source_clients)
    fs_util_v1_ceph2.auth_list(target_clients)

    source_fs = "cephfs" if not erasure else "cephfs-ec"
    target_fs = "cephfs" if not erasure else "cephfs-ec"

    log.info("Erasure coded pools enabled: %s", erasure)
    log.info("Source filesystem: %s, Target filesystem: %s", source_fs, target_fs)

    source_client = source_clients[0]
    target_client = target_clients[0]

    fs_details_source = fs_util_v1_ceph1.get_fs_info(source_client, source_fs)
    if not fs_details_source:
        log.info("Creating source filesystem: %s", source_fs)
        fs_util_v1_ceph1.create_fs(source_client, source_fs, validate=True)
        fs_util_v1_ceph1.wait_for_mds_process(source_client, source_fs)
    else:
        log.info("Source filesystem %s already exists", source_fs)

    fs_details_target = fs_util_v1_ceph1.get_fs_info(target_client, target_fs)
    if not fs_details_target:
        log.info("Creating target filesystem: %s", target_fs)
        fs_util_v1_ceph2.create_fs(target_client, target_fs, validate=True)
        fs_util_v1_ceph2.wait_for_mds_process(target_client, target_fs)
    else:
        log.info("Target filesystem %s already exists", target_fs)

    target_user = "mirror_remote"
    target_site_name = "remote_site"
    log.info("Deploy CephFS Mirroring Configuration")
    fs_mirroring_utils.deploy_cephfs_mirroring(
        source_fs,
        source_client,
        target_fs,
        target_client,
        target_user,
        target_site_name,
    )

    log.info("Create Subvolumes for adding data")
    subvolumegroup_list = [
        {"vol_name": source_fs, "group_name": "subvolgroup_1"},
    ]
    for subvolumegroup in subvolumegroup_list:
        fs_util_v1_ceph1.create_subvolumegroup(source_client, **subvolumegroup)

    mount_types = [
        ("kernel", 1),
        ("kernel", 2),
        ("fuse", 1),
        ("fuse", 2),
        ("nfs", 1),
        ("nfs", 2),
    ]

    subvolume_list = [
        {
            "vol_name": source_fs,
            "subvol_name": f"subvol_{kind}_{n}",
            "group_name": "subvolgroup_1",
            "size": "5368709120",
        }
        for kind, n in mount_types
    ]
    for subvolume in subvolume_list:
        fs_util_v1_ceph1.create_subvolume(source_client, **subvolume)

    mounting_dir_suffix = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    mounting_dirs = [
        f"/mnt/cephfs_{kind}_{mounting_dir_suffix}_{n}/" for kind, n in mount_types
    ]

    mon_node_ips = fs_util_v1_ceph1.get_mon_node_ips()
    subvol_list = [d["subvol_name"] for d in subvolume_list]

    subvolume_paths = []
    for subvol in subvol_list:
        log.info(
            "Get the path of %s on filesystem and add for mirroring",
            subvol,
        )
        subvol_path, _ = source_client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} {subvol} subvolgroup_1",
        )
        index = subvol_path.find(f"{subvol}/")
        if index != -1:
            subvol_path = subvol_path[: index + len(f"{subvol}/")]
        log.info(subvol_path)
        fs_mirroring_utils.add_path_for_mirroring(source_client, source_fs, subvol_path)
        subvolume_paths.append(subvol_path)

    full_subvolume_path = []
    for mount_dir, subvol_path in zip(mounting_dirs, subvolume_paths):
        if "nfs" in mount_dir:
            full_subvolume_path.append(mount_dir)
        else:
            full_subvolume_path.append(f"{mount_dir}{subvol_path}")

    # NFS cluster and exports
    source_nfs_servers = ceph_cluster_dict.get("ceph1").get_ceph_objects("nfs")
    source_nfs_server = source_nfs_servers[0].node.hostname
    nfs_name = "cephfs-nfs"

    log.info("Enable NFS module on source cluster")
    source_client.exec_command(sudo=True, cmd="ceph mgr module enable nfs")

    log.info("Creating NFS cluster: %s on '%s'", nfs_name, source_nfs_server)
    fs_util_v1_ceph1.create_nfs(
        source_client,
        nfs_cluster_name=nfs_name,
        nfs_server_name=source_nfs_server,
    )
    if wait_for_process(client=source_client, process_name=nfs_name, ispresent=True):
        log.info("NFS cluster created successfully on source cluster")
    else:
        log.error("NFS cluster process not found after creation")

    nfs_export_name_1 = "/export_source_nfs_1_" + "".join(
        secrets.choice(string.digits) for _ in range(3)
    )
    nfs_export_name_2 = "/export_source_nfs_2_" + "".join(
        secrets.choice(string.digits) for _ in range(3)
    )
    nfs_export_map = {
        mounting_dirs[4]: nfs_export_name_1,
        mounting_dirs[5]: nfs_export_name_2,
    }
    log.info("Create NFS exports on source cluster")
    for export_name, subvol_path in zip(
        [nfs_export_name_1, nfs_export_name_2], subvolume_paths[-2:]
    ):
        fs_util_v1_ceph1.create_nfs_export(
            source_client,
            nfs_name,
            export_name,
            source_fs,
            path=subvol_path,
        )
        log.info("Export %s created successfully.", export_name)

    # Mount filesystems
    log.info("Mounting FS as kernel, fuse and NFS mounts")
    for mount_dir in mounting_dirs:
        if "kernel" in mount_dir:
            fs_util_v1_ceph1.kernel_mount(
                [source_client],
                mount_dir,
                ",".join(mon_node_ips),
                extra_params=f",fs={source_fs}",
            )
        elif "fuse" in mount_dir:
            fs_util_v1_ceph1.fuse_mount(
                [source_client],
                mount_dir,
                extra_params=f" --client_fs {source_fs}",
            )
        elif "nfs" in mount_dir:
            export_name = nfs_export_map[mount_dir]
            rc = fs_util_v1_ceph1.cephfs_nfs_mount(
                source_client,
                source_nfs_server,
                export_name,
                mount_dir,
            )
            if not rc:
                raise Exception(f"NFS export {export_name} mount failed")

    # Snapshot schedules
    log.info("Enable snap schedule on primary cluster")
    snap_util.enable_snap_schedule(source_client)
    time.sleep(10)
    log.info("Allow minutely Snap Schedule on primary cluster")
    snap_util.allow_minutely_schedule(source_client, allow=True)
    time.sleep(10)

    log.info("Creating snapshot schedules with 1-minute interval")
    for path in subvolume_paths:
        snap_params = {
            "client": source_client,
            "path": path,
            "sched": "1m",
            "fs_name": source_fs,
            "validate": True,
        }
        result = snap_util.create_snap_schedule(snap_params)
        if result != 0:
            raise Exception(f"Failed to create snapshot schedule for {path}")
        log.info("Snapshot schedule for %s created successfully.", path)

        sched_list = snap_util.get_snap_schedule_list(source_client, path, source_fs)
        log.info("Snap Schedule list for %s : %s", path, sched_list)

    time.sleep(60)
    log.info("Wait for 60 secs for snapshot creation via snap schedule")

    # Fetch mirroring metadata
    log.info(
        "Fetching mirroring metadata (fsid, daemon_name, asok_file, "
        "filesystem_id, peer_uuid)"
    )
    fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_nodes[0])
    log.info("fsid on ceph cluster : %s", fsid)
    daemon_name = fs_mirroring_utils.get_daemon_name(source_client)
    log.info("Name of the cephfs-mirror daemon : %s", daemon_name)
    asok_file = fs_mirroring_utils.get_asok_file_with_connectivity_check(
        cephfs_mirror_nodes, fsid, daemon_name
    )
    if not asok_file:
        raise Exception("Failed to get admin socket file for cephfs-mirror daemon")
    log.info("Admin Socket file of cephfs-mirror daemon : %s", asok_file)
    filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
        source_client, source_fs
    )
    log.info("filesystem id of %s is : %s", source_fs, filesystem_id)
    peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(source_client, source_fs)
    log.info("peer uuid of %s is : %s", source_fs, peer_uuid)

    return {
        "snap_util": snap_util,
        "cephfs_common_utils": cephfs_common_utils,
        "fs_util_v1_ceph1": fs_util_v1_ceph1,
        "fs_util_v1_ceph2": fs_util_v1_ceph2,
        "fs_mirroring_utils": fs_mirroring_utils,
        "source_clients": source_clients,
        "target_clients": target_clients,
        "cephfs_mirror_nodes": cephfs_mirror_nodes,
        "ceph_cluster_dict": ceph_cluster_dict,
        "build": build,
        "num_of_mons": num_of_mons,
        "num_of_osds": num_of_osds,
        "source_fs": source_fs,
        "target_fs": target_fs,
        "target_user": target_user,
        "subvolumegroup_list": subvolumegroup_list,
        "subvolume_list": subvolume_list,
        "subvol_list": subvol_list,
        "subvolume_paths": subvolume_paths,
        "mounting_dirs": mounting_dirs,
        "full_subvolume_path": full_subvolume_path,
        "nfs_name": nfs_name,
        "nfs_export_map": nfs_export_map,
        "fsid": fsid,
        "daemon_name": daemon_name,
        "asok_file": asok_file,
        "filesystem_id": filesystem_id,
        "peer_uuid": peer_uuid,
    }


def cleanup_mirroring_test_environment(env):
    """
    Clean up the mirroring system test environment.

    Removes snapshot schedules, snapshots, IO directories, unmounts,
    removes mirroring paths, destroys mirroring, removes subvolumes
    and subvolume groups.

    Args:
        env: dict returned by setup_mirroring_test_environment
    """
    snap_util = env["snap_util"]
    fs_util_v1_ceph1 = env["fs_util_v1_ceph1"]
    fs_mirroring_utils = env["fs_mirroring_utils"]
    source_client = env["source_clients"][0]
    target_client = env["target_clients"][0]
    source_fs = env["source_fs"]
    target_fs = env["target_fs"]
    target_user = env["target_user"]
    peer_uuid = env["peer_uuid"]
    subvolume_paths = env["subvolume_paths"]
    subvol_list = env["subvol_list"]
    full_subvolume_path = env["full_subvolume_path"]
    mounting_dirs = env["mounting_dirs"]
    subvolume_list = env["subvolume_list"]
    subvolumegroup_list = env["subvolumegroup_list"]

    log.info("Remove snapshot schedules")
    for path in subvolume_paths:
        snap_util.remove_snap_schedule(source_client, path, fs_name=source_fs)

    log.info("Delete the snapshots")
    snapshots_to_delete = [f"{path}.snap/*" for path in full_subvolume_path[:-2]]
    for snapshot_path in snapshots_to_delete:
        source_client.exec_command(
            sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
        )

    for subvol in subvol_list[-2:]:
        out, rc = source_client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot ls {source_fs} {subvol} subvolgroup_1 --format json",
        )
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
        log.info("NFS Snapshots found for subvol path: %s, %s", snapshots, subvol)
        for snapshot_name in snapshots:
            source_client.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume snapshot rm {source_fs} {subvol} {snapshot_name} subvolgroup_1",
                check_ec=False,
            )

    cleanup_io_dirs(source_client, mounting_dirs)
    for mounting_dir in mounting_dirs:
        log.info("Unmount the paths")
        source_client.exec_command(
            sudo=True, cmd=f"umount -l {mounting_dir}", check_ec=False
        )
    for path in subvolume_paths:
        log.info("Remove paths used for mirroring")
        fs_mirroring_utils.remove_path_from_mirroring(source_client, source_fs, path)
    log.info("Destroy CephFS Mirroring setup.")
    try:
        fs_mirroring_utils.destroy_cephfs_mirroring(
            source_fs,
            source_client,
            target_fs,
            target_client,
            target_user,
            peer_uuid,
        )
    except Exception as e:
        log.error("Error during mirroring cleanup: %s", e)

    log.info("Remove Subvolumes")
    for subvolume in subvolume_list:
        try:
            fs_util_v1_ceph1.remove_subvolume(source_client, **subvolume)
        except Exception as e:
            log.error("Error removing subvolume %s: %s", subvolume, e)

    log.info("Remove Subvolume Group")
    for subvolumegroup in subvolumegroup_list:
        try:
            fs_util_v1_ceph1.remove_subvolumegroup(source_client, **subvolumegroup)
        except Exception as e:
            log.error("Error removing subvolumegroup %s: %s", subvolumegroup, e)

    log.info("Delete the mounted paths")
    for mounting_dir in mounting_dirs:
        source_client.exec_command(
            sudo=True, cmd=f"rm -rf {mounting_dir}", check_ec=False
        )
