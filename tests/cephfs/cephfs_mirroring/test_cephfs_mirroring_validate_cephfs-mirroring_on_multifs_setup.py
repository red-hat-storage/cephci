import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574107 - Validate cephfs-mirroring on multifs setup
    This function performs a series of operations to test CephFS mirroring on multiple filesystem(2)
    It has pre-requisites,
    performs various operations, and cleans up the system after the tests.

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
        # fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        build = config.get("build", config.get("rhbuild"))
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.info(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1
        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)
        log.info("Delete the existing FileSystems on both source and target ")
        log.info("Delete filesystem on Source Cluster")
        rc, ec = source_clients[0].exec_command(
            sudo=True, cmd="ceph fs ls --format json-pretty"
        )
        result = json.loads(rc)
        source_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for fs in result:
            fs_name = fs["name"]
            source_clients[0].exec_command(
                sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
            )
        time.sleep(30)

        log.info("Delete filesystem on Target Cluster")
        rc, ec = target_clients[0].exec_command(
            sudo=True, cmd="ceph fs ls --format json-pretty"
        )
        result = json.loads(rc)
        target_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for fs in result:
            fs_name = fs["name"]
            target_clients[0].exec_command(
                sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
            )
        time.sleep(30)

        log.info("Create required filesystem on Source Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        source_fs_1 = "cephfs1"
        source_fs_2 = "cephfs2"

        source_fs_1 = "cephfs1" if not erasure else "cephfs1-ec"
        source_fs_2 = "cephfs2" if not erasure else "cephfs2-ec"

        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-6:-4]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        fs_util_ceph1.create_fs(
            source_clients[0], source_fs_1, placement=f"2 {mds_hosts_1}"
        )
        # source_clients[0].exec_command(
        #     sudo=True,
        #     cmd=f'ceph fs volume create {source_fs_1} --placement="2 {mds_hosts_1}"',
        # )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs_1)

        hosts_list2 = mds_names[-4:-2]
        mds_hosts_2 = " ".join(hosts_list2) + " "
        log.info(f"MDS host list 2 {mds_hosts_2}")
        fs_util_ceph1.create_fs(
            source_clients[0], source_fs_2, placement=f"2 {mds_hosts_2}"
        )
        # source_clients[0].exec_command(
        #     sudo=True,
        #     cmd=f'ceph fs volume create {source_fs_2} --placement="2 {mds_hosts_2}"',
        # )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs_2)

        log.info("Create required filesystem on Target Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        # target_fs_1 = "cephfs-rem1"
        # target_fs_2 = "cephfs-rem2"

        target_fs_1 = "cephfs-rem1" if not erasure else "cephfs-rem1-ec"
        target_fs_2 = "cephfs-rem2" if not erasure else "cephfs-rem2-ec"
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-4:-2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        fs_util_ceph2.create_fs(
            target_clients[0], target_fs_1, placement=f"2 {mds_hosts_1}"
        )
        # target_clients[0].exec_command(
        #     sudo=True,
        #     cmd=f'ceph fs volume create {target_fs_1} --placement="2 {mds_hosts_1}"',
        # )
        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs_1)

        hosts_list2 = mds_names[-2:]
        mds_hosts_2 = " ".join(hosts_list2) + " "
        log.info(f"MDS host list 2 {mds_hosts_2}")
        fs_util_ceph2.create_fs(
            target_clients[0], target_fs_2, placement=f"2 {mds_hosts_2}"
        )
        # target_clients[0].exec_command(
        #     sudo=True,
        #     cmd=f'ceph fs volume create {target_fs_2} --placement="2 {mds_hosts_2}"',
        # )
        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs_2)

        target_user1 = "mirror_remote1"
        target_user2 = "mirror_remote2"
        target_site_name = "remote_site"

        log.info("Enable mirroring mgr module on source and destination")
        enable_mirroring_on_source = fs_mirroring_utils.enable_mirroring_module(
            source_clients[0]
        )
        if enable_mirroring_on_source:
            log.error("Mirroring module not enabled on Source Cluster.")
            raise CommandFailed("Enable mirroring mgr module failed")

        enable_mirroring_on_target = fs_mirroring_utils.enable_mirroring_module(
            target_clients[0]
        )
        if enable_mirroring_on_target:
            log.error("Mirroring module not enabled on Target Cluster.")
            raise CommandFailed("Enable mirroring mgr module failed")

        log.info("Create users on target cluster for peer connection")
        target_user_for_peer_1 = fs_mirroring_utils.create_authorize_user(
            target_fs_1,
            target_user1,
            target_clients[0],
        )
        if target_user_for_peer_1:
            log.error("User Creation Failed with the expected caps")
            raise CommandFailed("User Creation Failed")

        target_user_for_peer_2 = fs_mirroring_utils.create_authorize_user(
            target_fs_2,
            target_user2,
            target_clients[0],
        )
        if target_user_for_peer_2:
            log.error("User Creation Failed with the expected caps")
            raise CommandFailed("User Creation Failed")

        log.info("Enable cephfs mirroring module on all the filesystems")
        log.info(f"Enable cephfs mirroring module on {source_fs_1}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs_1, source_clients[0])
        log.info(f"Enable cephfs mirroring module on {source_fs_2}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs_2, source_clients[0])

        log.info("Create the peer bootstrap")
        log.info(f"Create the peer bootstrap for {target_fs_1}")
        bootstrap_token1 = fs_mirroring_utils.create_peer_bootstrap(
            target_fs_1, target_user1, target_site_name, target_clients[0]
        )
        log.info(f"Create the peer bootstrap for {target_fs_2}")
        bootstrap_token2 = fs_mirroring_utils.create_peer_bootstrap(
            target_fs_2, target_user2, target_site_name, target_clients[0]
        )

        log.info("Import the bootstrap on source on all the filesystem")
        log.info(f"Import the bootstrap on {source_fs_1}")
        fs_mirroring_utils.import_peer_bootstrap(
            source_fs_1, bootstrap_token1, source_clients[0]
        )
        log.info(f"Import the bootstrap on {source_fs_2}")
        fs_mirroring_utils.import_peer_bootstrap(
            source_fs_2, bootstrap_token2, source_clients[0]
        )

        log.info("Get Peer Connection Information for all the filesystem")
        log.info(f"Get Peer Connection Information for {source_fs_1}")
        validate_peer_connection_for_fs1 = fs_mirroring_utils.validate_peer_connection(
            source_clients[0], source_fs_1, target_site_name, target_user1, target_fs_1
        )
        log.info(f"Get Peer Connection Information for {source_fs_2}")
        validate_peer_connection_for_fs2 = fs_mirroring_utils.validate_peer_connection(
            source_clients[0], source_fs_2, target_site_name, target_user2, target_fs_2
        )
        if validate_peer_connection_for_fs1 and validate_peer_connection_for_fs2:
            log.error("Peer Connection not established")
            raise CommandFailed("Peer Connection failed to establish")

        log.info("Create Directories and mount on clients")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs_1}",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {source_fs_1}",
        )

        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs_2}",
        )
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_2,
            extra_params=f" --client_fs {source_fs_2}",
        )

        log.info("Add paths for mirroring to remote location")
        kernel_path_1 = "ker_dir1"
        kernel_path_2 = "ker_dir2"
        fuse_path_1 = "fuse_dir1"
        fuse_path_2 = "fuse_dir2"
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}{kernel_path_1}"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_mounting_dir_2}{kernel_path_2}"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{fuse_path_1}"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_2}{fuse_path_2}"
        )

        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_1, f"/{kernel_path_1}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_2, f"/{kernel_path_2}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_1, f"/{fuse_path_1}"
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_2, f"/{fuse_path_2}"
        )

        log.info("Add files into the path and create snapshot on each path")
        snap_on_kernel_path1 = "snap_k1"
        snap_on_kernel_path2 = "snap_k2"
        snap_on_fuse_path1 = "snap_f1"
        snap_on_fuse_path2 = "snap_f2"

        file_on_kernel_path1 = "hello_k1"
        file_on_kernel_path2 = "hello_k2"
        file_on_fuse_path1 = "hello_f1"
        file_on_fuse_path2 = "hello_f2"

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"touch {kernel_mounting_dir_1}{kernel_path_1}/{file_on_kernel_path1}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"touch {kernel_mounting_dir_2}{kernel_path_2}/{file_on_kernel_path2}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_1}{fuse_path_1}/{file_on_fuse_path1}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"touch {fuse_mounting_dir_2}{fuse_path_2}/{file_on_fuse_path2}",
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_1}{kernel_path_1}/.snap/{snap_on_kernel_path1}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_2}{kernel_path_2}/.snap/{snap_on_kernel_path2}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir_1}{fuse_path_1}/.snap/{snap_on_fuse_path1}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir_2}{fuse_path_2}/.snap/{snap_on_fuse_path2}",
        )

        log.info("Validate the Snapshot Synchronisation status on Target Cluster")
        snap_count = 2
        validate_sync_for_source_fs1 = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs_1, snap_count
        )
        validate_sync_for_source_fs2 = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs_2, snap_count
        )
        if validate_sync_for_source_fs1 and validate_sync_for_source_fs2:
            log.error("Snapshot Synchronisation failed..")
            raise CommandFailed("Snapshot Synchronisation failed")

        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        log.info(f"Name of the cephfs-mirror daemon : {daemon_name}")
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name
        )
        log.info(f"Admin Socket file of cephfs-mirror daemon : {asok_file}")
        filesystem_id_1 = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs_1
        )
        log.info(f"filesystem id of {source_fs_1} is : {filesystem_id_1}")
        peer_uuid_1 = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs_1
        )
        log.info(f"peer uuid of {source_fs_1} is : {peer_uuid_1}")
        filesystem_id_2 = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs_2
        )
        log.info(f"filesystem id of {source_fs_2} is : {filesystem_id_2}")
        peer_uuid_2 = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs_2
        )
        log.info(f"peer uuid of {source_fs_2} is : {peer_uuid_2}")

        log.info("Validate if Snapshots are synced to Target Cluster")
        result_snap_k1 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs_1,
            snap_on_kernel_path1,
            fsid,
            asok_file,
            filesystem_id_1,
            peer_uuid_1,
        )
        result_snap_k2 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs_2,
            snap_on_kernel_path2,
            fsid,
            asok_file,
            filesystem_id_2,
            peer_uuid_2,
        )

        result_snap_f1 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs_1,
            snap_on_fuse_path1,
            fsid,
            asok_file,
            filesystem_id_1,
            peer_uuid_1,
        )
        result_snap_f2 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs_2,
            snap_on_fuse_path2,
            fsid,
            asok_file,
            filesystem_id_2,
            peer_uuid_2,
        )
        if result_snap_k1 and result_snap_f1 and result_snap_k2 and result_snap_f2:
            log.info("All snapshots synced.")
        else:
            log.error("One or both snapshots not found or not synced.")

        log.info("Validate the snapshots and data synced to the target cluster")
        target_mount_path1 = "/mnt/remote_dir1"
        target_mount_path2 = "/mnt/remote_dir2"
        target_mount_path3 = "/mnt/remote_dir3"
        target_mount_path4 = "/mnt/remote_dir4"
        target_client_user = "client.admin"
        source_path1 = f"/{kernel_path_1}/"
        source_path2 = f"/{kernel_path_2}/"
        source_path3 = f"/{fuse_path_1}/"
        source_path4 = f"/{fuse_path_2}/"
        snapshot_name1 = snap_on_kernel_path1
        snapshot_name2 = snap_on_kernel_path2
        snapshot_name3 = snap_on_fuse_path1
        snapshot_name4 = snap_on_fuse_path2
        expected_files1 = file_on_kernel_path1
        expected_files2 = file_on_kernel_path2
        expected_files3 = file_on_fuse_path1
        expected_files4 = file_on_fuse_path2

        (
            success1,
            result1,
        ) = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data(
            target_clients[0],
            target_mount_path1,
            target_client_user,
            source_path1,
            snapshot_name1,
            expected_files1,
            target_fs_1,
        )
        (
            success2,
            result2,
        ) = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data(
            target_clients[0],
            target_mount_path2,
            target_client_user,
            source_path2,
            snapshot_name2,
            expected_files2,
            target_fs_2,
        )
        (
            success3,
            result3,
        ) = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data(
            target_clients[0],
            target_mount_path3,
            target_client_user,
            source_path3,
            snapshot_name3,
            expected_files3,
            target_fs_1,
        )
        (
            success4,
            result4,
        ) = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data(
            target_clients[0],
            target_mount_path4,
            target_client_user,
            source_path4,
            snapshot_name4,
            expected_files4,
            target_fs_2,
        )

        if success1 and success2 and success3 and success4:
            log.info(
                "Test Completed Successfully."
                "All snapshots and data from all the filesystem are synced to target cluster"
            )
        else:
            log.error("Snapshots or Data are missing on target cluster.")
            raise CommandFailed(
                "Test Failed, Snapshots or Data failed to sync to target cluster"
            )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Delete the snapshots")
        paths = [
            (kernel_mounting_dir_1, kernel_path_1, snap_on_kernel_path1),
            (kernel_mounting_dir_2, kernel_path_2, snap_on_kernel_path2),
            (fuse_mounting_dir_1, fuse_path_1, snap_on_fuse_path1),
            (fuse_mounting_dir_2, fuse_path_2, snap_on_fuse_path2),
        ]
        for path_parts in paths:
            mounting_dir, path, snap = path_parts
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {mounting_dir}{path}/.snap/{snap}"
            )

        log.info("Unmount the paths on source")
        mounting_dirs = [
            kernel_mounting_dir_1,
            kernel_mounting_dir_2,
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
        ]
        for mounting_dir in mounting_dirs:
            source_clients[0].exec_command(sudo=True, cmd=f"umount -l {mounting_dir}")

        log.info("Remove paths used for mirroring")
        mirroring_data = [
            (source_fs_1, f"/{kernel_path_1}"),
            (source_fs_2, f"/{kernel_path_2}"),
            (source_fs_1, f"/{fuse_path_1}"),
            (source_fs_2, f"/{fuse_path_2}"),
        ]
        for fs, path in mirroring_data:
            fs_mirroring_utils.remove_path_from_mirroring(source_clients[0], fs, path)

        log.info("Remove peer from snapshot mirroring")
        mirroring_data = [
            (source_fs_1, peer_uuid_1),
            (source_fs_2, peer_uuid_2),
        ]
        for fs, peer_uuid in mirroring_data:
            if fs_mirroring_utils.remove_snapshot_mirror_peer(
                source_clients[0], fs, peer_uuid
            ):
                log.info("Peer removal is successful.")
            else:
                log.error("Peer removal failed.")

        log.info("Disable CephFS Snapshot mirroring")
        source_filesystems = [source_fs_1, source_fs_2]
        for fs in source_filesystems:
            fs_mirroring_utils.disable_snapshot_mirroring(fs, source_clients[0])

        log.info("Delete the mounted paths")
        mounting_dirs = [
            kernel_mounting_dir_1,
            kernel_mounting_dir_2,
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
        ]
        for mounting_dir in mounting_dirs:
            source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {mounting_dir}")

        log.info("Disable mirroring mgr module on source and destination")
        clients = [source_clients[0], target_clients[0]]
        for client in clients:
            fs_mirroring_utils.disable_mirroring_module(client)

        log.info("Delete the users used for creating peer bootstrap")
        fs_mirroring_utils.remove_user_used_for_peer_connection(
            target_user1, target_clients[0]
        )
        fs_mirroring_utils.remove_user_used_for_peer_connection(
            target_user2, target_clients[0]
        )

        log.info("Cleanup Target Client")
        target_mount_paths = [
            target_mount_path1,
            target_mount_path2,
            target_mount_path3,
            target_mount_path4,
        ]
        for mount_path in target_mount_paths:
            fs_mirroring_utils.cleanup_target_client(target_clients[0], mount_path)

        log.info("Delete all the FileSystems created on both source and target ")
        log.info("Delete filesystem on Source Cluster")
        rc, ec = source_clients[0].exec_command(
            sudo=True, cmd="ceph fs ls --format json-pretty"
        )
        result = json.loads(rc)
        source_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for fs in result:
            fs_name = fs["name"]
            source_clients[0].exec_command(
                sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
            )
        time.sleep(30)

        log.info("Delete filesystem on Target Cluster")
        rc, ec = target_clients[0].exec_command(
            sudo=True, cmd="ceph fs ls --format json-pretty"
        )
        result = json.loads(rc)
        target_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        for fs in result:
            fs_name = fs["name"]
            target_clients[0].exec_command(
                sudo=True, cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it"
            )
        time.sleep(30)

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        source_fs = "cephfs"
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-6:-4]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {source_fs} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)

        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        target_fs = "cephfs"
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-4:-2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        target_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {target_fs} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs)
