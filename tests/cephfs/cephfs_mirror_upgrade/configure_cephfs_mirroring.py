import pickle
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574099 - Validate the Synchronisation is successful upon enabling fs mirroring.
    This function performs a series of operations to test CephFS mirroring.
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
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"))
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"))
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
        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        source_fs = "cephfs_upgrade_mirror"
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

        log.info("Create required filesystem on Target Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        target_fs = "cephfs_upgrade_mirror_remote"
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

        target_user = "mirror_remote_user_upgrade"
        target_site_name = "remote_site_upgrade"

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

        log.info("Create a user on target cluster for peer connection")
        target_user_for_peer = fs_mirroring_utils.create_authorize_user(
            target_fs,
            target_user,
            target_clients[0],
        )
        if target_user_for_peer:
            log.error("User Creation Failed with the expected caps")
            raise CommandFailed("User Creation Failed")

        log.info(f"Enable cephfs mirroring module on the {source_fs}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs, source_clients[0])

        log.info("Create the peer bootstrap")
        bootstrap_token = fs_mirroring_utils.create_peer_bootstrap(
            target_fs, target_user, target_site_name, target_clients[0]
        )

        log.info("Import the bootstrap on source")
        fs_mirroring_utils.import_peer_bootstrap(
            source_fs, bootstrap_token, source_clients[0]
        )

        log.info("Get Peer Connection Information")
        validate_peer_connection = fs_mirroring_utils.validate_peer_connection(
            source_clients[0], source_fs, target_site_name, target_user, target_fs
        )
        if validate_peer_connection:
            log.error("Peer Connection not established")
            raise CommandFailed("Peer Connection failed to establish")

        log.info("Create Subvolumes for adding Data")
        log.info("Scenario 1 : ")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolume on kernel and 1 subvloume on Fuse → Client1")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        log.info("Get the path of subvolume1 on  filesystem")
        subvol_path1, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_1 subvolgroup_1",
        )
        index = subvol_path1.find("subvol_1/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len("subvol_1/")]
        else:
            subvol1_path = subvol_path1
        log.info(subvol1_path)

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )
        log.info("Get the path of subvolume2 on filesystem")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_2 subvolgroup_1",
        )
        index = subvol_path2.find("subvol_2/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len("subvol_2/")]
        else:
            subvol2_path = subvol_path2
        log.info(subvol2_path)
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {source_fs}",
        )

        log.info("Add subvolumes for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path
        )

        log.info("Add files into the path and create snapshot on each path")
        snap1 = "snap_k1"
        snap2 = "snap_f1"
        file_name1 = "hello_kernel"
        file_name2 = "hello_fuse"
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {kernel_mounting_dir_1}{subvol1_path}hello_kernel"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {fuse_mounting_dir_1}{subvol2_path}hello_fuse"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}"
        )

        log.info("Validate the Snapshot Synchronisation on Target Cluster")
        snap_count = 2
        validate_syncronisation = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, snap_count
        )
        if validate_syncronisation:
            log.error("Snapshot Synchronisation failed..")
            raise CommandFailed("Snapshot Synchronisation failed")

        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        log.info(f"Name of the cephfs-mirror daemon : {daemon_name}")
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name
        )
        log.info(f"Admin Socket file of cephfs-mirror daemon : {asok_file}")
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")

        log.info("Validate if the Snapshots are synced to Target Cluster")
        result_snap_k1 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            snap1,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        result_snap_f1 = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            snap2,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )
        if result_snap_k1 and result_snap_f1:
            log.info(f"Snapshot '{result_snap_k1['snapshot_name']}' has been synced:")
            log.info(
                f"Sync Duration: {result_snap_k1['sync_duration']} of '{result_snap_k1['snapshot_name']}'"
            )
            log.info(
                f"Sync Time Stamp: {result_snap_k1['sync_time_stamp']} of '{result_snap_k1['snapshot_name']}'"
            )
            log.info(
                f"Snaps Synced: {result_snap_k1['snaps_synced']} of '{result_snap_k1['snapshot_name']}'"
            )

            log.info(f"Snapshot '{result_snap_f1['snapshot_name']}' has been synced:")
            log.info(
                f"Sync Duration: {result_snap_f1['sync_duration']} of '{result_snap_f1['snapshot_name']}'"
            )
            log.info(
                f"Sync Time Stamp: {result_snap_f1['sync_time_stamp']} of '{result_snap_f1['snapshot_name']}'"
            )
            log.info(
                f"Snaps Synced: {result_snap_f1['snaps_synced']} of '{result_snap_f1['snapshot_name']}'"
            )

            log.info("All snapshots synced.")
        else:
            log.error("One or both snapshots not found or not synced.")

        log.info(
            "Validate the synced snapshots and data available on the target cluster"
        )
        target_mount_path1 = "/mnt/remote_dir1"
        target_mount_path2 = "/mnt/remote_dir2"
        target_client_user = "client.admin"
        source_path1 = subvol1_path
        source_path2 = subvol2_path
        snapshot_name1 = snap1
        snapshot_name2 = snap2
        expected_files1 = file_name1
        expected_files2 = file_name2

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
            target_fs,
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
            target_fs,
        )

        if success1 and success2:
            log.info(
                "Test Completed Successfully.All snapshots and data are synced to target cluster"
            )
        else:
            log.error("Test Failed. Snapshots or Data are missing on target cluster.")
            log.info(f"Status of {snap1}: {result1}")
            log.info(f"Status of {snap2}: {result2}")
            return 1
        log.info(f"Status of {snap1}: {result1}")
        log.info(f"Status of {snap2}: {result2}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Create a Variables file for post validations")
        with open("variables.pkl", "wb") as file:
            pickle.dump(
                (
                    source_fs,
                    target_fs,
                    target_user,
                    target_site_name,
                    kernel_mounting_dir_1,
                    fuse_mounting_dir_1,
                    subvol1_path,
                    subvol2_path,
                    fs_util_ceph1,
                    fs_util_ceph2,
                    subvolumegroup_list,
                    subvolume_list,
                ),
                file,
            )
