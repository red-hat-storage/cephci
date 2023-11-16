import random
import string
import traceback
from itertools import product

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574109 - Modify the user capabilities with limited access to the mds and osd
                    so that the snapshot creation should fail and observe the behavior on MDS

    Test Scenarios Covered :
    Scenario 1: Create users with r, rw ,rwp on mds and see token creation fails
    Scenario 2: Update user auths and try to create bootstrap token
    Scenario 3: Modify the user permissions for the user created. Sync should still continue

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
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)
        source_fs = "cephfs"
        target_fs = "cephfs"
        target_user = "mirror_remote_caps"
        target_site_name = "remote_site_caps"

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
        log.info(
            "Scenario 1: Create users with r, rw ,rwp on mds and see token creation fails"
        )
        permission_list = ["r", "rw", "rwp"]
        for permission in permission_list:
            perform_fs_mirroring_operations(
                fs_mirroring_utils,
                target_fs,
                target_user,
                target_clients,
                source_fs,
                source_clients,
                target_site_name,
                permissions=permission,
            )
        log.info("Scenario 2: Update user auths and try to create bootstrap token")
        log.info("Create a user on target cluster for peer connection")
        target_user_for_peer = fs_mirroring_utils.create_authorize_user(
            target_fs, target_user, target_clients[0]
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
        out, rc = target_clients[0].exec_command(
            sudo=True, cmd=f"ceph auth get client.{target_user}"
        )
        log.info(out)
        log.info("Before Modifying the caps")
        caps_list = ["r", "rw", "rwp"]
        for caps in caps_list:
            target_clients[0].exec_command(
                sudo=True,
                cmd=f'ceph auth caps client.{target_user} mds "allow {caps} fsname={target_fs}" '
                f'mon "allow r fsname={target_fs}" '
                f'osd "allow rw tag cephfs data={target_fs}"',
            )
            out, rc = target_clients[0].exec_command(
                sudo=True, cmd=f"ceph auth get client.{target_user}"
            )
            log.info(out)
            command = (
                f"ceph fs snapshot mirror peer_bootstrap create "
                f"{target_fs} client.{target_user} {target_site_name} -f json"
            )
            out, rc = target_clients[0].exec_command(
                sudo=True, cmd=command, check_ec=False
            )

            if not rc:
                log.error(f"Bootstrap token has been created with permissions {caps}")
                raise CommandFailed(
                    f"Bootstrap token has been created with permissions {caps} after updating user caps"
                )
        target_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph auth caps client.{target_user} mds "allow rwps fsname={target_fs}" '
            f'mon "allow r fsname={target_fs}" '
            f'osd "allow rw tag cephfs data={target_fs}"',
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
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_caps_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_caps_2",
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
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_caps_1 subvolgroup_1",
        )
        index = subvol_path1.find("subvol_caps_1/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len("subvol_caps_1/")]
        else:
            subvol1_path = subvol_path1
        log.info(subvol1_path)

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
        )
        log.info("Get the path of subvolume2 on filesystem")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_caps_2 subvolgroup_1",
        )
        index = subvol_path2.find("subvol_caps_2/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len("subvol_caps_2/")]
        else:
            subvol2_path = subvol_path2
        log.info(subvol2_path)
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
        )

        log.info("Add subvolumes for mirroring to remote location")
        for path in [subvol1_path, subvol2_path]:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, path
            )
        out, rc = target_clients[0].exec_command(
            sudo=True, cmd=f"ceph auth get client.{target_user}"
        )
        log.info(out)
        log.info(
            "Scenario 3: Modify the user permissions for the user created. Sync should still continue"
        )
        mds_caps_list = ["r", "rw", "rwp"]
        osd_caps_list = ["r", "rw"]
        test_status = 0
        for mds_caps, osd_caps in product(mds_caps_list, osd_caps_list):
            log.info(
                f"MDS caps updated to :{mds_caps} \n OSD Caps Updated to :{osd_caps}"
            )
            target_clients[0].exec_command(
                sudo=True,
                cmd=f'ceph auth caps client.{target_user} mds "allow {mds_caps} fsname={target_fs}" '
                f'mon "allow r fsname={target_fs}" '
                f'osd "allow {osd_caps} tag cephfs data={target_fs}"',
            )
            out, rc = target_clients[0].exec_command(
                sudo=True, cmd=f"ceph auth get client.{target_user}"
            )
            log.info(out)
            log.info("Add files into the path and create snapshot on each path")
            snap1 = f"snap_k1_{mds_caps}_{osd_caps}"
            snap2 = f"snap_f1_{mds_caps}_{osd_caps}"
            file_name1 = f"hello_kernel_{mds_caps}_{osd_caps}"
            file_name2 = f"hello_fuse_{mds_caps}_{osd_caps}"
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"touch {kernel_mounting_dir_1}{subvol1_path}hello_kernel_{mds_caps}_{osd_caps}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"touch {fuse_mounting_dir_1}{subvol2_path}hello_fuse_{mds_caps}_{osd_caps}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}",
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}"
            )

            log.info("Validate the Snapshot Synchronisation on Target Cluster")
            validate_syncronisation = fs_mirroring_utils.validate_synchronization(
                cephfs_mirror_node[0], source_clients[0], source_fs, 2
            )
            if validate_syncronisation:
                log.error("Snapshot Synchronisation failed..")
                raise CommandFailed("Snapshot Synchronisation failed")
            daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
            log.info(f"Name of the cephfs-mirror daemon : {daemon_name}")
            fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
            log.info(f"fsid on ceph cluster : {fsid}")
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
            log.info(f"{result_snap_k1} {result_snap_f1}")
            if result_snap_k1 and result_snap_f1:
                log.info(
                    f"Snapshot '{result_snap_k1['snapshot_name']}' has been synced:"
                )
                log.info(
                    f"Sync Duration: {result_snap_k1['sync_duration']} of '{result_snap_k1['snapshot_name']}'"
                )
                log.info(
                    f"Sync Time Stamp: {result_snap_k1['sync_time_stamp']} of '{result_snap_k1['snapshot_name']}'"
                )
                log.info(
                    f"Snaps Synced: {result_snap_k1['snaps_synced']} of '{result_snap_k1['snapshot_name']}'"
                )

                log.info(
                    f"Snapshot '{result_snap_f1['snapshot_name']}' has been synced:"
                )
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
                raise CommandFailed("One or both snapshots not found or not synced.")

            log.info("Validate the snapshots and data synced to the target cluster")
            target_mount_path1 = f"/mnt/remote_dir1_{mds_caps}_{osd_caps}"
            target_mount_path2 = f"/mnt/remote_dir2_{mds_caps}_{osd_caps}"
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
                log.error(
                    "Test Failed. Snapshots or Data are missing on target cluster."
                )
                log.error(f"{result1} \n {result2}")
                test_status = 1
                failed_snaps = f"{result1} \n {result2}"
            log.info(f"Status of {snap1}: {result1}")
            log.info(f"Status of {snap2}: {result2}")

            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rm -rf {kernel_mounting_dir_1}{subvol1_path}hello_kernel_{mds_caps}_{osd_caps}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rm -rf {fuse_mounting_dir_1}{subvol2_path}hello_fuse_{mds_caps}_{osd_caps}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume snapshot rm {target_fs} subvol_caps_1 {snap1} --group_name subvolgroup_1",
            )

            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume snapshot rm {target_fs} subvol_caps_2 {snap2} --group_name subvolgroup_1",
            )
            if test_status:
                log.error(f"{failed_snaps}")
                return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")

        log.info("Unmount the paths")
        source_clients[0].exec_command(
            sudo=True, cmd=f"umount -l {kernel_mounting_dir_1}", check_ec=False
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"umount -l {fuse_mounting_dir_1}", check_ec=False
        )

        log.info("Remove paths used for mirroring")
        for path in [subvol1_path, subvol2_path]:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs snapshot mirror remove {source_fs} {path}",
                check_ec=False,
            )

        log.info("Remove snapshot mirroring")
        if fs_mirroring_utils.remove_snapshot_mirror_peer(
            source_clients[0], source_fs, peer_uuid
        ):
            log.info("Peer removal is successful.")
        else:
            log.error("Peer removal failed.")

        log.info("Disable CephFS Snapshot mirroring")
        fs_mirroring_utils.disable_snapshot_mirroring(source_fs, source_clients[0])

        log.info("Remove Subvolumes")
        for subvolume in subvolume_list:
            fs_util_ceph1.remove_subvolume(
                source_clients[0], **subvolume, check_ec=False
            )

        log.info("Remove Subvolume Group")
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0], **subvolumegroup, check_ec=False
            )

        log.info("Delete the mounted paths")
        source_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}", check_ec=False
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}", check_ec=False
        )

        log.info("Disable mirroring mgr module on source and destination")
        fs_mirroring_utils.disable_mirroring_module(source_clients[0])
        fs_mirroring_utils.disable_mirroring_module(target_clients[0])

        log.info("Cleanup Target Client")
        fs_mirroring_utils.cleanup_target_client(target_clients[0], target_mount_path1)
        fs_mirroring_utils.cleanup_target_client(target_clients[0], target_mount_path2)

        log.info("Cleanup the User Created")
        target_clients[0].exec_command(
            sudo=True, cmd=f"ceph auth del client.{target_user}", check_ec=False
        )


def perform_fs_mirroring_operations(
    fs_mirroring_utils,
    target_fs,
    target_user,
    target_clients,
    source_fs,
    source_clients,
    target_site_name,
    permissions="r",
):
    try:
        log.info("Create a user on the target cluster for peer connection")
        auth_command = (
            f"ceph fs authorize {target_fs} client.{target_user} / {permissions}"
        )
        out, rc = target_clients[0].exec_command(sudo=True, cmd=auth_command)
        log.info(f"Enable cephfs mirroring module on the {source_fs}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs, source_clients[0])

        command = (
            f"ceph fs snapshot mirror peer_bootstrap create "
            f"{target_fs} client.{target_user} {target_site_name} -f json"
        )
        out, rc = target_clients[0].exec_command(sudo=True, cmd=command, check_ec=False)

        if not rc:
            log.error(
                f"Bootstrap token has been created with permissions {permissions}"
            )
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        raise CommandFailed(
            f"Bootstrap token has been created with permissions {permissions}"
        )
    finally:
        target_clients[0].exec_command(
            sudo=True, cmd=f"ceph auth del client.{target_user}", check_ec=False
        )
