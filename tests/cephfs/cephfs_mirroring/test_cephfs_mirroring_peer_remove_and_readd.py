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
    CEPH-83632394 - Validate peer removal and re-addition using bootstrap_import
    resumes snapshot synchronization correctly.

    Test Workflow:
        1. Validate pre-requisites (clients on both clusters).
        2. Prepare clients, authenticate, ensure CephFS available on both clusters.
        3. Configure CephFS mirroring using peer_bootstrap.
        4. Verify peer addition and registration.
        5. Create subvolumes, mount on kernel and FUSE, add to mirroring.
        6. Create snapshots and validate sync to remote cluster.
        7. Remove the mirror peer using peer_remove and verify clean removal.
        8. Re-import peer using bootstrap_import.
        9. Verify peer re-registration and sync resumes correctly.
    """
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
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
            log.error(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details_source = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)
        fs_details_target = fs_util_ceph2.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            fs_util_ceph2.create_fs(target_clients[0], target_fs)
        fs_util_ceph2.wait_for_mds_process(target_clients[0], target_fs)

        target_user = "mirror_remote"
        target_site_name = "remote_site"

        log.info("Step 3: Deploy CephFS Mirroring using peer_bootstrap")
        token = fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        log.info("Step 4: Verify peer addition")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        if not peer_uuid:
            raise CommandFailed("Peer UUID not found for %s" % source_fs)
        log.info("Peer UUID for %s: %s", source_fs, peer_uuid)

        if (
            fs_mirroring_utils.validate_peer_connection(
                source_clients[0], source_fs, target_site_name, target_user, target_fs
            )
            != 0
        ):
            raise CommandFailed("Peer connection validation failed for %s" % source_fs)

        log.info("Step 5: Create subvolumes and mount")
        subvol_group_name = "subvolgroup_1"
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": subvol_group_name},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_1",
                "group_name": subvol_group_name,
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_2",
                "group_name": subvol_group_name,
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()

        subvol_path1, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_1 {subvol_group_name}",
        )
        subvol_path1 = subvol_path1.strip()
        index = subvol_path1.find("subvol_1/")
        subvol1_path = (
            subvol_path1[: index + len("subvol_1/")] if index != -1 else subvol_path1
        )
        if not subvol1_path.endswith("/"):
            subvol1_path += "/"

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        subvol_path2, _ = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_2 {subvol_group_name}",
        )
        subvol_path2 = subvol_path2.strip()
        index = subvol_path2.find("subvol_2/")
        subvol2_path = (
            subvol_path2[: index + len("subvol_2/")] if index != -1 else subvol_path2
        )
        if not subvol2_path.endswith("/"):
            subvol2_path += "/"

        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {source_fs}",
        )

        log.info("Step 6: Add paths for mirroring, create snapshots, validate sync")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path
        )

        snap1 = "snap_k1"
        snap2 = "snap_f1"
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {kernel_mounting_dir_1}{subvol1_path}hello_kernel"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {fuse_mounting_dir_1}{subvol2_path}hello_fuse"
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}",
            check_ec=False,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}"
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}",
            check_ec=False,
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}"
        )

        log.info("Step 7: Validate snapshot sync before peer removal")
        snap_count = 2
        validate_sync = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, snap_count
        )
        if validate_sync:
            log.error("Snapshot Synchronisation failed before peer removal")
            raise CommandFailed("Snapshot Synchronisation failed")
        log.info("Snapshot sync validated successfully before peer removal")

        log.info("Step 8: Remove mirror peer and verify clean removal")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        result = fs_mirroring_utils.remove_snapshot_mirror_peer(
            source_clients[0], source_fs, peer_uuid
        )
        if not result:
            raise CommandFailed(
                "Peer removal failed for UUID '%s' on filesystem '%s'"
                % (peer_uuid, source_fs)
            )
        log.info(
            "Peer '%s' removed successfully from '%s' - clean removal confirmed",
            peer_uuid,
            source_fs,
        )

        log.info("Step 9: Re-import peer using bootstrap_import")
        token = fs_mirroring_utils.create_peer_bootstrap(
            target_fs, target_user, target_site_name, target_clients[0]
        )
        fs_mirroring_utils.import_peer_bootstrap(source_fs, token, source_clients[0])

        log.info("Step 10: Verify peer re-registration and sync resumes")
        new_peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        if not new_peer_uuid:
            raise CommandFailed(
                "Peer UUID not found after re-import for %s" % source_fs
            )
        log.info("New peer UUID after re-import: %s", new_peer_uuid)

        if (
            fs_mirroring_utils.validate_peer_connection(
                source_clients[0], source_fs, target_site_name, target_user, target_fs
            )
            != 0
        ):
            raise CommandFailed(
                "Peer connection validation failed after re-import for %s" % source_fs
            )

        snap3 = "snap_k2_readd"
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"touch {kernel_mounting_dir_1}{subvol1_path}hello_after_readd",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"rmdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap3}",
            check_ec=False,
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap3}",
        )

        snap_count = 2
        validate_sync_after = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, snap_count
        )
        if validate_sync_after:
            log.error("Snapshot sync failed after peer re-addition")
            raise CommandFailed("Snapshot sync failed after peer re-addition")
        log.info("Snapshot sync resumed successfully after peer re-addition")

        log.info(
            "Test Completed Successfully. Peer removal and re-addition via "
            "bootstrap_import works correctly with sync resuming after re-add."
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        try:
            snapshots_to_delete = [
                f"{kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}",
                f"{fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}",
                f"{kernel_mounting_dir_1}{subvol1_path}.snap/{snap3}",
            ]
            for snapshot_path in snapshots_to_delete:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
                )
        except Exception as e:
            log.warning("Snapshot cleanup failed: %s", e)

        try:
            paths_to_unmount = [kernel_mounting_dir_1, fuse_mounting_dir_1]
            for path in paths_to_unmount:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"umount -l {path}", check_ec=False
                )
        except Exception as e:
            log.warning("Unmount failed: %s", e)

        try:
            for path in [subvol1_path, subvol2_path]:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, path
                )
        except Exception as e:
            log.warning("Remove mirroring paths failed: %s", e)

        try:
            cleanup_peer = fs_mirroring_utils.get_peer_uuid_by_name(
                source_clients[0], source_fs
            )
            fs_mirroring_utils.destroy_cephfs_mirroring(
                source_fs,
                source_clients[0],
                target_fs,
                target_clients[0],
                target_user,
                cleanup_peer,
            )
        except Exception as e:
            log.warning("Destroy mirroring failed: %s", e)

        try:
            for subvolume in subvolume_list:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0], **subvolume, check_ec=False
                )
            for subvolumegroup in subvolumegroup_list:
                fs_util_ceph1.remove_subvolumegroup(
                    source_clients[0], **subvolumegroup, check_ec=False
                )
        except Exception as e:
            log.warning("Subvolume cleanup failed: %s", e)

        try:
            for path in [kernel_mounting_dir_1, fuse_mounting_dir_1]:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rm -rf {path}", check_ec=False
                )
        except Exception as e:
            log.warning("Mount path cleanup failed: %s", e)
