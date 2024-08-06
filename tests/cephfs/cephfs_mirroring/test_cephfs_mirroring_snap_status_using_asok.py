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
        source_fs = "cephfs"
        target_fs = "cephfs"
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

        subvol_group_name = "subvolgroup_1"
        subvol_name = "subvol"
        subvol_size = "5368709120"
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}"
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}"
        subvol_details = [
            {
                "subvol_name": f"{subvol_name}_1",
                "subvol_size": subvol_size,
                "mount_type": "kernel",
                "mount_dir": f"{kernel_mounting_dir}_1",
            },
            {
                "subvol_name": f"{subvol_name}_2",
                "subvol_size": subvol_size,
                "mount_type": "fuse",
                "mount_dir": f"{fuse_mounting_dir}_1",
            },
        ]
        subvolume_paths = fs_mirroring_utils.setup_subvolumes_and_mounts(
            source_fs,
            source_clients[0],
            fs_util_ceph1,
            subvol_group_name,
            subvol_details,
        )
        log.info(f"Subvolume Paths are : {subvolume_paths}")

        log.info("Add subvolumes for mirroring to remote location")
        for subvol_path in subvolume_paths:
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )

        log.info("Fetch the Status of Snapshot Mirroring")
        out, _ = source_clients[0].exec_command(
            sudo=True, cmd="ceph fs snapshot mirror daemon status -f json"
        )
        snap_mirror_status = json.loads(out)
        log.info(f"Snapshot Mirroring Status : {snap_mirror_status}")

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}"
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}"
        subvol_path1 = subvolume_paths[0]
        subvol_path2 = subvolume_paths[1]

        log.info("Scenario 1 : Validate Snap Sync Status using asok file")
        fs_mirror_peer_status_before_snap_creation = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status before Snap Creation : {fs_mirror_peer_status_before_snap_creation}"
        )

        log.info("Add Data and Validate Snapshot Status")
        snap_name = "snap"
        file_name = "file"
        mount_paths = [
            f"{kernel_mounting_dir}_1{subvol_path1}",
            f"{fuse_mounting_dir}_1{subvol_path2}",
        ]
        for mount_path in mount_paths:
            source_clients[0].exec_command(
                sudo=True, cmd=f"touch {mount_path}{file_name}"
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {mount_path}.snap/{snap_name}"
            )
        time.sleep(60)
        fs_mirror_peer_status_after_snap_creation = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status after Snap Creation : {fs_mirror_peer_status_after_snap_creation}"
        )

        results = fs_mirroring_utils.validate_snaps_status_increment(
            fs_mirror_peer_status_before_snap_creation,
            fs_mirror_peer_status_after_snap_creation,
            snap_status="snaps_synced",
        )
        for path, result in results.items():
            if result:
                log.info(f"Validation passed for {path}: snaps_synced incremented.")
            else:
                log.error(
                    f"Validation failed for {path}: snaps_synced did not increment."
                )
                raise CommandFailed("Validation Failed")

        log.info("Scenario 2 : Validate Snap Rename Status using asok file")
        fs_mirror_peer_status_before_snap_rename = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status before Snap Rename : {fs_mirror_peer_status_before_snap_rename}"
        )

        log.info("Rename Snapshot name and Validate Snapshot Rename Update")
        snap_name = "snap"
        new_snap_name = "snap_new"
        mount_paths = [
            f"{kernel_mounting_dir}_1{subvol_path1}",
            f"{fuse_mounting_dir}_1{subvol_path2}",
        ]
        for mount_path in mount_paths:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"mv {mount_path}.snap/{snap_name} {mount_path}.snap/{new_snap_name}",
            )
        time.sleep(60)
        fs_mirror_peer_status_after_snap_rename = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status after Snap Rename : {fs_mirror_peer_status_after_snap_rename}"
        )

        results = fs_mirroring_utils.validate_snaps_status_increment(
            fs_mirror_peer_status_before_snap_rename,
            fs_mirror_peer_status_after_snap_rename,
            snap_status="snaps_renamed",
        )
        for path, result in results.items():
            if result:
                log.info(f"Validation passed for {path}: snaps_renamed incremented.")
            else:
                log.error(
                    f"Validation failed for {path}: snaps_renamed did not increment."
                )
                raise CommandFailed("Validation Failed")

        log.info("Scenario 3 : Validate Snap Delete Status using asok file")
        fs_mirror_peer_status_before_snap_delete = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status before Snap Creation : {fs_mirror_peer_status_before_snap_delete}"
        )

        log.info("Delete Snapshot name and Validate Snapshot Delete Update")
        new_snap_name = "snap_new"
        mount_paths = [
            f"{kernel_mounting_dir}_1{subvol_path1}",
            f"{fuse_mounting_dir}_1{subvol_path2}",
        ]
        for mount_path in mount_paths:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {mount_path}.snap/{new_snap_name}"
            )
        time.sleep(60)
        fs_mirror_peer_status_after_snap_delete = (
            fs_mirroring_utils.get_fs_mirror_peer_status_using_asok(
                cephfs_mirror_node[0],
                source_clients[0],
                source_fs,
            )
        )
        log.info(
            f"Peer Mirror Status after Snap Creation : {fs_mirror_peer_status_after_snap_delete}"
        )

        results = fs_mirroring_utils.validate_snaps_status_increment(
            fs_mirror_peer_status_before_snap_delete,
            fs_mirror_peer_status_after_snap_delete,
            snap_status="snaps_deleted",
        )
        for path, result in results.items():
            if result:
                log.info(f"Validation passed for {path}: snaps_deleted incremented.")
            else:
                log.error(
                    f"Validation failed for {path}: snaps_deleted did not increment."
                )
                raise CommandFailed("Validation Failed")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):

            log.info("Unmount the paths")
            paths_to_unmount = [f"{kernel_mounting_dir}_1", f"{fuse_mounting_dir}_1"]
            for path in paths_to_unmount:
                source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

            log.info("Remove paths used for mirroring")
            for subvol_path in subvolume_paths:
                fs_mirroring_utils.remove_path_from_mirroring(
                    source_clients[0], source_fs, subvol_path
                )

            log.info("Destroy CephFS Mirroring setup.")
            peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
                source_clients[0], source_fs
            )
            fs_mirroring_utils.destroy_cephfs_mirroring(
                source_fs,
                source_clients[0],
                target_fs,
                target_clients[0],
                target_user,
                peer_uuid,
            )

            log.info("Remove Subvolumes")
            subvolume_list = [
                {
                    "vol_name": source_fs,
                    "subvol_name": f"{subvol_name}_1",
                    "group_name": subvol_group_name,
                    "size": "5368709120",
                },
                {
                    "vol_name": source_fs,
                    "subvol_name": f"{subvol_name}_2",
                    "group_name": subvol_group_name,
                    "size": "5368709120",
                },
            ]

            for subvolume in subvolume_list:
                fs_util_ceph1.remove_subvolume(
                    source_clients[0],
                    **subvolume,
                )

            log.info("Remove Subvolume Group")
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0],
                vol_name=source_fs,
                group_name=subvol_group_name,
                validate=False,
            )

            log.info("Delete the mounted paths")
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir}"
            )
            source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {fuse_mounting_dir}")
