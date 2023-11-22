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
    CEPH-83575336 - Deploy CephFS Mirroring on n-1 cluster with few paths(directory and subvolumes) part of mirroring
    and Upon upgrade to n version, ensure the existing paths are not impacted

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
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"))
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"))
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )
        with open("variables.pkl", "rb") as file:
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
            ) = pickle.load(file)

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.info(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info("Add files into the path and create snapshot on each path")
        fs_mirroring_utils.add_files_and_validate(
            source_clients,
            kernel_mounting_dir_1,
            subvol1_path,
            fuse_mounting_dir_1,
            subvol2_path,
            cephfs_mirror_node,
            source_fs,
            "single_upgrade",
            snap_count=2,
        )

        log.info(
            "Validate the synced snapshots and data available on the target cluster"
        )
        md5sum = "md5sum_script.sh"
        target_clients[0].upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh",
            dst=f"/root/{md5sum}",
        )
        source_clients[0].upload_file(
            sudo=True,
            src="tests/cephfs/cephfs_mirror_upgrade/md5sum_script.sh",
            dst=f"/root/{md5sum}",
        )
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol2_path,
            source_clients[0],
            fuse_mounting_dir_1,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol1_path,
            source_clients[0],
            kernel_mounting_dir_1,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")
        subvolumegroup_list_upgrade = [
            {"vol_name": source_fs, "group_name": "subvolgroup_3_upgrade"},
        ]
        for subvolumegroup in subvolumegroup_list_upgrade:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list_upgrade = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_3_upgrade",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_3_upgrade",
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list_upgrade:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolume on kernel and 1 subvloume on Fuse → Client1")
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        log.info("Get the path of subvolume1 on  filesystem")
        subvol_path1, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_1 subvolgroup_3_upgrade",
        )
        index = subvol_path1.find("subvol_1/")
        if index != -1:
            subvol1_path_upgrade = subvol_path1[: index + len("subvol_1/")]
        else:
            subvol1_path_upgrade = subvol_path1
        log.info(subvol1_path_upgrade)

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )
        log.info("Get the path of subvolume2 on filesystem")
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_2 subvolgroup_3_upgrade",
        )
        index = subvol_path2.find("subvol_2/")
        if index != -1:
            subvol2_path_upgrade = subvol_path2[: index + len("subvol_2/")]
        else:
            subvol2_path_upgrade = subvol_path2
        log.info(subvol2_path)
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_2,
            extra_params=f"--client_fs {source_fs}",
        )

        log.info("Add subvolumes for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path_upgrade
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path_upgrade
        )

        log.info("Add files into the path and create snapshot on each path")
        fs_mirroring_utils.add_files_and_validate(
            source_clients,
            kernel_mounting_dir_1,
            subvol1_path_upgrade,
            fuse_mounting_dir_1,
            subvol2_path_upgrade,
            cephfs_mirror_node,
            source_fs,
            "single_upgrade_new_path",
            snap_count=4,
        )

        log.info(
            "Validate the synced snapshots and data available on the target cluster"
        )
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol2_path_upgrade,
            source_clients[0],
            fuse_mounting_dir_2,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol1_path_upgrade,
            source_clients[0],
            kernel_mounting_dir_2,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)
        if len(mds_names_hostname) > 2:
            hosts_list1 = mds_names_hostname[1:3]
        else:
            hosts_list1 = mds_names_hostname

        mds_hosts_1 = " ".join(hosts_list1)
        log.info(f"MDS host list 1 {mds_hosts_1}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph orch apply cephfs-mirror --placement='2 {mds_hosts_1}'",
        )
        fs_util_ceph1.validate_services(source_clients[0], "cephfs-mirror")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        if len(mds_nodes) > 2:
            cephfs_mirror_node = mds_nodes[1:3]
        else:
            cephfs_mirror_node = mds_nodes
        fs_mirroring_utils.add_files_and_validate(
            source_clients,
            kernel_mounting_dir_1,
            subvol1_path,
            fuse_mounting_dir_1,
            subvol2_path,
            cephfs_mirror_node,
            source_fs,
            "ha_service_upgrade",
            snap_count=2,
        )

        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol2_path,
            source_clients[0],
            fuse_mounting_dir_1,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")
        out, rc = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data_checksum(
            target_clients[0],
            "client.admin",
            subvol1_path,
            source_clients[0],
            kernel_mounting_dir_1,
            target_fs,
        )
        if not out:
            raise CommandFailed(f"{rc}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Delete the snapshots")
        snapshots_to_delete = [
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/snap_k1*",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/snap_f1*",
            f"{kernel_mounting_dir_2}{subvol1_path_upgrade}.snap/snap_k1*",
            f"{fuse_mounting_dir_2}{subvol2_path_upgrade}.snap/snap_f1*",
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
            )

        log.info("Unmount the paths")

        paths_to_unmount = [
            kernel_mounting_dir_1,
            kernel_mounting_dir_2,
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
        ]
        for path in paths_to_unmount:
            source_clients[0].exec_command(
                sudo=True, cmd=f"umount -l {path}", check_ec=False
            )

        log.info("Remove paths used for mirroring")
        paths_to_remove = [
            subvol1_path,
            subvol2_path,
            subvol2_path_upgrade,
            subvol1_path_upgrade,
        ]
        for path in paths_to_remove:
            fs_mirroring_utils.remove_path_from_mirroring(
                source_clients[0], source_fs, path
            )

        log.info("Destroy CephFS Mirroring setup.")
        fs_mirroring_utils.destroy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            peer_uuid,
        )

        log.info("Remove Subvolumes")
        for subvolume in subvolume_list_upgrade + subvolume_list:
            fs_util_ceph1.remove_subvolume(
                source_clients[0], **subvolume, check_ec=False
            )

        log.info("Remove Subvolume Group")
        for subvolumegroup in subvolumegroup_list_upgrade + subvolumegroup_list:
            fs_util_ceph1.remove_subvolumegroup(
                source_clients[0], **subvolumegroup, check_ec=False
            )

        log.info("Delete the mounted paths")

        for path in paths_to_unmount:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {path}", timeout=180, check_ec=False
            )
