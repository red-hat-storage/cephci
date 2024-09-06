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
    CEPH-83575338 - Create more than 2 Node - CephFS Mirroring HA(say 3 or 4) and
    check if the Service has come up as expected and validate all the information

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

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details_source = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
        fs_details_target = fs_util_ceph1.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            fs_util_ceph1.create_fs(target_clients[0], target_fs)
        target_user = "mirror_remote"
        target_site_name = "remote_site"

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)
        if len(mds_names_hostname) > 2:
            hosts_list1 = mds_names_hostname[1:3]
        else:
            hosts_list1 = mds_names_hostname
        cephfs_mirror_node.extend(mds_nodes[1:3])
        mds_hosts_1 = " ".join(hosts_list1)
        log.info(f"MDS host list 1 {mds_hosts_1}")
        ha_nodes = f"{cephfs_mirror_node[0].node.hostname} {cephfs_mirror_node[1].node.hostname} {mds_hosts_1}"
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph orch apply cephfs-mirror --placement='4 {ha_nodes}'",
        )
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="yum install -y ceph-common --nogpgcheck")
        fs_util_ceph1.validate_services(source_clients[0], "cephfs-mirror")

        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        log.info("Create Subvolumes for adding Data")
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
            extra_params=f" --client_fs {source_fs}",
        )

        log.info("Add subvolumes for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path
        )

        fs_mirroring_utils.add_files_and_validate(
            source_clients,
            kernel_mounting_dir_1,
            subvol1_path,
            fuse_mounting_dir_1,
            subvol2_path,
            cephfs_mirror_node,
            source_fs,
            "ha_service_4_node",
            snap_count=1,
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
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
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(sudo=True, cmd=f"rmdir {snapshot_path}")

        log.info("Unmount the paths")
        paths_to_unmount = [kernel_mounting_dir_1, fuse_mounting_dir_1]
        for path in paths_to_unmount:
            source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

        log.info("Remove paths used for mirroring")
        paths_to_remove = [subvol1_path, subvol2_path]
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
        for subvolume in subvolume_list:
            fs_util_ceph1.remove_subvolume(
                source_clients[0],
                **subvolume,
            )

        log.info("Remove Subvolume Group")
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.remove_subvolumegroup(source_clients[0], **subvolumegroup)

        log.info("Delete the mounted paths")
        mounted_paths = [kernel_mounting_dir_1, fuse_mounting_dir_1]
        for path in mounted_paths:
            source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {path}")
        source_clients[0].exec_command(
            sudo=True,
            cmd="ceph orch apply cephfs-mirror --placement='label:cephfs-mirror'",
        )
