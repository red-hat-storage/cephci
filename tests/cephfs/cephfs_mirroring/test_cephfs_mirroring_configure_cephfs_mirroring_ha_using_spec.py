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
    """ """

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

        log.info("Remove the existing Cephfs Mirroring Service")
        source_clients[0].exec_command(sudo=True, cmd="ceph orch rm cephfs-mirror")
        time.sleep(30)

        log.info("Create cephfs-mirror service using a spec file")
        cephfs_mirror_ha_spec = {
            "service_type": "cephfs-mirror",
            "placement": {
                "hosts": [
                    cephfs_mirror_node[0].node.hostname,
                    cephfs_mirror_node[1].node.hostname,
                ],
            },
        }

        file_path = "/tmp/cephfs-mirror-ha-spec.yaml"
        source_clients[0].exec_command(
            sudo=True, cmd=f"echo '{cephfs_mirror_ha_spec}' > {file_path}"
        )
        source_clients[0].exec_command(sudo=True, cmd=f"ceph orch apply -i {file_path}")
        fs_util_ceph1.validate_services(source_clients[0], "cephfs-mirror")

        source_fs = "cephfs"
        target_fs = "cephfs"
        target_user = "mirror_remote"
        target_site_name = "remote_site"
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
        )

        log.info("Add subvolumes for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path
        )

        snaps = ["snap_k1", "snap_f1"]
        snap_count = len(snaps)
        file_names = ["hello_kernel", "hello_fuse"]
        for i, snap in enumerate(snaps):
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"touch {kernel_mounting_dir_1 if i == 0 else fuse_mounting_dir_1}"
                f"{subvol1_path if i == 0 else subvol2_path}{file_names[i]}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"mkdir {kernel_mounting_dir_1 if i == 0 else fuse_mounting_dir_1}"
                f"{subvol1_path if i == 0 else subvol2_path}.snap/{snap}",
            )

        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        log.info(f"Name of the cephfs-mirror daemon : {daemon_name}")
        asok_file1 = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name[:-1]
        )
        asok_file2 = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[1], fsid, daemon_name[1:]
        )
        log.info(
            f"Admin Socket file of {cephfs_mirror_node[0].node.hostname} daemon : {asok_file1}"
        )
        log.info(
            f"Admin Socket file of {cephfs_mirror_node[1].node.hostname} daemon : {asok_file2}"
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")

        log.info("Validate the Snapshot Synchronisation on Target Cluster")
        validate_sync_dir_on_mirror_node1 = fs_mirroring_utils.get_synced_dir_count(
            cephfs_mirror_node[0], source_clients[0], source_fs, daemon_name[:-1]
        )
        log.info(
            f"Synced directory found on {cephfs_mirror_node[0].node.hostname} "
            f"is {validate_sync_dir_on_mirror_node1}"
        )

        validate_sync_dir_on_mirror_node2 = fs_mirroring_utils.get_synced_dir_count(
            cephfs_mirror_node[1], source_clients[0], source_fs, daemon_name[1:]
        )
        log.info(
            f"Synced directory found on {cephfs_mirror_node[1].node.hostname} "
            f"is {validate_sync_dir_on_mirror_node2}"
        )

        total_dir_synced = int(validate_sync_dir_on_mirror_node1) + int(
            validate_sync_dir_on_mirror_node2
        )
        log.info(f"Total Dir synced is {total_dir_synced}")
        if total_dir_synced == snap_count:
            log.info("Total directory count matched snap_count")
        else:
            log.error("Total directory count does not match snap_count")
            raise CommandFailed(
                "Snapshot count doesn't match, Synchronisation has failed"
            )

        synced_snapshots = []
        validate_synced_snaps_on_mirror_node1 = (
            fs_mirroring_utils.extract_synced_snapshots(
                cephfs_mirror_node[0],
                source_fs,
                fsid,
                asok_file1,
                filesystem_id,
                peer_uuid,
            )
        )
        log.info(
            f"Synced directory found on {cephfs_mirror_node[0].node.hostname} is"
            f" {validate_synced_snaps_on_mirror_node1}"
        )
        synced_snapshots.append(validate_synced_snaps_on_mirror_node1)

        validate_synced_snaps_on_mirror_node2 = (
            fs_mirroring_utils.extract_synced_snapshots(
                cephfs_mirror_node[1],
                source_fs,
                fsid,
                asok_file2,
                filesystem_id,
                peer_uuid,
            )
        )
        log.info(
            f"Synced directory found on {cephfs_mirror_node[1].node.hostname} is "
            f"{validate_synced_snaps_on_mirror_node2}"
        )
        synced_snapshots.append(validate_synced_snaps_on_mirror_node2)
        log.info(f"Synced Snapshots are {synced_snapshots}")
        if all(snapshot in synced_snapshots for snapshot in snaps):
            log.info("All snapshots are synced")
        else:
            log.error("One or more snapshots are not synced")

        log.info(
            "Validate the synced snapshots and data available on the target cluster"
        )
        target_mount_paths = ["/mnt/remote_dir1", "/mnt/remote_dir2"]
        target_client_user = "client.admin"
        source_paths = [subvol1_path, subvol2_path]
        snapshot_names = snaps
        expected_files = file_names

        results = []
        successes = []

        for target_mount_path, source_path, snapshot_name, expected_file in zip(
            target_mount_paths, source_paths, snapshot_names, expected_files
        ):
            (
                success,
                result,
            ) = fs_mirroring_utils.list_and_verify_remote_snapshots_and_data(
                target_clients[0],
                target_mount_path,
                target_client_user,
                source_path,
                snapshot_name,
                expected_file,
                target_fs,
            )
            successes.append(success)
            results.append(result)
            log.info(f"Status of {snapshot_name}: {result}")

        if all(successes):
            log.info(
                "Test Completed Successfully. All snapshots and data are synced to the target cluster"
            )
        else:
            log.error(
                "Test Failed. Snapshots or Data are missing on the target cluster."
            )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Delete the snapshots")
        snapshots_to_delete = [
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/snap_k1",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/snap_f1",
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
        log.info("Cleanup Target Client")
        for target_mount_path in target_mount_paths:
            fs_mirroring_utils.cleanup_target_client(
                target_clients[0], target_mount_path
            )
