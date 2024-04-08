import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83575625"
        log.info(f"Running CephFS tests for Polarion ID -{tc}")
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
        fs_name = "".join(random.choice(string.ascii_lowercase) for i in range(3))
        source_fs = f"cephfs_{fs_name}"
        target_fs = f"cephfs_{fs_name}"
        # create fs
        fs_util_ceph1.create_fs(source_clients[0], source_fs)
        fs_util_ceph2.create_fs(target_clients[0], target_fs)
        target_user = f"mirror_remote_{fs_name}"
        target_site_name = f"remote_site_{fs_name}"
        log.info("Deploy CephFS Mirroring Configuration")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        log.info("Create Subvolumes for adding Data")
        log.info("Scenario 1 : ")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_jul1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)
        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_jul1",
                "group_name": "subvolgroup_jul1",
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_jul2",
                "group_name": "subvolgroup_jul1",
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
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_jul1 subvolgroup_jul1",
        )
        index = subvol_path1.find("subvol_jul1/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len("subvol_jul1/")]
            log.info(subvol1_path)
        else:
            subvol1_path = subvol_path1[:-1]

        subvol1_path = subvol1_path + "/"
        # mount the client in the volume created
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
            cmd=f"ceph fs subvolume getpath {source_fs} subvol_jul2 subvolgroup_jul1",
        )
        index = subvol_path2.find("subvol_jul2/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len("subvol_jul2/")]
        else:
            subvol2_path = subvol_path2[:-1]
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
        log.info(f"{subvol1_path}")
        log.info(f"{subvol2_path}")
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {kernel_mounting_dir_1}{subvol1_path}/hello_kernel"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {fuse_mounting_dir_1}{subvol2_path}/hello_fuse"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}/.snap/{snap1}"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}/.snap/{snap2}"
        )

        log.info("Validate the Snapshot Synchronisation on Target Cluster")
        snap_count = 2
        validate_synchronisation = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, snap_count
        )
        if validate_synchronisation:
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
            raise CommandFailed("Snapshot sync failed")
        log.info("scenario 1")
        log.info("This should be done after the snapshot sync is successful.")
        log.info(
            "1. Create a Path within the filesystem(source) and enable it for mirroring."
        )
        log.info(
            "2. Compare the stat of the dir on the source node as well as on the remote node "
        )
        target_mount_path = f"/mnt/cephfs_target_{mounting_dir}_1/"
        target_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {target_mount_path}",
        )
        fs_util_ceph2.fuse_mount(
            [target_clients[0]],
            target_mount_path,
            extra_params=f"--client_fs {target_fs}",
        )

        @retry(CommandFailed, tries=5, delay=30)
        def check_perm(client1, filepath1, client2, filepath2, statlist):
            for stat in statlist:
                perm1 = fs_util_ceph1.get_stats(client1, filepath1)[stat]
                perm2 = fs_util_ceph2.get_stats(client2, filepath2)[stat]
                log.info(f"Source dir stat : {perm1}")
                log.info(f"Target dir stat : {perm2}")
                if perm1 != perm2:
                    raise CommandFailed("Source and Target dir stat are not same")

        log.info("ls in the target_mount_path")
        log.info(
            f"Target dir ls: {target_clients[0].exec_command(sudo=True, cmd=f'ls {target_mount_path}')}"
        )
        log.info("get stat of the dir on the source node such as gid uid links")
        check_perm(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol1_path}",
            target_clients[0],
            f"{target_mount_path}{subvol1_path}",
            ["Octal_Permission", "Gid", "Uid", "Links"],
        )
        log.info("scenario 2")
        log.info("modify the permission of the dir on the source node")
        log.info(
            "compare the stat of the dir on the source node as well as on the remote node - Both should be the same"
        )
        perm = 600
        snap3 = "snap_f2"
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"chmod {perm} {fuse_mounting_dir_1}{subvol1_path}",
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol1_path}/.snap/{snap3}"
        )
        check_perm(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol1_path}",
            target_clients[0],
            f"{target_mount_path}{subvol1_path}",
            ["Octal_Permission", "Gid", "Uid", "Links"],
        )
        log.info("scenario 3")
        log.info("modify the permission of the dir on the target node")
        log.info(
            "Even if the permission is modified on the target node, it should change back to the permission"
        )
        perm2 = 700
        target_clients[0].exec_command(
            sudo=True,
            cmd=f"chmod {perm2} {target_mount_path}{subvol1_path}",
        )
        check_perm(
            source_clients[0],
            f"{fuse_mounting_dir_1}{subvol1_path}",
            target_clients[0],
            f"{target_mount_path}{subvol1_path}",
            ["Octal_Permission"],
        )
        log.info("scenario 4")
        log.info("create 5 dirs and 5 files on the source node")  #
        log.info(
            "compare the stat of the dir on the source node as well as on the remote node"
        )
        source_dirs = []
        source_files = []
        perm_list = ["644", "700", "666", "646", "767"]
        rand_list = []
        for i in range(5):
            rand_name = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(5))
            )
            rand_list.append(rand_name)
            source_dir = f"{fuse_mounting_dir_1}{subvol1_path}dir_{rand_name}"
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"mkdir {source_dir}",
            )
            source_dirs.append(source_dir)
            source_file = f"{fuse_mounting_dir_1}{subvol1_path}file_{rand_name}"
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"touch {source_file}",
            )
            source_files.append(f"{source_file}")
        log.info("change permission of the dirs and files on the source node")
        for i in range(5):
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"chmod {perm_list[i]} {source_dirs[i]}",
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"chmod {perm_list[i]} {source_files[i]}",
            )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}{subvol1_path}.snap/snap_f3"
        )

        @retry(CommandFailed, tries=5, delay=10)
        def check_perm2(client1, filepath1, client2, filepath2, perm):
            perm1 = fs_util_ceph1.get_stats(client1, filepath1)["Octal_Permission"]
            perm2 = fs_util_ceph2.get_stats(client2, filepath2)["Octal_Permission"]
            log.info(f"Source dir stat : {perm1}")
            log.info(f"Target dir stat : {perm2}")
            if perm1 != perm or perm2 != perm:
                raise CommandFailed("Source and Target dir stat are not same")

        for i in range(5):
            check_perm2(
                source_clients[0],
                f"{source_dirs[i]}",
                target_clients[0],
                f"{target_mount_path}{subvol1_path}dir_{rand_list[i]}",
                perm_list[i],
            )
            check_perm2(
                source_clients[0],
                f"{source_files[i]}",
                target_clients[0],
                f"{target_mount_path}{subvol1_path}file_{rand_list[i]}",
                perm_list[i],
            )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Delete the snapshots")
        snap_del_list = ["snap_k1", "snap_f1", "snap_f2", "snap_f3"]
        for snap in snap_del_list:
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {kernel_mounting_dir_1}{subvol1_path}/.snap/{snap}",
                check_ec=False,
            )
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"rmdir {fuse_mounting_dir_1}{subvol2_path}/.snap/{snap}",
                check_ec=False,
            )
        log.info("Unmount the paths")
        paths_to_unmount = [kernel_mounting_dir_1, fuse_mounting_dir_1]
        for path in paths_to_unmount:
            source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

        log.info("Remove paths used for mirroring")
        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs, subvol2_path
        )
        log.info("Destroy CephFS Mirroring Setup")
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
            # print the file in the subvolume
            source_clients[0].exec_command(
                sudo=True,
                cmd=f"ls {kernel_mounting_dir_1}{subvolume['vol_name']}/{subvolume['subvol_name']}",
                check_ec=False,
            )
            fs_util_ceph1.remove_subvolume(
                source_clients[0],
                **subvolume,
            )
        log.info("Remove Subvolume Group")
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.remove_subvolumegroup(source_clients[0], **subvolumegroup)
        log.info("Delete the mounted paths")
        source_clients[0].exec_command(sudo=True, cmd=f"rmdir {kernel_mounting_dir_1}")
        source_clients[0].exec_command(sudo=True, cmd=f"rmdir {fuse_mounting_dir_1}")

        log.info("Cleanup Target Client")
        fs_mirroring_utils.cleanup_target_client(target_clients[0], target_mount_path)
