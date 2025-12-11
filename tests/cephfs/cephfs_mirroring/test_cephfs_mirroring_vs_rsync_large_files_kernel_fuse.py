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
    CEPH- - Performs rsync vs mirroring tests for large files with kernel and fuse mounts.

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
        fs_util_ceph1.setup_ssh_root_keys(clients=source_clients + target_clients)
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
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "32212254720",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_1",
                "size": "32212254720",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_3",
                "group_name": "subvolgroup_1",
                "size": "32212254720",
            },
            {
                "vol_name": source_fs,
                "subvol_name": "subvol_4",
                "group_name": "subvolgroup_1",
                "size": "32212254720",
            },
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount FS as kernel and fuse → Client1")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )

        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {source_fs}",
        )
        sub_vol_paths = {}
        for subvol in ["subvol_1", "subvol_2", "subvol_3", "subvol_4"]:
            log.info(
                f"Get the path of {subvol} on filesystem and add them for mirroring to remote location"
            )
            subvol_path, rc = source_clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {source_fs} {subvol} subvolgroup_1",
            )
            index = subvol_path.find(f"{subvol}/")
            if index != -1:
                subvol_path = subvol_path[: index + len(f"{subvol}/")]
            else:
                subvol_path = subvol_path
            log.info(subvol_path)
            log.info("Add subvolumes for mirroring to remote location")
            fs_mirroring_utils.add_path_for_mirroring(
                source_clients[0], source_fs, subvol_path
            )
            sub_vol_paths[subvol] = subvol_path
        dir_struct = {}
        for k, v in sub_vol_paths.items():
            if k in ["subvol_1", "subvol_3"]:
                dir_struct[k] = f"{kernel_mounting_dir_1}{v}"
            else:
                dir_struct[k] = f"{fuse_mounting_dir_1}{v}"

        file_name1 = "hello_kernel"
        file_name2 = "hello_fuse"

        # results = {
        #     "subvol_1": {"create": 0, "mirror": 0, "rsync_live_head": 0, "rsync_snap": 0},
        #     "subvol_2": {"create": 0, "mirror": 0, "rsync_live_head": 0, "rsync_snap": 0},
        #     "subvol_3": {"create": 0, "mirror": 0, "rsync_live_head": 0, "rsync_snap": 0},
        #     "subvol_4": {"create": 0, "mirror": 0, "rsync_live_head": 0, "rsync_snap": 0},
        # }

        log.info("Adding data to subvol_1: Single 5G file")
        # start = time.time()
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {dir_struct['subvol_1']}{mounting_dir};"
            f"dd if=/dev/urandom of={dir_struct['subvol_1']}{mounting_dir}/{file_name1} bs=1M count=5120",
        )
        # results["subvol_1"]["create"] = time.time() - start
        log.info("Adding data to subvol_2: Single 5G file")
        # start = time.time()
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {dir_struct['subvol_2']}{mounting_dir};"
            f"dd if=/dev/urandom of={dir_struct['subvol_2']}{mounting_dir}/{file_name2} bs=1M count=5120",
        )
        # results["subvol_2"]["create"] = time.time() - start
        log.info("Adding data to subvol_3: Multiple 1G files")
        cmd = (
            f"mkdir -p {dir_struct['subvol_3']}{mounting_dir}; "
            f"for i in {{1..5}}; do "
            f"dd if=/dev/urandom of={dir_struct['subvol_3']}{mounting_dir}/{file_name1}_$i bs=1M count=1024; "
            f"done"
        )
        # start = time.time()
        source_clients[0].exec_command(sudo=True, cmd=cmd)
        # results["subvol_3"]["create"] = time.time() - start
        log.info("Adding data to subvol_4: Multiple 1G files")
        cmd = (
            f"mkdir -p {dir_struct['subvol_4']}{mounting_dir}; "
            f"for i in {{1..5}}; do "
            f"dd if=/dev/urandom of={dir_struct['subvol_4']}{mounting_dir}/{file_name2}_$i bs=1M count=1024; "
            f"done"
        )
        # start = time.time()
        source_clients[0].exec_command(sudo=True, cmd=cmd)
        # results["subvol_4"]["create"] = time.time() - start
        snapshots = [
            "snap_k1_single_large_file",
            "snap_f1_single_large_file",
            "snap_k1_multiple_large_files",
            "snap_f1_multiple_large_files",
        ]

        log.info("Creating snapshots")
        for path, snapshot_name in zip(dir_struct.values(), snapshots):
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {path}.snap/{snapshot_name}"
            )

        log.info("Validate the Snapshot Synchronisation on Target Cluster")
        validate_syncronisation = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, len(snapshots)
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

        log.info("Validate if the Snapshots are syned to Target Cluster")
        #     snapshot_to_subvol = {
        #     "snap_k1_single_large_file": "subvol_1",
        #     "snap_f1_single_large_file": "subvol_2",
        #     "snap_k1_multiple_large_files": "subvol_3",
        #     "snap_f1_multiple_large_files": "subvol_4",
        # }
        for snap in snapshots:
            result_snap = fs_mirroring_utils.validate_snapshot_sync_status(
                cephfs_mirror_node[0],
                source_fs,
                snap,
                fsid,
                asok_file,
                filesystem_id,
                peer_uuid,
            )

            if result_snap:
                log.info(f"Snapshot '{result_snap['snapshot_name']}' has been synced:")
                log.info(
                    f"Sync Duration: {result_snap['sync_duration']} of '{result_snap['snapshot_name']}'"
                )
                log.info(
                    f"Sync Time Stamp: {result_snap['sync_time_stamp']} of '{result_snap['snapshot_name']}'"
                )
                log.info(
                    f"Snaps Synced: {result_snap['snaps_synced']} of '{result_snap['snapshot_name']}'"
                )
                # subvol_name = snapshot_to_subvol[snap]
                # results[subvol_name]["mirror"] = int(result_snap['sync_duration'])

            log.info(f"snapshot {snap} synced.")
        else:
            log.error(f"snapshot {snap} not found or not synced.")

        target_remote_kernel = "/mnt/remote_kernel"
        target_remote_fuse = "/mnt/remote_fuse"

        single_large_k1 = f"{target_remote_kernel}/single_large_k1"
        single_large_f1 = f"{target_remote_fuse}/single_large_f1"
        single_large_k2_snap = f"{target_remote_kernel}/single_large_k2_snap"
        single_large_f2_snap = f"{target_remote_fuse}/single_large_f2_snap"
        multiple_large_k1 = f"{target_remote_kernel}/multiple_large_k1"
        multiple_large_f1 = f"{target_remote_fuse}/multiple_large_f1"
        multiple_large_k2_snap = f"{target_remote_kernel}/multiple_large_k2_snap"
        multiple_large_f2_snap = f"{target_remote_fuse}/multiple_large_f2_snap"

        ceph2_mon_node_ips = fs_util_ceph2.get_mon_node_ips()

        fs_util_ceph2.kernel_mount(
            [target_clients[0]],
            target_remote_kernel,
            ",".join(ceph2_mon_node_ips),
            extra_params=f",fs={target_fs}",
        )

        fs_util_ceph2.fuse_mount(
            [target_clients[0]],
            target_remote_fuse,
            extra_params=f" --client_fs {target_fs}",
        )
        for dir1 in [
            single_large_k1,
            single_large_f1,
            single_large_k2_snap,
            single_large_f2_snap,
            multiple_large_k1,
            multiple_large_f1,
            multiple_large_k2_snap,
            multiple_large_f2_snap,
        ]:
            target_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {dir1}")

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_1']}{mounting_dir}",
                target_path=single_large_k1,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_1"]["rsync_live_head"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_2']}{mounting_dir}",
                target_path=single_large_f1,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_2"]["rsync_live_head"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_1']}.snap/{snapshots[0]}",
                target_path=single_large_k2_snap,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_1"]["rsync_snap"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_2']}.snap/{snapshots[1]}",
                target_path=single_large_f2_snap,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_2"]["rsync_snap"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_3']}{mounting_dir}",
                target_path=multiple_large_k1,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_3"]["rsync_live_head"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_4']}{mounting_dir}",
                target_path=multiple_large_f1,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_4"]["rsync_live_head"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_3']}.snap/{snapshots[2]}",
                target_path=multiple_large_k2_snap,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_3"]["rsync_snap"] = time.time() - start
        log.info(out)
        log.info(err)

        # start = time.time()
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{dir_struct['subvol_4']}.snap/{snapshots[3]}",
                target_path=multiple_large_f2_snap,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        # results["subvol_4"]["rsync_snap"] = time.time() - start
        log.info(out)
        log.info(err)
        # # Prepare data
        # table = []
        # for subvol, metrics in results.items():
        #     table.append([
        #         subvol,
        #         round(metrics["create"], 2),
        #         round(metrics["mirror"], 2),
        #         round(metrics["rsync_live_head"], 2),
        #         round(metrics["rsync_snap"], 2)
        #     ])

        # # Print table
        # print(tabulate(
        #     table,
        #     headers=["Subvol", "Create (s)", "Mirror (s)", "Rsync live head(s)", "Rsync snap(s)"],
        #     tablefmt="grid"
        # ))

        log.info(
            "Test Completed Successfully.All snapshots are synced to target cluster"
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        if config.get("cleanup", True):
            log.info("Delete the snapshots")
            for path, snapshot_name in zip(dir_struct.values(), snapshots):
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rmdir {path}.snap/{snapshot_name}"
                )
            log.info("Unmount the paths")
            paths_to_unmount = [kernel_mounting_dir_1, fuse_mounting_dir_1]
            for path in paths_to_unmount:
                source_clients[0].exec_command(sudo=True, cmd=f"umount -l {path}")

            log.info("Remove paths used for mirroring")
            for path in sub_vol_paths.values():
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
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}"
            )
            source_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}"
            )

            log.info("Cleanup Target Client")
            fs_mirroring_utils.cleanup_target_client(
                target_clients[0], target_remote_kernel
            )
            fs_mirroring_utils.cleanup_target_client(
                target_clients[0], target_remote_fuse
            )
