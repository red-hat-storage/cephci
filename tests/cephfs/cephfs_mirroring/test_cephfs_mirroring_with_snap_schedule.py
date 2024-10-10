import json
import random
import string
import time
import traceback

from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83598968 - Validate the snapshot synchronisation by using snap schedules

    This function automates the deployment and validation of CephFS mirroring, including setting up
    snapshot schedules, data mirroring between source and target clusters, and verifying that snapshots
    are successfully synced between clusters.

    Args:
        ceph_cluster (CephCluster): A Ceph cluster object used to interact with the Ceph environment.
        **kw: Dictionary of keyword arguments containing:
            - config (dict): Configuration parameters for the Ceph setup and test.
            - ceph_cluster_dict (dict): Dictionary containing details of two Ceph clusters (source and target).
            - test_data (dict): Additional test data configurations.

    Test Workflow:
        1. Validates pre-requisites by ensuring there are clients available on both the source and target clusters.
        2. Prepares clients, authenticates them, and ensures the CephFS filesystem is available on both clusters.
        3. Configures CephFS mirroring between the source and target Ceph clusters.
        4. Creates subvolumes and mounts them on both kernel and FUSE clients.
        5. Adds subvolumes to the mirroring setup and initiates the data synchronization.
        6. Enables snapshot schedules for the created subvolumes with a 1-minute interval.
        7. Verifies the snapshots created and checks if they have been successfully synced between clusters.
        8. Deactivates the snapshot schedules and captures the sync statistics.
        9. Compares the snapshot counts between the source and target clusters to ensure proper synchronization.

    Returns:
        int:
            - 0 on success when snapshot counts match and mirroring is successful.
            - 1 on failure if there is any mismatch in snapshot counts or any part of the process fails.

    Cleanup:
        - Removes all created snapshot schedules.
        - Unmounts and removes the mounted directories and paths.
        - Deletes CephFS mirroring configuration and removes the associated subvolumes and subvolume groups.
        - Removes the CephFS filesystem from both the source and target clusters.
    """
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        snap_util = SnapUtils(ceph_cluster)
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
        source_fs = "cephfs_ss" if not erasure else "cephfs_ss-ec"
        target_fs = "cephfs_ss" if not erasure else "cephfs_ss-ec"
        fs_details_source = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
        fs_details_target = fs_util_ceph1.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            fs_util_ceph2.create_fs(target_clients[0], target_fs)

        target_user = "mirror_remote_ss"
        target_site_name = "remote_site_ss"
        fs_details = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details:
            fs_util_ceph1.create_fs(client=source_clients[0], vol_name=source_fs)
            fs_util_ceph2.wait_for_mds_process(source_clients[0], source_fs)
        fs_details = fs_util_ceph2.get_fs_info(target_clients[0], target_fs)
        if not fs_details:
            fs_util_ceph2.create_fs(client=target_clients[0], vol_name=target_fs)
            fs_util_ceph2.wait_for_mds_process(target_clients[0], target_fs)
        log.info("Deploy CephFS Mirroring Configuration")
        token = fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )
        log.info(token)

        subvolgroup_name = "subvolgroup_1"
        subvolume_name1 = "subvol_1"
        subvolume_name2 = "subvol_2"
        log.info("Create Subvolumes for adding Data")
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": subvolgroup_name},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": subvolume_name1,
                "group_name": subvolgroup_name,
                "size": "5368709120",
            },
            {
                "vol_name": source_fs,
                "subvol_name": subvolume_name2,
                "group_name": subvolgroup_name,
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
        log.info(f"Get the path of {subvolume_name1} on  filesystem")
        subvol_path1, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} {subvolume_name1} {subvolgroup_name}",
        )
        index = subvol_path1.find(f"{subvolume_name1}/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len(f"{subvolume_name1}/")]
        else:
            subvol1_path = subvol_path1
        log.info(subvol1_path)
        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs}",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs} {subvolume_name2} {subvolgroup_name}",
        )
        index = subvol_path2.find(f"{subvolume_name2}/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len(f"{subvolume_name2}/")]
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

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {kernel_mounting_dir_1}{subvol1_path}{mounting_dir};"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 10 "
            f"--files 50 --files-per-dir 5 --dirs-per-dir 5 --top "
            f"{kernel_mounting_dir_1}{subvol1_path}{mounting_dir}",
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {fuse_mounting_dir_1}{subvol2_path}{mounting_dir};"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 10 "
            f"--files 50 --files-per-dir 5 --dirs-per-dir 5 --top "
            f"{fuse_mounting_dir_1}{subvol2_path}{mounting_dir}",
        )

        log.info("Enable snap schedule on primary cluster")
        snap_util.enable_snap_schedule(source_clients[0])
        time.sleep(10)
        log.info("Allow minutely Snap Schedule on primry cluster")
        snap_util.allow_minutely_schedule(source_clients[0], allow=True)
        time.sleep(10)

        log.info("Creating snapshot schedules for two paths with a 1-minute interval")
        interval = "1m"
        snap_path1 = f"{kernel_mounting_dir_1}{subvol1_path}"
        snap_path2 = f"{fuse_mounting_dir_1}{subvol2_path}"
        snap_params_1 = {
            "client": source_clients[0],
            "path": subvol1_path,
            "sched": interval,
            "fs_name": source_fs,
            "validate": True,
        }
        snap_params_2 = {
            "client": source_clients[0],
            "path": subvol2_path,
            "sched": interval,
            "fs_name": source_fs,
            "validate": True,
        }

        result_1 = snap_util.create_snap_schedule(snap_params_1)
        if result_1 != 0:
            log.error(f"Failed to create snapshot schedule for {subvol1_path}")
            return 1

        result_2 = snap_util.create_snap_schedule(snap_params_2)
        if result_2 != 0:
            log.error(f"Failed to create snapshot schedule for {subvol2_path}")
            return 1
        log.info("Snapshot schedules for both paths created successfully.")

        sched_list1 = snap_util.get_snap_schedule_list(
            source_clients[0], subvol1_path, source_fs
        )
        log.info(f"Snap Schedule list for {subvol1_path} : {sched_list1}")
        sched_list2 = snap_util.get_snap_schedule_list(
            source_clients[0], subvol2_path, source_fs
        )
        log.info(f"Snap Schedule list for {subvol2_path} : {sched_list2}")

        time.sleep(360)

        log.info("Deactivate Snap Schedule")
        snap_util.deactivate_snap_schedule(
            source_clients[0],
            subvol1_path,
            sched_val=interval,
            fs_name=source_fs,
        )
        snap_util.deactivate_snap_schedule(
            source_clients[0],
            subvol2_path,
            sched_val=interval,
            fs_name=source_fs,
        )

        time.sleep(30)

        snap_schedule_list1 = snap_util.get_scheduled_snapshots(
            source_clients[0], snap_path1
        )
        log.info(
            f"list of scheduled snapshots from client mount path : {snap_schedule_list1}"
        )
        snap_schedule_list2 = snap_util.get_scheduled_snapshots(
            source_clients[0], snap_path2
        )
        log.info(
            f"list of scheduled snapshots from client mount path : {snap_schedule_list2}"
        )

        log.info("Capture the snapshot created count")
        snap_count1, rc = source_clients[0].exec_command(
            sudo=True, cmd=f'ls -lrt {snap_path1}/.snap/| grep "scheduled" | wc -l'
        )
        snap_count1 = int(snap_count1.strip())
        log.info(f"Snap count for {subvol1_path} is {snap_count1}")
        snap_count2, rc = source_clients[0].exec_command(
            sudo=True, cmd=f'ls -lrt {snap_path2}/.snap/| grep "scheduled" | wc -l'
        )
        snap_count2 = int(snap_count2.strip())
        log.info(f"Snap count for {subvol2_path} is {snap_count2}")

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
        log.info(
            f"Admin Socket file of cephfs-mirror daemon : {asok_file[cephfs_mirror_node[0].node.hostname][1]}"
        )
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")

        snaps_synced1 = get_snaps_synced(
            source_fs, fsid, asok_file, filesystem_id, peer_uuid, subvol1_path
        )
        snaps_synced2 = get_snaps_synced(
            source_fs, fsid, asok_file, filesystem_id, peer_uuid, subvol1_path
        )
        if snaps_synced1 is not None and snaps_synced2 is not None:
            log.info(
                f"Snaps synced for path '{subvol1_path}' is {snaps_synced1} & '{subvol2_path}' is {snaps_synced2}"
            )

            if snap_count1 != snaps_synced1:
                log.error(
                    f"Mismatch for {subvol1_path}: snap_count1 ({snap_count1}) != snaps_synced1 ({snaps_synced1})"
                )
                return 1

            if snap_count2 != snaps_synced2:
                log.error(
                    f"Mismatch for {subvol2_path}: snap_count2 ({snap_count2}) != snaps_synced2 ({snaps_synced2})"
                )
                return 1

            log.info("Snap counts and snaps synced match for both subvolumes.")
            return 0
        else:
            log.error("Failed to capture snaps_synced for one or both subvolumes.")
            return 1

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Delete the snapshots")
        snapshots_to_delete = [
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/*",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/*",
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
            )

        snap_util.remove_snap_schedule(
            source_clients[0], subvol1_path, fs_name=source_fs
        ),
        snap_util.remove_snap_schedule(
            source_clients[0], subvol2_path, fs_name=source_fs
        ),

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
        source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir_1}")
        source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}")
        for client in [source_clients[0], target_clients[0]]:
            client.exec_command(
                sudo=True,
                cmd="ceph config set mon mon_allow_pool_delete true",
                check_ec=False,
            )
        fs_util_ceph1.remove_fs(source_clients[0], source_fs, validate=False)
        fs_util_ceph1.remove_fs(target_clients[0], target_fs, validate=False)


def get_snaps_synced(fs_name, fsid, asok_file, filesystem_id, peer_uuid, path):
    """
    Captures the 'snaps_synced' value for a given path in CephFS mirroring status.

    Args:
        fs_name (str): The CephFS volume name and rank (e.g., cephfs@5).
        fsid (str): The unique CephFS file system ID.
        asok_file (dict): A dictionary containing the node and its corresponding admin socket.
                         (e.g., {"node1": [client_object, "ceph-client.cephfs-mirror.<host>.asok"]})
        filesystem_id (int): The ID for the CephFS filesystem (e.g., 2).
        peer_uuid (str): The UUID of the peer to check mirror status for (e.g., 2c969645-e086-4e40-824a-b99c500c8749).
        path (str): The absolute path for which to capture the 'snaps_synced' value
                    (e.g., /volumes/subvolgroup_1/subvol_1).

    Returns:
        int: The 'snaps_synced' value for the specified path if found, or 1 if the path is not found.
    """
    log.info("Get peer mirror status")
    for node, asok in asok_file.items():
        asok[0].exec_command(sudo=True, cmd="dnf install -y ceph-common --nogpgcheck")
    cmd = (
        f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok[1]} fs mirror peer status "
        f"{fs_name}@{filesystem_id} {peer_uuid} -f json"
    )
    out, _ = asok[0].exec_command(sudo=True, cmd=cmd)
    data = json.loads(out)
    log.info(f"Paths found in mirror status: {list(data.keys())}")
    absolute_path = path.rstrip("/")

    if absolute_path in data:
        snaps_synced = data[absolute_path].get("snaps_synced")
        return snaps_synced
    else:
        log.error(f"Path '{absolute_path}' not found in the mirror status.")
        return 1
