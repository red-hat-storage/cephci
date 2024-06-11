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
    CEPH-83584059 - Validate end to end cephfs-mirroring metrics reports

    Steps :
    1. From an existing CephFS Mirroring Configuration, retrive all the metrics.
    2. Validate the same with the arguments provided and data retrieved from other cephfs-commands.
    3. Validate the complete workflow by adding additonal FS, enabling mirror on FS and a path and create snaps.
    4. Validate the change in the counters after each phase.

    Returns :
    0 if succesful, 1 if any errors found.
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
        source_fs = "cephfs"
        target_fs = "cephfs"
        target_site_name = "remote_site"
        snap1 = "snap_k1"
        snap2 = "snap_f1"
        snap_count = 2
        directory_count = 2
        initial_fs_count = 1
        peer_connection = 1

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

        log.info("Get the Metrics of Initial Configuration")
        data = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )

        log.info(
            "Validate if all the Details of Mirrored Filesystem is displayed in the Counters"
        )
        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data
        )
        peers = counter.get("mirroring_peers")

        if peer_connection == peers:
            log.info(
                f"Configured Peer : {peer_connection} matches in Metrics : {peers}"
            )
        else:
            log.error(
                f"Configured Peer : {peer_connection} doesn't exist in Metrics: {peers}"
            )
            raise CommandFailed("Configured Mirroring Peers is mismatching in Metrics")

        dir_count = counter.get("directory_count")
        if directory_count == dir_count:
            log.info(
                f"Enabled directory count : {directory_count} matches in Metrics : {dir_count}"
            )
        else:
            log.error(
                f"Enabled directory count : {directory_count} doesn't match in Metrics : {dir_count}"
            )
            raise CommandFailed("Configured Directory Counts is mismatching in Metrics")

        log.info(
            "Validate if all the Details of Peer Connection is displayed in the Counters"
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data
        )

        peer_cluster_filesystem = label.get("peer_cluster_filesystem")
        if target_fs == peer_cluster_filesystem:
            log.info(
                f"Configured Remote FS : {target_fs} matches in the Metrics : {peer_cluster_filesystem}"
            )
        else:
            log.error(
                f"Configured Remote FS : {target_fs} doesn't exist in Metrics : {peer_cluster_filesystem}"
            )
            raise CommandFailed(
                "Configured Peer Cluster Filesystem is mismatching in Metrics"
            )

        peer_cluster_name = label.get("peer_cluster_name")
        if target_site_name == peer_cluster_name:
            log.info(
                f"Configured Site Name : {target_site_name} matches in the Metrics : {peer_cluster_name}"
            )
        else:
            log.error(
                f"Configured Site Name : {target_site_name} doesn't exist in Metrics : {peer_cluster_name}"
            )
            raise CommandFailed("Configured Peer Site Name is mismatching in Metrics")

        source_filesystem = label.get("source_filesystem")
        if source_fs == source_filesystem:
            log.info(
                f"Configured Source FS : {source_fs} matches in the Metrics : {source_filesystem}"
            )
        else:
            log.error(
                f"Configured Source FS : {source_fs} doesn't exist in Metrics : {source_filesystem}"
            )
            raise CommandFailed(
                "Configured Source FileSystem is mismatching in Metrics"
            )

        source_fscid = label.get("source_fscid")
        if int(source_fscid) == int(filesystem_id):
            log.info(
                f"Configured FSCID : {source_fscid} matches in the Metrics : {filesystem_id}"
            )
        else:
            log.error(
                f"Configured FSCID : {source_fscid} doesn't exist in Metrics {filesystem_id}"
            )
            raise CommandFailed("Configured FSCID is mismatching in Metrics")

        snaps_synced = counter.get("snaps_synced")
        if snap_count == snaps_synced:
            log.info(
                f"Snapshot created count : {snap_count} matches in the Metrics : {snaps_synced}"
            )
        else:
            log.error(
                f"Snapshot created count : {snap_count} doesn't exist in Metrics : {snaps_synced}"
            )
            raise CommandFailed("Snapshot created count is mismatching in Metrics")

        snaps_deleted = counter.get("snaps_deleted")
        log.info(f"Snaps Deleted: {snaps_deleted}")

        snaps_renamed = counter.get("snaps_renamed")
        log.info(f"Snaps Renamed: {snaps_renamed}")

        sync_failures = counter.get("sync_failures")
        log.info(f"Sync Failures: {sync_failures}")

        avg_sync_count = counter.get("avg_sync_time", {}).get("avgcount")
        log.info(f"Average Sync Count: {avg_sync_count}")

        sum = counter.get("avg_sync_time", {}).get("sum")
        log.info(f"Average Sync Sum: {sum}")

        avg_time = counter.get("avg_sync_time", {}).get("avgtime")
        log.info(f"Average Sync Time: {avg_time}")

        sync_bytes = counter.get("sync_bytes")
        log.info(f"Sync Bytes: {sync_bytes}")

        log.info(
            "Validate if all the details of Cephfs Mirrors are displayed in the counters"
        )
        report = data["cephfs_mirror"]
        for value in report:
            counters = value["counters"]
            mirrored_fs = counters.get("mirrored_filesystems", "")
            mirror_enable_failure = counters.get("mirror_enable_failures", "")

            if initial_fs_count == mirrored_fs:
                log.info(
                    f"Configured FS count : {initial_fs_count} matches in the Metrics : {mirrored_fs}"
                )
            else:
                log.error(
                    f"Configured FS count : {initial_fs_count} is not matching in Metrics : {mirrored_fs}"
                )
                raise CommandFailed("Mirrored Filesystem is mismatching in Metrics")

            log.info(f"Mirrored FS Failure Count: {mirror_enable_failure}")

        log.info(
            "Create a new FileSystem and enable for mirroring and ensure the details of new  "
            "FileSystem is updated in counters"
        )
        log.info("Create required filesystem on Source Cluster and Target Cluster...")
        source_fs_new = "cephfs-snew"
        target_fs_new = "cephfs-tnew"
        target_user_new = "mirror_remote_tnew"
        target_site_name_new = "remote_site_tnew"

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-6:-4]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        source_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {source_fs_new} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs_new)

        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        log.info(f"Available MDS Nodes {mds_nodes[0]}")
        log.info(len(mds_nodes))
        mds_names = []
        for mds in mds_nodes:
            mds_names.append(mds.node.hostname)
        hosts_list1 = mds_names[-4:-2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        target_clients[0].exec_command(
            sudo=True,
            cmd=f'ceph fs volume create {target_fs_new} --placement="2 {mds_hosts_1}"',
        )
        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs_new)

        log.info(f"Enable cephfs mirroring module on the {source_fs_new}")
        fs_mirroring_utils.enable_snapshot_mirroring(source_fs_new, source_clients[0])

        log.info(f"Create the peer bootstrap on Target Cluster on {target_fs_new}")
        bootstrap_token_new = fs_mirroring_utils.create_peer_bootstrap(
            target_fs_new, target_user_new, target_site_name_new, target_clients[0]
        )

        log.info("Import the bootstrap on source on all the filesystem")
        log.info(f"Import the bootstrap on {source_fs_new}")
        fs_mirroring_utils.import_peer_bootstrap(
            source_fs_new, bootstrap_token_new, source_clients[0]
        )

        log.info(
            "Add Data and Snapshots on the newly added FS and valdaite the changes in counters"
        )
        log.info("Create Subvolumes for adding Data")
        subvolumegroup_name = "subvolgroup_1"
        subvol_new_1 = "subvol_new_1"
        subvol_new_2 = "subvol_new_2"
        snap_suffix = "new"

        subvolumegroup_list = [
            {"vol_name": source_fs_new, "group_name": subvolumegroup_name},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs_new,
                "subvol_name": subvol_new_1,
                "group_name": subvolumegroup_name,
                "size": "5368709120",
            },
            {
                "vol_name": source_fs_new,
                "subvol_name": subvol_new_2,
                "group_name": subvolumegroup_name,
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info(f"Mount {subvol_new_1} on kernel and {subvol_new_2} on Fuse → Client1")
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()
        log.info(f"Get the path of {subvol_new_1} on  filesystem")
        subvol_path1, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs_new} {subvol_new_1} {subvolumegroup_name}",
        )
        index = subvol_path1.find(f"{subvol_new_1}/")
        if index != -1:
            subvol1_path = subvol_path1[: index + len(f"{subvol_new_1}/")]
        else:
            subvol1_path = subvol_path1
        log.info(subvol1_path)

        fs_util_ceph1.kernel_mount(
            [source_clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={source_fs_new}",
        )
        log.info(f"Get the path of {subvol_new_2} on filesystem")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        subvol_path2, rc = source_clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {source_fs_new} {subvol_new_2} {subvolumegroup_name}",
        )
        index = subvol_path2.find(f"{subvol_new_2}/")
        if index != -1:
            subvol2_path = subvol_path2[: index + len(f"{subvol_new_2}/")]
        else:
            subvol2_path = subvol_path2
        log.info(subvol2_path)
        fs_util_ceph1.fuse_mount(
            [source_clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {source_fs_new}",
        )

        log.info("Add new subvolume paths for mirroring")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_new, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs_new, subvol2_path
        )
        filesystem_id_new = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs_new
        )
        fs_mirroring_utils.add_files_and_validate(
            source_clients,
            kernel_mounting_dir_1,
            subvol1_path,
            fuse_mounting_dir_1,
            subvol2_path,
            cephfs_mirror_node,
            source_fs_new,
            snap_suffix,
            snap_count,
        )

        log.info("Get the Metrics after adding a new Filesystem")
        data_new = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )

        new_fs_count = initial_fs_count + 1
        log.info(f"new_count = {new_fs_count}")
        log.info(
            "Validate if all the details of Cephfs Mirrors are updated after adding a new filesystem"
        )
        report = data_new["cephfs_mirror"]
        for value in report:
            counters = value["counters"]
            mirrored_fs = counters.get("mirrored_filesystems", "")

            if new_fs_count == mirrored_fs:
                log.info(
                    f"Configured FS count : {new_fs_count} matches in the Metrics : {mirrored_fs}"
                )
            else:
                log.error(
                    f"Configured FS count : {new_fs_count} is not matching in Metrics : {mirrored_fs}"
                )
                raise CommandFailed("Mirrored Filesystem is mismatching in Metrics")

        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs_new
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_new
        )
        log.info(f"Labels : {label}")
        log.info(f"Counter : {counter}")
        peers = counter.get("mirroring_peers")
        log.info(f"Peers : {peers}")
        if peer_connection == peers:
            log.info(
                f"Configured Peer : {peer_connection} matches in Metrics : {peers}"
            )
        else:
            log.error(
                f"Configured Peer : {peer_connection} doesn't exist in Metrics: {peers}"
            )
            raise CommandFailed("Configured Mirroring Peers is mismatching in Metrics")

        dir_count = counter.get("directory_count")
        log.info(f"Dir Count : {dir_count}")
        if directory_count == dir_count:
            log.info(
                f"Enabled directory count : {directory_count} matches in Metrics : {dir_count}"
            )
        else:
            log.error(
                f"Enabled directory count : {directory_count} doesn't match in Metrics : {dir_count}"
            )
            raise CommandFailed("Configured Directory Counts is mismatching in Metrics")

        log.info(
            "Validate if all the Details of Peer Connection is updated in the Counters "
            "after adding a new Filesystem"
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_new
        )

        peer_cluster_filesystem = label.get("peer_cluster_filesystem")
        if target_fs_new == peer_cluster_filesystem:
            log.info(
                f"Configured Remote FS : {target_fs_new} matches in the Metrics : {peer_cluster_filesystem}"
            )
        else:
            log.error(
                f"Configured Remote FS : {target_fs_new} doesn't exist in Metrics : {peer_cluster_filesystem}"
            )
            raise CommandFailed(
                "Configured Peer Cluster Filesystem is mismatching in Metrics"
            )

        peer_cluster_name = label.get("peer_cluster_name")
        if target_site_name_new == peer_cluster_name:
            log.info(
                f"Configured Site Name : {target_site_name_new} matches in the Metrics : {peer_cluster_name}"
            )
        else:
            log.error(
                f"Configured Site Name : {target_site_name_new} doesn't exist in Metrics : {peer_cluster_name}"
            )
            raise CommandFailed("Configured Peer Site Name is mismatching in Metrics")

        source_filesystem = label.get("source_filesystem")
        if source_fs_new == source_filesystem:
            log.info(
                f"Configured Source FS : {source_fs_new} matches in the Metrics : {source_filesystem}"
            )
        else:
            log.error(
                f"Configured Source FS : {source_fs_new} doesn't exist in Metrics : {source_filesystem}"
            )
            raise CommandFailed(
                "Configured Source FileSystem is mismatching in Metrics"
            )

        source_fscid = label.get("source_fscid")
        log.info(f"Source FSID : {source_fscid}")
        log.info(f"FileSystem ID : {filesystem_id_new}")
        if int(source_fscid) == int(filesystem_id_new):
            log.info(
                f"Configured FSCID : {source_fscid} matches in the Metrics : {filesystem_id_new}"
            )
        else:
            log.error(
                f"Configured FSCID : {source_fscid} doesn't exist in Metrics {filesystem_id_new}"
            )
            raise CommandFailed("Configured FSCID is mismatching in Metrics")

        snaps_synced = counter.get("snaps_synced")
        if snap_count == snaps_synced:
            log.info(
                f"Snapshot created count : {snap_count} matches in the Metrics : {snaps_synced}"
            )
        else:
            log.error(
                f"Snapshot created count : {snap_count} doesn't exist in Metrics : {snaps_synced}"
            )
            raise CommandFailed("Snapshot created count is mismatching in Metrics")

        snaps_deleted = counter.get("snaps_deleted")
        log.info(f"Snaps Deleted: {snaps_deleted}")

        snaps_renamed = counter.get("snaps_renamed")
        log.info(f"Snaps Renamed: {snaps_renamed}")

        sync_failures = counter.get("sync_failures")
        log.info(f"Sync Failures: {sync_failures}")

        avg_sync_count = counter.get("avg_sync_time", {}).get("avgcount")
        log.info(f"Average Sync Count: {avg_sync_count}")

        sum = counter.get("avg_sync_time", {}).get("sum")
        log.info(f"Average Sync Sum: {sum}")

        avg_time = counter.get("avg_sync_time", {}).get("avgtime")
        log.info(f"Average Sync Time: {avg_time}")

        sync_bytes = counter.get("sync_bytes")
        log.info(f"Sync Bytes: {sync_bytes}")

        log.info("Negative : Induce a Sync failure and capture the metrics")
        target_mount_path1 = "/mnt/remote_dir_new"
        target_client_user = "client.admin"
        snap_target = "snap_target"
        source_path1 = subvol1_path
        target_fs_new = "cephfs-tnew"

        log.info("Inject a Sync Failure")
        fs_mirroring_utils.inject_sync_failure(
            target_clients[0],
            target_mount_path1,
            target_client_user,
            source_path1,
            snap_target,
            target_fs_new,
        )

        log.info(
            "Wait for some time and check if the counters are reflecting the sync failure"
        )
        time.sleep(30)

        log.info("Get the Metrics after injecting a sync failure")
        data_after_sync_failure = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )

        log.info(
            "Validate if sync_failure count has increased after injecting failures"
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new

        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_after_sync_failure
        )

        if counter:
            sync_failures = counter.get("sync_failures", 0)
            if sync_failures > 0:
                log.info("Validation Passed: Sync failure count is more than 0")
            else:
                log.error("Validation Failed: Sync failure count is 0 or less")
        else:
            log.error(
                "Validation Failed: Sync Failures counters mismatch in the metrics."
            )
            raise CommandFailed(
                "Sync Failure failed to increase after injecting a failure"
            )

        log.info(
            "Delete the snapshot used for injecting sync failure and unmount the paths"
        )
        cmds = [
            f"rmdir {target_mount_path1}{source_path1}.snap/{snap_target}",
            f"umount -l {target_mount_path1}",
            f"rm -rf {target_mount_path1}",
        ]
        for cmd in cmds:
            target_clients[0].exec_command(
                sudo=True,
                cmd=cmd,
            )
            time.sleep(10)

        log.info("Delete the snapshots")
        snapshots_to_delete = [
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}_{snap_suffix}*",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}_{snap_suffix}*",
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
            )
        time.sleep(30)

        log.info(
            "Validate if all the Details of Peer Connection is updated in the Counters "
            "after removing snapshots and  Filesystem"
        )

        log.info("Get the Metrics after removing snapshots and Filesystem")
        data_after_snap_delete = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )

        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_after_snap_delete
        )

        snaps_deleted = counter.get("snaps_deleted")
        log.info(f"Snaps Deleted: {snaps_deleted}")
        if snap_count == snaps_deleted:
            log.info(
                f"Snapshot Deleted count : {snap_count} matches in the Metrics : {snaps_deleted}"
            )
        else:
            log.error(
                f"Snapshot Deleted count : {snap_count} doesn't exist in Metrics : {snaps_deleted}"
            )
            raise CommandFailed("Snapshot Deleted count is mismatching in Metrics")

        log.info("Remove subvolume paths from mirroring")
        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs_new, subvol1_path
        )
        fs_mirroring_utils.remove_path_from_mirroring(
            source_clients[0], source_fs_new, subvol2_path
        )

        log.info("Remove the newly added Filesystem from mirroring")
        log.info(f"Disable cephfs mirroring module on the {source_fs_new}")
        fs_mirroring_utils.disable_snapshot_mirroring(source_fs_new, source_clients[0])
        time.sleep(20)

        data_after_fs_remove = fs_mirroring_utils.get_cephfs_mirror_counters(
            cephfs_mirror_node, fsid, asok_file
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_after_fs_remove
        )

        if label is None and counter is None:
            log.info(f"FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"FS : {source_fs_new} still exists in the metrics")
            raise CommandFailed("Mismatch in the metrics.")

        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs_new
        label, counter = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, data_after_fs_remove
        )
        if label is None and counter is None:
            log.info(f"Mirrored FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"Mirrored FS : {source_fs_new} still exists in the metrics")
            raise CommandFailed("Mismatch in the metrics.")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        log.info("Unmount the paths on source")
        mounting_dirs = [
            kernel_mounting_dir_1,
            fuse_mounting_dir_1,
        ]
        for mounting_dir in mounting_dirs:
            source_clients[0].exec_command(sudo=True, cmd=f"umount -l {mounting_dir}")

        log.info("Delete the mounted paths")
        mounting_dirs = [
            kernel_mounting_dir_1,
            fuse_mounting_dir_1,
        ]
        for mounting_dir in mounting_dirs:
            source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {mounting_dir}")

        log.info("Delete the users used for creating peer bootstrap")
        fs_mirroring_utils.remove_user_used_for_peer_connection(
            target_user_new, target_clients[0]
        )

        log.info("Delete filesystem on Source and Target Cluster")
        source_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        target_clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"ceph fs volume rm {source_fs_new} --yes-i-really-mean-it"
        )
        target_clients[0].exec_command(
            sudo=True, cmd=f"ceph fs volume rm {target_fs_new} --yes-i-really-mean-it"
        )
