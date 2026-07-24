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
    CEPH-83584059 - Validate end to end cephfs-mirroring metrics reports

    Steps :
    1. From an existing CephFS Mirroring HA Configuration, retrive all the metrics.
    2. Validate the same with the arguments provided and data retrieved from other cephfs commands.
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
        target_site_name = "remote_site"
        peer_connection = 1
        directory_count = 1
        snap_count = 2
        initial_fs_count = 1
        snap1 = "snap_k1"
        snap2 = "snap_f1"

        log.info("Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id")
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")

        log.info("Name of the cephfs-mirror daemons")
        daemon_names = fs_mirroring_utils.get_daemon_name(source_clients[0])
        asok_files = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node, fsid, daemon_names
        )
        log.info(f"Admin Socket files of all cephfs-mirror daemon : {asok_files}")

        cephfs_mirror_node1 = cephfs_mirror_node[0].node.hostname
        cephfs_mirror_node2 = cephfs_mirror_node[1].node.hostname

        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")

        initial_metrics_data = fetch_cephfs_mirror_counters(fsid, asok_files)
        log.info("Get the Metrics of Initial Configuration on 1st Mirroring Daemon")
        mirror_node1_data = fetch_mirror_node_data(
            initial_metrics_data, cephfs_mirror_node1
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node1_data}")
        cephfs_mirror_node1_data = json.loads(mirror_node1_data)

        log.info("Get the Metrics of Initial Configuration on 2nd Mirroring Daemon")
        mirror_node2_data = fetch_mirror_node_data(
            initial_metrics_data, cephfs_mirror_node2
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node2_data}")
        cephfs_mirror_node2_data = json.loads(mirror_node2_data)

        log.info(
            "Validate if all the Details of Mirrored Filesystem is displayed in the Counters"
        )
        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )

        peers_on_node1 = counter_on_node1.get("mirroring_peers")
        peers_on_node2 = counter_on_node2.get("mirroring_peers")
        if peer_connection == peers_on_node1 and peer_connection == peers_on_node2:
            log.info(
                f"Configured Peer : {peer_connection} matches in Metrics : "
                f"{peers_on_node1} and {peers_on_node2}"
            )
        else:
            log.error(
                f"Configured Peer : {peer_connection} doesn't exist in Metrics: "
                f"{peers_on_node1} and {peers_on_node2}"
            )
            raise CommandFailed("Configured Mirroring Peers is mismatching in Metrics")

        dir_count_on_node1 = counter_on_node1.get("directory_count")
        dir_count_on_node2 = counter_on_node2.get("directory_count")

        if (
            directory_count == dir_count_on_node1
            and directory_count == dir_count_on_node2
        ):
            log.info(
                f"Enabled directory count : {directory_count} matches in Metrics : "
                f"{dir_count_on_node1} and {dir_count_on_node2}"
            )
        else:
            log.error(
                f"Enabled directory count : {directory_count} doesn't match in Metrics : "
                f"{dir_count_on_node1} and {dir_count_on_node2}"
            )
            raise CommandFailed("Configured Directory Counts is mismatching in Metrics")

        log.info(
            "Validate if all the Details of Peer Connection is displayed in the Counters"
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )

        peer_cluster_filesystem_on_node1 = label_on_node1.get("peer_cluster_filesystem")
        peer_cluster_filesystem_on_node2 = label_on_node2.get("peer_cluster_filesystem")
        if (
            target_fs == peer_cluster_filesystem_on_node1
            and target_fs == peer_cluster_filesystem_on_node2
        ):
            log.info(
                f"Configured Remote FS : {target_fs} matches in the Metrics : "
                f"{peer_cluster_filesystem_on_node1} and {peer_cluster_filesystem_on_node2}"
            )
        else:
            log.error(
                f"Configured Remote FS : {target_fs} doesn't exist in Metrics : "
                f"{peer_cluster_filesystem_on_node1} and {peer_cluster_filesystem_on_node2}"
            )
            raise CommandFailed(
                "Configured Peer Cluster Filesystem is mismatching in Metrics"
            )

        peer_cluster_name_on_node1 = label_on_node1.get("peer_cluster_name")
        peer_cluster_name_on_node2 = label_on_node2.get("peer_cluster_name")

        if (
            target_site_name == peer_cluster_name_on_node1
            and target_site_name == peer_cluster_name_on_node2
        ):
            log.info(
                f"Configured Site Name : {target_site_name} matches in the Metrics : "
                f"{peer_cluster_name_on_node1} and {peer_cluster_name_on_node2}"
            )
        else:
            log.error(
                f"Configured Site Name : {target_site_name} doesn't exist in Metrics : "
                f"{peer_cluster_name_on_node1} and {peer_cluster_name_on_node2}"
            )
            raise CommandFailed("Configured Peer Site Name is mismatching in Metrics")

        source_filesystem_on_node1 = label_on_node1.get("source_filesystem")
        source_filesystem_on_node2 = label_on_node2.get("source_filesystem")
        if (
            source_fs == source_filesystem_on_node1
            and source_fs == source_filesystem_on_node2
        ):
            log.info(
                f"Configured Source FS : {source_fs} matches in the Metrics : "
                f"{source_filesystem_on_node1} and {source_filesystem_on_node2}"
            )
        else:
            log.error(
                f"Configured Source FS : {source_fs} doesn't exist in Metrics : "
                f"{source_filesystem_on_node1} and {source_filesystem_on_node2}"
            )
            raise CommandFailed(
                "Configured Source FileSystem is mismatching in Metrics"
            )

        source_fscid_on_node1 = label_on_node1.get("source_fscid")
        source_fscid_on_node2 = label_on_node2.get("source_fscid")
        if int(source_fscid_on_node1) == int(filesystem_id) and int(
            source_fscid_on_node2
        ) == int(filesystem_id):
            log.info(
                f"Configured FSCID : {source_fscid_on_node1} and "
                f"{source_fscid_on_node2} matches in the Metrics : {filesystem_id}"
            )
        else:
            log.error(
                f"Configured FSCID : {source_fscid_on_node1} and "
                f"{source_fscid_on_node2} doesn't exist in Metrics {filesystem_id}"
            )
            raise CommandFailed("Configured FSCID is mismatching in Metrics")

        snaps_synced_on_node1 = counter_on_node1.get("snaps_synced")
        snaps_synced_on_node2 = counter_on_node2.get("snaps_synced")
        if snap_count == snaps_synced_on_node1 + snaps_synced_on_node2:
            log.info(
                f"Snapshot created count : {snap_count} matches in the Metrics : "
                f"{snaps_synced_on_node1} and {snaps_synced_on_node2}"
            )
        else:
            log.error(
                f"Snapshot created count : {snap_count} doesn't exist in Metrics : "
                f"{snaps_synced_on_node1} and {snaps_synced_on_node2}"
            )
            raise CommandFailed("Snapshot created count is mismatching in Metrics")

        snaps_deleted_on_node1 = counter_on_node1.get("snaps_deleted")
        snaps_deleted_on_node2 = counter_on_node2.get("snaps_deleted")
        log.info(
            f"Snaps Deleted: {snaps_deleted_on_node1} and {snaps_deleted_on_node2}"
        )

        snaps_renamed_on_node1 = counter_on_node1.get("snaps_renamed")
        snaps_renamed_on_node2 = counter_on_node2.get("snaps_renamed")
        log.info(
            f"Snaps Renamed: {snaps_renamed_on_node1} and {snaps_renamed_on_node2}"
        )

        sync_failures_on_node1 = counter_on_node1.get("sync_failures")
        sync_failures_on_node2 = counter_on_node2.get("sync_failures")
        log.info(
            f"Sync Failures: {sync_failures_on_node1} and {sync_failures_on_node2}"
        )

        avg_sync_count_on_node1 = counter_on_node1.get("avg_sync_time", {}).get(
            "avgcount"
        )
        avg_sync_count_on_node2 = counter_on_node2.get("avg_sync_time", {}).get(
            "avgcount"
        )
        log.info(
            f"Average Sync Count: {avg_sync_count_on_node1} and {avg_sync_count_on_node2}"
        )

        sum_on_node1 = counter_on_node1.get("avg_sync_time", {}).get("sum")
        sum_on_node2 = counter_on_node2.get("avg_sync_time", {}).get("sum")
        log.info(f"Average Sync Sum: {sum_on_node1} and {sum_on_node2}")

        avg_time_on_node1 = counter_on_node1.get("avg_sync_time", {}).get("avgtime")
        avg_time_on_node2 = counter_on_node2.get("avg_sync_time", {}).get("avgtime")
        log.info(f"Average Sync Time: {avg_time_on_node1} and {avg_time_on_node2}")

        sync_bytes_on_node1 = counter_on_node1.get("sync_bytes")
        sync_bytes_on_node2 = counter_on_node2.get("sync_bytes")
        log.info(f"Sync Bytes: {sync_bytes_on_node1} and {sync_bytes_on_node2}")

        log.info(
            "Validate if all the details of Cephfs Mirrors are displayed in the counters"
        )
        report_on_node1 = cephfs_mirror_node1_data["cephfs_mirror"]
        report_on_node2 = cephfs_mirror_node2_data["cephfs_mirror"]
        for report_name, report_data in [
            ("report1", report_on_node1),
            ("report2", report_on_node2),
        ]:
            for value in report_data:
                counters = value.get("counters", {})
                mirrored_fs = counters.get("mirrored_filesystems", "")
                mirror_enable_failure = counters.get("mirror_enable_failures", "")

                if initial_fs_count == mirrored_fs:
                    log.info(
                        f"Configured FS count in {report_name}: "
                        f"{initial_fs_count} matches in the Metrics: {mirrored_fs}"
                    )
                else:
                    log.error(
                        f"Configured FS count in {report_name}: "
                        f"{initial_fs_count} is not matching in Metrics: {mirrored_fs}"
                    )
                    raise CommandFailed("Mirrored Filesystem is mismatching in Metrics")

                log.info(
                    f"Mirrored FS Failure Count in {report_name}: {mirror_enable_failure}"
                )

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
        metrics_data_after_adding_newfs = fetch_cephfs_mirror_counters(fsid, asok_files)
        log.info("Get the Metrics of Initial Configuration on 1st Mirroring Daemon")
        mirror_node1_data = fetch_mirror_node_data(
            metrics_data_after_adding_newfs, cephfs_mirror_node1
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node1_data}")
        cephfs_mirror_node1_data = json.loads(mirror_node1_data)

        log.info("Get the Metrics of Initial Configuration on 2nd Mirroring Daemon")
        mirror_node2_data = fetch_mirror_node_data(
            metrics_data_after_adding_newfs, cephfs_mirror_node2
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node1_data}")
        cephfs_mirror_node2_data = json.loads(mirror_node2_data)

        new_fs_count = initial_fs_count + 1
        log.info(f"new_count = {new_fs_count}")
        log.info(
            "Validate if all the details of Cephfs Mirrors are updated after adding a new filesystem"
        )

        report_on_node1 = cephfs_mirror_node1_data["cephfs_mirror"]
        report_on_node2 = cephfs_mirror_node2_data["cephfs_mirror"]
        for report_name, report_data in [
            ("report1", report_on_node1),
            ("report2", report_on_node2),
        ]:
            for value in report_data:
                counters = value.get("counters", {})
                mirrored_fs = counters.get("mirrored_filesystems", "")
                mirror_enable_failure = counters.get("mirror_enable_failures", "")

                if new_fs_count == mirrored_fs:
                    log.info(
                        f"Configured FS count in {report_name}: "
                        f"{new_fs_count} matches in the Metrics: {mirrored_fs}"
                    )
                else:
                    log.error(
                        f"Configured FS count in {report_name}: "
                        f"{new_fs_count} is not matching in Metrics: {mirrored_fs}"
                    )
                    raise CommandFailed("Mirrored Filesystem is mismatching in Metrics")

                log.info(
                    f"Mirrored FS Failure Count in {report_name}: {mirror_enable_failure}"
                )

        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs_new
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )

        peers_on_node1 = counter_on_node1.get("mirroring_peers")
        peers_on_node2 = counter_on_node2.get("mirroring_peers")
        log.info(f"Peers : {peers_on_node1} and {peers_on_node2}")
        if peer_connection == peers_on_node1 and peer_connection == peers_on_node2:
            log.info(
                f"Configured Peer : {peer_connection} matches in Metrics : "
                f"{peers_on_node1} and {peers_on_node2}"
            )
        else:
            log.error(
                f"Configured Peer : {peer_connection} doesn't exist in Metrics: "
                f"{peers_on_node1} and {peers_on_node2}"
            )
            raise CommandFailed("Configured Mirroring Peers is mismatching in Metrics")

        dir_count_on_node1 = counter_on_node1.get("directory_count")
        dir_count_on_node2 = counter_on_node2.get("directory_count")
        log.info(f"Dir Count : {dir_count_on_node1} and {dir_count_on_node2}")
        if (
            directory_count == dir_count_on_node1
            and directory_count == dir_count_on_node2
        ):
            log.info(
                f"Enabled directory count : {directory_count} matches in Metrics : "
                f"{dir_count_on_node1} and {dir_count_on_node2}"
            )
        else:
            log.error(
                f"Enabled directory count : {directory_count} doesn't match in Metrics : "
                f"{dir_count_on_node1} and {dir_count_on_node2}"
            )
            raise CommandFailed("Configured Directory Counts is mismatching in Metrics")

        log.info(
            "Validate if all the Details of Peer Connection is updated in the Counters "
            "after adding a new Filesystem"
        )
        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )

        peer_cluster_filesystem_on_node1 = label_on_node1.get("peer_cluster_filesystem")
        peer_cluster_filesystem_on_node2 = label_on_node2.get("peer_cluster_filesystem")
        if (
            target_fs_new == peer_cluster_filesystem_on_node1
            and target_fs_new == peer_cluster_filesystem_on_node2
        ):
            log.info(
                f"Configured Remote FS : {target_fs_new} matches in the Metrics : "
                f"{peer_cluster_filesystem_on_node1} and {peer_cluster_filesystem_on_node2}"
            )
        else:
            log.error(
                f"Configured Remote FS : {target_fs_new} doesn't exist in Metrics : "
                f"{peer_cluster_filesystem_on_node1} and {peer_cluster_filesystem_on_node2}"
            )
            raise CommandFailed(
                "Configured Peer Cluster Filesystem is mismatching in Metrics"
            )

        peer_cluster_name_on_node1 = label_on_node1.get("peer_cluster_name")
        peer_cluster_name_on_node2 = label_on_node2.get("peer_cluster_name")

        if (
            target_site_name_new == peer_cluster_name_on_node1
            and target_site_name_new == peer_cluster_name_on_node2
        ):
            log.info(
                f"Configured Site Name : {target_site_name_new} matches in the Metrics : "
                f"{peer_cluster_name_on_node1} and {peer_cluster_name_on_node2}"
            )
        else:
            log.error(
                f"Configured Site Name : {target_site_name_new} doesn't exist in Metrics : "
                f"{peer_cluster_name_on_node1} and {peer_cluster_name_on_node2}"
            )
            raise CommandFailed("Configured Peer Site Name is mismatching in Metrics")

        source_filesystem_on_node1 = label_on_node1.get("source_filesystem")
        source_filesystem_on_node2 = label_on_node2.get("source_filesystem")
        if (
            source_fs_new == source_filesystem_on_node1
            and source_fs_new == source_filesystem_on_node2
        ):
            log.info(
                f"Configured Source FS : {source_fs_new} matches in the Metrics : "
                f"{source_filesystem_on_node1} and {source_filesystem_on_node2}"
            )
        else:
            log.error(
                f"Configured Source FS : {source_fs_new} doesn't exist in Metrics : "
                f"{source_filesystem_on_node1} and {source_filesystem_on_node2}"
            )
            raise CommandFailed(
                "Configured Source FileSystem is mismatching in Metrics"
            )

        source_fscid_on_node1 = label_on_node1.get("source_fscid")
        source_fscid_on_node2 = label_on_node2.get("source_fscid")

        if int(source_fscid_on_node1) == int(filesystem_id_new) and int(
            source_fscid_on_node2
        ) == int(filesystem_id_new):
            log.info(
                f"Configured FSCID : {source_fscid_on_node1} and "
                f"{source_fscid_on_node2} matches in the Metrics : {filesystem_id_new}"
            )
        else:
            log.error(
                f"Configured FSCID : {source_fscid_on_node1} and "
                f"{source_fscid_on_node2} doesn't exist in Metrics {filesystem_id_new}"
            )
            raise CommandFailed("Configured FSCID is mismatching in Metrics")

        snaps_synced_on_node1 = counter_on_node1.get("snaps_synced")
        snaps_synced_on_node2 = counter_on_node2.get("snaps_synced")
        if snap_count == snaps_synced_on_node1 + snaps_synced_on_node2:
            log.info(
                f"Snapshot created count : {snap_count} matches in the Metrics : "
                f"{snaps_synced_on_node1} and {snaps_synced_on_node2}"
            )
        else:
            log.error(
                f"Snapshot created count : {snap_count} doesn't exist in Metrics : "
                f"{snaps_synced_on_node1} and {snaps_synced_on_node2}"
            )
            raise CommandFailed("Snapshot created count is mismatching in Metrics")

        snaps_deleted_on_node1 = counter_on_node1.get("snaps_deleted")
        snaps_deleted_on_node2 = counter_on_node2.get("snaps_deleted")
        log.info(
            f"Snaps Deleted: {snaps_deleted_on_node1} and {snaps_deleted_on_node2}"
        )

        snaps_renamed_on_node1 = counter_on_node1.get("snaps_renamed")
        snaps_renamed_on_node2 = counter_on_node2.get("snaps_renamed")
        log.info(
            f"Snaps Renamed: {snaps_renamed_on_node1} and {snaps_renamed_on_node2}"
        )

        sync_failures_on_node1 = counter_on_node1.get("sync_failures")
        sync_failures_on_node2 = counter_on_node2.get("sync_failures")
        log.info(
            f"Sync Failures: {sync_failures_on_node1} and {sync_failures_on_node2}"
        )

        avg_sync_count_on_node1 = counter_on_node1.get("avg_sync_time", {}).get(
            "avgcount"
        )
        avg_sync_count_on_node2 = counter_on_node2.get("avg_sync_time", {}).get(
            "avgcount"
        )
        log.info(
            f"Average Sync Count: {avg_sync_count_on_node1} and {avg_sync_count_on_node2}"
        )

        sum_on_node1 = counter_on_node1.get("avg_sync_time", {}).get("sum")
        sum_on_node2 = counter_on_node2.get("avg_sync_time", {}).get("sum")
        log.info(f"Average Sync Sum: {sum_on_node1} and {sum_on_node2}")

        avg_time_on_node1 = counter_on_node1.get("avg_sync_time", {}).get("avgtime")
        avg_time_on_node2 = counter_on_node2.get("avg_sync_time", {}).get("avgtime")
        log.info(f"Average Sync Time: {avg_time_on_node1} and {avg_time_on_node2}")

        sync_bytes_on_node1 = counter_on_node1.get("sync_bytes")
        sync_bytes_on_node2 = counter_on_node2.get("sync_bytes")
        log.info(f"Sync Bytes: {sync_bytes_on_node1} and {sync_bytes_on_node2}")

        log.info("Delete the snapshots")
        snapshots_to_delete = [
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}_{snap_suffix}*",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}_{snap_suffix}*",
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
            )
        time.sleep(60)

        log.info(
            "Validate if all the Details of Peer Connection is updated in the Counters "
            "after removing snapshots and  Filesystem"
        )
        metrics_data_after_snap_delete = fetch_cephfs_mirror_counters(fsid, asok_files)
        log.info("Get the Metrics on 1st Mirroring Daemon")
        mirror_node1_data = fetch_mirror_node_data(
            metrics_data_after_snap_delete, cephfs_mirror_node1
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node1_data}")
        cephfs_mirror_node1_data = json.loads(mirror_node1_data)

        log.info("Get the Metrics on 2nd Mirroring Daemon")
        mirror_node2_data = fetch_mirror_node_data(
            metrics_data_after_snap_delete, cephfs_mirror_node2
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node2_data}")
        cephfs_mirror_node2_data = json.loads(mirror_node2_data)

        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )

        snaps_deleted_on_node1 = counter_on_node1.get("snaps_deleted")
        snaps_deleted_on_node2 = counter_on_node2.get("snaps_deleted")
        log.info(
            f"Snaps Deleted: {snaps_deleted_on_node1} and {snaps_deleted_on_node2}"
        )
        if snap_count == snaps_deleted_on_node1 + snaps_deleted_on_node2:
            log.info(
                f"Snapshot Deleted count : {snap_count} matches in the Metrics : "
                f"{snaps_deleted_on_node1} and {snaps_deleted_on_node2}"
            )
        else:
            log.error(
                f"Snapshot Deleted count : {snap_count} doesn't exist in Metrics : "
                f"{snaps_deleted_on_node1} and {snaps_deleted_on_node2}"
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
        time.sleep(60)

        metrics_data_after_fs_remove = fetch_cephfs_mirror_counters(fsid, asok_files)
        log.info("Get the Metrics on 1st Mirroring Daemon")
        mirror_node1_data = fetch_mirror_node_data(
            metrics_data_after_fs_remove, cephfs_mirror_node1
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node1_data}")
        cephfs_mirror_node1_data = json.loads(mirror_node1_data)

        log.info("Get the Metrics on 2nd Mirroring Daemon")
        mirror_node2_data = fetch_mirror_node_data(
            metrics_data_after_fs_remove, cephfs_mirror_node2
        )
        log.info(f"CephFS Mirror Metrics for mirror_node1 : {mirror_node2_data}")
        cephfs_mirror_node2_data = json.loads(mirror_node2_data)

        resource_name = "cephfs_mirror_peers"
        filesystem_name = source_fs_new
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        if label_on_node1 is None and counter_on_node1 is None:
            log.info(f"Mirrored FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"Mirrored FS : {source_fs_new} still exists in the metrics.")
            raise CommandFailed("Mismatch in the metrics")

        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )
        if label_on_node2 is None and counter_on_node2 is None:
            log.info(f"Mirrored FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"Mirrored FS : {source_fs_new} still exists in the metrics.")
            raise CommandFailed("Mismatch in the metrics.")

        resource_name = "cephfs_mirror_mirrored_filesystems"
        filesystem_name = source_fs_new
        label_on_node1, counter_on_node1 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node1_data
        )
        if label_on_node1 is None and counter_on_node1 is None:
            log.info(f"Mirrored FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"Mirrored FS : {source_fs_new} still exists in the metrics.")
            raise CommandFailed("Mismatch in the metrics.")

        label_on_node2, counter_on_node2 = fs_mirroring_utils.get_labels_and_counters(
            resource_name, filesystem_name, cephfs_mirror_node2_data
        )
        if label_on_node2 is None and counter_on_node2 is None:
            log.info(f"Mirrored FS : {source_fs_new} is removed from the metrics.")
        else:
            log.error(f"Mirrored FS : {source_fs_new} still exists in the metrics.")
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


def fetch_cephfs_mirror_counters(fsid, asok_files):
    """
    Get the cephfs mirror counters from the admin socket files.

    Parameters:
        fsid (str): Filesystem ID.
        asok_files (dict): Dictionary containing the asok files for each node.

    Returns:
        dict: Dictionary containing the cephfs mirror counters for each node.
    """
    counters = {}
    for node_name, asok_file_info in asok_files.items():
        asok_file = asok_file_info[1]
        command = f"ceph --admin-daemon {asok_file} counter dump -f json"
        out, _ = asok_file_info[0].exec_command(
            sudo=True, cmd=f"cd /var/run/ceph/{fsid}/ ; {command}"
        )
        data = json.loads(out)
        counters[node_name] = data
        log.info(f"Output of Metrics Report for {node_name}: {data}")
    return counters


def fetch_mirror_node_data(json_output, cephfs_mirror_node):
    if isinstance(json_output, dict):
        data_dict = json_output
    else:
        data_dict = json.loads(json_output)
    if cephfs_mirror_node in data_dict:
        cephfs_mirror_node_data = data_dict[cephfs_mirror_node]
        return json.dumps(cephfs_mirror_node_data, indent=4)
    else:
        return f"Node '{cephfs_mirror_node}' not found in the data."
