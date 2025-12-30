import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH- - Performs rsync vs mirroring tests: Mixed file sizes in MiB

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
        mount_type = config.get("mount_type", "kernel")
        subvol = config.get("subvol", "subvol_1")
        target_size_GiB = config.get("target_size_GiB", "5")
        file_block_size = config.get("file_block_size", "1M")
        max_file_size = config.get("max_file_size", "10")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_util_v1_ceph1 = FsUtilsV1(
            ceph_cluster_dict.get("ceph1"), test_data=test_data
        )
        fs_util_v1_ceph2 = FsUtilsV1(
            ceph_cluster_dict.get("ceph2"), test_data=test_data
        )
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
        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": "subvolgroup_1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util_ceph1.create_subvolumegroup(source_clients[0], **subvolumegroup)

        subvolume_list = [
            {
                "vol_name": source_fs,
                "subvol_name": subvol,
                "group_name": "subvolgroup_1",
                "size": "32212254720",
            }
        ]
        for subvolume in subvolume_list:
            fs_util_ceph1.create_subvolume(source_clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount FS as %s → Client1", mount_type)
        mounting_dir_1 = f"/mnt/cephfs_{mount_type}{mounting_dir}_1/"
        mon_node_ips = fs_util_ceph1.get_mon_node_ips()

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
        full_subvolume_path = f"{mounting_dir_1}{subvol_path}"
        if mount_type == "nfs":
            source_nfs_servers = ceph_cluster_dict.get("ceph1").get_ceph_objects("nfs")
            target_nfs_servers = ceph_cluster_dict.get("ceph2").get_ceph_objects("nfs")
            source_nfs_server = source_nfs_servers[0].node.hostname
            target_nfs_server = target_nfs_servers[0].node.hostname
            nfs_name = "cephfs-nfs"
            log.info("Create NFS cluster and NFS export on source and target clusters")

            log.info("Enable NFS module on source cluster")
            source_clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")
            log.info("Enable NFS module on target cluster")
            target_clients[0].exec_command(sudo=True, cmd="ceph mgr module enable nfs")

            log.info("Create nfs cluster on source cluster")
            fs_util_v1_ceph1.create_nfs(
                source_clients[0],
                nfs_cluster_name=nfs_name,
                nfs_server_name=source_nfs_server,
            )
            if wait_for_process(
                client=source_clients[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully on source cluster")
            else:
                raise CommandFailed("Failed to create nfs cluster on source cluster")

            log.info("Create nfs cluster on target cluster")
            fs_util_v1_ceph1.create_nfs(
                target_clients[0],
                nfs_cluster_name=nfs_name,
                nfs_server_name=target_nfs_server,
            )
            if wait_for_process(
                client=target_clients[0], process_name=nfs_name, ispresent=True
            ):
                log.info("ceph nfs cluster created successfully on target cluster")
            else:
                raise CommandFailed("Failed to create nfs cluster on target cluster")

            source_nfs_export_name = "/export_source" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            target_nfs_export_name = "/export_target" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            log.info("Create nfs export on source cluster")
            fs_util_v1_ceph1.create_nfs_export(
                source_clients[0],
                nfs_name,
                source_nfs_export_name,
                source_fs,
                path=subvol_path,
            )
            log.info(source_nfs_export_name)

            log.info("Create nfs export on target cluster")
            export_path = "/"

            fs_util_v1_ceph2.create_nfs_export(
                target_clients[0],
                nfs_name,
                target_nfs_export_name,
                target_fs,
                path=export_path,
            )
            log.info(source_nfs_export_name)
            full_subvolume_path = f"{mounting_dir_1}"
        log.info("full_subvolume_path is: %s", full_subvolume_path)
        if mount_type == "kernel":
            fs_util_ceph1.kernel_mount(
                [source_clients[0]],
                mounting_dir_1,
                ",".join(mon_node_ips),
                extra_params=f",fs={source_fs}",
            )
        elif mount_type == "fuse":
            fs_util_ceph1.fuse_mount(
                [source_clients[0]],
                mounting_dir_1,
                extra_params=f" --client_fs {source_fs}",
            )
        else:
            rc = fs_util_ceph1.cephfs_nfs_mount(
                source_clients[0],
                source_nfs_server,
                source_nfs_export_name,
                mounting_dir_1,
            )
            if not rc:
                log.error("cephfs nfs export %s mount failed", source_nfs_export_name)
                return 1

        log.info(
            "Adding data to subvolume %s: 1M to %sM file sizes", subvol, max_file_size
        )

        cmd = f"""
        mkdir -p {full_subvolume_path}/{mounting_dir}
        bash -c '
        target=$(({target_size_GiB}*1024))
        written=0
        idx=1
        while [ $written -lt $target ]; do
            size=$((RANDOM % {max_file_size} + 1))
            dd if=/dev/zero of="{full_subvolume_path}/{mounting_dir}/file_${{idx}}_${{size}}M.bin" \
            bs={file_block_size} count=$size status=none
            written=$((written + size))
            idx=$((idx + 1))
        done
        '
        """
        source_clients[0].exec_command(sudo=True, cmd=cmd)
        snapshot_name = f"snap_{mount_type}_1m_{max_file_size}m_sized_files"

        log.info("Creating snapshot")

        if mount_type == "nfs":
            source_clients[0].exec_command(
                sudo=True,
                cmd=(
                    f"ceph fs subvolume snapshot create {source_fs} {subvol} "
                    f"{snapshot_name} --group_name subvolgroup_1"
                ),
            )
        else:
            source_clients[0].exec_command(
                sudo=True, cmd=f"mkdir {full_subvolume_path}/.snap/{snapshot_name}"
            )
        snap_count = 1
        log.info("Validate the Snapshot Synchronisation on Target Cluster")
        validate_syncronisation = fs_mirroring_utils.validate_synchronization(
            cephfs_mirror_node[0], source_clients[0], source_fs, snap_count
        )

        if validate_syncronisation:
            log.error("Snapshot Synchronisation failed..")
            raise CommandFailed("Snapshot Synchronisation failed")

        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info("fsid on ceph cluster : %s", fsid)
        daemon_name = fs_mirroring_utils.get_daemon_name(source_clients[0])
        log.info("Name of the cephfs-mirror daemon : %s", daemon_name)
        asok_file = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node[0], fsid, daemon_name
        )
        log.info("Admin Socket file of cephfs-mirror daemon : %s", asok_file)
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info("filesystem id of %s is : %s", source_fs, filesystem_id)
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info("peer uuid of %s is : %s", source_fs, peer_uuid)

        log.info("Validate if the Snapshots are syned to Target Cluster")

        result_snap = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node[0],
            source_fs,
            snapshot_name,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        if result_snap:
            log.info("Snapshot %s has been synced:", result_snap["snapshot_name"])
            log.info(
                "Sync Duration: %s of '%s'",
                result_snap["sync_duration"],
                result_snap["snapshot_name"],
            )
            log.info(
                "Sync Time Stamp: %s of '%s'",
                result_snap["sync_time_stamp"],
                result_snap["snapshot_name"],
            )
            log.info(
                "Snaps Synced: %s of '%s'",
                result_snap["snaps_synced"],
                result_snap["snapshot_name"],
            )

            log.info("snapshot %s synced.", snapshot_name)
        else:
            log.error("snapshot %s not found or not synced.", snapshot_name)

        target_mounting_dir_1 = f"/mnt/remote_{mount_type}"

        from_live_head = f"{target_mounting_dir_1}/from_live_head_{mount_type}"
        from_snap_directory = (
            f"{target_mounting_dir_1}/from_snap_directory_{mount_type}"
        )

        if mount_type == "kernel":
            ceph2_mon_node_ips = fs_util_ceph2.get_mon_node_ips()

            fs_util_ceph2.kernel_mount(
                [target_clients[0]],
                target_mounting_dir_1,
                ",".join(ceph2_mon_node_ips),
                extra_params=f",fs={target_fs}",
            )
        elif mount_type == "fuse":

            fs_util_ceph2.fuse_mount(
                [target_clients[0]],
                target_mounting_dir_1,
                extra_params=f" --client_fs {target_fs}",
            )
        else:
            rc = fs_util_ceph2.cephfs_nfs_mount(
                target_clients[0],
                target_nfs_server,
                target_nfs_export_name,
                target_mounting_dir_1,
            )
            if not rc:
                log.error("cephfs nfs export %s mount failed", target_nfs_export_name)
                return 1

        for dir1 in [from_live_head, from_snap_directory]:
            target_clients[0].exec_command(sudo=True, cmd=f"mkdir -p {dir1}")

        log.info("Run rsync from live head to remote mount -> %s", mount_type)
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{full_subvolume_path}/{mounting_dir}",
                target_path=from_live_head,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        log.info(err)
        log.info("Run rsync from snap directory to remote mount -> %s", mount_type)
        out, err = source_clients[0].exec_command(
            sudo=True,
            cmd=fs_mirroring_utils.get_rsync_command(
                source_path=f"{full_subvolume_path}/.snap/{snapshot_name}",
                target_path=from_snap_directory,
                target_ip=target_clients[0].node.ip_address,
            ),
        )
        log.info(err)
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
            if mount_type == "nfs":
                source_clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs subvolume snapshot rm {source_fs} {subvol} {snapshot_name} subvolgroup_1",
                )
            else:
                source_clients[0].exec_command(
                    sudo=True, cmd=f"rmdir {full_subvolume_path}/.snap/{snapshot_name}"
                )
            log.info("Unmount the paths")
            source_clients[0].exec_command(sudo=True, cmd=f"umount -l {mounting_dir_1}")

            log.info("Remove paths used for mirroring")
            fs_mirroring_utils.remove_path_from_mirroring(
                source_clients[0], source_fs, subvol_path
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
            source_clients[0].exec_command(sudo=True, cmd=f"rm -rf {mounting_dir_1}")

            log.info("Delete directories created for rsync")
            target_clients[0].exec_command(sudo=True, cmd=f"rm -rf {from_live_head}")
            target_clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {from_snap_directory}"
            )

            log.info("Cleanup Target Client")
            fs_mirroring_utils.cleanup_target_client(
                target_clients[0], target_mounting_dir_1
            )
