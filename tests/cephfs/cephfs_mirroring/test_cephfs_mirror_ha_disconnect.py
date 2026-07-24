import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575334 - On a 2 Node Mirroring HA service, Interrupt the Sync on one of the HA nodes
    and ensure the sync is still served from the other Node
    It has pre-requisites,
    performs various operations, and cleans up the system after the tests.

    Scenarios Covered :
    Sceneario 1 : Disconnect the mirroring Node network for 90 seconds.
    Sceneario 2 : Disconnect the mirroring by removing the peer connection.
    Sceneario 3 : Disconnect mds nodes on source cluster
    Sceneario 4 : Disconnect mds nodes on target cluster

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
        source_fs = "cephfs_nw_ha" if not erasure else "cephfs_nw_ha-ec"
        target_fs = "cephfs_rem_ha" if not erasure else "cephfs_rem_ha-ec"
        target_user = "mirror_remote_ha_nw"
        target_site_name = "remote_site_ha_nw"
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
        #
        log.info("Add subvolumes for mirroring to remote location")
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol1_path
        )
        fs_mirroring_utils.add_path_for_mirroring(
            source_clients[0], source_fs, subvol2_path
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
        snap1 = "snap_k1_nw"
        snap2 = "snap_f1_nw"
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {kernel_mounting_dir_1}/{subvol1_path}/{mounting_dir};"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1 "
            f"--files 5 --files-per-dir 1 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}/{subvol1_path}/{mounting_dir}",
        )

        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}",
        )
        fsid = fs_mirroring_utils.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")
        daemon_names = fs_mirroring_utils.get_daemon_name(source_clients[0])

        asok_files = fs_mirroring_utils.get_asok_file(
            cephfs_mirror_node, fsid, daemon_names
        )
        log.info(f"Admin Socket file of cephfs-mirror daemon : {asok_files}")
        filesystem_id = fs_mirroring_utils.get_filesystem_id_by_name(
            source_clients[0], source_fs
        )
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")
        log.info(asok_files)

        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="yum install -y ceph-common --nogpgcheck")

        log.info(
            "=" * 50
            + "\nSceneario 1 : Disconnect the mirroring Node network for 90 seconds.\n"
            + "=" * 50
        )
        aosk_files_node2 = {
            key: value
            for key, value in asok_files.items()
            if key != cephfs_mirror_node[0].node.hostname
        }
        with parallel() as p:
            p.spawn(
                fs_util_ceph1.network_disconnect_v1,
                cephfs_mirror_node[0],
                sleep_time="90",
            )
            p.spawn(
                fs_mirroring_utils.validate_snapshot_sync_status,
                cephfs_mirror_node[1],
                source_fs,
                snap1,
                fsid,
                aosk_files_node2,
                filesystem_id,
                peer_uuid,
            )

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

        log.info(
            f"{snap1} was synced even after disconnecting the mirror daemon node for 90 seconds "
        )
        log.info(
            "=" * 50
            + "\nSceneario 2 : Disconnect the mirroring by removing the peer connection.\n"
            + "=" * 50
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {fuse_mounting_dir_1}/{subvol2_path}/{mounting_dir};"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1 "
            f"--files 5 --files-per-dir 1 --dirs-per-dir 2 --top "
            f"{fuse_mounting_dir_1}/{subvol2_path}/{mounting_dir}",
        )
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}",
        )
        fs_mirroring_utils.remove_snapshot_mirror_peer(
            source_clients[0], source_fs, peer_uuid
        )
        out, _ = source_clients[0].exec_command(
            f"ceph fs snapshot mirror peer_list {source_fs} --format json-pretty"
        )
        log.info(out)
        if out:
            log.error("Peer list not removed")
        log.info("Import token again")
        log.info(token)
        fs_mirroring_utils.import_peer_bootstrap(source_fs, token, source_clients[0])
        max_duration_seconds = 5 * 60
        start_time = time.time()
        while True:
            peer_uuid_new = fs_mirroring_utils.get_peer_uuid_by_name(
                source_clients[0], source_fs
            )
            if peer_uuid == peer_uuid_new:
                log.info("Peer UUID has not changed even after new import ")
                time.sleep(30)

                # Check if the maximum duration has been reached
                elapsed_time = time.time() - start_time
                if elapsed_time >= max_duration_seconds:
                    log.info("Maximum duration reached. Breaking out of the loop.")
                    break
            else:
                break
        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap2,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid_new,
        )
        peer_uuid = peer_uuid_new
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
        log.info(
            f"{snap2} was synced with out any errors even when mirror node bootstrap token is removed and added back"
        )
        log.info(
            "=" * 50
            + "\nSceneario 3 : Disconnect mds nodes on source cluster\n"
            + "=" * 50
        )
        mds_nodes = fs_util_ceph1.get_mds_nodes(source_clients[0], source_fs)
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {kernel_mounting_dir_1}/{subvol1_path}/{mounting_dir}_mds;"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1 "
            f"--files 5 --files-per-dir 1 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}/{subvol1_path}/{mounting_dir}_mds",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {kernel_mounting_dir_1}{subvol1_path}.snap/{snap1}_mds",
        )
        with parallel() as p:
            for mds_node in mds_nodes:
                p.spawn(fs_util_ceph1.network_disconnect_v1, mds_node, 60)

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            f"{snap1}_mds",
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
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
        log.info(
            f"{snap1}_mds was synced with out any errors even when mds nodes disconnected on source cluster"
        )
        log.info(
            "=" * 50
            + "\nSceneario 4 : Disconnect mds nodes on target cluster\n"
            + "=" * 50
        )
        mds_nodes_target = fs_util_ceph2.get_mds_nodes(target_clients[0], target_fs)
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir -p {fuse_mounting_dir_1}/{subvol2_path}/{mounting_dir};"
            f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 1 --file-size 1 "
            f"--files 5 --files-per-dir 1 --dirs-per-dir 2 --top "
            f"{fuse_mounting_dir_1}/{subvol2_path}/{mounting_dir}",
        )
        source_clients[0].exec_command(
            sudo=True,
            cmd=f"mkdir {fuse_mounting_dir_1}{subvol2_path}.snap/{snap2}_mds",
        )
        with parallel() as p:
            for mds_node in mds_nodes_target:
                p.spawn(fs_util_ceph2.network_disconnect_v1, mds_node, 60)

        fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            f"{snap2}_mds",
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid_new,
        )
        peer_uuid = peer_uuid_new
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
        log.info(
            f"{snap2}_mds was synced with out any errors even when mds nodes disconnected on target cluster"
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
            f"{kernel_mounting_dir_1}{subvol1_path}.snap/snap_k1*",
            f"{fuse_mounting_dir_1}{subvol2_path}.snap/snap_f1*",
        ]
        for snapshot_path in snapshots_to_delete:
            source_clients[0].exec_command(
                sudo=True, cmd=f"rmdir {snapshot_path}", check_ec=False
            )

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
