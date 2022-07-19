import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. creats fs volume create cephfs_new
    2. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
    3. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
       [--pool_layout <data_pool_name>] [--uid <uid>] [--gid <gid>] [--mode <octal_mode>]  [--namespace-isolated]
    4. Create Data on the subvolume → Data1 → say files 1, 2, 3

    snapshot Operations:
    1. ceph fs subvolume snapshot create <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>] → Snap1
       Write Data on Subvolume → Data2 -->say files 1,2,3 4,5,6
    2. ceph fs subvolume snapshot create <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>] → Snap2
       Write Data on Subvolume → Data3 -->say files 1,2,3 4,5,6 7,8,9 → Collect checksum
    3. Delete Data1 contents -->1,2,3
    4. Revert to Snap1(copy files from snap1 folder), then compare Data1_contents  == Data1 – >1,2,3 4,5,6 7,8,9
    5. Delete Data1 contents and Data2 Contents -->1,2,3 4,5,6
    6. Revert to Snap2(copy files from snap2 folder), then compare Data2_contents  == Data2 → 1,2,3 4,5,6 7,8,9
    7. ceph fs subvolume snapshot rm <vol_name> <subvol_name> <snap_name> [--group_name <subvol_group_name>] snap1
    8. ceph fs subvolume snapshot rm <vol_name> <subvol_name> <snap_name> → snap2

    Clean-up:
    1. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    2. ceph fs subvolumegroup rm <vol_name> <group_name>
    3. ceph fs volume rm cephfs_new --yes-i-really-mean-it
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        results = []
        tc1 = "83571366"
        log.info(f"Execution of testcase {tc1} started")
        log.info("Create and list a volume")
        commands = [
            "ceph fs volume create cephfs_new",
            "ceph fs subvolumegroup create cephfs_new snap_group cephfs.cephfs_new.data",
            "ceph fs subvolume create cephfs_new snap_vol --size 5368706371 --group_name snap_group",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        if not fs_util.wait_for_mds_process(client1, "cephfs_new"):
            raise CommandFailed("Failed to start MDS deamons")
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs_new snap_vol snap_group"
        )
        log.info("Make directory fot mounting")
        client1.exec_command(sudo=True, cmd="mkdir /mnt/mycephfs1")

        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -r {subvol_path.strip()} /mnt/mycephfs1 --client_fs cephfs_new",
        )
        fs_util.create_file_data(client1, "/mnt/mycephfs1", 3, "snap1", "snap_1_data ")
        client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume snapshot create cephfs_new snap_vol snap_1 --group_name snap_group",
        )

        fs_util.create_file_data(client1, "/mnt/mycephfs1", 3, "snap2", "snap_2_data ")
        client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume snapshot create cephfs_new snap_vol snap_2 --group_name snap_group",
        )

        fs_util.create_file_data(client1, "/mnt/mycephfs1", 3, "snap3", "snap_3_data ")
        expected_files_checksum = fs_util.get_files_and_checksum(
            client1, "/mnt/mycephfs1"
        )

        log.info("delete Files created as part of snap1")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;rm -rf snap1*")

        log.info("copy files from snapshot")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;cp .snap/_snap_1_*/* .")

        snap1_files_checksum = fs_util.get_files_and_checksum(client1, "/mnt/mycephfs1")
        if expected_files_checksum != snap1_files_checksum:
            log.error("checksum is not matching after snapshot1 revert")
            return 1

        log.info("delete Files created as part of snap2")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;rm -rf snap2*")

        log.info("copy files from snapshot")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;cp .snap/_snap_2_*/* .")

        snap2_files_checksum = fs_util.get_files_and_checksum(client1, "/mnt/mycephfs1")
        log.info(f"expected_files_checksum : {expected_files_checksum}")
        log.info(f"snap2_files_checksum : {snap2_files_checksum}")
        if expected_files_checksum != snap2_files_checksum:
            log.error("checksum is not matching after snapshot1 revert")
            return 1
        log.info("unmount the drive")
        client1.exec_command(sudo=True, cmd="fusermount -u /mnt/mycephfs1")

        log.info("cleanup the system")
        commands = [
            "ceph fs subvolume snapshot rm cephfs_new snap_vol snap_1 --group_name snap_group",
            "ceph fs subvolume snapshot rm cephfs_new snap_vol snap_2 --group_name snap_group",
            "ceph fs subvolume rm cephfs_new snap_vol --group_name snap_group",
            "ceph fs subvolumegroup rm cephfs_new snap_group",
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs volume rm cephfs_new --yes-i-really-mean-it",
            "rm -rf /mnt/mycephfs1",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")

        log.info(f"Execution of testcase {tc1} ended")
        log.info("Testcase Results:")
        for res in results:
            log.info(res)
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
