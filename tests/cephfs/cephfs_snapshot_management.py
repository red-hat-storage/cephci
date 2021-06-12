import logging
import random
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils

log = logging.getLogger(__name__)


def create_file_data(client, directory, no_of_files, file_name, data):
    """
    This function will write files to the directory with the data given
    :param client:
    :param directory:
    :param no_of_files:
    :param file_name:
    :param data:
    :return:
    """
    files = [f"{file_name}_{i}" for i in range(0, no_of_files)]
    client.exec_command(
        sudo=True,
        cmd=f"cd {directory};echo {data * random.randint(100,500)} | tee {' '.join(files)}",
    )


def get_files_and_checksum(client, directory):
    """
    This will collect the filenames and their respective checksums and returns the dictionary
    :param client:
    :param directory:
    :return:
    """
    out, rc = client.exec_command(
        sudo=True, cmd=f"cd {directory};ls -lrt |  awk {{'print $9'}}"
    )
    file_list = out.read().decode().strip().split()
    file_dict = {}
    for file in file_list:
        out, rc = client.exec_command(sudo=True, cmd=f"md5sum {directory}/{file}")
        md5sum = out.read().decode().strip().split()
        file_dict[file] = md5sum[0]
    return file_dict


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
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = client_info["fuse_clients"][0]
        results = []
        tc1 = "83571366"
        log.info(f"Execution of testcase {tc1} started")
        log.info("Create and list a volume")
        commands = [
            "ceph fs volume create cephfs_new",
            "ceph fs subvolumegroup create cephfs_new snap_group cephfs.cephfs_new.data",
            "ceph fs subvolume create cephfs_new snap_vol --size 5368706371 --group_name snap_group --pool_layout "
            "cephfs.cephfs_new.data",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
            results.append(f"{command} successfully executed")
        log.info("Get the path of sub volume")
        subvol_path, rc = client1.exec_command(
            sudo=True, cmd="ceph fs subvolume getpath cephfs_new snap_vol snap_group"
        )
        log.info("Make directory fot mounting")
        client1.exec_command(sudo=True, cmd="mkdir /mnt/mycephfs1")

        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -r {subvol_path.read().decode().strip()} /mnt/mycephfs1 --client_fs cephfs_new",
        )
        create_file_data(client1, "/mnt/mycephfs1", 3, "snap1", "snap_1_data ")
        client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume snapshot create cephfs_new snap_vol snap_1 --group_name snap_group",
        )

        create_file_data(client1, "/mnt/mycephfs1", 3, "snap2", "snap_2_data ")
        client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolume snapshot create cephfs_new snap_vol snap_2 --group_name snap_group",
        )

        create_file_data(client1, "/mnt/mycephfs1", 3, "snap3", "snap_3_data ")
        expected_files_checksum = get_files_and_checksum(client1, "/mnt/mycephfs1")

        log.info("delete Files created as part of snap1")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;rm -rf snap1*")

        log.info("copy files from snapshot")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;cp .snap/_snap_1_*/* .")

        snap1_files_checksum = get_files_and_checksum(client1, "/mnt/mycephfs1")
        if expected_files_checksum != snap1_files_checksum:
            log.error("checksum is not matching after snapshot1 revert")
            return 1

        log.info("delete Files created as part of snap2")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;rm -rf snap2*")

        log.info("copy files from snapshot")
        client1.exec_command(sudo=True, cmd="cd /mnt/mycephfs1;cp .snap/_snap_2_*/* .")

        snap2_files_checksum = get_files_and_checksum(client1, "/mnt/mycephfs1")
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
            "ceph fs subvolumegroup create cephfs_new snap_group cephfs.cephfs_new.data",
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
