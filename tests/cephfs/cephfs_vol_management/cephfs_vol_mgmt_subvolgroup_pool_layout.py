import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. create fs volume create cephfs and cephfs-ec

    Subvolume Group Operations :
    1. ceph fs subvolumegroup create <vol_name> <group_name> --pool_layout <data_pool_name>
    2. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
    3. Mount subvolume on both fuse and kernel clients and run IO's

    Clean-up:
    1. Remove files from mountpoint, Unmount subvolumes.
    2. Remove the pools added as part of pool_layout
    3. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    4. ceph fs subvolumegroup rm <vol_name> <group_name>
    """

    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs"
        if build.startswith("4"):
            # create EC pool
            list_cmds = [
                "ceph fs flag set enable_multiple true",
                "ceph osd pool create cephfs-data-ec 64 erasure",
                "ceph osd pool create cephfs-metadata 64",
                "ceph osd pool set cephfs-data-ec allow_ec_overwrites true",
                "ceph fs new cephfs-ec cephfs-metadata cephfs-data-ec --force",
            ]
            if fs_util.get_fs_info(clients[0], "cephfs_new"):
                default_fs = "cephfs_new"
                list_cmds.append("ceph fs volume create cephfs")
            for cmd in list_cmds:
                clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Create 2 pools, 1 - Replicated , 1 - EC Data Pool")
        create_pools = [
            "ceph osd pool create cephfs-data-pool-layout",
            "ceph osd pool create cephfs-data-pool-layout-ec 64 erasure",
            "ceph osd pool set cephfs-data-pool-layout-ec allow_ec_overwrites true",
        ]
        for cmd in create_pools:
            clients[0].exec_command(sudo=True, cmd=cmd)
        log.info("Add created data pools to each of the filesystem")
        add_pool_to_FS = [
            "ceph fs add_data_pool cephfs cephfs-data-pool-layout",
            "ceph fs add_data_pool cephfs-ec cephfs-data-pool-layout-ec",
        ]
        for cmd in add_pool_to_FS:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Create cephfs subvolumegroup with desired data pool_layout")
        subvolumegroup_list = [
            {
                "vol_name": default_fs,
                "group_name": "subvolgroup_1",
                "pool_layout": "cephfs-data-pool-layout",
            },
            {
                "vol_name": "cephfs-ec",
                "group_name": "subvolgroup_ec1",
                "pool_layout": "cephfs-data-pool-layout-ec",
            },
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        log.info("Create 2 Sub volumes on each of the subvolume group Size 5 GB")
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368706371",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_1",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_3",
                "group_name": "subvolgroup_ec1",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_4",
                "group_name": "subvolgroup_ec1",
                "size": "5368706371",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info(
            "Mount 1 subvolumegroup/subvolume on kernel and 1 subvloume on Fuse → Client1"
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        log.info("Get the path of sub volume")
        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_1 subvolgroup_1",
        )
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            sub_dir=f"{subvol_path.strip()}",
        )

        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_1",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()}",
        )

        log.info(
            "On EC,Mount 1 subvolumegroup/subvolume on kernal and 1 subvloume on Fuse → Client2"
        )
        if build.startswith("5"):
            kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_EC_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")

            subvol_path, rc = clients[1].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_3 subvolgroup_ec1",
            )
            fs_util.kernel_mount(
                [clients[1]],
                kernel_mounting_dir_2,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.strip()}",
                extra_params=",fs=cephfs-ec",
            )

            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_4 subvolgroup_ec1",
            )
            fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_EC_1/"
            fs_util.fuse_mount(
                [clients[1]],
                fuse_mounting_dir_2,
                extra_params=f" -r {subvol_path.strip()} --client_fs cephfs-ec",
            )

        log.info(
            "Check the Pool status before the IO's to confirm if no IO's are going on on the pool attached"
        )
        get_pool_status_before = fs_util.get_pool_df(
            client=clients[0], pool_name="cephfs-data-pool-layout", vol_name=default_fs
        )
        get_pool_status_before_EC = fs_util.get_pool_df(
            client=clients[1],
            pool_name="cephfs-data-pool-layout-ec",
            vol_name="cephfs-ec",
        )

        run_ios(clients[0], kernel_mounting_dir_1)
        run_ios(clients[0], fuse_mounting_dir_1)
        run_ios(clients[1], kernel_mounting_dir_2)
        run_ios(clients[1], fuse_mounting_dir_2)

        log.info(
            "Check the Pool status and verify the IO's are going only to the Pool attached"
        )
        get_pool_status_after = fs_util.get_pool_df(
            client=clients[0], pool_name="cephfs-data-pool-layout", vol_name=default_fs
        )
        get_pool_status_after_EC = fs_util.get_pool_df(
            client=clients[1],
            pool_name="cephfs-data-pool-layout-ec",
            vol_name="cephfs-ec",
        )

        if get_pool_status_after["used"] < get_pool_status_before["used"]:
            log.error("Pool attached is unused")
            return 1
        if get_pool_status_after_EC["used"] < get_pool_status_before_EC["used"]:
            log.info("EC Pool attached is unused")
            return 1

        log.info("Clean up the system")
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_2
        )

        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_2
        )

        clients[1].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )

        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)
        log.info(
            "Remove the data pools from the filesystem and delete the created pools."
        )
        rm_pool_from_FS = [
            "ceph fs rm_data_pool cephfs cephfs-data-pool-layout",
            "ceph fs rm_data_pool cephfs-ec cephfs-data-pool-layout-ec",
            "ceph osd pool delete cephfs-data-pool-layout "
            "cephfs-data-pool-layout --yes-i-really-really-mean-it-not-faking",
            "ceph osd pool delete cephfs-data-pool-layout-ec "
            "cephfs-data-pool-layout-ec --yes-i-really-really-mean-it-not-faking",
        ]
        for cmd in rm_pool_from_FS:
            clients[0].exec_command(sudo=True, cmd=cmd)

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def run_ios(client, mounting_dir):
    def smallfile():
        client.exec_command(
            sudo=True,
            cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
            f"create symlink stat chmod ls-l delete cleanup  ; "
            f"do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i --threads 8 --file-size 10240 "
            f"--files 10 --top {mounting_dir} ; done",
        )

    def dd():
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}_dd bs=100M "
            f"count=5",
        )

    io_tools = [dd, smallfile]
    f = random.choice(io_tools)
    f()
