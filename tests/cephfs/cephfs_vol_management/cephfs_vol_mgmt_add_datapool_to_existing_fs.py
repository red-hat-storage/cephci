import random
import string
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. create fs volume create cephfs and cephfs-ec

    Operations :
    1. ceph fs subvolumegroup create <vol_name> <group_name> --mode <octal_value>
    2. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
    3. Mount subvolume on both fuse and kernel clients and run IO's
    4. Add additional Data Pool to the existing FS
    5. Run IO's again.

    Clean-up:
    1. Remove files from mountpoint, Unmount subvolumes.
    2. ceph fs subvolume rm <vol_name> <subvol_name> [--group_name <subvol_group_name>]
    3. ceph fs subvolumegroup rm <vol_name> <group_name>
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

        log.info("Create SubVolumeGroups on each filesystem")
        subvolumegroup_list = [
            {"vol_name": default_fs, "group_name": "subvolgroup_1"},
            {"vol_name": "cephfs-ec", "group_name": "subvolgroup_ec1"},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        log.info(
            "Create a Sub volumes on each of the subgroup on different pool with Size 5GB"
        )
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_ec1",
                "size": "5368709120",
            },
        ]
        for subvolume in subvolume_list:
            fs_util.create_subvolume(clients[0], **subvolume)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        log.info("Mount 1 subvolume on kernel and 1 subvloume on Fuse → Client1")
        if build.startswith("5"):
            kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of subvolume on default filesystem")
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_1 subvolgroup_1",
            )
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_1,
                ",".join(mon_node_ips),
                sub_dir=f"{subvol_path.strip()}",
                extra_params=f",fs={default_fs}",
            )
            log.info("Get the path of subvolume on EC filesystem")
            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_EC_1/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_2 subvolgroup_ec1",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f" -r {subvol_path.strip()} --client_fs cephfs-ec",
            )
        else:
            kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of subvolume on default filesystem")
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
            log.info("Get the path of subvolume on EC filesystem")
            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_EC_1/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_2 subvolgroup_ec1",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f" -r {subvol_path.strip()}",
            )

        log.info("Run IO's")
        with parallel() as p:
            for i in [
                kernel_mounting_dir_1,
                fuse_mounting_dir_1,
            ]:
                p.spawn(fs_util.run_ios, clients[0], i)

        log.info("Create 2 Data Pools, 1 - Replicated , 1 - EC Data Pool")
        create_pools = [
            "ceph osd pool create cephfs-new-data-pool",
            "ceph osd pool create cephfs-new-data-pool-ec erasure",
            "ceph osd pool set cephfs-new-data-pool-ec allow_ec_overwrites true",
        ]
        for cmd in create_pools:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Add created data pools to each of the filesystem")
        add_pool_to_FS = [
            "ceph fs add_data_pool cephfs cephfs-new-data-pool",
            "ceph fs add_data_pool cephfs-ec cephfs-new-data-pool-ec",
        ]
        for cmd in add_pool_to_FS:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Run IO's")
        with parallel() as p:
            for i in [
                kernel_mounting_dir_1,
                fuse_mounting_dir_1,
            ]:
                p.spawn(fs_util.run_ios, clients[0], i)

        log.info("Clean up the system")
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)

        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
