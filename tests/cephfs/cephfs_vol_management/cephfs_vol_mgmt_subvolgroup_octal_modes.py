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

    Subvolume Group Operations :
    1. ceph fs subvolumegroup create <vol_name> <group_name> --mode <octal_value>
    2. ceph fs subvolume create <vol_name> <subvol_name> [--size <size_in_bytes>] [--group_name <subvol_group_name>]
    3. Mount subvolume on both fuse and kernel clients and run IO's

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

        log.info("Create cephfs subvolumegroup with different octal modes")
        subvolumegroup_list = [
            {
                "vol_name": default_fs,
                "group_name": "subvolgroup_1",
                "mode": "777",
            },
            {
                "vol_name": default_fs,
                "group_name": "subvolgroup_2",
                "mode": "700",
            },
            {
                "vol_name": "cephfs-ec",
                "group_name": "subvolgroup_ec1",
                "mode": "755",
            },
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        log.info("Create 2 Sub volumes on each of the subvolume group of Size 5GB")
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
                "vol_name": default_fs,
                "subvol_name": "subvol_3",
                "group_name": "subvolgroup_2",
                "size": "5368706371",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_4",
                "group_name": "subvolgroup_2",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_5",
                "group_name": "subvolgroup_ec1",
                "size": "5368706371",
            },
            {
                "vol_name": "cephfs-ec",
                "subvol_name": "subvol_6",
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
            "Mount 1 subvolumegroup/subvolume on kernel and 1 subvolume on Fuse → Client1"
        )
        if build.startswith("5"):
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
                extra_params=f",fs={default_fs}",
            )
            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_1",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
                extra_params=f" --client_fs {default_fs}",
            )

            kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_3 subvolgroup_2",
            )
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_2,
                ",".join(mon_node_ips),
                extra_params=f",fs={default_fs}",
            )
            fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_4 subvolgroup_2",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_2,
                extra_params=f" --client_fs {default_fs}",
            )

            kernel_mounting_dir_3 = f"/mnt/cephfs_kernel{mounting_dir}_EC_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")
            subvol_path, rc = clients[1].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_5 subvolgroup_ec1",
            )
            fs_util.kernel_mount(
                [clients[1]],
                kernel_mounting_dir_3,
                ",".join(mon_node_ips),
                extra_params=",fs=cephfs-ec",
            )
            fuse_mounting_dir_3 = f"/mnt/cephfs_fuse{mounting_dir}_EC_1/"
            subvol_path, rc = clients[1].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_6 subvolgroup_ec1",
            )
            fs_util.fuse_mount(
                [clients[1]],
                fuse_mounting_dir_3,
                extra_params=" --client_fs cephfs-ec",
            )
        else:
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
            )
            fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_1",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_1,
            )

            kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_3 subvolgroup_2",
            )
            fs_util.kernel_mount(
                [clients[0]],
                kernel_mounting_dir_2,
                ",".join(mon_node_ips),
            )
            fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} subvol_4 subvolgroup_2",
            )
            fs_util.fuse_mount(
                [clients[0]],
                fuse_mounting_dir_2,
            )

            kernel_mounting_dir_3 = f"/mnt/cephfs_kernel{mounting_dir}_EC_1/"
            mon_node_ips = fs_util.get_mon_node_ips()
            log.info("Get the path of sub volume")
            subvol_path, rc = clients[1].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_5 subvolgroup_ec1",
            )
            fs_util.kernel_mount(
                [clients[1]],
                kernel_mounting_dir_3,
                ",".join(mon_node_ips),
                extra_params=",fs=cephfs-ec",
            )
            fuse_mounting_dir_3 = f"/mnt/cephfs_fuse{mounting_dir}_EC_1/"
            subvol_path, rc = clients[0].exec_command(
                sudo=True,
                cmd="ceph fs subvolume getpath cephfs-ec subvol_6 subvolgroup_ec1",
            )
            fs_util.fuse_mount(
                [clients[1]],
                fuse_mounting_dir_3,
                extra_params=" --client_fs cephfs-ec",
            )

        log.info("Get the path of subvolume groups")
        subvolgroup1_getpath, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup getpath {default_fs} subvolgroup_1",
        )
        subvolgroup1_getpath = subvolgroup1_getpath.strip()

        subvolgroup2_getpath, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup getpath {default_fs} subvolgroup_2",
        )
        subvolgroup2_getpath = subvolgroup2_getpath.strip()

        subvolgroup_ec_getpath, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph fs subvolumegroup getpath cephfs-ec subvolgroup_ec1",
        )
        subvolgroup_ec_getpath = subvolgroup_ec_getpath.strip()

        def get_defined_mode(group_name, subvolumegroup_list):
            for subvolumegroup in subvolumegroup_list:
                if group_name == subvolumegroup["group_name"]:
                    return subvolumegroup.get("mode")

        log.info("Validate the octal mode set on the subgroup")
        subgroup_1_mode = get_defined_mode(
            "subvolgroup_1", subvolumegroup_list=subvolumegroup_list
        )
        subgroup_2_mode = get_defined_mode(
            "subvolgroup_2", subvolumegroup_list=subvolumegroup_list
        )
        subgroup_ec_mode = get_defined_mode(
            "subvolgroup_ec1", subvolumegroup_list=subvolumegroup_list
        )
        stat_of_octal_mode_on_kernel_dir1 = fs_util.get_stats(
            client=clients[0],
            file_path=kernel_mounting_dir_1.rstrip("/") + subvolgroup1_getpath,
            format="%a",
        )
        stat_of_octal_mode_on_kernel_dir2 = fs_util.get_stats(
            client=clients[0],
            file_path=kernel_mounting_dir_2.rstrip("/") + subvolgroup2_getpath,
            format="%a",
        )
        stat_of_octal_mode_on_kernel_dir3 = fs_util.get_stats(
            client=clients[1],
            file_path=kernel_mounting_dir_3.rstrip("/") + subvolgroup_ec_getpath,
            format="%a",
        )
        stat_of_octal_mode_on_fuse_dir1 = fs_util.get_stats(
            client=clients[0],
            file_path=fuse_mounting_dir_1.rstrip("/") + subvolgroup1_getpath,
            format="%a",
        )
        stat_of_octal_mode_on_fuse_dir2 = fs_util.get_stats(
            client=clients[0],
            file_path=fuse_mounting_dir_2.rstrip("/") + subvolgroup2_getpath,
            format="%a",
        )
        stat_of_octal_mode_on_fuse_dir3 = fs_util.get_stats(
            client=clients[1],
            file_path=fuse_mounting_dir_3.rstrip("/") + subvolgroup_ec_getpath,
            format="%a",
        )

        if int(subgroup_1_mode) != int(stat_of_octal_mode_on_kernel_dir1) and int(
            subgroup_1_mode
        ) != int(stat_of_octal_mode_on_fuse_dir1):
            log.error("Octal values are mismatching on subvolgroup_1")
            return 1
        if int(subgroup_2_mode) != int(stat_of_octal_mode_on_kernel_dir2) and int(
            subgroup_2_mode
        ) != int(stat_of_octal_mode_on_fuse_dir2):
            log.error("Octal values are mismatching on subvolgroup_2")
            return 1
        if int(subgroup_ec_mode) != int(stat_of_octal_mode_on_kernel_dir3) and int(
            subgroup_ec_mode
        ) != int(stat_of_octal_mode_on_fuse_dir3):
            log.error("Octal values are mismatching on subvolgroup_ec1")
            return 1

        log.info("Run IO's")
        with parallel() as p:
            for i in [
                kernel_mounting_dir_1,
                fuse_mounting_dir_1,
                kernel_mounting_dir_2,
                fuse_mounting_dir_2,
            ]:
                p.spawn(fs_util.run_ios, clients[0], i)

            for i in [kernel_mounting_dir_3, fuse_mounting_dir_3]:
                p.spawn(fs_util.run_ios, clients[1], i)

        log.info("Clean up the system")
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)

        for i in [kernel_mounting_dir_1, kernel_mounting_dir_2]:
            fs_util.client_clean_up(
                "umount", kernel_clients=[clients[0]], mounting_dir=i
            )

        for i in [fuse_mounting_dir_1, fuse_mounting_dir_2]:
            fs_util.client_clean_up("umount", fuse_clients=[clients[0]], mounting_dir=i)

        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[1]], mounting_dir=kernel_mounting_dir_3
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[1]], mounting_dir=fuse_mounting_dir_3
        )

        clients[1].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
