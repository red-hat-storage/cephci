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
    1. ceph fs subvolumegroup create <vol_name> <group_name> --gid <num> --uid <num>
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

        log.info("Create cephfs subvolumegroup with customized uid and gid ")
        subvolumegroup_list = [
            {
                "vol_name": default_fs,
                "group_name": "subvolgroup_1",
                "uid": "20",
                "gid": "30",
            },
            {
                "vol_name": "cephfs-ec",
                "group_name": "subvolgroup_ec1",
                "uid": "40",
                "gid": "50",
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
        )

        subvol_path, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {default_fs} subvol_2 subvolgroup_1",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
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
                extra_params=" --client_fs cephfs-ec",
            )

        log.info("Get the path of subvolume group")
        subvolgroup_default, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup getpath {default_fs} subvolgroup_1",
        )
        subvolgroup_default_path = subvolgroup_default.strip()
        subvolgroup_ec, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph fs subvolumegroup getpath cephfs-ec subvolgroup_ec1",
        )
        subvolgroup_ec_path = subvolgroup_ec.strip()

        def get_defined_uid(group_name, subvolumegroup_list):
            for subvolumegroup in subvolumegroup_list:
                if group_name == subvolumegroup["group_name"]:
                    return subvolumegroup.get("uid")

        log.info("Validate the uid of the subgroup")
        subgroup_1_uid = get_defined_uid(
            "subvolgroup_1", subvolumegroup_list=subvolumegroup_list
        )
        subgroup_2_uid = get_defined_uid(
            "subvolgroup_ec1", subvolumegroup_list=subvolumegroup_list
        )
        stat_of_uid_on_kernel_default_fs = fs_util.get_stats(
            client=clients[0],
            file_path=kernel_mounting_dir_1.rstrip("/") + subvolgroup_default_path,
            format="%u",
        )
        stat_of_uid_on_kernel_default_ec = fs_util.get_stats(
            client=clients[1],
            file_path=kernel_mounting_dir_2.rstrip("/") + subvolgroup_ec_path,
            format="%u",
        )
        stat_of_uid_on_fuse_default_fs = fs_util.get_stats(
            client=clients[0],
            file_path=fuse_mounting_dir_1.rstrip("/") + subvolgroup_default_path,
            format="%u",
        )
        stat_of_uid_on_fuse_default_ec = fs_util.get_stats(
            client=clients[1],
            file_path=fuse_mounting_dir_2.rstrip("/") + subvolgroup_ec_path,
            format="%u",
        )
        if int(subgroup_1_uid) != int(stat_of_uid_on_kernel_default_fs) and int(
            subgroup_1_uid
        ) != int(stat_of_uid_on_fuse_default_fs):
            log.error("UID is mismatching on sunvolgroup_1")
            return 1
        if int(subgroup_2_uid) != int(stat_of_uid_on_fuse_default_ec) and int(
            subgroup_2_uid
        ) != int(stat_of_uid_on_kernel_default_ec):
            log.error("UID is mismatching on subvolgroup_ec1")
            return 1

        def get_defined_gid(group_name, subvolumegroup_list):
            for subvolumegroup in subvolumegroup_list:
                if group_name == subvolumegroup["group_name"]:
                    return subvolumegroup.get("gid")

        log.info("Validate the gid of the subgroup")
        subgroup_1_gid = get_defined_gid(
            "subvolgroup_1", subvolumegroup_list=subvolumegroup_list
        )
        subgroup_2_gid = get_defined_gid(
            "subvolgroup_ec1", subvolumegroup_list=subvolumegroup_list
        )
        stat_of_gid_on_kernel_default_fs = fs_util.get_stats(
            client=clients[0],
            file_path=kernel_mounting_dir_1.rstrip("/") + subvolgroup_default_path,
            format="%g",
        )
        stat_of_gid_on_kernel_default_ec = fs_util.get_stats(
            client=clients[1],
            file_path=kernel_mounting_dir_2.rstrip("/") + subvolgroup_ec_path,
            format="%g",
        )
        stat_of_gid_on_fuse_default_fs = fs_util.get_stats(
            client=clients[0],
            file_path=fuse_mounting_dir_1.rstrip("/") + subvolgroup_default_path,
            format="%g",
        )
        stat_of_gid_on_fuse_default_ec = fs_util.get_stats(
            client=clients[1],
            file_path=fuse_mounting_dir_2.rstrip("/") + subvolgroup_ec_path,
            format="%g",
        )
        if int(subgroup_1_gid) != int(stat_of_gid_on_kernel_default_fs) and int(
            subgroup_1_gid
        ) != int(stat_of_gid_on_fuse_default_fs):
            log.error("GID is mismatching on sunvolgroup_1")
            return 1
        if int(subgroup_2_gid) != int(stat_of_gid_on_kernel_default_ec) and int(
            subgroup_2_gid
        ) != int(stat_of_gid_on_fuse_default_ec):
            log.error("GID is mismatching on subvolgroup_ec1")
            return 1

        run_ios(clients[0], kernel_mounting_dir_1)
        run_ios(clients[0], fuse_mounting_dir_1)
        run_ios(clients[1], kernel_mounting_dir_2)
        run_ios(clients[1], fuse_mounting_dir_2)

        log.info("Clean up the system")
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)

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
