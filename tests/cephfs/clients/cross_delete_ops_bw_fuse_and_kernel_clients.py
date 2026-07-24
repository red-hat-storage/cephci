import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573532 - scp of files from kernel/fuse to local and remote directory.
        Pre-requisites:
    1. Create cephfs volume
       ceph fs create volume create <vol_name>

    Test operation:
    1. Create 2 subvolumes
    ceph fs subvolume create <sub_v1>
    2. Run IO's and fill data into subvolumes mounted on kernel and fuse directories.
    3. delete file written within kernel mount from fuse mount
    4. delete file written within fuse mount from kernel mount

    Clean-up:
    1. remove directories
    2. unmount paths
    3. Remove subvolumes, subvolume groups.
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.setup_ssh_root_keys(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 2:
            log.info(
                f"This test requires minimum 2 client nodes.This has only {len(clients)} clients"
            )
            return 1
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(clients[0], default_fs)

        if not fs_details:
            fs_util.create_fs(clients[0], default_fs)
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
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(clients[0], **subvolumegroup)

        log.info("Create 2 Sub volumes on each of the pool with Size 5GB")
        subvolume_list = [
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_1",
                "group_name": "subvolgroup_1",
                "size": "5368709120",
            },
            {
                "vol_name": default_fs,
                "subvol_name": "subvol_2",
                "group_name": "subvolgroup_1",
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

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {default_fs}",
        )

        io_tool = ["dd"]
        io_file_kernelname = "file_kernel"
        io_file_fusename = "file_fuse"
        fs_util.run_ios(
            clients[0], kernel_mounting_dir_1, io_tool, f"{io_file_kernelname}"
        )
        fs_util.run_ios(clients[0], fuse_mounting_dir_1, io_tool, f"{io_file_fusename}")

        fuse_file_to_delete_from_kernel = (
            f"{kernel_mounting_dir_1}{clients[0].node.hostname}_dd_file_fuse"
        )
        kernel_file_to_delete_from_fuse = (
            f"{fuse_mounting_dir_1}{clients[0].node.hostname}_dd_file_kernel"
        )

        log.info("Delete file written by kernel mount from fuse mount")
        commands = [
            f"rm -rf {fuse_file_to_delete_from_kernel}",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("Delete file written by fuse mount from kernel mount")
        commands = [
            f"rm -rf {kernel_file_to_delete_from_fuse}",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)

        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
