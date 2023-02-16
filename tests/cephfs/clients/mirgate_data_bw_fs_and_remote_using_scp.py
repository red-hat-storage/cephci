import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-11299 - scp of files from kernel/fuse to local and remote directory.
        Pre-requisites:
    1. Create cephfs volume
       ceph fs create volume create <vol_name>

    Test operation:
    1. Create 2 subvolumes
    ceph fs subvolume create <sub_v1>
    2. Run IO's and fill data into subvolumes mounted on kernel and fuse directories.
    3. scp of date file from kernel mount to local and remote dir and validate the copy
    4. scp of date file from fuse mount to local and remote dir and validate the copy

    Clean-up:
    1. remove directories
    2. unmount paths
    3. Remove subvolumes, subvolume groups.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
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
        client1_ip = clients[0].node.ip_address
        client2_ip = clients[1].node.ip_address
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

        log.info("Create 2 Sub volumes on each of the pool with Size 5GB")
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
        subvol_path, rc = clients[1].exec_command(
            sudo=True,
            cmd="ceph fs subvolume getpath cephfs-ec subvol_2 subvolgroup_ec1",
        )
        fs_util.fuse_mount(
            [clients[1]],
            fuse_mounting_dir_1,
            extra_params=f" -r {subvol_path.strip()} --client_fs cephfs-ec",
        )

        io_tool = ["dd"]
        io_file_kernelname = "file_kernel"
        io_file_fusename = "file_fuse"
        fs_util.run_ios(
            clients[0], kernel_mounting_dir_1, io_tool, f"{io_file_kernelname}"
        )
        fs_util.run_ios(clients[1], fuse_mounting_dir_1, io_tool, f"{io_file_fusename}")
        path1 = f"{kernel_mounting_dir_1}{clients[0].node.hostname}_dd_file_kernel"
        path2 = f"{fuse_mounting_dir_1}{clients[1].node.hostname}_dd_file_fuse"

        local_dir = f"/mnt/local_{mounting_dir}_1/"
        clients[0].exec_command(sudo=True, cmd=f"mkdir -p {local_dir}")
        clients[1].exec_command(sudo=True, cmd=f"mkdir -p {local_dir}")
        remote_dir = f"/mnt/remote_{mounting_dir}_1/"
        clients[0].exec_command(sudo=True, cmd=f"mkdir -p {remote_dir}")
        clients[1].exec_command(sudo=True, cmd=f"mkdir -p {remote_dir}")

        log.info("scp from kernel mount to a local and remote dir.")
        commands = [
            f"scp {path1} {local_dir}",
            f"scp {path1} {client2_ip}:{remote_dir}",
        ]
        for cmd in commands:
            clients[0].exec_command(sudo=True, cmd=cmd)

        log.info("scp from fuse mount to a local and remote dir")
        commands = [
            f"scp {path2} {local_dir}",
            f"scp {path2} {client1_ip}:{remote_dir}",
        ]
        for cmd in commands:
            clients[1].exec_command(sudo=True, cmd=cmd)

        verify_data_movement = [
            f"diff -qr {path1} {local_dir}{clients[0].node.hostname}_dd_file_kernel",
            f"diff -qr <(ssh {client2_ip} 'cat {path2}') {remote_dir}{clients[1].node.hostname}_dd_file_fuse",
        ]
        for cmd in verify_data_movement:
            clients[0].exec_command(sudo=True, cmd=cmd)
        verify_data_movement = [
            f"diff -qr {path2} {local_dir}{clients[1].node.hostname}_dd_file_fuse",
            f"diff -qr <(ssh {client1_ip} ' cat {path1}') {remote_dir}{clients[0].node.hostname}_dd_file_kernel",
        ]
        for cmd in verify_data_movement:
            clients[1].exec_command(sudo=True, cmd=cmd)
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf {local_dir}", check_ec=False)
        clients[0].exec_command(sudo=True, cmd=f"rm -rf {remote_dir}", check_ec=False)
        clients[1].exec_command(sudo=True, cmd=f"rm -rf {local_dir}", check_ec=False)
        clients[1].exec_command(sudo=True, cmd=f"rm -rf {remote_dir}", check_ec=False)
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_1
        )
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )

        for subvolume in subvolume_list:
            fs_util.remove_subvolume(clients[0], **subvolume)

        for subvolumegroup in subvolumegroup_list:
            fs_util.remove_subvolumegroup(clients[0], **subvolumegroup)
