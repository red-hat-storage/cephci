import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573995 - Export the nfs share with cli using an existing path
    Pre-requisites:
    1. Create cephfs volume
       create fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Mount cephfs with kernel & fuse clients
    2. Create directories on cephfs kernel & fuse mounts
    3. Create cephfs nfs export on directories created on kernel & fuse mounts
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    4. Verify path of cephfs nfs export
       ceph nfs export get <nfs_name> <nfs_export_name>
    5. Mount nfs export.
    6. Run IO's on both exports
    7. Move data b/w cephfs mounts and nfs exports and vice versa.

    Clean-up:
    1. Remove all data from cephfs
    2. Remove cephfs nfs export
    3. Remove NFS Cluster
    """
    try:
        tc = "CEPH-11309"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        default_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
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

        client1.exec_command(sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}dir1")
        client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}dir2")

        nfs_export_1 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_2 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path_1 = "/dir1"
        export_path_2 = "/dir2"
        fs_name = "cephfs" if not erasure else "cephfs-ec"

        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_1} path={export_path_1}",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_2} path={export_path_2}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_1} {fs_name} path={export_path_1}",
            )
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_2} {fs_name} path={export_path_2}",
            )
        nfs_mounting_dir_1 = f"/mnt/cephfs_{mounting_dir}_1/"
        nfs_mounting_dir_2 = f"/mnt/cephfs_{mounting_dir}_2/"
        commands = [
            f"mkdir -p {nfs_mounting_dir_1}",
            f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_1} {nfs_mounting_dir_1}",
            f"mkdir -p {nfs_mounting_dir_2}",
            f"mount -t nfs -o port=2049 {nfs_server}:{nfs_export_2} {nfs_mounting_dir_2}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)

        fs_util.run_ios(client1, nfs_mounting_dir_1)
        fs_util.run_ios(client1, nfs_mounting_dir_2)

        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        commands = [
            f"rm -rf {nfs_mounting_dir_1}*",
            f"umount -l {nfs_mounting_dir_1}",
            f"umount -l {nfs_mounting_dir_2}",
            f"umount -l {nfs_mounting_dir_2}",
            f"rm -rf {nfs_mounting_dir_1}",
            f"rm -rf {nfs_mounting_dir_2}",
            f"ceph nfs export delete {nfs_name} {nfs_export_1}",
            f"ceph nfs export delete {nfs_name} {nfs_export_2}",
            f"ceph nfs cluster rm {nfs_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
