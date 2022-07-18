import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574003 - Export the nfs share with cli with RO access
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>

    Test operation:
    1. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> --readonly path=<export_path>
    2. Verify write operation is not allowed on nfs export
    3. Verify read operation is allowed

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Delete all cephfs nfs export
    """
    try:
        tc = "CEPH-83574003"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        rhbuild = config.get("rhbuild")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        nfs_export_1 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_2 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs"
        nfs_mounting_dir_1 = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        nfs_mounting_dir_2 = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name}"
                f" {nfs_export_1} --readonly path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_1} {fs_name} {export_path} --readonly",
            )
        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_1, nfs_mounting_dir_1
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        out, err = client1.exec_command(
            sudo=True, cmd=f"touch {nfs_mounting_dir_1}/file", check_ec=False
        )
        if not err:
            raise CommandFailed("NFS export has permission to write")

        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name}"
                f" {nfs_export_2} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_2} {fs_name} path={export_path}",
            )
        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_2, nfs_mounting_dir_2
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        commands = [
            f"dd if=/dev/urandom of={nfs_mounting_dir_2}/file bs=1M count=1000",
            f"dd if={nfs_mounting_dir_1}/file of=~/copy_file bs=1M count=1000",
            f"diff {nfs_mounting_dir_2}/file ~/copy_file",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        commands = [
            f"umount {nfs_mounting_dir_1}",
            f"rm -rf {nfs_mounting_dir_2}/*",
            f"umount {nfs_mounting_dir_2}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        commands = [
            f"rm -rf {nfs_mounting_dir_2}/",
            "rm -f ~/copy_file",
            f"ceph nfs export delete {nfs_name} {nfs_export_1}",
            f"ceph nfs export delete {nfs_name} {nfs_export_2}",
        ]
        for command in commands:
            client1.exec_command(
                sudo=True, cmd=command, long_running=True, check_ec=False
            )
