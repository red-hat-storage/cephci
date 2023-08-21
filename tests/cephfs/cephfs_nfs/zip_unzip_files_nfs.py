import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83574026 - zip unzip files continuously on a nfs share
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create cephfs nfs export
       ceph nfs export create cephfs <fs_name> <nfs_name> <nfs_export_name> path=<export_path>
    2. Create multiple files
    3. Continuously zip & unzip files create in step 2
    4. Repeat step 2-3
    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    3. Delete cephfs nfs export
    """
    try:
        tc = "CEPH-83574024"
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
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs"
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        if "5.0" in rhbuild:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {fs_name} {nfs_name} "
                f"{nfs_export_name} path={export_path}",
            )
        else:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export_name} {fs_name} path={export_path}",
            )
        rc = fs_util.cephfs_nfs_mount(
            client1, nfs_server, nfs_export_name, nfs_mounting_dir
        )
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1
        client1.exec_command(sudo=True, cmd="yum install zip -y", long_running=True)
        client1.exec_command(sudo=True, cmd=f"mkdir {nfs_mounting_dir}/dir1..10")
        fs_util.create_file_data(
            client1, nfs_mounting_dir + "/files_dir1", 100, "file", "file_data"
        )
        fs_util.create_file_data(
            client1, nfs_mounting_dir + "/dir7", 100, "file", "random_data"
        )
        commands = [
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir2/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir1/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir2/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir2/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir3/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir2/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir3/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir3/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir4/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir3/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir4/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir4/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir5/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir4/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir5/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir5/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir6/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir5/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir6/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir6/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir7/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir6/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir7/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir7/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir8/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir7/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir8/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir8/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir9/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir8/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir9/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir9/",
            f"for n in {{1..100}}; do zip {nfs_mounting_dir}/dir10/file_$( printf %03d "
            "$n"
            " ).zip  {nfs_mounting_dir}/dir9/file_$( printf %03d "
            "$n"
            " )",
            f"for n in {{1..100}}; do unzip {nfs_mounting_dir}/dir10/file_$( printf %03d "
            "$n"
            " ).zip -d {nfs_mounting_dir}/dir10/",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        commands = [
            f"rm -rf {nfs_mounting_dir}/*",
            f"umount {nfs_mounting_dir}",
            f"ceph nfs export delete {nfs_name} {nfs_export_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
        )
