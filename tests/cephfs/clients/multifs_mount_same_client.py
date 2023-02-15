import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Create 2 cephfs volume
       creats fs volume create <vol_name>

    Test operation:
    1. Mount both cephfs with same auth_key(client)
    2. Run IO's on both cephfs mounts

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    """
    try:
        tc = "CEPH-83573877"
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
        mount_points = []
        multiple_cephfs = ["cephfs", "cephfs-ec"]
        for fs_name in multiple_cephfs:
            log.info(f"Mounting {fs_name} on fuse & kernel client with same auth_key")
            kernel_mount_dir = "/mnt/kernel_" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            fs_util.kernel_mount(
                clients,
                kernel_mount_dir,
                mon_node_ip,
                new_client_hostname="admin",
                extra_params=f",fs={fs_name}",
            )
            fuse_mount_dir = "/mnt/fuse_" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )
            mount_points.extend([kernel_mount_dir, fuse_mount_dir])
            fs_util.fuse_mount(
                clients,
                fuse_mount_dir,
                new_client_hostname="admin",
                extra_params=f" --client_fs {fs_name}",
            )
            log.info(f"Running IO's on {fs_name}")
            commands = [
                f"mkdir {kernel_mount_dir}/dir1 {fuse_mount_dir}/dir2",
                f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --top {kernel_mount_dir}/dir1",
                f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --top {kernel_mount_dir}/dir1",
                f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --top {fuse_mount_dir}/dir2",
                f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation read --threads 10 --file-size 4 "
                f"--files 1000 --files-per-dir 10 --top {fuse_mount_dir}/dir2",
            ]
            for command in commands:
                err = client1.exec_command(sudo=True, cmd=command, long_running=True)
                if err:
                    return 1
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        client1.exec_command(sudo=True, cmd=f"rm -rf {mount_points[0]}/*")
        client1.exec_command(sudo=True, cmd=f"rm -rf {mount_points[2]}/*")
        for client in clients:
            for mount_point in mount_points:
                client.exec_command(sudo=True, cmd=f"umount {mount_point}")
