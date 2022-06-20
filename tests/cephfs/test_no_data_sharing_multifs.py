import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites:
    1. Create 2 cephfs volume
       creats fs volume create <vol_name>

    Test operation:
    1. Mount both cephfs with same client
    2. Remove all data from both cephfs
    3. Run IO's on first cephfs
    4. Verify second cephfs has not data
    5. Copy first cephfs data to local directory
    6. Run IO's on second cephfs
    7. Verify data consistency of first cephfs using local directory
    8. Copy second cephfs data to another local directory
    9. Run IO's on first cephfs
    10. Verify data consistency of second cephfs using local directory

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Remove all the cephfs mounts
    """
    try:
        tc = "CEPH-83573876"
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
        fs1 = "cephfs"
        log.info(f"mounting {fs1}")
        fs1_kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.kernel_mount(
            clients,
            fs1_kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs1}",
        )
        fs1_fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        mount_points.extend([fs1_kernel_mount_dir, fs1_fuse_mount_dir])
        fs_util.fuse_mount(
            clients,
            fs1_fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs1}",
        )
        fs2 = "cephfs-ec"
        log.info(f"mounting {fs2}")
        fs2_kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.kernel_mount(
            clients,
            fs2_kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs2}",
        )
        fs2_fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        mount_points.extend([fs2_kernel_mount_dir, fs2_fuse_mount_dir])
        fs_util.fuse_mount(
            clients,
            fs2_fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs2}",
        )
        log.info(f"Remove all data in both {fs1} & {fs2}")
        commands = [
            f"rm -rf {fs1_kernel_mount_dir}/*",
            f"rm -rf {fs2_kernel_mount_dir}/*",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info(f"Run IO's on {fs1}")
        commands = [
            f"mkdir {fs1_kernel_mount_dir}/dir1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {fs1_kernel_mount_dir}/dir1",
            f"dd if=/dev/urandom of={fs1_kernel_mount_dir}/file1 bs=4M count=1000; done",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info(f"Copy {fs1} data to local directory")
        commands = [
            "mkdir /home/cephuser/cephfs_backup",
            f"cp {fs1_kernel_mount_dir}/* -R /home/cephuser/cephfs_backup/",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info(f"Verifying {fs2} has no data after IO's on {fs1}")
        output, rc = client1.exec_command(sudo=True, cmd=f"ls {fs2_kernel_mount_dir}")
        if "" == output:
            log.info(f"{fs2} has no data")
        else:
            if "file1" in output or "dir1" in output:
                log.error("Data is being shared accross file systems")
                return 1
            else:
                log.error("Directory is not empty")
                return 1
        log.info(f"Run IO's on {fs2}")
        commands = [
            f"mkdir {fs2_kernel_mount_dir}/dir2",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {fs2_kernel_mount_dir}/dir2",
            f"dd if=/dev/urandom of={fs2_kernel_mount_dir}/file2 bs=1M count=1000; done",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info(f"Copy {fs2} data to another local directory")
        commands = [
            "mkdir /home/cephuser/cephfs_ec_backup",
            f"cp {fs2_kernel_mount_dir}/* -R /home/cephuser/cephfs_ec_backup/",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        log.info(f"Verifying data consistency in {fs1} after IO's on {fs2}")
        command = f"diff -qr {fs1_kernel_mount_dir} /home/cephuser/cephfs_backup/"
        rc = client1.exec_command(sudo=True, cmd=command, long_running=True)
        if rc == 0:
            log.info(f"Data is consistent in {fs1}")
        else:
            log.error(f"Data is inconsistent in {fs1}")
            return 1
        log.info(f"Run IO's on {fs1}")
        commands = [
            f"mkdir {fs1_kernel_mount_dir}/dir3",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --top {fs1_kernel_mount_dir}/dir3",
            f"dd if=/dev/urandom of={fs1_kernel_mount_dir}/file3 bs=8M count=1000; done",
        ]
        log.info(f"Verifying data consistency in {fs2} after IO's on {fs1}")
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        command = f"diff -qr {fs2_kernel_mount_dir} /home/cephuser/cephfs_ec_backup/"
        rc = client1.exec_command(sudo=True, cmd=command, long_running=True)
        if rc == 0:
            log.info(f"Data is consistent in {fs2}")
        else:
            log.error(f"Data is inconsistent in {fs2}")
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
        client1.exec_command(
            sudo=True, cmd="rm -rf /home/cephuser/*backup/", check_ec=False
        )
        for client in clients:
            for mount_point in mount_points:
                client.exec_command(
                    sudo=True, cmd=f"umount {mount_point}", check_ec=False
                )
