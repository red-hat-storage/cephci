import re
import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def get_result_file(out):
    match = re.search(r"(/[\w/\-]+\.csv)", out)

    if match:
        file_path = match.group(1)
        print(file_path)
        return file_path
    else:
        print("File path not found in the string.")


def run(ceph_cluster, **kw):
    """
    CEPH-10560 - CephFS performance between Fuse and kernel client.

    Test Steps:
    1. Mount Fuse and Kernel mounts
    2. Run IO on fuse mount using samllfile
    3. Run Io on Kernel Mount using smallfile
    4. Display the response times for both

    Test config:
    config :
        files : 10
        file_size: 1024
        threads: 8
    Args:
        ceph_cluster:
        **kw:

    Returns:

    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        fs_name = "cephfs"

        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        mon_node_ip = fs_util_v1.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        kernel_mount_dir = "/mnt/kernel_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util_v1.kernel_mount(
            [clients[0]],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="admin",
            extra_params=f",fs={fs_name}",
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util_v1.fuse_mount(
            [clients[0]],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        clients[0].exec_command(sudo=True, cmd=f"mkdir -p {fuse_mount_dir}/fuse_ios")
        files = config.get("files", 10)
        file_size = config.get("file_size", 1024)
        threads = config.get("threads", 10)
        clients[0].exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
            f"--threads {threads} --file-size {file_size} --files {files} --response-times Y --top "
            f"{fuse_mount_dir}/fuse_ios",
            long_running=True,
        )
        clients[0].exec_command(
            sudo=True, cmd=f"mkdir -p {kernel_mount_dir}/kernel_ios"
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
            f"--threads {threads} --file-size {file_size} --files {files} --response-times Y --top "
            f"{kernel_mount_dir}/kernel_ios",
            long_running=True,
        )

        out, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_rsptimes_stats.py "
            f"{kernel_mount_dir}/kernel_ios/network_shared",
        )
        fuse_report = get_result_file(out.strip().split("\n")[1])
        out, rc = clients[0].exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_rsptimes_stats.py "
            f"{fuse_mount_dir}/fuse_ios/network_shared",
        )
        kernel_report = get_result_file(out.strip().split("\n")[1])

        out_fuse, rc = clients[0].exec_command(sudo=True, cmd=f"cat {fuse_report}")
        out_kernel, rc = clients[0].exec_command(sudo=True, cmd=f"cat {kernel_report}")
        log.info("FUSE Mount Respones times ")
        log.info("------------------------------------------")
        log.info(out_fuse)
        log.info("------------------------------------------")

        log.info("Kernel Mount Respones times ")
        log.info("------------------------------------------")
        log.info(out_kernel)
        log.info("------------------------------------------")
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        fs_util_v1.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mount_dir
        )
        fs_util_v1.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mount_dir,
        )
