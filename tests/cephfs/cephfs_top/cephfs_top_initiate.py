import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Verify cephfs-top initial status and basic configuration

Steps to Reproduce:
1. Try to test install cephfs-top package
2. Check if cephfs-top packages install is working well
3. Check if cephfs-top is working without enabling mgr stats
4. check if cephfs-top client is created well with appropriate permission
5. check if cephfs-top -h,--help is working well
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83573829"
        log.info(f"Running CephFS tests for ceph {tc}")
        # Initialize the utility class for CephFS
        fs_util = FsUtils(ceph_cluster)
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))
        # install "cephfs-top" by "dnf install cephfs-top"
        log.info("Install cephfs-top by dnf install cephfs-top")
        client1.exec_command(
            sudo=True,
            cmd="dnf install cephfs-top -y",
        )
        out, rc = client1.exec_command(
            sudo=True,
            cmd="dnf list cephfs-top",
        )
        if "cephfs-top" not in out:
            log.error("cephfs-top package could not be installed")
            return 1
        log.info("Create cephfs-top client user and verify")
        client_user = "client.fstop"
        cmd = f"ceph auth get-or-create {client_user} mon 'allow r' mds 'allow r' osd 'allow r' mgr 'allow r'"
        cmd += " > /etc/ceph/ceph.client.fstop.keyring"
        client1.exec_command(
            sudo=True,
            cmd=cmd,
        )
        out, _ = client1.exec_command(
            sudo=True,
            cmd="ceph auth ls | grep fstop",
        )
        if client_user not in out:
            log.error(f"{client_user} user not created")
            return 1
        log.info("run cephfs-top before enabling stats")
        # check if it is not working
        out1, ec1 = client1.exec_command(
            sudo=True,
            cmd="cephfs-top",
            check_ec=False,
        )
        log.info(out1)
        log.info(ec1)
        if "module not enabled" not in ec1:
            log.error("cephfs-top is working before enabling stats")
            return 1
        log.info("Enable stats in mgr module")
        client1.exec_command(
            sudo=True,
            cmd="ceph mgr module enable stats",
        )
        # run cephfs-top --id <username>
        log.info("run cephfs-top --id <client_name>")
        out2, ec2 = client1.exec_command(
            sudo=True,
            cmd="cephfs-top --id fstop",
            check_ec=False,
        )
        log.info(out2)
        log.info(ec2)
        if "error" in ec2:
            log.error("cephfs-top is not working")
            return 1
        # check cephfs-top manual
        out3, ec3 = client1.exec_command(sudo=True, cmd="cephfs-top -h")
        if "usage" not in out3:
            log.error("cephfs-top -h is not working")
        out4, ec4 = client1.exec_command(sudo=True, cmd="cephfs-top --help")
        if "usage" not in out4:
            log.error("cephfs-top --help is not working")
        log.info("Checking if cephfs-top uninstall is successful")
        client1.exec_command(
            sudo=True,
            cmd="dnf remove cephfs-top -y",
        )
        out5, rc = client1.exec_command(
            sudo=True,
            cmd="dnf list installed cephfs-top",
            check_ec=False,
        )
        log.info(out5)
        log.info(rc)
        if "cephfs-top" in out5:
            log.error("cephfs-top package could not be removed")
            return 1
        log.info("cephfs-top package removed successfully")
        log.info("Run cephfs-top without installing cephfs-top")
        out6, ec6 = client1.exec_command(
            sudo=True,
            cmd="cephfs-top",
            check_ec=False,
        )
        if "command not found" not in ec6:
            log.error("cephfs-top is working without installing cephfs-top")
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("disable stats in mgr module")
        client1.exec_command(
            sudo=True,
            cmd="ceph mgr module disable stats",
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        log.info("Remove the client user")
        client1.exec_command(
            sudo=True,
            cmd="ceph auth del client.fstop",
        )
