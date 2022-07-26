import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573871   Explore ceph-fuse mount of more than 2 Filesystem on same client.
                    Also verify persistent mounts upon reboots.
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 2 file systems if not present
    2. mount both the file systems and using fuse mount and fstab entry
    3. reboot the node
    4. validate if the mount points are still present
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        client1.exec_command(
            sudo=True, cmd="ceph fs volume create cephfs_new", check_ec=False
        )
        if not fs_util.wait_for_mds_process(client1, "cephfs_new"):
            raise CommandFailed("Failed to start MDS deamons")
        total_fs = fs_util.get_fs_details(client1)
        if len(total_fs) < 2:
            log.error(
                "We can't proceed with the test case as we are not able to create 2 filesystems"
            )

        fs_names = [fs["name"] for fs in total_fs]
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"

        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f"--client_fs {fs_names[0]}",
            fstab=True,
        )
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_2,
            extra_params=f"--client_fs {fs_names[1]}",
            fstab=True,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_1}",
            long_running=True,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_2}",
            long_running=True,
        )
        fs_util.reboot_node(client1)
        out, rc = client1.exec_command(cmd="mount")
        mount_output = out.split()
        log.info("validate fuse mount:")
        assert fuse_mounting_dir_1.rstrip("/") in mount_output, "fuse mount failed"
        assert fuse_mounting_dir_2.rstrip("/") in mount_output, "fuse mount failed"
        client1.exec_command(
            sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}/io_after_reboot"
        )
        client1.exec_command(
            sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_2}/io_after_reboot"
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_1}/io_after_reboot",
            long_running=True,
        )
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400 "
            f"--files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
            f"{fuse_mounting_dir_2}/io_after_reboot",
            long_running=True,
        )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_2
        )
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs volume rm cephfs_new --yes-i-really-mean-it",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        client1.exec_command(
            sudo=True, cmd="mv /etc/fstab.backup /etc/fstab", check_ec=False
        )
