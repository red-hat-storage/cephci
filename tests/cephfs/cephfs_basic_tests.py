import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        if not build.startswith(("3", "4", "5")):
            if not fs_util.validate_fs_info(clients[0], "cephfs"):
                log.error("FS info Validation failed")
                return 1
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount(clients, fuse_mounting_dir)

        mount_test_case(clients, fuse_mounting_dir)

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(clients, kernel_mounting_dir, ",".join(mon_node_ips))

        mount_test_case(clients, kernel_mounting_dir)

        log.info("Cleaning up!-----")
        rc = fs_util.client_clean_up(
            [],
            clients,
            kernel_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("fuse clients cleanup failed")
        log.info("Fuse clients cleaned up successfully")

        rc = fs_util.client_clean_up(
            clients,
            [],
            fuse_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("kernel clients cleanup failed")
        log.info("kernel clients cleaned up successfully")
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def mount_test_case(clients, mounting_dir):
    tc1 = "11293"
    dir1 = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    dir2 = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    dir3 = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    results = []
    return_counts = []
    log.info("Create files and directories of 1000 depth and 1000 breadth")
    for client in clients:
        client.exec_command(
            sudo=True,
            cmd=f"mkdir -p {mounting_dir}{dir1} {mounting_dir}{dir2} {mounting_dir}{dir3}",
        )
        log.info(f"Execution of testcase {tc1} started")
        client.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 "
            f"--files 1000 --files-per-dir 10 --dirs-per-dir 2 --top "
            f"{mounting_dir}{dir1}",
            long_running=True,
        )
        client.exec_command(
            sudo=True, cmd=f"cp -r  {mounting_dir}{dir1}/* {mounting_dir}{dir2}/"
        )
        client.exec_command(
            sudo=True, cmd=f"diff -qr  {mounting_dir}{dir1} {mounting_dir}{dir2}/"
        )
        client.exec_command(
            sudo=True, cmd=f"mv  -t {mounting_dir}{dir1}/* {mounting_dir}{dir2}/"
        )
        for client in clients:
            if client.pkg_type != "deb":
                client.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}.txt bs=100M "
                    "count=5",
                )
                out1, err = client.exec_command(
                    sudo=True,
                    cmd=f" ls -c -ltd -- {mounting_dir}{client.node.hostname}.*",
                )
                client.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}.txt bs=200M "
                    "count=5",
                )
                out2, err = client.exec_command(
                    sudo=True,
                    cmd=f" ls -c -ltd -- {mounting_dir}{client.node.hostname}.*",
                )
                if out1 != out2:
                    return_counts.append(0)
                    return_counts.append(0)
                else:
                    raise CommandFailed("Metadata info command failed")
                break
        log.info(return_counts)
        rc_set = set(return_counts)
        if len(rc_set) == 1:
            results.append(f"TC {tc1} passed")
        break
    return 0
