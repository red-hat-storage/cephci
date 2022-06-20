import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def validate_mds_placements(fs_name, daemon_ls_after, host_list, daemon_count):
    i = 0
    for daemon in daemon_ls_after:
        if daemon.get("daemon_id").startswith(fs_name):
            i += 1
            if daemon.get("hostname") not in host_list:
                return 1
    if i != daemon_count:
        return 1
    return 0


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573870 - Create 2 Filesystem with default values on different MDS daemons
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 2 file systems with placement arguments
    2. validate the mds came on the specified placements
    3. mount both the file systems and using fuse mount
    4. Run IOs on the FS
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
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
        )
        daemon_ls_before = json.loads(out)
        daemon_count_before = len(daemon_ls_before)
        host_list = [
            client1.node.hostname.replace("node7", "node2"),
            client1.node.hostname.replace("node7", "node3"),
        ]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume create cephfs_df_fs --placement='2 {hosts}'",
            check_ec=False,
        )
        fs_util.wait_for_mds_process(client1, "cephfs_df_fs")
        out, rc = client1.exec_command(
            sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
        )
        daemon_ls_after = json.loads(out)
        daemon_count_after = len(daemon_ls_after)
        assert daemon_count_after > daemon_count_before, (
            f"daemon count is reduced after creating FS. "
            f"Expectation is MDS daemons whould be more"
            f"daemon count before: {daemon_count_before}"
            f"daemon count after: {daemon_count_after}"
        )

        total_fs = fs_util.get_fs_details(client1)
        if len(total_fs) < 2:
            log.error(
                "We can't proceed with the test case as we are not able to create 2 filesystems"
            )
        fs_names = [fs["name"] for fs in total_fs]
        validate_mds_placements("cephfs_df_fs", daemon_ls_after, hosts, 2)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"

        fs_util.fuse_mount(
            [clients[0]], fuse_mounting_dir_1, extra_params=f"--client_fs {fs_names[0]}"
        )
        fs_util.fuse_mount(
            [clients[0]], fuse_mounting_dir_2, extra_params=f"--client_fs {fs_names[1]}"
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
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        fs_util.remove_fs(client1, "cephfs_df_fs")
