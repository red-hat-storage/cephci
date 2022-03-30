import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573867 - Create 4-5 Filesystem randomly on different MDS daemons

    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 5 file systems with default values
    2. Validate the mds counts and the file systems counts
    3. mount all the file systems and using fuse mount
    4. Run IOs on the FSs
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
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        for i in range(1, 5):
            out, rc = client1.exec_command(
                sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
            )
            daemon_ls_before = json.loads(out)
            daemon_count_before = len(daemon_ls_before)
            client1.exec_command(
                sudo=True,
                cmd=f"ceph fs volume create cephfs_{i}",
                check_ec=False,
            )
            fs_util.wait_for_mds_process(client1, f"cephfs_{i}")
            out_after, rc = client1.exec_command(
                sudo=True, cmd="ceph orch ps --daemon_type mds -f json"
            )
            daemon_ls_after = json.loads(out_after)
            daemon_count_after = len(daemon_ls_after)
            assert daemon_count_after > daemon_count_before, (
                f"daemon count is reduced after creating FS. Demons count before : {daemon_count_before} ;"
                f"after:{daemon_count_after}"
                "Expectation is MDS daemons whould be more"
            )
            fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}_{i}/"
            fs_util.fuse_mount(
                [clients[0]], fuse_mounting_dir, extra_params=f"--client_fs cephfs_{i}"
            )
            client1.exec_command(
                sudo=True,
                cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 400"
                f" --files 100 --files-per-dir 10 --dirs-per-dir 1 --top "
                f"{fuse_mounting_dir}",
                long_running=True,
            )
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    finally:
        for i in range(1, 5):
            fs_util.client_clean_up(
                "umount",
                fuse_clients=[clients[0]],
                mounting_dir=f"/mnt/cephfs_fuse{mounting_dir}_{i}/",
            )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        [fs_util.remove_fs(client1, f"cephfs_{i}") for i in range(1, 5)]
