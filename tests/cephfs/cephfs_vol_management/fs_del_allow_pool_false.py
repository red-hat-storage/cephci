import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83574159 - test fs volume deletion when mon_allow_pool_delete is false
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create new FS(cephfs_new)
    2. set the mon_allow_pool_delete flag value to False
    3. Del cephfs_new should error out
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
        fs_util.create_fs(client1, "cephfs_new")
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete false"
        )
        out, rc = fs_util.remove_fs(
            client1, "cephfs_new", validate=False, check_ec=False
        )
        if rc == 0:
            raise CommandFailed(
                "We are able to delete filesystems even after setting mon_allow_pool_delete to false"
            )
        log.info(
            "We are not able to delete filesystems even after setting mon_allow_pool_delete to false as expected"
        )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            "ceph fs volume rm cephfs_new --yes-i-really-mean-it",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
