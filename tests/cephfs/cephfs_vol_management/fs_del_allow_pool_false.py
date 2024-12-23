import traceback

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
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
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
        cephfs_name = "cephfs_new"
        fs_util.create_fs(client1, cephfs_name)

        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete false"
        )
        rmvolume_cmd = f"ceph fs volume rm {cephfs_name} --yes-i-really-mean-it"

        try:
            client1.exec_command(sudo=True, cmd=rmvolume_cmd)
            log.error(
                "We are able to delete filesystems even after setting mon_allow_pool_delete to false"
            )
            return 1
        # Handling the error gracefully. Expected to fail
        except Exception as e:
            log.info(f"Exception: {rmvolume_cmd} is expected to fail")
            log.info(f"Error: {e}")

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        fs_util.remove_fs(client1, cephfs_name, validate=True, check_ec=False)
        return 0
