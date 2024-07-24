import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573873   Try creating 2 Filesystem using same Pool(negative)
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Check if cephfs filesystem is present, if not create cephfs
    2. collect data pool and meta datapool info of cephfs
    3. try creating cephfs1 with data pool and meta datapool of cephfs
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
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_details = fs_util.get_fs_info(client1)
        out, rc = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs new cephfs1 {fs_details['metadata_pool_name']} {fs_details['data_pool_name']}",
            check_ec=False,
        )
        if rc == 0:
            raise CommandFailed(
                "We are able to create filesystems with same pool used by other filesystem"
            )
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
