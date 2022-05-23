import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83574284 - Deploy MDS with default values using cephadm.
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 1 file systems with --placements
    2. Validate MDS has come up on specific hosts with the labels
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        mds_list = ceph_cluster.get_ceph_objects("mds")
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
        host_list = [mds.node.hostname for mds in mds_list]
        fs_name = "cephfs"
        for host in host_list:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host
            ):
                raise CommandFailed(f"Failed to start MDS on particular nodes {host}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
