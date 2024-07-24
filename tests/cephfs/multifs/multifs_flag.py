import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83573878   Verify the option to enable/disable multiFS support
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. check the enable_multiple flag value
    2. Get total number of filesystems present
    3. Disable enable_multiple if enabled and try creating filesystem
    4. Enable enable_multiple and try creating filesystem
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
        total_fs = fs_util.get_fs_details(client1)
        if len(total_fs) == 1:
            client1.exec_command(
                sudo=True, cmd="ceph fs flag set enable_multiple false"
            )
        out, rc = fs_util.create_fs(
            client1, vol_name="cephfs_new", check_ec=False, validate=False
        )

        if rc == 0:
            raise CommandFailed(
                "We are able to create multipe filesystems even after setting enable_multiple to false"
            )
        log.info(
            "We are not able to create multipe filesystems after setting enable_multiple to false as expected"
        )
        client1.exec_command(sudo=True, cmd="ceph fs flag set enable_multiple true")
        fs_util.create_fs(client1, vol_name="cephfs_new")

        log.info(
            "We are able to create multipe filesystems after setting enable_multiple to True as expected"
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
            client1.exec_command(sudo=True, cmd=command)
