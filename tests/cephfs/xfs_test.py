import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.lib.xfs_lib.xfs_utils import XfsTestSetup
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Running XFS tests on the client node and run the tests

Steps to Reproduce:

1. Install required packages for xfstests
2. Mount the kernel clients
3. Run the xfstests on the clients
4. Verify the results of the tests
"""


def run(ceph_cluster, **kw):
    try:
        log.info("Running CephFS tests for ceph kernel xfstests")
        fs_util = FsUtils(ceph_cluster)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
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
        fs_name = "cephfs"
        if not fs_details:
            fs_util.create_fs(client1, fs_name)

        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client1, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1

        failure_count = {"kernel": 0, "fuse": 0}
        for mount_type in ["kernel"]:
            xfs_test = XfsTestSetup(ceph_cluster, client1)
            if xfs_test.setup_environment():
                log.error("Failed to set up the environment for XFS tests")
                return 1

            if xfs_test.clone_and_build_xfstests():
                log.error("Failed to clone and build xfstests")
                return 1

            rand = "".join(
                random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
            )
            test_mount_point = f"/mnt/cephfs_{mount_type}_{rand}_test"
            scratch_mount_point = f"/mnt/cephfs_{mount_type}_{rand}_scratch"

            if mount_type == "kernel":
                mount_info = {
                    "test_mount": test_mount_point,
                    "scratch_mount": scratch_mount_point,
                    "mount_type": mount_type,
                    "FSTYP": "ceph",
                    "fs_name": fs_name,
                    "test_dev": f"{rand}_a",
                    "scratch_dev": f"{rand}_b",
                }
            elif mount_type == "fuse":
                mount_info = {
                    "test_mount": test_mount_point,
                    "scratch_mount": scratch_mount_point,
                    "mount_type": mount_type,
                    "FSTYP": "ceph-fuse",
                    "fs_name": fs_name,
                    "test_dev": "ceph-fuse",
                    "scratch_dev": "ceph-fuse",
                }

            if xfs_test.mount_fs(mount_info):
                log.error("Failed to mount the CephFS filesystem")
                return 1

            if xfs_test.configure_local_config(mount_info):
                log.error("Failed to configure local.config for xfstests")
                return 1

            len_failed_tc = xfs_test.run_tests(mount_type)
            failure_count[mount_type] = len_failed_tc

            log.info("XFS tests completed successfully")

            if xfs_test.cleanup(mount_info):
                log.error("Failed to clean up the XFS test environment")
                return 1

        log.info("Verify Cluster is healthy after test")
        if cephfs_common_utils.wait_for_healthy_ceph(client1, 600):
            log.error("Cluster health is not OK even after waiting for 600 secs")
            return 1

        log.info("Reporting the failure")
        if failure_count["kernel"] > 0 or failure_count["fuse"] > 0:
            log.error("XFS tests failed for mounts")
            log.error("Kernel/Fuse failures: {}".format(failure_count))
            return 1
        else:
            log.info("XFS tests passed for all clients")

        return 0

    except Exception as e:
        dmesg, _ = client1.exec_command(sudo=True, cmd="dmesg")
        log.error(dmesg)
        log.error(e)
        log.error(traceback.format_exc())
        if xfs_test.cleanup(mount_info):
            log.error("Failed to clean up the XFS test environment")
            return 1
        return 1
