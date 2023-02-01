import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. Create a subvolume name that does not exist
    2. Try to remove the subvolume name that does not exist
    3. The command should fail because the subvolume name does not exist.
    4. Check if the command is failed
    """
    try:
        tc = "CEPH-83574182"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        target_delete_subvolume = "non_exist_subvolume"
        c_out, c_err = fs_util.remove_subvolume(
            client1, "cephfs", target_delete_subvolume, validate=False, check_ec=False
        )
        if c_err:
            return 0
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
