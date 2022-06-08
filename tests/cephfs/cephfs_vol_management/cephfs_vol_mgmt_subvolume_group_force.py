import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):

    """
    Test operation:
    1. Create a subvolume_group name that does not exist
    2. Try to remove the subvolume_group name that does not exist with force option
    3. The command should fail because the subvolume_group name does not exist.
    4. Check if the command is failed
    """
    try:

        tc = "CEPH-83574169"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        target_delete_subvolume_group = "non_exist_subvolume_group_name"
        c_out, c_err = fs_util.remove_subvolumegroup(
            client1,
            "cephfs",
            target_delete_subvolume_group,
            force=True,
            validate=False,
            check_ec=False,
        )
        if c_err:
            log.error(
                "The Subvolumegroup does not exist --force option should make the command succeed"
            )
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
