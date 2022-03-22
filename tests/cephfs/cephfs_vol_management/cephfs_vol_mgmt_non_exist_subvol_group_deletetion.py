import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create a subvolume group that does not exist
    Test operation:
    1. Try to delete the subvolume group
    2. Check if subvolume group deletion fails

    """
    try:

        tc = "CEPH-83574162"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        subvolume_group = "non_exist_subvolume_group_name"
        output, err = fs_util.remove_subvolumegroup(
            client1, "cephfs", subvolume_group, check_ec=False
        )
        if output == 0:
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
