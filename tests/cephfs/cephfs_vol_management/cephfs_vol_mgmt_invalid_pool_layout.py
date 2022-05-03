import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. prepare invalid pool_name
    Test operation:
    1. Try to create a subvolume group with invalid pool
    2. Check if the subvolume group is not created beacuse of the invalid pool
    3. Using get_path, check if subvolumegroup path is cleaned up
    """
    try:
        tc = "CEPH-83574163"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        subvol_group_name = "subvol_group"
        invalid_pool_name = "non_exist_pool"
        out1, err1 = fs_util.create_subvolumegroup(
            client1,
            "cephfs",
            subvol_group_name,
            validate=False,
            pool_layout=invalid_pool_name,
            check_ec=False,
        )
        out2, err2 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath cephfs {subvol_group_name}",
            check_ec=False,
        )
        if out1 == 0 or out2 == 0:
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
