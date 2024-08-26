import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create a random subvolume_group name that does not exist
    Test operation:
    1. Try to create a subvolume on the subvolume_group name
    2. If the creation fails, memo it
    3. Try to delete a subvolume on the subvolume_group name
    4. If the deletion fails, memo it.
    5. If the creation and deletion are failed, return 0
    """
    try:
        tc = "CEPH-83574161"
        log.info(f"Running CephFS tests for BZ-{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        result = 0
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        subvolume_group = "non_exist_subvolume_group_name"
        subvolume_name_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvolume = {
            "vol_name": f"{fs_name}",
            "subvol_name": f"subvol_{subvolume_name_generate}",
            "size": "5368706371",
            "group_name": f"{subvolume_group}",
        }
        c_out, c_err = fs_util.create_subvolume(
            client1, **subvolume, validate=False, check_ec=False
        )
        if c_err:
            result = result + 1
        c_out2, c_err2 = fs_util.remove_subvolume(
            client1, **subvolume, validate=False, check_ec=False
        )
        if c_err2:
            result = result + 1
        if result != 2:
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
