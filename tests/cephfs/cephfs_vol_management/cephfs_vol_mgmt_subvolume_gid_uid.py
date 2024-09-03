import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. Create a subvolume
    2. Check info for the subvolume
    3. Check if gid and uid are set to 0
    """
    try:
        tc = "CEPH-83574181"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        subvolume_name_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvolume = {
            "vol_name": f"{fs_name}",
            "subvol_name": f"subvol_{subvolume_name_generate}",
            "size": "5368706371",
        }
        fs_util.create_subvolume(client1, **subvolume, check_ec=False)
        c_out, c_err = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume info {fs_name} subvol_{subvolume_name_generate}",
        )
        c_out_decoded = json.loads(c_out)
        gid = c_out_decoded["gid"]
        uid = c_out_decoded["uid"]
        if gid != 0 or uid != 0:
            return 1
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
