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
    1. Generate random name fo subvolume creation
    2. Create cephfs subvolume with name created in the first step
    3. Again create cephfs subvolume withg name created in first step with resize parameter
    4. Check if cephfs subvolume is resixed
    5. Remove cephfs subvolume
    6. Verify if trash directory is empty
    """
    try:

        tc = "CEPH-83574186"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        subvolume_name_generate = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvolume = {
            "vol_name": "cephfs",
            "subvol_name": f"subvol_{subvolume_name_generate}",
            "size": "5368706371",
        }
        subvolume_name = subvolume["subvol_name"]
        fs_util.create_subvolume(client1, **subvolume)
        new_size = "26843531685"
        c_out, c_err = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume resize cephfs {subvolume_name} {new_size}",
        )
        c_out_result = json.loads(c_out)
        target_size = c_out_result[1]["bytes_quota"]
        if int(target_size) != int(new_size):
            return 1
        c_out2, c_err2 = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume info cephfs {subvolume_name} -f json"
        )
        c_out2_result = json.loads(c_out2)
        target_quota = c_out2_result["bytes_quota"]
        if int(target_quota) != int(new_size):
            return 1
        fs_util.remove_subvolume(client1, "cephfs", subvolume_name, validate=True)
        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
