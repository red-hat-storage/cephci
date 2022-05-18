import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):

    """
    pre-requisites:
    1. Create a volume with a name
    2. Create a subvolume with a name
    3. Create a subvolume group with a name
    Test operation:
    1. Try to create a volume with the same name
    2. Try to create a subvolume with the same name
    3. Try to create a subvolume group with the same name
    """
    try:
        tc = "CEPH-83573428"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)

        random_name = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        volume_name = "vol_01" + random_name
        subvolume_name = "subvol_01" + random_name
        subvolume_group_name = "subvol_group_name_01" + random_name
        log.info("Ceph Build number is " + build[0])
        fs_util.create_fs(client1, volume_name)
        fs_util.create_subvolume(client1, volume_name, subvolume_name)
        fs_util.create_subvolumegroup(client1, "cephfs", subvolume_group_name)
        output1, err1 = fs_util.create_fs(client1, volume_name, check_ec=False)
        output2, err2 = fs_util.create_subvolume(
            client1, volume_name, subvolume_name, check_ec=False
        )
        output3, err3 = fs_util.create_subvolumegroup(
            client1, volume_name, subvolume_name, check_ec=False
        )
        if output1 == 0 or output2 == 0 or output3 == 0:
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
