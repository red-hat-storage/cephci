import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Prepare isolated_namespace name
    Test operation:
    1. Create a subvolume with isolated_namespace option
    2. Check if the creation is successful
    3. After the creation, check if the subvolume is created in isolated namespace using `ceph fs subvolume info`
    4. Remove the subvolume
    """
    try:
        tc = "CEPH-83574187"
        log.info(f"Running CephFS tests for Polarion ID -{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        random_name = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvol_name = "subvol_name_" + random_name
        namespace = "namespace_" + random_name
        fs_util.create_subvolume(
            client1, f"{fs_name}", subvol_name, namespace_isolated=namespace
        )
        out1, err1 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume info {fs_name} {subvol_name}  --format json",
        )
        isolated_pool_name = f"fsvolumens_{subvol_name}"
        output1 = json.loads(out1)
        target_ns_name = output1["pool_namespace"]
        if target_ns_name != isolated_pool_name:
            log.error("Isolated namespace name are not identical")
            return 1
        fs_util.remove_subvolume(client1, f"{fs_name}", subvol_name)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
