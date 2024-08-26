import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. Create cephfs subvolume with invalid data pool layout
    2. Check if cephfs subvolume is cleaned up
    3. verify trash dir is clean
    4. Check if number of files in the directory is equal to 0
    """
    try:
        tc = "CEPH-83574188"
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
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}"
        trash_dir = kernel_mounting_dir_1 + "/volumes/_deleting"
        subvol_name = "subvol_0"
        invalid_pool = "invalid_pool"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        out1, err1 = fs_util.create_subvolume(
            client1,
            f"{fs_name}",
            subvol_name,
            pool_layout=invalid_pool,
            validate=False,
            check_ec=False,
        )
        if out1 == 0:
            log.error("Subvolume should not be created because of the invalid pool")
            return 1
        out2, err2 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol_name}",
            check_ec=False,
        )
        if out2 == 0:
            log.error("Subvolume path should not be created")
            return 1
        out3, err3 = client1.exec_command(sudo=True, cmd=f"ls {trash_dir} | wc -l")
        if int(out3) != 0:
            log.error("Number of files in the directory should be 0")
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
