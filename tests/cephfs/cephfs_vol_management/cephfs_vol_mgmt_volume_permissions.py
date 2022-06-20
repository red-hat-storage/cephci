import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create cephfs subvolumegroup
    2. Create cephfs subvolume in subvolume group with default permission
    3. Create cephfs subvolumes in subvolumegroup with different permission
    Test operation:
    1. Get path of all created subvolumes
    2. Remove all the subvolumes
    3. Remove the subvolumegroup
    """
    try:
        tc = "CEPH-83574190"
        log.info(f"Running CephFS tests for Polarion ID -{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        vol_name_rand = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        subvol_group = f"subvolume_groupname_{vol_name_rand}"
        subvol_default = f"subvol_default_{vol_name_rand}"
        subvol_different = f"subvol_different_{vol_name_rand}"
        fs_util.create_subvolumegroup(client1, "cephfs", subvol_group)
        fs_util.create_subvolume(
            client1, "cephfs", subvol_default, group_name=subvol_group
        )
        fs_util.create_subvolume(
            client1, "cephfs", subvol_different, group_name=subvol_group, mode=777
        )
        out1 = fs_util.get_subvolume_info(
            client1, "cephfs", subvol_default, group_name=subvol_group
        )
        out2 = fs_util.get_subvolume_info(
            client1, "cephfs", subvol_different, group_name=subvol_group
        )
        out3, err3 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath cephfs {subvol_default} --group_name {subvol_group}",
        )
        out4, err4 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath cephfs {subvol_different} --group_name {subvol_group}",
        )
        mode1 = out1["mode"]
        mode2 = out2["mode"]
        if str(mode1) != "16877" or str(mode2) != "16895" or out3 == 1 or out4 == 1:
            return 1
        fs_util.remove_subvolume(
            client1, "cephfs", subvol_default, group_name=subvol_group
        )
        fs_util.remove_subvolume(
            client1, "cephfs", subvol_different, group_name=subvol_group
        )
        fs_util.remove_subvolumegroup(client1, "cephfs", subvol_group, force=True)
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
