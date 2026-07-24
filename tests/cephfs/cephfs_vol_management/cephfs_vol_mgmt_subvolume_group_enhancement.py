import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1. create a group with name test_group with non-existent volume -> should not be created
    2. create a subvolumegroup -> should be created
    3. get subvolumegroup info without any subvolume -> should return 0
    4. create a subvolume in the subvolumegroup -> should be created
    5. get subvolume info for the created subvolume -> should return 0
    6. Check subvolumes in the volume with exist -> if exists, "subvolume exists"
    7. remove the subvolume in the subvolumegroup -> should be removed
    8. check subvolumes in the volume with exist -> if not exists, "no subvolume exists"
    9. remove the subvolumegroup with subvolume -> should not be removed
    10. remove the subvolume in the subvolumegroup -> should be removed
    11. remove the subvolumegroup -> should be removed
    12. remove the non-existing subvolumegroup --force -> should show that the subvolumegroup does not exist
    13. get subvolume path
    14. change subvolume uid and gid (chown 2001:3001 sub_1/)
    15. check if permission has changed
    16. change subvolume mode (chmod 700 sub_1/)
    17. check if permission has changed
    """
    try:
        tc = "CEPH-83602913"
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
        vol_name_rand = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )

        mount = f"/mnt/mount_{vol_name_rand}"
        fs_util.fuse_mount([client1], mount)
        subvol_group = f"subvolume_groupname_{vol_name_rand}"
        subvol_default = f"subvol_default_{vol_name_rand}"
        log.info("create a subvolume group name with non-existent volume")
        out1, ec1 = client1.exec_command(
            sudo=True,
            cmd="ceph fs subvolumegroup create non_exist_volume nonexistentvolume --group_name testgroup",
            check_ec=False,
        )
        log.info(out1)
        log.info(ec1)
        if "not found" not in ec1:
            log.error("subvolumegroup created with non-existent volume")
            log.error(ec1)
            return 1
        # create a subvolumegroup
        fs_util.create_subvolumegroup(client1, f"{fs_name}", subvol_group)
        # get subvolumegroup info without any subvolume
        out2, ec2 = client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolumegroup info {fs_name} {subvol_group}"
        )
        log.info(out2)  # print
        if ec2 == 0:
            log.error("subvolumegroup info without any subvolume failed")
            log.error(ec2)
            return 1
        # create a subvolume in the subvolumegroup
        fs_util.create_subvolume(
            client1, f"{fs_name}", subvol_default, group_name=subvol_group
        )
        # get subvolume info for the created subvolume
        out3, ec3 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume info {fs_name} {subvol_default} --group_name {subvol_group}",
        )
        log.info(out3)  # print
        log.info(ec3)
        if ec3 != 0:
            log.error("subvolume info for the created subvolume failed")
            log.error(ec3)
        log.info("subvolume exists functional testing")
        log.info("Check subvolumes in the volume with exist")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume create {fs_name} {subvol_default} --group_name {subvol_group}",
        )
        out4, ec4 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume exist {fs_name} --group_name {subvol_group}",
        )
        if "no subvolume exists" in out4:
            log.error("subvolume does not exist")
            log.error(out4)
            log.error(ec4)
            return 1
        # remove the subvolume in the volume
        fs_util.remove_subvolume(
            client1, f"{fs_name}", subvol_default, group_name=subvol_group
        )
        out5, ec5 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume exist {fs_name} --group_name {subvol_group}",
        )
        if "no subvolume exists" not in out5:
            log.error("subvolume exists")
            log.error(out5)
            log.error(ec5)
            return 1
        log.info("remove the subvolumegroup with subvolume")
        # create a subvolume
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume create {fs_name} {subvol_default} --group_name {subvol_group}",
        )
        out6, ec6 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {fs_name} {subvol_group}",
            check_ec=False,
        )
        log.info(ec6)
        if "contains subvolume" not in ec6:
            log.error("subvolumegroup still contains subvolume")
            log.error(ec6)
            return 1
        log.info("remove the subvolume in the subvolumegroup")
        fs_util.remove_subvolume(
            client1, f"{fs_name}", subvol_default, group_name=subvol_group
        )
        log.info("remove the subvolumegroup")
        out7, ec7 = fs_util.remove_subvolumegroup(
            client1, f"{fs_name}", subvol_group, force=True
        )
        if ec7 == 0:
            log.error("subvolumegroup not removed")
            log.error(out7)
            log.error(ec7)
            return 1
        # getting subvolume path
        log.info("get subvolume path")
        # create a subvolume
        client1.exec_command(
            sudo=True, cmd=f"ceph fs subvolume create {fs_name} {subvol_default}"
        )
        # get the subvolume uid and gid
        out8, ec8 = client1.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol_default}",
        )
        log.info("change subvolume uid and gid")
        client1.exec_command(sudo=True, cmd=f"chown 2001:3001 {mount}{out8}")
        # check if permission has changed using stat
        stat_subvolume, ec_stat = get_stat(client1, f"{mount}{out8}")
        log.info(stat_subvolume)
        if "2001" not in stat_subvolume["Uid"] or "3001" not in stat_subvolume["Gid"]:
            log.error("permission has not changed")
            log.error(stat_subvolume)
            log.error(ec_stat)
            return 1
        log.info("permission has changed")
        log.info(stat_subvolume)
        client1.exec_command(sudo=True, cmd=f"chmod 777 {mount}{out8}")
        # check if permission has changed using stat
        stat_subvolume2, ec_stat = get_stat(client1, f"{mount}{out8}")
        log.info(stat_subvolume2["Octal_Permission"])
        if "777" not in stat_subvolume2["Octal_Permission"]:
            log.error("permission has not changed")
            log.error(stat_subvolume2)
            log.error(ec_stat)
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # unmount cephfs
        fs_util.client_clean_up("umount", fuse_clients=[client1], mounting_dir=mount)


def get_stat(client, path):
    path.replace("\n", "")
    path = path.rstrip("\n")
    out, ec = client.exec_command(
        sudo=True, cmd=f"stat {path} --printf=%n,%a,%A,%b,%w,%x,%u,%g,%s,%h"
    )
    key_list = [
        "File",
        "Octal_Permission",
        "Permission",
        "Blocks",
        "Birth",
        "Access",
        "Uid",
        "Gid",
        "Size",
        "Links",
    ]
    output_dic = dict(zip(key_list, out.split(",")))
    return output_dic, ec
