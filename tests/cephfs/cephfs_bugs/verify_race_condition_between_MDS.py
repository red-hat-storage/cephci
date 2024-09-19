import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83572726"
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
        client2 = clients[1]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        fs_util.auth_list([client2])

        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")
        client2.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")
        log.info("Setting the max mds to 2")
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 2")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        log.info("fuse mount on client1")
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {fs_name}",
        )

        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"
        log.info("fuse mount on client2")

        fs_util.fuse_mount(
            [client2],
            fuse_mounting_dir_2,
            extra_params=f" --client_fs {fs_name}",
        )
        for _ in range(3):
            dir_name1 = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(2))
            )
            dir_name2 = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(2))
            )
            client1_cmd = (
                f"mkdir -p {fuse_mounting_dir_1}{dir_name1} &&"
                f" setfattr -n ceph.dir.pin -v 1 {fuse_mounting_dir_1}{dir_name1}"
            )
            client2_cmd = (
                f"mkdir -p {fuse_mounting_dir_2}{dir_name2} &&"
                f" setfattr -n ceph.dir.pin -v 2 {fuse_mounting_dir_2}{dir_name2}"
            )
            set_race_cmd = (
                "ceph config set client mds_inject_migrator_session_race true"
            )
            client1.exec_command(sudo=True, cmd=client1_cmd)
            client1.exec_command(sudo=True, cmd=set_race_cmd)
            client2.exec_command(sudo=True, cmd=client2_cmd)
            time.sleep(5)
            client2.exec_command(
                sudo=True,
                cmd="ceph config set client mds_inject_migrator_session_race false",
            )
            client2.exec_command(sudo=True, cmd=f"ls {fuse_mounting_dir_2}{dir_name1}")
            time.sleep(120)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_2
        )
