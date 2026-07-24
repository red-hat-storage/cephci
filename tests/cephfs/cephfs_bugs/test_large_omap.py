import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575630 - Bug 2164338 - Large Omap objects found in pool 'ocs-storagecluster-cephfilesystem-metadata'

    Steps Performed:
    1. Create a new filesystem
    2. Mount the filesystem on a client
    3. Set mds_bal_split_size to 1000
    4. Touch 11000 file and create sanpshot
    5. Initiate scrub on the filesystem
    4. Check the ceph health for large omaps
    5. Cleanup

    Returns:

    """
    try:
        tc = "CEPH-83575630"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_large_omap"
        fs_util.create_fs(client1, fs_name)
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        default_mds_split_size, rc = client1.exec_command(
            sudo=True, cmd="ceph config get mds mds_bal_split_size"
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mds mds_bal_split_size 1000"
        )
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mount_dir}/dir_test")
        for i in range(1, 10):
            log.info(f"Iteration : {i}")
            client1.exec_command(
                sudo=True, cmd=f"touch {fuse_mount_dir}/dir_test/file{{0..11000}}"
            )
            client1.exec_command(
                sudo=True, cmd=f"mkdir {fuse_mount_dir}/dir_test/.snap/snap_{i}"
            )
            client1.exec_command(
                sudo=True, cmd=f"ceph tell mds.{fs_name}:0 scrub start / recursive"
            )
            while True:
                out, rc = client1.exec_command(
                    sudo=True, cmd=f"ceph tell mds.{fs_name}:0 scrub status -f json"
                )
                scrubs = json.loads(out)
                if not scrubs["scrubs"]:
                    break
                time.sleep(60)
            out, rc = client1.exec_command(sudo=True, cmd="ceph health detail")
            if "LARGE_OMAP_OBJECTS" in out:
                raise CommandFailed(
                    "LARGE_OMAP_OBJECTS are getting created which is not expected"
                )
            client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}/dir_test/*")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_bal_split_size {default_mds_split_size}",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False)
        client1.exec_command(
            sudo=True,
            cmd="ceph config set mon mon_allow_pool_delete true",
            check_ec=False,
        )
        fs_util.remove_fs(client1, fs_name)
