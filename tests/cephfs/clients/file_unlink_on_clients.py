import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575042 - Test file unlink , stat , open concurrently

    Steps Performed:
    1. Create FS and mount on all the ways (Fuse, kernel, NFS)
    2. Create a file in mount and lock it.
    3. Try to unlink the file from kernel mount from other node
    4. file should be deleted
    Args:
        ceph_cluster:
        **kw:
    Exception : FCNTL locking is not working on nfs mounts
    Returns:
        0 --> if test PASS
        1 --> if test FAIL

    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )

        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        clients[0].upload_file(
            "tests/cephfs/clients/file_lock_utitlity.py",
            "/home/cephuser/file_lock_utility.py",
            sudo=True,
        )
        clients[1].upload_file(
            "tests/cephfs/clients/file_lock_utitlity.py",
            "/home/cephuser/file_lock_utility.py",
            sudo=True,
        )
        version, rc = clients[0].exec_command(
            sudo=True, cmd="ceph version --format json"
        )
        fs_util.prepare_clients([clients[0]], build)
        fs_util.auth_list([clients[0], clients[1]])

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(clients[0], fs_name)

        if not fs_details:
            fs_util.create_fs(clients[0], fs_name)
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount(
            [clients[0], clients[1]],
            fuse_mounting_dir,
            extra_params=f" --client_fs {fs_name}",
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0], clients[1]],
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        rc = unlink_file(
            clients[0],
            clients[1],
            "fuse_mount.txt",
            fuse_mounting_dir,
            validate_from=[kernel_mounting_dir],
        )

        if rc:
            raise CommandFailed("Unlink of the file is failing when file is locked")
        rc = unlink_file(
            clients[0],
            clients[1],
            "kernel_mount.txt",
            kernel_mounting_dir,
            validate_from=[fuse_mounting_dir],
        )
        if rc:
            raise CommandFailed("Unlink of the file is failing when file is locked")

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("---clean up---------")
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir
        )
        fs_util.client_clean_up(
            "umount",
            kernel_clients=[clients[0]],
            mounting_dir=kernel_mounting_dir,
        )


def unlink_file(client1, client2, file_name, lock_from_mount_dir, validate_from=[]):
    client1.exec_command(sudo=True, cmd=f"touch {lock_from_mount_dir}{file_name}")
    client1.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/file_lock_utility.py {lock_from_mount_dir}{file_name} lock "
        f"--timeout 120 &> /dev/null &",
        long_running=True,
    )
    out, rc = client2.exec_command(
        sudo=True,
        cmd=f"unlink {validate_from[0]}{file_name}",
    )
    out, rc = client1.exec_command(
        sudo=True, cmd=f"ls -lrt {lock_from_mount_dir}{file_name}", check_ec=False
    )
    log.info(out)
    log.info(rc)
    if rc == 0:
        raise CommandFailed(
            f"Unlink of {lock_from_mount_dir}{file_name} has not deleted it"
        )
