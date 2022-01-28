import secrets
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.subvolume_authorize import verify_mount_failure_on_root
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites:
    1. Create 2 cephfs volume
       creats fs volume create <vol_name>

    Test operation:
    1. Create client1 restricted to first cephfs
       ceph fs authorize <fs_name> client.<client_id> <path-in-cephfs> rw
    2. Create client2 restricted to second cephfs
    3. Mount first cephfs with client1
    4. Verify mounting second cephfs with client1 fails
    5. Mount second cephfs with client2
    6. Verify mounting first cephfs with client2 fails

    Clean-up:
    1. Remove all the cephfs mounts
    2. Remove all the created clients
    """
    try:
        tc = "CEPH-83573869"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        mount_points = []
        fs1 = "cephfs"
        fs2 = "cephfs-ec"
        log.info(f"Creating client authorized to {fs1}")
        fs_util.fs_client_authorize(client1, fs1, "client1", "/", "rw")
        log.info(f"Creating client authorized to {fs2}")
        fs_util.fs_client_authorize(client1, fs2, "client2", "/", "rw")
        kernel_mount_dir = "/mnt/kernel" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/fuse" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(f"Mounting {fs1} with client1")
        fs_util.kernel_mount(
            clients,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="client1",
            extra_params=f",fs={fs1}",
        )
        fs_util.fuse_mount(
            clients,
            fuse_mount_dir,
            new_client_hostname="client1",
            extra_params=f" --client_fs {fs1}",
        )
        log.info(f"Verifying mount failure for client1 for {fs2}")
        rc = verify_mount_failure_on_root(
            fs_util,
            clients,
            kernel_mount_dir + "_dir1",
            fuse_mount_dir + "_dir1",
            "client1",
            mon_node_ip,
            fs_name=f"{fs2}",
        )
        if rc == 1:
            log.error(f"Mount success on {fs2} with client1")
            return 1
        kernel_mount_dir = "/mnt/kernel" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/fuse" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(f"Mounting {fs2} with client2")
        fs_util.kernel_mount(
            clients,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="client2",
            extra_params=f",fs={fs2}",
        )
        fs_util.fuse_mount(
            clients,
            fuse_mount_dir,
            new_client_hostname="client2",
            extra_params=f" --client_fs {fs2}",
        )
        rc = verify_mount_failure_on_root(
            fs_util,
            clients,
            kernel_mount_dir + "_dir2",
            fuse_mount_dir + "_dir2",
            "client2",
            mon_node_ip,
            fs_name=f"{fs1}",
        )
        log.info(f"Verifying mount failure for client2 for {fs1}")
        if rc == 1:
            log.error(f"Mount success on {fs1} with client2")
            return 1
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        for client in clients:
            for mount_point in mount_points:
                client.exec_command(sudo=True, cmd=f"umount {mount_point}")
        for num in range(1, 4):
            client1.exec_command(sudo=True, cmd=f"ceph auth rm client.client{num}")
