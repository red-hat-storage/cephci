import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    pre-requisites:
    1. Create a pool with "-" name
    Test operation:
    1. Create a cephfs
    2. Add the cephfs to the pool name with "-"
    3. Create a subvolume with in the cephfs attatched to the pool
    4. Run IOs
    5. Check if any failure happens during the operation

    """
    try:
        tc = "CEPH-83573528"
        log.info(f"Running CephFS tests for BZ-{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
        )
        pool_names = ["ceph-fs-pool"]

        for pool_name in pool_names:
            client1.exec_command(f"ceph osd pool create {pool_name}")
            output, err = client1.exec_command(
                f"ceph fs add_data_pool cephfs {pool_name}"
            )
            if output == 1:
                return 1
            subvol_name = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            fs_util.create_subvolume(client1, "cephfs", f"subvol_{subvol_name}")
            run_ios(client1, kernel_mounting_dir_1)
            fs_util.remove_subvolume(client1, "cephfs", f"subvol_{subvol_name}")

        return 0
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def run_ios(client, mounting_dir):
    def smallfile():
        client.exec_command(
            sudo=True,
            cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
            f"create symlink stat chmod ls-l delete cleanup  ; "
            f"do python3 /home/cephuser/smallfile/smallfile_cli.py --operation $i --threads 8 --file-size 10240 "
            f"--files 10 --top {mounting_dir} ; done",
        )

    def dd():
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/zero of={mounting_dir}{client.node.hostname}_dd bs=100M "
            f"count=5",
        )

    io_tools = [dd, smallfile]
    f = random.choice(io_tools)
    f()
