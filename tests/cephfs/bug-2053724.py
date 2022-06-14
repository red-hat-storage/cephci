import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites:
    1. Create cephfs

    Test operation:
    1. Mount cephfs using kernel client
    2. Create a new temp file
    3. Write data to the temp file
    4. fsync() the temp file
    5. rename the temp file to the appropriate name
    6. fsync() the containing directory
    7. Verify time required for "Write file" operation is less than 150 milliseconds

    Cleanup:
    1. Remove all data in Cephfs
    2. Remove mount of cephfs
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_util.auth_list(clients)
        mon_node_ips = fs_util.get_mon_node_ips()
        kernel_mounting_dir = "/mnt/cephfs_kernel" + "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fs_util.kernel_mount(
            clients,
            kernel_mounting_dir,
            ",".join(mon_node_ips),
        )
        client1.exec_command(sudo=True, cmd="mkdir -p /tmp/test")
        client1.exec_command(
            sudo=True,
            cmd="git clone https://github.com/yogesh-mane/fsync_test /tmp/test",
        )
        client1.exec_command(sudo=True, cmd='yum group install "Development Tools" -y')
        client1.exec_command(
            sudo=True,
            cmd="g++ -Wall -Wextra -Os /tmp/test/fsynctest.cpp -o /tmp/test/fsynctest",
        )
        out, rc = client1.exec_command(
            sudo=True, cmd=f"/tmp/test/fsynctest {kernel_mounting_dir}/any_file 5"
        )
        log.info(f"{out}")
        output = out.splitlines()
        log.info(f"{output}")
        for out in output:
            line_out = out.split()
            time = line_out[4]
            log.info(f"{time}")
            t = int(time[:-2])
            log.info(f"time ={t}")
            if t > 150:
                raise Exception("Time required is greater than 150ms")
        log.info("Performance is appropriate")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir}/*")
        client1.exec_command(sudo=True, cmd=f"umount {kernel_mounting_dir}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mounting_dir}/")
        client1.exec_command(sudo=True, cmd="rm -rf /tmp/test")
