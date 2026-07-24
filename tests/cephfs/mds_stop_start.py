import secrets
import string
import traceback
from datetime import datetime, timedelta
from time import sleep

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)
global stop_flag


def start_io_time(fs_util, client1, mounting_dir, timeout=300):
    global stop_flag
    stop_flag = False
    iter = 0
    if timeout:
        stop = datetime.now() + timedelta(seconds=timeout)
    else:
        stop = 0

    while not stop_flag:
        if stop and datetime.now() > stop:
            log.info("Timed out *************************")
            break
        client1.exec_command(sudo=True, cmd=f"mkdir -p {mounting_dir}/run_ios_{iter}")
        fs_util.run_ios(
            client1, f"{mounting_dir}/run_ios_{iter}", io_tools=["smallfile"]
        )
        iter = iter + 1
        log.info(f"IO stop flag {stop_flag}")


def mds_stop_start(fs_util, mds_nodes):
    global stop_flag
    for mds in mds_nodes:
        deamon_name = fs_util.deamon_name(mds, "mds")
        fs_util.deamon_op(mds, "mds", "stop")
        sleep(60)
        fs_util.deamon_op(mds, "mds", "start", service_name=deamon_name)
    stop_flag = True


def run(ceph_cluster, get_fs_info=None, **kw):
    try:
        """
        CEPH-83574339 - Test Start/Stop mds daemons
        Pre-requisites :
        1. Create cephfs volume

        Test Case Flow:
        1. Start IOs
        2. Stop mds daemon
        3. IO's shouldn't fail
        4. Start mds daemon
        5. IO's shouldn't fail

        Cleanup:
        1. Remove all the data in cephfs
        2. Remove all the mounts
        """
        tc = "CEPH-83574339"
        log.info("Running cephfs %s test case" % (tc))

        fs_util = FsUtils(ceph_cluster)
        client = ceph_cluster.get_ceph_objects("client")
        mdss = ceph_cluster.get_ceph_objects("mds")
        client1 = client[0]
        mount_dir = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        kernel_mount_dir = f"/mnt/kernel_{mount_dir}"
        fuse_mount_dir = f"/mnt/fuse_{mount_dir}"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount(
            [client1], kernel_mount_dir, mon_node_ip, new_client_hostname="admin"
        )
        fs_util.fuse_mount([client1], fuse_mount_dir, new_client_hostname="admin")
        global stop_flag
        with parallel() as p:
            p.spawn(mds_stop_start, fs_util, mdss)
            p.spawn(
                start_io_time,
                fs_util,
                client1,
                kernel_mount_dir + "/kernel_1",
                timeout=0,
            )
            p.spawn(
                start_io_time,
                fs_util,
                client1,
                fuse_mount_dir + "/fuse_1",
                timeout=0,
            )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mount_dir}/*")
        client1.exec_command(sudo=True, cmd=f"umount {kernel_mount_dir}")
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {kernel_mount_dir}/")
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}/")
