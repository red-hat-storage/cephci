import secrets
import string
import traceback
from datetime import datetime, timedelta

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import add_osd
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.rados.rados_test_util import get_device_path, wait_for_device
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

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


def rm_osd_id(ceph_cluster, osd_id, rados_obj, host, dev_path):
    try:
        global stop_flag
        log.debug(
            f"osd1 device path  : {dev_path}, osd_id : {osd_id}, host.hostname : {host.hostname}"
        )
        utils.set_osd_devices_unmanaged(ceph_cluster, osd_id, unmanaged=True)
        method_should_succeed(utils.set_osd_out, ceph_cluster, osd_id)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        utils.osd_remove(ceph_cluster, osd_id)
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        method_should_succeed(utils.zap_device, ceph_cluster, host.hostname, dev_path)
        method_should_succeed(wait_for_device, host, osd_id, action="remove")
        stop_flag = True
        return dev_path, host, 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        raise CommandFailed("Removing osd failed")


def add_osd_id(ceph_cluster, osd_id, rados_obj, host, dev_path):
    try:
        global stop_flag
        add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
        method_should_succeed(wait_for_device, host, osd_id, action="add")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        stop_flag = True
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        raise CommandFailed("Adding osd failed")


def run(ceph_cluster, **kw):
    try:
        """
        CEPH-11258 - OSD node/service add and removal test, with client IO
        Pre-requisites :
        1. Create cephfs volume

        Test Case Flow:
        1. Start IOs
        2. Start removing osd from each osd node
        3. IO's shouldn't fail
        4. Start adding back osd to each osd node
        5. IO's shouldn't fail

        Cleanup:
        1. Remove all the data in cephfs
        2. Remove all the mounts
        """
        config = kw["config"]
        fs_util = FsUtils(ceph_cluster)
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        mount_dir = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        kernel_mount_dir = f"/mnt/kernel_{mount_dir}"
        fuse_mount_dir = f"/mnt/fuse_{mount_dir}"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount([client1], kernel_mount_dir, mon_node_ip)
        fs_util.fuse_mount([client1], fuse_mount_dir)
        client1.exec_command(sudo=True, cmd=f"mkdir {kernel_mount_dir}/kernel_{{1..8}}")
        client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mount_dir}/fuse_{{1..8}}")
        dev_paths = []
        hosts = []
        iteration = 0
        osd_list = ["0", "1", "2"]
        for osd_id in osd_list:
            log.info(f"Removing osd {osd_id}")
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            hosts.append(host)
            should_not_be_empty(host, "Failed to fetch host details")
            dev_path = get_device_path(host, osd_id)
            dev_paths.append(dev_path)
            global stop_flag
            with parallel() as p:
                p.spawn(
                    start_io_time,
                    fs_util,
                    client1,
                    kernel_mount_dir + f"/kernel_{iteration}",
                    timeout=0,
                )
                p.spawn(
                    start_io_time,
                    fs_util,
                    client1,
                    fuse_mount_dir + f"/fuse_{iteration}",
                    timeout=0,
                )
                p.spawn(rm_osd_id, ceph_cluster, osd_id, rados_obj, host, dev_path)
            iteration = iteration + 1
            client1.exec_command(sudo=True, cmd="ceph osd tree")
        iteration = 4
        id = 0
        for osd_id in osd_list:
            log.info(f"Adding osd {osd_id}")
            global stop_flag
            with parallel() as p:
                p.spawn(
                    start_io_time,
                    fs_util,
                    client1,
                    kernel_mount_dir + f"/kernel_{iteration}",
                    timeout=0,
                )
                p.spawn(
                    start_io_time,
                    fs_util,
                    client1,
                    fuse_mount_dir + f"/fuse_{iteration}",
                    timeout=0,
                )
                p.spawn(
                    add_osd_id,
                    ceph_cluster,
                    osd_id,
                    rados_obj,
                    hosts[id],
                    dev_paths[id],
                )
            id = id + 1
            iteration = iteration + 1
            client1.exec_command(sudo=True, cmd="ceph osd tree")
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
