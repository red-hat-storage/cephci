from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.rados_test_util import (
    create_pools,
    get_device_path,
    wait_for_device,
    write_to_pools,
)
from ceph.utils import get_node_by_id
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rados.test_9281 import do_rados_get
from utility.log import Log
from tests.cephfs.cephfs_utilsV1 import FsUtils
from ceph.parallel import parallel
from tests.io.fs_io import fs_io
from tests.cephfs.cephfs_system.mon_failure_with_client_IO import start_io_time

from utility.utils import method_should_succeed, should_not_be_empty

from utility.log import Log

log = Log(__name__)

def rm_osd_id(ceph_cluster, osd_id, rados_obj, host, dev_path):
    try:
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
        return dev_path, host, 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        raise CommandFailed("Removing osd failed")

def add_osd_id(ceph_cluster, osd_id, rados_obj,  host, dev_path):
    try:
        utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
        method_should_succeed(wait_for_device, host, osd_id, action="add")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        raise CommandFailed("Adding osd failed")

def run(ceph_cluster, **kw):
    try:
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
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir,
            mon_node_ip
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir
        )
        client1.exec_command(sudo=True, cmd=f"mkdir {kernel_mount_dir}/kernel_{{1..8}}")
        client1.exec_command(sudo=True, cmd=f"mkdir {fuse_mount_dir}/fuse_{{1..8}}")
        dev_paths = []
        hosts = []
        iteration = 0
        osd_list = ["0","1","2"]
        for osd_id in osd_list:
            #osd_id = osd_id[4:]
            log.info(f"Removing osd {osd_id}")
            host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            hosts.append(host)
            should_not_be_empty(host, "Failed to fetch host details")
            dev_path = get_device_path(host, osd_id)
            dev_paths.append(dev_path)
            with parallel() as p:
                p.spawn(start_io_time, fs_util, client1, kernel_mount_dir + f"/kernel_{iteration}")
                p.spawn(start_io_time, fs_util, client1, fuse_mount_dir + f"/fuse_{iteration}")
                p.spawn(rm_osd_id, ceph_cluster, osd_id, rados_obj, host, dev_path)
            iteration = iteration + 1
            client1.exec_command(sudo=True,cmd="ceph osd tree")
        iteration = 4
        id = 0
        for osd_id in osd_list:
            log.info(f"Adding osd {osd_id}")
            with parallel() as p:
                p.spawn(start_io_time, fs_util, client1, kernel_mount_dir + f"/kernel_{iteration}")
                p.spawn(start_io_time, fs_util, client1, fuse_mount_dir + f"/fuse_{iteration}")
                p.spawn(add_osd_id, ceph_cluster, osd_id, rados_obj, hosts[id], dev_paths[id])
            id = id + 1
            iteration = iteration + 1
            client1.exec_command(sudo=True,cmd="ceph osd tree")
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
~                                                                       
