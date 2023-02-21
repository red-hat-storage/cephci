import traceback

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados import utils
from ceph.rados.core_workflows import RadosOrchestrator
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from tests.rados.rados_test_util import get_device_path, wait_for_device
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def add_rm_osd(ceph_cluster, osd_id, rados_obj):
    """
    CEPH_11258 - OSD service add & removal test with clientIO
    Pre-requisites :
    1. Create cephfs volume
       creats fs volume create <vol_name>
    Operations:
    1. Start Client IO
    2. Remove osd
    3. IOs should continue
    4. Add osd
    5. IOs should continue
    """
    try:
        host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        should_not_be_empty(host, "Failed to fetch host details")
        dev_path = get_device_path(host, osd_id)
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

        utils.add_osd(ceph_cluster, host.hostname, dev_path, osd_id)
        method_should_succeed(wait_for_device, host, osd_id, action="add")
        method_should_succeed(wait_for_clean_pg_sets, rados_obj)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1


def run(ceph_cluster, **kw):
    try:
        config = kw["config"]
        fs_util = FsUtils(ceph_cluster)
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        clients = ceph_cluster.get_ceph_objects("client")
        cephfs = {
            "fill_data": 20,
            "io_tool": "smallfile",
            "mount": "fuse",
            "filesystem": "cephfs",
            "mount_dir": "/mnt/mycephfs1",
        }
        fs_io_obj = fs_io(client=clients[0], fs_config=cephfs, fs_util=fs_util)
        with parallel() as p:
            p.spawn(add_rm_osd, ceph_cluster, "1", rados_obj)
            p.spawn(fs_io_obj.run_fs_io)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
