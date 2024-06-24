import traceback

from cli.io.cbt import CBT
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        installer = ceph_cluster.get_ceph_objects("installer")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon = ceph_cluster.get_ceph_objects("mon")
        osds = ceph_cluster.get_ceph_objects("osds")
        osd_list = [osd.node.hostname for osd in osds]
        benchmarks = """benchmarks:
  librbdfio:
    time: 300
    vol_size: 16384
    mode: [write]
    op_size: [4194304]
    concurrent_procs: [1]
    iodepth: [1]
    osd_ra: [4096]
    cmd_path: 'fio'
    pool_profile: 'rbd'
"""
        a = CBT(installer[0])
        a.setup_CBT()
        a.collect_cluster_conf(mon[0].node.hostname, mon[0].node.ip_address, osd_list)
        a.prepare_cbt_conf()
        a.execute_cbt(benchmarks, "fio")

        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        pass
