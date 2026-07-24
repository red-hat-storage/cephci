import random
import string
import traceback

from cli.io.cbt.cbt import CBT
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
        osds = ceph_cluster.get_nodes("osd")
        osd_list = [osd.hostname for osd in osds]
        benchmarks = config.get("cbt_benchmarks")
        a = CBT(installer[0])
        a.collect_cluster_conf(mon[0].node.hostname, mon[0].node.ip_address, osd_list)
        a.prepare_cbt_conf()
        characters = string.ascii_letters + string.digits
        # Generate a random string
        random_string = "".join(random.choice(characters) for _ in range(8))
        a.execute_cbt(benchmarks, config.get("benchmark_name", random_string))

        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        pass
