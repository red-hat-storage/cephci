import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from tests.io.rbd_io import rbd_io
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        io_func = []
        if config.get("cephfs"):
            fs_util = FsUtils(ceph_cluster)
            fs_util.prepare_clients(clients, build)
            fs_util.auth_list(clients)
            fs_io_obj = fs_io(client=clients[0], config=config.get("cephfs"))
            io_func.append(fs_io_obj.run_fs_io)
        if config.get("rbd"):
            rbd_io_obj = rbd_io(
                client=clients[0],
                config=config.get("rbd"),
                cluster_config=config,
                ceph_nodes=ceph_cluster,
            )
            io_func.append(rbd_io_obj.run_rbd_io)
        with parallel() as p:
            for io in io_func:
                p.spawn(io)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
