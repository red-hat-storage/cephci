import traceback
from time import sleep

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
            client_count = config.get("cephfs").get("num_of_clients", 1)
            if client_count > len(clients):
                log.error(
                    f"NFS clients required to perform test is {client_count} but "
                    f"conf file has only {len(clients)}"
                )
                return 1
            for client in clients[:client_count]:
                fs_io_obj = fs_io(
                    client=client, fs_config=config.get("cephfs"), fs_util=fs_util
                )
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
        if config.get("wait_for_io"):
            while True:
                out, rc = clients[0].exec_command(sudo=True, cmd="podman ps -q")
                log.info(out)
                if not out:
                    break
                sleep(30)
            out, rc = clients[0].exec_command(sudo=True, cmd="podman ps -a -q")
            contiainter_ids = out.strip().split("\n")
            for container_id in contiainter_ids:
                out, rc = clients[0].exec_command(
                    sudo=True, cmd=f"podman rm {container_id}"
                )
                if out:
                    print(out)
                else:
                    print(f"Container {container_id} removed")
        out, rc = clients[0].exec_command(sudo=True, cmd="ceph df")
        log.info(out)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
