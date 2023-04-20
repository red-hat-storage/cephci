import secrets
import string
import traceback
from time import sleep

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.io.fs_io import fs_io
from utility.log import Log
from utility.utils import get_storage_stats

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Automation - T3 - CEPH-11328 - Fill cluster to 85 - 95% and check MDS accepts IO to perform delete of data

    Steps Performed :
    1. Fill the cluster till 95 %
    2. Delete the contents of the cluster
    """
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        io_func = []
        cluster_stats = get_storage_stats(client=clients[0])
        cluster_used_percent = (
            cluster_stats["total_used_bytes"] / float(cluster_stats["total_bytes"])
        ) * 100
        cluster_to_fill = config.get("cephfs").get("fill_data") - cluster_used_percent
        if cluster_to_fill <= 0:
            log.info(
                f"cluster is already filled with {config.get('cephfs').get('fill_data')}"
            )
            fuse_mount_dir = "/mnt/fuse_" + "".join(
                secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
            )

            fs_util_v1.fuse_mount(
                [clients[0]],
                fuse_mount_dir,
                new_client_hostname="admin",
                extra_params=" --client_fs cephfs",
            )
            clients[0].exec_command(sudo=True, cmd=f"rm -rf {fuse_mount_dir}/*")
        else:
            # config.get("cephfs")["fill_data"] = cluster_to_fill
            if config.get("cephfs"):
                fs_util_v1.prepare_clients(clients, build)
                fs_util_v1.auth_list(clients)
                fs_io_obj = fs_io(
                    client=clients[0],
                    fs_config=config.get("cephfs"),
                    fs_util=fs_util_v1,
                )
                io_func.append(fs_io_obj.run_fs_io)
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
            out, rc = clients[0].exec_command(
                sudo=True,
                cmd=f"ls -lrt /mnt/ | grep {fs_io_obj.mounting_dir} |  awk {{'print $9'}} ",
            )

            clients[0].exec_command(sudo=True, cmd=f"rm -rf /mnt/{out.strip()}/*")
            cluster_stats_after = get_storage_stats(client=clients[0])
            cluster_used_percent_after_del = (
                cluster_stats_after["total_used_bytes"]
                / float(cluster_stats_after["total_bytes"])
            ) * 100
            if cluster_used_percent_after_del <= cluster_to_fill:
                log.info(
                    f"Cluster is allowing to delete even after filling  {config.get('cephfs').get('fill_data')}%"
                )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
