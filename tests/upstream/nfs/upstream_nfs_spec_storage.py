from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import OperationFailedError
from cli.io.spec_storage import SpecStorage
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")
    benchmark = config.get("benchmark", "SWBUILD")
    benchmark_defination = config.get("benchmark_defination")
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients_obj = ceph_cluster.get_nodes("client")
    clients = clients_obj[: len(config.get("clients", "clients1"))]
    primary_client = clients[0]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    try:
        log.info("Setup nfs cluster")
        setup_nfs_cluster(
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ceph_cluster=ceph_cluster,
        )
        cmd = "yum install -y sshpass"
        clients[0].exec_command(cmd=cmd, sudo=True, verbose=True)
        
        cmd = f"sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no -f -i ~/.ssh/id_rsa.pub root@{clients[0].hostname}"
        clients[0].exec_command(cmd=cmd, sudo=True, verbose=True)
        cmd = f"sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no -f -i ~/.ssh/id_rsa.pub root@{clients[0].ip_address}"
        clients[0].exec_command(cmd=cmd, sudo=True, verbose=True)

        log.info(f"Run SPECstorage with {benchmark} benchmark")
        if SpecStorage(primary_client).run_spec_storage(
            benchmark,
            load,
            incr_load,
            num_runs,
            [clients[0]],
            nfs_mount,
            benchmark_defination,
        ):
            raise OperationFailedError("SPECstorage run failed")
        log.info("SPECstorage run completed")
    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        log.info("Cleaning up skipped")
        # cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        # log.info("Cleaning up successfull")
    return 0
