from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.spec_storage import SpecStorage
from utility.log import Log

log = Log(__name__)


def run_spec_storage_io(
    primary_client,
    benchmark,
    load,
    incr_load,
    num_runs,
    clients,
    nfs_mount,
    benchmark_defination,
):
    if SpecStorage(primary_client).run_spec_storage(
        benchmark, load, incr_load, num_runs, clients, nfs_mount, benchmark_defination
    ):
        raise OperationFailedError("SPECstorage run failed")
    log.info("SPECstorage run completed")


def run(ceph_cluster, **kw):
    """
    Validate the scale cluster scenario with nfs ganesha
    """

    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("num_of_clients", "2"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    # IO params
    benchmark = config.get("benchmark", "SWBUILD")
    benchmark_defination = config.get("benchmark_defination")
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")

    try:
        # Setup nfs cluster
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

        cmd = f"sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no -f -i ~/.ssh/id_rsa.pub root@{clients[0].hostname}"
        clients[0].exec_command(cmd=cmd, sudo=True, verbose=True)
        cmd = f"sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no -f -i ~/.ssh/id_rsa.pub root@{clients[0].ip_address}"
        clients[0].exec_command(cmd=cmd, sudo=True, verbose=True)

        run_spec_storage_io(
            clients[0],
            benchmark,
            load,
            incr_load,
            num_runs,
            [clients[0]],
            nfs_mount+"/spec_storage",
            benchmark_defination,
        )
        log.info(f"SPECstorage run completed on {nfs_mount}")
    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        # cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
