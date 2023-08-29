from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
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

    # client addr export parameters
    nfs_export_client = "/export_client_addr"
    nfs_client_addr_mount = "/mnt/nfs_test_mount"

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
        )

        # Create export with client permission
        Ceph(clients).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_client,
            fs=fs_name,
            client_addr=clients[1].hostname,
        )

        # Mount the export on client which is unauthorized.Mount should pass
        clients[1].create_dirs(dir_path=nfs_client_addr_mount, sudo=True)
        if Mount(clients[1]).nfs(
            mount=nfs_client_addr_mount,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export_client,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        # Mount the export on client which is unauthorized.Mount should fail
        clients[0].create_dirs(dir_path=nfs_client_addr_mount, sudo=True)
        cmd = (
            f"mount -t nfs -o vers={version},port={port} "
            f"{nfs_server_name}:{nfs_export_client} {nfs_client_addr_mount}"
        )
        _, rc = clients[0].exec_command(cmd=cmd, sudo=True, check_ec=False)
        if "No such file or directory" in str(rc):
            log.info("Mount on unauthorized client failed with expected error")
            pass
        else:
            log.error(f"Mount passed on unauthorized client: {clients[0].hostname}")

    except Exception as e:
        log.error(f"Failed to perform export client addr validation : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        # Cleaning up the client addr export and mount dir
        log.info("Unmounting nfs-ganesha client addr mount on client:")
        for client in clients:
            if Unmount(client).unmount(nfs_client_addr_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            log.info("Removing nfs-ganesha client addr dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_client_addr_mount}")
            Ceph(client).nfs.export.delete(nfs_name, nfs_export_client)

        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
