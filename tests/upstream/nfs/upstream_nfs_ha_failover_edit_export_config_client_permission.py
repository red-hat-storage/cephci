from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from utility.log import Log

log = Log(__name__)


def update_export_conf(
    client, nfs_name, nfs_export_client, original_clients_value, new_clients_values
):
    try:
        out = Ceph(client).nfs.export.get(nfs_name, nfs_export_client)
        client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        client.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_clients_value}/{new_clients_values}/' export.conf",
        )
        Ceph(client).nfs.export.apply(nfs_name, "export.conf")
    except Exception:
        raise OperationFailedError("failed to edit clients in export conf file")


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    no_servers = int(config.get("servers", "2"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # If the setup doesn't have required number of nfs server, exit.
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

    # Export Conf Parameter
    nfs_export_client = "/export_client_access"
    nfs_client_mount = "/mnt/nfs_client_mount"
    original_clients_value = "client_address"
    new_clients_values = f"{clients[0].hostname}"

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
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_client,
            fs=fs_name,
            client_addr="client_address",
            installer=nfs_nodes[0]
        )

        # Edit the export config to mount with client 1 access value
        update_export_conf(
            clients[0],
            nfs_name,
            nfs_export_client,
            original_clients_value,
            new_clients_values,
        )

        # Perfrom and verify failover
        failover_node = nfs_nodes[0]
        perform_failover(nfs_nodes, failover_node, vip)

        # Mount the export on client1 which is unauthorized.Mount should fail
        clients[1].create_dirs(dir_path=nfs_client_mount, sudo=True)
        vip_ip = vip.split("/")[0]
        cmd = (
            f"mount -t nfs -o vers={version},port={port} "
            f"{vip_ip}:{nfs_export_client} {nfs_client_mount}"
        )
        _, rc = clients[1].exec_command(cmd=cmd, sudo=True, check_ec=False)
        if "No such file or directory" in str(rc):
            log.info("As expected, Mount on unauthorized client failed")
            pass
        else:
            log.error(f"Mount passed on unauthorized client: {clients[0].hostname}")

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        sleep(30)
        log.info("Unmounting nfs-ganesha client addr mount on client:")
        for client in clients:
            if Unmount(client).unmount(nfs_client_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            log.info("Removing nfs-ganesha client addr dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_client_mount}")
            Ceph(client).nfs.export.delete(nfs_name, nfs_export_client)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
