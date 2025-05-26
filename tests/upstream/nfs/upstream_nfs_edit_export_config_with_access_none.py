from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def update_export_conf(
    client, nfs_name, nfs_export, original_access_type, new_access_type
):
    try:
        out = Ceph(client).nfs.export.get(nfs_name, nfs_export)
        client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        client.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_access_type}/{new_access_type}/g' export.conf",
        )
        Ceph(client).nfs.export.apply(nfs_name, "export.conf")
    except Exception:
        raise OperationFailedError(
            "failed to update access type in export conf file as NONE"
        )


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
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    nfs_server_name = nfs_node.hostname

    # Export Conf Parameter
    nfs_export_access_none = "/exportNone"
    original_access_type = '"access_type": "RW"'
    new_access_type = '"access_type": "NONE"'

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
            fs_name,
            ceph_cluster=ceph_cluster,
        )
        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_access_none,
            fs=fs_name,
            installer=nfs_nodes[0]
        )

        # Edit the export config to mount with access_type 'RO'
        update_export_conf(
            clients[0],
            nfs_name,
            nfs_export_access_none,
            original_access_type,
            new_access_type,
        )

        # Verify the update was successful
        out = Ceph(clients[0]).nfs.export.get(nfs_name, nfs_export_access_none)
        if '"access_type": "NONE"' not in out:
            raise OperationFailedError(
                "The update of export with access_type as NONE failed. Export conf not updated"
            )

        log.info("Export update succeeded with access_type updated as NONE")
    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        sleep(30)
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_access_none)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
