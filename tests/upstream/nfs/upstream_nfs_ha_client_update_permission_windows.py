from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, perform_failover, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, remove_files
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def modify_export(client, nfs_name, nfs_export, client_ip):
    # Restrict the client access to single client
    out = Ceph(client).nfs.export.get(nfs_name, f"{nfs_export}_0")
    client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
    client.exec_command(
        sudo=True,
        cmd=(
            f'sed -i -e \'s/"access_type": "RW"/"access_type": "none"/\' '
            f'-e \'s/"clients": \\[\\]/"clients": [\\n'
            f"   {{\\n"
            f'      "access_type": "RO",\\n'
            f'      "addresses": [\\n'
            f'         "{client_ip}"\\n'
            f"      ],\\n"
            f'      "squash": "none"\\n'
            f"   }}\\n]/' export.conf"
        ),
    )
    Ceph(client).nfs.export.apply(nfs_name, "export.conf")


def run(ceph_cluster, **kw):
    """Verify HA with restricting 1 client access for windows client"""
    config = kw.get("config")
    # nfs cluster details
    nfs_nodes = ceph_cluster.get_nodes("installer")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]
    port = config.get("port", "2049")
    version = config.get("nfs_version", "3")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    window_nfs_mount = "Z:"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    port = "12049"

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")
    client = linux_clients[0]

    # Windows clients
    windows_clients = []
    for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
        windows_clients.append(windows_client_obj)
    window_client_ip_restricted = config.get("windows_clients")[1]["ip"]

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            linux_clients,
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

        # Fetch the VIP
        if "/" in vip:
            vip = vip.split("/")[0]

        # Mount NFS-Ganesha V3 to both window client
        for win_client in windows_clients:
            cmd = f"mount {vip}:/export_0 {window_nfs_mount}"
            win_client.exec_command(cmd=cmd)

        # Run IO's from windows client
        create_files(windows_clients[0], window_nfs_mount, 10, True)

        # Delete files
        remove_files(windows_clients[0], window_nfs_mount, 10, True)

        failover_node = nfs_nodes[0]

        # Unmount the export from 2nd client
        for win_client in windows_clients:
            cmd = f"umount {window_nfs_mount}"
            win_client.exec_command(cmd=cmd)

        operations = []

        # Perform node reboot operation
        th = Thread(
            target=perform_failover,
            args=(nfs_nodes, failover_node, vip),
        )
        operations.append(th)

        # Restrict the client access to single client
        th = Thread(
            target=modify_export,
            args=(client, nfs_name, nfs_export, window_client_ip_restricted),
        )
        operations.append(th)

        # Start the operations
        for op in operations:
            op.start()
            sleep(1)

        # Wait for the ops to complete
        for op in operations:
            op.join()
            sleep(3)

        # Mount the share on Client 2
        cmd = f"mount {vip}:/export_0 {window_nfs_mount}"
        windows_clients[1].exec_command(cmd=cmd)

        # Write on Client 2. It should be denied
        cmd = f"type nul > {window_nfs_mount}\\file_ro"
        out, rc = windows_clients[1].exec_command(cmd=cmd, check_ec=False)
        if "The media is write protected" in str(rc):
            log.info("Export is readonly")
        else:
            raise OperationFailedError("File created on Readonly export")

    except Exception as e:
        log.error(
            f"Failed to validate export delete with failover on a ha cluster: {e}"
        )
        # Cleanup
        cmd = f"umount {window_nfs_mount}"
        windows_clients[1].exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        # Cleanup
        log.info("Cleanup")
        cmd = f"umount {window_nfs_mount}"
        windows_clients[1].exec_command(cmd=cmd)
        cleanup_cluster(linux_clients, nfs_mount, nfs_name, nfs_export)
    return 0
