from time import sleep, time

from nfs_operations import cleanup_cluster, open_mandatory_v3_ports, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def _nfs_rpc_state(nfs_node):
    """Return whether nfs is registered, and the current TCP mountd port."""
    out, _ = nfs_node.exec_command(sudo=True, cmd="sudo rpcinfo -p", check_ec=False)
    nfs_up = False
    mountd_port = None
    for line in (out or "").splitlines():
        columns = line.split()
        if len(columns) < 5:
            continue
        proto, port, service = columns[2], columns[3], columns[4]
        if service == "nfs":
            nfs_up = True
        elif service == "mountd" and (mountd_port is None or proto == "tcp"):
            mountd_port = port
    return nfs_up, mountd_port


def _lazy_unmount(client, mountpoint):
    client.exec_command(cmd="umount -l %s" % mountpoint, sudo=True, check_ec=False)


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
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]

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
    nfs_export_client = "/export_client_access"
    nfs_client_mount = "/mnt/nfs_client_mount"
    original_clients_value = "client_address"
    # Ganesha matches the client ACL against the TCP source address.
    new_clients_values = f"{clients[0].ip_address}"

    window_nfs_mount = "Z:"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")

    # Windows clients
    windows_clients = []
    is_windows = config.get("windows_clients", None)
    if is_windows:
        for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
            windows_clients.append(windows_client_obj)
        if windows_clients:
            new_clients_values = f"{windows_clients[0].ip_address}"

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
            ha,
            vip,
            ceph_cluster=ceph_cluster,
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )

        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_client,
            fs=fs_name,
            client_addr="client_address",
        )

        # Edit the export config to mount with client 1 access value
        update_export_conf(
            clients[0],
            nfs_name,
            nfs_export_client,
            original_clients_value,
            new_clients_values,
        )

        nfs_version = None
        if version:
            # Check if version contains "3" (e.g., "3", "3.0", or list containing 3)
            if isinstance(version, (list, tuple)):
                nfs_version = 3 if 3 in version else None
            elif isinstance(version, (int, str)):
                # Convert to string and check if it starts with "3"
                version_str = str(version)
                if version_str.startswith("3") or version_str == "3":
                    nfs_version = 3

        if is_windows:
            sleep(10)
            if nfs_version == 3:
                open_mandatory_v3_ports(nfs_node, ["portmapper", "mountd"])
            clients[1].create_dirs(dir_path=nfs_client_mount, sudo=True)
            cmd = "mount -t nfs -o vers=%s,port=%s %s:%s %s" % (
                version,
                port,
                nfs_server_name[0],
                nfs_export_client,
                nfs_client_mount,
            )
            out, err = clients[1].exec_command(cmd=cmd, sudo=True, check_ec=False)
            result = "%s\n%s" % (out or "", err or "")
            unauthorized = clients[1].hostname
            if "access denied by server" in result:
                log.info(
                    "As expected, mount on unauthorized client %s failed", unauthorized
                )
            elif clients[1].exit_status == 0:
                raise OperationFailedError(
                    "Mount passed on unauthorized client: %s" % unauthorized
                )
            else:
                raise OperationFailedError(
                    "Unauthorized client %s mount failed unexpectedly: %s"
                    % (unauthorized, result.strip())
                )
            sleep(15)
            cmd = "mount %s:/export_1 %s" % (nfs_nodes[0].ip_address, window_nfs_mount)
            out = windows_clients[0].exec_command(cmd=cmd)
            if "is now successfully connected" not in out[0]:
                raise OperationFailedError(
                    "Failed to mount nfs on %s" % clients[0].ip_address
                )
            sleep(3)
            log.info(
                "TEST PASSED - windows nfs_version=%s export=%s mounted on %s",
                version,
                nfs_export_client,
                clients[0].hostname,
            )
        else:
            # Export apply restarts Ganesha. Mount the authorized client first
            # so the export is loaded, then probe the unauthorized client once.
            # Re-open the NFSv3 firewall ports when mountd moves to a new port.
            # retry=0 keeps a closed mountd port from blocking for 2 minutes.
            # NFSv4 reports a hidden export as "No such file or directory".
            mount_server = nfs_server_name[0]
            if ha:
                mount_server = vip.split("/")[0]
            clients[1].create_dirs(dir_path=nfs_client_mount, sudo=True)
            clients[0].create_dirs(dir_path=nfs_client_mount, sudo=True)
            unauth_cmd = "mount -t nfs -o vers=%s,port=%s,retry=0 %s:%s %s" % (
                version,
                port,
                mount_server,
                nfs_export_client,
                nfs_client_mount,
            )
            unauthorized = clients[1].hostname
            deadline = time() + 180
            opened_mountd_port = None
            authorized_ok = False
            while time() < deadline:
                nfs_up, mountd_port = _nfs_rpc_state(nfs_node)
                if not nfs_up or (nfs_version == 3 and not mountd_port):
                    log.info(
                        "Polling NFS %s until rpcinfo shows the server",
                        nfs_node.hostname,
                    )
                    sleep(2)
                    continue
                # Export apply restarts Ganesha and mountd takes a new port.
                # Open that port once, then again only when the port changes.
                if nfs_version == 3 and mountd_port != opened_mountd_port:
                    try:
                        open_mandatory_v3_ports(nfs_node, ["portmapper", "mountd"])
                    except OperationFailedError as port_err:
                        log.info(
                            "NFSv3 ports are not ready on %s: %s",
                            nfs_node.hostname,
                            port_err,
                        )
                        sleep(2)
                        continue
                    opened_mountd_port = mountd_port
                    sleep(2)
                    continue
                try:
                    Mount(clients[0]).nfs(
                        mount=nfs_client_mount,
                        version=version,
                        port=port,
                        server=mount_server,
                        export=nfs_export_client,
                        extra_mount_options="retry=0",
                        timeout=120,
                    )
                except Exception as mount_err:
                    log.info(
                        "Authorized mount of %s is not ready: %s",
                        nfs_export_client,
                        mount_err,
                    )
                    _lazy_unmount(clients[0], nfs_client_mount)
                    sleep(5)
                    continue
                authorized_ok = True
                break
            if not authorized_ok:
                raise OperationFailedError(
                    "Authorized client %s did not mount %s within 180s"
                    % (clients[0].hostname, nfs_export_client)
                )
            out, err = clients[1].exec_command(
                cmd=unauth_cmd, sudo=True, check_ec=False, timeout=120
            )
            last_unauth = ("%s\n%s" % (out or "", err or "")).strip().lower()
            unauth_status = clients[1].exit_status
            _lazy_unmount(clients[1], nfs_client_mount)
            if unauth_status == 0:
                raise OperationFailedError(
                    "Mount passed on unauthorized client: %s" % unauthorized
                )
            if (
                "access denied by server" not in last_unauth
                and "no such file or directory" not in last_unauth
            ):
                raise OperationFailedError(
                    "Unauthorized client %s failed unexpectedly: %s"
                    % (unauthorized, last_unauth)
                )
            log.info(
                "As expected, mount on unauthorized client %s failed", unauthorized
            )
            log.info(
                "TEST PASSED - nfs_version=%s export=%s: "
                "unauthorized client %s denied, authorized client %s mounted",
                version,
                nfs_export_client,
                clients[1].hostname,
                clients[0].hostname,
            )
        return 0
    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        if is_windows:
            for windows_client in windows_clients:
                cmd = f"del /q /f {window_nfs_mount}\\*.*"
                windows_client.exec_command(cmd=cmd)
                cmd = f"umount {window_nfs_mount}"
                windows_client.exec_command(cmd=cmd)

        # Cleaning up the client export and mount dir
        for client in clients[:2]:
            if Unmount(client).unmount(nfs_client_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_client_mount}")
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_client)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=servers)
        log.info("Cleaning up successfull")
