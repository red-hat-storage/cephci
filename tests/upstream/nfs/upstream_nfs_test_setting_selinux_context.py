from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify selinux context label is preserved with the NFS mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
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
    filename = "Testfile"

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

        # Mount the share on Client 2
        cmd = f"umount -l {nfs_mount}"
        clients[1].exec_command(sudo=True, cmd=cmd)

        cmd = f"mount -t nfs {nfs_nodes[0].ip_address}:{nfs_export}_0 {nfs_mount}"
        clients[1].exec_command(sudo=True, cmd=cmd)

        # Create a file on Mount point from client 1
        cmd = f"touch {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Set the selinux context on the file from client 1
        chcon_cmd = f"chcon -t httpd_sys_content_t {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=chcon_cmd, sudo=True)

        # Verify the selinux label is set on the file from client 2
        cmd = f"ls -Z {nfs_mount}/{filename}"
        out = clients[1].exec_command(cmd=cmd, sudo=True)

        if "httpd_sys_content_t" in out[0]:
            log.info(f"selinux lable is set correctly: {out[0]}")
        else:
            raise OperationFailedError("Failed to set/get the selinux context")

    except Exception as e:
        log.error(f"Failed to set the selinux label on NFS v4.2 : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
