from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify setting selinux context label while mounting NFS share
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
    dirname = "Directory"
    num_files = 20
    num_dirs = 10

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
        # Remount the share on Client with selinux label
        cmd = f"umount -l {nfs_mount}"
        clients[0].exec_command(sudo=True, cmd=cmd)

        cmd = f"mount -t nfs -o context=system_u:object_r:nfs_t:s0 {nfs_nodes[0].ip_address}:{nfs_export}_0 {nfs_mount}"
        clients[0].exec_command(sudo=True, cmd=cmd)

        # Create the files on mount point
        for i in range(1, num_files + 1):
            cmd = f"touch {nfs_mount}/{filename}_{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)

        # Create the directories and file inside directories on mount point
        for i in range(1, num_dirs + 1):
            cmd = f"mkdir {nfs_mount}/{dirname}_{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)
            cmd = f"touch {nfs_mount}/{dirname}_{i}/dir{i}_{filename}"
            clients[0].exec_command(cmd=cmd, sudo=True)

        # Check the labels of the files on mount point. It should be same as mount point
        for i in range(1, num_files + 1):
            cmd = f"ls -Z {nfs_mount}/{filename}_{i}"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            if "nfs_t" in out[0]:
                log.info(
                    f"selinux lable is set correctly for file {filename}_{i} :  {out[0]}"
                )
            else:
                raise OperationFailedError(
                    "Selinux label is not the same as the NFS mount point"
                )

        # Check the labels of the files created inside directories. It should be same as mount point
        for i in range(1, num_dirs + 1):
            cmd = f"ls -Z {nfs_mount}/{dirname}_{i}/dir{i}_{filename}"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            if "nfs_t" in out[0]:
                log.info(
                    f"selinux lable is set correctly for the file dir{i}_{filename} :  {out[0]}"
                )
            else:
                raise OperationFailedError(
                    "Selinux label is not the same as the NFS mount point"
                )

    except Exception as e:
        log.error(
            f"Failed to verify the selinux label set on mount point NFS v4.2 : {e}"
        )
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
