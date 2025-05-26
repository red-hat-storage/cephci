from upstream_nfs_operations import cleanup_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount
from utility.log import Log

log = Log(__name__)


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
    fs = "cephfs"
    nfs_server = nfs_node.hostname

    try:
        # Setup nfs cluster
        # Step 1: Enable nfs
        Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)

        # Step 2: Create an NFS cluster
        Ceph(clients[0]).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server)

        # Step 3: Create Export
        for client in clients:
            Ceph(client).nfs.export.create(
                fs_name=fs_name,
                nfs_name=nfs_name,
                nfs_export=nfs_export,
                fs=fs,
                squash="rootsquash",
                installer=nfs_nodes[0]
            )

        # Step 4: Perform nfs mount
        for client in clients:
            client.create_dirs(dir_path=nfs_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_mount,
                version=version,
                port=port,
                server=nfs_server,
                export=nfs_export,
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
        log.info("Mount succeeded on all clients")

        # Try to change the permission of mount dir
        try:
            clients[0].exec_command(sudo=True, cmd=f"chmod 777 {nfs_mount}/")
            log.error(
                "Unexpected: Directory permission change succeeded with rootsquash enabled"
            )
            return 1
        except Exception as e:
            log.info(
                f"Expected. Failed to change mount point permission when rootsquash enabled. {e}"
            )

        # Try to create a file on mount dir
        try:
            cmd = f"dd if=/dev/urandom of={nfs_mount}/sample.txt bs=1G count=1"
            clients[0].exec_command(cmd=cmd, sudo=True)
            log.error("Unexpected: File creation succeeded with rootsquash enabled")
            return 1
        except Exception as e:
            log.info(f"Expected. Failed to create file when rootsquash enabled. {e}")

    except Exception as e:
        log.error(f"Failed to validate read dir operations : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
