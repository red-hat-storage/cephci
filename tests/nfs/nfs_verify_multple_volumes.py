from random import choice

from nfs_operations import cleanup_cluster

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
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    no_volumes = int(config.get("no_volumes", "2"))
    repeat_loop = bool(config.get("perform_mount_umount_loop", False))
    loop_count = int(config.get("no_loops", 1))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server = nfs_node.hostname

    client = clients[0]
    mounts = {}
    exports = {}
    try:
        # Step 1: Enable nfs
        Ceph(client).mgr.module(action="enable", module="nfs", force=True)

        # Step 2: Create an NFS cluster
        Ceph(client).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server)

        # Step 3: Create the required number of volumes
        for loop in range(loop_count):
            for i in range(no_volumes):
                vol_name = f"{fs}_{str(i)}"
                Ceph(client).fs.volume.create(vol_name)

                # Perform export for the vol created
                export = f"{nfs_export}_{str(i)}"
                Ceph(client).nfs.export.create(
                    fs_name=fs, nfs_name=nfs_name, nfs_export=export, fs=vol_name
                )
                exports[nfs_name] = export

                # Mount it on a client - Selecting the client randomly
                client_node = choice(clients)
                mount_path = f"{nfs_mount}_{str(i)}"
                client.exec_command(cmd=f"mkdir -p {mount_path}", sudo=True)
                if Mount(client_node).nfs(
                    mount=mount_path,
                    version=version,
                    port=port,
                    server=nfs_server,
                    export=export,
                ):
                    raise OperationFailedError(
                        f"Failed to mount nfs on {client.hostname}"
                    )
                mounts[client_node.hostname] = mount_path

            # If repeat process, then unmount
            if repeat_loop:
                log.info("Performing cleanup")
                cleanup_cluster(clients, mounts, nfs_name, exports)

    except Exception as e:
        log.error(f"Failed to validate multiple volume export and mount : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, mounts, nfs_name, exports)
        log.info("Cleaning up successful")
    return 0
