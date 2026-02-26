from time import sleep

from nfs_operations import cleanup_cluster, run_as_user, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify owner of symbolic link and the owner of the target file can be different users.
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))

    # Optional non-root user (set by nfs_create_run_user step; None = run as root)
    run_user = kw.get("test_data", {}).get("nfs_run_user")

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
            run_user=run_user,
        )

        # Create file
        run_as_user(clients[0], f"touch {nfs_mount}/test1_file", run_user)

        # Change owner of file to "cephuser" (stays as root sudo — ownership change)
        clients[0].exec_command(
            cmd=f"chown cephuser {nfs_mount}/test1_file", sudo=True
        )

        # Create symbolic link
        run_as_user(
            clients[0],
            f"ln -s {nfs_mount}/test1_file {nfs_mount}/link1_file",
            run_user,
        )

        # verify owner of symbolic link file and target file is different
        owner_target_file = clients[0].exec_command(
            cmd="ls -l /mnt/nfs/test1_file | awk '{print $3}'", sudo=True
        )[0]
        owner_sym_link_file = clients[0].exec_command(
            cmd="ls -l /mnt/nfs/link1_file | awk '{print $3}'", sudo=True
        )[0]
        if owner_target_file == owner_sym_link_file:
            raise OperationFailedError("Owner of target and sym link files same")
        else:
            log.info("Expected! owner of target and sym link files are different")
        return 0
    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        sleep(3)
        cleanup_cluster(
            clients,
            nfs_mount,
            nfs_name,
            nfs_export,
            nfs_nodes=nfs_node,
            run_user=run_user,
        )
        log.info("Cleaning up successfull")
