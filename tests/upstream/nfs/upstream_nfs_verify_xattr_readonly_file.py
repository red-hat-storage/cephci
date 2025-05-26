from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the extended attributes on readonly file
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
    user1 = "Test1"
    user2 = "Test2"
    attribute_name = "myattr"
    attribute_value = "value"

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

        # Create a file on Mount point
        cmd = f"touch {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Add Test1 and Test2 User
        cmd = f"useradd {user1} && useradd {user2}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Change the owner of file as Test1 user
        cmd = f"chown {user1} {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Change the permission of file to Readonly
        cmd = f"chmod 444 {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Try changing the extended attribute of Readonly file
        cmd = f"su -c \"setfattr -n user.{attribute_name} -v '{attribute_value}' '{nfs_mount}/{filename}'\" {user1}"
        _, rc = clients[0].exec_command(cmd=cmd, sudo=True, check_ec=False)
        if "Permission denied" in str(rc):
            log.info("Expected : Attributes cannot be set on readpnly file")
        else:
            raise OperationFailedError("Unexpected: Attributes set on Readonly file")

    except Exception as e:
        log.error(
            f"Failed to perform xattr operation validation across filesystem operation : {e}"
        )
        for i in range(1, 3):
            cmd = f"userdel Test{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        for i in range(1, 3):
            cmd = f"userdel Test{i}"
            clients[0].exec_command(cmd=cmd, sudo=True)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
