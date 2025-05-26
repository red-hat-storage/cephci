from upstream_nfs_operations import (
    cleanup_cluster,
    getfattr,
    removeattr,
    setfattr,
    setup_nfs_cluster,
)

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify setting multiple extended attribute on file and deleting all of them
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

        # Create a file on Mount point
        cmd = f"touch {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Set multiple extended attribute of the file
        for i in range(1, 11):
            setfattr(
                client=clients[0],
                file_path=f"{nfs_mount}/{filename}",
                attribute_name=f"myattr{i}",
                attribute_value=f"value{i}",
            )
            log.info(f"Successfully set the attribute 'myattr{i}' on file - {filename}")

        # Fetch the extended attribute of the file
        out = getfattr(client=clients[0], file_path=f"{nfs_mount}/{filename}")

        # Extract attribute name and value from the output
        attr_names = []
        attr_values = []
        for item in out:
            lines = item.splitlines()
            for line in lines:
                if line.startswith("user."):
                    attr = line.split("=")
                    attr_name = attr[0].split(".")[1]
                    attr_value = attr[1].strip('"')
                    attr_names.append(attr_name)
                    attr_values.append(attr_value)
                    log.info(f"Attribute Name: {attr_name}")
                    log.info(f"Attribute Value: {attr_value}")
        for i in range(1, 11):
            if f"myattr{i}" in attr_names and f"value{i}" in attr_values:
                log.info(
                    f"Validated :Attribute 'myattr{i}' and 'value{i}' is successfully set."
                )
            else:
                log.info(
                    f"Attribute 'myattr{i}' and 'value{i}' not found in the output."
                )
                return 1

        # Deleting all the attributes on file
        for i in range(1, 11):
            removeattr(
                client=clients[0],
                file_path=f"{nfs_mount}/{filename}",
                attribute_name=f"myattr{i}",
            )
            log.info(
                f"Successfully removed the attribute 'myattr{i}' on file - {filename}"
            )

        # Fetch the extended attribute of the file
        out = getfattr(client=clients[0], file_path=f"{nfs_mount}/{filename}")
        log.info("Listing all the attributes on {filename} ")

    except Exception as e:
        log.error(f"Failed to validate multiple xttars on file : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
