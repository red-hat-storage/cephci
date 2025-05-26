from upstream_nfs_operations import cleanup_cluster, getfattr, setfattr, setup_nfs_cluster

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the basic getfattr and setfattr
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

        # Set the extended attribute of the file
        setfattr(
            client=clients[0],
            file_path=f"{nfs_mount}/{filename}",
            attribute_name="myattr",
            attribute_value="value",
        )

        # Fetch the extended attribute of the file
        out = getfattr(client=clients[0], file_path=f"{nfs_mount}/{filename}")

        # Extract attribute name and value from the output
        for item in out:
            lines = item.splitlines()
            attr_name = lines[1].split(".")[1].split("=")[0]
            attr_value = lines[1].split("=")[1].strip('"')
            log.info(f"Attribute Name: {attr_name}")
            log.info(f"Attribute Value: {attr_value}")
            if attr_name == "myattr" and attr_value == "value":
                log.info(
                    "Validated :Attribute 'myattr' is set to 'value' in the output."
                )
                break
            else:
                log.info("Attribute 'myattr' set to 'value' not found in the output.")
                return 1

    except Exception as e:
        log.error(f"Failed to perform export client addr validation : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
