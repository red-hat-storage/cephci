from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
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
    nfs_server_name = nfs_node.hostname

    try:
        # Setup Nfs cluster
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

        # Install bonnie++ on clients
        for client in clients:
            cmds = [
                "wget https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/Packages/b/bonnie++-2.00a-7.el9"
                ".x86_64.rpm",
                "yum install bonnie++-2.00a-7.el9.x86_64.rpm -y",
            ]
            for cmd in cmds:
                client.exec_command(cmd=cmd, sudo=True)

        # Run Bonnie++ on all the clients
        ctr = 0
        for client in clients:
            folder_name = f"{nfs_mount}/test_folder_{str(ctr)}"
            cmds = [
                f"mkdir -p {folder_name}",
                f"chmod 777 {folder_name}",
                f"bonnie++ -d {folder_name} -u cephuser",
            ]
            ctr += 1
            for cmd in cmds:
                client.exec_command(cmd=cmd, sudo=True)
        log.info("Successfully completed pynfs tests on nfs cluster")
    except Exception as e:
        log.error(f"Failed to run bonnie tests : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
