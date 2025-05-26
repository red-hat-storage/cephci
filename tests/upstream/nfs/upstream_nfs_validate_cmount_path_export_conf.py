import json

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Test if the export file has the cmount_path param
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version")
    no_clients = int(config.get("clients", "1"))
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    nfs_server_name = nfs_nodes[0].hostname
    fs_name = "cephfs"
    export_name = "/export_1"

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")
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
            ceph_cluster=ceph_cluster,
        )

        # Fetch the export info
        out = Ceph(clients[0]).nfs.export.get(nfs_name, export_name)
        log.info(out)
        data = json.loads(out)

        # Check if cmount_path is present in output or not
        if data.get("fsal", {}).get("cmount_path") == "/":
            log.info("Test Passed: 'cmount_path': '/' is present.")
        else:
            log.info("Test Failed: 'cmount_path': '/' is not present.")

    except Exception as e:
        log.error(f"Failed to validate the cmount_param in export file : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")

    return 0
