from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount
from tests.nfs.nfs_operations import cleanup_cluster, create_nfs_via_file_and_verify
from tests.nfs.test_nfs_qos_on_cluster_level_enablement import (
    _within_qos_limit,
    capture_copy_details,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify nfs cluster deployment with QoS using spec file PerShare configuration
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "2"))
    installer = ceph_cluster.get_nodes(role="installer")[0]
    original_config = config.get("spec", None)
    timeout = int(config.get("timeout", 300))
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_mount = "/mnt/nfs"
    fs_name = "cephfs"
    nfs_name = "nfs"
    nfs_export = "/export"
    port = config.get("port", "2049")
    version = config.get("nfs_version")
    nfs_server_name = nfs_nodes[0].hostname
    subvolume_group = "ganeshagroup"
    read_bw = original_config["spec"]["cluster_qos_config"]["max_export_read_bw"]
    write_bw = original_config["spec"]["cluster_qos_config"]["max_export_write_bw"]

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    Ceph(clients[0]).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    clients = clients[:no_clients]  # Select only the required number of clients
    try:
        new_objects = []
        new_object = {
            "service_type": original_config["service_type"],
            "service_id": original_config["service_id"],
            "placement": {"label": original_config["placement"]["label"]},
            "spec": {
                "cluster_qos_config": original_config["spec"]["cluster_qos_config"],
            },
        }
        new_objects.append(new_object)
        log.info(f"New NFS Ganesha objects to be created: {new_objects}")

        # Create a nfs instance using the provided configuration
        if not create_nfs_via_file_and_verify(
            installer, new_objects, timeout, nfs_nodes
        ):
            return 1
        log.info("NFS Ganesha instances created successfully")

        # Create NFS export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export,
            fs=fs_name,
        )

        # Mount the export on client
        sleep(9)
        clients[0].create_dirs(dir_path=nfs_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_mount,
            version=version,
            port=port,
            server=nfs_server_name,
            export=nfs_export,
        ):
            raise OperationFailedError(f"Failed to mount nfs on {clients[0].hostname}")
        log.info("Mount succeeded on client")

        speed = capture_copy_details(clients[0], nfs_mount, "sample.txt")
        log.info(
            "Transfer speed is {0} for QoS {1} enabled in export level".format(
                speed, "PerShare"
            )
        )

        write_speed = speed.get("write_speed")
        read_speed = speed.get("read_speed")

        # Compare with some allowed measurement slack (same logic as the working test)
        if _within_qos_limit(write_bw, write_speed) and _within_qos_limit(
            read_bw, read_speed
        ):
            log.info(
                "Test passed: PerShare QoS enabled successfully with cluster deployment"
            )
        else:
            raise OperationFailedError(
                "Test failed: PerShare QoS was not enabled successfully with cluster deployment"
            )
        return 0

    except Exception as e:
        log.error(f"Failed to validate PerShare QoS at cluster level : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes)
        log.info("Cleaning up successful")
