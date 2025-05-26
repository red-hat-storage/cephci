import json
from threading import Thread
from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from cli.io.small_file import SmallFile
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify NFS HA cluster creation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", "2"))
    no_servers = int(config.get("servers", "1"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

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
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )
        log.info("Successfully setup NFS cluster")

        # Start IO on the client nodes
        th = Thread(
            target=SmallFile(clients[0]).run,
            args=(nfs_mount, ["create"], 4, 4194, 1024),
        )
        th.start()

        # Keeping a static sleep for the IO to start before performing failover
        sleep(3)

        # Now perform export while IO in progress
        for i in range(10, 60):
            nfs_export_name = f"/export_{i}"
            Ceph(clients[0]).nfs.export.create(
                fs_name=fs_name, nfs_name=nfs_name, nfs_export=nfs_export_name, fs=fs, installer=nfs_nodes[0]
            )
            log.info("ceph nfs export created successfully")

            # Get volume path
            subvol_name = nfs_export_name.replace("/", "")
            cmd = f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
            out = clients[0].exec_command(sudo=True, cmd=cmd)
            export_path = out[0].strip()

            out = Ceph(clients[0]).nfs.export.get(nfs_name, nfs_export_name)
            log.info(out)
            output = json.loads(out)
            export_get_path = output["path"]
            if export_get_path != export_path:
                log.error("Export path is not correct")
                return 1

        # Perform unexport while IO in progress
        for i in range(10, 60):
            nfs_export_name = f"/export_{i}"
            Ceph(clients[0]).nfs.export.delete(cluster=nfs_name, export=nfs_export_name)

        # Wait for the IO to complete
        th.join()

    except Exception as e:
        log.error(f"Failed to setup nfs cluster {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        pass
    return 0
