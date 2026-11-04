import json

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def _export_list(client, nfs_name):
    raw = Ceph(client).nfs.export.ls(nfs_name)
    if isinstance(raw, str):
        raw = json.loads(raw)
    return raw


def run(ceph_cluster, **kw):
    """Create and delete several exports in a loop.

    The exports are not mounted. Unexport while a client holds the share
    is a different case and is already CEPH-83575994.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", "1"))
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")
    clients = clients[:no_clients]
    client = clients[0]
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    fs_name = "cephfs"
    nfs_server = nfs_nodes[0].hostname
    loop_exports = [f"/loop_export_{i}" for i in range(1, 4)]

    try:
        setup_nfs_cluster(
            clients,
            nfs_server,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs_name,
            ceph_cluster=ceph_cluster,
            single_export=True,
        )
        for cycle in range(2):
            for export_name in loop_exports:
                Ceph(client).nfs.export.create(
                    fs_name=fs_name,
                    nfs_name=nfs_name,
                    nfs_export=export_name,
                    fs=fs_name,
                )
                if export_name not in _export_list(client, nfs_name):
                    raise OperationFailedError(
                        f"{export_name} was not created on cycle {cycle}"
                    )
                Ceph(client).nfs.export.delete(nfs_name, export_name)
                if export_name in _export_list(client, nfs_name):
                    raise OperationFailedError(
                        f"{export_name} was not deleted on cycle {cycle}"
                    )
                log.info("Cycle %s exported and unexported %s", cycle, export_name)
        log.info(
            "Export and unexport loop completed with no client mount on the loop exports"
        )
        return 0
    except Exception as exc:
        log.error(f"Failed export/unexport loop: {exc}")
        return 1
    finally:
        log.info("Cleaning up")
        for export_name in loop_exports:
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {export_name}",
                check_ec=False,
            )
        cleanup_cluster(
            clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes[0]
        )
