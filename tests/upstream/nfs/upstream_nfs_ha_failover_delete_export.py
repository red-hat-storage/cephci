import json
from threading import Thread
from time import sleep

from upstream_nfs_operations import perform_failover

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def delete_export(client, nfs_name, nfs_export, total_export):
    for i in range(0, total_export):
        try:
            Ceph(client).nfs.export.delete(nfs_name, f"{nfs_export}_{i}")
            sleep(1)
        except Exception:
            raise OperationFailedError(f"Fail to delete {nfs_export}_{i}")


def run(ceph_cluster, **kw):
    """Perform failover when linux untar is running
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "1"))
    no_servers = int(config.get("servers", "3"))
    no_export_per_client = int(config.get("no_export_per_client", "10"))
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)
    total_export = no_clients * no_export_per_client

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    # If the setup doesn't have required number of nfs server, exit.
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")

    clients = clients[:no_clients]
    servers = nfs_nodes[:no_servers]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = [nfs_node.hostname for nfs_node in servers]

    try:
        # Setup nfs cluster
        Ceph(clients[0]).mgr.module.enable(module="nfs", force=True)
        Ceph(clients[0]).nfs.cluster.create(
            name=nfs_name, nfs_server=nfs_server_name, ha=ha, vip=vip
        )
        for i in range(0, total_export):
            Ceph(clients[0]).nfs.export.create(
                fs_name=fs_name,
                nfs_name=nfs_name,
                nfs_export=f"{nfs_export}_{i}",
                fs=fs,
                installer=nfs_nodes[0]
            )
            log.info(f"Export: {nfs_export}_{i} successfuly created")
            sleep(3)

        # Delete all exports
        th = Thread(
            target=delete_export,
            args=(
                clients[0],
                nfs_name,
                nfs_export,
                total_export,
            ),
        )
        th.start()

        # Perfrom and verify failover
        failover_node = nfs_nodes[0]
        perform_failover(nfs_nodes, failover_node, vip)

        # Wait to complete delete export
        th.join()

        # Check export
        out = (
            clients[0]
            .exec_command(cmd=f"ceph nfs export ls {nfs_name}", sudo=True)[0]
            .strip()
        )
        if out != "[]":
            raise OperationFailedError("All export not deleted")
            return 1

    except Exception as e:
        log.error(f"Error : {e}")
        log.info("Cleaning up")
        Ceph(clients[0]).nfs.cluster.delete(nfs_name)
        # Delete the subvolume
        for i in range(len(clients)):
            cmd = "ceph fs subvolume ls cephfs --group_name ganeshagroup"
            out = clients[0].exec_command(sudo=True, cmd=cmd)
            json_string, _ = out
            data = json.loads(json_string)
            # Extract names of subvolume
            for item in data:
                subvol = item["name"]
                cmd = f"ceph fs subvolume rm cephfs {subvol} --group_name ganeshagroup"
                clients[0].exec_command(sudo=True, cmd=cmd)

        # Delete the subvolume group
        cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
        clients[0].exec_command(sudo=True, cmd=cmd)
        return 1

    finally:
        log.info("Cleaning up")
        Ceph(clients[0]).nfs.cluster.delete(nfs_name)
        # Delete the subvolume
        for i in range(len(clients)):
            cmd = "ceph fs subvolume ls cephfs --group_name ganeshagroup"
            out = clients[0].exec_command(sudo=True, cmd=cmd)
            json_string, _ = out
            data = json.loads(json_string)
            # Extract names of subvolume
            for item in data:
                subvol = item["name"]
                cmd = f"ceph fs subvolume rm cephfs {subvol} --group_name ganeshagroup"
                clients[0].exec_command(sudo=True, cmd=cmd)

        # Delete the subvolume group
        cmd = "ceph fs subvolumegroup rm cephfs ganeshagroup --force"
        clients[0].exec_command(sudo=True, cmd=cmd)
    return 0
