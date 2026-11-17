from time import sleep

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)

SQUASH_NONE = '"squash": "none"'
SQUASH_ROOT = '"squash": "rootsquash"'


def run(ceph_cluster, **kw):
    """Root and nobody edit one file after rootsquash is enabled.

    On Tentacle, rootsquash maps client root to the anonymous uid, which is
    the same uid as local nobody. The file is made world-writable before
    squash is turned on, so both identities can append to it.
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
    export_name = f"{nfs_export}_0"
    shared = f"{nfs_mount}/shared_file"
    fs_name = "cephfs"
    nfs_server = nfs_nodes[0].hostname

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
        client.exec_command(sudo=True, cmd=f"echo original > {shared}")
        client.exec_command(sudo=True, cmd=f"chmod 666 {shared}")

        out = Ceph(client).nfs.export.get(nfs_name, export_name)
        client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        client.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{SQUASH_NONE}/{SQUASH_ROOT}/g' export.conf",
        )
        Ceph(client).nfs.export.apply(nfs_name, "export.conf")
        sleep(10)
        if Unmount(client).unmount(nfs_mount):
            raise OperationFailedError(f"Failed to unmount {nfs_mount} before remount")
        if Mount(client).nfs(
            mount=nfs_mount,
            version=version,
            port=port,
            server=nfs_server,
            export=export_name,
        ):
            raise OperationFailedError(f"Failed to remount {export_name}")

        _, err = client.exec_command(
            sudo=True,
            cmd=f"echo root-edit >> {shared}",
            check_ec=False,
        )
        if err and "Permission denied" in str(err):
            raise OperationFailedError(
                "Squashed root could not edit the world-writable file"
            )
        _, err = client.exec_command(
            sudo=True,
            cmd=f"runuser -u nobody -- bash -c 'echo nobody-edit >> {shared}'",
            check_ec=False,
        )
        if err and "Permission denied" in str(err):
            raise OperationFailedError("nobody could not edit the world-writable file")

        content, _ = client.exec_command(sudo=True, cmd=f"cat {shared}")
        if "root-edit" not in content or "nobody-edit" not in content:
            raise OperationFailedError(
                f"Both edits were not stored in the file: {content}"
            )
        log.info("Root and nobody both edited the file under rootsquash")
        return 0
    except Exception as exc:
        log.error(f"Failed root and nobody edit under rootsquash: {exc}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(
            clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes[0]
        )
