from time import sleep

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount
from cli.utilities.utils import get_file_owner
from utility.log import Log

log = Log(__name__)

SQUASH_NONE = '"squash": "none"'
SQUASH_ROOT = '"squash": "rootsquash"'


def _enable_rootsquash(client, nfs_name, export_name):
    """Rewrite an existing export from squash none to rootsquash."""
    out = Ceph(client).nfs.export.get(nfs_name, export_name)
    client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
    client.exec_command(
        sudo=True,
        cmd=f"sed -i 's/{SQUASH_NONE}/{SQUASH_ROOT}/g' export.conf",
    )
    Ceph(client).nfs.export.apply(nfs_name, "export.conf")
    sleep(10)


def run(ceph_cluster, **kw):
    """Edit an export and enable rootsquash.

    Root can create a file on the original export. After the export conf
    is edited to rootsquash, root create on that export is denied. That is
    the Tentacle tier-1 rootsquash result.
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
    squash_mount = "/mnt/nfs_squash"
    nfs_export = "/export"
    squash_export = "/export_squash"
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
        client.exec_command(sudo=True, cmd=f"touch {nfs_mount}/file_plain")
        if "rootuser" not in get_file_owner(f"{nfs_mount}/file_plain", clients):
            raise OperationFailedError(
                "File on the unsquashed export is not owned by root"
            )
        log.info("Root created a file before rootsquash was enabled")

        Ceph(client).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=squash_export,
            fs=fs_name,
        )
        _enable_rootsquash(client, nfs_name, squash_export)
        client.create_dirs(dir_path=squash_mount, sudo=True)
        if Mount(client).nfs(
            mount=squash_mount,
            version=version,
            port=port,
            server=nfs_server,
            export=squash_export,
        ):
            raise OperationFailedError(f"Failed to mount {squash_export}")

        _, err = client.exec_command(
            sudo=True,
            cmd=f"touch {squash_mount}/file_squashed",
            check_ec=False,
        )
        if "Permission denied" not in str(err):
            raise OperationFailedError(
                "Root create was not denied after the export was edited to rootsquash"
            )
        log.info("Rootsquash is enabled: root create is denied")
        return 0
    except Exception as exc:
        log.error(f"Failed to edit export and enable rootsquash: {exc}")
        return 1
    finally:
        log.info("Cleaning up")
        client.exec_command(sudo=True, cmd=f"umount -l {squash_mount}", check_ec=False)
        client.exec_command(sudo=True, cmd=f"rm -rf {squash_mount}", check_ec=False)
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {squash_export}",
            check_ec=False,
        )
        cleanup_cluster(
            clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes[0]
        )
