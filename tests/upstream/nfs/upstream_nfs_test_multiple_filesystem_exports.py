from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def create_cephfs_filesystem(client, index):
    """Create CephFS filesystems and subvolumes."""
    cmd_create_fs = f"ceph fs volume create cephfs{index}"
    client.exec_command(cmd=cmd_create_fs, sudo=True)

    # Create subvolume group
    cmd_create_svg = f"ceph fs subvolumegroup create cephfs{index} ganeshagroup{index}"
    client.exec_command(cmd=cmd_create_svg, sudo=True)

    # Create subvolume
    cmd_create_subvol = (
        f"ceph fs subvolume create cephfs{index} "
        f"ganesha_subvol{index} --group_name ganeshagroup{index} "
        f"--namespace-isolated"
    )
    client.exec_command(cmd=cmd_create_subvol, sudo=True)


def get_subvolume_path(client, index):
    """Retrieve the path of a subvolume."""
    cmd = f"ceph fs subvolume getpath cephfs{index} ganesha_subvol{index} --group_name ganeshagroup{index}"
    out, _ = client.exec_command(cmd=cmd, sudo=True)
    return out.strip()


def create_nfs_export(client, fs_name, nfs_name, nfs_export_name, path, index, nfs_node):
    """Create an NFS export."""
    cmd = f"ceph nfs export create cephfs {nfs_name} {nfs_export_name} cephfs{index} --path={path}"
    client.exec_command(cmd=cmd, sudo=True)
    Ceph(client).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=nfs_export_name,
        fs=fs_name,
        installer=nfs_node
    )
    log.info(f"NFS export '{nfs_export_name}' created successfully.")


def mount_and_validate_export(client, version, port, server_name, export_name, index):
    """Mount the NFS export and validate."""
    mount_dir = f"/mnt/ganesha{index}"
    client.create_dirs(dir_path=f"ganesha{index}", sudo=True)
    if Mount(client).nfs(
        mount=mount_dir,
        version=version,
        port=port,
        server=server_name,
        export=export_name,
    ):
        raise OperationFailedError(f"Failed to mount NFS on {client.hostname}")
    log.info("Mount succeeded on client.")

    # Create a file on mount point
    client.exec_command(sudo=True, cmd=f"touch {mount_dir}/file{index}")


def cleanup_exports_and_filesystems(client):
    """Clean up the exports and filesystems."""
    cmd = "ceph config set mon mon_allow_pool_delete true"
    client.exec_command(sudo=True, cmd=cmd)

    for i in range(1, 5):
        mount_dir = f"/mnt/ganesha{i}"
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_dir}/*", long_running=True)
        log.info("Unmounting NFS-ganesha exports from client.")
        if Unmount(client).unmount(mount_dir):
            raise OperationFailedError(f"Failed to unmount NFS on {client.hostname}")
        log.info("Removing NFS-ganesha mount dir on client.")
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_dir}")
        sleep(3)

        # Delete the NFS export and CephFS components
        nfs_export_name = f"/ganesha_{i}"

        # Delete the NFS export
        Ceph(client).nfs.export.delete("cephfs-nfs", nfs_export_name)

        # Delete the CephFS subvolume
        cmd_subvolume_rm = (
            f"ceph fs subvolume rm cephfs{i} ganesha_subvol{i} "
            f"--group_name ganeshagroup{i}"
        )
        client.exec_command(sudo=True, cmd=cmd_subvolume_rm)

        # Delete the CephFS subvolume group
        cmd_subvolumegroup_rm = (
            f"ceph fs subvolumegroup rm cephfs{i} ganeshagroup{i} --force"
        )
        client.exec_command(sudo=True, cmd=cmd_subvolumegroup_rm)

        # Delete the CephFS volume
        cmd_volume_rm = f"ceph fs volume rm cephfs{i} --yes-i-really-mean-it"
        client.exec_command(sudo=True, cmd=cmd_volume_rm)


def run(ceph_cluster, **kw):
    """Test multiple export creation from multiple filesystems."""
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version")
    no_clients = int(config.get("clients", "3"))

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    nfs_server_name = nfs_nodes[0].hostname

    try:
        """Setup NFS cluster and create multiple CephFS filesystems and exports."""
        port = config.get("port", "2049")
        version = config.get("nfs_version")
        nfs_name = "cephfs-nfs"
        nfs_mount = "/mnt/nfs"
        nfs_export = "/export"
        fs_name = "cephfs"

        # Setup NFS cluster
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

        for i in range(1, 5):
            create_cephfs_filesystem(clients[0], i)

            # Create export
            nfs_export_name = f"/ganesha_{i}"
            path = get_subvolume_path(clients[0], i)
            create_nfs_export(clients[0], fs_name, nfs_name, nfs_export_name, path, i, nfs_nodes[0])

            # Mount and validate export
            mount_and_validate_export(
                clients[0], version, port, nfs_server_name, nfs_export_name, i
            )
    except Exception as e:
        log.error(
            f"Failed to validate multiple export creation from multiple filesystems: {e}"
        )
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_exports_and_filesystems(clients[0])
        cleanup_cluster(clients[0], "/mnt/nfs", "cephfs-nfs", "/export")
        log.info("Cleanup successful")
    return 0
