from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount
from utility.retry import retry


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def mount_retry(
    clients, client_num, mount_name, version, port, nfs_server, export_name, ha=False
):
    if Mount(clients[client_num]).nfs(
        mount=mount_name,
        version=version,
        port=port,
        server=nfs_server,
        export=export_name,
    ):
        raise OperationFailedError("Failed to mount nfs on %s" % export_name.hostname)
    return True


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def create_file(client, nfs_mount, file_name):
    """Create a file in the NFS mount point"""
    try:
        cmd = f"touch {nfs_mount}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(f"Failed to create file {file_name}: {e}")


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def delete_file(client, nfs_mount, file_name):
    """Delete a file in the NFS mount point"""
    try:
        cmd = f"rm -rf {nfs_mount}/{file_name}"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(f"Failed to delete file {file_name}: {e}")


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def rename_file(client, nfs_mount, old_name, new_name):
    """Rename a file in the NFS mount point"""
    try:
        cmd = f"mv {nfs_mount}/{old_name} {nfs_mount}/{new_name}"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(
            f"Failed to rename file {old_name} to {new_name}: {e}"
        )


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def write_to_file_using_dd_command(client, nfs_mount, file_name, size):
    """Write to a file in the NFS mount point using dd command"""
    try:
        cmd = f"dd if=/dev/zero of={nfs_mount}/{file_name} bs={size}M count=5"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(f"Failed to write to file {file_name}: {e}")


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def read_from_file_using_dd_command(client, nfs_mount, file_name, size):
    """Read from a file in the NFS mount point using dd command"""
    try:
        cmd = f"dd if={nfs_mount}/{file_name} of=/dev/null bs={size}M count=5"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(f"Failed to read from file {file_name}: {e}")


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def permission_to_directory(client, nfs_mount):
    """Provide permission to directory"""
    try:
        cmd = f"chmod 777 {nfs_mount}"
        client.exec_command(cmd=cmd, sudo=True)
    except Exception as e:
        raise OperationFailedError(
            f"Failed to provide permission to directory {nfs_mount}: {e}"
        )


@retry(OperationFailedError, tries=4, delay=5, backoff=2)
def perform_lookups(client, nfs_mount):
    """Perform lookups on the NFS mount point"""
    try:
        cmd = f"ls -lart {nfs_mount}"
        client.exec_command(cmd=cmd, sudo=True)

        cmd = f"du -sh {nfs_mount}"
        client.exec_command(cmd=cmd, sudo=True)
        return True
    except Exception as e:
        raise OperationFailedError(f"Failed to perform lookups on {nfs_mount}: {e}")
