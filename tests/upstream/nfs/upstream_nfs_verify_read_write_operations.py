from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    operation = config.get("operation")
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
            ceph_cluster=ceph_cluster,
        )

        if operation == "verify_permission":
            # Create file in nfs share
            cmd = f"touch {nfs_mount}/test_file"
            clients[1].exec_command(cmd=cmd, sudo=True)

            # Now modify the permission of the file to root user only
            cmd = f"chown -R root {nfs_mount}/test_file"
            clients[1].exec_command(cmd=cmd, sudo=True)

            # Set the permissions to be read only for just the user
            cmd = f"chmod -R 400 {nfs_mount}/test_file"
            clients[1].exec_command(cmd=cmd, sudo=True)

            # Now create a new user and try reading the file
            cmd = "useradd test_non_root_user"
            clients[1].exec_command(cmd=cmd, sudo=True, check_ec=False)

            # Try accessing the file using the new user
            flag = False
            try:
                cmd = f"su test_non_root_user -c 'cat {nfs_mount}/test_file'"
                clients[1].exec_command(cmd=cmd, sudo=True)
                flag = True
            except Exception as e:
                if "Permission denied" in str(e):
                    log.info(
                        f"Expected! Permission denied error for user without access: {e}"
                    )
                else:
                    raise OperationFailedError(
                        f"Failed with an error other than permission denied: {e}"
                    )
            if flag:
                raise OperationFailedError(
                    "Unexpected! User without permission able to access the file"
                )

        elif operation == "verify_non_existing_file":
            # Check 1: Try reading a non-existing file
            flag = False
            try:
                cmd = f"cat {nfs_mount}/non_existing_file"
                clients[0].exec_command(cmd=cmd, sudo=True)
                flag = True
            except Exception as e:
                log.info(f"Expected. Failed to read a non existing file. {e}")
            # Raise an assertion if the operation passed
            if flag:
                raise OperationFailedError(
                    "Unexpected: Read on non existing file passed"
                )

            # Check 2: Try writing to a non-existing file
            flag = False
            try:
                cmd = f"cat 'some random text' {nfs_mount}/non_existing_file"
                clients[0].exec_command(cmd=cmd, sudo=True)
                flag = True
            except Exception as e:
                log.info(f"Expected. Failed to write a non existing file. {e}")
            # Raise an assertion if the operation passed
            if flag:
                raise OperationFailedError(
                    "Unexpected: Write on non existing file passed"
                )
        elif operation == "mv_file":
            cmd = f"touch {nfs_mount}/test_file"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Now modify the permission of the file to root user only
            cmd = f"chown -R root {nfs_mount}/test_file"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Set the permissions to be read only for just the user
            cmd = f"chmod -R 400 {nfs_mount}/test_file"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Now create a new user and try reading the file
            try:
                cmd = "useradd test_non_root_user"
                clients[0].exec_command(cmd=cmd, sudo=True)
            except Exception as e:
                log.error(f"User creation failed with {str(e)}")

            # Try accessing the file using the new user
            flag = False
            try:
                cmd = f"su test_non_root_user -c 'mv {nfs_mount}/test_file /mnt'"
                clients[0].exec_command(cmd=cmd, sudo=True)
                flag = True
            except Exception as e:
                if "Permission denied" in str(e):
                    log.info(
                        f"Expected! Permission denied error for user without access: {e}"
                    )
                else:
                    raise OperationFailedError(
                        f"Failed with an error other than permission denied: {e}"
                    )
            if flag:
                raise OperationFailedError(
                    "Unexpected! User without permission able to access the file"
                )

        elif operation == "mv_file_overwrite":
            # Create two files
            cmd = f"echo test1 > {nfs_mount}/file1"
            clients[0].exec_command(cmd=cmd, sudo=True)

            cmd = "echo test2 > /mnt/file1"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Now use mv to replace test1 with test2
            cmd = f"mv /mnt/file1 {nfs_mount}/file1"
            clients[0].exec_command(cmd=cmd, sudo=True)

            # Verify the file1 inside nfs share has content of the moved file
            cmd = f"cat {nfs_mount}/file1"
            out, _ = clients[0].exec_command(cmd=cmd, sudo=True)
            if "test2" not in out:
                raise OperationFailedError(
                    "Mv operation doesn't overwrite the content of existing file"
                )
            log.info("Expected: mv operation has overwritten the file")

    except Exception as e:
        log.error(f"Failed to validate read write operations : {e}")
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
