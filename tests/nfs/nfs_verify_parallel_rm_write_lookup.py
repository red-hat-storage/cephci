import time
from threading import Thread

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from ceph.ceph import CommandFailed
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_files, perform_lookups
from utility.log import Log

log = Log(__name__)


def perform_rm(client, nfs_mount, file_count, sudo=True):
    """Remove files from mount point.

    Args:
        client: Client node
        nfs_mount: Mount point path
        file_count: Number of files to remove
        sudo: Whether to use sudo (default: True to match create_files)
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=sudo,
                cmd=f"rm -rf {nfs_mount}/file{i}",
                timeout=30,
            )
            log.info(f"Removed file{i}")
        except CommandFailed as e:
            log.error(f"Failed to remove file{i}: {e}")
            raise OperationFailedError(f"failed to Remove file 'file{i}': {e}")
        except Exception as e:
            log.error(f"Unexpected error removing file{i}: {e}")
            raise OperationFailedError(f"failed to Remove file 'file{i}': {e}")


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "3"))
    sudo = config.get("sudo", False)

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    file_count = 100
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
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )

        operations = []
        # Create large files from client 1
        th = Thread(
            target=create_files,
            args=(clients[1], nfs_mount, file_count),
            kwargs={"sudo": sudo},
        )
        operations.append(th)

        # Perform lookups from client 2
        th = Thread(
            target=perform_lookups,
            args=(clients[2], nfs_mount, file_count),
            kwargs={"sudo": sudo},
        )
        operations.append(th)

        # Start the operations
        for op in operations:
            op.start()
            # Adding explicit sleep for file create to start before rm
            time.sleep(1)

        # Perform rm while file creation and lookup is in progress
        max_total_time = 300
        start_time = time.time()
        iteration = 0
        log.info("Performing rm while file creation and lookup in progress")
        while operations[0].is_alive() or operations[1].is_alive():
            if time.time() - start_time > max_total_time:
                log.error(f"Operations exceeded {max_total_time}s timeout")
                raise OperationFailedError("Parallel operations timed out")

            iteration += 1
            if iteration % 30 == 0:
                log.info(
                    f"RM loop iteration {iteration}, threads alive: "
                    f"create={operations[0].is_alive()}, "
                    f"lookup={operations[1].is_alive()}"
                )
            try:
                perform_rm(clients[0], nfs_mount, file_count, sudo=sudo)
            except Exception as e:
                log.warning(f"RM operation failed (expected during parallel ops): {e}")
            time.sleep(1)

        # Wait for the ops to complete with timeout
        for idx, op in enumerate(operations):
            op.join(timeout=300)
            if op.is_alive():
                op_name = "create" if idx == 0 else "lookup"
                log.error(f"Thread {idx} ({op_name}) did not complete in 300s")
                raise OperationFailedError(f"Thread {idx} hung")
        return 0

    except Exception as e:
        log.error(f"Failed to validate parallel write, lookup and rm: {e}")
        return 1
    finally:
        log.info("Cleaning up")
        cleanup_cluster(
            clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_nodes[0]
        )
        log.info("Cleaning up successful")
