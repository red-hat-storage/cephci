from threading import Thread

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


CMD = """python3 -c 'from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN;from time import sleep;f = open(
"/mnt/nfs/sample_file", "w");flock(f.fileno(), LOCK_EX | LOCK_NB);sleep(30);flock(f.fileno(), LOCK_UN)'"""


def get_file_lock(client):
    try:
        client.exec_command(cmd=CMD, sudo=True)
        log.info("Executed successfully")
    except Exception:
        log.info(f"Exception happened while running on {client.hostname}")


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")

    nfs_node = nfs_nodes[0]

    # Step 1: Enable nfs
    Ceph(clients[0]).mgr.module(action="enable", module="nfs", force=True)

    # Step 2: Create an NFS cluster
    nfs_name = "cephfs-nfs"
    nfs_server_name = nfs_node.hostname
    Ceph(clients[0]).nfs.cluster.create(name=nfs_name, nfs_server=nfs_server_name)

    # Step 3: Perform Export on clients
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    for client in clients[:1]:
        Ceph(client).nfs.export.create(
            fs_name=fs_name, nfs_name=nfs_name, nfs_export=nfs_export, fs=fs
        )

    # Step 4: Perform nfs mount
    for client in clients[:1]:
        # Create mount dirs
        client.exec_command(cmd=f"mkdir -p {nfs_mount}", sudo=True)
        cmd = f"mount -t nfs -o vers={version},port={port} {nfs_server_name}:{nfs_export} {nfs_mount}"
        out, _ = client.exec_command(cmd=cmd, sudo=True)
        if out:
            raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")

    # Step 5: Create a file on Client 1
    file_path = "/mnt/nfs/sample_file"
    clients[0].exec_command(cmd=f"touch {file_path}", sudo=True)

    # Step 6: Perform File Lock from client 1
    c1 = Thread(target=get_file_lock, args=(clients[0],))
    c1.start()
    log.info("Acquired lock from client 1")

    # At the same time, try locking file from client 2
    try:
        clients[1].exec_command(cmd=CMD, sudo=True)
        raise OperationFailedError(
            "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
        )
    except Exception:
        log.info(
            "Expected: Failed to acquire lock from client 2 while client 1 lock is in on"
        )

    # Wait for the lock to release
    c1.join()

    # Try again when the lock is released
    try:
        clients[1].exec_command(cmd=CMD, sudo=True)
        log.info(
            "Expected: Successfully acquired lock from client 2 while client 1 lock is released"
        )
    except Exception:
        raise OperationFailedError(
            "Unexpected: Failed to acquire lock from client 2 while client 1 lock is in removed"
        )
    finally:
        for client in clients:
            client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}/*")
            log.info("Unmounting nfs-ganesha mount on client:")
            client.exec_command(sudo=True, cmd=" umount %s -l" % (nfs_mount))
            log.info("Removing nfs-ganesha mount dir on client:")
            client.exec_command(sudo=True, cmd="rm -rf  %s" % (nfs_mount))
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {nfs_export}",
                check_ec=False,
            )
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {nfs_name}",
                check_ec=False,
            )
            log.info("Cleaning up successfull")
    return 0
