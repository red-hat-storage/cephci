import shlex
from threading import Thread
from time import sleep, time

from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)

LOCK_READY_FILENAME = ".cephci_nfs_lock_ready"


def wait_for_lock_holder_ready(client, marker_path, timeout=90):
    """Wait until client 1 creates the marker after taking LOCK_EX (sync for contender)."""
    deadline = time() + timeout
    while time() < deadline:
        try:
            client.exec_command(sudo=True, cmd=f"test -f {marker_path}")
            log.info(f"Lock holder ready ({marker_path})")
            return
        except Exception:
            sleep(1)
    raise OperationFailedError(f"Timeout waiting for lock holder marker {marker_path}")


def rootsquash_using_conf(
    client, nfs_name, nfs_export_squash, original_squash_value, new_squash_value
):
    """Create rootsquash using conf file
    Args:
        client(obj): client object
        nfs_name(str): nfs server name
        nfs_export_squash(str): nfs squash export name
        original_squash_value(str): original squash value in conf file
        new_squash_value(str): new squash value in conf file
    """
    try:
        out = Ceph(client).nfs.export.get(nfs_name, nfs_export_squash)
        client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
        client.exec_command(
            sudo=True,
            cmd=f"sed -i 's/{original_squash_value}/{new_squash_value}/g' export.conf",
        )
        Ceph(client).nfs.export.apply(nfs_name, "export.conf")
    except Exception:
        raise OperationFailedError("failed to enable rootsquash using conf file")


def _flock_hold_script(squash_mount, publish_ready_marker):
    """Build python3 -c command for exclusive flock on sample_file over NFS.

    Opens sample_file, takes LOCK_EX | LOCK_NB, holds 30s, then unlocks.
    If publish_ready_marker is True, creates LOCK_READY_FILENAME after flock
    so the second client can synchronize before contending.

    Args:
        squash_mount: NFS mount path (e.g. /mnt/nfs_squash).
        publish_ready_marker: Touch ready marker after lock (client 1 only).

    Returns:
        Command string for client.exec_command.
    """
    sample = f"{squash_mount}/sample_file"
    ready = f"{squash_mount}/{LOCK_READY_FILENAME}"
    parts = [
        "from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN",
        "from time import sleep",
        f"f = open({repr(sample)}, 'w')",
        "flock(f.fileno(), LOCK_EX | LOCK_NB)",
    ]
    if publish_ready_marker:
        parts.append(f"open({repr(ready)}, 'w').close()")
    parts.extend(["sleep(30)", "flock(f.fileno(), LOCK_UN)"])
    py = "; ".join(parts)
    return f"python3 -c {shlex.quote(py)}"


def get_file_lock(client, squash_mount):
    """
    Acquire exclusive non-blocking lock on sample_file, hold ~30s, release.
    """
    client.exec_command(
        sudo=True,
        cmd=_flock_hold_script(squash_mount, publish_ready_marker=False),
    )


def get_file_lock_holder(client, squash_mount):
    """Same as get_file_lock but drops a marker after flock so client 2 can synchronize."""
    client.exec_command(
        sudo=True,
        cmd=_flock_hold_script(squash_mount, publish_ready_marker=True),
    )


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
    no_clients = int(config.get("clients", "2"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    nfs_server_name = nfs_node.hostname
    installer = ceph_cluster.get_nodes("installer")[0]

    # Squashed export parameters
    nfs_export_squash = "/export_squash"
    nfs_squash_mount = "/mnt/nfs_squash"
    original_squash_value = '"squash": "none"'
    new_squash_value = '"squash": "rootsquash"'

    try:
        # single_export: one CephFS export for all clients so flock is on the same inode;
        # without it each client gets a separate export and locks do not block each other.
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
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
            single_export=True,
        )

        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_squash,
            fs=fs_name,
        )
        # Mount the volume with rootsquash enable on client 1 and 2
        for client in clients[:2]:
            client.create_dirs(dir_path=nfs_squash_mount, sudo=True)

        # Change the permission of mount dir and mount the exports
        for client in clients[:2]:
            if Mount(client).nfs(
                mount=nfs_squash_mount,
                version=version,
                port=port,
                server=nfs_server_name,
                export=nfs_export_squash,
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
            client.exec_command(sudo=True, cmd=f"chmod 777 {nfs_squash_mount}/")
        log.info("Mount succeeded on client")

        # Enable rootsquash using conf file
        rootsquash_using_conf(
            clients[1],
            nfs_name,
            nfs_export_squash,
            original_squash_value,
            new_squash_value,
        )
        sleep(90)  # allow Ganesha to finish grace after export apply

    except Exception as e:
        log.error(f"Failed to setup nfs cluster with rootsquash enabled : Error - {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    try:
        # Check the mount protocol and enable locking for v3
        if version == 3:
            enable_v3_locking(installer, nfs_name, nfs_node, nfs_server_name)

        # Create file on squashed dir; chmod the file so squashed UIDs can open it for "w"
        # (chmod on the mount dir alone leaves sample_file 644 and the second lock fails).
        clients[0].exec_command(
            sudo=True,
            cmd=(
                f"touch {nfs_squash_mount}/sample_file && "
                f"chmod 666 {nfs_squash_mount}/sample_file"
            ),
        )
        ready = f"{nfs_squash_mount}/{LOCK_READY_FILENAME}"
        clients[0].exec_command(sudo=True, cmd=f"rm -f {ready}")
    except Exception as e:
        log.error(f"Failed to create rootsquash dir. Error: {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    try:
        # Client 1 holds lock in a thread; marker file syncs before client 2 contends
        c1 = Thread(
            target=get_file_lock_holder,
            args=(
                clients[0],
                nfs_squash_mount,
            ),
        )
        c1.start()
        try:
            wait_for_lock_holder_ready(
                clients[0], f"{nfs_squash_mount}/{LOCK_READY_FILENAME}"
            )
        except Exception as e:
            log.error(f"Lock setup sync failed: {e}")
            c1.join(timeout=120)
            return 1

        try:
            get_file_lock(clients[1], nfs_squash_mount)
            log.error(
                "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
            )
            c1.join(timeout=120)
            return 1
        except Exception as e:
            log.info(
                f"Expected: Failed to acquire lock from client 2 while client 1 lock is in on {e}"
            )

        c1.join()

        clients[0].exec_command(
            sudo=True,
            cmd=f"rm -f {nfs_squash_mount}/{LOCK_READY_FILENAME}",
        )

        try:
            get_file_lock(clients[1], nfs_squash_mount)
            log.info(
                "Expected: Successfully acquired lock from client 2 while client 1 lock is released"
            )
            return 0

        except Exception as e:
            log.error(
                f"Unexpected: Failed to acquire lock from client 2 while client 1 lock is in removed {e}"
            )
            return 1

    finally:
        log.info("Cleaning up")
        # Cleaning up the squash export and mount dir
        for client in clients[:2]:
            log.info("Unmounting nfs-ganesha squash mount on client:")
            if Unmount(client).unmount(nfs_squash_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {clients[0].hostname}"
                )
            log.info("Removing nfs-ganesha squash mount dir on client:")
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_squash_mount}")
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_squash)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleaning up successfull")
