from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.io.io import linux_untar
from utility.log import Log

log = Log(__name__)

FS_NAME = "cephfs"
NFS_NAME = "cephfs-nfs"
NFS_EXPORT = "/export"
NFS_MOUNT = "/mnt/nfs"
FS = "cephfs"


def setup_nfs_cluster(clients, nfs_node, port, version):
    # Step 1: Enable nfs
    Ceph(clients[0]).mgr.module(action="enable", module="nfs", force=True)

    # Step 2: Create an NFS cluster
    nfs_server_name = nfs_node.hostname
    Ceph(clients[0]).nfs.cluster.create(name=NFS_NAME, nfs_server=nfs_server_name)

    # Step 3: Perform Export on clients
    for client in clients:
        Ceph(client).nfs.export.create(
            fs_name=FS_NAME, nfs_name=NFS_NAME, nfs_export=NFS_EXPORT, fs=FS
        )

    # Step 4: Perform nfs mount
    for client in clients:
        # Create mount dirs
        client.exec_command(cmd=f"mkdir -p {NFS_MOUNT}", sudo=True)
        cmd = f"mount -t nfs -o vers={version},port={port} {nfs_server_name}:{NFS_EXPORT} {NFS_MOUNT}"
        out, _ = client.exec_command(cmd=cmd, sudo=True)
        if out:
            raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")


def cleanup_cluster(clients):
    for client in clients:
        client.exec_command(sudo=True, cmd=f"rm -rf {NFS_MOUNT}/*")
        log.info("Unmounting nfs-ganesha mount on client:")
        client.exec_command(sudo=True, cmd=" umount %s -l" % (NFS_MOUNT))
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd="rm -rf  %s" % (NFS_MOUNT))
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {NFS_NAME} {NFS_EXPORT}",
            check_ec=False,
        )
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {NFS_NAME}",
            check_ec=False,
        )


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    nfs_node = nfs_nodes[0]

    try:
        setup_nfs_cluster(clients, nfs_node, port, version)

        # Linux untar on client 1
        _ = linux_untar(clients[0], NFS_MOUNT)

        # Test 1: Perform Linux untar from 1 client and do readir operation from other client (ls -lart)
        cmd = f"ls -lart {NFS_MOUNT}"
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Test 2: Perform Linux untar from 1 client and do readir operation from other client (du -sh)
        cmd = f"du -sh {NFS_MOUNT}"
        clients[2].exec_command(cmd=cmd, sudo=True)

        # Perform Linux untar from 1 client and do readir operation from other client (finds)
        cmd = f"find {NFS_MOUNT} -name *.txt"
        clients[3].exec_command(cmd=cmd, sudo=True)

    except Exception as e:
        log.error(f"Error : {e}")
    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients)
        log.info("Cleaning up successfull")
    return 0
