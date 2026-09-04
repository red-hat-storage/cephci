import json
from time import sleep

from nfs_operations import (
    cleanup_cluster,
    get_nfs_cluster_backend_endpoint,
    setup_nfs_cluster,
    wait_for_nfs_endpoint_ready,
)

from ceph.ceph import Ceph
from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import reboot_node
from utility.log import Log

log = Log(__name__)


def _inode_or_none(client, path):
    """Return inode as string, or None if ls times out / fails (hard-mount hang)."""
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"timeout 30 ls -i {path} | awk '{{print $1}}'",
            check_ec=False,
            timeout=45,
        )
    except Exception:
        return None
    if int(getattr(client, "exit_status", 1)) != 0:
        return None
    inode = (out or "").strip()
    return inode or None


def _lazy_unmount(clients, nfs_mount):
    """Drop a stuck hard NFS mount so cleanup rm does not block."""
    for client in clients:
        client.exec_command(
            sudo=True,
            cmd=f"umount -f -l {nfs_mount} >/dev/null 2>&1 || true",
            check_ec=False,
            timeout=30,
        )


def _nfs_backend_endpoint(client, nfs_name, nfs_node, port):
    """Best-effort backend IP:port for TCP probes after reboot (no orch ps wait)."""
    try:
        raw = Ceph(client).nfs.cluster.info(nfs_name)
        if isinstance(raw, str):
            info = json.loads(raw)
        else:
            info = raw or {}
        backend_ip, backend_port = get_nfs_cluster_backend_endpoint(info, nfs_name)
        return backend_ip, str(backend_port)
    except Exception as exc:
        log.info(
            "Using NFS node IP for post-reboot probe (%s); cluster info unavailable: %s",
            nfs_node.hostname,
            exc,
        )
        return nfs_node.ip_address, str(port)


def run(ceph_cluster, **kw):
    """Verify hard links remain valid after an NFS server reboot.
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
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
            enable_rdma=config.get("enable_rdma", False),
            rdma_port=config.get("rdma_port"),
        )

        # Drop leftovers from a prior reuse run so ln is not EEXIST.
        clients[0].exec_command(
            sudo=True,
            cmd=f"rm -f {nfs_mount}/test_file {nfs_mount}/link_file",
            check_ec=False,
        )

        # Create file in local file system
        cmd = f"touch {nfs_mount}/test_file"
        clients[0].exec_command(cmd=cmd)

        # Create hard links
        cmd = f"ln {nfs_mount}/test_file {nfs_mount}/link_file"
        clients[0].exec_command(cmd=cmd)

        # Reboot NFS server
        reboot_node(nfs_node)

        # SSH reconnect is not enough: a hard NFS mount blocks until Ganesha
        # listens again. Do not wait on ``ceph orch ps`` here — mgr cache
        # defaults to ~10 minutes and keeps "host is offline" after the node
        # is back (https://docs.ceph.com/en/latest/cephadm/services/).
        backend_ip, backend_port = _nfs_backend_endpoint(
            clients[0], nfs_name, nfs_node, port
        )
        wait_for_nfs_endpoint_ready(clients[0], backend_ip, backend_port, timeout=600)

        original_file_inode = None
        hard_link_file_inode = None
        for _ in WaitUntil(timeout=300, interval=10):
            original_file_inode = _inode_or_none(clients[0], f"{nfs_mount}/test_file")
            hard_link_file_inode = _inode_or_none(clients[0], f"{nfs_mount}/link_file")
            if original_file_inode and hard_link_file_inode:
                break
            log.info(
                "NFS mount not readable yet after reboot (grace/reconnect); retrying"
            )

        if not original_file_inode or not hard_link_file_inode:
            raise OperationFailedError(
                "timed out waiting for NFS mount to recover after server reboot"
            )
        if original_file_inode != hard_link_file_inode:
            raise OperationFailedError(
                "hard link file not have same inode as original file"
            )
        log.info(
            "TEST PASSED - hard link inodes match after NFS server reboot "
            "(CEPH-83575971)"
        )
        return 0
    except Exception as e:
        log.error("Error : %s", e)
        return 1
    finally:
        log.info("Cleaning up")
        sleep(3)
        try:
            _lazy_unmount(clients, nfs_mount)
            cleanup_cluster(
                clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
            )
            log.info("Cleaning up successful")
        except Exception as cleanup_err:
            log.warning("Cleanup after hard-links reboot test failed: %s", cleanup_err)
