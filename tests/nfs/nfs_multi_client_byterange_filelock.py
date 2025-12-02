from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]

    if not nfs_nodes or not clients:
        raise ConfigError("Missing NFS nodes or clients in cluster")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    required_clients = int(config.get("clients", "2"))

    if required_clients > len(clients):
        raise ConfigError("Requested more clients than available")

    clients = clients[:required_clients]
    nfs_node = nfs_nodes[0]

    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    mount_path = "/mnt/nfs"

    rc = 1

    try:
        log.info("=== Setting up NFS Cluster ===")
        setup_nfs_cluster(
            clients,
            nfs_node.hostname,
            port,
            version,
            nfs_name,
            mount_path,
            fs_name,
            nfs_export,
            fs_name,
            ceph_cluster=ceph_cluster,
        )

        if version == 3:
            enable_v3_locking(installer, nfs_name, nfs_node, nfs_node.hostname)

        log.info("=== Running byte-range locking test on 2 clients ===")
        result = run_multi_client_byte_range_test(clients, mount_path)

        if not result["success"]:
            raise Exception("Lock test failed: %s" % result["error"])

        log.info("Byte-range lock test with multiple clients PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_multi_client_byte_range_test(clients, mount_path):
    """
    Upload multi-client locker script to all given clients and run simultaneously.
    """

    script_src = "tests/nfs/scripts_tools/multi_client_byte_range.py"
    script_dst = "/root/multi_client_byte_range.py"

    # Upload script to all clients
    for c in clients:
        log.info("Uploading multi-client byte-range script to %s..." % c.hostname)
        c.upload_file(sudo=True, src=script_src, dst=script_dst)
        c.exec_command(sudo=True, cmd=f"chmod +x {script_dst}")

    LOCK_START = 10
    LOCK_LEN = 100
    cmd = "python3 %s %s %s %s" % (script_dst, mount_path, LOCK_START, LOCK_LEN)

    log.info(
        "Starting multi-client byte-range lock test on %s clients..." % len(clients)
    )

    all_outputs = []

    with parallel() as p:
        for c in clients:
            p.spawn(c.exec_command, sudo=True, cmd=cmd, check_ec=False)

        for result in p:
            all_outputs.append(result)

    decoded_outs = []
    for idx, (out, err) in enumerate(all_outputs):
        out = out.decode() if isinstance(out, bytes) else out
        decoded_outs.append(out)
        log.debug("[Client %s Output]\n%s" % (idx + 1, out))

    for i, out in enumerate(decoded_outs):
        if "Lock acquired" not in out:
            return {
                "success": False,
                "error": "Client %s never acquired lock." % (i + 1),
            }

    waited_clients = [
        i
        for i, out in enumerate(decoded_outs)
        if "retry" in out.lower() or "held by another client" in out.lower()
    ]

    if len(clients) > 1 and len(waited_clients) == 0:
        return {
            "success": False,
            "error": "Expected clients to wait, but no retries detected.",
        }

    return {
        "success": True,
        "response": "All %s clients successfully acquired lock sequentially."
        % len(clients),
        "error": None,
    }
