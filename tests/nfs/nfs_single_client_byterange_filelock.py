from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from ceph.ceph import CommandFailed
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

        log.info("=== Running byte-range locking test ===")

        result = run_single_byte_range_test(clients[0], mount_path)

        if not result["success"]:
            raise Exception("Lock test failed: %s" % result["error"])

        log.info("Byte-range lock test PASSED.")
        rc = 0

    except (CommandFailed, Exception) as e:
        log.error("Test failed: %s" % e)

    finally:
        log.info("Cleaning up exports and NFS cluster...")
        cleanup_cluster(clients, mount_path, nfs_name, nfs_export)

    return rc


def run_single_byte_range_test(client, mount_path):
    """
    Uploads, chmods, runs and interprets the single_client_byte_range.py script.
    """

    script_src = "tests/nfs/scripts_tools/single_client_byte_range.py"
    script_dst = "/root/single_client_byte_range.py"

    client.exec_command(cmd="mkdir -p %s" % mount_path, sudo=True)

    log.info("Uploading byte-range lock test script to client...")
    client.upload_file(sudo=True, src=script_src, dst=script_dst)
    client.exec_command(sudo=True, cmd="chmod +x %s" % script_dst)

    log.info("Executing byte-range lock script...")

    cmd = "python3 %s %s" % (script_dst, mount_path)
    out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=300)

    log.debug("Script STDOUT:\n%s" % out)
    log.debug("Script STDERR:\n%s" % err)

    # Detect success/failure based on script's output
    if "Lock acquired successfully" in out:
        return {
            "success": True,
            "response": "Byte-range lock acquired",
            "error": None,
        }

    if "Failed to acquire lock" in out:
        return {
            "success": False,
            "response": None,
            "error": "Lock already held by another process",
        }

    return {
        "success": False,
        "response": None,
        "error": "Unexpected script output",
    }
