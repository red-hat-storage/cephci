from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
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
        )

        # Create a file on Client 1
        cmd = (
            "python3 -m pip install ply;"
            f"cd {nfs_mount};"
            "git clone git://git.linux-nfs.org/projects/bfields/pynfs.git;cd pynfs;-- "
            f"yes |"
            f"python setup.py build;cd nfs{version};./testserver.py {nfs_server_name}:{nfs_export}_0 -v --outfile "
            f"~/pynfs.run --maketree --showomit --rundep all"
        )

        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=600)
        if "FailureException" in out:
            OperationFailedError(f"Failed to run {cmd} on {clients[0].hostname}")

        # Parse test output to detect failures
        failed_tests = []
        allowed_failures = {"EID9", "SEQ6"}

        for line in out.splitlines():
            line = line.strip()
            if line.endswith(": FAILURE"):
                test_id = line.split()[0]
                if test_id not in allowed_failures:
                    failed_tests.append(test_id)

        if failed_tests:
            log.error(f"Unexpected pynfs test failures: {failed_tests}")
            return 1

    except Exception as e:
        log.error(f"Failed to run pynfs on {clients[0].hostname}, Error: {e}")
        return 1

    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
