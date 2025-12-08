import re

from nfs_operations import cleanup_cluster, enable_v3_locking, setup_nfs_cluster

from ceph.ceph import CommandFailed
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    1. Setup NFS cluster
    2. Create lock export and mount
    3. Clone and run nfstest_lock suite
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]

    if not nfs_nodes or not clients:
        raise ConfigError("NFS nodes or clients missing in cluster")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", "2"))

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    nfs_server_name = nfs_node.hostname

    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_lock_export = "/nfs_lock_export"

    if version == 3:
        enable_v3_locking(installer, nfs_name, nfs_node, nfs_server_name)

    nfstest_repo = "git://git.linux-nfs.org/projects/mora/nfstest.git"
    nfstest_dir = "/root/nfstest"
    nfstest_lock = "%s/test/nfstest_lock" % nfstest_dir
    log_file = "%s/nfstest.log" % nfstest_dir

    try:
        log.info("=== NFS Cluster Setup ===")
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

        log.info("=== Create lock export ===")
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_lock_export,
            fs=fs_name,
        )

        log.info("=== Install & run nfstest ===")
        client = clients[0]  # run suite from client0
        client.exec_command(
            cmd="dnf install -y git python3 python3-devel tcpdump wireshark firewalld",
            sudo=True,
        )

        client.exec_command(
            cmd="if [ -d %s ]; then rm -rf %s; fi" % (nfstest_dir, nfstest_dir),
            sudo=True,
        )
        client.exec_command(
            cmd="git clone %s %s" % (nfstest_repo, nfstest_dir), sudo=True
        )
        client.exec_command(cmd="export PYTHONPATH=%s/nfstest" % nfstest_dir, sudo=True)
        client.exec_command(cmd="ls %s" % nfstest_lock, sudo=True)

        test_cmd = (
            "cd %s && "
            "export PYTHONPATH=%s:%s/nfstest:%s/lib && "
            "%s --server %s "
            "--export %s "
            "--nfsversion %s "
            "> %s 2>&1"
            % (
                nfstest_dir,
                nfstest_dir,
                nfstest_dir,
                nfstest_dir,
                nfstest_lock,
                nfs_server_name,
                nfs_lock_export,
                version,
                log_file,
            )
        )
        client.exec_command(cmd=test_cmd, sudo=True, timeout=2400)

        result = validate_nfstest_log(client=clients[0], log_path=log_file)

        if result != 0:
            log.error("NFSTEST Locking test failed")
            return 1

        log.info("NFSTEST Locking test succeeded")
        return 0

    except CommandFailed as e:
        log.error("NFSTEST command failed: %s" % e)
        return 1
    except Exception as e:
        log.error("Unexpected failure: %s" % e)
        return 1
    finally:
        log.info("Clean up: unmount exports & cleanup")
        Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_lock_export)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)


def setup_passwordless_ssh(from_client, to_client):
    ip = to_client.ip_address

    from_client.exec_command(
        cmd="mkdir -p /root/.ssh && chmod 700 /root/.ssh && "
        "test -f /root/.ssh/id_rsa || ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa",
        sudo=True,
    )

    result = from_client.exec_command(
        cmd="cat /root/.ssh/id_rsa.pub",
        sudo=True,
    )
    pub_key = result[0].strip() if isinstance(result, tuple) else result.strip()

    to_client.exec_command(
        cmd="mkdir -p /root/.ssh && chmod 700 /root/.ssh && "
        f'grep -qxF "{pub_key}" /root/.ssh/authorized_keys || '
        f'echo "{pub_key}" >> /root/.ssh/authorized_keys && '
        "chmod 600 /root/.ssh/authorized_keys",
        sudo=True,
    )

    from_client.exec_command(
        cmd="ssh-keyscan -H %s >> /root/.ssh/known_hosts" % ip,
        sudo=True,
    )


def validate_nfstest_log(client, log_path="nfstest.log"):
    log.info("Validating NFSTEST log file: %s" % log_path)

    out, _ = client.exec_command(cmd="cat %s" % log_path, sudo=True)
    summary_regex = r"(\d+)\s+tests\s+\((\d+)\s+passed,\s+(\d+)\s+failed\)"
    match = re.search(summary_regex, out)
    if not match:
        log.error("NFSTEST summary line not found in log file!")
        return 1

    total_tests = int(match.group(1))
    passed = int(match.group(2))
    failed = int(match.group(3))

    log.info(
        "NFSTEST Summary: %s tests (%s passed, %s failed)"
        % (total_tests, passed, failed)
    )
    if failed > 0:
        log.error("NFSTEST detected failures — failing test case!")
        return 1

    log.info("NFSTEST: All tests passed successfully.")
    return 0
