import shlex

from looseversion import LooseVersion
from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Run pynfs testserver against the NFS export and check results.

    On Ceph >= 20.2, runs ``ceph nfs export update ... rw`` for delegation export
    semantics. Failures are matched against defaults plus version rules: all ``DELEG*``
    allowed when Ceph is unknown or < 20.2; all ``ALLOC*`` allowed when NFS minor is
    < 4.2 (e.g. Ganesha may not support OP_ALLOCATE on older exports).

    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    # YAML often parses 4.1 as float; ``version == "4.1"`` would be False.
    pynfs_nfs_minor = str(version).strip()
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
    pynfs_workdir = None

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

        ceph_ver = None
        try:
            ceph_ver = get_ceph_version_from_cluster(clients[0])
        except Exception as exc:
            log.warning("Could not get ceph version from cluster: %s", exc)

        # Allowed failures for pynfs
        allowed_exact_failures = {"EID9", "SEQ6", "DELEG7"}
        allowed_prefixes_failures = set()

        # Checking if the Cluster supports delegation
        # Delegation is supported only on Ceph versions >= 20.2.x
        if bool(ceph_ver) and LooseVersion(ceph_ver) >= LooseVersion("20.2"):

            log.info("Running ceph nfs export update for ceph %s (>= 20.2.", ceph_ver)
            # Also set the export Delegatio to rw
            clients[0].exec_command(
                cmd=f"ceph nfs export update {nfs_name} {nfs_export}_0 rw",
                sudo=True,
            )
        else:
            allowed_prefixes_failures.add("DELEG")

        # Only if the NFS version is < 4.2, we allow the ALLOC failures
        if LooseVersion(pynfs_nfs_minor) < LooseVersion("4.2"):
            allowed_prefixes_failures.add("ALLOC")

        log.info(f"Allowed failures: {allowed_prefixes_failures}")
        log.info(f"Allowed exact failures: {allowed_exact_failures}")

        # Creating a temporary directory for pynfs
        out_mk, _ = clients[0].exec_command(
            cmd="mktemp -d /tmp/cephci-pynfs.XXXXXX",
            sudo=True,
        )
        pynfs_workdir = out_mk.strip().splitlines()[-1].strip()
        if not pynfs_workdir.startswith("/tmp/cephci-pynfs."):
            raise OperationFailedError(
                f"Refusing to use unexpected mktemp path: {pynfs_workdir!r}"
            )

        log.info(f"Setting up and running pynfs on {clients[0].hostname}")

        repo = f"{pynfs_workdir}/pynfs"
        qrepo = shlex.quote(repo)
        # Isolate clone/build under /tmp (unique per run); testserver targets the NFS export over the network.
        cmd = (
            "dnf install -y python3-ply && "
            f"git clone https://github.com/s-athani/pynfs.git {qrepo} && "
            # f"git clone git://git.linux-nfs.org/projects/cdmackay/pynfs.git {qrepo} && "
            f"cd {qrepo} && "
            "yes | python setup.py build && "
            f"cd {qrepo}/nfs{pynfs_nfs_minor} && "
            f"./testserver.py {nfs_server_name}:{nfs_export}_0 -v --outfile "
            f"~/pynfs.run --maketree --showomit --rundep all"
        )

        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=600)
        log.info(f"Pynfs Output:\n {out}")

        if "FailureException" in out:
            raise OperationFailedError(f"Failed to run {cmd} on {clients[0].hostname}")

        # Parse test output to detect failures
        failed_tests = []

        # Check for allowed failures
        for line in out.splitlines():
            line = line.strip()
            if not line.endswith(": FAILURE"):
                continue
            test_id = line.split()[0]
            if test_id in allowed_exact_failures:
                log.info(f"Skipping {test_id} as it is an allowed failure")
                continue
            if any(test_id.startswith(p) for p in allowed_prefixes_failures):
                log.info(f"Skipping {test_id} as it is an allowed failure")
                continue
            failed_tests.append(test_id)

        if failed_tests:
            log.error(
                "Unexpected pynfs test failures: %s",
                sorted(set(failed_tests)),
            )
            return 1

        log.info("================================================")
        log.info("Pynfs test completed successfully")
        log.info("================================================")
        return 0

    except Exception as e:
        log.error(f"Failed to run pynfs on {clients[0].hostname}, Error: {e}")
        return 1

    finally:
        if pynfs_workdir:
            wd = shlex.quote(pynfs_workdir)
            try:
                clients[0].exec_command(
                    cmd=f"rm -rf -- {wd}",
                    sudo=True,
                    check_ec=False,
                )
            except Exception as exc:
                log.warning("Could not remove pynfs workdir %s: %s", pynfs_workdir, exc)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleaning up successful")
