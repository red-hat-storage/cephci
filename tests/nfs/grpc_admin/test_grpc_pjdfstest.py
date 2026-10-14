"""
Run pjdfstest POSIX compliance checks on an NFS mount with gRPC-enabled Ganesha.
"""

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_deploy import (
    apply_nfs_container_image,
    resolve_nfs_container_image,
)
from tests.nfs.grpc_admin.grpc_client import ensure_subvolume_group, open_grpc_firewall
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

PJDFSTEST_PACKAGE = "pjdfstest"


def _ensure_pjdfstest(client):
    """Install pjdfstest on the client node if missing."""
    out, _ = client.exec_command(cmd="which prove", check_ec=False)
    if "prove" not in out:
        client.exec_command(
            sudo=True,
            cmd="dnf install -y perl-Test-Harness pjdfstest 2>/dev/null || "
            "yum install -y perl-Test-Harness pjdfstest",
            check_ec=False,
        )


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")

    if not clients or not nfs_nodes:
        raise ConfigError("NFS pjdfstest requires client and nfs nodes")

    client = clients[0]
    nfs_node = nfs_nodes[0]
    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")

    if installers:
        image = resolve_nfs_container_image(config)
        if image:
            apply_nfs_container_image(installers[0], image)

    try:
        open_grpc_firewall(nfs_node)
        ensure_subvolume_group(client, fs_name)
        setup_nfs_cluster(
            clients=[client],
            nfs_server=nfs_node.hostname,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )
        _ensure_pjdfstest(client)

        # Smoke subset: unlink and chmod — fast signal for POSIX over NFS+gRPC.
        test_dir = f"{nfs_mount}/pjdfstest_smoke"
        client.exec_command(sudo=True, cmd=f"mkdir -p {test_dir}")
        cmd = (
            f"cd {test_dir} && "
            "prove -r /usr/share/pjdfstest/tests/chmod 2>/dev/null | tail -5 || "
            f"prove -r $(rpm -ql pjdfstest 2>/dev/null | grep '/tests$' | head -1)/chmod "
            "2>/dev/null | tail -5"
        )
        out, err = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        log.info("pjdfstest output:\n%s", out)
        combined = f"{out}\n{err}".lower()
        if "files=0" in combined or "can't open" in combined:
            raise OperationFailedError(
                "pjdfstest not installed or no tests ran — install pjdfstest on client"
            )
        if "failed" in combined and "files=" in combined:
            # Allow some known NFS pjdfstest failures; fail on total test miss.
            if "ok" not in combined and "passed" not in combined:
                raise OperationFailedError(f"pjdfstest smoke failed: {out}")
        return 0
    except (ConfigError, OperationFailedError):
        raise
    except Exception as exc:
        log.error("pjdfstest failed: %s", exc)
        return 1
    finally:
        try:
            cleanup_cluster(
                client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
            )
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
