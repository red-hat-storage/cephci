"""
Regional FS parity — EIT
TLS/stunnel NFS deployed; plain (non-TLS) mount must fail.
"""

import traceback

from tests.nfs.security.security_utils import (
    assert_mount_failed,
    attempt_nfs_mount_expect_failure,
    deploy_tls_nfs_cluster_with_certs,
    full_tls_stack_cleanup,
    generate_tls_cert_bundle,
    mount_export_via_stunnel,
    setup_stunnel_client,
    stop_stunnel_client,
    verify_ceph_orch_nfs_running,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    stunnel_started = False
    client = None
    nfs_name = None
    tls_mount = None
    plain_mount = None
    try:
        config = kw.get("config", {})
        installer = ceph_cluster.get_nodes(role="installer")[0]
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        clients = ceph_cluster.get_nodes("client")
        client = clients[0]
        nfs_node = nfs_nodes[0]
        nfs_name = config.get("nfs_name", "cephfs-nfs-eit")
        fs_name = config.get("fs_name", "cephfs")
        export = config.get("nfs_export", "/export_eit")
        tls_mount = config.get("tls_mount", "/mnt/nfs_eit_tls")
        plain_mount = config.get("plain_mount", "/mnt/nfs_eit_plain")
        nfs_port = int(config.get("nfs_port", 2049))
        stunnel_port = int(config.get("stunnel_accept_port", 49152))
        nfs_version = str(config.get("nfs_version", "4.2"))

        cert_key, cert, ca_cert = generate_tls_cert_bundle(nfs_node)
        deploy_tls_nfs_cluster_with_certs(
            installer,
            nfs_node,
            nfs_name,
            cert_key,
            cert,
            ca_cert,
            tls_ktls=bool(config.get("tls_ktls", True)),
        )
        verify_ceph_orch_nfs_running(installer, nfs_name)

        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph nfs export create cephfs {nfs_name} {export} {fs_name} / "
                f"|| true"
            ),
        )

        setup_stunnel_client(
            client,
            ca_cert,
            nfs_node.ip_address,
            server_port=nfs_port,
            accept_port=stunnel_port,
        )
        stunnel_started = True
        mount_export_via_stunnel(
            client, export, tls_mount, nfs_version, stunnel_port
        )
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={tls_mount}/ok.bin bs=1M count=1 status=none",
        )
        log.info("TLS/stunnel mount succeeded")

        client.exec_command(sudo=True, cmd=f"mkdir -p {plain_mount}")
        plain_cmd = (
            f"mount -t nfs -o vers={nfs_version},proto=tcp,port={nfs_port} "
            f"{nfs_node.ip_address}:{export} {plain_mount}"
        )
        result = attempt_nfs_mount_expect_failure(client, plain_cmd)
        assert_mount_failed(
            result, "cleartext mount against EIT NFS", client, plain_mount
        )
        log.info("EIT cleartext reject passed")
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("EIT reject-cleartext failed: %s", exc)
        return 1
    finally:
        try:
            if stunnel_started and client:
                stop_stunnel_client(client)
        except Exception:  # noqa: BLE001
            pass
        try:
            if client and nfs_name:
                full_tls_stack_cleanup(
                    client,
                    nfs_name,
                    mount_candidates=[tls_mount, plain_mount],
                )
        except Exception:  # noqa: BLE001
            pass
