"""
Helpers for gRPC-enabled cephadm NFS deployment in CephCI suites.

Set ``nfs_container_image`` in suite test config to override the NFS daemon
container before ``setup_nfs_cluster()`` runs.

Temporary mTLS workaround: place certs on the NFS node under ``/root/certs``
and set ``deploy_grpc_certs: true`` in test config to copy them into the
cephadm instance path under ``/var/lib/ceph/<fsid>/nfs.<name>.../etc/ganesha/certs``.
"""

import os
import time
from shlex import quote

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    install_grpcurl,
    list_grpc_services,
    verify_grpc_port_listening,
)
from utility.log import Log

log = Log(__name__)

NFS_CONTAINER_IMAGE_KEY = "mgr/cephadm/container_image_nfs"
DEFAULT_GRPC_CERT_SOURCE = "/root/certs"
NFS_GANESHA_CERTS_REL = "etc/ganesha/certs"


def resolve_nfs_container_image(config):
    """Return NFS container image URL from test config, if provided."""
    return config.get("nfs_container_image") or config.get("container_image_nfs")


def apply_nfs_container_image(installer, image):
    """
    Set mgr NFS container image before deploying nfs.* services.

    Args:
        installer: Ceph installer node
        image: Full container image reference (registry/path:tag)
    """
    if not image:
        log.info("No nfs_container_image override in config; using cluster default")
        return

    cmd = (
        f"cephadm shell -- ceph config set mgr "
        f"{NFS_CONTAINER_IMAGE_KEY} {quote(image)}"
    )
    installer.exec_command(sudo=True, cmd=cmd)
    log.info("Set %s to %s", NFS_CONTAINER_IMAGE_KEY, image)


def get_nfs_container_id(installer, nfs_name):
    """Return container_id for the first nfs.<nfs_name> daemon."""
    out = CephAdm(installer).ceph.orch.ps(service_name=f"nfs.{nfs_name}", format="json")
    processes = __import__("json").loads(out) if out else []
    if not processes:
        return None
    return processes[0].get("container_id")


def _host_path_exists(node, path):
    out, _ = node.exec_command(
        sudo=True, cmd=f"test -d {path} && echo yes", check_ec=False
    )
    return "yes" in out


def find_nfs_instance_dirs(nfs_node, nfs_name):
    """
    Locate cephadm NFS instance directories on the NFS node host.

    Example instance dir:
        /var/lib/ceph/<fsid>/nfs.cephfs-nfs.0.0.<hostname>.<id>/

    Returns:
        tuple: (fsid, list of absolute instance directory paths)
    """
    fsids = nfs_node.get_dir_list("/var/lib/ceph", sudo=True)
    if not fsids:
        raise OperationFailedError(
            f"No fsid directory under /var/lib/ceph on {nfs_node.hostname}"
        )

    for fsid in fsids:
        base = f"/var/lib/ceph/{fsid}"
        try:
            entries = nfs_node.get_dir_list(base, sudo=True)
        except Exception:
            continue
        instances = [
            os.path.join(base, entry)
            for entry in entries
            if entry.startswith("nfs.") and nfs_name in entry
        ]
        if instances:
            return fsid, instances

    raise OperationFailedError(
        f"No nfs.{nfs_name} instance directory found under /var/lib/ceph on "
        f"{nfs_node.hostname}"
    )


def copy_grpc_certs_to_nfs_instances(
    nfs_node,
    nfs_name,
    cert_source=DEFAULT_GRPC_CERT_SOURCE,
    restart_nfs=False,
    installer=None,
    restart_sleep=15,
):
    """
    Copy TLS material from a host path into cephadm NFS ganesha cert dirs.

    Cephadm bind-mounts ``<instance>/etc/ganesha/certs`` into the running
    nfs-ganesha container, so updating the host path is sufficient.

    Args:
        nfs_node: Node hosting the nfs.* daemon
        nfs_name: NFS cluster name (e.g. cephfs-nfs)
        cert_source: Host directory containing ca.crt, server.crt, etc.
        restart_nfs: Restart nfs.<nfs_name> after copy so ganesha reloads certs
        installer: Installer node for ``ceph orch restart`` (required if restart)
        restart_sleep: Seconds to wait after restart

    Returns:
        bool: True when at least one instance directory was updated
    """
    if not _host_path_exists(nfs_node, cert_source):
        log.warning(
            "Cert source %s not found on %s — skipping gRPC cert copy",
            cert_source,
            nfs_node.hostname,
        )
        return False

    fsid, instance_dirs = find_nfs_instance_dirs(nfs_node, nfs_name)
    log.info(
        "Copying gRPC certs from %s into %s NFS instance(s) under fsid %s",
        cert_source,
        len(instance_dirs),
        fsid,
    )

    for inst_dir in instance_dirs:
        dest = os.path.join(inst_dir, NFS_GANESHA_CERTS_REL)
        nfs_node.exec_command(sudo=True, cmd=f"mkdir -p {quote(dest)}")
        nfs_node.exec_command(
            sudo=True, cmd=f"cp -af {quote(cert_source)}/. {quote(dest)}/"
        )
        out, _ = nfs_node.exec_command(sudo=True, cmd=f"ls -la {quote(dest)}")
        log.info(
            "Copied certs to %s on %s:\n%s",
            dest,
            nfs_node.hostname,
            out.strip(),
        )

    if restart_nfs:
        if not installer:
            raise OperationFailedError(
                "installer node required to restart nfs service after cert copy"
            )
        restart_cmd = f"cephadm shell -- ceph orch restart nfs.{quote(nfs_name)}"
        installer.exec_command(sudo=True, cmd=restart_cmd)
        log.info(
            "Restarted nfs.%s; waiting %ss for ganesha to reload certs",
            nfs_name,
            restart_sleep,
        )
        time.sleep(restart_sleep)

    return True


def deploy_grpc_certs_if_configured(config, nfs_node, nfs_name, installer=None):
    """
    Copy host certs into NFS container paths when enabled in test config.

    Config keys:
        deploy_grpc_certs (bool): enable copy (default False)
        grpc_cert_source (str): host source dir (default /root/certs)
        restart_nfs_after_cert_copy (bool): restart nfs service (default True)
    """
    if not config.get("deploy_grpc_certs", False):
        return False

    source = config.get("grpc_cert_source", DEFAULT_GRPC_CERT_SOURCE)
    restart = config.get("restart_nfs_after_cert_copy", True)
    return copy_grpc_certs_to_nfs_instances(
        nfs_node,
        nfs_name,
        cert_source=source,
        restart_nfs=restart,
        installer=installer,
        restart_sleep=int(config.get("cert_copy_restart_sleep", 15)),
    )


def verify_grpc_in_nfs_pod(
    nfs_node,
    client_node,
    nfs_ip,
    grpc_port=DEFAULT_GRPC_PORT,
    plaintext=True,
    tls_paths=None,
):
    """
    Verify gRPC port and service discovery on a deployed NFS service.

    Returns:
        bool: True when port is listening and grpcurl lists services
    """
    if not verify_grpc_port_listening(nfs_node, grpc_port):
        return False
    install_grpcurl(client_node)
    target = f"{nfs_ip}:{grpc_port}"
    ok, services = list_grpc_services(
        client_node, target, plaintext=plaintext, tls_paths=tls_paths
    )
    log.info("gRPC services on %s: %s", target, services)
    return ok and bool(services)
