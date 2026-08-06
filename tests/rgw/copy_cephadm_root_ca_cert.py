"""Copy cephadm root CA cert to RGW and client nodes without sshpass."""

import os
import re

from utility.log import Log

LOG = Log(__name__)

CEPHADM_ROOT_CA_FILE = "cephadm-root-ca.crt"
CA_TRUST_ANCHORS = "/etc/pki/ca-trust/source/anchors"


def get_cephadm_root_ca_cert(installer):
    """Fetch the cephadm root CA certificate from the cluster."""
    version_out, _ = installer.exec_command(
        cmd="cephadm shell -- ceph version", sudo=True
    )
    ceph_version = ""
    match = re.search(r"ceph version (\S+)", version_out)
    if match:
        ceph_version = match.group(1).split("-")[0]

    if ceph_version == "19.2.0":
        cmd = "cephadm shell -- ceph orch cert-store get cert cephadm_root_ca_cert"
    else:
        cmd = "cephadm shell -- ceph orch certmgr cert get cephadm_root_ca_cert"

    cert, _ = installer.exec_command(cmd=cmd, sudo=True)
    return cert


def install_cephadm_root_ca_cert(node, cert_content, cert_name=CEPHADM_ROOT_CA_FILE):
    """Install cephadm root CA cert on a node using remote_file."""
    if node.pkg_type == "deb":
        cert_path = f"/usr/local/share/ca-certificates/{cert_name}"
        update_cmd = "update-ca-certificates"
    else:
        cert_path = f"{CA_TRUST_ANCHORS}/{cert_name}"
        update_cmd = "update-ca-trust extract"

    node.exec_command(sudo=True, cmd=f"mkdir -p {os.path.dirname(cert_path)}")
    cert_file = node.remote_file(sudo=True, file_name=cert_path, file_mode="w")
    cert_file.write(cert_content)
    cert_file.flush()
    cert_file.close()
    node.exec_command(sudo=True, cmd=update_cmd)


def copy_cephadm_root_ca_cert_to_roles(cluster, roles=("rgw", "client")):
    """Copy cephadm root CA cert to nodes matching the given roles."""
    installer = cluster.get_nodes(role="installer")[0]
    cert = get_cephadm_root_ca_cert(installer)
    seen = set()
    for role in roles:
        for node in cluster.get_nodes(role=role):
            if node.hostname in seen:
                continue
            seen.add(node.hostname)
            LOG.info("Copying cephadm root CA cert to %s", node.hostname)
            install_cephadm_root_ca_cert(node, cert)


def copy_peer_cas_to_roles(cluster, ceph_cluster_dict, roles=("rgw", "client")):
    """Install peer cluster cephadm root CAs for multisite SSL trust."""
    if len(ceph_cluster_dict) <= 1:
        return

    for peer_name, peer_cluster in ceph_cluster_dict.items():
        if peer_cluster.name == cluster.name:
            continue
        peer_installer = peer_cluster.get_nodes(role="installer")[0]
        cert = get_cephadm_root_ca_cert(peer_installer)
        cert_name = f"cephadm-root-ca-{peer_name}.crt"
        seen = set()
        for role in roles:
            for node in cluster.get_nodes(role=role):
                if node.hostname in seen:
                    continue
                seen.add(node.hostname)
                LOG.info(
                    "Copying peer %s cephadm root CA cert to %s",
                    peer_name,
                    node.hostname,
                )
                install_cephadm_root_ca_cert(node, cert, cert_name=cert_name)


def run(ceph_cluster, **kwargs):
    """
    Copy cephadm root CA cert to nodes by role.

    Run after client setup in SSL RGW suites.

    Config keys:
        roles: list of node roles (default: rgw, client)
    """
    config = kwargs.get("config", {})
    roles = config.get("roles", ["rgw", "client"])
    ceph_cluster_dict = kwargs.get("ceph_cluster_dict", {})
    try:
        copy_cephadm_root_ca_cert_to_roles(ceph_cluster, roles=roles)
        copy_peer_cas_to_roles(ceph_cluster, ceph_cluster_dict, roles=roles)
    except Exception as exc:
        LOG.error("Failed to copy cephadm root CA cert: %s", exc)
        return 1
    return 0
