"""Install the RGW reference certificate into the client trust store.

The certificate is taken from the Beast config-key. Only the certificate is
copied. The private key stays in the config-key for the RGW daemon.
"""

import re

from copy_cephadm_root_ca_cert import install_cephadm_root_ca_cert
from utility.log import Log

LOG = Log(__name__)

REFERENCE_CERT_FILE = "rgw-reference.crt"
CONFIG_KEY = "rgw/cert/rgw.rgw.ssl"
ROLES = ("client",)
PORT = 443


def get_reference_cert(installer, config_key=CONFIG_KEY):
    """Return the certificate from an RGW config-key, without the private key."""
    out, _ = installer.exec_command(
        cmd=f"cephadm shell -- ceph config-key get {config_key}",
        sudo=True,
    )
    match = re.search(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        out,
        re.DOTALL,
    )
    if not match:
        raise ValueError(f"{config_key} does not contain a certificate")
    return match.group(0) + "\n"


def copy_reference_cert_to_roles(cluster, config_key=CONFIG_KEY, roles=ROLES):
    """Install the RGW reference certificate into the system trust store."""
    installer = cluster.get_nodes(role="installer")[0]
    cert = get_reference_cert(installer, config_key)
    seen = set()
    for role in roles:
        for node in cluster.get_nodes(role=role):
            if node.hostname in seen:
                continue
            seen.add(node.hostname)
            LOG.info("Copying RGW reference certificate to %s", node.hostname)
            install_cephadm_root_ca_cert(node, cert, cert_name=REFERENCE_CERT_FILE)


def confirm_client_trusts_endpoint(cluster, port=PORT):
    """Fail unless the client accepts the RGW certificate without -k."""
    client = cluster.get_nodes(role="client")[0]
    rgw = cluster.get_nodes(role="rgw")[0]
    url = f"https://{rgw.ip_address}:{port}"
    LOG.info("Checking %s trusts %s", client.hostname, url)
    cmd = (
        "bash -c '"
        "for i in $(seq 1 6); do "
        f"curl -sS --connect-timeout 10 -o /dev/null {url} && exit 0; "
        "sleep 5; "
        "done; "
        "exit 1'"
    )
    client.exec_command(sudo=True, cmd=cmd)


def run(ceph_cluster, **kwargs):
    """Copy the reference certificate to the client and verify HTTPS."""
    try:
        copy_reference_cert_to_roles(ceph_cluster)
        confirm_client_trusts_endpoint(ceph_cluster)
    except Exception as exc:
        LOG.error("Failed to trust the RGW reference certificate: %s", exc)
        return 1
    return 0
