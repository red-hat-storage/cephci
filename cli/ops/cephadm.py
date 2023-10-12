from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError

CONTAINER_IMAGE_CONFIG = "mgr/cephadm/container_image_{}"
CONTAINER_IMAGES = [
    "grafana",
    "keepalived",
    "haproxy",
    "prometheus",
    "node_exporter",
    "alertmanager",
    "promtail",
    "snmp_gateway",
    "loki",
    "nvmeof",
]


def set_container_image_config(node, configs):
    """Set container image config

    Args:
        node (CephInstallerNode): Ceph installer node
        kw (list): List of string with 'image=URL' format
            Supported images:
                grafana: grafana image URL
                keepalived: keepalived image URL
                haproxy: haproxy image URL
                prometheus: prometheus image URL
                node_exporter: node exporter image URL
                alertmanager: alertmanager image URL
                promtail: promtail image URL
                snmp_gateway: snmp gateway image URL
                loki: loki image URL
                nvmeof: nvmeof image URL
    """
    # Set CephAdm object
    cephadm = CephAdm(node)

    for config in configs:
        # Check for possible images
        k, v = config.split("=", 1)
        k = k.replace("_image", "")
        if k not in CONTAINER_IMAGES:
            continue

        # Set mgr configs
        out, _ = cephadm.ceph.config.set(
            daemon="mgr", key=CONTAINER_IMAGE_CONFIG.format(k), value=v
        )
        if "Error EINVAL" in out:
            raise OperationFailedError(
                f"Failed to set container image config for image '{v}'"
            )

    return True


def bootstrap(node, **kw):
    """Bootstrap cluster

    Args:
        node (CephInstallerNode): Ceph installer node
        kw (dict): Key/Value pairs to be provided to bootstrap
            Supported keys:
                fsid (str): Cluster FSID
                image (str): Container image
                yes_i_know (bool): Flag to set option `yes-i-know`
                registry-url (str): URL for custom registry
                registry-username (str): Username for custom registry
                registry-password (str): Password for custom registry
                registry-json (str): json file with custom registry login info
                mon-ip (str): Mon IP address
                config (str): Ceph conf file to incorporate
                skip-dashboard (str): Do not enable the Ceph Dashboard
                initial-dashboard-user (str): Initial user for the dashboard
                initial-dashboard-password (str): Initial password for the initial dashboard user
                allow-overwrite (str): Allow overwrite of existing --output-* config/keyring/ssh files
                allow-fqdn-hostname (str): Allow hostname that is fully-qualified
                output-dir (str): Directory to write config, keyring, and pub key files
                apply-spec (str): Apply cluster spec after bootstrap
                cluster-network (str): Subnet to use for cluster replication, recovery and heartbeats
    """
    # Get ceph image
    image = kw.pop("image") if kw.get("image") else None

    # Get yes-i-know tag
    yes_i_know = kw.pop("yes-i-know") if kw.get("yes-i-know") else None

    # Bootstrap ceph cluster
    if CephAdm(node).bootstrap(image=image, yes_i_know=yes_i_know, **kw):
        raise OperationFailedError("Failed to bootstrap cluster")

    return True
