import json

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, UnexpectedStateError
from cli.utilities.operations import wait_for_cluster_health
from cli.utilities.utils import create_yaml_config


def run(ceph_cluster, **kw):
    """Deploy OSDs using DriveGroup spec crush_device_class"""
    config = kw.get("config") or {}
    specs = config.get("spec") or {}
    expected_class = specs.get("crush_device_class") or specs.get("spec", {}).get(
        "crush_device_class"
    )
    if not expected_class:
        raise ConfigError("OSD spec must define 'crush_device_class'")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(installer)

    # Generate a yaml file
    file = create_yaml_config(installer, specs)

    # Create OSDs with yaml file
    c = {"pos_args": [], "input": file, "check_ec": True}
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)

    # Wait for cluster health to be ok
    wait_for_cluster_health(node=installer, status="HEALTH_OK")

    # Verify the configured device class for every OSD, including after buckets.
    kw = {"format": "json-pretty"}
    out = cephadm.ceph.osd.tree(**kw)
    data = json.loads(out)
    osds = [node for node in data.get("nodes", []) if node.get("type") == "osd"]
    if not osds:
        raise UnexpectedStateError("No OSDs found after applying the OSD spec")
    for node in osds:
        actual_class = node.get("device_class")
        if actual_class != expected_class:
            osd_name = node.get("name", node.get("id"))
            raise UnexpectedStateError(
                f"crush_device_class for {osd_name} is {actual_class!r}; "
                f"expected {expected_class!r}"
            )
    return 0
