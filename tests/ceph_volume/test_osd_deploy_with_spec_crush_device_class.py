import json

from cephci.utils.configs import get_configs
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import UnexpectedStateError
from cli.utilities.operations import wait_for_cluster_health
from cli.utilities.utils import create_yaml_config


def run(ceph_cluster, **kw):
    """Deploy OSDs using DriveGroup spec crush_device_class"""
    # Get configs
    get_configs()
    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(installer)

    # Generate a yaml file
    file = create_yaml_config(installer, config)

    # Create OSDs with yaml file
    c = {"pos_args": [], "input": file}
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c)

    # Wait for cluster health to be ok
    wait_for_cluster_health(node=installer, status="HEALTH_OK")

    # Verify crush_device_class for all OSDs is set to ssd
    kw = {"format": "json-pretty"}
    out = cephadm.ceph.osd.tree(**kw)
    data = json.loads(out)
    for node in data["nodes"]:
        _type = node.get("type")
        _class = node.get("device_class")
        if _type == "osd" and _class != "ssd":
            raise UnexpectedStateError("crush_device_class is not set to ssd for OSD")
        return 0
