from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config


def run(ceph_cluster, **kw):
    """Verify support to customize daemon container cpu limit"""

    config = kw.get("config")

    # Get the installer and OSD nodes
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Create spec for unmanaged OSDs
    specs = config.get("spec")
    file = create_yaml_config(installer, specs)

    # Update OSD as unmanged
    c = {"pos_args": [], "input": file}
    if CephAdm(nodes=installer, mount=file).ceph.orch.apply(**c):
        raise OperationFailedError("Failed to set daemon container cpu limit.")

    # Verify the apply operation was successful
    conf = {"service-name": "osd.all-available-devices", "format": "yaml"}
    out = CephAdm(nodes=installer).ceph.orch.ls(**conf)
    if not out:
        raise OperationFailedError("Failed to set daemon container cpu limit.")

    return 0
