import re
from json import loads

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.containers import Container


def run(ceph_cluster, **kw):
    """
    Validate if the ceph orch ps command reflects the version
    for monitoring stack images
    """
    config = kw["config"]
    installer_node = ceph_cluster.get_nodes(role="installer")[0]
    cephadm = CephAdm(installer_node)
    monitoring_images = config.get("monitoring_images")

    # Get the image version from config
    prometheus_img = monitoring_images["ose-prometheus"]
    alertmanager_img = monitoring_images["ose-prometheus-alertmanager"]
    node_exporter_img = monitoring_images["ose-prometheus-node-exporter"]

    # Get the version using podman command
    out = Container(installer_node).run(
        interactive=True,
        rm=True,
        user="root",
        entry_point="/bin/bash",
        image=prometheus_img,
        cmds=' -c "prometheus --version"',
        long_running=False,
    )
    _version = re.search(r"version\s+(\d+\.\d+\.\d+)", out)
    if _version:
        _prom_vers = _version.group(1)
    out = Container(installer_node).run(
        interactive=True,
        tty=True,
        rm=True,
        user="root",
        entry_point="/bin/bash",
        image=alertmanager_img,
        cmds=' -c "alertmanager --version"',
        long_running=False,
    )
    _version = re.search(r"version\s+(\d+\.\d+\.\d+)", out)
    if _version:
        _alert_vers = _version.group(1)
    out = Container(installer_node).run(
        interactive=True,
        tty=True,
        rm=True,
        user="root",
        entry_point="/bin/bash",
        image=node_exporter_img,
        cmds=' -c "node_exporter --version"',
        long_running=False,
    )
    _version = re.search(r"version\s+(\d+\.\d+\.\d+)", out)
    if _version:
        _node_vers = _version.group(1)

    # Validate the ceph orch command displayed the correct version
    services = {
        "prometheus": _prom_vers,
        "alertmanager": _alert_vers,
        "node-exporter": _node_vers,
    }
    for service, _version in services.items():
        out = loads(cephadm.ceph.orch.ps(service_name=service, format="json-pretty"))
    if out[0]["version"] != _version:
        raise OperationFailedError(
            f"Ceph orch is not displaying correct {service} version"
        )
    return 0
