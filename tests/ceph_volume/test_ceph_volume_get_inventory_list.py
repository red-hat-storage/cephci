from collections.abc import Mapping

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.containers import Container
from tests.ceph_volume.utils import parse_inventory_json


def get_inventory_list(node, ceph_image, output_format):
    """Run inventory using the selected test image and require a successful exit."""
    cmd = f"--cluster ceph inventory --format={output_format}"
    volumes = [
        "/run/lock/lvm:/run/lock/lvm:z",
        "/var/run/udev/:/var/run/udev/:z",
        "/dev:/dev",
        "/run/lvm/:/run/lvm/",
        "/var/lib/ceph/:/var/lib/ceph/:z",
        "/var/log/ceph/:/var/log/ceph/:z",
    ]
    out = Container(node).run(
        rm=True,
        privileged=True,
        user="root",
        cmds=cmd,
        entry_point="ceph-volume",
        volume=volumes,
        image=ceph_image,
        long_running=False,
    )
    status = getattr(node, "exit_status", None)
    if type(status) is not int or status != 0:
        raise OperationFailedError(
            f"ceph-volume inventory --format={output_format} failed on {node.hostname}; "
            f"exit status: {status}; output: {out}"
        )
    return out


def run(ceph_cluster, **kw):
    """Verify plain and JSON inventory on bootstrap and non-bootstrap OSD hosts."""
    config = kw.get("config")
    if not isinstance(config, Mapping):
        raise ConfigError("Ceph-volume inventory config must be provided")
    ceph_image = (
        config.get("ceph_image")
        or config.get("container_image")
        or kw.get("container_image")
    )
    if not isinstance(ceph_image, str) or not ceph_image.strip():
        raise ConfigError(
            "Ceph-volume inventory requires ceph_image or container_image"
        )

    installers = ceph_cluster.get_nodes(role="installer")
    if not installers:
        raise ConfigError("Ceph-volume inventory requires a bootstrap installer node")
    installer = installers[0]

    non_bs_nodes = [
        node for node in ceph_cluster.get_nodes(role="osd") if node != installer
    ]
    if not non_bs_nodes:
        raise ConfigError("Ceph-volume inventory requires a non-bootstrap OSD node")
    for node in (installer, non_bs_nodes[0]):
        plain_output = get_inventory_list(node, ceph_image, output_format="plain")
        if not plain_output.strip():
            raise OperationFailedError(
                f"ceph-volume plain inventory is empty on {node.hostname}"
            )

        # Regression for BZ 1977888: JSON must be valid, including cluster-name tags.
        out = get_inventory_list(node, ceph_image, output_format="json")
        parse_inventory_json(out, node.hostname)
    return 0
