import json
import tempfile
from json import loads

import yaml

from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import (
    get_disk_devlinks,
    get_lvm_on_osd_container,
    get_running_containers,
)

SPEC_FILE_PATH = "/tmp/osd.yaml"


class SpecFileCreationError(Exception):
    pass


class SpecFileApplyError(Exception):
    pass


def _identify_disks(node):
    """
    Identifies the disks associated with the node
    Args:
        node (ceph): Node in which the cmd is executed

    Returns (list): List of available disks
    """

    disks = []
    conf = {"format": "json"}
    available_disks = json.loads(CephAdm(node).ceph.orch.device.ls(**conf))
    for key in available_disks:
        if key.get("devices"):
            for dev in key.get("devices"):
                disks.append(dev.get("path"))
    return list(set(disks))


def _create_spec_file(node, specs):
    """
    Creates the spec file as per the requirement
    Args:
        node (ceph): Node to execute cmd
        specs(str): given spec file data
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")
    spec = yaml.dump(specs, sort_keys=False, indent=2).encode("utf-8")
    temp_file.write(spec)
    temp_file.flush()
    node.upload_file(src=temp_file.name, dst=SPEC_FILE_PATH, sudo=True)
    return temp_file.name


def run(ceph_cluster, **kw):
    """Check LVM persistent naming
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")

    mon_node = ceph_cluster.get_nodes(role="mon")[0]
    client_node = ceph_cluster.get_nodes(role="client")[0]
    disks = _identify_disks(mon_node)

    nodes = config.get("specs", {}).get("placement", {}).get("hosts")
    host = [ceph_cluster.get_nodes()[int(node[-1])].hostname for node in nodes]
    running_containers, _ = get_running_containers(
        mon_node, format="json", expr="name=osd", sudo=True
    )
    container_ids = [item.get("Names")[0] for item in loads(running_containers)]

    # Get the initial lvm list
    initial_lvm_list = get_lvm_on_osd_container(container_ids[0], mon_node)

    by_id, by_path = get_disk_devlinks(mon_node, disks[0])

    # Create spec file specific to by-path and by-id
    for data_device in [by_path[0], by_id[0]]:
        specs = config.get("specs")
        specs["data_devices"]["paths"] = data_device
        specs["db_devices"]["paths"] = data_device
        specs["placement"]["hosts"] = host
        spec_file = _create_spec_file(client_node, specs)
        if not spec_file:
            raise SpecFileCreationError(
                f"Failed to create Spec file with {data_device}"
            )

        # Execute orch apply with spec file
        conf = {"pos_args": ["-i", spec_file]}
        if Ceph(client_node).orch.apply(**conf):
            raise SpecFileApplyError("Failed to apply spec file")

    final_lvm_list = get_lvm_on_osd_container(container_ids[0], mon_node)

    # Checking whether the lvm has been updated or not
    if initial_lvm_list != final_lvm_list:
        raise SpecFileApplyError("LVM's not updated after applying spec")

    return 0
