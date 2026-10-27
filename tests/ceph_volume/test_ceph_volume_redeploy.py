from ceph.waiter import WaitUntil
from cephci.utils.configs import get_cloud_credentials, get_configs
from cli.cephadm.cephadm import CephAdm
from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config
from tests.ceph_volume.test_ceph_volume_non_collocated import (
    _get_host_lvm_list,
    _get_osd_fsid,
    _get_osd_layout,
    _get_service_lvm_list,
    _get_spec_osd_nodes,
    _remove_osd,
    _resolve_spec_hosts,
    _reuse_existing_device_selectors,
    _wait_for_osd_removal,
    _wait_for_redeployment,
)
from tests.ceph_volume.test_ceph_volume_wrong_db_size_compute import (
    _get_lv_sizes,
    _size_bytes,
)


def _add_node_volumes(nodes, config, size, count):
    """Create volume in bulk and attach to node.

    Args:
        nodes (ceph): nodes on volumes to create
        config (dict): ceph config
        size (str): size of volume
        count (int): number of volume
    """
    cloud_type = config.get("cloud-type")
    cloud_configs = get_cloud_credentials(cloud_type)
    cloud = CloudProvider(cloud_type, **cloud_configs)

    nodes = nodes if type(nodes) in (list, tuple) else [nodes]
    for node in nodes:
        for i in range(count):
            volname = f"{node.ceph_nodename}-vol-{size}-{i}"
            Volume(volname, cloud).create(size=size)
            Node(node.ceph_nodename, cloud).attach_volume(volname)
    return True


def _verify_db_sizes(node, layout, expected_sizes=None):
    """Read current DB LV paths and require their actual allocated byte sizes."""
    lvm_list = _get_host_lvm_list(node)
    current = {osd_id: lvm_list.get(osd_id, []) for osd_id in layout}
    if _get_osd_layout(current) != layout:
        raise OperationFailedError(
            f"OSD layout changed while reading DB sizes on {node.hostname}"
        )
    if expected_sizes is not None and set(expected_sizes) != {
        data for data, _ in layout.values()
    }:
        raise OperationFailedError("DB size baseline does not cover all data devices")

    lv_sizes = _get_lv_sizes(node)
    actual_sizes, db_paths = {}, set()
    for osd_id, (data, _) in layout.items():
        dbs = [entry for entry in current[osd_id] if entry.get("type") == "db"]
        if len(dbs) != 1:
            raise OperationFailedError(
                f"OSD {osd_id} does not have exactly one dedicated DB LV"
            )
        db_path = dbs[0].get("lv_path")
        if not isinstance(db_path, str) or not db_path.strip():
            raise OperationFailedError(f"OSD {osd_id} has no DB LV path")
        if db_path in db_paths or data in actual_sizes:
            raise OperationFailedError(
                f"OSD {osd_id} shares a DB LV or data devices with another OSD"
            )
        actual_size = lv_sizes.get(db_path)
        if actual_size is None:
            raise OperationFailedError(
                f"OSD {osd_id} DB LV {db_path} is missing from host LVM sizes"
            )
        if expected_sizes is not None and actual_size != expected_sizes[data]:
            raise OperationFailedError(
                f"OSD {osd_id} DB LV {db_path} has {actual_size} bytes; "
                f"expected {expected_sizes[data]} bytes for data devices {data}"
            )
        actual_sizes[data] = actual_size
        db_paths.add(db_path)
    return actual_sizes


def run(ceph_cluster, **kw):
    """Re-deploy OSDs and verify every dedicated DB mapping and allocated size.

    Set create_volumes=False to reuse the configured service's existing OSDs,
    or deploy from preattached free devices if that service has no OSDs yet.
    reuse_existing_devices=True also derives its selectors from the existing
    service's LVM layout and requires that service to exist before applying.
    osd_count (default 4) is the required OSD count on the selected target host.
    """
    get_configs()
    config = kw.get("config")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    specs = _resolve_spec_hosts(ceph_cluster, config["spec"])
    device_spec = specs.get("spec", {})
    if "block_db_size" in device_spec:
        expected_size = _size_bytes(device_spec["block_db_size"])
    elif "block_db_size" in specs:
        expected_size = _size_bytes(specs["block_db_size"])
    else:
        expected_size = None
    osd_nodes = _get_spec_osd_nodes(ceph_cluster, specs)
    osd_node = osd_nodes[0]
    initial_lvm = _get_host_lvm_list(osd_node)
    original_ids = set(initial_lvm)

    volume_count = config.get("osd_count", 4)
    if (
        not isinstance(volume_count, int)
        or isinstance(volume_count, bool)
        or volume_count < 1
    ):
        raise OperationFailedError("osd_count must be a positive integer")
    reuse_devices = config.get("reuse_existing_devices", False)
    if not isinstance(reuse_devices, bool):
        raise OperationFailedError("reuse_existing_devices must be a boolean")
    create_volumes = config.get("create_volumes", not reuse_devices)
    if not isinstance(create_volumes, bool):
        raise OperationFailedError("create_volumes must be a boolean")
    if reuse_devices and create_volumes:
        raise OperationFailedError(
            "reuse_existing_devices requires create_volumes to be false"
        )
    existing_service = _get_service_lvm_list(initial_lvm, specs.get("service_id"))
    if reuse_devices:
        if not existing_service:
            raise OperationFailedError(
                "reuse_existing_devices requires existing OSDs for the configured service"
            )
        _reuse_existing_device_selectors(specs, _get_osd_layout(existing_service))
    if create_volumes:
        device_spec = specs.get("spec", {})
        size = device_spec.get("data_devices", {}).get("size", "").replace("GB", "")
        if not size:
            raise OperationFailedError(
                "A data device size is required for redeployment"
            )
        _add_node_volumes(nodes=osd_nodes, config=config, size=size, count=volume_count)
    elif existing_service and len(existing_service) != volume_count:
        raise OperationFailedError(
            f"Expected {volume_count} existing service OSDs on {osd_node.hostname}, "
            f"found {len(existing_service)}"
        )

    if not CephAdm(installer).ceph.orch.device.ls(refresh=True):
        raise OperationFailedError("Devices are not refreshed")
    file = create_yaml_config(installer, specs)
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(
        pos_args=[], input=file, check_ec=True
    )

    if not create_volumes and existing_service:
        lvm_list = existing_service
    else:
        # Existing OSDs alone must not satisfy a wait for newly attached disks.
        for _ in WaitUntil(timeout=300, interval=6):
            service_osds = _get_service_lvm_list(
                _get_host_lvm_list(osd_node), specs.get("service_id")
            )
            lvm_list = {
                osd_id: items
                for osd_id, items in service_osds.items()
                if osd_id not in original_ids
            }
            if len(lvm_list) == volume_count:
                break
        else:
            raise OperationFailedError(
                f"Timed out waiting for {volume_count} new OSDs on {osd_node.hostname}"
            )
    before = _get_osd_layout(lvm_list)
    original_fsids = {osd_id: _get_osd_fsid(lvm_list, osd_id) for osd_id in before}
    current = _wait_for_redeployment(installer, osd_node, before)
    expected_sizes = (
        {data: expected_size for data, _ in before.values()}
        if expected_size is not None
        else None
    )
    baseline_sizes = _verify_db_sizes(osd_node, current, expected_sizes)
    if expected_sizes is None:
        expected_sizes = baseline_sizes

    # Keep this host available to size-three CRUSH rules while replacing all OSDs.
    replaced_fsids = {}
    for osd_id, (data, _) in before.items():
        _remove_osd(installer, osd_id)
        _wait_for_osd_removal(installer)
        replaced_fsids = {
            **replaced_fsids,
            data: replaced_fsids.get(data, set()) | {original_fsids[osd_id]},
        }
        current = _wait_for_redeployment(installer, osd_node, before, replaced_fsids)
        _verify_db_sizes(osd_node, current, expected_sizes)
    return 0
