from collections import Counter
from copy import deepcopy
from json import loads
from shlex import quote

from ceph.waiter import WaitUntil
from cephci.utils.configs import get_configs
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.utils import create_yaml_config


def _get_osd_devices(osd_id, lvm_list, device_type):
    """Return every physical device of the requested type for one OSD."""
    devices = set()
    for item in lvm_list.get(osd_id, []):
        if item.get("type") == device_type:
            paths = item.get("devices")
            if (
                not isinstance(paths, list)
                or not paths
                or not all(isinstance(path, str) and path for path in paths)
            ):
                raise OperationFailedError(
                    f"OSD {osd_id} has no physical {device_type} devices"
                )
            devices.update(paths)
    if not devices:
        raise OperationFailedError(
            f"OSD {osd_id} has no dedicated {device_type} devices"
        )
    return tuple(sorted(devices))


def _get_osd_db_id(osd_ids, lvm_list):
    """Return a complete OSD-to-DB mapping, failing on missing dedicated DBs."""
    if not osd_ids:
        raise OperationFailedError("No OSDs found for dedicated DB validation")
    return {osd_id: _get_osd_devices(osd_id, lvm_list, "db") for osd_id in osd_ids}


def _get_osd_layout(lvm_list):
    """Validate and record data and dedicated DB devices for every host OSD."""
    db_devices = _get_osd_db_id(list(lvm_list), lvm_list)
    layout = {}
    for osd_id, db in db_devices.items():
        data = _get_osd_devices(osd_id, lvm_list, "block")
        if set(data).intersection(db):
            raise OperationFailedError(
                f"OSD {osd_id} DB is colocated on data devices {data}"
            )
        layout[osd_id] = (data, db)
    data_pool = {device for data, _ in layout.values() for device in data}
    db_pool = {device for _, db in layout.values() for device in db}
    if data_pool.intersection(db_pool):
        raise OperationFailedError("Dedicated DB pool overlaps the target data devices")
    return layout


def _get_osd_fsid(lvm_list, osd_id):
    """Read the OSD identity from block LV tags, independently of its numeric ID."""
    entries = [item for item in lvm_list.get(osd_id, []) if item.get("type") == "block"]
    fsids = {item.get("tags", {}).get("ceph.osd_fsid") for item in entries}
    if len(fsids) != 1 or not all(isinstance(fsid, str) and fsid for fsid in fsids):
        raise OperationFailedError(f"OSD {osd_id} has no unique block LV OSD FSID")
    return next(iter(fsids))


def _validate_redeployment(before, lvm_list, replaced_fsids=None):
    """Match data and DB devices, and require new identities for removed OSDs."""
    if not before:
        raise OperationFailedError("No baseline OSD layout for redeployment")
    expected_data = Counter(data for data, _ in before.values())
    expected_paths = {device for data in expected_data for device in data}
    # Other services on this host must not satisfy (or obstruct) this check.
    affected = {
        osd_id: items
        for osd_id, items in lvm_list.items()
        if any(
            item.get("type") == "block"
            and isinstance(item.get("devices"), list)
            and all(isinstance(path, str) for path in item["devices"])
            and expected_paths.intersection(item["devices"])
            for item in items
        )
    }
    after = _get_osd_layout(affected)
    actual_data = Counter(data for data, _ in after.values())
    if actual_data != expected_data:
        raise OperationFailedError(
            f"Redeployed data devices {actual_data} differ from {expected_data}"
        )
    expected_db = {device for _, db in before.values() for device in db}
    actual_db = {device for _, db in after.values() for device in db}
    if not actual_db.issubset(expected_db):
        raise OperationFailedError(
            f"Redeployed DB devices {actual_db} are outside DB pool {expected_db}"
        )
    for osd_id, (data, _) in after.items():
        if data in (replaced_fsids or {}):
            fsid = _get_osd_fsid(lvm_list, osd_id)
            if fsid in replaced_fsids[data]:
                raise OperationFailedError(
                    f"Data devices {data} still have removed OSD FSID {fsid}"
                )
    return after


def _get_service_lvm_list(lvm_list, service_id):
    """Select the configured OSD service before choosing any OSDs to remove."""
    if not service_id:
        raise OperationFailedError("An OSD service_id is required for redeployment")
    return {
        osd_id: items
        for osd_id, items in lvm_list.items()
        if any(
            item.get("tags", {}).get("ceph.osdspec_affinity") == service_id
            for item in items
        )
    }


def _get_host_lvm_list(node):
    """Inspect local LVM without depending on a running OSD container."""
    lvm_list = loads(CephAdm(node).ceph_volume.lvm.list(data="", check_ec=True))
    if not isinstance(lvm_list, dict):
        raise OperationFailedError(f"Invalid LVM inventory on {node.hostname}")
    return lvm_list


def _wait_for_redeployment(installer, node, before, replaced_fsids=None):
    """Wait for every original data device to have a running OSD and DB again."""
    last_error = "No redeployed OSDs found"
    for _ in WaitUntil(timeout=300, interval=6):
        try:
            after = _validate_redeployment(
                before, _get_host_lvm_list(node), replaced_fsids
            )
        except OperationFailedError as error:
            last_error = str(error)
            continue
        out = CephAdm(installer).execute(
            cmd="cephadm shell -- ceph osd tree --format json",
            sudo=True,
            check_ec=True,
        )
        try:
            tree = loads(out[0] if isinstance(out, tuple) else out)
        except (TypeError, ValueError) as error:
            raise OperationFailedError("Invalid OSD tree JSON response") from error
        if not isinstance(tree, dict) or not isinstance(tree.get("nodes"), list):
            raise OperationFailedError("Invalid OSD tree response: missing nodes")
        up_ids = {
            str(daemon["id"])
            for daemon in tree["nodes"]
            if daemon.get("type") == "osd" and daemon.get("status") == "up"
        }
        missing = set(map(str, after)) - up_ids
        if missing:
            last_error = f"OSDs are not up: {sorted(missing)}"
            continue
        return after
    raise OperationFailedError(
        f"Non-collocated OSD redeployment timed out on {node.hostname}: {last_error}"
    )


def _remove_osd(installer, osd_id):
    """Require a successful removal command without passing check_ec as a CLI flag."""
    return CephAdm(installer).execute(
        cmd=f"cephadm shell -- ceph orch osd rm {quote(str(osd_id))} --zap --force",
        sudo=True,
        check_ec=True,
    )


def _wait_for_osd_removal(installer):
    """Accept both JSON and human-readable empty removal queues."""
    for _ in WaitUntil(timeout=300, interval=6):
        out = CephAdm(installer).execute(
            cmd="cephadm shell -- ceph orch osd rm status --format json",
            sudo=True,
            check_ec=True,
        )
        out = out[0] if isinstance(out, tuple) else out
        if out.strip() == "[]" or "No OSD remove/replace operations reported" in out:
            return
    raise OperationFailedError("Failed to perform OSD removal. Timed out!")


def _resolve_spec_hosts(ceph_cluster, spec):
    """Copy a suite spec and resolve its zero-based node placement references."""
    specs = deepcopy(spec)
    hosts = specs.get("placement", {}).get("hosts")
    if hosts:
        nodes = ceph_cluster.get_nodes()
        specs["placement"]["hosts"] = [
            (
                nodes[int(host[4:])].hostname
                if host.startswith("node") and host[4:].isdigit()
                else host
            )
            for host in hosts
        ]
    return specs


def _get_spec_osd_nodes(ceph_cluster, specs):
    """Limit device inspection and volume creation to the requested placement."""
    hosts = specs.get("placement", {}).get("hosts")
    nodes = [
        node
        for node in ceph_cluster.get_nodes(role="osd")
        if not hosts or node.hostname in hosts
    ]
    if not nodes:
        raise OperationFailedError("No OSD hosts match the service placement")
    return nodes


def _reuse_existing_device_selectors(specs, before):
    """Retain the service's actual data and DB disks when reapplying its spec."""
    if not before:
        raise OperationFailedError(
            "reuse_existing_devices requires existing OSDs for the configured service"
        )
    device_spec = specs.get("spec", specs)
    device_spec["data_devices"] = {
        "paths": sorted({device for data, _ in before.values() for device in data})
    }
    device_spec["db_devices"] = {
        "paths": sorted({device for _, db in before.values() for device in db})
    }


def run(ceph_cluster, **kw):
    """Verify redeployed OSDs retain dedicated DB devices after a zap.

    Set reuse_existing_devices=True to derive the service's explicit data and DB
    selectors from its existing LVM layout while retaining its configured DB size.
    """
    get_configs()
    config = kw.get("config")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    specs = _resolve_spec_hosts(ceph_cluster, config["spec"])
    osd_node = _get_spec_osd_nodes(ceph_cluster, specs)[0]
    service_lvm = _get_service_lvm_list(
        _get_host_lvm_list(osd_node), specs.get("service_id")
    )
    reuse_devices = config.get("reuse_existing_devices", False)
    if not isinstance(reuse_devices, bool):
        raise OperationFailedError("reuse_existing_devices must be a boolean")
    if reuse_devices and not service_lvm:
        raise OperationFailedError(
            "reuse_existing_devices requires existing OSDs for the configured service"
        )
    before = _get_osd_layout(service_lvm)
    if reuse_devices:
        _reuse_existing_device_selectors(specs, before)
    osd_id = next(iter(before))
    replaced_fsids = {before[osd_id][0]: {_get_osd_fsid(service_lvm, osd_id)}}
    specs["unmanaged"] = True
    file = create_yaml_config(installer, specs)
    CephAdm(nodes=installer, mount=file).ceph.orch.apply(
        pos_args=[], input=file, check_ec=True
    )

    try:
        _remove_osd(installer, osd_id)
        _wait_for_osd_removal(installer)
    finally:
        specs["unmanaged"] = False
        file = create_yaml_config(installer, specs)
        CephAdm(nodes=installer, mount=file).ceph.orch.apply(
            pos_args=[], input=file, check_ec=True
        )
    _wait_for_redeployment(installer, osd_node, before, replaced_fsids)
    return 0
