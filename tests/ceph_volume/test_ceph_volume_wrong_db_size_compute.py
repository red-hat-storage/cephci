"""Verify the DB LV actually created for every requested non-collocated OSD."""

import json
import math
import re
from copy import deepcopy
from decimal import Decimal

from ceph.waiter import WaitUntil
from cephci.utils.configs import get_cloud_credentials, get_configs
from cli.cephadm.cephadm import CephAdm
from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import create_yaml_config
from tests.ceph_volume.utils import ceph_volume_shell, get_ceph_volume
from utility.log import Log

log = Log(__name__)
DB_DEVICE_COUNT = 2
DATA_DEVICE_COUNT = 4
GIB = 1024**3


def _size_bytes(value):
    """Interpret Ceph sizes as bytes, with binary K/M/G/T suffixes."""
    if isinstance(value, bool):
        raise ConfigError("block_db_size must be a positive byte count or size")
    match = re.fullmatch(
        r"([0-9]+(?:[.][0-9]+)?)\s*([KMGT]?)(?:I?B)?", str(value).strip().upper()
    )
    if not match:
        raise ConfigError(f"Invalid block_db_size: {value!r}")
    size = Decimal(match[1]) * 1024 ** ("KMGT".index(match[2]) + 1 if match[2] else 0)
    if size <= 0 or size != size.to_integral_value():
        raise ConfigError(
            f"block_db_size must resolve to positive whole bytes: {value!r}"
        )
    return int(size)


def _resolve_target_nodes(ceph_cluster, hosts):
    """Resolve legacy node0-based references, rejecting non-OSD placements."""
    all_nodes = ceph_cluster.get_nodes()
    osd_nodes = ceph_cluster.get_nodes(role="osd")
    if (
        not isinstance(hosts, list)
        or not hosts
        or not all(isinstance(host, str) for host in hosts)
    ):
        raise ConfigError("specs.placement.hosts must be a nonempty list of OSD hosts")
    targets = []
    for host in hosts:
        match = re.fullmatch(r"node([0-9]+)", host)
        if match:
            index = int(match[1])
            if index >= len(all_nodes):
                raise ConfigError(
                    f"Placement {host!r} is outside the cluster node list"
                )
            node = all_nodes[index]
        else:
            node = next((node for node in all_nodes if node.hostname == host), None)
        if node is None or node not in osd_nodes:
            raise ConfigError(f"Placement {host!r} does not resolve to an OSD host")
        if node in targets:
            raise ConfigError(f"Duplicate OSD host in placement: {node.hostname}")
        targets.append(node)
    return targets


def _available_devices(node):
    """Inspect only this host's devices; inventory excludes used/system disks."""
    out = CephAdm(node).execute(
        cmd=f"{ceph_volume_shell()} ceph-volume inventory --format json",
        sudo=True,
        check_ec=True,
    )
    devices = json.loads(out[0] if isinstance(out, tuple) else out)
    if not isinstance(devices, list):
        raise OperationFailedError(f"Invalid device inventory on {node.hostname}")
    return sorted(
        device["path"]
        for device in devices
        if device.get("available") is True
        and device.get("path", "").startswith("/dev/")
    )


def _get_host_lvm(node):
    """Read all OSD LVs locally, including OSDs not yet running."""
    return json.loads(get_ceph_volume(node).lvm.list(data="", check_ec=True))


def _get_lv_sizes(node):
    """Read allocated LV bytes, including PVs omitted by the host devices file."""
    out = node.exec_command(
        cmd="lvs --devicesfile '' --reportformat json --units b --nosuffix -o lv_path,lv_size",
        sudo=True,
        check_ec=True,
    )
    reports = json.loads(out[0] if isinstance(out, tuple) else out)
    return {
        lv["lv_path"].strip(): _size_bytes(lv["lv_size"].strip())
        for report in reports.get("report", [])
        for lv in report.get("lv", [])
    }


def _validate_db_allocations(
    lvm_list,
    lv_sizes,
    data_devices,
    db_devices,
    expected_size,
    original_ids,
    service_id,
):
    """Require one new OSD and a correctly sized dedicated DB per data disk."""
    expected_data, expected_db = set(data_devices), set(db_devices)
    allocated, allocated_db = {}, set()
    for osd_id, entries in lvm_list.items():
        if str(osd_id) in original_ids:
            continue
        blocks = [entry for entry in entries if entry.get("type") == "block"]
        actual_data = {path for entry in blocks for path in entry.get("devices", [])}
        if not actual_data.intersection(expected_data):
            continue
        if (
            len(blocks) != 1
            or len(actual_data) != 1
            or not actual_data.issubset(expected_data)
        ):
            raise OperationFailedError(
                f"OSD {osd_id} has unexpected data devices {actual_data}"
            )
        affinity = blocks[0].get("tags", {}).get("ceph.osdspec_affinity")
        if affinity != service_id:
            raise OperationFailedError(
                f"OSD {osd_id} has ceph.osdspec_affinity {affinity!r}; expected {service_id!r}"
            )
        data = next(iter(actual_data))
        if data in allocated:
            raise OperationFailedError(f"Multiple new OSDs use data device {data}")
        dbs = [entry for entry in entries if entry.get("type") == "db"]
        if len(dbs) != 1:
            raise OperationFailedError(
                f"OSD {osd_id} does not have exactly one dedicated DB LV"
            )
        actual_db = set(dbs[0].get("devices", []))
        if (
            not actual_db
            or not actual_db.issubset(expected_db)
            or actual_db.intersection(actual_data)
        ):
            raise OperationFailedError(
                f"OSD {osd_id} has unexpected or colocated DB devices {actual_db}"
            )
        db_path = dbs[0].get("lv_path")
        if db_path in allocated_db:
            raise OperationFailedError(
                f"DB LV {db_path} is shared by multiple new OSDs"
            )
        actual_size = lv_sizes.get(db_path)
        if actual_size != expected_size:
            raise OperationFailedError(
                f"OSD {osd_id} DB LV {db_path} has {actual_size} bytes; expected {expected_size} bytes"
            )
        allocated[data] = str(osd_id)
        allocated_db.add(db_path)
    if set(allocated) != expected_data:
        raise OperationFailedError(
            f"Missing new OSDs for data devices {sorted(expected_data - set(allocated))}"
        )
    return set(allocated.values())


def _add_node_volumes(nodes, config, size, count):
    """Create and attach volumes only on the selected OSD hosts."""
    cloud_type = config["cloud-type"]
    cloud_configs = get_cloud_credentials(cloud_type)
    cloud = CloudProvider(cloud_type, **cloud_configs)
    for node in nodes:
        for index in range(count):
            volname = f"{node.ceph_nodename}-vol-{size}-{index}"
            Volume(volname, cloud).create(size=size)
            Node(node.ceph_nodename, cloud).attach_volume(volname)


def _wait_for_devices(node, excluded, timeout, interval):
    for _ in WaitUntil(timeout=timeout, interval=interval):
        devices = [path for path in _available_devices(node) if path not in excluded]
        if len(devices) >= DB_DEVICE_COUNT + DATA_DEVICE_COUNT:
            return devices[: DB_DEVICE_COUNT + DATA_DEVICE_COUNT]
    raise OperationFailedError(
        f"Timed out waiting for six available devices on {node.hostname}"
    )


def _wait_for_db_osds(installer, deployments, expected_size, timeout, interval):
    last_error = "No new OSDs found"
    for _ in WaitUntil(timeout=timeout, interval=interval):
        try:
            osd_ids = set()
            for node, data, db, original_ids, service_id in deployments:
                host_ids = _validate_db_allocations(
                    _get_host_lvm(node),
                    _get_lv_sizes(node),
                    data,
                    db,
                    expected_size,
                    original_ids,
                    service_id,
                )
                if osd_ids.intersection(host_ids):
                    raise OperationFailedError("New OSD IDs appear on multiple hosts")
                osd_ids.update(host_ids)
            tree = json.loads(CephAdm(installer).ceph.osd.tree(format="json"))
            up_ids = {
                str(node["id"])
                for node in tree.get("nodes", [])
                if node.get("type") == "osd" and node.get("status") == "up"
            }
            if not osd_ids.issubset(up_ids):
                raise OperationFailedError(
                    f"New OSDs are not up: {sorted(osd_ids - up_ids)}"
                )
        except OperationFailedError as error:
            last_error = str(error)
            continue
        log.info(
            f"Validated {len(osd_ids)} new OSDs with dedicated DB LVs of {expected_size} bytes, all up"
        )
        return
    raise OperationFailedError(
        f"Timed out validating DB size and new OSD deployment: {last_error}"
    )


def run(ceph_cluster, **kw):
    """Create non-collocated OSDs and verify their actual configured DB LV size.

    Set create_volumes=False for six preattached disposable devices per host.
    Optional service_ids maps each placement reference to an explicit service ID;
    otherwise multi-host services are suffixed with their cloud node names.
    """
    config = deepcopy(kw.get("config") or {})
    specs = config.get("specs")
    if (
        not isinstance(specs, dict)
        or specs.get("service_type") != "osd"
        or not specs.get("service_id")
    ):
        raise ConfigError("specs must define service_type 'osd' and a service_id")
    expected_size = _size_bytes(specs.get("block_db_size"))
    create_volumes = config.get("create_volumes", True)
    if not isinstance(create_volumes, bool):
        raise ConfigError("create_volumes must be a boolean")
    if create_volumes and not config.get("cloud-type"):
        raise ConfigError("cloud-type is required when create_volumes is true")
    timeout, interval = config.get("timeout", 300), config.get("interval", 6)
    if (
        not isinstance(timeout, int)
        or isinstance(timeout, bool)
        or timeout <= 0
        or not isinstance(interval, int)
        or isinstance(interval, bool)
        or interval <= 0
    ):
        raise ConfigError("timeout and interval must be positive integers")
    placement = specs.get("placement") or {}
    targets = _resolve_target_nodes(ceph_cluster, placement.get("hosts"))
    service_ids = config.get("service_ids")
    if service_ids is not None:
        if not isinstance(service_ids, dict) or set(service_ids) != set(
            placement["hosts"]
        ):
            raise ConfigError(
                "service_ids must map every placement host to a service ID"
            )
        resolved_ids = [service_ids[host] for host in placement["hosts"]]
    else:
        resolved_ids = [
            (
                f"{specs['service_id']}-{node.ceph_nodename}"
                if len(targets) > 1
                else specs["service_id"]
            )
            for node in targets
        ]
    if not all(
        isinstance(service_id, str) and re.fullmatch(r"[A-Za-z0-9_.-]+", service_id)
        for service_id in resolved_ids
    ) or len(set(resolved_ids)) != len(resolved_ids):
        raise ConfigError("Each host must have a distinct, valid OSD service ID")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    # Capture existing devices before attaching; unrelated free disks cannot count as newly added volumes.
    excluded = {
        node.hostname: set(_available_devices(node)) if create_volumes else set()
        for node in targets
    }
    if create_volumes:
        get_configs()
        # Two DB devices must have room for two configured DB LVs each, plus LVM metadata.
        volume_size = max(32, math.ceil(expected_size * 2 / GIB) + 1)
        _add_node_volumes(
            targets, config, size=volume_size, count=DB_DEVICE_COUNT + DATA_DEVICE_COUNT
        )
    deployments, host_specs = [], []
    for node, service_id in zip(targets, resolved_ids):
        devices = _wait_for_devices(node, excluded[node.hostname], timeout, interval)
        db, data = devices[:DB_DEVICE_COUNT], devices[DB_DEVICE_COUNT:]
        original_ids = set(map(str, _get_host_lvm(node)))
        host_spec = deepcopy(specs)
        host_spec["placement"]["hosts"] = [node.hostname]
        host_spec["data_devices"] = {"paths": data}
        host_spec["db_devices"] = {"paths": db}
        host_spec["service_id"] = service_id
        log.info(
            f"Deploying OSD service {service_id} on {node.hostname}: "
            f"data={data}, DB={db}, block_db_size={specs['block_db_size']}"
        )
        host_specs.append(host_spec)
        deployments.append((node, data, db, original_ids, service_id))
    # cephadm must refresh its cached inventory after attaching new devices.
    CephAdm(installer).execute(
        cmd="cephadm shell -- ceph orch device ls --refresh --format json",
        sudo=True,
        check_ec=True,
    )
    for host_spec in host_specs:
        file = create_yaml_config(installer, host_spec)
        CephAdm(nodes=installer, mount=file).ceph.orch.apply(
            pos_args=[], input=file, check_ec=True
        )
    _wait_for_db_osds(installer, deployments, expected_size, timeout, interval)
    return 0
