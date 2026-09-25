"""
Comprehensive NVMe-oF upgrade E2E orchestrator.

Phases (single test execution):
  1. Deploy NVMe-oF service and configure all gateway entities from suite YAML
  2. Pre-upgrade feature validation (QoS, namespace resize, namespace masking)
  3. Connect initiators, validate namespace visibility, start continuous IO
  4. Upgrade the Ceph cluster while IO runs in the background
  5. Validate post-upgrade NVMe/SPDK versions and masked namespaces (>=2), stop IO
  6. Re-validate namespace masking at initiator after upgrade IO stops
  7. Run HA failover/failback with IO validation
  8. Optional cleanup

Uses existing workflow modules and helpers without modifying them.

``_comprehensive_upgrade_prerequisites`` and ``_comprehensive_cephadm_install``
intentionally mirror ``test_ceph_nvmeof_upgrade`` helpers but run registry
login and cephadm install on non-installer hosts via installer-jump SSH using
ceph orch host names (gateway nodes are often unreachable from the runner).
"""

import json
import shlex
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.ceph_admin.common import config_dict_to_string
from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.initiators.linux import Initiator
from ceph.utils import get_node_by_id
from cephci.utils.configs import get_configs, get_registry_credentials
from cli.utilities.configure import setup_ibm_licence
from cli.utilities.containers import Registry
from tests.nvmeof.test_ceph_nvmeof_ns_resize import (
    check_io_percent,
    execute_io,
    resize_rbd_image,
    set_auto_resize,
    verfiy_auto_resize,
    verfiy_namespace_size,
)
from tests.nvmeof.test_ceph_nvmeof_qos_tests import configure_qos
from tests.nvmeof.test_ceph_nvmeof_upgrade import (
    compare_nvme_versions,
    fetch_nvme_versions,
)
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import (
    Initiators,
    NVMeInitiator,
    prepare_io_execution,
    purge_cached_initiators,
)
from tests.nvmeof.workflows.ns_masking import NamespaceMasking
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)

DEFAULT_REGISTRY_LOGIN_TIMEOUT = 1200
DEFAULT_CEPHADM_INSTALL_TIMEOUT = 1200
MIN_MASKING_NAMESPACE_COUNT = 2
CEPHADM_INSTALL_CMD = "yum -y install cephadm --nogpgcheck"
CEPHADM_VERIFY_CMD = "rpm -qa | grep cephadm"
IBM_LICENSE_INSTALL_CMD = (
    "ACCEPT_EULA=Y yum install -y ibm-storage-ceph-license --nogpgcheck"
)
IBM_LICENSE_ACCEPT_CMD = "cat /usr/share/ibm-storage-ceph-license/accept"
UPGRADE_FIO_OUTPUT_DIR = "/tmp/cephci_nvmeof_upgrade_io"


# --- Pre-upgrade feature helpers ---


def _get_inband_auth_node_map(config):
    """Map inband-auth initiator nodes to their subsystem NQN."""
    auth_nodes = {}
    for subsystem in config.get("subsystems", []):
        if not subsystem.get("inband_auth"):
            continue
        nqn = subsystem.get("subnqn") or subsystem.get("nqn")
        for host in subsystem.get("hosts", []):
            if isinstance(host, dict) and host.get("inband_auth"):
                auth_nodes[host["node"]] = nqn
    return auth_nodes


def _collect_preconfigured_initiators(config):
    """Collect DHCHAP initiators for inband-auth nodes only.

    Passing every cached initiator into ``prepare_io_execution`` causes
    connect-all nodes to reuse auth-scoped objects and apply DHCHAP where
    it is not required.
    """
    auth_node_map = _get_inband_auth_node_map(config)
    if not auth_node_map:
        return []

    seen = set()
    initiators = []
    for (node_id, subsystem_nqn), initiator in Initiators.items():
        if node_id not in auth_node_map:
            continue
        if not initiator.host_key:
            continue
        expected_nqn = auth_node_map.get(node_id)
        if expected_nqn and subsystem_nqn != expected_nqn:
            continue
        initiator_id = id(initiator)
        if initiator_id in seen:
            continue
        seen.add(initiator_id)
        initiators.append(initiator)
    return initiators


def _uses_inband_auth(config):
    for subsystem in config.get("subsystems", []):
        if subsystem.get("inband_auth"):
            return True
    return config.get("inband_auth_mode") is not None


def _normalize_inband_auth_subsystems(config):
    """Align inband-auth subsystem entries with workflow expectations."""
    for subsystem in config.get("subsystems", []):
        if not subsystem.get("inband_auth"):
            continue

        nqn = subsystem.get("subnqn") or subsystem.get("nqn")
        if not nqn:
            raise ValueError("inband_auth subsystem requires nqn or subnqn")
        subsystem["subnqn"] = nqn
        subsystem.setdefault("nqn", nqn)
        if "auth_mode" not in subsystem and config.get("inband_auth_mode"):
            subsystem["auth_mode"] = config.get("inband_auth_mode")

        normalized_hosts = []
        for host in subsystem.get("hosts", []):
            if isinstance(host, dict):
                host_cfg = dict(host)
            else:
                host_cfg = {"node": host, "inband_auth": True}
            if "node" not in host_cfg:
                raise ValueError(
                    f"inband_auth host entry must include node: {host_cfg}"
                )
            normalized_hosts.append(host_cfg)
        subsystem["hosts"] = normalized_hosts


def _resolve_initiator_configs(config):
    """Map connect-all to subsystem NQN for inband-auth initiator hosts.

    ``prepare_io_execution`` overwrites a preconfigured DHCHAP initiator's NQN
    when the suite uses connect-all, which breaks bidirectional auth connects.
    """
    auth_nodes = _get_inband_auth_node_map(config)

    resolved = []
    for initiator_cfg in config.get("initiators", []):
        cfg = dict(initiator_cfg)
        node = cfg.get("node")
        if cfg.get("nqn") == "connect-all" and node in auth_nodes:
            cfg["nqn"] = auth_nodes[node]
            cfg.setdefault("inband_auth", True)
            LOG.info(
                "Resolved connect-all to %s for inband-auth node=%s",
                cfg["nqn"],
                node,
            )
        resolved.append(cfg)
    return resolved


def _subsystem_host_nodes(subsystem):
    """Return explicit host node ids for a subsystem, or None for open host."""
    if subsystem.get("allow_host") == "*":
        return None
    host_nodes = []
    for host in subsystem.get("hosts", []):
        if isinstance(host, str):
            host_nodes.append(host)
        elif isinstance(host, dict) and host.get("node"):
            host_nodes.append(host["node"])
    return host_nodes


def _build_namespace_uuids_by_subsystem(namespaces):
    by_subsystem = defaultdict(list)
    for namespace in namespaces:
        by_subsystem[namespace["ns_subsystem_nqn"]].append(namespace["uuid"])
    return by_subsystem


def _expected_uuids_for_initiator(initiator_cfg, config, namespaces_by_sub):
    """Return namespace UUIDs an initiator should see based on suite ACLs."""
    node = initiator_cfg["node"]
    requested_nqn = initiator_cfg.get("nqn")
    expected = []

    for subsystem in config.get("subsystems", []):
        sub_nqn = subsystem.get("subnqn") or subsystem.get("nqn")
        uuids = namespaces_by_sub.get(sub_nqn, [])
        if not uuids:
            continue

        if (
            requested_nqn not in ("connect-all", "discover-all")
            and requested_nqn != sub_nqn
        ):
            continue

        if subsystem.get("inband_auth"):
            if not any(
                isinstance(host, dict)
                and host.get("inband_auth")
                and host.get("node") == node
                for host in subsystem.get("hosts", [])
            ):
                continue
        elif subsystem.get("allow_host") == "*":
            pass
        else:
            host_nodes = _subsystem_host_nodes(subsystem) or []
            if node not in host_nodes:
                continue

        expected.extend(uuids)

    return expected


def _validate_namespace_coverage(clients, namespaces, config):
    """Validate per-initiator visibility and full cluster namespace coverage."""
    namespaces_by_sub = _build_namespace_uuids_by_subsystem(namespaces)
    all_expected = {namespace["uuid"] for namespace in namespaces}
    initiator_cfg_by_node = {
        initiator_cfg["node"]: initiator_cfg for initiator_cfg in config["initiators"]
    }
    union_seen = set()

    for client in clients:
        initiator_cfg = initiator_cfg_by_node.get(client.node.id)
        if not initiator_cfg:
            raise ValueError(f"No initiator config found for node {client.node.id}")

        expected = set(
            _expected_uuids_for_initiator(initiator_cfg, config, namespaces_by_sub)
        )
        seen = set(client.fetch_lsblk_nvme_devices())
        union_seen |= seen
        missing = expected - seen
        extra = seen - expected
        if missing or extra:
            raise OSError(
                f"Initiator {client.node.id} namespace mismatch on "
                f"{initiator_cfg.get('nqn')}: missing={sorted(missing)}, "
                f"extra={sorted(extra)}"
            )
        LOG.info(
            "Initiator %s validated %s namespace(s) on %s",
            client.node.id,
            len(seen),
            initiator_cfg.get("nqn"),
        )

    missing_cluster = all_expected - union_seen
    extra_cluster = union_seen - all_expected
    if missing_cluster or extra_cluster:
        raise OSError(
            "Cluster namespace coverage mismatch: "
            f"missing={sorted(missing_cluster)}, extra={sorted(extra_cluster)}"
        )
    LOG.info(
        "All %s namespaces are visible across initiator clients", len(all_expected)
    )


def _prepare_initiator_connection_state(ceph_cluster, config):
    """Disconnect initiators and drop stale non-auth cache before reconnect."""
    _disconnect_initiator_nodes(ceph_cluster, config)
    if not _uses_inband_auth(config):
        return

    auth_nodes = set(_get_inband_auth_node_map(config))
    non_auth_nodes = {
        initiator_cfg["node"]
        for initiator_cfg in config.get("initiators", [])
        if initiator_cfg.get("node") not in auth_nodes
    }
    if non_auth_nodes:
        LOG.info(
            "Purging cached initiators for non-auth nodes before reconnect: %s",
            sorted(non_auth_nodes),
        )
        purge_cached_initiators(non_auth_nodes, cluster=ceph_cluster, disconnect=False)


def _fetch_ns_data_for_subsystem(gateway, subsystem_nqn):
    """Build ns_data mapping used by namespace resize helpers."""
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": subsystem_nqn}}
    out, _ = gateway.namespace.list(**args)
    ns_data = {}
    for ns in json.loads(out).get("namespaces", []):
        image = ns["rbd_image_name"]
        ns_data[image] = {
            "ns_subsystem_nqn": subsystem_nqn,
            "nsid": ns["nsid"],
            "rbd_image_name": image,
            "rbd_pool_name": ns["rbd_pool_name"],
        }
    if not ns_data:
        raise ValueError(f"No namespaces found on subsystem {subsystem_nqn}")
    return ns_data


def _disconnect_initiator_nodes(ceph_cluster, config):
    """Disconnect all configured initiators between feature phases."""
    for initiator_cfg in config.get("initiators", []):
        node = get_node_by_id(ceph_cluster, initiator_cfg["node"])
        Initiator(node).disconnect_all()


def _get_subsystem_config(config, subsystem_nqn):
    """Return subsystem entry from suite config matching the given NQN."""
    subsystem = next(
        (
            sub
            for sub in config.get("subsystems", [])
            if (sub.get("nqn") or sub.get("subnqn")) == subsystem_nqn
        ),
        None,
    )
    if not subsystem:
        raise ValueError(f"Subsystem {subsystem_nqn} not found in suite config")
    return subsystem


def _run_qos_validation(ceph_cluster, config, nvme_service):
    """Apply and validate namespace QoS limits using existing QoS test helpers."""
    qos_cfg = config.get("qos_validation")
    if not qos_cfg:
        return

    subsystem_nqn = qos_cfg.get("subsystem_nqn")
    if not subsystem_nqn:
        raise ValueError("qos_validation.subsystem_nqn is required")

    subsystem = _get_subsystem_config(config, subsystem_nqn)

    qos_initiators = qos_cfg.get(
        "initiators",
        [
            {
                "nqn": subsystem_nqn,
                "listener_port": subsystem.get("listener_port", 4420),
                "node": config["initiators"][0]["node"],
            }
        ],
    )
    qos_config = {
        "initiators": qos_initiators,
        "subsystems": [subsystem],
    }
    LOG.info("Running pre-upgrade QoS validation on %s", subsystem_nqn)
    configure_qos(
        ceph_cluster,
        nvme_service.gateways[0],
        subsystem,
        qos_config,
    )
    _disconnect_initiator_nodes(ceph_cluster, {"initiators": qos_initiators})


def _run_namespace_resize_validation(ceph_cluster, config, nvme_service, rbd_obj, orch):
    """Exercise manual/auto namespace resize before upgrade."""
    resize_cfg = config.get("namespace_resize")
    if not resize_cfg:
        return

    subsystem_nqn = resize_cfg["subsystem_nqn"]
    subsystem = _get_subsystem_config(config, subsystem_nqn)
    rbd_pool = config["rbd_pool"]
    gateway = nvme_service.gateways[0]
    ns_data = _fetch_ns_data_for_subsystem(gateway, subsystem_nqn)

    resize_initiators = resize_cfg.get(
        "initiators",
        [
            {
                "nqn": subsystem_nqn,
                "listener_port": subsystem.get("listener_port", 4420),
                "node": config["initiators"][0]["node"],
            }
        ],
    )
    resize_config = {"initiators": resize_initiators}

    LOG.info("Running pre-upgrade namespace resize validation on %s", subsystem_nqn)
    execute_io(resize_config, gateway, ceph_cluster, ns_data)
    check_io_percent(ns_data, orch, "2")

    set_auto_resize(ns_data, False, gateway)
    verfiy_auto_resize(ns_data, gateway, "True")

    resize_rbd_image(ns_data, rbd_obj, rbd_pool, "3G")
    verfiy_namespace_size(ns_data, gateway, "2G")

    execute_io(resize_config, gateway, ceph_cluster, ns_data)
    check_io_percent(ns_data, orch, "2")

    for image in ns_data:
        ns_args = {
            "subsystem": ns_data[image]["ns_subsystem_nqn"],
            "nsid": ns_data[image]["nsid"],
        }
        gateway.namespace.refresh_size(**{"args": ns_args})

    verfiy_namespace_size(ns_data, gateway, "3G")
    execute_io(resize_config, gateway, ceph_cluster, ns_data)
    check_io_percent(ns_data, orch, "3")

    set_auto_resize(ns_data, True, gateway)
    verfiy_auto_resize(ns_data, gateway, "False")

    resize_rbd_image(ns_data, rbd_obj, rbd_pool, "5G")
    verfiy_namespace_size(ns_data, gateway, "5G")
    execute_io(resize_config, gateway, ceph_cluster, ns_data)
    check_io_percent(ns_data, orch, "5")
    _disconnect_initiator_nodes(ceph_cluster, {"initiators": resize_initiators})


def _masking_connect_config(subsystem_nqn, listener_port, initiator_node):
    """Build initiator config scoped to a single masking subsystem."""
    return {
        "nqn": subsystem_nqn,
        "listener_port": listener_port,
        "node": initiator_node,
    }


def _validate_masking_initiator_visibility(
    gateway,
    ceph_cluster,
    initiator_node,
    subsystem_nqn,
    listener_port,
    expected_visibility,
    expected_device_count=None,
):
    """Validate namespace visibility using a subsystem-scoped connect."""
    initiator_host = get_node_by_id(ceph_cluster, initiator_node)
    initiator = NVMeInitiator(initiator_host)
    initiator.disconnect_all()
    initiator.connect_targets(
        gateway,
        _masking_connect_config(subsystem_nqn, listener_port, initiator_node),
    )

    devices_json, _ = initiator_host.exec_command(
        cmd="nvme list --output-format=json", sudo=True
    )
    devices = json.loads(devices_json).get("Devices", [])

    if not expected_visibility:
        if devices:
            raise RuntimeError(
                f"Initiator {initiator_node} has devices when NS visibility is "
                f"restricted on {subsystem_nqn}: {devices}"
            )
        LOG.info(
            "Validated - no devices visible on %s for %s",
            initiator_node,
            subsystem_nqn,
        )
        return

    if not devices:
        raise RuntimeError(
            f"Initiator {initiator_node} has no devices when NS visibility is "
            f"enabled on {subsystem_nqn}"
        )
    if expected_device_count is not None and len(devices) != expected_device_count:
        raise RuntimeError(
            f"Expected {expected_device_count} devices on {subsystem_nqn}, "
            f"found {len(devices)}: {devices}"
        )
    LOG.info(
        "Validated - %s device(s) visible on %s for %s",
        len(devices),
        initiator_node,
        subsystem_nqn,
    )


def _execute_masking_io(
    ceph_cluster,
    gateway,
    initiator_node,
    subsystem_nqn,
    listener_port,
    masked_images,
    ns_masking,
):
    """Run FIO and validate RBD usage for masking namespaces on one subsystem."""
    initiator_host = get_node_by_id(ceph_cluster, initiator_node)
    initiator = NVMeInitiator(initiator_host)
    initiator.disconnect_all()
    initiator.connect_targets(
        gateway,
        _masking_connect_config(subsystem_nqn, listener_port, initiator_node),
    )
    paths = initiator.list_devices()
    if len(paths) != len(masked_images):
        raise ValueError(
            f"Expected {len(masked_images)} devices on {subsystem_nqn}, found {paths}"
        )

    # Use a partial write size so RBD used_size keeps growing during
    # validate_io sampling; 100% fills the image before samples complete.
    fio_thread = threading.Thread(
        target=initiator.start_fio,
        kwargs={
            "paths": paths,
            "io_size": "30%",
            "runtime": 300,
            "time_based": True,
            "io_type": "randwrite",
            "iodepth": 16,
        },
    )
    validate_thread = threading.Thread(
        target=ns_masking.validate_io,
        args=(masked_images, False),
    )
    fio_thread.start()
    validate_thread.start()
    try:
        validate_thread.join()
    finally:
        initiator.stop_fio()
        fio_thread.join(timeout=60)


def _count_subsystem_namespaces(gateway, subsystem_nqn):
    """Return the number of namespaces on a subsystem."""
    args = {
        "base_cmd_args": {"format": "json"},
        "args": {"subsystem": subsystem_nqn},
    }
    out, _ = gateway.namespace.list(**args)
    return len(json.loads(out).get("namespaces", []))


def _run_namespace_masking_workflow(ceph_cluster, config, nvme_service, rbd_obj, orch):
    """Run namespace masking add/host/visibility workflow before upgrade."""
    masking_cfg = config.get("namespace_masking")
    if not masking_cfg:
        return

    subsystem_nqn = masking_cfg["subsystem_nqn"]
    subsystem = _get_subsystem_config(config, subsystem_nqn)
    initiator_node = masking_cfg["initiator_node"]
    pool = masking_cfg.get("pool", config["rbd_pool"])
    image_size = masking_cfg.get("image_size", "5G")
    namespace_count = masking_cfg.get("namespace_count", MIN_MASKING_NAMESPACE_COUNT)
    if namespace_count < MIN_MASKING_NAMESPACE_COUNT:
        raise ValueError(
            f"namespace_masking.namespace_count must be >= "
            f"{MIN_MASKING_NAMESPACE_COUNT}, got {namespace_count}"
        )
    listener_port = masking_cfg.get(
        "listener_port", subsystem.get("listener_port", 4420)
    )
    LOG.info(
        "Running pre-upgrade namespace masking on %s with %s namespace(s)",
        subsystem_nqn,
        namespace_count,
    )

    gateway = nvme_service.gateways[0]
    ns_masking = NamespaceMasking(
        ceph_cluster,
        nvme_service.gateways,
        [],
        orch,
    )

    initiator_host = get_node_by_id(ceph_cluster, initiator_node)
    initiator_host.exec_command(cmd="nvme gen-hostnqn > /etc/nvme/hostnqn", sudo=True)
    hostnqn, _ = initiator_host.exec_command(cmd="cat /etc/nvme/hostnqn", sudo=True)
    hostnqn = hostnqn.strip()
    hostnqn_dict = {initiator_node: hostnqn}

    masked_images = []
    masked_nsids = []
    for index in range(1, namespace_count + 1):
        image = f"{generate_unique_id(length=4)}-mask-image{index}"
        rbd_obj.create_image(pool, image, image_size)
        add_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "rbd-image": image,
                "rbd-pool": pool,
                "subsystem": subsystem_nqn,
                "no-auto-visible": "",
            },
        }
        response, _ = gateway.namespace.add(**add_args)
        nsid = json.loads(response)["nsid"]
        masked_nsids.append(nsid)
        list_args = {
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsystem_nqn},
        }
        namespace_response, _ = gateway.namespace.list(**list_args)
        ns_visibility = json.loads(namespace_response)["namespaces"][0]["auto_visible"]
        ns_masking.validate_namespace_masking(
            nsid,
            subsystem_nqn,
            namespace_count,
            hostnqn_dict,
            ns_visibility,
            "add",
            False,
        )
        masked_images.append(f"{subsystem_nqn}|{pool}|{image}")

    _validate_masking_initiator_visibility(
        gateway,
        ceph_cluster,
        initiator_node,
        subsystem_nqn,
        listener_port,
        expected_visibility=False,
    )

    for nsid, image_path in zip(masked_nsids, masked_images):
        host_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "nsid": nsid,
                "host": hostnqn,
                "subsystem": subsystem_nqn,
                "force": "",
            },
        }
        gateway.namespace.add_host(**host_args)
        list_args = {
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsystem_nqn},
        }
        namespace_response, _ = gateway.namespace.list(**list_args)
        ns_obj = json.loads(namespace_response)["namespaces"][0]
        ns_masking.validate_namespace_masking(
            nsid,
            subsystem_nqn,
            namespace_count,
            hostnqn_dict,
            ns_obj["hosts"],
            "add_host",
            False,
        )

    # After add_host the initiator is on the NS ACL and should see devices
    # even though auto_visible remains false.
    _validate_masking_initiator_visibility(
        gateway,
        ceph_cluster,
        initiator_node,
        subsystem_nqn,
        listener_port,
        expected_visibility=True,
        expected_device_count=namespace_count,
    )

    for nsid in masked_nsids:
        visibility_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "nsid": nsid,
                "subsystem": subsystem_nqn,
                "auto-visible": "yes",
                "force": "",
            },
        }
        gateway.namespace.change_visibility(**visibility_args)
        list_args = {
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsystem_nqn},
        }
        namespace_response, _ = gateway.namespace.list(**list_args)
        ns_visibility = json.loads(namespace_response)["namespaces"][0]["auto_visible"]
        ns_masking.validate_namespace_masking(
            nsid,
            subsystem_nqn,
            namespace_count,
            hostnqn_dict,
            ns_visibility,
            "change_visibility",
            True,
        )

    _validate_masking_initiator_visibility(
        gateway,
        ceph_cluster,
        initiator_node,
        subsystem_nqn,
        listener_port,
        expected_visibility=True,
        expected_device_count=namespace_count,
    )
    _execute_masking_io(
        ceph_cluster,
        gateway,
        initiator_node,
        subsystem_nqn,
        listener_port,
        masked_images,
        ns_masking,
    )
    Initiator(initiator_host).disconnect_all()

    config["namespace_masking_state"] = {
        "subsystem_nqn": subsystem_nqn,
        "initiator_node": initiator_node,
        "listener_port": listener_port,
        "namespace_count": namespace_count,
        "masked_nsids": masked_nsids,
        "masked_images": masked_images,
    }
    LOG.info(
        "Namespace masking complete: %s masked namespace(s) on %s",
        namespace_count,
        subsystem_nqn,
    )


def _validate_masking_on_gateway(gateway, masking_state):
    """Verify masked namespaces still exist on the gateway (API-only check)."""
    subsystem_nqn = masking_state["subsystem_nqn"]
    expected_count = masking_state["namespace_count"]
    actual_count = _count_subsystem_namespaces(gateway, subsystem_nqn)
    if actual_count < expected_count:
        raise RuntimeError(
            f"Expected at least {expected_count} namespace(s) on {subsystem_nqn} "
            f"after upgrade, found {actual_count}"
        )
    LOG.info(
        "Gateway reports %s namespace(s) on %s (expected >= %s)",
        actual_count,
        subsystem_nqn,
        expected_count,
    )


def _validate_post_upgrade_masking(ceph_cluster, config, nvme_service):
    """Re-validate masked namespace visibility after upgrade IO has stopped."""
    masking_state = config.get("namespace_masking_state")
    if not masking_state:
        if config.get("namespace_masking"):
            raise RuntimeError(
                "namespace_masking is configured but workflow state was not recorded"
            )
        return

    gateway = nvme_service.gateways[0]
    _validate_masking_on_gateway(gateway, masking_state)
    _validate_masking_initiator_visibility(
        gateway,
        ceph_cluster,
        masking_state["initiator_node"],
        masking_state["subsystem_nqn"],
        masking_state["listener_port"],
        expected_visibility=True,
        expected_device_count=masking_state["namespace_count"],
    )
    Initiator(
        get_node_by_id(ceph_cluster, masking_state["initiator_node"])
    ).disconnect_all()
    LOG.info(
        "Post-upgrade namespace masking validated for %s namespace(s) on %s",
        masking_state["namespace_count"],
        masking_state["subsystem_nqn"],
    )


def _run_pre_upgrade_features(ceph_cluster, config, nvme_service, rbd_obj, orch):
    """Run optional pre-upgrade feature validations configured in the suite YAML."""
    if config.get("qos_validation"):
        _run_qos_validation(ceph_cluster, config, nvme_service)
    if config.get("namespace_resize"):
        _run_namespace_resize_validation(
            ceph_cluster, config, nvme_service, rbd_obj, orch
        )
    if config.get("namespace_masking"):
        _run_namespace_masking_workflow(
            ceph_cluster, config, nvme_service, rbd_obj, orch
        )


def _deploy_and_configure(ceph_cluster, config, rbd_obj, kwargs):
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    LOG.info("Check and set NVMe CLI image")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    _normalize_inband_auth_subsystems(config)
    config["initiators"] = _resolve_initiator_configs(config)

    nvme_service = NVMeService(config, ceph_cluster)
    LOG.info("Deploy NVMe-oF service")
    nvme_service.deploy()
    LOG.info("Initialize gateways")
    nvme_service.init_gateways()
    # Keep config in sync so run() finally can teardown if configure fails later
    config["nvme_service"] = nvme_service

    LOG.info(
        "Configure NVMe-oF gateway entities (subsystems, hosts, namespaces, listeners)"
    )
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)
    return nvme_service


def _prepare_initiator_clients(ceph_cluster, config, nvme_service):
    """Connect initiators and validate namespace visibility at clients.

    Runs after pre-upgrade workflows (e.g. namespace masking), so namespace
    lists include any namespaces created during those phases.
    """
    namespaces = fetch_namespaces(nvme_service.gateways[0])

    _prepare_initiator_connection_state(ceph_cluster, config)

    preconfigured = (
        _collect_preconfigured_initiators(config) if _uses_inband_auth(config) else None
    )
    clients = prepare_io_execution(
        config["initiators"],
        gateways=nvme_service.gateways,
        cluster=ceph_cluster,
        return_clients=True,
        pre_configured_initiators=preconfigured,
    )
    _validate_namespace_coverage(clients, namespaces, config)
    return clients


# --- Upgrade orchestration ---


def _is_same_inventory_node(left, right):
    """Return True when two inventory node objects refer to the same host."""
    if left.ip_address and right.ip_address and left.ip_address == right.ip_address:
        return True
    if left.hostname == right.hostname:
        return True
    left_short = left.shortname or left.hostname.split(".")[0]
    right_short = right.shortname or right.hostname.split(".")[0]
    if left_short == right_short:
        return True
    if left.id and right.id and left.id == right.id:
        return True
    return False


def _resolve_orch_host_map(cluster, orch):
    """Map inventory nodes to ceph orch host names."""
    out, _ = orch.shell(args=["ceph", "orch", "host", "ls", "--format=json"])
    orch_hosts = json.loads(out)

    hosts_by_ip = {}
    hosts_by_name = {}
    for entry in orch_hosts:
        name = entry["hostname"]
        hosts_by_name[name] = name
        for addr in entry.get("addr", "").split(","):
            addr = addr.strip()
            if addr:
                hosts_by_ip[addr] = name

    known_hosts = set(hosts_by_name)
    host_map = {}
    for node in cluster.get_nodes(ignore="client"):
        orch_name = hosts_by_ip.get(node.ip_address)
        if not orch_name:
            orch_name = hosts_by_name.get(node.hostname)
        if not orch_name and node.id:
            orch_name = hosts_by_name.get(node.id)
        if not orch_name:
            short = node.shortname or node.hostname.split(".")[0]
            orch_name = hosts_by_name.get(short)
        if not orch_name or orch_name not in known_hosts:
            raise ValueError(
                f"Cannot map inventory node {node.hostname} "
                f"(ip={node.ip_address}, id={node.id}) to a ceph orch host"
            )
        host_map[node] = orch_name
    return host_map


def _registry_login_args(registry):
    return {
        "registry-url": registry["registry"],
        "registry-username": registry["username"],
        "registry-password": registry["password"],
    }


def _registry_podman_login_cmd(registry):
    return (
        f"podman login --username {shlex.quote(registry['username'])} "
        f"--password {shlex.quote(registry['password'])} "
        f"{shlex.quote(registry['registry'])}"
    )


def _registry_cephadm_login_cmd(registry):
    return f"cephadm registry-login {config_dict_to_string(_registry_login_args(registry))}"


def _run_registry_login_on_node(node, registry, orch, timeout):
    """Run podman login and cephadm registry-login on a directly reachable node."""
    Registry(node).login(
        registry["registry"], registry["username"], registry["password"]
    )
    orch.registry_login(
        node=node,
        args=_registry_login_args(registry),
        timeout=timeout,
        long_running=True,
    )


def _node_root_password(node):
    """Return the root password for installer-jump SSH."""
    return getattr(node, "root_passwd", None) or "passwd"


def _ensure_sshpass_on_installer(installer_node):
    """Install sshpass on the installer when password-based SSH is required."""
    out, _ = installer_node.exec_command(
        cmd="command -v sshpass || rpm -q sshpass 2>/dev/null",
        check_ec=False,
    )
    if "sshpass" in out:
        return
    installer_node.exec_command(
        sudo=True,
        cmd="yum -y install sshpass --nogpgcheck",
        long_running=True,
    )


def _run_via_installer_ssh(
    installer_node, orch_host, remote_cmd, timeout, root_password="passwd"
):
    """Run a command on a cluster host by SSH from the installer node.

    Uses sshpass with root credentials because inter-node SSH keys are often
    not configured on freshly bootstrapped IBM/RHEL test clusters.
    """
    _ensure_sshpass_on_installer(installer_node)
    remote_target = f"root@{orch_host}"
    installer_node.exec_command(
        cmd=(
            f"sshpass -p {shlex.quote(root_password)} ssh "
            f"-o StrictHostKeyChecking=no -o ConnectTimeout=120 "
            f"-o UserKnownHostsFile=/dev/null "
            f"{shlex.quote(remote_target)} {shlex.quote(remote_cmd)}"
        ),
        sudo=True,
        long_running=True,
        timeout=timeout,
    )


def _run_registry_login_via_installer(
    installer_node, orch_host, registry, timeout, root_password="passwd"
):
    """Run registry login on a cluster host using installer SSH (outside cephadm shell)."""
    for cmd in (
        _registry_podman_login_cmd(registry),
        _registry_cephadm_login_cmd(registry),
    ):
        _run_via_installer_ssh(
            installer_node, orch_host, cmd, timeout, root_password=root_password
        )


def _install_ibm_license_on_node(node):
    """Install and accept the IBM Storage Ceph license on a reachable node."""
    setup_ibm_licence(node, build_type=None)


def _install_ibm_license_via_installer(
    installer_node, orch_host, timeout, root_password="passwd"
):
    """Install and accept the IBM license using installer SSH."""
    for cmd in (IBM_LICENSE_INSTALL_CMD, IBM_LICENSE_ACCEPT_CMD):
        _run_via_installer_ssh(
            installer_node, orch_host, f"sudo {cmd}", timeout, root_password
        )


def _install_cephadm_on_node(node, timeout):
    """Install cephadm RPM on a node reachable from the test runner."""
    node.exec_command(
        sudo=True,
        cmd=CEPHADM_INSTALL_CMD,
        long_running=True,
        timeout=timeout,
    )
    node.exec_command(cmd=CEPHADM_VERIFY_CMD)


def _install_cephadm_via_installer(
    installer_node, orch_host, timeout, root_password="passwd"
):
    """Install cephadm RPM on a cluster host using installer SSH."""
    _run_via_installer_ssh(
        installer_node, orch_host, f"sudo {CEPHADM_INSTALL_CMD}", timeout, root_password
    )
    _run_via_installer_ssh(
        installer_node, orch_host, CEPHADM_VERIFY_CMD, timeout, root_password
    )


def _comprehensive_cephadm_install(cluster, orch, ibm_build, timeout):
    """Install cephadm on all cluster hosts.

    ``orch.install()`` SSHs from the test runner to every node. Gateway hosts are
    often reachable only from the installer, so non-installer nodes use the same
    installer-jump pattern as registry login.
    """
    installer_node = cluster.get_ceph_object("installer").node
    host_map = _resolve_orch_host_map(cluster, orch)

    for node, orch_host in host_map.items():
        if _is_same_inventory_node(node, installer_node):
            LOG.info(
                "Installing cephadm on %s (direct, timeout=%ss)",
                node.hostname,
                timeout,
            )
            if ibm_build:
                _install_ibm_license_on_node(node)
            _install_cephadm_on_node(node, timeout)
            continue

        LOG.info(
            "Installing cephadm on %s via installer %s (orch host %s, timeout=%ss)",
            node.hostname,
            installer_node.hostname,
            orch_host,
            timeout,
        )
        root_password = _node_root_password(node)
        if ibm_build:
            _install_ibm_license_via_installer(
                installer_node, orch_host, timeout, root_password
            )
        _install_cephadm_via_installer(
            installer_node, orch_host, timeout, root_password
        )


def _login_registry_on_cluster(
    cluster, orch, registry, timeout=DEFAULT_REGISTRY_LOGIN_TIMEOUT
):
    """Log in to the registry on every cluster host."""
    installer_node = cluster.get_ceph_object("installer").node
    host_map = _resolve_orch_host_map(cluster, orch)

    for node, orch_host in host_map.items():
        if _is_same_inventory_node(node, installer_node):
            LOG.info(
                "Registry login on %s (direct, timeout=%ss)",
                node.hostname,
                timeout,
            )
            _run_registry_login_on_node(node, registry, orch, timeout)
            continue

        LOG.info(
            "Registry login on %s via installer %s (orch host %s, timeout=%ss)",
            node.hostname,
            installer_node.hostname,
            orch_host,
            timeout,
        )
        _run_registry_login_via_installer(
            installer_node,
            orch_host,
            registry,
            timeout,
            root_password=_node_root_password(node),
        )


def _comprehensive_upgrade_prerequisites(cluster, orch, **upg_cfg):
    """Upgrade prerequisites with installer-jump registry login for this test only."""
    cdn = upg_cfg.get("cdn", False)
    ibm_build = upg_cfg.get("ibm_build", False)
    overrides = upg_cfg.get("overrides")
    release = upg_cfg.get("release")
    registry = None

    if ibm_build:
        get_configs()
        if not cdn:
            registry = get_registry_credentials("stage", "ibm")
        else:
            registry = get_registry_credentials("cdn", "ibm")
    if registry:
        login_timeout = upg_cfg.get(
            "registry_login_timeout", DEFAULT_REGISTRY_LOGIN_TIMEOUT
        )
        _login_registry_on_cluster(cluster, orch, registry, login_timeout)

    if not cdn:
        if overrides and not cdn:
            override_dict = overrides
            supported_overrides = [
                "grafana",
                "keepalived",
                "haproxy",
                "prometheus",
                "node_exporter",
                "alertmanager",
                "promtail",
                "snmp_gateway",
                "loki",
            ]
            if release and release >= LooseVersion("7.0"):
                supported_overrides += [
                    "nvmeof",
                ]

            for image in supported_overrides:
                image_key = f"{image}_image"
                if override_dict.get(image_key):
                    cmd = f"ceph config set mgr mgr/cephadm/container_image_{image}"
                    cmd += f" {override_dict[image_key]}"
                    orch.shell(args=[cmd])
    else:
        orch.set_cdn_tool_repo(release)


def _upgrade_fio_output_dir(config):
    upgrade_io_cfg = config.get("upgrade_io", {})
    return upgrade_io_cfg.get("fio_output_dir", UPGRADE_FIO_OUTPUT_DIR)


def _fio_process_counts(clients):
    """Return a map of client -> (fio_count, device_count)."""
    counts = {}
    for client in clients:
        out, _ = client.node.exec_command(cmd="pgrep -c fio || true", sudo=True)
        fio_count = int(out.strip() or "0")
        device_count = len(client.list_devices())
        counts[client] = (fio_count, device_count)
    return counts


def _validate_fio_processes(clients, min_processes_per_device=False):
    """Verify FIO is running on initiator nodes."""
    for client, (fio_count, device_count) in _fio_process_counts(clients).items():
        if fio_count < 1:
            raise RuntimeError(
                f"No FIO processes on {client.node.hostname} "
                f"(expected IO on {device_count} device(s))"
            )
        if min_processes_per_device and fio_count < device_count:
            raise RuntimeError(
                f"Insufficient FIO on {client.node.hostname}: "
                f"{fio_count} process(es) for {device_count} device(s)"
            )
        LOG.info(
            "FIO active on %s: %s process(es) across %s device(s)",
            client.node.hostname,
            fio_count,
            device_count,
        )


def _raise_completed_io_task_failures(io_tasks):
    """Surface start_fio failures as soon as the executor task completes."""
    for task in io_tasks:
        if task.done():
            task.result()


def _wait_for_upgrade_fio_ready(clients, io_tasks, timeout=300, poll_interval=5):
    """Poll until background FIO is active on every initiator.

    Executor tasks run ``start_fio``, which blocks until FIO exits. Only
    check pgrep-based readiness here; do not call ``task.result()`` on the
    success path or the main thread would wait for the full FIO runtime
    instead of starting the cluster upgrade.
    """
    LOG.info("Waiting up to %ss for background FIO to start", timeout)
    deadline = time.time() + timeout
    while time.time() < deadline:
        _raise_completed_io_task_failures(io_tasks)
        try:
            _validate_fio_processes(clients, min_processes_per_device=True)
            LOG.info("Background FIO is active on all initiator nodes")
            return
        except RuntimeError:
            time.sleep(poll_interval)
    _raise_completed_io_task_failures(io_tasks)
    raise RuntimeError(
        f"Background FIO did not become ready on all initiators within {timeout}s"
    )


def _print_upgrade_fio_stats(clients, output_dir, require_stats=False):
    """Read FIO JSON artifacts and log per-job IO statistics."""
    LOG.info("Upgrade IO statistics (output_dir=%s)", output_dir)
    stats_found = False

    for client in clients:
        listing, _ = client.node.exec_command(
            cmd=f"ls -1 {output_dir}/*_json 2>/dev/null || true",
            sudo=True,
        )
        json_files = [path for path in listing.splitlines() if path.strip()]
        if not json_files:
            LOG.warning(
                "No FIO JSON output found on %s under %s",
                client.node.hostname,
                output_dir,
            )
            continue

        for json_file in json_files:
            content, _ = client.node.exec_command(cmd=f"cat {json_file}", sudo=True)
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                LOG.warning(
                    "Could not parse FIO output %s on %s",
                    json_file,
                    client.node.hostname,
                )
                continue

            stats_found = True
            for job in data.get("jobs", []):
                write_stats = job.get("write", {})
                read_stats = job.get("read", {})
                LOG.info(
                    "Upgrade IO stats on %s [%s]: "
                    "write_iops=%.2f write_bw_kbps=%.2f write_bytes=%s "
                    "read_iops=%.2f read_bw_kbps=%.2f read_bytes=%s runtime_ms=%s",
                    client.node.hostname,
                    job.get("jobname", json_file),
                    write_stats.get("iops", 0),
                    write_stats.get("bw", 0),
                    write_stats.get("io_bytes", 0),
                    read_stats.get("iops", 0),
                    read_stats.get("bw", 0),
                    read_stats.get("io_bytes", 0),
                    job.get("job_runtime", job.get("elapsed", "n/a")),
                )

    if require_stats and not stats_found:
        raise RuntimeError(
            f"No FIO JSON statistics found under {output_dir} on any initiator"
        )


def _wait_for_fio_exit(clients, timeout=60):
    """Wait until FIO processes exit after a graceful stop."""
    for _ in range(timeout):
        running = sum(count for count, _ in _fio_process_counts(clients).values())
        if running == 0:
            return
        time.sleep(1)
    LOG.warning("Timed out waiting for FIO to exit after %ss", timeout)


def _finalize_upgrade_io(clients, executor, io_tasks, output_dir, require_stats=False):
    """Gracefully stop upgrade FIO, print stats, and shut down the IO executor."""
    LOG.info("Stopping upgrade background FIO and collecting IO statistics")
    for client in clients:
        client.stop_fio()

    _wait_for_fio_exit(clients)
    _print_upgrade_fio_stats(clients, output_dir, require_stats=require_stats)

    if io_tasks:
        LOG.info("Shutting down IO executor")
        executor.shutdown(wait=True, cancel_futures=True)


def _clear_stale_fio(clients):
    """Stop any leftover FIO from earlier phases before upgrade IO starts."""
    for client in clients:
        client.stop_fio()


def _start_continuous_io(config, clients, executor):
    """Submit background FIO jobs on already-connected initiator clients."""
    _clear_stale_fio(clients)
    io_tasks = []
    upgrade_io_cfg = config.get("upgrade_io", {})
    output_dir = _upgrade_fio_output_dir(config)
    fio_defaults = {
        "runtime": upgrade_io_cfg.get("fio_runtime", 10800),
        "io_type": upgrade_io_cfg.get("io_type", "write"),
        "io_size": upgrade_io_cfg.get("io_size", "30%"),
        "iodepth": upgrade_io_cfg.get("iodepth", 1),
        "time_based": True,
        "test_name": "upgrade-io",
        "output_dir": output_dir,
    }

    for client in clients:
        client.node.exec_command(cmd=f"mkdir -p {output_dir}", sudo=True)
        paths = client.list_devices()
        if not paths:
            raise RuntimeError(
                f"No NVMe devices found on {client.node.hostname} for upgrade IO"
            )
        fio_kwargs = dict(fio_defaults)
        if "rwmixread" in upgrade_io_cfg:
            fio_kwargs["rwmixread"] = upgrade_io_cfg["rwmixread"]
        LOG.info(
            "Starting background FIO on %s, paths=%s, fio=%s",
            client.node.hostname,
            paths,
            fio_kwargs,
        )
        io_tasks.append(
            executor.submit(
                client.start_fio,
                paths=paths,
                **fio_kwargs,
            )
        )

    return io_tasks, clients, output_dir


def _upgrade_settings(config, kwargs, ibm_build):
    """Build upgrade helper config shared by prepare/execute upgrade steps."""
    upgrade = config["upgrade"]
    upgrade_io_cfg = config.get("upgrade_io", {})
    return {
        "release": upgrade.get("release") or config.get("rhbuild"),
        "cdn": upgrade["cdn"],
        "overrides": kwargs.get("test_data", {}).get("custom_config_dict"),
        "ibm_build": ibm_build,
        "container_image": config.get("container_image"),
        "registry_login_timeout": upgrade_io_cfg.get(
            "registry_login_timeout", DEFAULT_REGISTRY_LOGIN_TIMEOUT
        ),
        "cephadm_install_timeout": upgrade_io_cfg.get(
            "cephadm_install_timeout",
            upgrade_io_cfg.get(
                "registry_login_timeout", DEFAULT_CEPHADM_INSTALL_TIMEOUT
            ),
        ),
    }


def _prepare_upgrade(ceph_cluster, config, orch, kwargs, ibm_build):
    """Run registry login, cephadm install, and upgrade-check before heavy IO.

    Running these steps while continuous FIO is active can cause SSH/registry
    operations to time out on busy gateway nodes.
    """
    upg_cfg = _upgrade_settings(config, kwargs, ibm_build)

    LOG.info("Upgrade prerequisites (registry login and repo setup)")
    _comprehensive_upgrade_prerequisites(ceph_cluster, orch, **upg_cfg)

    LOG.info("Install/upgrade cephadm on cluster nodes")
    _comprehensive_cephadm_install(
        ceph_cluster,
        orch,
        ibm_build,
        upg_cfg["cephadm_install_timeout"],
    )

    LOG.info("Verify upgrade target images are available")
    orch.upgrade_check(image=upg_cfg["container_image"])


def _execute_upgrade(orch, config):
    """Start and monitor cluster upgrade while background IO continues.

    Mutates ``config`` in place by setting ``config["args"]["image"]`` to
    ``"latest"`` for ``orch.start_upgrade``.
    """
    LOG.info("Start cluster upgrade with NVMe IO in background")
    config.update({"args": {"image": "latest"}})
    orch.start_upgrade(config)
    orch.monitor_upgrade_status()


def _validate_post_upgrade(nvme_service, pre_upg_versions, config, clients):
    upgrade_io_cfg = config.get("upgrade_io", {})
    post_sleep = upgrade_io_cfg.get("post_upgrade_sleep", 60)
    LOG.info(
        "Waiting %ss for NVMe-oF daemons to stabilize after upgrade",
        post_sleep,
    )
    time.sleep(post_sleep)

    nvme_service.gateways = []
    nvme_service.init_gateways()
    post_upg_versions = fetch_nvme_versions(nvme_service.gateways)
    compare_nvme_versions(pre_upg_versions, post_upg_versions)

    LOG.info("Verifying background FIO survived cluster upgrade")
    _validate_fio_processes(clients, min_processes_per_device=True)

    if config.get("namespace_masking_state"):
        LOG.info("Verifying masked namespaces on gateway during upgrade IO")
        _validate_masking_on_gateway(
            nvme_service.gateways[0], config["namespace_masking_state"]
        )


def _perform_ha_operations(ceph_cluster, config, nvme_service):
    if not config.get("fault-injection-methods"):
        LOG.info("No fault-injection-methods configured; skipping HA phase")
        return

    LOG.info("Starting post-upgrade HA failover and failback")
    _prepare_initiator_connection_state(ceph_cluster, config)

    config.update({"nvme_service": nvme_service})
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways

    ha.config["initiators"] = config["initiators"]
    if _uses_inband_auth(config):
        preconfigured = _collect_preconfigured_initiators(config)
        if preconfigured:
            ha.config["pre_configured_initiators"] = preconfigured

    if config.get("iodepth"):
        for depth in config["iodepth"]:
            ha.run(iodepth=int(depth))
    else:
        ha.run(iodepth=2)


def _stop_background_io(io_tasks, initiator_objs, executor):
    if io_tasks:
        _raise_completed_io_task_failures(io_tasks)
    if not io_tasks and not initiator_objs:
        return
    LOG.info("Stopping background FIO on all initiator nodes")
    for initiator_obj in initiator_objs:
        initiator_obj.stop_fio()
    LOG.info("Shutting down IO executor")
    executor.shutdown(wait=True, cancel_futures=True)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Execute comprehensive NVMe-oF upgrade with IO and post-upgrade HA.

    Example suite config::

        config:
          install: true
          gw_group: gw_group1
          gw_nodes: [node6, node7, node8, node9]
          subsystems: [...]          # multi-subsystem feature matrix
          initiators: [...]          # IO clients
          fault-injection-methods: [...]
          namespace_masking:        # min 2 namespaces; validated through upgrade
            subsystem_nqn: nqn.2016-06.io.spdk:cnode6
            initiator_node: node10
            namespace_count: 2
            image_size: 5G
          upgrade:
            cdn: false
            release: null
          upgrade_io:
            registry_login_timeout: 1200
            cephadm_install_timeout: 1200
            fio_ready_timeout: 300
            post_upgrade_sleep: 60
            fio_runtime: 10800
            io_type: write
            io_size: 30%
            iodepth: 1
            require_fio_stats: false
    """
    LOG.info("Starting comprehensive NVMe-oF upgrade E2E test")
    config = kwargs["config"]
    ctm = config["manifest"]
    ibm_build = ctm.product == "ibm"

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    orch = Orch(cluster=ceph_cluster, **config)

    nvme_service = None
    executor = ThreadPoolExecutor()
    io_tasks = []
    initiator_objs = []
    fio_output_dir = UPGRADE_FIO_OUTPUT_DIR
    upgrade_io_finalized = False

    try:
        if not config.get("install"):
            raise ValueError("install: true is required for comprehensive upgrade test")

        nvme_service = _deploy_and_configure(ceph_cluster, config, rbd_obj, kwargs)
        pre_upg_versions = fetch_nvme_versions(nvme_service.gateways)

        _run_pre_upgrade_features(ceph_cluster, config, nvme_service, rbd_obj, orch)

        clients = _prepare_initiator_clients(ceph_cluster, config, nvme_service)

        _prepare_upgrade(ceph_cluster, config, orch, kwargs, ibm_build)

        io_tasks, initiator_objs, fio_output_dir = _start_continuous_io(
            config, clients, executor
        )
        upgrade_io_cfg = config.get("upgrade_io", {})
        fio_ready_timeout = upgrade_io_cfg.get("fio_ready_timeout", 300)
        _wait_for_upgrade_fio_ready(clients, io_tasks, timeout=fio_ready_timeout)
        LOG.info("Starting cluster upgrade with background FIO in parallel")
        _execute_upgrade(orch, config)
        _validate_post_upgrade(nvme_service, pre_upg_versions, config, clients)

        require_fio_stats = upgrade_io_cfg.get("require_fio_stats", False)
        _finalize_upgrade_io(
            initiator_objs,
            executor,
            io_tasks,
            fio_output_dir,
            require_stats=require_fio_stats,
        )
        upgrade_io_finalized = True
        io_tasks = []
        initiator_objs = []

        _validate_post_upgrade_masking(ceph_cluster, config, nvme_service)
        _perform_ha_operations(ceph_cluster, config, nvme_service)
        LOG.info("Comprehensive NVMe-oF upgrade E2E test completed successfully")
        return 0
    except Exception as err:
        LOG.error(
            "Comprehensive NVMe-oF upgrade E2E test failed: %s",
            err,
            exc_info=True,
        )
        return 1
    finally:
        if not upgrade_io_finalized:
            _stop_background_io(io_tasks, initiator_objs, executor)
        service_to_clean = nvme_service or config.get("nvme_service")
        if config.get("cleanup") and service_to_clean is not None:
            LOG.info("Running NVMe-oF teardown (cleanup=%s)", config.get("cleanup"))
            try:
                teardown(service_to_clean, rbd_obj)
            except Exception as teardown_err:
                LOG.error(
                    "Teardown in finally failed: %s",
                    teardown_err,
                    exc_info=True,
                )
