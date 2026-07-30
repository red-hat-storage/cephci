"""
Holistic E2E for NVMeoF Auto-add listeners (ISCE-2203) and
Refresh Network for Auto Listeners (ISCE-5078).

Generic dual-IPv4 baremetal flow (no hardcoded iface names):
  1. Discover IPv4 interfaces via ``ip a`` on each GW (require >= 2)
  2. Primary = IPv4 hosting the GW (node.ip_address) → minimal network-mask
  3. Secondary = next IPv4 set on another iface → minimal secondary mask
  4. Create 4 subsystems with primary mask → validate auto listeners
  5. Scale-down / scale-up → validate auto listeners
  6. Take secondary iface IPv4s DOWN on all GWs, then add_network(secondary)
     → mask present but NO secondary listeners (IPs gone)
  7. Bring secondary IPv4s UP; scoped refresh_network → ADD secondary listeners
  8. Initiator ``nvme list-subsys`` + IO
  9. Take secondary DOWN; scoped refresh → DELETE secondary listeners
 10. list-subsys + IO
 11. Bring secondary UP; scoped refresh → ADD again
 12. del_network + scoped refresh → secondary listeners gone
 13. Dummy out-of-subnet add_network + refresh → listeners unchanged

Why iface flap: add_network while secondary IPs are live can pre-create
listeners, so refresh would be a no-op. Flapping IPs forces refresh to be
the mechanism that adds/removes auto-listeners (ISCE-5078 intent).

CLI notes:
  - ``gateway refresh_network --subsystem <nqn>`` is required and local to one GW
"""

import ipaddress
import time
from copy import deepcopy

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_nodes_by_ids
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    disconnect_initiators,
    teardown,
    validate_listeners,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    discover_gateway_network_roles,
    get_listener_traddrs,
    get_subsystem_network_masks,
    refresh_gateway_network,
    secondary_iface_state_on_gateways,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster, run_fio

LOG = Log(__name__)


def _redeploy_gateways(nvme_service, gw_node_ids):
    """Redeploy NVMeoF service on the given gateway node ids (scale up/down)."""
    if not isinstance(gw_node_ids, list):
        gw_node_ids = [gw_node_ids]
    LOG.info(f"Redeploying NVMeoF gateways on: {gw_node_ids}")
    nvme_service.config["gw_nodes"] = gw_node_ids
    nvme_service.gw_nodes = get_nodes_by_ids(nvme_service.ceph_cluster, gw_node_ids)
    nvme_service.deploy()
    nvme_service.gateways = []
    nvme_service.init_gateways()
    return nvme_service.gateways


def _run_io(ceph_cluster, gateway, initiator_cfg):
    """Connect initiator and run a short FIO workload."""
    client = get_node_by_id(ceph_cluster, initiator_cfg["node"])
    initiator = NVMeInitiator(client)
    cfg = {
        "nqn": initiator_cfg.get("nqn", "connect-all"),
        "listener_port": initiator_cfg.get("listener_port", 4420),
    }
    initiator.connect_targets(gateway, cfg)
    _log_nvme_list_subsys(client)
    paths = initiator.list_devices()
    if not paths:
        raise RuntimeError(f"No NVMe devices on {client.hostname}")

    io_args = initiator_cfg.get("io_args", {"size": "100M", "runtime": 30})
    with parallel() as p:
        for path in paths:
            p.spawn(
                run_fio,
                **{
                    **io_args,
                    "device_name": path,
                    "client_node": client,
                    "long_running": True,
                    "cmd_timeout": "notimeout",
                },
            )
        for op in p:
            if isinstance(op, int) and op != 0:
                raise RuntimeError(f"FIO failed with exit code: {op}")
    disconnect_initiators(None, node=client)
    return paths


def _log_nvme_list_subsys(client):
    """Capture initiator path view for logs (ISCE-5078 validation aid)."""
    out, err = client.exec_command(cmd="nvme list-subsys", sudo=True, check_ec=False)
    LOG.info(
        f"[{client.hostname}] nvme list-subsys:\n{(out or '').strip()}\n"
        f"stderr={(err or '').strip()}"
    )


def _log_list_subsys_all(ceph_cluster, initiator_cfgs):
    for init_cfg in initiator_cfgs or []:
        client = get_node_by_id(ceph_cluster, init_cfg["node"])
        _log_nvme_list_subsys(client)


def _assert_secondary_listeners(listeners, secondary_ips, present, context):
    """Assert secondary IPs are present or absent in a listener traddr list."""
    for ip in secondary_ips:
        if present and ip not in listeners:
            raise RuntimeError(
                f"Expected secondary listener {ip} {context}; have {listeners}"
            )
        if not present and ip in listeners:
            raise RuntimeError(
                f"Secondary listener {ip} unexpectedly present {context}; "
                f"have {listeners}"
            )


def _assert_primary_listeners(listeners, primary_ips, context):
    for ip in primary_ips:
        if ip not in listeners:
            raise RuntimeError(
                f"Primary listener {ip} missing {context}; have {listeners}"
            )


def _primary_listener_ips(gateways):
    """Primary IPs used for auto-listeners at subsystem create."""
    return sorted(
        {gw.node.ip_address for gw in gateways if getattr(gw.node, "ip_address", None)}
    )


def _expected_auto_listeners(gateways, listener_port):
    expected = []
    for gateway in gateways:
        expected.append(
            {
                "traddr": gateway.node.ip_address,
                "trsvcid": listener_port,
                "host-name": gateway.node.hostname,
            }
        )
    return expected


def _validate_auto_listeners(nvme_service, nqn, listener_port):
    """Ensure each GW has an auto-listener on its primary IP for the subsystem."""
    gateway = nvme_service.gateways[0]
    expected = _expected_auto_listeners(nvme_service.gateways, listener_port)
    validate_listeners(gateway, expected, nqn)
    LOG.info(
        f"Auto-listeners validated for {nqn}: {get_listener_traddrs(gateway, nqn)}"
    )


def _validate_all_subsystems_auto_listeners(nvme_service, subsystems):
    """Validate primary auto-listeners on every configured subsystem."""
    for sub in subsystems:
        nqn = sub["nqn"]
        port = sub.get("listener_port", 4420)
        _validate_auto_listeners(nvme_service, nqn, port)


def _assert_refresh_ok(result, context):
    if result.get("status", 0) not in (0, "0", None):
        raise RuntimeError(f"refresh_network failed ({context}): {result}")


def _refresh_subsystem_on_all_gateways(gateways, nqn, context):
    """
    Run refresh_network for ONE subsystem on EACH gateway separately.

    CLI is local to a single gateway and requires --subsystem, so each GW must
    be refreshed independently for that subsystem only.
    """
    LOG.info(
        f"refresh_network for subsystem {nqn} on each gateway separately ({context})"
    )
    for gw in gateways:
        LOG.info(f"  -> {gw.node.hostname}: refresh_network --subsystem {nqn}")
        result = refresh_gateway_network(gw, nqn)
        _assert_refresh_ok(result, f"{context} on {gw.node.hostname} for {nqn}")


def _snapshot_listeners(gateway, nqns):
    """Return {nqn: [traddrs]} for the given subsystems."""
    return {nqn: get_listener_traddrs(gateway, nqn) for nqn in nqns}


def _assert_listeners_unchanged(before, after, context):
    """Fail if any subsystem listener set changed."""
    for nqn in before:
        if after.get(nqn) != before[nqn]:
            raise RuntimeError(
                f"Listeners changed after {context} on {nqn}: "
                f"{before[nqn]} -> {after.get(nqn)}"
            )


def _pick_dummy_network_mask(refresh_cfg, primary_mask, secondary_mask, gw_ips):
    """
    Pick an out-of-subnet dummy mask that does not cover any GW IPv4.

    Default: RFC5737 TEST-NET-1 ``192.0.2.0/24``. Override via
    ``refresh_network.dummy_network_mask``.
    """
    dummy = refresh_cfg.get("dummy_network_mask", "192.0.2.0/24")
    try:
        net = ipaddress.ip_network(dummy, strict=False)
    except ValueError as exc:
        raise ValueError(f"Invalid dummy_network_mask {dummy}: {exc}") from exc

    if str(net) in (primary_mask, secondary_mask):
        raise RuntimeError(
            f"dummy_network_mask {net} collides with primary/secondary masks"
        )
    for ip in gw_ips:
        if ipaddress.ip_address(ip) in net:
            raise RuntimeError(
                f"dummy_network_mask {net} covers GW IP {ip}; pick another CIDR"
            )
    return str(net)


def _add_network_all(gateway, nqns, network_mask):
    """Add network-mask to every subsystem."""
    for nqn in nqns:
        LOG.info(f"add_network {network_mask} on subsystem {nqn}")
        gateway.subsystem.add_network(
            **{"args": {"subsystem": nqn, "network-mask": network_mask}}
        )
        masks = get_subsystem_network_masks(gateway, nqn)
        if network_mask not in masks:
            raise RuntimeError(f"Failed to add {network_mask} to {nqn}; have {masks}")


def _del_network_all(gateway, nqns, network_mask):
    """Delete network-mask from every subsystem."""
    for nqn in nqns:
        LOG.info(f"del_network {network_mask} on subsystem {nqn}")
        gateway.subsystem.del_network(
            **{"args": {"subsystem": nqn, "network-mask": network_mask}}
        )
        masks = get_subsystem_network_masks(gateway, nqn)
        if network_mask in masks:
            raise RuntimeError(
                f"Network mask {network_mask} still present on {nqn}: {masks}"
            )


def _resolve_networks(gateways, refresh_cfg):
    """
    Discover primary/secondary IPv4 roles from ``ip a`` on GW nodes.

    Optional suite overrides:
      refresh_network.secondary_iface
      refresh_network.secondary_network_mask
      refresh_network.primary_network_mask
    """
    roles = discover_gateway_network_roles(
        gateways, secondary_iface=refresh_cfg.get("secondary_iface")
    )
    primary_mask = refresh_cfg.get("primary_network_mask") or roles["primary_mask"]
    secondary_mask = (
        refresh_cfg.get("secondary_network_mask") or roles["secondary_mask"]
    )
    if not primary_mask or not secondary_mask:
        raise RuntimeError(
            f"Unable to derive network masks from GW ip a "
            f"(primary={primary_mask}, secondary={secondary_mask})"
        )
    if primary_mask == secondary_mask:
        raise RuntimeError(
            f"Primary and secondary network masks resolved to the same CIDR "
            f"{primary_mask}; GWs need IPv4s in two distinct subnets"
        )
    return {
        "primary_ips": roles["primary_ips"],
        "secondary_ips": roles["secondary_ips"],
        "primary_mask": primary_mask,
        "secondary_mask": secondary_mask,
        "secondary_iface": roles["secondary_iface"],
        "per_gateway": roles["per_gateway"],
    }


def test_refresh_network_auto_listeners(ceph_cluster, config, rbd_obj):
    """
    Multi-subsystem ISCE-2203 + ISCE-5078 workflow with generic iface discovery.

    Suite config:
      subsystems: 4 entries
      refresh_network:
        refresh_subsystem: nqn to refresh (required for scoped validation)
        secondary_iface / primary_network_mask / secondary_network_mask: optional
        dummy_network_mask: optional out-of-subnet CIDR (default 192.0.2.0/24)
    """
    nvme_service = NVMeService(config, ceph_cluster)

    if config.get("install"):
        LOG.info("Deploying NVMeoF gateway service")
        nvme_service.deploy()

    nvme_service.init_gateways()
    ceph_version = get_ceph_version_from_cluster(nvme_service.clients[0])
    if LooseVersion(ceph_version) < LooseVersion("20.2.1"):
        raise RuntimeError(
            "Auto-listeners / refresh_network require ceph >= 20.2.1 "
            f"(found {ceph_version})"
        )

    subsystems = config["subsystems"]
    if len(subsystems) < 2:
        raise ValueError("At least 2 subsystems required (prefer 4) for scoped refresh")

    all_nqns = [s["nqn"] for s in subsystems]
    refresh_cfg = config.get("refresh_network", {}) or {}

    networks = _resolve_networks(nvme_service.gateways, refresh_cfg)
    primary_mask = networks["primary_mask"]
    secondary_mask = networks["secondary_mask"]
    primary_ips = networks["primary_ips"]
    secondary_ip_list = networks["secondary_ips"]

    for sub in subsystems:
        sub["network_mask"] = primary_mask

    refresh_nqn = refresh_cfg.get("refresh_subsystem") or all_nqns[0]
    if refresh_nqn not in all_nqns:
        raise ValueError(
            f"refresh_network.refresh_subsystem {refresh_nqn} "
            f"not in configured subsystems {all_nqns}"
        )
    other_nqns = [nqn for nqn in all_nqns if nqn != refresh_nqn]

    LOG.info(
        f"Configuring {len(subsystems)} subsystems with discovered primary "
        f"network-mask {primary_mask}; secondary_mask={secondary_mask}; "
        f"refresh scoped to {refresh_nqn} only"
    )
    configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

    gateway = nvme_service.gateways[0]

    # --- ISCE-2203: auto listeners on all subsystems (primary IPv4) ---
    _validate_all_subsystems_auto_listeners(nvme_service, subsystems)
    for nqn in all_nqns:
        masks = get_subsystem_network_masks(gateway, nqn)
        if primary_mask not in masks:
            raise RuntimeError(
                f"Expected primary network_mask {primary_mask} on {nqn}, have {masks}"
            )

    LOG.info(f"Primary auto-listeners OK on all subsystems; primary_ips={primary_ips}")

    LOG.info("Run IO after primary auto-listeners")
    for init_cfg in config.get("initiators", []):
        _run_io(ceph_cluster, gateway, init_cfg)

    # --- ISCE-2203: scale-down / scale-up ---
    all_gw_nodes = list(config.get("gw_nodes", []))
    for step in config.get("load_balancing", []):
        if step.get("scale_down"):
            remaining = [
                n
                for n in nvme_service.config["gw_nodes"]
                if n not in step["scale_down"]
            ]
            LOG.info(
                f"Scale-down gateways {step['scale_down']} -> remaining {remaining}"
            )
            _redeploy_gateways(nvme_service, remaining)
            time.sleep(15)
            _validate_all_subsystems_auto_listeners(nvme_service, subsystems)
        if step.get("scale_up"):
            scaled = list(
                dict.fromkeys(list(nvme_service.config["gw_nodes"]) + step["scale_up"])
            )
            if set(step["scale_up"]).issubset(set(all_gw_nodes)):
                scaled = list(dict.fromkeys(all_gw_nodes))
            LOG.info(f"Scale-up gateways {step['scale_up']} -> {scaled}")
            _redeploy_gateways(nvme_service, scaled)
            time.sleep(15)
            _validate_all_subsystems_auto_listeners(nvme_service, subsystems)

    gateway = nvme_service.gateways[0]
    # Re-discover after scale (GW set / IPs may have changed)
    networks = _resolve_networks(nvme_service.gateways, refresh_cfg)
    primary_ips = networks["primary_ips"]
    secondary_ip_list = networks["secondary_ips"]
    # Keep original primary_mask used at subsystem create; secondary may be recalculated
    if not refresh_cfg.get("secondary_network_mask"):
        secondary_mask = networks["secondary_mask"]

    # ------------------------------------------------------------------
    # ISCE-5078: exercise refresh via secondary iface IP flap
    # add_network while IPs are live can pre-create listeners; take IPs
    # down first so only refresh_network adds/removes them.
    # Always restore secondary IPv4s in finally so a mid-section failure
    # does not leave GWs without secondary addresses for later jobs.
    # ------------------------------------------------------------------
    try:
        LOG.info(
            f"Taking secondary iface IPv4s DOWN on all GWs before add_network "
            f"(mask={secondary_mask}, ips={secondary_ip_list})"
        )
        secondary_iface_state_on_gateways(nvme_service.gateways, networks, state="down")

        LOG.info(
            f"add_network {secondary_mask} on ALL subsystems while secondary IPs are down"
        )
        _add_network_all(gateway, all_nqns, secondary_mask)
        for nqn in all_nqns:
            masks = get_subsystem_network_masks(gateway, nqn)
            if primary_mask not in masks or secondary_mask not in masks:
                raise RuntimeError(
                    f"Expected both masks on {nqn} after add_network; have {masks}"
                )
            _assert_secondary_listeners(
                get_listener_traddrs(gateway, nqn),
                secondary_ip_list,
                present=False,
                context=f"after add_network with secondary DOWN on {nqn}",
            )
        LOG.info("add_network OK: secondary mask present, no secondary listeners yet")

        LOG.info(
            "Bringing secondary iface IPv4s UP on all GWs (listeners should stay stale)"
        )
        secondary_iface_state_on_gateways(nvme_service.gateways, networks, state="up")
        time.sleep(5)

        listeners_before_refresh = _snapshot_listeners(gateway, all_nqns)
        for nqn in all_nqns:
            _assert_secondary_listeners(
                listeners_before_refresh[nqn],
                secondary_ip_list,
                present=False,
                context=(
                    f"after secondary UP but BEFORE refresh on {nqn} "
                    f"(refresh must be what adds listeners)"
                ),
            )

        # --- refresh ONLY one subsystem, on EACH gateway → ADD secondary ---
        _refresh_subsystem_on_all_gateways(
            nvme_service.gateways, refresh_nqn, "post-secondary-up"
        )
        listeners_after_refresh = _snapshot_listeners(gateway, all_nqns)
        _assert_primary_listeners(
            listeners_after_refresh[refresh_nqn],
            primary_ips,
            f"on refreshed {refresh_nqn} after secondary-up refresh",
        )
        _assert_secondary_listeners(
            listeners_after_refresh[refresh_nqn],
            secondary_ip_list,
            present=True,
            context=f"on refreshed {refresh_nqn} after secondary-up refresh",
        )
        for nqn in other_nqns:
            if listeners_after_refresh[nqn] != listeners_before_refresh[nqn]:
                raise RuntimeError(
                    f"Listeners on non-refreshed subsystem {nqn} changed after "
                    f"refresh of {refresh_nqn}: {listeners_before_refresh[nqn]} -> "
                    f"{listeners_after_refresh[nqn]}"
                )
            _assert_secondary_listeners(
                listeners_after_refresh[nqn],
                secondary_ip_list,
                present=False,
                context=f"on non-refreshed {nqn} (scoped --subsystem)",
            )
        LOG.info(
            f"Scoped refresh ADD OK on {refresh_nqn}; secondary={secondary_ip_list}; "
            f"other subsystems unchanged"
        )

        LOG.info(
            "Initiator list-subsys + IO after secondary listeners added via refresh"
        )
        _log_list_subsys_all(ceph_cluster, config.get("initiators", []))
        for init_cfg in config.get("initiators", []):
            _run_io(ceph_cluster, gateway, init_cfg)

        # --- secondary DOWN → refresh → DELETE secondary listeners ---
        LOG.info(
            "Taking secondary iface IPv4s DOWN; refresh should remove secondary listeners"
        )
        secondary_iface_state_on_gateways(nvme_service.gateways, networks, state="down")
        time.sleep(5)
        listeners_before_down_refresh = _snapshot_listeners(gateway, all_nqns)
        # Stale until refresh: secondary should still be listed on refresh_nqn
        _assert_secondary_listeners(
            listeners_before_down_refresh[refresh_nqn],
            secondary_ip_list,
            present=True,
            context=(
                f"on {refresh_nqn} after secondary DOWN but BEFORE refresh "
                f"(stale listeners until refresh)"
            ),
        )

        _refresh_subsystem_on_all_gateways(
            nvme_service.gateways, refresh_nqn, "post-secondary-down"
        )
        listeners_after_down_refresh = _snapshot_listeners(gateway, all_nqns)
        _assert_primary_listeners(
            listeners_after_down_refresh[refresh_nqn],
            primary_ips,
            f"on {refresh_nqn} after secondary-down refresh",
        )
        _assert_secondary_listeners(
            listeners_after_down_refresh[refresh_nqn],
            secondary_ip_list,
            present=False,
            context=f"on {refresh_nqn} after secondary-down refresh",
        )
        for nqn in other_nqns:
            if listeners_after_down_refresh[nqn] != listeners_before_down_refresh[nqn]:
                raise RuntimeError(
                    f"Listeners on non-refreshed subsystem {nqn} changed after "
                    f"down-refresh of {refresh_nqn}"
                )
        LOG.info(f"Scoped refresh DELETE OK on {refresh_nqn} after secondary DOWN")

        LOG.info(
            "Initiator list-subsys + IO after secondary listeners removed via refresh"
        )
        _log_list_subsys_all(ceph_cluster, config.get("initiators", []))
        for init_cfg in config.get("initiators", []):
            _run_io(ceph_cluster, gateway, init_cfg)

        # --- secondary UP again → refresh → ADD secondary listeners ---
        LOG.info(
            "Bringing secondary iface IPv4s UP again; refresh should re-add listeners"
        )
        secondary_iface_state_on_gateways(nvme_service.gateways, networks, state="up")
        time.sleep(5)
        _refresh_subsystem_on_all_gateways(
            nvme_service.gateways, refresh_nqn, "post-secondary-up-again"
        )
        listeners_after_reup = _snapshot_listeners(gateway, all_nqns)
        _assert_secondary_listeners(
            listeners_after_reup[refresh_nqn],
            secondary_ip_list,
            present=True,
            context=f"on {refresh_nqn} after secondary re-UP refresh",
        )
        for nqn in other_nqns:
            _assert_secondary_listeners(
                listeners_after_reup[nqn],
                secondary_ip_list,
                present=False,
                context=f"on non-refreshed {nqn} after secondary re-UP",
            )

        LOG.info("Initiator list-subsys + IO after secondary listeners re-added")
        _log_list_subsys_all(ceph_cluster, config.get("initiators", []))
        for init_cfg in config.get("initiators", []):
            _run_io(ceph_cluster, gateway, init_cfg)

        # --- del_network on ALL subsystems → scoped refresh cleans secondary ---
        LOG.info(f"del_network {secondary_mask} on ALL subsystems")
        _del_network_all(gateway, all_nqns, secondary_mask)
        for nqn in all_nqns:
            masks = get_subsystem_network_masks(gateway, nqn)
            if secondary_mask in masks:
                raise RuntimeError(f"{secondary_mask} still on {nqn}: {masks}")
            if primary_mask not in masks:
                raise RuntimeError(f"Primary mask missing on {nqn} after del: {masks}")

        listeners_before_del_refresh = _snapshot_listeners(gateway, all_nqns)
        _refresh_subsystem_on_all_gateways(
            nvme_service.gateways, refresh_nqn, "post-del-network"
        )
        listeners_after_del_refresh = _snapshot_listeners(gateway, all_nqns)
        _assert_secondary_listeners(
            listeners_after_del_refresh[refresh_nqn],
            secondary_ip_list,
            present=False,
            context=f"on {refresh_nqn} after del_network+refresh",
        )
        _assert_primary_listeners(
            listeners_after_del_refresh[refresh_nqn],
            primary_ips,
            f"on {refresh_nqn} after del_network+refresh",
        )
        for nqn in other_nqns:
            if listeners_after_del_refresh[nqn] != listeners_before_del_refresh[nqn]:
                raise RuntimeError(
                    f"Listeners on non-refreshed subsystem {nqn} changed after "
                    f"del refresh of {refresh_nqn}: "
                    f"{listeners_before_del_refresh[nqn]} -> "
                    f"{listeners_after_del_refresh[nqn]}"
                )

        LOG.info(
            f"del_network + scoped refresh OK on {refresh_nqn}; "
            f"primary listeners remain; other subsystems untouched"
        )
    finally:
        # Always restore secondary IPv4s (even on assertion/refresh failure)
        try:
            LOG.info(
                "Restoring secondary iface IPv4s on all GWs "
                "(post ISCE-5078 flap, success or failure)"
            )
            secondary_iface_state_on_gateways(
                nvme_service.gateways, networks, state="up"
            )
        except Exception as restore_exc:  # noqa: BLE001
            LOG.warning(f"Secondary iface restore after IP flap section: {restore_exc}")

    # --- Dummy out-of-subnet network (last): refresh must NOT change listeners ---
    dummy_mask = _pick_dummy_network_mask(
        refresh_cfg,
        primary_mask,
        secondary_mask,
        primary_ips + secondary_ip_list,
    )
    LOG.info(
        f"add_network dummy out-of-subnet mask {dummy_mask} on ALL subsystems; "
        f"scoped refresh must leave listeners unchanged"
    )
    _add_network_all(gateway, all_nqns, dummy_mask)
    listeners_before_dummy = _snapshot_listeners(gateway, all_nqns)
    _refresh_subsystem_on_all_gateways(
        nvme_service.gateways, refresh_nqn, "post-dummy-add-network"
    )
    listeners_after_dummy = _snapshot_listeners(gateway, all_nqns)
    _assert_listeners_unchanged(
        listeners_before_dummy,
        listeners_after_dummy,
        f"dummy add_network+refresh ({dummy_mask})",
    )
    LOG.info(
        f"Dummy network {dummy_mask}: listeners unchanged after scoped refresh "
        f"on {refresh_nqn}"
    )
    _del_network_all(gateway, all_nqns, dummy_mask)
    listeners_before_dummy_del = _snapshot_listeners(gateway, all_nqns)
    _refresh_subsystem_on_all_gateways(
        nvme_service.gateways, refresh_nqn, "post-dummy-del-network"
    )
    listeners_after_dummy_del = _snapshot_listeners(gateway, all_nqns)
    _assert_listeners_unchanged(
        listeners_before_dummy_del,
        listeners_after_dummy_del,
        f"dummy del_network+refresh ({dummy_mask})",
    )
    LOG.info(
        f"Dummy network {dummy_mask} removed; listeners still unchanged after refresh"
    )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point for cephci suite execution."""
    LOG.info("Starting NVMeoF Auto-listeners + Refresh Network holistic E2E")
    config = deepcopy(kwargs["config"])
    rbd_pools = initial_rbd_config(**kwargs)
    if not rbd_pools or not rbd_pools.get("rbd_reppool"):
        LOG.error(
            "RBD pool setup failed (initial_rbd_config returned %s). "
            "Check earlier 'Pool creation failed' logs — usually "
            "`ceph osd pool create <pool> 64 64` failed because the pool "
            "already exists, the cluster lacks capacity for 64 PGs, or "
            "ceph CLI access from the client node failed.",
            rbd_pools,
        )
        return 1
    rbd_obj = rbd_pools["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    try:
        if config.get("cleanup-only"):
            nvme_service = NVMeService(config, ceph_cluster)
            nvme_service.init_gateways()
            return teardown(nvme_service, rbd_obj)

        test_refresh_network_auto_listeners(ceph_cluster, config, rbd_obj)
        return 0
    except Exception as exc:  # noqa: BLE001
        LOG.error(f"Refresh-network / auto-listeners E2E failed: {exc}")
        return 1
    finally:
        if config.get("cleanup") and not config.get("cleanup-only"):
            try:
                nvme_service = nvme_service or NVMeService(config, ceph_cluster)
                if not getattr(nvme_service, "gateways", None):
                    nvme_service.init_gateways()
                teardown(nvme_service, rbd_obj)
            except Exception as cleanup_exc:  # noqa: BLE001
                LOG.warning(f"Teardown warning: {cleanup_exc}")
