"""
Stretch-cluster NVMe-oF test module.

- Deploy two-site stretch cluster with tie-breaker (CRUSH rule, election strategy, stretch mode).
- Deploy NVMe service with gateways per datacenter (DC1, DC2), configure subsystems, listeners,
  hosts, namespaces with location, and initiators.
- Test GW location set/get/modify/unset (ceph nvme-gw set-location, show).
- Test namespace locations: valid location + ANA + IO, invalid reject, change_location + ANA + IO.
- Single gateway failover (`ceph orch daemon stop/start`) and admin disable/enable with same-location preference and IO.
- Test homeless namespaces: unset all GWs for a location, verify alerts/LOA/IO blocked,
  recover via change_location, rebalance to another location, then add a GW back to a location.
- Relocate ANA groups of a site's failed gateways onto recovered same-site gateways
  (3 GW per site: disaster-clear gives all Site A ANA to one recovered GW, no-disaster
  same-site relocation, then failback as remaining same-site GWs come up).

Conf reference: conf/tentacle/nvmeof/9-node-cluster-2-clients.yaml
Suite: suites/tentacle/nvmeof/tier-2_nvmeof_stretch_cluster.yaml
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import (
    compare_client_namespace,
    prepare_io_execution,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import string_to_dict, validate_io
from tests.nvmeof.workflows.stretch_ha import build_gw_location_map
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def _log_step(step, total, action, expected=None):
    """Emit a scan-friendly step header for suite log readers."""
    header = f"STEP {step}/{total}: {action}"
    log.info("=" * 72)
    log.info(header)
    if expected:
        log.info("Expected: %s", expected)
    log.info("=" * 72)


def _parse_json_blob(out):
    """Parse JSON from cephadm output that may include leading log lines."""
    if isinstance(out, (dict, list)):
        return out
    text = out if isinstance(out, str) else str(out)
    idx_obj, idx_arr = text.find("{"), text.find("[")
    candidates = []
    if idx_obj != -1:
        candidates.append((idx_obj, "{", "}"))
    if idx_arr != -1:
        candidates.append((idx_arr, "[", "]"))
    candidates.sort(key=lambda item: item[0])
    last_err = None
    for _, opener, closer in candidates:
        start, end = text.find(opener), text.rfind(closer)
        if start == -1 or end <= start:
            continue
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError as extra:
            last_err = extra
    if last_err:
        raise last_err
    raise ValueError(f"No JSON found in output: {text[:300]}")


def _node_location_map(config):
    """Build node-id -> location from nested location.DC*.gw_nodes or dc1/dc2 lists."""
    loc_cfg = config.get("location") or {}
    node_to_loc = {}
    if loc_cfg:
        for loc_name, loc_data in loc_cfg.items():
            nodes = (
                loc_data.get("gw_nodes", loc_data)
                if isinstance(loc_data, dict)
                else loc_data
            )
            for node in nodes or []:
                node_to_loc[node] = loc_name
        return node_to_loc
    dc1 = config.get("dc1_nodes", ["node3", "node4", "node5"])
    dc2 = config.get("dc2_nodes", ["node7", "node8", "node9"])
    loc_dc1 = config.get("location_dc1", "DC1")
    loc_dc2 = config.get("location_dc2", "DC2")
    for node in dc1:
        node_to_loc[node] = loc_dc1
    for node in dc2:
        node_to_loc[node] = loc_dc2
    return node_to_loc


def _add_crush_rules_via_cephadm(cephadm, rule_name: str, rules: str) -> bool:
    """
    Add a CRUSH rule using ceph/crushtool inside cephadm shell on the installer.

    Client nodes may not have ceph-common/ceph-base (AppStream deps), so crush
    map edits run in the bootstrap container with host /tmp bind-mounted.
    """
    installer = getattr(cephadm.installer, "node", cephadm.installer)
    try:
        installer.exec_command(
            sudo=True,
            cmd=(
                "cephadm shell --mount /tmp:/tmp -- bash -c "
                "'ceph osd getcrushmap > /tmp/crush.map.bin && "
                "crushtool -d /tmp/crush.map.bin -o /tmp/crush.map.txt'"
            ),
        )
        installer.exec_command(
            sudo=True,
            cmd=f"""cat <<'EOF' >> /tmp/crush.map.txt
rule {rule_name} {{
{rules}
}}
EOF""",
        )
        installer.exec_command(
            sudo=True,
            cmd=(
                "cephadm shell --mount /tmp:/tmp -- bash -c "
                "'crushtool -c /tmp/crush.map.txt -o /tmp/crush2.map.bin && "
                "ceph osd setcrushmap -i /tmp/crush2.map.bin'"
            ),
        )
        log.info("Crush rule: %s added successfully", rule_name)
        return True
    except Exception as err:
        log.error("Failed to set the crush rules: %s", err)
        return False


def _deploy_stretch_cluster(ceph_cluster, config):
    """
    Deploy two-site stretch cluster with tie-breaker.
    Returns 0 on success, 1 on failure.
    """
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)

    stretch_rule_name = config.get("stretch_rule_name", "stretch_rule")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "tiebreaker")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    no_affinity_crush_rule = config.get("no_affinity", False)

    rule_list = rados_obj.run_ceph_command(cmd="ceph osd crush rule ls") or []
    if stretch_rule_name in rule_list:
        log.info("Crush rule '%s' already present: %s", stretch_rule_name, rule_list)
    else:
        if no_affinity_crush_rule:
            crush_rules = """id 11
type replicated
step take default
step choose firstn 0 type datacenter
step chooseleaf firstn 2 type host
step emit"""
        else:
            osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
            dc_buckets = [
                n
                for n in osd_tree.get("nodes", [])
                if n.get("type") == stretch_bucket
                and n["name"] != tiebreaker_mon_site_name
            ]
            if len(dc_buckets) < 2:
                log.error(
                    f"Need at least 2 datacenter buckets (excluding tiebreaker), "
                    f"found: {[b['name'] for b in dc_buckets]}"
                )
                return 1
            dc_1_name, dc_2_name = dc_buckets[0]["name"], dc_buckets[1]["name"]
            log.info(f"Using data sites: {dc_1_name}, {dc_2_name}")
            crush_rules = f"""id 111
type replicated
min_size 1
max_size 10
step take {dc_1_name}
step chooseleaf firstn 2 type host
step emit
step take {dc_2_name}
step chooseleaf firstn 2 type host
step emit"""

        if not _add_crush_rules_via_cephadm(cephadm, stretch_rule_name, crush_rules):
            log.error("Failed to add crush rules in the crush map")
            return 1

        time.sleep(5)
        rule_list = rados_obj.run_ceph_command(cmd="ceph osd crush rule ls")
        if stretch_rule_name not in rule_list:
            log.error(f"Rule '{stretch_rule_name}' not in crush rule list: {rule_list}")
            return 1

    if mon_obj.get_election_strategy() != 3:
        if not mon_obj.set_election_strategy(mode="connectivity"):
            log.error("Could not set election strategy to connectivity mode")
            return 1
        time.sleep(2)
        if mon_obj.get_election_strategy() != 3:
            log.error("Election strategy is not connectivity")
            return 1
    else:
        log.info("Election strategy already set to connectivity")

    mon_dump = rados_obj.run_ceph_command(cmd="ceph mon dump")
    tiebreaker_mon = None
    for mon in mon_dump.get("mons", []):
        if tiebreaker_mon_site_name in str(mon.get("crush_location", "{}")):
            tiebreaker_mon = mon["name"]
            break
    if not tiebreaker_mon:
        for mon in mon_dump.get("mons", []):
            if mon.get("crush_location") in ("{}", ""):
                tiebreaker_mon = mon["name"]
                break
    if not tiebreaker_mon:
        log.error(
            f"Could not find tiebreaker mon with site '{tiebreaker_mon_site_name}'"
        )
        return 1

    try:
        stretch_dump = rados_obj.run_ceph_command(cmd="ceph osd dump").get(
            "stretch_mode", {}
        )
        if stretch_dump.get("stretch_mode_enabled"):
            log.info("Stretch mode already enabled: %s", stretch_dump)
        else:
            cephadm.shell(
                [
                    f"ceph mon enable_stretch_mode {tiebreaker_mon} {stretch_rule_name} {stretch_bucket}"
                ]
            )
            time.sleep(5)
            stretch_dump = rados_obj.run_ceph_command(cmd="ceph osd dump").get(
                "stretch_mode", {}
            )
            if not stretch_dump.get("stretch_mode_enabled"):
                log.error("Stretch mode not enabled: %s", stretch_dump)
                return 1
    except Exception as err:
        log.error("Error enabling stretch mode: %s", err)
        return 1

    if not wait_for_clean_pg_sets(rados_obj):
        log.error("PGs did not reach active+clean after stretch mode enable")
        return 1
    log.info("Stretch cluster deployed successfully")
    return 0


def _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj):
    """Deploy NVMe service (3 GW per DC) and configure entities plus initiators."""
    # from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image

    # check_and_set_nvme_cli_image(ceph_cluster, config=config.get("custom-config"))
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    config["nvme_service"] = nvme_service

    configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)
    log.info("NVMe gateways deployed and entities configured")
    return 0


def _test_gw_locations(ceph_cluster, config):
    """Set, get, modify one, unset one, and restore GW locations."""
    log.info(
        "TEST: Set, get, modify, and unset NVMe-oF gateway locations "
        "(ceph nvme-gw set-location / show)"
    )
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.init_gateways()
    config["nvme_service"] = nvme_service
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    nvme_gw = _nvme_gw_cli(ceph_cluster)

    node_to_loc = _node_location_map(config)
    gw_location_map = build_gw_location_map(nvme_service.gateways, node_to_loc)
    if not gw_location_map:
        log.error(
            "Could not map any gateways to locations from config: %s", node_to_loc
        )
        return 1

    target_id = next(iter(gw_location_map))
    original = gw_location_map[target_id]
    modified = config.get("location_modified", f"{original}_modified")
    log.info("Gateway location map: %s", gw_location_map)
    log.info("Will modify then unset only %s (original=%s)", target_id, original)

    def _restore():
        log.info("Restoring all gateway locations to the original map")
        for gw_id, loc in gw_location_map.items():
            try:
                nvme_gw.set_location(gw_id, pool, group, loc)
            except Exception as extra:
                log.warning("Restore location %s on %s: %s", loc, gw_id, extra)
        try:
            _assert_gw_locations(nvme_gw, pool, group, gw_location_map)
        except Exception as extra:
            log.warning("Assert restored GW locations: %s", extra)

    try:
        _log_step(
            1,
            4,
            "Set location on every gateway in the group",
            "nvme-gw show lists the configured location for each GW",
        )
        for gw_id, loc in gw_location_map.items():
            nvme_gw.set_location(gw_id, pool, group, loc)
        log.info("Set GW locations: %s", gw_location_map)
        _assert_gw_locations(nvme_gw, pool, group, gw_location_map)
        log.info("nvme-gw show:\n%s", nvme_gw.show(pool, group, format="json"))

        _log_step(
            2,
            4,
            f"Modify location of {target_id} to {modified}",
            "Only that GW changes; the rest keep their original locations",
        )
        nvme_gw.set_location(target_id, pool, group, modified)
        expected = dict(gw_location_map)
        expected[target_id] = modified
        log.info("Modified GW %s location to %s", target_id, modified)
        _assert_gw_locations(nvme_gw, pool, group, expected)

        _log_step(
            3,
            4,
            f"Unset location of {target_id}",
            "That GW location is empty; other GWs are unchanged",
        )
        nvme_gw.set_location(target_id, pool, group, "")
        expected[target_id] = ""
        log.info("Unset GW %s location", target_id)
        _assert_gw_locations(nvme_gw, pool, group, expected)

        _log_step(
            4,
            4,
            "Restore original locations",
            "All GWs match the initial DC1/DC2 map for later tests",
        )
    finally:
        _restore()
    log.info("TEST PASS: gateway locations set, modified, unset, and restored")
    return 0


def _test_namespace_locations(ceph_cluster, config):
    """Valid NS location + ANA + IO, invalid location reject, change_location + ANA + IO."""
    log.info(
        "TEST: Add namespaces with valid/invalid locations, verify ANA placement, "
        "IO, then change_location"
    )
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.init_gateways()
    config["nvme_service"] = nvme_service
    gateway = nvme_service.gateways[0]
    nqn = config.get("subsystems", [{}])[0].get("nqn") or config.get(
        "subsystems", [{}]
    )[0].get("subnqn")
    if not nqn:
        log.error("No subsystem nqn in config")
        return 1

    nvme_gw = _nvme_gw_cli(ceph_cluster)
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    rbd_pool = config.get("rbd_pool", "rbd")
    orch = Orch(cluster=ceph_cluster, **{})
    valid_locations = config.get("valid_namespace_locations", ["DC1", "DC2"])
    new_location = config.get("namespace_new_location", valid_locations[-1])
    image_size = _ns_image_size(config)
    log.info(
        "Subsystem %s; valid locations %s; change_location target %s; image size %s",
        nqn,
        valid_locations,
        new_location,
        image_size,
    )

    def _add_ns(image, location):
        try:
            gateway.namespace.add(
                args={
                    "subsystem": nqn,
                    "rbd-pool": rbd_pool,
                    "rbd-image": image,
                    "size": image_size,
                    "rbd-create-image": True,
                    "location": location,
                }
            )
            log.info("Added namespace %s with location %s", image, location)
        except Exception as e:
            msg = str(e).lower()
            if "already used" not in msg and "already exist" not in msg:
                raise
            log.info("Namespace %s already present, reusing it: %s", image, e)

    _log_step(
        1,
        4,
        f"Create namespaces with valid locations {valid_locations}",
        "Each NS lists that location and its ANA group is ACTIVE on a GW in that location",
    )
    for i, loc in enumerate(valid_locations):
        _add_ns(f"test-valid-loc-{i}", loc)

    created = []
    for i, loc in enumerate(valid_locations):
        image = f"test-valid-loc-{i}"
        ns = None
        last_err = None
        for _ in WaitUntil(timeout=60, interval=5):
            namespaces = _list_namespaces(gateway, nqn)
            ns = _ns_by_image(namespaces, image)
            if not ns:
                last_err = AssertionError(f"Namespace {image} not found yet")
                continue
            try:
                _assert_ns_at_location(ns, loc, nvme_gw, pool, group)
                last_err = None
                break
            except AssertionError as exc:
                last_err = exc
                log.info("Waiting for namespace %s at %s: %s", image, loc, exc)
        if last_err:
            raise last_err
        created.append(ns)

    _ensure_ns_image_size(gateway, nqn, created, image_size)
    ns_info = [_ns_info(nqn, ns) for ns in created]
    _log_step(
        2,
        4,
        "Run IO on the new namespaces",
        f"rbd du used_size increases on the {image_size} images",
    )
    with _background_io(config, nvme_service, ceph_cluster):
        if ns_info:
            validate_io(orch, ns_info)
        log.info("IO succeeded on namespaces with valid locations")

        invalid_location = config.get("invalid_namespace_location", "INVALID_SITE")
        _log_step(
            3,
            4,
            f"Add a namespace with invalid location {invalid_location}",
            "Command is rejected; no namespace is created",
        )
        try:
            gateway.namespace.add(
                args={
                    "subsystem": nqn,
                    "rbd-pool": rbd_pool,
                    "rbd-image": "test-invalid-loc",
                    "size": "1G",
                    "rbd-create-image": True,
                    "location": invalid_location,
                }
            )
            log.error("Adding namespace with invalid location should have failed")
            return 1
        except Exception as e:
            log.info("Expected reject for invalid location: %s", e)

        target = created[0]
        nsid = config.get("namespace_change_location_nsid") or target.get("nsid")
        _log_step(
            4,
            4,
            f"change_location nsid {nsid} to {new_location}, then restore {valid_locations[0]}",
            "ANA moves to the new location (up to 15 min), IO continues, then NS is restored",
        )
        original_location = valid_locations[0]
        gateway.namespace.change_location(
            args={"subsystem": nqn, "nsid": nsid, "location": new_location}
        )
        log.info("Changed namespace %s location to %s", nsid, new_location)
        try:
            moved = _wait_ns_at_location(
                gateway, nqn, nsid, new_location, nvme_gw, pool, group
            )
            validate_io(orch, [_ns_info(nqn, moved)])
        finally:
            try:
                gateway.namespace.change_location(
                    args={
                        "subsystem": nqn,
                        "nsid": nsid,
                        "location": original_location,
                    }
                )
                log.info(
                    "Restored namespace %s location to %s for later HA tests",
                    nsid,
                    original_location,
                )
                _wait_ns_at_location(
                    gateway, nqn, nsid, original_location, nvme_gw, pool, group
                )
            except Exception as extra:
                log.warning(
                    "Failed to restore namespace %s to %s: %s",
                    nsid,
                    original_location,
                    extra,
                )
    log.info("TEST PASS: valid NS locations, invalid reject, change_location, IO")
    return 0


def _test_failover_failback(ceph_cluster, config):
    """Single GW failover via ceph orch daemon then nvme-gw disable/enable; same-location ANA and IO."""
    log.info(
        "TEST: Single gateway failover/failback with ceph orch daemon stop/start, then nvme-gw "
        "disable/enable; ANA must stay in the same location and IO must continue"
    )
    nvme_service = config.get("nvme_service")
    if not nvme_service:
        nvme_service = NVMeService(config, ceph_cluster)
        nvme_service.init_gateways()
    config["nvme_service"] = nvme_service

    dc1_nodes = config.get("dc1_nodes", ["node3", "node4", "node5"])
    dc2_nodes = config.get("dc2_nodes", ["node7", "node8", "node9"])
    fail_node = config.get("failover_node", dc1_nodes[0])
    site_loc = config.get("location_dc1", "DC1")
    gw_locations = build_gw_location_map(
        nvme_service.gateways,
        _node_location_map(config)
        or {
            **{n: site_loc for n in dc1_nodes},
            **{n: config.get("location_dc2", "DC2") for n in dc2_nodes},
        },
    )
    config["stretch_mode"] = True
    config["gw_locations"] = gw_locations

    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways
    ha.set_gateway_locations(gw_locations)

    dc1_gws = _gateways_for_nodes(nvme_service, dc1_nodes)
    preferred = next((g for g in dc1_gws if g.node.id == fail_node), dc1_gws[0])
    gw = None
    namespaces = []
    for candidate in [preferred] + [g for g in dc1_gws if g is not preferred]:
        namespaces = fetch_namespaces(
            candidate, [candidate.ana_group_id], get_list=True
        )
        if namespaces:
            gw = candidate
            break
    if not gw:
        gw = preferred
        log.warning("No namespaces on any DC1 GW; failing over %s anyway", gw.hostname)
    elif gw.node.id != fail_node:
        log.info(
            "Failing over %s (has namespaces) instead of configured %s",
            gw.hostname,
            fail_node,
        )

    nqn = ((config.get("subsystems") or [{}])[0].get("nqn")) or (
        (namespaces[0]["info"].split("|", 1)[0] if namespaces else None)
    )
    if nqn and namespaces:
        _ensure_ns_image_size(
            nvme_service.gateways[0], nqn, namespaces, _ns_image_size(config)
        )

    ns_info = [n.get("info") for n in namespaces]
    if not ns_info:
        log.warning(
            "No namespaces on failed GW %s; IO checks will be skipped", gw.hostname
        )
    else:
        log.info(
            "Failing %s (node %s, ANA group %s) in location %s; namespaces=%s",
            gw.hostname,
            gw.node.id,
            gw.ana_group_id,
            site_loc,
            ns_info,
        )
    ns_uuids = [
        (n["list"].get("uuid") or (n["list"].get("wwn") or "").replace("uuid.", ""))
        for n in namespaces
        if n["list"].get("uuid") or n["list"].get("wwn")
    ]
    nvme_gw = _nvme_gw_cli(ceph_cluster)
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    gw_down = False
    gw_disabled = False

    def _check_same_site_and_io(stage):
        holders = _wait_ana_active_on_location(
            nvme_gw, pool, group, gw.ana_group_id, site_loc
        )
        log.info("%s: ANA %s active on %s", stage, gw.ana_group_id, holders)
        if ns_info:
            validate_io(ha.orch, ns_info)

    with _background_io(config, nvme_service, ceph_cluster) as clients:
        _log_step(
            1,
            5,
            "Baseline IO before failover",
            "IO is progressing on the chosen namespaces",
        )
        if clients and ns_uuids:
            compare_client_namespace(clients, ns_uuids, FEWR_NAMESPACES=True)
        if ns_info:
            validate_io(ha.orch, ns_info)

        try:
            _log_step(
                2,
                5,
                f"Stop {gw.hostname} with ceph orch daemon stop",
                f"ANA {gw.ana_group_id} becomes ACTIVE on another {site_loc} GW; IO continues",
            )
            _stop_gws(ha, [gw])
            gw_down = True
            _check_same_site_and_io("orch daemon failover")

            _log_step(
                3,
                5,
                f"Start {gw.hostname} (ceph orch daemon start)",
                "Gateway becomes AVAILABLE and IO continues",
            )
            # Stretch does not require ANA to fail back to this GW. ha.failback()
            # waits for the original ANA path and would timeout here.
            _start_gws(ha, [gw])
            gw_down = False
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            if ns_info:
                validate_io(ha.orch, ns_info)
            log.info("orch daemon failover and failback completed")

            _log_step(
                4,
                5,
                f"nvme-gw disable {gw.hostname}",
                "Admin state is not ENABLED; ANA stays on another same-location GW; IO continues",
            )
            nvme_gw.disable(gw.gw_id, pool, group)
            gw_disabled = True
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
            entry = _gw_show_entry(_show_created_gateways(nvme_gw, pool, group), gw)
            admin_state = (entry or {}).get("admin state")
            if admin_state == "ENABLED":
                log.error("Gateway %s still ENABLED after nvme-gw disable", gw.hostname)
                return 1
            log.info(
                "Gateway %s admin state after disable: %s", gw.hostname, admin_state
            )
            _check_same_site_and_io("nvme-gw disable")

            _log_step(
                5,
                5,
                f"nvme-gw enable {gw.hostname}",
                "Gateway becomes AVAILABLE and IO continues",
            )
            nvme_gw.enable(gw.gw_id, pool, group)
            gw_disabled = False
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            if ns_info:
                validate_io(ha.orch, ns_info)
            log.info("nvme-gw disable and enable completed")
            log.info(
                "TEST PASS: same-location failover/failback via orch daemon and nvme-gw disable/enable"
            )
            return 0
        finally:
            if gw_down:
                try:
                    log.info("Restoring gateway %s after failover test", gw.hostname)
                    _start_gws(ha, [gw])
                except Exception as exc:
                    log.warning("Failback during cleanup failed: %s", exc)
            if gw_disabled:
                try:
                    nvme_gw.enable(gw.gw_id, pool, group)
                except Exception as exc:
                    log.warning("Enable during cleanup failed: %s", exc)


def _nvme_gw_cli(ceph_cluster):
    from cli.ceph.nvme_gw import NvmeGw

    orch = Orch(cluster=ceph_cluster, **{})
    return NvmeGw(orch.installer, "ceph")


def _show_created_gateways(nvme_gw, pool, group):
    out = nvme_gw.show(pool, group, format="json")
    data = _parse_json_blob(out)
    entries = data.get("Created Gateways:", data.get("gateways", []))
    _log_all_gw_status(entries)
    return entries


def _hostname_from_show_entry(entry):
    """Best-effort hostname from nvme-gw show ``gw-id`` (client.<daemon_name>)."""
    gid = (entry or {}).get("gw-id") or ""
    name = gid[7:] if str(gid).startswith("client.") else str(gid)
    parts = [part for part in name.split(".") if part]
    for part in reversed(parts[:-1] or parts):
        if "-" in part or "node" in part.lower():
            return part
    if len(parts) >= 2:
        return parts[-2]
    return name or gid or "unknown"


def _format_ana_states(entry):
    """Format ANA states as ``1: STANDBY ,  2: STANDBY ,  3: ACTIVE``."""
    raw = (entry or {}).get("ana states", "")
    if isinstance(raw, dict):
        states = {}
        for key, value in raw.items():
            try:
                states[int(key)] = value
            except (TypeError, ValueError):
                states[key] = value
    else:
        try:
            states = string_to_dict(raw or "")
        except Exception:
            return str(raw or "")
    return " ,  ".join(
        f"{ana_id}: {str(state).upper()}"
        for ana_id, state in sorted(
            states.items(), key=lambda item: (isinstance(item[0], str), item[0])
        )
    )


def _log_all_gw_status(entries):
    """Log every gateway availability and ANA map for side-by-side comparison."""
    if not entries:
        log.info("Gateway status: no gateways in nvme-gw show")
        return
    for entry in entries:
        log.info(
            "Gateway %s is %s (ana= %s)",
            _hostname_from_show_entry(entry),
            (entry or {}).get("Availability") or "UNKNOWN",
            _format_ana_states(entry),
        )


def _gw_show_entry(entries, gw):
    gid = gw.gw_id
    for item in entries:
        item_id = item.get("gw-id") or ""
        if gid and item_id == gid:
            return item
        if gw.hostname and gw.hostname in item_id:
            return item
    return None


def _active_ana_ids(entry):
    if not entry:
        return []
    states = string_to_dict(entry.get("ana states", ""))
    return sorted(
        ana_id for ana_id, state in states.items() if str(state).upper() == "ACTIVE"
    )


def _wait_for_gateway_state(nvme_gw, pool, group, gw, availability, timeout=120):
    for _ in WaitUntil(timeout=timeout, interval=5):
        entry = _gw_show_entry(_show_created_gateways(nvme_gw, pool, group), gw)
        if entry and entry.get("Availability") == availability:
            log.info("Gateway %s reached %s", gw.hostname, availability)
            return entry
        log.info(
            "Waiting for %s to become %s (current=%s)",
            gw.hostname,
            availability,
            (entry or {}).get("Availability"),
        )
    raise TimeoutError(
        f"Gateway {gw.hostname} did not reach {availability} within {timeout}s"
    )


def _wait_for_active_ana_contains(nvme_gw, pool, group, gw, required, timeout=180):
    """Wait until gw is ACTIVE for at least the required ANA group ids.

    Extra groups from other same-site GWs that are still down are allowed.
    """
    required = set(required)
    last = set()
    for _ in WaitUntil(timeout=timeout, interval=5):
        entry = _gw_show_entry(_show_created_gateways(nvme_gw, pool, group), gw)
        last = set(_active_ana_ids(entry))
        log.info(
            "Gateway %s active ANA groups=%s (need %s); states=%s",
            gw.hostname,
            last,
            required,
            (entry or {}).get("ana states"),
        )
        if required <= last:
            return list(last)
    raise TimeoutError(
        f"Gateway {gw.hostname} has {last} active ANA groups, need {required}"
    )


def _wait_recovered_hold_site_a_ana(
    nvme_gw, pool, group, recovered_gws, site_a_ana, timeout=180
):
    """Wait until Site A ANA is fully on recovered GWs and each holds at least one group."""
    site_a_ana = set(site_a_ana)
    last = {}
    for _ in WaitUntil(timeout=timeout, interval=5):
        entries = _show_created_gateways(nvme_gw, pool, group)
        last = {
            gw.hostname: set(_active_ana_ids(_gw_show_entry(entries, gw)))
            for gw in recovered_gws
        }
        held = [ana & site_a_ana for ana in last.values()]
        union = set().union(*held) if held else set()
        nonempty = sum(1 for groups in held if groups)
        log.info(
            "Recovered Site A ANA: %s (need all of %s on %s recovered GW(s))",
            last,
            site_a_ana,
            len(recovered_gws),
        )
        if union == site_a_ana and nonempty == len(recovered_gws):
            return last
    raise TimeoutError(
        f"Site A ANA {site_a_ana} not fully on recovered GWs; have {last}"
    )


def _gateways_for_nodes(nvme_service, node_ids):
    found = []
    for node_id in node_ids:
        match = [gw for gw in nvme_service.gateways if gw.node.id == node_id]
        if not match:
            raise ValueError(f"No NVMe gateway found for {node_id}")
        found.append(match[0])
    return found


def _list_nvmeof_daemons(ha):
    """Return nvmeof daemon records from ``ceph orch ps``."""
    out, _ = ha.orch.shell(
        args=[
            "ceph",
            "orch",
            "ps",
            "--daemon-type",
            "nvmeof",
            "--format",
            "json",
            "--refresh",
        ]
    )
    data = _parse_json_blob(out)
    if isinstance(data, dict):
        data = data.get("daemons") or []
    if not isinstance(data, list):
        raise ValueError(f"Unexpected ceph orch ps output: {data!r}")
    return data


def _nvmeof_daemon_for_gw(ha, gw):
    """Resolve the orch nvmeof daemon for a gateway from ``ceph orch ps``."""
    hostname = gw.hostname
    short = hostname.split(".")[0]
    hosts = {hostname, short, getattr(gw.node, "shortname", "") or ""}
    hosts.discard("")
    daemons = _list_nvmeof_daemons(ha)
    for daemon in daemons:
        dhost = daemon.get("hostname") or ""
        dname = daemon.get("daemon_name") or daemon.get("daemon_id") or ""
        if not dname:
            continue
        if dhost in hosts or hostname in dname or short in dname.split("."):
            gw.daemon_name = dname
            return daemon
    listed = [
        (
            d.get("hostname"),
            d.get("daemon_name") or d.get("daemon_id"),
            d.get("status_desc"),
        )
        for d in daemons
    ]
    raise ValueError(
        f"No nvmeof daemon in ceph orch ps for gateway {hostname}; listed={listed}"
    )


def _log_nvmeof_orch_ps(ha):
    """Log every nvmeof daemon from ``ceph orch ps`` for comparison."""
    daemons = _list_nvmeof_daemons(ha)
    if not daemons:
        log.info("ceph orch ps: no nvmeof daemons listed")
        return daemons
    for daemon in daemons:
        log.info(
            "ceph orch ps: %s host=%s status=%s status_desc=%s",
            daemon.get("daemon_name") or daemon.get("daemon_id"),
            daemon.get("hostname"),
            daemon.get("status"),
            daemon.get("status_desc"),
        )
    return daemons


def _orch_ps_daemon(ha, daemon_name):
    for daemon in _list_nvmeof_daemons(ha):
        name = daemon.get("daemon_name") or daemon.get("daemon_id") or ""
        if name == daemon_name:
            return daemon
    return None


def _orch_ps_is_started(daemon):
    if not daemon:
        return False
    desc = (daemon.get("status_desc") or "").lower()
    return daemon.get("status") == 1 or desc == "running"


def _orch_ps_is_stopped(daemon):
    if not daemon:
        return False
    desc = (daemon.get("status_desc") or "").lower()
    return daemon.get("status") == 0 or desc == "stopped"


def _wait_orch_ps_state(ha, daemon_name, action, timeout=300):
    """Do not continue until ceph orch ps shows the daemon started or stopped."""
    expected = "running" if action == "start" else "stopped"
    last = None
    for _ in WaitUntil(timeout=timeout, interval=5):
        last = _orch_ps_daemon(ha, daemon_name)
        desc = (last or {}).get("status_desc")
        status = (last or {}).get("status")
        log.info(
            "ceph orch ps: %s host=%s status=%s status_desc=%s (want %s)",
            daemon_name,
            (last or {}).get("hostname"),
            status,
            desc,
            expected,
        )
        if action == "start" and _orch_ps_is_started(last):
            log.info(
                "Verified %s is started from ceph orch ps (status=%s status_desc=%s)",
                daemon_name,
                status,
                desc,
            )
            return last
        if action == "stop" and _orch_ps_is_stopped(last):
            log.info(
                "Verified %s is stopped from ceph orch ps (status=%s status_desc=%s)",
                daemon_name,
                status,
                desc,
            )
            return last
        log.info(
            "Waiting for ceph orch ps to show %s as %s before the next step",
            daemon_name,
            expected,
        )
    raise TimeoutError(
        f"ceph orch ps did not show {daemon_name} as {expected} within {timeout}s; "
        f"last status={(last or {}).get('status')} "
        f"status_desc={(last or {}).get('status_desc')}"
    )


def _orch_daemon_gw(ha, gw, action, retries=3):
    """Stop or start a gateway with ``ceph orch daemon`` using the orch ps name."""
    last = None
    daemon_name = None
    for attempt in range(1, retries + 1):
        daemon = _nvmeof_daemon_for_gw(ha, gw)
        daemon_name = daemon.get("daemon_name") or daemon.get("daemon_id")
        log.info(
            "%s gateway %s via ceph orch daemon %s %s (attempt %s/%s)",
            action.capitalize(),
            gw.hostname,
            action,
            daemon_name,
            attempt,
            retries,
        )
        try:
            cmd = ["ceph", "orch", "daemon", action, daemon_name]
            log.info("Executing: %s", " ".join(cmd))
            ha.orch.shell(args=cmd)
            last = None
            break
        except (CommandFailed, EOFError, OSError) as extra:
            last = extra
            log.warning(
                "orch daemon %s %s attempt %s failed: %s",
                action,
                gw.hostname,
                attempt,
                extra,
            )
            time.sleep(5)
    if last:
        raise last

    log.info(
        "ceph orch daemon %s issued for %s; verifying from ceph orch ps before continuing",
        action,
        daemon_name,
    )
    _log_nvmeof_orch_ps(ha)
    _wait_orch_ps_state(ha, daemon_name, action)


def _stop_gws(ha, gws):
    log.info(
        "ceph orch daemon stop for gateways: %s",
        [gw.hostname for gw in gws],
    )
    for gw in gws:
        _orch_daemon_gw(ha, gw, "stop")


def _start_gws(ha, gws):
    log.info(
        "ceph orch daemon start for gateways: %s",
        [gw.hostname for gw in gws],
    )
    for gw in gws:
        _orch_daemon_gw(ha, gw, "start")


def _show_location(entry):
    return (entry or {}).get("location") or ""


def _match_gw_id(shown_id, expected_id):
    if not shown_id or not expected_id:
        return False
    return shown_id == expected_id or expected_id in shown_id or shown_id in expected_id


def _assert_gw_locations(nvme_gw, pool, group, expected):
    entries = _show_created_gateways(nvme_gw, pool, group)
    for gid, loc in expected.items():
        entry = next(
            (item for item in entries if _match_gw_id(item.get("gw-id"), gid)), None
        )
        actual = _show_location(entry)
        if actual != loc:
            raise AssertionError(
                f"GW {gid} location '{actual}' != '{loc}' (entry={entry})"
            )
        log.info("Verified GW %s location=%s", gid, loc)


def _list_namespaces(gateway, nqn):
    out, _ = gateway.namespace.list(
        **{"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    )
    data = _parse_json_blob(out)
    return data.get("namespaces") or []


def _ns_by_image(namespaces, image):
    for ns in namespaces:
        if ns.get("rbd_image_name") == image:
            return ns
    return None


def _ns_info(nqn, ns):
    image_path = ns["rbd_image_name"]
    if ns.get("rados_namespace_name"):
        image_path = f"{ns['rados_namespace_name']}/{image_path}"
    return f"{nqn}|nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{image_path}"


def _ns_image_size(config, default="10G"):
    """Image size large enough for rbd-du IO checks across sequential FIO windows."""
    if config.get("namespace_image_size"):
        return str(config["namespace_image_size"])
    for sub in config.get("subsystems") or []:
        bdevs = sub.get("bdevs") or []
        if isinstance(bdevs, dict):
            bdevs = [bdevs]
        for bdev in bdevs:
            if isinstance(bdev, dict) and bdev.get("size"):
                return str(bdev["size"])
    return default


def _size_to_bytes(size):
    text = str(size).strip().upper()
    if text.isdigit():
        return int(text)
    unit = text[-1]
    value = float(text[:-1])
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    if unit not in multipliers:
        raise ValueError(f"Unsupported image size {size}")
    return int(value * multipliers[unit])


def _ns_entry(ns):
    if isinstance(ns, dict) and ns.get("list"):
        return ns["list"]
    return ns


def _ensure_ns_image_size(gateway, nqn, namespaces, size):
    """Grow undersized images so validate_io can still see used_size increase."""
    wanted = _size_to_bytes(size)
    for ns in namespaces:
        entry = _ns_entry(ns)
        if not entry:
            continue
        try:
            current = int(entry.get("rbd_image_size") or 0)
        except (TypeError, ValueError):
            current = 0
        nsid = entry.get("nsid")
        if current >= wanted:
            log.info(
                "Namespace nsid %s image %s is %s bytes (>= %s)",
                nsid,
                entry.get("rbd_image_name"),
                current,
                size,
            )
            continue
        log.info(
            "Resizing namespace nsid %s image %s from %s bytes to %s for IO verification",
            nsid,
            entry.get("rbd_image_name"),
            current,
            size,
        )
        gateway.namespace.resize(args={"subsystem": nqn, "nsid": nsid, "size": size})


def _ana_holder(entries, ana_id):
    try:
        ana_id = int(ana_id)
    except (TypeError, ValueError):
        pass
    for entry in entries:
        states = string_to_dict(entry.get("ana states", ""))
        if str(states.get(ana_id, "")).upper() == "ACTIVE":
            return entry
    return None


def _assert_ns_at_location(ns, location, nvme_gw, pool, group):
    actual = ns.get("location") or ""
    if actual != location:
        raise AssertionError(
            f"Namespace {ns.get('nsid')} location '{actual}' != '{location}'"
        )
    ana_id = ns.get("load_balancing_group")
    holder = _ana_holder(_show_created_gateways(nvme_gw, pool, group), ana_id)
    holder_loc = _show_location(holder)
    if holder_loc != location:
        raise AssertionError(
            f"Namespace {ns.get('nsid')} ANA {ana_id} is active on location "
            f"'{holder_loc}', expected '{location}'"
        )
    log.info(
        "Namespace %s location=%s ANA=%s on %s",
        ns.get("nsid"),
        actual,
        ana_id,
        (holder or {}).get("gw-id"),
    )


def _assert_no_active_on_gws(nvme_gw, pool, group, gws, reason):
    entries = _show_created_gateways(nvme_gw, pool, group)
    for gw in gws:
        active = _active_ana_ids(_gw_show_entry(entries, gw))
        if active:
            raise AssertionError(f"{reason}: {gw.hostname} has active ANA {active}")


def _assert_ana_active_on_location(nvme_gw, pool, group, ana_id, location):
    entries = _show_created_gateways(nvme_gw, pool, group)
    holder = _ana_holder(entries, ana_id)
    if not holder:
        raise AssertionError(f"ANA group {ana_id} has no ACTIVE gateway")
    actual = _show_location(holder)
    if actual != location:
        raise AssertionError(
            f"ANA group {ana_id} active on {holder.get('gw-id')} location "
            f"'{actual}', expected '{location}'"
        )
    return holder.get("gw-id")


def _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, location, timeout=120):
    last = None
    for _ in WaitUntil(timeout=timeout, interval=5):
        try:
            return _assert_ana_active_on_location(
                nvme_gw, pool, group, ana_id, location
            )
        except AssertionError as exc:
            last = exc
            log.info("Waiting for ANA %s on %s: %s", ana_id, location, exc)
    raise TimeoutError(str(last))


def _wait_no_active_on_gws(nvme_gw, pool, group, gws, reason, timeout=60):
    last = None
    for _ in WaitUntil(timeout=timeout, interval=5):
        try:
            _assert_no_active_on_gws(nvme_gw, pool, group, gws, reason)
            return
        except AssertionError as extra:
            last = extra
            log.info("%s", extra)
    raise TimeoutError(str(last))


def _wait_gw_owns_own_ana(nvme_gw, pool, group, gw, timeout=120):
    last = []
    for _ in WaitUntil(timeout=timeout, interval=5):
        active = _active_ana_ids(
            _gw_show_entry(_show_created_gateways(nvme_gw, pool, group), gw)
        )
        last = active
        if gw.ana_group_id in active:
            log.info("Gateway %s owns its ANA group %s", gw.hostname, gw.ana_group_id)
            return active
        log.info(
            "Waiting for %s to own ANA %s (active=%s)",
            gw.hostname,
            gw.ana_group_id,
            active,
        )
    raise TimeoutError(
        f"Gateway {gw.hostname} did not regain ANA {gw.ana_group_id}, active={last}"
    )


def _collect_ns_info(gateway, ana_ids):
    return [
        n.get("info")
        for n in fetch_namespaces(gateway, list(ana_ids), get_list=True)
        if n.get("info")
    ]


def _disconnect_configured_initiators(ceph_cluster, config):
    """Drop leftover NVMe initiator sessions without removing the gateway service."""
    from ceph.nvmeof.initiators.linux import Initiator
    from ceph.utils import get_node_by_id
    from tests.nvmeof.workflows.gateway_entities import disconnect_initiators

    nvme_svc = config.get("nvme_service")
    if nvme_svc:
        try:
            disconnect_initiators(nvme_svc)
            log.info("Disconnected NVMe initiators")
            return
        except Exception as extra:
            log.warning("disconnect_initiators failed: %s", extra)
    for initiator_cfg in config.get("initiators") or []:
        node_id = initiator_cfg.get("node")
        if not node_id:
            continue
        try:
            Initiator(get_node_by_id(ceph_cluster, node_id)).disconnect_all()
            log.info("Disconnected all NVMe controllers on %s", node_id)
        except Exception as extra:
            log.warning("disconnect-all on %s failed: %s", node_id, extra)


def _available_gateways(nvme_service, ceph_cluster):
    """Gateways that are AVAILABLE for initiator discover/connect-all.

    prepare_io_execution always uses gateways[0]. After a test stops a GW, that
    first node can be down and connect-all finds no devices.
    """
    try:
        nvme_gw = _nvme_gw_cli(ceph_cluster)
        entries = _show_created_gateways(
            nvme_gw, nvme_service.nvme_metadata_pool, nvme_service.group
        )
    except Exception as extra:
        log.warning("Could not query gateway availability: %s", extra)
        return list(nvme_service.gateways)

    live = []
    down = []
    for gw in nvme_service.gateways:
        entry = _gw_show_entry(entries, gw)
        if entry and entry.get("Availability") == "AVAILABLE":
            live.append(gw)
        else:
            down.append(gw)
    if down:
        log.info(
            "Skipping UNAVAILABLE gateways for initiator connect: %s",
            [gw.hostname for gw in down],
        )
    if not live:
        log.warning("No AVAILABLE gateways; falling back to full list")
        return list(nvme_service.gateways)
    live.sort(key=lambda gw: (0 if _active_ana_ids(_gw_show_entry(entries, gw)) else 1))
    log.info(
        "Initiator connect via AVAILABLE GWs (first=%s): %s",
        live[0].hostname,
        [gw.hostname for gw in live],
    )
    return live


@contextmanager
def _background_io(config, nvme_service, ceph_cluster, runtime=300):
    initiators = config.get("initiators")
    if not initiators:
        yield []
        return
    clients = prepare_io_execution(
        initiators,
        gateways=_available_gateways(nvme_service, ceph_cluster),
        cluster=ceph_cluster,
        return_clients=True,
    )
    executor = ThreadPoolExecutor(max_workers=max(len(clients), 1))
    for client in clients:
        executor.submit(client.start_fio, runtime=runtime, time_based=True, iodepth=8)
    time.sleep(15)
    try:
        yield clients
    finally:
        for client in clients:
            try:
                client.stop_fio()
            except Exception as extra:
                log.warning("stop_fio on %s failed: %s", client.node.hostname, extra)
            try:
                client.disconnect_all()
                log.info("Disconnected NVMe initiators on %s", client.node.hostname)
            except Exception as extra:
                log.warning(
                    "disconnect-all on %s failed: %s", client.node.hostname, extra
                )
        executor.shutdown(wait=False)


def _wait_ns_at_location(
    gateway, nqn, nsid, location, nvme_gw, pool, group, timeout=900, interval=60
):
    """Wait for namespace location and ANA to land after load-balancing.

    Auto-rebalance can take several minutes. Poll once a minute for up to 15 minutes.
    """
    last = None
    moved = None
    log.info(
        "Waiting up to %ss (poll every %ss) for nsid %s ANA at %s",
        timeout,
        interval,
        nsid,
        location,
    )
    for _ in WaitUntil(timeout=timeout, interval=interval):
        namespaces = _list_namespaces(gateway, nqn)
        moved = next((ns for ns in namespaces if ns.get("nsid") == nsid), None)
        if not moved:
            last = AssertionError(f"nsid {nsid} not listed")
            log.info("Waiting for namespace %s at %s: %s", nsid, location, last)
            continue
        try:
            _assert_ns_at_location(moved, location, nvme_gw, pool, group)
            return moved
        except AssertionError as extra:
            last = extra
            log.info("Waiting for namespace %s at %s: %s", nsid, location, extra)
    raise TimeoutError(str(last))


def _ns_uuid(ns):
    return ns.get("uuid") or (ns.get("wwn") or "").replace("uuid.", "")


def _optimized_paths_for_uuid(clients, uuid):
    if not uuid or not clients:
        return None
    for client in clients:
        for device in client.fetch_lsblk_nvme_devices_dict():
            wwn = device.get("wwn") or ""
            name = device.get("name")
            if uuid in wwn and name:
                return client.fetch_anastate(f"/dev/{name}")
    return None


def _test_homeless_namespaces(ceph_cluster, config):
    """Homeless namespaces, change_location recovery, rebalance, and new GW in location."""
    log.info(
        "TEST: Make namespaces homeless by unsetting every GW in their location, "
        "verify alerts/LOA/IO blocked, recover via change_location, then add a GW "
        "back into that location"
    )
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.init_gateways()
    config["nvme_service"] = nvme_service
    gateway = nvme_service.gateways[0]
    nqn = (config.get("subsystems") or [{}])[0].get("nqn") or (
        config.get("subsystems") or [{}]
    )[0].get("subnqn")
    if not nqn:
        log.error("No subsystem nqn in config")
        return 1

    nvme_gw = _nvme_gw_cli(ceph_cluster)
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    orch = Orch(cluster=ceph_cluster, **{})
    node_to_loc = _node_location_map(config)
    gw_location_map = build_gw_location_map(nvme_service.gateways, node_to_loc)
    homeless_loc = config.get("homeless_location", "DC1")
    recover_loc = config.get("recover_location", "DC2")

    loc_gws = [
        gw
        for gw in nvme_service.gateways
        if node_to_loc.get(gw.node.id) == homeless_loc
    ]
    if len(loc_gws) < 2:
        log.error("Need at least two GWs in %s, got %s", homeless_loc, loc_gws)
        return 1
    new_gw = loc_gws[-1]
    keep_loc_gws = loc_gws[:-1]
    other_ana = {
        gw.ana_group_id
        for gw in nvme_service.gateways
        if node_to_loc.get(gw.node.id) != homeless_loc
    }

    config["stretch_mode"] = True
    config["gw_locations"] = gw_location_map
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways
    ha.set_gateway_locations(gw_location_map)

    namespaces = _list_namespaces(gateway, nqn)
    homeless_ns = [
        ns for ns in namespaces if (ns.get("location") or "") == homeless_loc
    ]
    image_size = _ns_image_size(config)
    if not homeless_ns:
        image = "test-homeless-loc"
        try:
            gateway.namespace.add(
                args={
                    "subsystem": nqn,
                    "rbd-pool": config.get("rbd_pool", "rbd"),
                    "rbd-image": image,
                    "size": image_size,
                    "rbd-create-image": True,
                    "location": homeless_loc,
                }
            )
        except Exception as extra:
            msg = str(extra).lower()
            if "already used" not in msg and "already exist" not in msg:
                raise
            log.info("Reusing namespace image %s: %s", image, extra)
        homeless_ns = [
            ns
            for ns in _list_namespaces(gateway, nqn)
            if (ns.get("location") or "") == homeless_loc
            or ns.get("rbd_image_name") == image
        ]
    if not homeless_ns:
        log.error("No namespaces with location %s", homeless_loc)
        return 1

    _ensure_ns_image_size(gateway, nqn, homeless_ns, image_size)
    original_ns_locs = {
        ns.get("nsid"): ns.get("location") or homeless_loc for ns in homeless_ns
    }
    ns_info = [_ns_info(nqn, ns) for ns in homeless_ns]
    log.info(
        "Homeless location %s; recover location %s; GWs to unset %s; "
        "GW held back for later add %s; namespaces %s",
        homeless_loc,
        recover_loc,
        [gw.hostname for gw in loc_gws],
        new_gw.hostname,
        ns_info,
    )
    stopped = []

    def _restore():
        log.info(
            "Restoring GW locations, started GWs, and original namespace locations"
        )
        for gw_id, loc in gw_location_map.items():
            try:
                nvme_gw.set_location(gw_id, pool, group, loc)
            except Exception as extra:
                log.warning("Restore location %s on %s: %s", loc, gw_id, extra)
        if stopped:
            try:
                _start_gws(ha, stopped)
            except Exception as extra:
                log.warning("Start gateways during restore: %s", extra)
        for ns in homeless_ns:
            nsid = ns.get("nsid")
            loc = original_ns_locs.get(nsid, homeless_loc)
            try:
                gateway.namespace.change_location(
                    args={"subsystem": nqn, "nsid": nsid, "location": loc}
                )
                _wait_ns_at_location(gateway, nqn, nsid, loc, nvme_gw, pool, group)
            except Exception as extra:
                log.warning("Restore NS %s location %s: %s", nsid, loc, extra)

    try:
        _log_step(
            1,
            6,
            f"Baseline: namespaces already in {homeless_loc}",
            "ANA is on a GW in that location (wait up to 15 min) and IO succeeds",
        )
        homeless_ns = [
            _wait_ns_at_location(
                gateway, nqn, ns.get("nsid"), homeless_loc, nvme_gw, pool, group
            )
            for ns in homeless_ns
        ]
        ns_info = [_ns_info(nqn, ns) for ns in homeless_ns]
        with _background_io(config, nvme_service, ceph_cluster, runtime=60):
            validate_io(orch, ns_info)
        log.info(
            "Baseline: namespaces in %s have ANA groups and IO succeeds", homeless_loc
        )

        _log_step(
            2,
            6,
            f"Unset location on every {homeless_loc} GW so those namespaces become homeless",
            "No GW has location L; auto rebalance does not move NS to another location",
        )
        for gw in loc_gws:
            nvme_gw.set_location(gw.gw_id, pool, group, "")
        time.sleep(20)
        entries = _show_created_gateways(nvme_gw, pool, group)
        still_set = [
            item.get("gw-id")
            for item in entries
            if _show_location(item) == homeless_loc
        ]
        if still_set:
            log.error(
                "GWs still have location %s after unset: %s", homeless_loc, still_set
            )
            return 1

        listed = _list_namespaces(gateway, nqn)
        current = [ns for ns in listed if ns.get("nsid") in original_ns_locs]
        if any((ns.get("location") or "") != homeless_loc for ns in current):
            log.error(
                "Homeless namespaces dropped location %s: %s", homeless_loc, current
            )
            return 1
        moved_to_other = [
            ns for ns in current if ns.get("load_balancing_group") in other_ana
        ]
        if moved_to_other:
            log.error(
                "Auto rebalance moved homeless NS onto another location: %s",
                moved_to_other,
            )
            return 1
        log.info(
            "Auto load balancing did not move homeless namespaces off %s", homeless_loc
        )

        _log_step(
            3,
            6,
            "Check alerts, initiator optimized paths (LOA), and IO on homeless namespaces",
            "Alert if present; no optimized path and/or IO blocked",
        )
        health, _ = orch.shell(args=["ceph health detail"])
        log.info(
            "ceph health detail after making %s homeless: %s", homeless_loc, health
        )
        show = nvme_gw.show(pool, group, format="json")
        log.info("nvme-gw show after homeless: %s", show)
        health_text = f"{health} {show}".upper()
        if "HEALTH_OK" in health_text and "HEALTH_WARN" not in health_text:
            log.warning(
                "No Ceph health alert for homeless namespaces; continuing with LOA/IO checks"
            )
        else:
            log.info("Health alert present for homeless namespaces")

        loa_blocked = True
        io_blocked = False
        try:
            with _background_io(
                config, nvme_service, ceph_cluster, runtime=45
            ) as clients:
                for ns in current:
                    paths = _optimized_paths_for_uuid(clients, _ns_uuid(ns))
                    log.info(
                        "Initiator ANA paths for nsid %s: %s", ns.get("nsid"), paths
                    )
                    if paths and paths.get("optimized"):
                        loa_blocked = False
                        log.warning(
                            "Namespace %s still has optimized paths while homeless: %s",
                            ns.get("nsid"),
                            paths,
                        )
                validate_io(orch, ns_info, negative=True)
                io_blocked = True
        except Exception as extra:
            log.warning("Homeless IO-blocked check: %s", extra)
            if (
                "no paths found" in str(extra).lower()
                or "no devices" in str(extra).lower()
            ):
                io_blocked = True
                loa_blocked = True
        if not (loa_blocked or io_blocked):
            log.error("Homeless namespaces still have LOA and progressing IO")
            return 1
        log.info("Homeless namespaces: no LOA and/or IO blocked")

        _log_step(
            4,
            6,
            f"change_location homeless namespaces to {recover_loc}",
            "ANA moves to the recover location and IO resumes",
        )
        for ns in current:
            gateway.namespace.change_location(
                args={
                    "subsystem": nqn,
                    "nsid": ns.get("nsid"),
                    "location": recover_loc,
                }
            )
        recovered = []
        for ns in current:
            recovered.append(
                _wait_ns_at_location(
                    gateway,
                    nqn,
                    ns.get("nsid"),
                    recover_loc,
                    nvme_gw,
                    pool,
                    group,
                )
            )
        with _background_io(config, nvme_service, ceph_cluster, runtime=60):
            validate_io(orch, [_ns_info(nqn, ns) for ns in recovered])
        log.info("Recovered homeless namespaces via change_location to %s", recover_loc)

        _log_step(
            5,
            6,
            f"Restore some {homeless_loc} GWs and change_location one NS back to {homeless_loc}",
            "That namespace's ANA group is ACTIVE on the restored location; IO continues",
        )
        for gw in keep_loc_gws:
            nvme_gw.set_location(gw.gw_id, pool, group, homeless_loc)
        mover = recovered[0]
        gateway.namespace.change_location(
            args={
                "subsystem": nqn,
                "nsid": mover.get("nsid"),
                "location": homeless_loc,
            }
        )
        mover = _wait_ns_at_location(
            gateway,
            nqn,
            mover.get("nsid"),
            homeless_loc,
            nvme_gw,
            pool,
            group,
        )
        with _background_io(config, nvme_service, ceph_cluster, runtime=60):
            validate_io(orch, [_ns_info(nqn, mover)])
        log.info(
            "change_location rebalanced nsid %s onto %s ANA groups",
            mover.get("nsid"),
            homeless_loc,
        )

        _log_step(
            6,
            6,
            f"Add GW {new_gw.hostname} back into {homeless_loc} and fail the other {homeless_loc} GWs",
            "The new GW gets ACTIVE ANA in that location; IO continues",
        )
        nvme_gw.set_location(new_gw.gw_id, pool, group, homeless_loc)
        _wait_for_gateway_state(nvme_gw, pool, group, new_gw, "AVAILABLE")
        log.info("Added GW %s back into location %s", new_gw.hostname, homeless_loc)
        _stop_gws(ha, keep_loc_gws)
        stopped.extend(keep_loc_gws)
        for gw in keep_loc_gws:
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
        _wait_ana_active_on_location(
            nvme_gw, pool, group, mover.get("load_balancing_group"), homeless_loc
        )
        holder = _ana_holder(
            _show_created_gateways(nvme_gw, pool, group),
            new_gw.ana_group_id,
        )
        new_active = _active_ana_ids(
            _gw_show_entry(_show_created_gateways(nvme_gw, pool, group), new_gw)
        )
        if not new_active:
            log.error(
                "New GW %s in %s did not acquire any ACTIVE ANA groups",
                new_gw.hostname,
                homeless_loc,
            )
            return 1
        log.info(
            "New GW %s holds ACTIVE ANA %s (holder=%s)",
            new_gw.hostname,
            new_active,
            (holder or {}).get("gw-id"),
        )
        with _background_io(config, nvme_service, ceph_cluster, runtime=60):
            validate_io(orch, [_ns_info(nqn, mover)])
        _start_gws(ha, keep_loc_gws)
        stopped = []
        log.info(
            "TEST PASS: homeless namespaces, change_location recovery, and new GW rebalance"
        )
        return 0
    finally:
        _restore()


def _test_ana_relocation(ceph_cluster, config):
    """Relocate ANA groups of a site's failed GWs onto recovered same-site GWs.

    Uses all configured Site A and Site B gateways (3 per site in the suite).

    i. Stop all Site A GWs, disaster-set, then recover GW1: GW1 stays Standby,
       Site A ANA stays ACTIVE on Site B; failing Site B GWs does not give ANA
       to GW1. Bring GW2 up while disaster is still set and confirm all Site A
       ANA groups stay ACTIVE on Site B. disaster-clear then relocates Site A
       ANA onto the recovered same-site GWs; remaining Site A GWs fail back
       one group each.
    ii. No disaster, all Site A GWs down: GW1 immediately takes all Site A ANA
        groups; remaining Site A GWs coming up fail back one group each.
    iii. Stop all Site A GWs: ANA fails over to Site B; disaster-set; Site A
        comes back AVAILABLE without ANA; Site B rebalance stays on B;
        Site B failback is allowed while A is still in disaster;
        disaster-clear drives monitor failback.
    """
    log.info(
        "TEST: Same-site ANA relocation, disaster-controlled failback, "
        "other-site failover while A is in disaster, and monitor-driven recovery"
    )
    nvme_service = config.get("nvme_service")
    if not nvme_service:
        nvme_service = NVMeService(config, ceph_cluster)
        nvme_service.init_gateways()
    config["nvme_service"] = nvme_service

    site_a_nodes = config.get("site_a_nodes") or config.get(
        "dc1_nodes", ["node3", "node4", "node5"]
    )
    if len(site_a_nodes) < 2:
        log.error("Need at least two Site A gateways, got %s", site_a_nodes)
        return 1
    site_loc = config.get("location_dc1", "DC1")
    peer_loc = config.get("location_dc2", "DC2")

    gw_locations = build_gw_location_map(
        nvme_service.gateways, _node_location_map(config)
    )
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways
    ha.set_gateway_locations(gw_locations)

    nvme_gw = _nvme_gw_cli(ceph_cluster)
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    nqn = ((config.get("subsystems") or [{}])[0].get("nqn")) or (
        (config.get("subsystems") or [{}])[0].get("subnqn")
    )
    if nqn:
        _ensure_ns_image_size(
            nvme_service.gateways[0],
            nqn,
            _list_namespaces(nvme_service.gateways[0], nqn),
            _ns_image_size(config),
        )
    site_a_gws = _gateways_for_nodes(nvme_service, site_a_nodes)
    gw1, remaining_a = site_a_gws[0], site_a_gws[1:]
    dc2_nodes = config.get("dc2_nodes", ["node7", "node8", "node9"])
    dc2_gws = _gateways_for_nodes(nvme_service, dc2_nodes)
    if not dc2_gws:
        log.error("Need at least one Site B gateway")
        return 1
    stopped = []

    def _restore():
        to_start = [gw for gw in stopped if gw]
        if to_start:
            try:
                log.info(
                    "Bringing up stopped GWs before disaster-clear: %s",
                    [gw.hostname for gw in to_start],
                )
                _start_gws(ha, to_start)
            except Exception as extra:
                log.warning("Start gateways during restore: %s", extra)
        try:
            nvme_gw.disaster_clear(pool, group, site_loc)
        except Exception as extra:
            log.warning("disaster-clear during restore: %s", extra)

    try:
        site_a_ana = {gw.ana_group_id for gw in site_a_gws}
        log.info(
            "Site A GWs %s (ANA %s); Site B GWs %s (ANA %s); "
            "disaster location %s; peer location %s",
            [gw.hostname for gw in site_a_gws],
            [gw.ana_group_id for gw in site_a_gws],
            [gw.hostname for gw in dc2_gws],
            [gw.ana_group_id for gw in dc2_gws],
            site_loc,
            peer_loc,
        )
        log.info("Site A ANA group ids: %s", site_a_ana)

        # --- Use case i: disaster state ---
        _log_step(
            1,
            3,
            "Use case i: stop all Site A GWs, disaster-set, recover GW1 then GW2, then disaster-clear",
            "GW1 and GW2 stay AVAILABLE without ACTIVE ANA until disaster-clear; Site A ANA stays ACTIVE on Site B",
        )
        log.info(
            "Use case i STEP 1/3: ceph orch daemon stop for Site A GWs %s, then disaster-set %s",
            [gw.hostname for gw in site_a_gws],
            site_loc,
        )
        _stop_gws(ha, site_a_gws)
        stopped.extend(site_a_gws)
        for gw in site_a_gws:
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
        nvme_gw.disaster_set(pool, group, site_loc)
        log.info("disaster-set issued for %s after all Site A GWs are down", site_loc)

        log.info("Recovering GW1 while Site A is in disaster")
        _start_gws(ha, [gw1])
        stopped = [gw for gw in stopped if gw is not gw1]
        _wait_for_gateway_state(nvme_gw, pool, group, gw1, "AVAILABLE")
        _wait_no_active_on_gws(
            nvme_gw,
            pool,
            group,
            [gw1],
            "GW1 acquired ANA while Site A is in disaster",
        )
        for ana_id in site_a_ana:
            _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
        log.info("GW1 is Standby; Site A ANA groups remain ACTIVE on %s", peer_loc)

        to_stop_b = dc2_gws[:2] if len(dc2_gws) > 1 else dc2_gws[:1]
        keep_b = [gw for gw in dc2_gws if gw not in to_stop_b]
        if not keep_b:
            log.error(
                "Need at least one Site B GW left up during partial Site A recovery"
            )
            return 1
        log.info(
            "Failing Site B GWs %s while only GW1 is recovered in disaster",
            [gw.hostname for gw in to_stop_b],
        )
        _stop_gws(ha, to_stop_b)
        stopped.extend(to_stop_b)
        for gw in to_stop_b:
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
        _wait_no_active_on_gws(
            nvme_gw,
            pool,
            group,
            [gw1],
            "Partially recovered Site A acquired ANA during Site B failover",
        )
        for ana_id in site_a_ana:
            _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
        log.info("Partially recovered Site A did not take ANA during Site B failover")
        log.info("Restoring Site B GWs before bringing GW2 up")

        _start_gws(ha, to_stop_b)
        stopped = [gw for gw in stopped if gw not in to_stop_b]
        for gw in to_stop_b:
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
        _wait_for_gateway_state(nvme_gw, pool, group, gw1, "AVAILABLE")

        gw2 = remaining_a[0]
        later_a = remaining_a[1:]
        log.info(
            "Bringing GW2 %s up while Site A is still in disaster",
            gw2.hostname,
        )
        _start_gws(ha, [gw2])
        stopped = [s for s in stopped if s is not gw2]
        _wait_for_gateway_state(nvme_gw, pool, group, gw2, "AVAILABLE")
        _wait_no_active_on_gws(
            nvme_gw,
            pool,
            group,
            [gw1, gw2],
            "GW1/GW2 acquired ANA while Site A is in disaster",
        )
        for ana_id in site_a_ana:
            _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
        log.info(
            "GW1 and GW2 are Standby; all Site A ANA groups remain ACTIVE on %s",
            peer_loc,
        )

        log.info(
            "Issuing disaster-clear for %s with GW1 and GW2 up so recovered "
            "same-site GWs can take Site A ANA groups",
            site_loc,
        )
        nvme_gw.disaster_clear(pool, group, site_loc)
        recovered_a = [gw1, gw2]
        held = _wait_recovered_hold_site_a_ana(
            nvme_gw, pool, group, recovered_a, site_a_ana
        )
        log.info(
            "After disaster-clear, recovered Site A GWs hold ANA groups %s",
            held,
        )

        for gw in later_a:
            _start_gws(ha, [gw])
            stopped = [s for s in stopped if s is not gw]
            recovered_a.append(gw)
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            held = _wait_recovered_hold_site_a_ana(
                nvme_gw, pool, group, recovered_a, site_a_ana
            )
            log.info(
                "After recovering %s, Site A ANA is split across recovered GWs: %s",
                gw.hostname,
                held,
            )
        log.info("Use case i completed: recovered Site A ANA map %s", held)

        # --- Use case ii: no disaster ---
        _log_step(
            2,
            3,
            "Use case ii: stop all Site A GWs without disaster, recover GW1 then remaining Site A GWs",
            "GW1 immediately takes all Site A ANA groups; each remaining GW failback receives one group",
        )
        log.info("Use case ii: all Site A GWs down without disaster")
        _stop_gws(ha, site_a_gws)
        stopped.extend(site_a_gws)
        for gw in site_a_gws:
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")

        _start_gws(ha, [gw1])
        stopped = [s for s in stopped if s is not gw1]
        _wait_for_gateway_state(nvme_gw, pool, group, gw1, "AVAILABLE")
        recovered = _wait_for_active_ana_contains(nvme_gw, pool, group, gw1, site_a_ana)
        log.info(
            "Without disaster, GW1 immediately holds Site A ANA groups %s (active=%s)",
            site_a_ana,
            recovered,
        )

        held = {gw1.hostname: set(recovered)}
        recovered_a = [gw1]
        for gw in remaining_a:
            _start_gws(ha, [gw])
            stopped = [s for s in stopped if s is not gw]
            recovered_a.append(gw)
            _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            held = _wait_recovered_hold_site_a_ana(
                nvme_gw, pool, group, recovered_a, site_a_ana
            )
            log.info(
                "GW %s failback received a Site A ANA group; recovered map %s",
                gw.hostname,
                held,
            )
        log.info("Use case ii completed: recovered Site A ANA map %s", held)

        # --- Use case iii: full site-A outage, disaster, B-only rebalance ---
        _log_step(
            3,
            3,
            "Use case iii: all Site A down, disaster-set, B rebalance, allowed B failback, disaster-clear",
            "ANA stays on B until disaster-clear; B GWs may fail back while A is in disaster; then ANA returns to A",
        )
        log.info(
            "Use case iii: stop all Site A GWs, disaster-set, B rebalance, disaster-clear"
        )
        dc1_gws = list(site_a_gws)
        dc1_ana = set(site_a_ana)
        all_ana = {gw.ana_group_id for gw in nvme_service.gateways}
        ns_info = _collect_ns_info(nvme_service.gateways[0], all_ana)
        with _background_io(config, nvme_service, ceph_cluster, runtime=1200):
            if ns_info:
                validate_io(ha.orch, ns_info)

            _stop_gws(ha, dc1_gws)
            stopped.extend(dc1_gws)
            for gw in dc1_gws:
                _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
            for ana_id in dc1_ana:
                _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
            log.info(
                "All Site A ANA groups are ACTIVE on %s after site A stop", peer_loc
            )
            if ns_info:
                validate_io(ha.orch, ns_info)

            nvme_gw.disaster_set(pool, group, site_loc)
            log.info(
                "Issued disaster-set for %s; failback to Site A is now blocked",
                site_loc,
            )

            _start_gws(ha, dc1_gws)
            stopped = [gw for gw in stopped if gw not in dc1_gws]
            for gw in dc1_gws:
                _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            time.sleep(15)
            try:
                _assert_no_active_on_gws(
                    nvme_gw,
                    pool,
                    group,
                    dc1_gws,
                    "Site A acquired ANA while in disaster",
                )
            except AssertionError as exc:
                log.error("%s", exc)
                return 1
            for ana_id in dc1_ana:
                _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
            log.info("Site A GWs are AVAILABLE without ANA ownership; IO stays on B")
            if ns_info:
                validate_io(ha.orch, ns_info)

            to_stop_b = dc2_gws[:2] if len(dc2_gws) > 1 else dc2_gws[:1]
            keep_b = [gw for gw in dc2_gws if gw not in to_stop_b]
            if not keep_b:
                log.error("Need at least one Site B GW left up for rebalance")
                return 1
            log.info(
                "Stopping Site B GWs %s to force rebalance onto %s",
                [gw.hostname for gw in to_stop_b],
                [gw.hostname for gw in keep_b],
            )
            _stop_gws(ha, to_stop_b)
            stopped.extend(to_stop_b)
            for gw in to_stop_b:
                _wait_for_gateway_state(nvme_gw, pool, group, gw, "UNAVAILABLE")
            time.sleep(10)
            try:
                _assert_no_active_on_gws(
                    nvme_gw,
                    pool,
                    group,
                    dc1_gws,
                    "Site B rebalance moved ACTIVE ANA to Site A",
                )
            except AssertionError as exc:
                log.error("%s", exc)
                return 1
            for ana_id in dc1_ana:
                _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
            log.info("Rebalance stayed on Site B; Site A still has no ACTIVE ANA")
            if ns_info:
                validate_io(ha.orch, ns_info)

            _start_gws(ha, to_stop_b)
            stopped = [gw for gw in stopped if gw not in to_stop_b]
            for gw in to_stop_b:
                _wait_for_gateway_state(nvme_gw, pool, group, gw, "AVAILABLE")
            for gw in to_stop_b:
                _wait_gw_owns_own_ana(nvme_gw, pool, group, gw)
            _wait_no_active_on_gws(
                nvme_gw,
                pool,
                group,
                dc1_gws,
                "Site A acquired ANA during allowed Site B failback",
            )
            for ana_id in dc1_ana:
                _wait_ana_active_on_location(nvme_gw, pool, group, ana_id, peer_loc)
            log.info(
                "Site B failback is allowed while Site A remains in disaster; "
                "Site A still has no ACTIVE ANA"
            )
            if ns_info:
                validate_io(ha.orch, ns_info)

            log.info(
                "Issuing disaster-clear for %s; monitor should fail ANA back to Site A",
                site_loc,
            )
            nvme_gw.disaster_clear(pool, group, site_loc)
            recovered = []
            for _ in WaitUntil(timeout=180, interval=5):
                recovered = []
                for gw in dc1_gws:
                    recovered.extend(
                        _active_ana_ids(
                            _gw_show_entry(
                                _show_created_gateways(nvme_gw, pool, group), gw
                            )
                        )
                    )
                if set(recovered) == dc1_ana:
                    break
                log.info(
                    "Waiting for Site A ANA failback: have %s expect %s",
                    recovered,
                    dc1_ana,
                )
            if set(recovered) != dc1_ana:
                log.error(
                    "After disaster-clear, Site A ANA %s != expected %s",
                    recovered,
                    dc1_ana,
                )
                return 1
            log.info(
                "Monitor-driven failback restored Site A ANA groups: %s", recovered
            )
            if ns_info:
                validate_io(ha.orch, ns_info)
        log.info("Use case iii completed")
        log.info(
            "TEST PASS: ANA relocation, disaster control, peer-site failover, and monitor failback"
        )
        return 0
    except Exception:
        log.exception("ANA relocation failed; restoring gateways and disaster state")
        raise
    finally:
        log.info("Restoring disaster-clear and any gateways still stopped")
        _restore()


def run(ceph_cluster, **kw):
    """
    Entry point for stretch-cluster NVMe-oF tests.

    config.test_case:
        deploy_stretch       - Deploy two-site stretch with tie-breaker only.
        deploy_stretch_nvme  - Deploy stretch then NVMe (3 GW per DC) + configure entities + initiators.
        gw_locations         - Set/get/modify/unset GW locations.
        namespace_locations  - Valid NS + ANA + IO, invalid reject, change_location + ANA + IO.
        homeless_namespaces  - Homeless NS after unsetting a location, recover, rebalance, new GW.
        failover_failback    - Single GW orch daemon failover then nvme-gw disable/enable with IO.
        ana_relocation       - Same-site ANA relocation, partial-recovery B failover,
                               allowed B failback, monitor failback.
    """
    config = kw.get("config", {})
    test_case = config.get("test_case", "deploy_stretch")
    rbd_obj = None
    if config.get("rep_pool_config") or config.get("rep-pool-only"):
        rbd_obj = initial_rbd_config(**kw).get("rbd_reppool")

    try:
        if test_case == "deploy_stretch":
            return _deploy_stretch_cluster(ceph_cluster, config)

        if test_case == "deploy_stretch_nvme":
            if _deploy_stretch_cluster(ceph_cluster, config) != 0:
                return 1
            return _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj)

        if test_case == "deploy_nvme":
            return _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj)

        if test_case == "gw_locations":
            return _test_gw_locations(ceph_cluster, config)

        if test_case == "namespace_locations":
            return _test_namespace_locations(ceph_cluster, config)

        if test_case == "homeless_namespaces":
            return _test_homeless_namespaces(ceph_cluster, config)

        if test_case == "failover_failback":
            return _test_failover_failback(ceph_cluster, config)

        if test_case == "ana_relocation":
            return _test_ana_relocation(ceph_cluster, config)

        log.error("Unknown test_case: %s", test_case)
        return 1
    except Exception as e:
        log.exception(e)
        return 1
    finally:
        try:
            rados_obj = RadosOrchestrator(
                node=CephAdmin(cluster=ceph_cluster, **config)
            )
            rados_obj.log_cluster_health()
        except Exception as health_err:
            log.warning("Could not log cluster health: %s", health_err)
        # Always drop initiator sessions after a stretch NVMe case (pass or fail).
        # Leave the gateway service running unless this test requested full cleanup.
        try:
            _disconnect_configured_initiators(ceph_cluster, config)
        except Exception as disc_err:
            log.warning("Initiator disconnect in finally failed: %s", disc_err)
        if config.get("cleanup"):
            from tests.nvmeof.workflows.gateway_entities import teardown

            nvme_svc = config.get("nvme_service")
            if nvme_svc:
                try:
                    teardown(nvme_svc, rbd_obj)
                except Exception as teardown_err:
                    log.error("Teardown in finally failed: %s", teardown_err)


def stretch_mode_status(rados_obj) -> bool:
    """
    Returns the status of stretch mode on cluster.

    Args:
        rados_obj: rados object for command execution

    Returns:
        if stretch mode is enabled on cluster -> True,
        If stretch mode is not enabled on cluster -> False
    """
    log.debug("Running checks to see if stretch mode is deployed on the cluster")
    stretch_details = rados_obj.get_stretch_mode_dump()
    return stretch_details["stretch_mode_enabled"]
