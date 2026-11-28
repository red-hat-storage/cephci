"""Verify an RGW multisite realm is configured on a cluster.

The suite steps that run ``radosgw-admin realm list`` and ``sync status``
only succeed when those commands exit 0. A realm, zone, endpoint, period,
or daemon setting can still be wrong. This module reads the configuration
and fails when it does not match the layout declared in the test.

With no realm, zonegroup, or zone in the config, the module reads the live
configuration. That covers two-zone and three-zone realms, archive zones,
HTTP and HTTPS endpoints, and HAProxy endpoints such as ``http://host:5000``.
A zone syncs from the peers in its ``sync_from`` list when ``sync_from_all``
is false, which is how archive zones are configured.

The primary cluster is checked first, then the secondary, then a tertiary or
archive cluster. Sync status is read immediately. When it already matches, the
suite continues. When it does not, only the primary cluster repeats the check
until 120 seconds have passed. The other clusters are checked once.

The runner calls ``run`` once per cluster. The first successful site stores
its realm, zonegroup, period, and zone list in ``test_data``. The next site
in the same realm must report the same ids.

Example::

    - test:
        abort-on-fail: true
        clusters:
          ceph-pri:
            config:
              realm: india
              zonegroup: shared
              zone: primary
              master: true
              endpoints:
                - "http://{node_ip:node3}:80"
              zones:
                - primary
                - secondary
              sync_user: repuser
              access_key: 21e86bce636c3aa0
              rgw_service_id: shared.pri
          ceph-sec:
            config:
              realm: india
              zonegroup: shared
              zone: secondary
              master: false
              master_zone: primary
              endpoints:
                - "http://{node_ip:node3}:80"
              zones:
                - primary
                - secondary
              access_key: 21e86bce636c3aa0
              rgw_service_id: shared.sec
        module: verify_multisite_config.py
        name: verify multisite configuration
"""

import json
import re
import time

from utility.log import Log

log = Log(__name__)

SNAPSHOT_KEY = "rgw_multisite_config"
SYNC_WAIT_KEY = "rgw_multisite_sync_wait"
SYNC_TIMEOUT = 120

SYNC_ERROR_MARKERS = (
    "failed to fetch master sync status",
    "failed to retrieve sync info",
    "failed to connect",
    "input/output error",
    "permission denied",
    "couldn't init storage provider",
    "could not init storage",
    "unknown error 2200",
    "(2200)",
)


class CheckFailure:
    """One multisite configuration mismatch."""

    def __init__(self, message, retryable=False):
        self.message = message
        self.retryable = retryable

    def __str__(self):
        return self.message


def as_bool(value):
    """Return True for boolean true and common true-like strings."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes"}


def normalize_endpoint(endpoint):
    """Return an endpoint without surrounding space or a trailing slash."""
    return str(endpoint).strip().rstrip("/")


_ENDPOINT_RE = re.compile(r"^https?://[^\s/,]+(?::\d+)?$", re.IGNORECASE)


def iter_endpoints(values):
    """Yield normalized endpoints, splitting comma-separated RGW endpoint lists."""
    for value in values or []:
        if value is None:
            continue
        for part in str(value).split(","):
            part = part.strip()
            if part:
                yield normalize_endpoint(part)


def _endpoint_set(values):
    return set(iter_endpoints(values))


def _invalid_endpoints(values):
    """Return endpoints that are not http or https URLs.

    SSL zones use ``https://host`` or ``https://host:443``. Non-SSL and HAProxy
    zones use ``http://host:80`` or ``http://host:5000``. A missing scheme or an
    empty list is a configuration error in every layout.
    """
    endpoints = list(iter_endpoints(values))
    if not endpoints:
        return ["<none>"]
    return [endpoint for endpoint in endpoints if not _ENDPOINT_RE.match(endpoint)]


def _is_archive(zone_entry):
    tier_type = str((zone_entry or {}).get("tier_type") or "").strip().lower()
    return tier_type == "archive"


def _sync_from_all(zone_entry):
    """RGW syncs from every other zone unless sync_from_all is false."""
    if "sync_from_all" not in (zone_entry or {}):
        return True
    value = zone_entry.get("sync_from_all")
    if value in (None, ""):
        return True
    return as_bool(value)


def _sync_peer_names(zone_entry, zones_by_name):
    """Return zone names this zone is configured to sync from."""
    local_name = (zone_entry or {}).get("name")
    id_to_name = {
        entry.get("id"): name
        for name, entry in (zones_by_name or {}).items()
        if entry.get("id")
    }
    if not _sync_from_all(zone_entry):
        peers = []
        for item in zone_entry.get("sync_from") or []:
            peers.append(id_to_name.get(item, item))
        return peers
    return [name for name in zones_by_name or {} if name != local_name]


def parse_json_output(text):
    """Return the first JSON object or array in a command's output."""
    raw = (text or "").strip()
    decoder = json.JSONDecoder()
    for index, char in enumerate(raw):
        if char not in "{[":
            continue
        try:
            value, _end = decoder.raw_decode(raw[index:])
        except json.JSONDecodeError:
            continue
        return value
    preview = raw[:400]
    raise ValueError(f"command output is not JSON: {preview}")


def sync_status_problems(text):
    """Return sync-status markers that mean the zone is not configured to sync."""
    lowered = (text or "").lower()
    found = [marker for marker in SYNC_ERROR_MARKERS if marker in lowered]
    if re.search(r"\bERROR\b", text or ""):
        found.append("ERROR")
    return found


def sync_status_names(text):
    """Return the realm, zonegroup, and zone named in the sync status header."""
    names = {}
    for label in ("realm", "zonegroup", "zone"):
        match = re.search(rf"(?m)^\s*{label}\s+\S+\s+\(([^)]+)\)", text or "")
        if match:
            names[label] = match.group(1)
    return names


def resolve_expected(config):
    """Return the multisite layout this cluster is expected to serve.

    Raises:
        ValueError: realm, zonegroup, or zone was not provided.
    """
    realm = config.get("realm")
    zonegroup = config.get("zonegroup")
    zone = config.get("zone")
    if not realm or not zonegroup or not zone:
        raise ValueError("realm, zonegroup, and zone are required")

    master = as_bool(config.get("master", False))
    zones = set(config.get("zones") or [])
    zones.update(config.get("peer_zones") or [])
    zones.add(zone)

    if "verify_user" in config:
        verify_user = as_bool(config.get("verify_user"))
    else:
        verify_user = master and bool(config.get("sync_user"))

    if "require_zone_default" in config:
        require_zone_default = as_bool(config.get("require_zone_default"))
    else:
        require_zone_default = master

    if "require_zonegroup_default" in config:
        require_zonegroup_default = as_bool(config.get("require_zonegroup_default"))
    else:
        require_zonegroup_default = master

    return {
        "realm": realm,
        "zonegroup": zonegroup,
        "zone": zone,
        "master": master,
        "master_zone": config.get("master_zone") or (zone if master else None),
        "zones": zones,
        "endpoints": [
            normalize_endpoint(item) for item in config.get("endpoints") or []
        ],
        "sync_user": config.get("sync_user"),
        "access_key": config.get("access_key"),
        "verify_user": verify_user,
        "system_user": as_bool(config.get("system_user", True)),
        "rgw_service_id": config.get("rgw_service_id"),
        "require_realm_default": as_bool(config.get("require_realm_default", True)),
        "require_zonegroup_default": require_zonegroup_default,
        "require_zone_default": require_zone_default,
        "zonegroup_master": as_bool(config.get("zonegroup_master", True)),
        "verify_sync": as_bool(config.get("verify_sync", True)),
        "verify_sync_peers": as_bool(config.get("verify_sync_peers", True)),
        "check_cluster_health": as_bool(config.get("check_cluster_health", True)),
    }


def _zones_by_name(zonegroup):
    zones = {}
    for zone in zonegroup.get("zones") or []:
        name = zone.get("name")
        if name:
            zones[name] = zone
    return zones


def _fail(errors, message, retryable=False):
    errors.append(CheckFailure(message, retryable=retryable))


def validate_multisite(observed, expected):
    """Return configuration mismatches for one cluster.

    Sync, daemon startup, and ``HEALTH_ERR`` are retryable. A wrong realm,
    zone, endpoint, period, or access key is returned immediately.
    """
    errors = []
    realm_list = observed.get("realm_list") or {}
    realm = observed.get("realm") or {}
    zonegroup_list = observed.get("zonegroup_list") or {}
    zonegroup = observed.get("zonegroup") or {}
    zone_list = observed.get("zone_list") or {}
    zone = observed.get("zone") or {}
    period = observed.get("period") or {}
    zones = _zones_by_name(zonegroup)

    realm_names = realm_list.get("realms") or []
    if expected["realm"] not in realm_names:
        _fail(
            errors, f"realm {expected['realm']} is absent from realm list {realm_names}"
        )
    if realm.get("name") != expected["realm"]:
        _fail(
            errors, f"realm name is {realm.get('name')}, expected {expected['realm']}"
        )
    if expected["require_realm_default"] and realm_list.get(
        "default_info"
    ) != realm.get("id"):
        _fail(
            errors,
            "realm "
            f"{expected['realm']} is not the default realm: "
            f"default id {realm_list.get('default_info')} != {realm.get('id')}",
        )
    if not realm.get("current_period"):
        _fail(errors, f"realm {expected['realm']} has no committed period")

    zonegroup_names = zonegroup_list.get("zonegroups") or []
    if expected["zonegroup"] not in zonegroup_names:
        _fail(
            errors,
            f"zonegroup {expected['zonegroup']} is absent from zonegroup list {zonegroup_names}",
        )
    if zonegroup.get("name") != expected["zonegroup"]:
        _fail(
            errors,
            f"zonegroup name is {zonegroup.get('name')}, expected {expected['zonegroup']}",
        )
    if expected["require_zonegroup_default"] and zonegroup_list.get(
        "default_info"
    ) != zonegroup.get("id"):
        _fail(
            errors,
            "zonegroup "
            f"{expected['zonegroup']} is not the default zonegroup: "
            f"default id {zonegroup_list.get('default_info')} != {zonegroup.get('id')}",
        )
    if as_bool(zonegroup.get("is_master")) != expected["zonegroup_master"]:
        _fail(
            errors,
            f"zonegroup {expected['zonegroup']} is_master is "
            f"{zonegroup.get('is_master')}, expected {expected['zonegroup_master']}",
        )
    if zonegroup.get("realm_id") and zonegroup.get("realm_id") != realm.get("id"):
        _fail(
            errors,
            f"zonegroup realm id {zonegroup.get('realm_id')} != realm id {realm.get('id')}",
        )

    zone_names = zone_list.get("zones") or []
    if expected["zone"] not in zone_names:
        _fail(errors, f"zone {expected['zone']} is absent from zone list {zone_names}")
    if zone.get("name") != expected["zone"]:
        _fail(errors, f"zone name is {zone.get('name')}, expected {expected['zone']}")
    if expected["require_zone_default"] and zone_list.get("default_info") != zone.get(
        "id"
    ):
        _fail(
            errors,
            "zone "
            f"{expected['zone']} is not the default zone: "
            f"default id {zone_list.get('default_info')} != {zone.get('id')}",
        )
    if zone.get("realm_id") and zone.get("realm_id") != realm.get("id"):
        _fail(
            errors,
            f"zone realm id {zone.get('realm_id')} != realm id {realm.get('id')}",
        )

    local_zone = zones.get(expected["zone"])
    if local_zone is None:
        _fail(
            errors,
            f"zonegroup {expected['zonegroup']} does not include zone {expected['zone']}. "
            f"zones={sorted(zones)}",
        )
    elif local_zone.get("id") != zone.get("id"):
        _fail(
            errors,
            f"zone {expected['zone']} id {zone.get('id')} != zonegroup copy {local_zone.get('id')}",
        )

    missing_zones = sorted(expected["zones"] - set(zones))
    if missing_zones:
        _fail(
            errors,
            f"zonegroup {expected['zonegroup']} is missing zones {missing_zones}. "
            f"found={sorted(zones)}",
        )
    for zone_name in sorted(expected["zones"]):
        entry = zones.get(zone_name)
        if entry is None:
            continue
        invalid = _invalid_endpoints(entry.get("endpoints"))
        if invalid == ["<none>"]:
            _fail(errors, f"zone {zone_name} has no endpoints")
        elif invalid:
            _fail(errors, f"zone {zone_name} has invalid endpoints {invalid}")
        if _is_archive(entry) and entry.get("id") == zonegroup.get("master_zone"):
            _fail(errors, f"archive zone {zone_name} is the master zone")
        if not _sync_from_all(entry) and not (entry.get("sync_from") or []):
            _fail(
                errors,
                f"zone {zone_name} sync_from_all is false and sync_from is empty",
            )

    if local_zone is not None and expected["endpoints"]:
        actual_endpoints = _endpoint_set(local_zone.get("endpoints"))
        for endpoint in expected["endpoints"]:
            if endpoint not in actual_endpoints:
                _fail(
                    errors,
                    f"zone {expected['zone']} endpoints {sorted(actual_endpoints)} "
                    f"do not include {endpoint}",
                )
        if zone.get("endpoints") is not None:
            zone_endpoints = _endpoint_set(zone.get("endpoints"))
            for endpoint in expected["endpoints"]:
                if endpoint not in zone_endpoints:
                    _fail(
                        errors,
                        f"zone get endpoints {sorted(zone_endpoints)} do not include {endpoint}",
                    )

    master_zone_id = zonegroup.get("master_zone")
    if expected["master"]:
        if local_zone is not None and local_zone.get("id") != master_zone_id:
            _fail(errors, f"zone {expected['zone']} is not the master zone")
    elif expected["master_zone"]:
        master_zone = zones.get(expected["master_zone"])
        if master_zone is None:
            _fail(
                errors, f"master zone {expected['master_zone']} is not in the zonegroup"
            )
        elif master_zone.get("id") != master_zone_id:
            _fail(
                errors,
                f"master zone id is {master_zone_id}, "
                f"expected {expected['master_zone']} ({master_zone.get('id')})",
            )
    elif local_zone is not None and local_zone.get("id") == master_zone_id:
        _fail(errors, f"zone {expected['zone']} is the master zone")

    if expected["access_key"]:
        actual_key = (zone.get("system_key") or {}).get("access_key")
        if actual_key != expected["access_key"]:
            _fail(
                errors,
                f"zone {expected['zone']} system access key is {actual_key!r}, "
                f"expected {expected['access_key']!r}",
            )

    if expected["verify_user"]:
        _validate_sync_user(errors, observed.get("user") or {}, expected)

    _validate_period(errors, period, realm, zonegroup, zones, expected)
    _validate_sync_status(
        errors,
        observed.get("sync_status") or "",
        expected,
        local_zone,
        zones,
    )
    _validate_health(errors, observed.get("health") or "", expected)
    _validate_daemons(errors, observed, expected)
    return errors


def _validate_sync_user(errors, user, expected):
    uid = expected["sync_user"]
    if not user:
        _fail(errors, f"sync user {uid} was not found")
        return
    if user.get("user_id") not in (None, uid):
        _fail(errors, f"sync user id is {user.get('user_id')}, expected {uid}")
    if expected["system_user"] and not as_bool(user.get("system")):
        _fail(errors, f"user {uid} is not a system user")
    if expected["access_key"]:
        keys = [item.get("access_key") for item in user.get("keys") or []]
        if expected["access_key"] not in keys:
            _fail(
                errors, f"user {uid} does not have access key {expected['access_key']}"
            )


def _validate_period(errors, period, realm, zonegroup, zones, expected):
    if period.get("id") != realm.get("current_period"):
        _fail(
            errors,
            f"period id {period.get('id')} != realm current_period {realm.get('current_period')}",
        )
    epoch = period.get("epoch")
    if epoch not in (None, ""):
        try:
            epoch_value = int(epoch)
        except (TypeError, ValueError):
            _fail(errors, f"period epoch is {epoch}")
        else:
            if epoch_value < 1:
                _fail(errors, f"period epoch is {epoch}")
    if period.get("realm_id") and period.get("realm_id") != realm.get("id"):
        _fail(
            errors,
            f"period realm id {period.get('realm_id')} != realm id {realm.get('id')}",
        )
    if period.get("realm_name") not in (None, "", expected["realm"]):
        _fail(
            errors,
            f"period realm name is {period.get('realm_name')}, expected {expected['realm']}",
        )
    if period.get("master_zonegroup") != zonegroup.get("id"):
        _fail(
            errors,
            f"period master zonegroup {period.get('master_zonegroup')} != "
            f"zonegroup id {zonegroup.get('id')}",
        )
    if period.get("master_zone") != zonegroup.get("master_zone"):
        _fail(
            errors,
            f"period master zone {period.get('master_zone')} != "
            f"zonegroup master zone {zonegroup.get('master_zone')}",
        )

    period_groups = (period.get("period_map") or {}).get("zonegroups") or []
    period_group = None
    for group in period_groups:
        if (
            group.get("id") == zonegroup.get("id")
            or group.get("name") == expected["zonegroup"]
        ):
            period_group = group
            break
    if period_group is None:
        _fail(errors, f"period map does not include zonegroup {expected['zonegroup']}")
        return

    period_zones = {item.get("name"): item for item in period_group.get("zones") or []}
    missing = sorted(expected["zones"] - set(period_zones))
    if missing:
        _fail(
            errors,
            f"period map is missing zones {missing}. found={sorted(period_zones)}",
        )
    local_period_zone = period_zones.get(expected["zone"])
    if local_period_zone is not None and expected["endpoints"]:
        actual_endpoints = _endpoint_set(local_period_zone.get("endpoints"))
        for endpoint in expected["endpoints"]:
            if endpoint not in actual_endpoints:
                _fail(
                    errors,
                    f"committed period endpoints for {expected['zone']} are "
                    f"{sorted(actual_endpoints)}, missing {endpoint}",
                )
    if zones and local_period_zone is not None:
        live_zone = zones.get(expected["zone"]) or {}
        if live_zone.get("id") and local_period_zone.get("id") != live_zone.get("id"):
            _fail(
                errors,
                f"period zone id {local_period_zone.get('id')} != "
                f"zonegroup zone id {live_zone.get('id')}",
            )


def _validate_sync_status(
    errors, sync_status, expected, local_zone=None, zones_by_name=None
):
    if not expected["verify_sync"]:
        return
    problems = sync_status_problems(sync_status)
    if problems:
        _fail(
            errors,
            f"sync status reports {problems}",
            retryable=True,
        )
    if not sync_status.strip():
        _fail(errors, "sync status is empty", retryable=True)
        return
    reported = sync_status_names(sync_status)
    for label in ("realm", "zonegroup", "zone"):
        if reported.get(label) != expected[label]:
            _fail(
                errors,
                f"sync status {label} is {reported.get(label)}, expected {expected[label]}",
                retryable=True,
            )

    lowered = sync_status.lower()
    if "metadata sync" not in lowered:
        _fail(errors, "sync status has no metadata sync section", retryable=True)
    elif expected["master"]:
        if "zone is master" not in lowered:
            _fail(
                errors,
                "master zone metadata sync does not report zone is master",
                retryable=True,
            )
    elif "zone is master" in lowered:
        _fail(
            errors,
            f"zone {expected['zone']} reports metadata sync as master",
            retryable=True,
        )

    if expected["verify_sync_peers"]:
        if local_zone and zones_by_name:
            peers = _sync_peer_names(local_zone, zones_by_name)
        else:
            peers = sorted(expected["zones"] - {expected["zone"]})
        for peer in peers:
            if peer == expected["zone"]:
                continue
            if f"({peer})" not in sync_status:
                _fail(
                    errors,
                    f"sync status does not list peer zone ({peer})",
                    retryable=True,
                )


def _validate_health(errors, health, expected):
    if not expected["check_cluster_health"]:
        return
    if not health.strip():
        _fail(errors, "ceph status is empty", retryable=True)
        return
    if "HEALTH_ERR" in health:
        _fail(errors, "cluster health is HEALTH_ERR", retryable=True)


def _daemon_matches(daemon, service_id):
    if daemon.get("daemon_type") not in (None, "rgw"):
        return False
    daemon_id = str(daemon.get("daemon_id") or "")
    service_name = str(daemon.get("service_name") or "")
    return (
        service_name == f"rgw.{service_id}"
        or daemon_id == service_id
        or daemon_id.startswith(f"{service_id}.")
    )


def _validate_daemons(errors, observed, expected):
    service_id = expected.get("rgw_service_id")
    if not service_id:
        return
    daemons = observed.get("daemons") or []
    if isinstance(daemons, dict):
        daemons = daemons.get("daemons") or daemons.get("items") or []
    matched = [daemon for daemon in daemons if _daemon_matches(daemon, service_id)]
    if not matched:
        _fail(
            errors,
            f"no running RGW daemon found for service {service_id}",
            retryable=True,
        )
        return

    daemon_config = observed.get("daemon_config") or {}
    for daemon in matched:
        daemon_id = daemon.get("daemon_id")
        status = (daemon.get("status_desc") or daemon.get("status") or "").lower()
        if status != "running":
            _fail(
                errors,
                f"RGW daemon {daemon_id} status is {status or 'unknown'}",
                retryable=True,
            )
            continue
        config = daemon_config.get(daemon_id) or {}
        expected_config = {
            "rgw_realm": expected["realm"],
            "rgw_zonegroup": expected["zonegroup"],
            "rgw_zone": expected["zone"],
        }
        for key, expected_value in expected_config.items():
            actual = (config.get(key) or "").strip()
            if actual != expected_value:
                _fail(
                    errors,
                    f"client.rgw.{daemon_id} {key} is {actual!r}, expected {expected_value!r}",
                )


def site_snapshot(cluster_name, observed, expected):
    """Return the ids used to compare this site with its peers."""
    zonegroup = observed.get("zonegroup") or {}
    zone_names = sorted(
        zone.get("name") for zone in zonegroup.get("zones") or [] if zone.get("name")
    )
    return {
        "cluster": cluster_name,
        "realm": expected["realm"],
        "realm_id": (observed.get("realm") or {}).get("id"),
        "zonegroup": expected["zonegroup"],
        "zonegroup_id": zonegroup.get("id"),
        "zone": expected["zone"],
        "period_id": (observed.get("period") or {}).get("id"),
        "master_zone_id": zonegroup.get("master_zone"),
        "zones": zone_names,
    }


def compare_sites(snapshot, peers):
    """Return mismatches between this site and sites already verified."""
    errors = []
    for other_name, other in peers.items():
        if other_name == snapshot["cluster"]:
            continue
        if (
            other.get("realm") != snapshot["realm"]
            or other.get("zonegroup") != snapshot["zonegroup"]
        ):
            continue
        for field, label in (
            ("realm_id", "realm id"),
            ("zonegroup_id", "zonegroup id"),
            ("period_id", "period id"),
            ("master_zone_id", "master zone id"),
        ):
            if snapshot.get(field) != other.get(field):
                _fail(
                    errors,
                    f"{label} on {snapshot['cluster']} is {snapshot.get(field)}, "
                    f"on {other_name} is {other.get(field)}",
                    retryable=True,
                )
        if snapshot.get("zones") != other.get("zones"):
            _fail(
                errors,
                f"zone list on {snapshot['cluster']} is {snapshot.get('zones')}, "
                f"on {other_name} is {other.get('zones')}",
                retryable=True,
            )
    return errors


def _translate_endpoints(endpoints, clusters, cluster_name):
    normalized = [normalize_endpoint(endpoint) for endpoint in endpoints or []]
    if not any("{node_ip:" in endpoint for endpoint in normalized):
        return normalized
    if not clusters or not cluster_name:
        raise ValueError("ceph_cluster_dict is required to resolve endpoint node IPs")
    from ceph.utils import translate_to_ip

    return [
        normalize_endpoint(translate_to_ip(clusters, cluster_name, endpoint))
        for endpoint in normalized
    ]


def _exec(node, command, use_cephadm):
    if use_cephadm:
        command = f"cephadm shell -- {command}"
    log.info("Executing command: %s", command)
    out, _err = node.exec_command(sudo=True, cmd=command, check_ec=True)
    if isinstance(out, bytes):
        return out.decode("utf-8", errors="replace")
    return "" if out is None else str(out)


def _config_get(node, daemon_id, key, use_cephadm):
    out = _exec(node, f"ceph config get client.rgw.{daemon_id} {key}", use_cephadm)
    return out.strip().strip('"')


def collect_multisite(node, expected, use_cephadm):
    """Read realm, zone, period, sync status, and RGW daemon config."""
    realm = expected["realm"]
    zonegroup = expected["zonegroup"]
    zone = expected["zone"]
    observed = {
        "realm_list": parse_json_output(
            _exec(node, "radosgw-admin realm list", use_cephadm)
        ),
        "realm": parse_json_output(
            _exec(node, f"radosgw-admin realm get --rgw-realm {realm}", use_cephadm)
        ),
        "zonegroup_list": parse_json_output(
            _exec(node, "radosgw-admin zonegroup list", use_cephadm)
        ),
        "zonegroup": parse_json_output(
            _exec(
                node,
                f"radosgw-admin zonegroup get --rgw-zonegroup {zonegroup}",
                use_cephadm,
            )
        ),
        "zone_list": parse_json_output(
            _exec(node, "radosgw-admin zone list", use_cephadm)
        ),
        "zone": parse_json_output(
            _exec(node, f"radosgw-admin zone get --rgw-zone {zone}", use_cephadm)
        ),
        "period": parse_json_output(
            _exec(node, "radosgw-admin period get", use_cephadm)
        ),
        "sync_status": _exec(
            node,
            "radosgw-admin sync status "
            f"--rgw-realm {realm} --rgw-zonegroup {zonegroup} --rgw-zone {zone}",
            use_cephadm,
        ),
        "health": _exec(node, "ceph -s", use_cephadm),
    }
    if expected["verify_user"] and expected["sync_user"]:
        observed["user"] = parse_json_output(
            _exec(
                node,
                f"radosgw-admin user info --uid={expected['sync_user']} --rgw-realm {realm}",
                use_cephadm,
            )
        )

    service_id = expected.get("rgw_service_id")
    if service_id:
        daemons = parse_json_output(
            _exec(node, "ceph orch ps --daemon_type rgw --format json", use_cephadm)
        )
        observed["daemons"] = daemons
        daemon_config = {}
        daemon_list = daemons
        if isinstance(daemons, dict):
            daemon_list = daemons.get("daemons") or daemons.get("items") or []
        for daemon in daemon_list:
            if not _daemon_matches(daemon, service_id):
                continue
            daemon_id = daemon.get("daemon_id")
            daemon_config[daemon_id] = {
                "rgw_realm": _config_get(node, daemon_id, "rgw_realm", use_cephadm),
                "rgw_zonegroup": _config_get(
                    node, daemon_id, "rgw_zonegroup", use_cephadm
                ),
                "rgw_zone": _config_get(node, daemon_id, "rgw_zone", use_cephadm),
            }
        observed["daemon_config"] = daemon_config
    return observed


def _log_observed(cluster_name, observed):
    realm = observed.get("realm") or {}
    zonegroup = observed.get("zonegroup") or {}
    zone = observed.get("zone") or {}
    period = observed.get("period") or {}
    zone_names = [item.get("name") for item in zonegroup.get("zones") or []]
    log.info(
        "Multisite read on %s: realm=%s id=%s period=%s zonegroup=%s id=%s "
        "master_zone=%s zones=%s local_zone=%s id=%s",
        cluster_name,
        realm.get("name"),
        realm.get("id"),
        period.get("id"),
        zonegroup.get("name"),
        zonegroup.get("id"),
        zonegroup.get("master_zone"),
        zone_names,
        zone.get("name"),
        zone.get("id"),
    )
    if observed.get("daemon_config") is not None:
        log.info(
            "RGW daemon config on %s: %s", cluster_name, observed.get("daemon_config")
        )
    log.info("sync status on %s:\n%s", cluster_name, observed.get("sync_status"))
    log.info("ceph status on %s:\n%s", cluster_name, observed.get("health"))


def _discovery_options(config):
    scheme = config.get("endpoint_scheme")
    return {
        "min_zones": int(config.get("min_zones", 2)),
        "verify_sync": as_bool(config.get("verify_sync", True)),
        "check_cluster_health": as_bool(config.get("check_cluster_health", True)),
        "endpoint_scheme": None if scheme in (None, "") else str(scheme).lower(),
        "require_system_key": as_bool(config.get("require_system_key", True)),
    }


def _peer_maps(test_data, kwargs):
    """Return the peer snapshots for this test only.

    A later test, such as failover or adding an archive zone, must not compare
    against the period recorded by an earlier verification.
    """
    run_config = kwargs.get("run_config") or {}
    test_name = run_config.get("test_name") or "default"
    store = test_data.setdefault(SNAPSHOT_KEY, {})
    if store and "realm_id" in next(iter(store.values()), {}):
        store = {"default": store}
        test_data[SNAPSHOT_KEY] = store
    return store.setdefault(test_name, {})


def validate_discovered(observed, options):
    """Validate a live multisite layout.

    Realm, zone, endpoint, and tier details come from the cluster. HTTP, HTTPS,
    HAProxy ports, archive zones, and extra zones on one cluster are all
    accepted when the period, endpoints, daemon config, and sync sources agree.
    """
    errors = []
    realms = observed.get("realms") or []
    if not realms:
        _fail(errors, "no realms are configured", retryable=True)
        return errors

    _validate_health(errors, observed.get("health") or "", options)
    daemon_config = observed.get("daemon_config") or {}
    if not daemon_config:
        _fail(errors, "no RGW daemons are configured", retryable=True)

    served = {}
    for daemon_id, config in daemon_config.items():
        status = (config.get("status") or "").lower()
        if status != "running":
            _fail(
                errors,
                f"RGW daemon {daemon_id} status is {status or 'unknown'}",
                retryable=True,
            )
            continue
        realm_name = (config.get("rgw_realm") or "").strip()
        zonegroup_name = (config.get("rgw_zonegroup") or "").strip()
        zone_name = (config.get("rgw_zone") or "").strip()
        if not realm_name or not zonegroup_name or not zone_name:
            _fail(
                errors,
                f"client.rgw.{daemon_id} is missing rgw_realm, rgw_zonegroup, or rgw_zone "
                f"({realm_name!r}, {zonegroup_name!r}, {zone_name!r})",
            )
            continue
        served.setdefault((realm_name, zonegroup_name, zone_name), []).append(daemon_id)

    for realm in realms:
        _validate_discovered_realm(errors, realm, served, options)
    return errors


def _validate_discovered_realm(errors, realm, served, options):
    realm_doc = realm.get("realm") or {}
    realm_name = realm.get("name") or realm_doc.get("name")
    period = realm.get("period") or {}
    if realm_doc.get("name") != realm_name:
        _fail(errors, f"realm name is {realm_doc.get('name')}, expected {realm_name}")
    if not realm_doc.get("current_period"):
        _fail(errors, f"realm {realm_name} has no committed period")
    elif period.get("id") != realm_doc.get("current_period"):
        _fail(
            errors,
            f"period id {period.get('id')} != realm {realm_name} current_period "
            f"{realm_doc.get('current_period')}",
        )
    if period.get("realm_id") and period.get("realm_id") != realm_doc.get("id"):
        _fail(
            errors,
            f"period realm id {period.get('realm_id')} != realm id {realm_doc.get('id')}",
        )

    period_groups = {
        group.get("name"): group
        for group in (period.get("period_map") or {}).get("zonegroups") or []
        if group.get("name")
    }
    live_groups = realm.get("zonegroups") or []
    if not live_groups:
        _fail(errors, f"realm {realm_name} has no zonegroup")
        return
    if not any(key[0] == realm_name for key in served):
        _fail(
            errors,
            f"realm {realm_name} has no running RGW daemon",
            retryable=True,
        )

    for item in live_groups:
        live = item.get("live") or {}
        group_name = live.get("name")
        period_group = period_groups.get(group_name)
        if period_group is None:
            _fail(errors, f"period map does not include zonegroup {group_name}")
            continue
        _validate_discovered_zonegroup(
            errors, realm, live, period_group, period, served, options
        )


def _validate_discovered_zonegroup(
    errors, realm, live, period_group, period, served, options
):
    realm_name = realm.get("name")
    realm_doc = realm.get("realm") or {}
    group_name = live.get("name")
    zones = _zones_by_name(live)
    period_zones = _zones_by_name(period_group)
    if len(zones) < options["min_zones"]:
        _fail(
            errors,
            f"zonegroup {group_name} has zones {sorted(zones)}, "
            f"multisite requires at least {options['min_zones']}",
        )
    if live.get("realm_id") and live.get("realm_id") != realm_doc.get("id"):
        _fail(
            errors,
            f"zonegroup {group_name} realm id {live.get('realm_id')} != "
            f"realm id {realm_doc.get('id')}",
        )
    master_id = live.get("master_zone")
    master_names = [
        name for name, entry in zones.items() if entry.get("id") == master_id
    ]
    if len(master_names) != 1:
        _fail(
            errors,
            f"zonegroup {group_name} master zone id {master_id} matches {master_names}",
        )
    is_master_group = live.get("id") == period.get("master_zonegroup")
    if as_bool(live.get("is_master")) != is_master_group:
        _fail(
            errors,
            f"zonegroup {group_name} is_master is {live.get('is_master')}, "
            f"period master zonegroup is {period.get('master_zonegroup')}",
        )
    if period.get("master_zone") != master_id:
        _fail(
            errors,
            f"period master zone {period.get('master_zone')} != "
            f"zonegroup master zone {master_id}",
        )
    if sorted(zones) != sorted(period_zones):
        _fail(
            errors,
            f"zonegroup {group_name} zones {sorted(zones)} != "
            f"period zones {sorted(period_zones)}",
        )

    for zone_name, entry in zones.items():
        invalid = _invalid_endpoints(entry.get("endpoints"))
        if invalid == ["<none>"]:
            _fail(errors, f"zone {zone_name} has no endpoints")
        elif invalid:
            _fail(errors, f"zone {zone_name} has invalid endpoints {invalid}")
        elif options["endpoint_scheme"]:
            wrong = [
                endpoint
                for endpoint in iter_endpoints(entry.get("endpoints"))
                if not endpoint.lower().startswith(options["endpoint_scheme"] + "://")
            ]
            if wrong:
                _fail(
                    errors,
                    f"zone {zone_name} endpoints {wrong} are not "
                    f"{options['endpoint_scheme']}",
                )
        period_endpoints = _endpoint_set(
            (period_zones.get(zone_name) or {}).get("endpoints")
        )
        live_endpoints = _endpoint_set(entry.get("endpoints"))
        if period_zones.get(zone_name) and live_endpoints != period_endpoints:
            _fail(
                errors,
                f"zone {zone_name} endpoints {sorted(live_endpoints)} != "
                f"committed period endpoints {sorted(period_endpoints)}",
            )
        if _is_archive(entry) and entry.get("id") == master_id:
            _fail(errors, f"archive zone {zone_name} is the master zone")
        if not _sync_from_all(entry) and not (entry.get("sync_from") or []):
            _fail(
                errors,
                f"zone {zone_name} sync_from_all is false and sync_from is empty",
            )
        if (
            period_zones.get(zone_name)
            and period_zones[zone_name].get("id")
            and period_zones[zone_name].get("id") != entry.get("id")
        ):
            _fail(
                errors,
                f"period zone id {period_zones[zone_name].get('id')} != "
                f"zone {zone_name} id {entry.get('id')}",
            )

    local_zones = realm.get("local_zones") or {}
    for zone_name, local in local_zones.items():
        if zone_name not in zones:
            continue
        if (realm_name, group_name, zone_name) not in served:
            _fail(
                errors,
                f"no running RGW daemon serves {realm_name}/{group_name}/{zone_name}",
                retryable=True,
            )
        _validate_local_zone(
            errors, realm_name, group_name, zone_name, local, zones, master_id, options
        )

    served_here = [
        zone_name
        for served_realm, served_group, zone_name in served
        if served_realm == realm_name and served_group == group_name
    ]
    for zone_name in served_here:
        if zone_name not in zones:
            _fail(
                errors,
                f"RGW daemon zone {zone_name} is not in zonegroup {group_name}",
            )
        elif zone_name not in local_zones:
            _fail(
                errors,
                f"zone {zone_name} is served by an RGW daemon but was not read",
                retryable=True,
            )


def _validate_local_zone(
    errors, realm_name, group_name, zone_name, local, zones, master_id, options
):
    zone = local.get("zone") or {}
    entry = zones.get(zone_name) or {}
    if zone.get("name") not in (None, zone_name):
        _fail(errors, f"zone name is {zone.get('name')}, expected {zone_name}")
    if zone.get("id") and entry.get("id") and zone.get("id") != entry.get("id"):
        _fail(
            errors,
            f"zone {zone_name} id {zone.get('id')} != zonegroup copy {entry.get('id')}",
        )
    if options["require_system_key"]:
        access_key = (zone.get("system_key") or {}).get("access_key")
        if not access_key:
            _fail(errors, f"zone {zone_name} has no system access key")
    if zone.get("endpoints") is not None:
        zone_endpoints = _endpoint_set(zone.get("endpoints"))
        group_endpoints = _endpoint_set(entry.get("endpoints"))
        if zone_endpoints and group_endpoints and zone_endpoints != group_endpoints:
            _fail(
                errors,
                f"zone get endpoints {sorted(zone_endpoints)} != "
                f"zonegroup endpoints {sorted(group_endpoints)}",
            )

    if not options["verify_sync"]:
        return
    sync_status = local.get("sync_status") or ""
    problems = sync_status_problems(sync_status)
    if problems:
        _fail(errors, f"sync status for {zone_name} reports {problems}", retryable=True)
    if not sync_status.strip():
        _fail(errors, f"sync status for {zone_name} is empty", retryable=True)
        return
    reported = sync_status_names(sync_status)
    expected_names = {
        "realm": realm_name,
        "zonegroup": group_name,
        "zone": zone_name,
    }
    for label, expected_name in expected_names.items():
        if reported.get(label) != expected_name:
            _fail(
                errors,
                f"sync status {label} is {reported.get(label)}, expected {expected_name}",
                retryable=True,
            )
    lowered = sync_status.lower()
    is_master = bool(entry.get("id")) and entry.get("id") == master_id
    if "metadata sync" not in lowered:
        _fail(
            errors,
            f"sync status for {zone_name} has no metadata sync section",
            retryable=True,
        )
    elif is_master and "zone is master" not in lowered:
        _fail(
            errors,
            f"master zone {zone_name} metadata sync does not report zone is master",
            retryable=True,
        )
    elif not is_master and "zone is master" in lowered:
        _fail(
            errors,
            f"zone {zone_name} reports metadata sync as master",
            retryable=True,
        )
    for peer in _sync_peer_names(entry, zones):
        if peer != zone_name and f"({peer})" not in sync_status:
            _fail(
                errors,
                f"sync status for {zone_name} does not list peer zone ({peer})",
                retryable=True,
            )


def discovered_site_snapshot(cluster_name, observed):
    """Return realm and zonegroup ids for comparison with peer clusters."""
    realms = {}
    for realm in observed.get("realms") or []:
        period = realm.get("period") or {}
        zonegroups = {}
        for item in realm.get("zonegroups") or []:
            live = item.get("live") or {}
            if not live.get("name"):
                continue
            zonegroups[live["name"]] = {
                "id": live.get("id"),
                "period_id": period.get("id"),
                "master_zone_id": live.get("master_zone"),
                "zones": sorted(
                    zone.get("name")
                    for zone in live.get("zones") or []
                    if zone.get("name")
                ),
            }
        realms[realm.get("name")] = {
            "realm_id": (realm.get("realm") or {}).get("id"),
            "zonegroups": zonegroups,
        }
    return {"cluster": cluster_name, "realms": realms}


def compare_discovered(snapshot, peers):
    """Return mismatches between discovered sites that share a realm."""
    errors = []
    for other_name, other in peers.items():
        if other_name == snapshot.get("cluster"):
            continue
        for realm_name, realm in (snapshot.get("realms") or {}).items():
            other_realm = (other.get("realms") or {}).get(realm_name)
            if not other_realm:
                continue
            if realm.get("realm_id") != other_realm.get("realm_id"):
                _fail(
                    errors,
                    f"realm id for {realm_name} on {snapshot.get('cluster')} is "
                    f"{realm.get('realm_id')}, on {other_name} is "
                    f"{other_realm.get('realm_id')}",
                    retryable=True,
                )
            for group_name, group in (realm.get("zonegroups") or {}).items():
                other_group = (other_realm.get("zonegroups") or {}).get(group_name)
                if not other_group:
                    continue
                for field, label in (
                    ("id", "zonegroup id"),
                    ("period_id", "period id"),
                    ("master_zone_id", "master zone id"),
                ):
                    if group.get(field) != other_group.get(field):
                        _fail(
                            errors,
                            f"{label} for {realm_name}/{group_name} on "
                            f"{snapshot.get('cluster')} is {group.get(field)}, "
                            f"on {other_name} is {other_group.get(field)}",
                            retryable=True,
                        )
                if group.get("zones") != other_group.get("zones"):
                    _fail(
                        errors,
                        f"zone list for {realm_name}/{group_name} on "
                        f"{snapshot.get('cluster')} is {group.get('zones')}, "
                        f"on {other_name} is {other_group.get('zones')}",
                        retryable=True,
                    )
    return errors


def _cephadm_unavailable(exc):
    text = str(exc).lower()
    return "cephadm" in text and (
        "not found" in text or "no such file" in text or "command not found" in text
    )


def _execution_node(ceph_cluster, use_cephadm):
    roles = ("installer",) if use_cephadm else ("client", "installer", "rgw")
    for role in roles:
        nodes = ceph_cluster.get_nodes(role=role)
        if nodes:
            return nodes[0]
    return None


def collect_discovered(node, use_cephadm):
    """Read every realm, local zone, daemon, and sync status on one cluster."""
    realm_list = parse_json_output(_exec(node, "radosgw-admin realm list", use_cephadm))
    realm_names = list(realm_list.get("realms") or [])
    daemons = parse_json_output(
        _exec(node, "ceph orch ps --daemon_type rgw --format json", use_cephadm)
    )
    if isinstance(daemons, dict):
        daemons = daemons.get("daemons") or daemons.get("items") or []
    daemon_config = {}
    for daemon in daemons:
        if daemon.get("daemon_type") not in (None, "rgw"):
            continue
        daemon_id = daemon.get("daemon_id")
        if not daemon_id:
            continue
        status = daemon.get("status_desc") or daemon.get("status") or ""
        config = {"status": status}
        if str(status).lower() == "running":
            config.update(
                {
                    "rgw_realm": _config_get(node, daemon_id, "rgw_realm", use_cephadm),
                    "rgw_zonegroup": _config_get(
                        node, daemon_id, "rgw_zonegroup", use_cephadm
                    ),
                    "rgw_zone": _config_get(node, daemon_id, "rgw_zone", use_cephadm),
                }
            )
        daemon_config[daemon_id] = config

    realms = []
    for realm_name in realm_names:
        realm_doc = parse_json_output(
            _exec(
                node, f"radosgw-admin realm get --rgw-realm {realm_name}", use_cephadm
            )
        )
        period = parse_json_output(
            _exec(
                node,
                f"radosgw-admin period get --rgw-realm {realm_name}",
                use_cephadm,
            )
        )
        zonegroups = []
        seen = set()
        for group in (period.get("period_map") or {}).get("zonegroups") or []:
            group_name = group.get("name")
            if not group_name or group_name in seen:
                continue
            seen.add(group_name)
            live = parse_json_output(
                _exec(
                    node,
                    "radosgw-admin zonegroup get "
                    f"--rgw-realm {realm_name} --rgw-zonegroup {group_name}",
                    use_cephadm,
                )
            )
            zonegroups.append({"live": live})
        local_names = sorted(
            {
                (config.get("rgw_zone") or "").strip()
                for config in daemon_config.values()
                if (config.get("rgw_realm") or "").strip() == realm_name
                and str(config.get("status") or "").lower() == "running"
                and (config.get("rgw_zone") or "").strip()
            }
        )
        local_zones = {}
        for zone_name in local_names:
            group_name = next(
                (
                    (config.get("rgw_zonegroup") or "").strip()
                    for config in daemon_config.values()
                    if (config.get("rgw_zone") or "").strip() == zone_name
                    and (config.get("rgw_realm") or "").strip() == realm_name
                ),
                "",
            )
            zone_doc = parse_json_output(
                _exec(
                    node,
                    "radosgw-admin zone get "
                    f"--rgw-realm {realm_name} --rgw-zone {zone_name}",
                    use_cephadm,
                )
            )
            sync_command = f"radosgw-admin sync status --rgw-realm {realm_name}"
            if group_name:
                sync_command += f" --rgw-zonegroup {group_name}"
            sync_command += f" --rgw-zone {zone_name}"
            local_zones[zone_name] = {
                "zone": zone_doc,
                "sync_status": _exec(node, sync_command, use_cephadm),
            }
        realms.append(
            {
                "name": realm_name,
                "realm": realm_doc,
                "period": period,
                "zonegroups": zonegroups,
                "local_zones": local_zones,
            }
        )
    return {
        "health": _exec(node, "ceph -s", use_cephadm),
        "daemons": daemons,
        "daemon_config": daemon_config,
        "realms": realms,
    }


def _log_discovered(cluster_name, observed):
    for realm in observed.get("realms") or []:
        for item in realm.get("zonegroups") or []:
            live = item.get("live") or {}
            summary = []
            for zone in live.get("zones") or []:
                endpoints = list(iter_endpoints(zone.get("endpoints")))
                tier = zone.get("tier_type") or "data"
                summary.append(f"{zone.get('name')}({tier}:{','.join(endpoints)})")
            log.info(
                "Multisite read on %s: realm=%s period=%s zonegroup=%s zones=%s",
                cluster_name,
                realm.get("name"),
                (realm.get("period") or {}).get("id"),
                live.get("name"),
                summary,
            )
        for zone_name, local in (realm.get("local_zones") or {}).items():
            log.info(
                "sync status on %s zone %s:\n%s",
                cluster_name,
                zone_name,
                local.get("sync_status"),
            )
    if observed.get("daemon_config") is not None:
        log.info(
            "RGW daemon config on %s: %s", cluster_name, observed.get("daemon_config")
        )
    log.info("ceph status on %s:\n%s", cluster_name, observed.get("health"))


def _sync_poll(config, cluster_name, kwargs):
    """Return the start time, timeout, and delay for this cluster.

    The first cluster in the test is the primary. It retries sync status for
    up to 120 seconds. Secondary, tertiary, and archive clusters are checked
    once because that wait already happened on the primary.
    """
    timeout = max(0, int(config.get("sync_timeout", SYNC_TIMEOUT)))
    delay = max(1, int(config.get("delay", 20)))
    test_data = kwargs.get("test_data")
    run_config = kwargs.get("run_config") or {}
    test_name = run_config.get("test_name") or "default"
    if isinstance(test_data, dict):
        waited = test_data.setdefault(SYNC_WAIT_KEY, {})
        primary = waited.get(test_name)
        if primary:
            log.info(
                "%s is checked once; sync status already waited on %s",
                cluster_name,
                primary,
            )
            timeout = 0
        else:
            waited[test_name] = cluster_name
            log.info(
                "%s is checked first and retries sync status for up to %s seconds",
                cluster_name,
                timeout,
            )
    return time.time(), timeout, delay


def _retry_sync(started, timeout, delay, errors):
    """Sleep while sync status can still become ready inside the timeout.

    A wrong realm, zone, endpoint, or period stops the poll. Sync status,
    daemon startup, and cluster health keep being checked until the timeout.
    """
    if not errors or not all(error.retryable for error in errors):
        return False
    remaining = timeout - (time.time() - started)
    if remaining <= 0:
        return False
    log.warning(
        "Sync status is not ready, %.0f seconds remain in the %s second timeout",
        remaining,
        timeout,
    )
    for error in errors:
        log.warning("%s", error)
    time.sleep(min(delay, remaining))
    return True


def _run_discovered(ceph_cluster, config, kwargs):
    options = _discovery_options(config)
    use_cephadm = as_bool(config.get("cephadm", True))
    allow_cephadm_fallback = "cephadm" not in config
    node = _execution_node(ceph_cluster, use_cephadm)
    if node is None:
        log.log_error(
            f"no installer or client node available on {getattr(ceph_cluster, 'name', '')}"
        )
        return 1
    test_data = kwargs.get("test_data")
    if test_data is None:
        test_data = {}
    cluster_name = getattr(ceph_cluster, "name", "")
    started, timeout, delay = _sync_poll(config, cluster_name, kwargs)

    while True:
        try:
            observed = collect_discovered(node, use_cephadm)
        except Exception as exc:
            if use_cephadm and allow_cephadm_fallback and _cephadm_unavailable(exc):
                log.warning("cephadm is unavailable, reading multisite config directly")
                use_cephadm = False
                allow_cephadm_fallback = False
                node = _execution_node(ceph_cluster, False) or node
                continue
            errors = [
                CheckFailure(
                    f"failed to read multisite configuration: {exc}", retryable=True
                )
            ]
        else:
            _log_discovered(cluster_name, observed)
            errors = validate_discovered(observed, options)
            if not errors:
                snapshot = discovered_site_snapshot(cluster_name, observed)
                peers = _peer_maps(test_data, kwargs)
                errors = compare_discovered(snapshot, peers)
                if not errors:
                    peers[cluster_name] = snapshot
                    log.info("Multisite configuration verified on %s", cluster_name)
                    return 0

        if _retry_sync(started, timeout, delay, errors):
            continue
        for error in errors:
            log.log_error(str(error))
        return 1


def run(ceph_cluster, **kwargs):
    """Verify multisite configuration on one cluster.

    Returns:
        0 when the site matches the expected layout.
        1 when required configuration is missing or still wrong after retries.
    """
    config = dict(kwargs.get("config") or {})
    if not (config.get("realm") and config.get("zonegroup") and config.get("zone")):
        return _run_discovered(ceph_cluster, config, kwargs)
    try:
        expected = resolve_expected(config)
        expected["endpoints"] = _translate_endpoints(
            expected["endpoints"],
            kwargs.get("ceph_cluster_dict"),
            getattr(ceph_cluster, "name", None),
        )
    except ValueError as exc:
        log.log_error(str(exc))
        return 1

    nodes = ceph_cluster.get_nodes(role="installer")
    if not nodes:
        log.log_error(
            f"no installer node available on {getattr(ceph_cluster, 'name', '')}"
        )
        return 1
    node = nodes[0]
    use_cephadm = as_bool(config.get("cephadm", True))
    test_data = kwargs.get("test_data")
    if test_data is None:
        test_data = {}
    cluster_name = getattr(ceph_cluster, "name", "")
    started, timeout, delay = _sync_poll(config, cluster_name, kwargs)

    while True:
        try:
            observed = collect_multisite(node, expected, use_cephadm)
        except Exception as exc:
            errors = [
                CheckFailure(
                    f"failed to read multisite configuration: {exc}", retryable=True
                )
            ]
        else:
            _log_observed(cluster_name, observed)
            errors = validate_multisite(observed, expected)
            if not errors:
                snapshot = site_snapshot(cluster_name, observed, expected)
                peers = _peer_maps(test_data, kwargs)
                errors = compare_sites(snapshot, peers)
                if not errors:
                    peers[cluster_name] = snapshot
                    log.info(
                        "Multisite configuration verified on %s: realm=%s zonegroup=%s "
                        "zone=%s period=%s",
                        cluster_name,
                        expected["realm"],
                        expected["zonegroup"],
                        expected["zone"],
                        snapshot["period_id"],
                    )
                    return 0

        if _retry_sync(started, timeout, delay, errors):
            continue
        for error in errors:
            log.log_error(str(error))
        return 1
