"""Helpers for Regional File Mount / NFS Virtual Server multi-route tests."""

import ipaddress
import json
import re
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import get_ip_from_node

try:
    from nfs_operations import (
        GANESHA_SUBVOL_GROUP,
        check_nfs_daemons_removed,
        ensure_ganeshagroup,
        mount_cleanup_retry,
        nfs_log_parser,
        setup_custom_nfs_cluster_multi_export_client,
        setup_nfs_cluster,
    )
except ImportError:
    from tests.nfs.nfs_operations import (
        GANESHA_SUBVOL_GROUP,
        check_nfs_daemons_removed,
        ensure_ganeshagroup,
        mount_cleanup_retry,
        nfs_log_parser,
        setup_custom_nfs_cluster_multi_export_client,
        setup_nfs_cluster,
    )
from utility.log import Log

log = Log(__name__)

DEFAULT_IO_USER = "cephuser"
_INTERFACES_READY = set()
_MULTI_ROUTE_CONF = "conf/tentacle/nfs/virtual-server-multi-subnet.yaml"


def _is_non_route_ip(ip_str):
    """Return True for loopback, link-local, or otherwise unusable route addresses."""
    try:
        ip = ipaddress.ip_address(ip_str)
        return ip.is_loopback or ip.is_link_local or ip.is_unspecified
    except ValueError:
        return False


def io_sudo_from_config(config):
    """
    Whether IO commands use root (sudo=True).

    Matches ``tier1-nfs-ganesha-v4-2.yaml``: ``sudo: false`` runs file IO as the
    client SSH user after ``chown cephuser`` on the mount (see ``nfs_operations``).

    Args:
        config: Configuration dictionary

    Returns:
        bool: True if commands should run with sudo

    Raises:
        TypeError: If config is not a dictionary or None
    """
    if config is not None and not isinstance(config, dict):
        log.error(f"Invalid config type: {type(config)}, expected dict or None")
        raise TypeError(f"config must be dict or None, got {type(config)}")

    use_sudo = (config or {}).get("sudo", True)
    log.debug(f"IO commands will {'use' if use_sudo else 'not use'} sudo")
    return use_sudo


def prepare_mount_for_nonroot_io(client, mount_path, io_user=DEFAULT_IO_USER):
    """Transfer mount ownership so non-root IO can write (same as setup_nfs_cluster)."""
    client.exec_command(
        sudo=True,
        cmd=f"chown -R {io_user}:{io_user} {mount_path}",
    )
    log.info(
        "Transferred ownership of %s to %s on %s for non-root IO",
        mount_path,
        io_user,
        client.hostname,
    )


def _multi_route_requirements_hint():
    return (
        "Virtual Server tests need dual-homed NFS and client VMs (2+ IPv4 addresses "
        f"per node on matching subnets). Use --global-conf {_MULTI_ROUTE_CONF} and "
        "attach at least two OpenStack provider networks, e.g. "
        "--custom-config openstack_networks=provider_net_cci_16,provider_net_cci_15. "
        "Verify on each node: ip -o -4 addr show"
    )


def ensure_host_network_interfaces_ready(node):
    """
    Bring up non-loopback NICs and request DHCP on interfaces without IPv4.

    OpenStack multi-network VMs often attach secondary ports that cloud-init does
    not configure; route discovery needs an address on every attached subnet.
    """
    hostname = getattr(node, "hostname", None)
    if hostname in _INTERFACES_READY:
        return

    cmd = r"""
for dev in $(ip -o link show | awk -F': ' '{print $2}' | sed 's/@.*//' | grep -Ev '^lo$'); do
  ip link set dev "$dev" up 2>/dev/null || true
done
sleep 1
for dev in $(ip -o link show | awk -F': ' '{print $2}' | sed 's/@.*//' | grep -Ev '^lo$'); do
  if ! ip -o -4 addr show dev "$dev" 2>/dev/null | grep -q ' inet '; then
    if command -v nmcli >/dev/null 2>&1; then
      nmcli device connect "$dev" 2>/dev/null || true
    fi
    if command -v dhclient >/dev/null 2>&1; then
      timeout 20 dhclient -1 "$dev" 2>/dev/null || true
    fi
  fi
done
sleep 2
"""
    node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if hostname:
        _INTERFACES_READY.add(hostname)
    log.info("Ensured network interfaces are up on %s", hostname or node)


def _openstack_ipv4_addresses(node):
    """IPv4 addresses Neutron assigned (diagnostics when host has fewer addrs)."""
    vm = getattr(node, "vm_node", None)
    os_node = getattr(vm, "node", None) if vm is not None else None
    if os_node is None:
        return []

    ips = []
    addresses = (getattr(os_node, "extra", None) or {}).get("addresses") or {}
    for addr_list in addresses.values():
        for entry in addr_list:
            if not isinstance(entry, dict):
                continue
            if entry.get("OS-EXT-IPS:type") == "floating":
                continue
            ip = entry.get("addr", "")
            if ":" in ip or _is_non_route_ip(ip) or ip.endswith(".255"):
                continue
            ips.append(ip)

    for ip in getattr(os_node, "private_ips", None) or []:
        if ":" not in ip and not _is_non_route_ip(ip) and not ip.endswith(".255"):
            ips.append(ip)

    return list(dict.fromkeys(ips))


def _host_ipv4_addresses(node):
    """
    Return IPv4 host addresses assigned to interfaces (not broadcast/peer).

    ``get_ip_from_node`` regex-matches every IPv4 in ``ip addr`` output,
    including ``brd 10.0.x.255`` broadcast addresses — that caused route
    discovery to pair 10.0.147.255 with itself and fail preflight ping.
    """
    out, _ = node.exec_command(
        sudo=True,
        cmd="ip -o -4 addr show",
        check_ec=False,
    )
    text = out if isinstance(out, str) else (out[0] if out else "")
    ips = []
    for line in text.splitlines():
        parts = line.split()
        for idx, token in enumerate(parts):
            if token != "inet" or idx + 1 >= len(parts):
                continue
            ip = parts[idx + 1].split("/")[0]
            if _is_non_route_ip(ip):
                continue
            if ip.endswith(".255"):
                continue
            ips.append(ip)
            break

    if not ips:
        # Fallback: parse only ``inet`` lines from full ``ip addr``
        out, _ = node.exec_command(sudo=True, cmd="ip addr", check_ec=False)
        text = out if isinstance(out, str) else (out[0] if out else "")
        for line in text.splitlines():
            line = line.strip()
            if "inet " not in line or "inet6" in line:
                continue
            match = re.search(r"inet (\d+\.\d+\.\d+\.\d+)/", line)
            if not match:
                continue
            ip = match.group(1)
            if _is_non_route_ip(ip) or ip.endswith(".255"):
                continue
            ips.append(ip)

    if not ips:
        out, _ = node.exec_command(sudo=True, cmd="hostname -I", check_ec=False)
        text = out if isinstance(out, str) else (out[0] if out else "")
        for ip in text.split():
            if not _is_non_route_ip(ip) and not ip.endswith(".255"):
                ips.append(ip)

    if not ips and getattr(node, "ip_address", None):
        ips = [node.ip_address]

    # Last-resort fallback (may include broadcast — filtered again below)
    if not ips:
        ips = [
            ip
            for ip in get_ip_from_node(node)
            if not _is_non_route_ip(ip) and not ip.endswith(".255")
        ]

    deduped = list(dict.fromkeys(ips))
    neutron_ips = _openstack_ipv4_addresses(node)
    if len(neutron_ips) > len(deduped):
        log.warning(
            "Host %s has %d interface IPv4 address(es) %s but Neutron reports %s; "
            "secondary NICs may be down or missing DHCP",
            node.hostname,
            len(deduped),
            deduped,
            neutron_ips,
        )
    log.info("Host IPv4 on %s: %s", node.hostname, deduped)
    return deduped


def _same_subnet(ip_a, ip_b):
    """Match /24-style subnets (sufficient for typical RHOS provider nets)."""
    return ".".join(ip_a.split(".")[:3]) == ".".join(ip_b.split(".")[:3])


def _route_candidate_pairs(nfs_ips, client_ips, pairing="same_subnet"):
    """
    Build server/client IP pairs to probe.

    Virtual Server routes use one client NIC per provider network; only pairs on
    the same subnet are valid (not every client_ip × server_ip combination).
    """
    candidates = []
    if pairing != "all":
        for server_ip in nfs_ips:
            for client_ip in client_ips:
                if server_ip != client_ip and _same_subnet(server_ip, client_ip):
                    candidates.append((server_ip, client_ip))
    if not candidates or pairing == "all":
        for server_ip in nfs_ips:
            for client_ip in client_ips:
                if server_ip == client_ip:
                    continue
                pair = (server_ip, client_ip)
                if pair not in candidates:
                    candidates.append(pair)
    return candidates


def _command_succeeded(client):
    """Return exit code from the last exec_command on this node."""
    return int(getattr(client, "exit_status", 1))


def _route_pair_key(route):
    return (route["server_ip"], route["client_ip"])


def _all_routes_same_client(routes):
    if not routes:
        return False
    first = routes[0].get("client")
    return all(r.get("client") is first for r in routes)


def format_route(route):
    """Human-readable route description for logs."""
    client = route.get("client")
    host = getattr(client, "hostname", "?") if client else "?"
    return (
        f"route_id={route.get('route_id')} "
        f"{host} client_nic={route['client_ip']} -> nfs={route['server_ip']}"
    )


def log_route_taken(route, phase, extra=""):
    """Log the network path used for mount, IO, or fencing."""
    msg = f"Taking route ({phase}): {format_route(route)}"
    if extra:
        msg = f"{msg} [{extra}]"
    log.info(msg)


def log_routes_summary(routes, header="Planned routes"):
    log.info("%s (%d):", header, len(routes))
    for route in routes:
        log_route_taken(route, "planned")


def ensure_distinct_routes(routes, min_routes=None):
    """
    Require unique (server_ip, client_ip) pairs.

    When every route uses the same client host, also require distinct client and
    server NIC addresses (one subnet path per interface).
    """
    seen = set()
    unique = []
    for route in routes:
        key = _route_pair_key(route)
        if key in seen:
            raise ConfigError(f"Duplicate route pair {key}: {format_route(route)}")
        seen.add(key)
        unique.append(route)

    for idx, route in enumerate(unique):
        route["route_id"] = idx

    if min_routes is not None and len(unique) < min_routes:
        raise ConfigError(
            f"Need at least {min_routes} distinct route(s), got {len(unique)}: "
            f"{[format_route(r) for r in unique]}"
        )

    if _all_routes_same_client(unique) and len(unique) > 1:
        client_ips = [r["client_ip"] for r in unique]
        server_ips = [r["server_ip"] for r in unique]
        if len(set(client_ips)) < len(unique):
            raise ConfigError(
                "Each route on the same client must use a different client NIC IP; "
                f"got client IPs {client_ips}"
            )
        if len(set(server_ips)) < len(unique):
            raise ConfigError(
                "Each route must use a different NFS server NIC IP; "
                f"got server IPs {server_ips}"
            )

    log_routes_summary(unique, header="Distinct routes selected")
    return unique


def _log_network_diagnostics(node):
    """Log interface state to help debug single-homed VMs."""
    out, _ = node.exec_command(
        sudo=True,
        cmd="ip -o link show | awk -F': ' '{print $2, $9}'",
        check_ec=False,
    )
    text = out if isinstance(out, str) else (out[0] if out else "")
    log.info("Interfaces on %s: %s", node.hostname, text.strip() or "(none)")
    out, _ = node.exec_command(
        sudo=True,
        cmd="ip -o -4 addr show | awk '{print $2, $4}'",
        check_ec=False,
    )
    text = out if isinstance(out, str) else (out[0] if out else "")
    log.info("IPv4 assignments on %s:\n%s", node.hostname, text.strip() or "(none)")


def _probe_route(
    client,
    client_ip,
    server_ip,
    port=2049,
    ping_only=False,
    require_client_binding=False,
):
    """
      Return True when the client can reach the NFS server on this path.

      Uses ICMP only for discovery (NFS is not running yet). After cluster setup,
    use ``preflight_route_connectivity`` if needed.

      When ``require_client_binding`` is set, only a successful ``ping -I <client_ip>``
      counts. Default-route ping is ignored so a pair is not attributed to a NIC that
      did not actually carry the traffic.

      Note: ``exec_command(..., check_ec=False)`` returns ``(stdout, stderr)``, not
      the exit code — use ``node.exit_status`` from the Ceph node object.
    """
    if server_ip == client_ip:
        return False

    ping_cmds = [
        (f"ping -c 2 -W 3 -I {client_ip} {server_ip}", f"ping -I {client_ip}"),
    ]
    if not require_client_binding:
        ping_cmds.append((f"ping -c 2 -W 3 {server_ip}", "ping (default route)"))
    for cmd, label in ping_cmds:
        client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        if _command_succeeded(client) == 0:
            log.info(
                "Route probe OK (%s): %s (%s) -> %s",
                label,
                client.hostname,
                client_ip,
                server_ip,
            )
            return True

    if not ping_only:
        for cmd, label in (
            (f"nc -z -w 3 {server_ip} {port} 2>/dev/null", f"tcp/{port}"),
            (
                f"timeout 3 bash -c 'cat < /dev/null > /dev/tcp/{server_ip}/{port}'",
                f"bash /dev/tcp/{server_ip}/{port}",
            ),
        ):
            client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            if _command_succeeded(client) == 0:
                log.info(
                    "Route probe OK (%s): %s (%s) -> %s",
                    label,
                    client.hostname,
                    client_ip,
                    server_ip,
                )
                return True

    log.info(
        "Route probe FAIL: %s (%s) -> %s",
        client.hostname,
        client_ip,
        server_ip,
    )
    return False


def _validate_address_count(nfs_node, nfs_ips, client_list, min_routes, config):
    """Fail early when there are not enough same-subnet route candidates."""
    pairing = config.get("route_pairing", "same_subnet")
    for client in client_list:
        client_ips = _host_ipv4_addresses(client)
        candidates = _route_candidate_pairs(nfs_ips, client_ips, pairing)
        if len(candidates) < min_routes:
            raise ConfigError(
                f"Only {len(candidates)} same-subnet NFS/client IP pair(s) for "
                f"{client.hostname} (need {min_routes}). NFS IPs: {nfs_ips}, "
                f"client IPs: {client_ips}. Each route needs client and server on "
                f"the same network (e.g. 10.0.154.x with 10.0.154.x). "
                f"{_multi_route_requirements_hint()}"
            )


def _find_ping_routes_for_client(
    nfs_ips, client, config, exclude_pairs=None, one_per_client_nic=False
):
    """Reachable same-subnet pairs from *client* (ping only — NFS not up yet)."""
    exclude_pairs = exclude_pairs or set()
    client_ips = _host_ipv4_addresses(client)
    pairing = config.get("route_pairing", "same_subnet")
    routes = []
    seen = set(exclude_pairs)
    used_client_nics = set()
    for server_ip, client_ip in _route_candidate_pairs(nfs_ips, client_ips, pairing):
        if one_per_client_nic and client_ip in used_client_nics:
            continue
        key = (server_ip, client_ip)
        if key in seen:
            continue
        if _probe_route(
            client,
            client_ip,
            server_ip,
            ping_only=True,
            require_client_binding=True,
        ):
            seen.add(key)
            routes.append(
                {
                    "server_ip": server_ip,
                    "client_ip": client_ip,
                    "route_id": len(routes),
                    "client": client,
                }
            )
            if one_per_client_nic:
                used_client_nics.add(client_ip)
    return routes


def _find_first_ping_route(nfs_ips, client, config, exclude_pairs=None):
    """First reachable route for *client* not already used by another client."""
    exclude_pairs = exclude_pairs or set()
    client_ips = _host_ipv4_addresses(client)
    pairing = config.get("route_pairing", "same_subnet")
    for server_ip, client_ip in _route_candidate_pairs(nfs_ips, client_ips, pairing):
        key = (server_ip, client_ip)
        if key in exclude_pairs:
            continue
        if _probe_route(
            client,
            client_ip,
            server_ip,
            ping_only=True,
            require_client_binding=True,
        ):
            return {
                "server_ip": server_ip,
                "client_ip": client_ip,
                "route_id": 0,
                "client": client,
            }
    return None


def discover_and_verify_routes(nfs_node, clients, config):
    """Discover route pairs and verify connectivity with explicit error logging."""
    try:
        log.info("Discovering route pairs between NFS server and clients")
        routes = discover_route_pairs(nfs_node, clients, config)
        log.info("Discovered %d route(s)", len(routes))
    except Exception as exc:
        log.error("Route discovery failed: %s", exc)
        log.error("Check network configuration and client connectivity")
        raise

    try:
        log.info("Verifying route connectivity")
        preflight_route_connectivity(routes, config)
    except Exception as exc:
        log.error("Route connectivity check failed: %s", exc)
        raise

    return routes


def preflight_multi_route_environment(ceph_cluster, config):
    """
    Validate dual-homed NFS/client connectivity before scenario tests run.

    Raises ConfigError when the cluster conf or OpenStack networks cannot
    support ``min_routes`` distinct same-subnet paths.
    """
    min_routes = int(config.get("min_routes", 2))
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if not nfs_nodes:
        raise ConfigError("No NFS role node found in cluster configuration")
    nfs_node = nfs_nodes[0]
    clients = get_clients(ceph_cluster, config)

    for node in [nfs_node] + clients:
        ensure_host_network_interfaces_ready(node)

    nfs_ips = _host_ipv4_addresses(nfs_node)
    if len(nfs_ips) < min_routes:
        neutron_ips = _openstack_ipv4_addresses(nfs_node)
        raise ConfigError(
            f"NFS node {nfs_node.hostname} has {len(nfs_ips)} usable IPv4 "
            f"address(es) {nfs_ips} (need {min_routes}). "
            f"Neutron addresses: {neutron_ips or '(unavailable)'}. "
            f"{_multi_route_requirements_hint()}"
        )

    _validate_address_count(nfs_node, nfs_ips, clients, min_routes, config)
    log.info(
        "Virtual server preflight passed: NFS %s, %d client(s), min_routes=%d",
        nfs_ips,
        len(clients),
        min_routes,
    )


def discover_route_pairs(nfs_node, clients, config):
    """
    Build server/client IP pairs for each network route.

    Uses explicit ``routes`` from suite config when provided. Otherwise probes
    every NFS-server IP × client IP combination with ``ping -I <client_ip>``
    and keeps pairs that succeed (no /24 subnet guessing).
    """
    explicit = config.get("routes")
    min_routes = int(config.get("min_routes", 2))
    client_list = clients if isinstance(clients, list) else [clients]
    for node in [nfs_node] + client_list:
        ensure_host_network_interfaces_ready(node)

    if explicit:
        routes = []
        for idx, route in enumerate(explicit):
            routes.append(
                {
                    "server_ip": route["server_ip"],
                    "client_ip": route["client_ip"],
                    "route_id": route.get("route_id", idx),
                    "client": route.get("client")
                    or client_list[min(idx, len(client_list) - 1)],
                }
            )
        return ensure_distinct_routes(routes, min_routes=min_routes)

    nfs_ips = _host_ipv4_addresses(nfs_node)
    if not nfs_ips:
        raise ConfigError(f"No usable IPv4 addresses on NFS node {nfs_node.hostname}")

    if len(nfs_ips) < min_routes:
        _log_network_diagnostics(nfs_node)
        for client in client_list:
            _log_network_diagnostics(client)

    _validate_address_count(nfs_node, nfs_ips, client_list, min_routes, config)

    if len(client_list) == 1:
        client = client_list[0]
        routes = _find_ping_routes_for_client(
            nfs_ips, client, config, one_per_client_nic=True
        )
        if len(routes) < min_routes:
            candidates = _route_candidate_pairs(
                nfs_ips,
                _host_ipv4_addresses(client),
                config.get("route_pairing", "same_subnet"),
            )
            raise ConfigError(
                f"Discovered {len(routes)} reachable route(s) on {client.hostname}; "
                f"need at least {min_routes}. Same-subnet pairs to probe: {candidates}. "
                f"Set explicit config.routes if ping works manually but automation fails."
            )
        return ensure_distinct_routes(routes, min_routes=min_routes)

    routes = []
    used_pairs = set()
    for client in client_list:
        route = _find_first_ping_route(
            nfs_ips, client, config, exclude_pairs=used_pairs
        )
        if not route:
            raise ConfigError(
                f"No reachable route for client {client.hostname} (ping). "
                f"NFS IPs: {nfs_ips}, client IPs: {_host_ipv4_addresses(client)}"
            )
        used_pairs.add(_route_pair_key(route))
        routes.append(route)

    if len(routes) < min(len(client_list), min_routes):
        raise ConfigError(
            f"Discovered {len(routes)} reachable client route(s); need at least "
            f"{min(len(client_list), min_routes)}. NFS IPs: {nfs_ips}"
        )
    return ensure_distinct_routes(routes, min_routes=min(len(client_list), min_routes))


def validate_virtual_server_support(client_node):
    """Fail fast when the cluster build lacks --enable-virtual-server."""
    out, _ = client_node.exec_command(
        sudo=True,
        cmd="ceph nfs cluster create --help",
        check_ec=False,
    )
    help_text = out if isinstance(out, str) else str(out)
    if "enable-virtual-server" not in help_text:
        raise ConfigError(
            "ceph nfs cluster create does not support --enable-virtual-server; "
            "use a Ceph build with Regional File Mount / Virtual Server (ISCE-1982)."
        )


def preflight_route_connectivity(routes, config=None):
    """
    Re-verify each discovered route with ping (skipped when already ping-probed).

    Explicit ``config.routes`` always get validated here.
    """
    config = config or {}
    if config.get("routes"):
        for route in routes:
            client = route["client"]
            server_ip = route["server_ip"]
            client_ip = route["client_ip"]
            if server_ip == client_ip:
                raise ConfigError(
                    f"Invalid route: server_ip and client_ip are both {server_ip}"
                )
            if not _probe_route(client, client_ip, server_ip, ping_only=True):
                raise ConfigError(
                    f"Route preflight failed: {client.hostname} ({client_ip}) "
                    f"cannot reach NFS server {server_ip}"
                )
        return

    log.info(
        "Skipping duplicate preflight ping (%d route(s) already verified by discovery)",
        len(routes),
    )


def default_nfs_names():
    return {
        "fs_name": "cephfs",
        "nfs_name": "cephfs-nfs",
        "nfs_export": "/export",
        "nfs_mount": "/mnt/nfs",
        "fs": "cephfs",
    }


def setup_virtual_server_exports(
    ceph_cluster,
    clients,
    nfs_server,
    config,
    single_export=False,
    export_num=None,
):
    """Create virtual-server NFS cluster and exports without mounting."""
    names = default_nfs_names()
    port = str(config.get("port", "2049"))
    version = config.get("nfs_version", "4.2")
    validate_virtual_server_support(clients[0])
    ensure_ganeshagroup(clients[0], fs_name=names["fs_name"], ceph_cluster=ceph_cluster)

    if export_num:
        return setup_custom_nfs_cluster_multi_export_client(
            clients,
            nfs_server,
            port,
            version,
            names["nfs_name"],
            names["fs_name"],
            names["fs"],
            names["nfs_mount"],
            names["nfs_export"],
            export_num=export_num,
            ceph_cluster=ceph_cluster,
            enable_virtual_server=True,
            skip_mount=True,
        )

    return setup_nfs_cluster(
        clients,
        nfs_server,
        port,
        version,
        names["nfs_name"],
        names["nfs_mount"],
        names["fs_name"],
        names["nfs_export"],
        names["fs"],
        ceph_cluster=ceph_cluster,
        single_export=single_export,
        enable_virtual_server=True,
        skip_mount=True,
    )


def export_name_for_index(nfs_export_base, index, single_export=False):
    if single_export:
        return f"{nfs_export_base}_0"
    return f"{nfs_export_base}_{index}"


def mount_path_for_route(base_mount, route_id):
    return f"{base_mount}_route{route_id}"


def mount_nfs_route(
    client,
    server_ip,
    export,
    mount_path,
    version,
    port,
    route=None,
    sudo=True,
):
    if route is not None:
        log_route_taken(route, "mount", extra=f"export={export} path={mount_path}")
    client.create_dirs(dir_path=mount_path, sudo=True)
    Mount(client).nfs(
        mount=mount_path,
        version=version,
        port=str(port),
        server=server_ip,
        export=export,
    )
    if not sudo:
        prepare_mount_for_nonroot_io(client, mount_path)
    log.info(
        "Mounted %s:%s on %s at %s (IO sudo=%s)",
        server_ip,
        export,
        client.hostname,
        mount_path,
        sudo,
    )


def mount_nfs_for_route(route, export, mount_path, version, port, sudo=True):
    """Mount using a route dict; logs the path taken."""
    mount_nfs_route(
        route["client"],
        route["server_ip"],
        export,
        mount_path,
        version,
        port,
        route=route,
        sudo=sudo,
    )


def unmount_nfs_route(client, mount_path):
    try:
        Unmount(client).unmount(mount_path)
    except Exception as exc:
        log.warning("Unmount %s on %s: %s", mount_path, client.hostname, exc)
    client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}", check_ec=False)


def run_nfs_io(client, mount_path, route_id, route=None, sudo=True):
    """Write and read a route-specific file on the mount."""
    if route is not None:
        log_route_taken(
            route,
            "io",
            extra=f"path={mount_path} sudo={sudo}",
        )
    test_file = f"{mount_path}/vs_route_{route_id}.dat"
    marker = f"virtual-server-route-{route_id}"
    client.exec_command(
        sudo=sudo,
        cmd=f"bash -c 'echo {marker} > {test_file}'",
    )
    out, _ = client.exec_command(sudo=sudo, cmd=f"cat {test_file}")
    if marker not in (out if isinstance(out, str) else str(out)):
        raise OperationFailedError(
            f"IO verify failed on {client.hostname} mount {mount_path} (sudo={sudo})"
        )
    client.exec_command(
        sudo=sudo,
        cmd=f"dd if=/dev/urandom of={mount_path}/vs_dd_{route_id} bs=4K count=4",
    )
    log.info(
        "IO succeeded on %s at %s (sudo=%s)",
        client.hostname,
        mount_path,
        sudo,
    )


def run_nfs_io_for_route(route, mount_path, sudo=True):
    run_nfs_io(
        route["client"],
        mount_path,
        route["route_id"],
        route=route,
        sudo=sudo,
    )


def assert_mount_fails(
    client, server_ip, export, mount_path, version, port, route=None
):
    if route is not None:
        log_route_taken(
            route,
            "mount-expected-fail",
            extra=f"export={export} path={mount_path}",
        )
    client.create_dirs(dir_path=mount_path, sudo=True)
    cmd = (
        f"mount -t nfs -o vers={version},port={port} "
        f"{server_ip}:{export} {mount_path}"
    )
    client.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if _command_succeeded(client) == 0:
        try:
            Unmount(client).unmount(mount_path)
        except Exception:
            pass
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}", check_ec=False)
        raise OperationFailedError(
            f"Mount should have failed: {client.hostname} "
            f"{server_ip}:{export} but succeeded"
        )
    log.info(
        "Mount correctly failed for %s:%s from %s",
        server_ip,
        export,
        client.hostname,
    )


def assert_mount_fails_for_route(route, export, mount_path, version, port):
    assert_mount_fails(
        route["client"],
        route["server_ip"],
        export,
        mount_path,
        version,
        port,
        route=route,
    )


def _inject_export_fencing(conf, server_ip, client_ip):
    conf = re.sub(r"\n\s*Server_Addrs\s*=.*;", "", conf)
    conf = re.sub(
        r"\n\s*CLIENT\s*\{[^}]*\}\s*;",
        "",
        conf,
        flags=re.DOTALL,
    )
    fencing = (
        f'\n    Server_Addrs = "{server_ip}";\n'
        f"    CLIENT {{\n"
        f'      Clients = "{client_ip}";\n'
        f"      Access_Type = RW;\n"
        f"    }}\n"
    )
    closing = conf.rfind("}")
    if closing == -1:
        raise OperationFailedError("Invalid export conf: missing closing brace")
    return conf[:closing] + fencing + conf[closing:]


def apply_export_fencing(client, nfs_name, export_name, server_ip, client_ip):
    """Apply Server_Addrs and CLIENT fencing blocks to an export."""
    out = Ceph(client).nfs.export.get(nfs_name, export_name)
    conf = out[0] if isinstance(out, tuple) else out
    if isinstance(conf, bytes):
        conf = conf.decode("utf-8", errors="replace")
    fenced = _inject_export_fencing(conf.strip(), server_ip, client_ip)
    conf_path = "/tmp/export_fence.conf"
    client.exec_command(
        sudo=True,
        cmd=f"cat > {conf_path} <<'VS_EXPORT_EOF'\n{fenced}\nVS_EXPORT_EOF",
    )
    Ceph(client).nfs.export.apply(nfs_name, conf_path)
    sleep(10)
    log.info(
        "Applied export fencing on %s: Server_Addrs=%s Clients=%s",
        export_name,
        server_ip,
        client_ip,
    )


def apply_export_fencing_for_route(client, nfs_name, export_name, route):
    log_route_taken(route, "fencing", extra=f"export={export_name}")
    apply_export_fencing(
        client,
        nfs_name,
        export_name,
        route["server_ip"],
        route["client_ip"],
    )


def _client_route_mounts(clients, mount_prefix):
    """Return (client, mount_path) for each active route mount."""
    if not isinstance(clients, list):
        clients = [clients]
    mounts = []
    for client in clients:
        out, _ = client.exec_command(sudo=True, cmd="mount", check_ec=False)
        mount_out = out if isinstance(out, str) else str(out)
        for line in mount_out.splitlines():
            if mount_prefix in line:
                mounts.append((client, line.split()[2]))
    return mounts


def cleanup_virtual_server(
    clients,
    nfs_name,
    nfs_export_base,
    mount_prefix="/mnt/nfs",
    export_names=None,
    nfs_nodes=None,
    fs_name="cephfs",
):
    """
    Unmount route mounts, delete exports, and remove the NFS cluster.

    Mirrors ``nfs_operations.cleanup_cluster`` / ``cleanup_custom_nfs_cluster_*``:
    collect orch status, Ganesha logs and conf, then unmount and delete resources.
    """
    names = default_nfs_names()
    nfs_name = nfs_name or names["nfs_name"]
    fs_name = fs_name or names["fs_name"]
    if not isinstance(clients, list):
        clients = [clients]
    client = clients[0]

    if nfs_nodes is not None:
        nfs_log_parser(client=client, nfs_node=nfs_nodes, nfs_name=nfs_name)
    else:
        log.warning(
            "Skipping nfs_log_parser (orch ls, container logs, ganesha.conf): "
            "nfs_nodes not passed to cleanup_virtual_server"
        )

    route_mounts = _client_route_mounts(clients, mount_prefix)
    for mount_client, mount_path in route_mounts:
        mount_cleanup_retry(mount_client, mount_path)
        log.info(
            "Unmounting nfs-ganesha mount %s on %s", mount_path, mount_client.hostname
        )
        sleep(3)
        try:
            Unmount(mount_client).unmount(mount_path)
        except Exception as exc:
            log.warning("Unmount %s on %s: %s", mount_path, mount_client.hostname, exc)
        log.info(
            "Removing nfs-ganesha mount dir %s on %s", mount_path, mount_client.hostname
        )
        mount_client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}", check_ec=False)
        sleep(3)

    if export_names is None:
        try:
            export_names = Ceph(client).nfs.export.ls(nfs_name)
        except Exception:
            export_names = []

    for export in export_names or []:
        try:
            Ceph(client).nfs.export.delete(nfs_name, export)
        except Exception as exc:
            log.warning("Failed to delete export %s: %s", export, exc)

    try:
        Ceph(client).nfs.cluster.delete(nfs_name)
    except Exception as exc:
        log.warning("Failed to delete NFS cluster %s: %s", nfs_name, exc)

    sleep(30)
    try:
        check_nfs_daemons_removed(client)
    except Exception as exc:
        log.warning("NFS daemon removal check: %s", exc)

    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume ls {fs_name} --group_name {GANESHA_SUBVOL_GROUP}",
            check_ec=False,
        )
        text = out if isinstance(out, str) else (out[0] if out else "")
        if text.strip():
            data = json.loads(text)
            for item in data:
                subvol = item["name"]
                client.exec_command(
                    sudo=True,
                    cmd=(
                        f"ceph fs subvolume rm {fs_name} {subvol} "
                        f"--group_name {GANESHA_SUBVOL_GROUP}"
                    ),
                    check_ec=False,
                )
        client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolumegroup rm {fs_name} {GANESHA_SUBVOL_GROUP} --force",
            check_ec=False,
        )
    except Exception as exc:
        log.warning("Subvolume cleanup skipped: %s", exc)

    sleep(5)


def get_clients(ceph_cluster, config):
    clients = ceph_cluster.get_nodes("client")
    count = int(config.get("clients", 2))
    if count > len(clients):
        raise ConfigError(
            f"Test requires {count} clients but only {len(clients)} available"
        )
    return clients[:count]


def get_nfs_server_hostname(ceph_cluster):
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if not nfs_nodes:
        raise ConfigError("No NFS role node found in cluster configuration")
    return nfs_nodes[0].hostname


def export_names_for_count(export_num, nfs_export_base="/export"):
    return [f"{nfs_export_base}_{i}" for i in range(export_num)]
