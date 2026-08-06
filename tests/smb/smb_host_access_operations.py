import ipaddress
import json
import shlex
import uuid
from copy import deepcopy
from time import sleep

from smb_operations import (
    check_ctdb_health,
    check_rados_clustermeta,
    deploy_smb_service_declarative,
    generate_apply_smb_spec,
    smb_cleanup,
)

from ceph.ceph_admin import CephAdmin
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


TEST_NETWORKS = (
    ipaddress.ip_network("192.0.2.0/24"),
    ipaddress.ip_network("198.51.100.0/24"),
    ipaddress.ip_network("203.0.113.0/24"),
)
ACCESS_DENIED_MARKERS = (
    "NT_STATUS_ACCESS_DENIED",
    "NT_STATUS_NETWORK_ACCESS_DENIED",
    "NT_STATUS_CONNECTION_DISCONNECTED",
)


def _default_spec(config):
    """Return the local-auth SMB resources used by host-access tests."""
    cluster_id = config.get("smb_cluster_id", "cluster1")
    users_groups_id = config.get("users_groups_id", "ug1")
    return [
        {
            "resource_type": "ceph.smb.cluster",
            "cluster_id": cluster_id,
            "auth_mode": "user",
            "user_group_settings": [
                {"source_type": "resource", "ref": users_groups_id}
            ],
            "placement": {"label": config.get("smb_placement_label", "smb")},
        },
        {
            "resource_type": "ceph.smb.usersgroups",
            "users_groups_id": users_groups_id,
            "values": {
                "users": [
                    {
                        "name": config.get("smb_user_name", "user1"),
                        "password": config.get("smb_user_password", "passwd"),
                    }
                ],
                "groups": [],
            },
        },
        {
            "resource_type": "ceph.smb.share",
            "cluster_id": cluster_id,
            "share_id": config.get("smb_share", "share1"),
            "cephfs": {
                "volume": config.get("cephfs_volume", "cephfs"),
                "subvolumegroup": config.get("smb_subvolume_group", "smb"),
                "subvolume": config.get("smb_subvolume", "sv1"),
                "path": config.get("smb_path", "/"),
            },
        },
    ]


def _parse_spec(spec):
    """Extract the values required by SMB deployment and access probes."""
    values = {
        "shares": [],
        "subvolumes": [],
        "domain_realm": None,
        "clustering": "default",
        "smb_port": "445",
    }
    for resource in spec:
        resource_type = resource.get("resource_type")
        if resource_type == "ceph.smb.cluster":
            values.update(
                {
                    "cluster_id": resource["cluster_id"],
                    "auth_mode": resource["auth_mode"],
                    "domain_realm": resource.get("domain_settings", {}).get("realm"),
                    "clustering": resource.get("clustering", "default"),
                    "smb_port": resource.get("custom_ports", {}).get("smb", "445"),
                }
            )
        elif resource_type == "ceph.smb.usersgroups":
            user = resource["values"]["users"][0]
            values.update({"username": user["name"], "password": user["password"]})
        elif resource_type == "ceph.smb.join.auth":
            values.update(
                {
                    "username": resource["auth"]["username"],
                    "password": resource["auth"]["password"],
                }
            )
        elif resource_type == "ceph.smb.share":
            cephfs = resource["cephfs"]
            values.update(
                {
                    "volume": cephfs["volume"],
                    "subvolume_group": cephfs["subvolumegroup"],
                }
            )
            values["subvolumes"].append(cephfs["subvolume"])
            values["shares"].append(resource["share_id"])

    required = (
        "cluster_id",
        "auth_mode",
        "username",
        "password",
        "volume",
        "subvolume_group",
    )
    missing = [name for name in required if name not in values]
    if missing or not values["shares"] or not values["subvolumes"]:
        missing.extend(
            name
            for name, items in (
                ("shares", values["shares"]),
                ("subvolumes", values["subvolumes"]),
            )
            if not items
        )
        raise ConfigError(f"SMB spec is missing required values: {', '.join(missing)}")
    return values


def _client_source(client, destination):
    """Return the IPv4 source address and subnet used to reach an SMB node."""
    destination = str(ipaddress.ip_address(destination))
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ip -j route get {shlex.quote(destination)}",
    )
    routes = json.loads(out)
    if not routes:
        raise ConfigError(
            f"No route from {client.hostname} to SMB endpoint {destination}"
        )

    route = routes[0]
    source = route.get("prefsrc") or route.get("src")
    device = route.get("dev")
    if not source or not device:
        raise ConfigError(
            f"Route from {client.hostname} to {destination} has no source or device"
        )

    source_ip = ipaddress.ip_address(source)
    if source_ip.version != 4:
        raise ConfigError("SMB hosts_access automation currently requires IPv4 clients")

    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ip -j address show dev {shlex.quote(device)}",
    )
    interfaces = json.loads(out)
    for interface in interfaces:
        for address in interface.get("addr_info", []):
            if address.get("family") != "inet" or address.get("local") != source:
                continue
            network = ipaddress.ip_network(
                f"{source}/{address['prefixlen']}", strict=False
            )
            return {"address": str(source_ip), "network": str(network)}

    raise ConfigError(
        f"Unable to determine the subnet for {source} on {client.hostname}"
    )


def _unused_test_network(client_addresses):
    """Select an RFC 5737 network that does not contain a test client."""
    addresses = [ipaddress.ip_address(item["address"]) for item in client_addresses]
    for network in TEST_NETWORKS:
        if not any(address in network for address in addresses):
            return network
    raise ConfigError("Unable to select an unused documentation network")


def _validate_client_network(client_addresses, network_client=0):
    """Ensure all test clients are in the selected client's IPv4 subnet."""
    try:
        network = ipaddress.ip_network(client_addresses[network_client]["network"])
    except IndexError as error:
        raise ConfigError(
            f"Client index {network_client} is not available for subnet validation"
        ) from error

    outside = [
        item["address"]
        for item in client_addresses
        if ipaddress.ip_address(item["address"]) not in network
    ]
    if outside:
        raise ConfigError(
            f"Clients {', '.join(outside)} are outside required test network {network}"
        )


def _resolve_rules(rules, client_addresses):
    """Resolve symbolic client/unused sources into ceph.smb hosts_access entries."""
    resolved = []
    unused_network = _unused_test_network(client_addresses)
    for rule in rules:
        access = rule.get("access")
        kind = rule.get("kind")
        source = rule.get("source", "client")
        if access not in ("allow", "deny"):
            raise ConfigError(f"Unsupported hosts_access action: {access}")
        if kind not in ("address", "network"):
            raise ConfigError(f"Unsupported hosts_access rule kind: {kind}")

        if source == "client":
            index = int(rule.get("client", 0))
            try:
                value = client_addresses[index][kind]
            except IndexError as error:
                raise ConfigError(f"Client index {index} is not available") from error
        elif source == "unused":
            value = (
                str(unused_network)
                if kind == "network"
                else str(next(unused_network.hosts()))
            )
        else:
            raise ConfigError(f"Unsupported hosts_access rule source: {source}")

        resolved.append({kind: value, "access": access})
    return resolved


def _spec_with_rules(spec, rules, include_empty=False):
    """Return a spec with the resolved rules applied to every SMB share."""
    updated = deepcopy(spec)
    shares = [
        resource
        for resource in updated
        if resource.get("resource_type") == "ceph.smb.share"
    ]
    if not shares:
        raise ConfigError("SMB spec does not contain a share resource")
    for share in shares:
        if rules or include_empty:
            share["hosts_access"] = deepcopy(rules)
        else:
            share.pop("hosts_access", None)
    return updated


def _write_auth_file(client, values):
    """Create a temporary smbclient authentication file without logging secrets."""
    path = f"/tmp/cephci-smb-host-access-{uuid.uuid4().hex}"
    lines = [
        f"username = {values['username']}",
        f"password = {values['password']}",
    ]
    if values["auth_mode"] == "active-directory":
        if not values["domain_realm"]:
            raise ConfigError("Active Directory SMB spec has no domain realm")
        lines.append(f"domain = {values['domain_realm'].split('.')[0].upper()}")

    auth_file = client.remote_file(
        sudo=True,
        file_name=path,
        file_mode="w",
    )
    auth_file.write("\n".join(lines) + "\n")
    auth_file.flush()
    auth_file.close()
    client.exec_command(sudo=True, cmd=f"chmod 0600 {shlex.quote(path)}")
    return path


def _run_smbclient(client, smb_node, share, auth_file, smb_port):
    """Run an SMB listing probe and return stdout, stderr, and exit status."""
    endpoint = f"//{smb_node.ip_address}/{share}"
    command = (
        f"smbclient --authentication-file={shlex.quote(auth_file)} "
        f"-p {int(smb_port)} {shlex.quote(endpoint)} -c ls"
    )
    out, err, exit_status, _ = client.exec_command(
        sudo=True,
        cmd=command,
        check_ec=False,
        verbose=True,
    )
    return out, err, exit_status


def _matches_expected_access(expected, out, err, exit_status):
    if expected == "allow":
        return exit_status == 0
    if expected != "deny":
        raise ConfigError(f"Unsupported expected access result: {expected}")
    response = f"{out}\n{err}"
    return exit_status != 0 and any(
        marker in response for marker in ACCESS_DENIED_MARKERS
    )


def _check_access(
    client,
    smb_node,
    share,
    auth_file,
    smb_port,
    expected,
    timeout,
    interval,
):
    """Wait for an SMB endpoint to return the expected policy result."""
    attempts = max(1, int(timeout / interval))
    last_result = ("", "", -1)
    for attempt in range(attempts):
        last_result = _run_smbclient(
            client,
            smb_node,
            share,
            auth_file,
            smb_port,
        )
        if _matches_expected_access(expected, *last_result):
            log.info(
                "SMB host-access check passed: client=%s endpoint=%s share=%s "
                "expected=%s",
                client.hostname,
                smb_node.ip_address,
                share,
                expected,
            )
            return
        if attempt + 1 < attempts:
            sleep(interval)

    out, err, exit_status = last_result
    raise OperationFailedError(
        "SMB host-access check failed: "
        f"client={client.hostname}, endpoint={smb_node.ip_address}, "
        f"share={share}, expected={expected}, exit_status={exit_status}, "
        f"stdout={out}, stderr={err}"
    )


def _run_checks(checks, clients, smb_nodes, auth_files, values, config):
    if not checks:
        raise ConfigError("At least one access check must be configured")
    timeout = int(config.get("access_check_timeout", 90))
    interval = int(config.get("access_check_interval", 5))
    if timeout <= 0 or interval <= 0:
        raise ConfigError("Access-check timeout and interval must be positive")

    for check in checks:
        index = int(check.get("client", 0))
        try:
            client = clients[index]
            auth_file = auth_files[index]
        except IndexError as error:
            raise ConfigError(f"Client index {index} is not available") from error
        for smb_node in smb_nodes:
            for share in values["shares"]:
                _check_access(
                    client,
                    smb_node,
                    share,
                    auth_file,
                    values["smb_port"],
                    check.get("expect"),
                    timeout,
                    interval,
                )


def _apply_host_access(installer, spec, file_type, file_mount):
    """Apply an updated declarative SMB configuration."""
    generate_apply_smb_spec(installer, file_type, spec, file_mount)


def run(ceph_cluster, **kw):
    """Validate declarative SMB hosts_access rules using two Linux clients."""
    config = kw.get("config") or {}
    file_type = config.get("file_type", "yaml")
    file_mount = config.get("file_mount", "/tmp")
    if file_type not in ("yaml", "json"):
        raise ConfigError("Host-access automation supports YAML or JSON specs")

    installer = ceph_cluster.get_nodes(role="installer")[0]
    smb_nodes = ceph_cluster.get_nodes("smb")
    clients = ceph_cluster.get_nodes(role="client")
    if not smb_nodes:
        raise ConfigError("No node with the 'smb' role is available")
    if len(clients) < 2:
        raise ConfigError("SMB hosts_access automation requires at least two clients")

    base_spec = deepcopy(config.get("spec") or _default_spec(config))
    base_spec = _spec_with_rules(base_spec, [])
    values = _parse_spec(base_spec)
    cephadm = CephAdmin(cluster=ceph_cluster, **config)

    client_addresses = [
        _client_source(client, smb_nodes[0].ip_address) for client in clients
    ]
    log.info("Resolved SMB client source addresses: %s", client_addresses)
    if config.get("require_clients_same_network"):
        _validate_client_network(
            client_addresses,
            int(config.get("network_client", 0)),
        )

    rules = _resolve_rules(config.get("host_access", []), client_addresses)
    initial_rules = _resolve_rules(
        config.get("initial_host_access", []), client_addresses
    )
    auth_files = []
    deployed = False
    result = 0

    try:
        deployed = True
        deploy_smb_service_declarative(
            installer,
            values["volume"],
            values["subvolume_group"],
            values["subvolumes"],
            values["cluster_id"],
            config.get("smb_subvolume_mode", "0777"),
            file_type,
            base_spec,
            file_mount,
        )
        if values["clustering"] != "never":
            if not check_rados_clustermeta(cephadm, values["cluster_id"], smb_nodes):
                raise OperationFailedError("Samba RADOS clustermeta was not found")
            if not check_ctdb_health(smb_nodes, values["cluster_id"]):
                raise OperationFailedError("Samba CTDB health check failed")

        for client in clients:
            auth_files.append(_write_auth_file(client, values))

        # Establish a positive baseline before testing expected failures. This
        # prevents authentication or service failures from passing as policy denials.
        _run_checks(
            [{"client": index, "expect": "allow"} for index in range(len(clients))],
            clients,
            smb_nodes,
            auth_files,
            values,
            config,
        )

        if initial_rules:
            initial_spec = _spec_with_rules(base_spec, initial_rules)
            _apply_host_access(installer, initial_spec, file_type, file_mount)
            _run_checks(
                config.get("initial_access_checks", []),
                clients,
                smb_nodes,
                auth_files,
                values,
                config,
            )

        updated_spec = _spec_with_rules(
            base_spec,
            rules,
            include_empty="host_access" in config,
        )
        _apply_host_access(installer, updated_spec, file_type, file_mount)
        _run_checks(
            config.get("access_checks", []),
            clients,
            smb_nodes,
            auth_files,
            values,
            config,
        )
    except Exception as error:
        log.error("SMB hosts_access test failed: %s", error, exc_info=True)
        result = 1
    finally:
        for client, auth_file in zip(clients, auth_files):
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -f {shlex.quote(auth_file)}",
                    check_ec=False,
                )
            except Exception as error:
                log.error(
                    "Failed to remove SMB authentication file on %s: %s",
                    client.hostname,
                    error,
                    exc_info=True,
                )
                result = 1
        if deployed:
            try:
                smb_cleanup(
                    installer,
                    values["shares"],
                    values["cluster_id"],
                    volume=values["volume"],
                    group_name=values["subvolume_group"],
                )
            except Exception as error:
                log.error("SMB hosts_access cleanup failed: %s", error, exc_info=True)
                result = 1
    return result
