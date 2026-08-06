"""SMB colocation test scenarios.

The scenarios in this module deploy multiple SMB clusters on one Ceph host and
validate that each cluster remains independently usable.
"""

import ipaddress
import json
import shlex

from smb_operations import (
    apply_smb_spec,
    create_vol_smb_subvol,
    enable_smb_module,
    generate_smb_spec,
    smb_cleanup,
)

from ceph.waiter import WaitUntil
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.utils import reboot_node
from utility.log import Log

log = Log(__name__)

VOLUME = "cephfs"
SUBVOLUME_GROUP = "smb"
SHARE = "share1"


def _credentials(index):
    """Return non-production credentials used only by the test deployment."""
    return f"user{index}", "".join(("pass", "wd", str(index)))


def _cluster(index, host, base_port):
    cluster_id = f"colocated{index}"
    username, password = _credentials(index)
    return {
        "id": cluster_id,
        "user": username,
        "password": password,
        "port": base_port + ((index - 1) * 10),
        "host": host,
        "subvolume": f"sv-{cluster_id}",
    }


def _node_network(smb_node):
    """Return the IPv4 network carrying the selected SMB node's test IP."""
    output, _ = smb_node.exec_command(
        sudo=True, cmd="ip -j -4 address show scope global"
    )
    node_ip = ipaddress.ip_address(str(smb_node.ip_address))
    for interface in json.loads(output):
        for address in interface.get("addr_info", []):
            local_address = address.get("local")
            if local_address and ipaddress.ip_address(local_address) == node_ip:
                return str(
                    ipaddress.ip_interface(
                        f"{local_address}/{address['prefixlen']}"
                    ).network
                )
    raise ConfigError(
        f"Unable to determine the network for SMB node IP {smb_node.ip_address}"
    )


def _resources(clusters):
    resources = []
    for item in clusters:
        if item.get("auth_mode") == "active-directory":
            cluster_resource = {
                "resource_type": "ceph.smb.cluster",
                "cluster_id": item["id"],
                "auth_mode": "active-directory",
                "domain_settings": {
                    "realm": item["realm"],
                    "join_sources": [
                        {
                            "source_type": "resource",
                            "ref": item["join_auth_id"],
                        }
                    ],
                },
                "custom_dns": item["custom_dns"],
                "custom_ports": {
                    "smb": item["port"],
                    "smbmetrics": item["port"] + 1,
                    "ctdb": item["port"] + 2,
                },
                "placement": {"hosts": [item["host"]]},
            }
            if item.get("bind_network"):
                cluster_resource["bind_addrs"] = [{"network": item["bind_network"]}]
            resources.extend(
                [
                    cluster_resource,
                    {
                        "resource_type": "ceph.smb.join.auth",
                        "auth_id": item["join_auth_id"],
                        "auth": {
                            "username": item["user"],
                            "password": item["password"],
                        },
                    },
                    {
                        "resource_type": "ceph.smb.share",
                        "cluster_id": item["id"],
                        "share_id": SHARE,
                        "cephfs": {
                            "volume": VOLUME,
                            "subvolumegroup": SUBVOLUME_GROUP,
                            "subvolume": item["subvolume"],
                            "path": "/",
                        },
                    },
                ]
            )
            continue
        user_group_id = f"ug-{item['id']}"
        cluster_resource = {
            "resource_type": "ceph.smb.cluster",
            "cluster_id": item["id"],
            "auth_mode": "user",
            "user_group_settings": [{"source_type": "resource", "ref": user_group_id}],
            "custom_ports": {
                "smb": item["port"],
                "smbmetrics": item["port"] + 1,
                "ctdb": item["port"] + 2,
            },
            "placement": {"hosts": [item["host"]]},
        }
        if item.get("bind_network"):
            cluster_resource["bind_addrs"] = [{"network": item["bind_network"]}]
        resources.extend(
            [
                cluster_resource,
                {
                    "resource_type": "ceph.smb.usersgroups",
                    "users_groups_id": user_group_id,
                    "values": {
                        "users": [{"name": item["user"], "password": item["password"]}],
                        "groups": [],
                    },
                },
                {
                    "resource_type": "ceph.smb.share",
                    "cluster_id": item["id"],
                    "share_id": SHARE,
                    "cephfs": {
                        "volume": VOLUME,
                        "subvolumegroup": SUBVOLUME_GROUP,
                        "subvolume": item["subvolume"],
                        "path": "/",
                    },
                },
            ]
        )
    return resources


def _apply(installer, clusters, file_mount):
    create_vol_smb_subvol(
        installer,
        VOLUME,
        SUBVOLUME_GROUP,
        [item["subvolume"] for item in clusters],
        "0777",
    )
    enable_smb_module(installer, clusters[0]["id"])
    spec_file = generate_smb_spec(installer, "yaml", _resources(clusters))
    apply_smb_spec(installer, spec_file, file_mount)
    _wait_for_services(installer, clusters)


def _create_additional_subvolumes(installer, clusters):
    for item in clusters:
        CephAdm(installer).ceph.fs.sub_volume.create(
            VOLUME,
            item["subvolume"],
            group_name=SUBVOLUME_GROUP,
            mode="0777",
        )


def _wait_for_services(installer, clusters, timeout=600):
    expected = {f"smb.{item['id']}" for item in clusters}
    for waiter in WaitUntil(timeout=timeout, interval=20):
        services = json.loads(
            CephAdm(installer).ceph.orch.ls(service_type="smb", format="json-pretty")
        )
        running = {
            service["service_name"]
            for service in services
            if service.get("status", {}).get("running", 0) > 0
        }
        if expected.issubset(running):
            return
    if waiter.expired:
        raise OperationFailedError(
            f"SMB services did not become ready: {sorted(expected - running)}"
        )


def _smbclient(client, smb_node, item, username=None, password=None, check_ec=True):
    username = username or item["user"]
    password = password or item["password"]
    if item.get("realm"):
        username = f"{item['realm'].split('.')[0].upper()}\\{username}"
    credential = shlex.quote(f"{username}%{password}")
    target = shlex.quote(f"//{smb_node.ip_address}/{SHARE}")
    command = f"smbclient -U {credential} -p {item['port']} {target} " "-c 'ls'"
    if check_ec:
        return client.exec_command(sudo=True, cmd=command, check_ec=True)
    out, err, exit_code, _ = client.exec_command(
        sudo=True, cmd=command, check_ec=False, verbose=True
    )
    return out, err, exit_code


def _verify_access(client, smb_node, clusters):
    for item in clusters:
        _smbclient(client, smb_node, item)


def _wait_for_access(client, smb_node, clusters, timeout=300):
    last_error = None
    for waiter in WaitUntil(timeout=timeout, interval=10):
        try:
            _verify_access(client, smb_node, clusters)
            return
        except Exception as error:
            last_error = error
            log.info("Waiting for colocated SMB shares to accept connections")
    if waiter.expired:
        raise OperationFailedError(
            f"SMB shares did not become accessible: {last_error}"
        )


def _verify_custom_ports(smb_node, clusters):
    for item in clusters:
        output, _ = smb_node.exec_command(
            sudo=True,
            cmd=f"ss -lnt '( sport = :{item['port']} )'",
        )
        if f":{item['port']}" not in output:
            raise OperationFailedError(
                f"SMB port {item['port']} is not listening for {item['id']}"
            )


def _verify_independent_auth(client, smb_node, clusters):
    _verify_access(client, smb_node, clusters)
    for current, other in zip(clusters, reversed(clusters)):
        if current["id"] == other["id"]:
            continue
        _, _, exit_code = _smbclient(
            client,
            smb_node,
            current,
            username=other["user"],
            password=other["password"],
            check_ec=False,
        )
        if exit_code == 0:
            raise OperationFailedError(
                f"Credentials from {other['id']} authenticated to {current['id']}"
            )


def _verify_simultaneous_io(client, smb_node, clusters):
    commands = []
    for item in clusters:
        credential = shlex.quote(f"{item['user']}%{item['password']}")
        target = shlex.quote(f"//{smb_node.ip_address}/{SHARE}")
        test_file = f"/tmp/{item['id']}-io.txt"
        client.exec_command(cmd=f"echo {item['id']} > {test_file}")
        commands.append(
            f"smbclient -U {credential} -p {item['port']} {target} "
            f"-c 'put {test_file} io.txt; get io.txt {test_file}.read'"
        )
    client.exec_command(sudo=True, cmd=" & ".join(commands) + " & wait")
    for item in clusters:
        test_file = f"/tmp/{item['id']}-io.txt"
        client.exec_command(cmd=f"cmp {test_file} {test_file}.read")


def _verify_mounts(client, smb_node, clusters):
    mount_points = []
    try:
        for item in clusters:
            mount_point = f"/mnt/{item['id']}"
            mount_points.append(mount_point)
            options = shlex.quote(
                f"username={item['user']},password={item['password']},port={item['port']}"
            )
            client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
            client.exec_command(
                sudo=True,
                cmd=(
                    f"mount.cifs //{smb_node.ip_address}/{SHARE} {mount_point} "
                    f"-o {options}"
                ),
            )
            client.exec_command(
                sudo=True,
                cmd=f"echo {item['id']} > {mount_point}/mount-test.txt",
            )
            client.exec_command(
                sudo=True,
                cmd=f"grep -Fx {item['id']} {mount_point}/mount-test.txt",
            )
    finally:
        for mount_point in mount_points:
            client.exec_command(sudo=True, cmd=f"umount {mount_point}", check_ec=False)


def _resource_snapshot(installer, smb_node, timeout=60):
    """Collect CPU and RSS without relying on the Podman stats API."""
    records = []
    last_error = None
    for waiter in WaitUntil(timeout=timeout, interval=5):
        records = []
        processes = json.loads(
            CephAdm(installer).ceph.orch.ps(daemon_type="smb", format="json")
        )
        for process in processes:
            if process.get("hostname") != smb_node.hostname:
                continue
            container_id = process.get("container_id")
            if not container_id:
                continue
            pid_output, error = smb_node.exec_command(
                sudo=True,
                cmd=(
                    f"podman inspect {shlex.quote(container_id)} "
                    "--format '{{.State.Pid}}'"
                ),
                check_ec=False,
            )
            pid = pid_output.strip()
            if error or not pid.isdigit() or pid == "0":
                last_error = error or f"No live PID for container {container_id}"
                continue
            usage, error = smb_node.exec_command(
                sudo=True,
                cmd=f"ps -p {pid} -o %cpu=,rss=",
                check_ec=False,
            )
            fields = usage.split()
            if error or len(fields) != 2:
                last_error = error or f"No process data for PID {pid}"
                continue
            records.append(
                {
                    "daemon_name": process.get("daemon_name"),
                    "container_id": container_id,
                    "pid": int(pid),
                    "cpu_percent": float(fields[0]),
                    "rss_kib": int(fields[1]),
                }
            )
        if records:
            break
    if not records:
        raise OperationFailedError(
            f"No SMB container resource data was returned: {last_error}"
        )
    log.info(f"SMB container resource snapshot: {records}")
    return records


def _cleanup(installer, clusters):
    if not clusters:
        return
    cluster_listing = CephAdm(installer).ceph.smb.cluster.ls()
    deployed = [item for item in clusters if item["id"] in cluster_listing]
    if not deployed:
        return
    for item in deployed[1:]:
        CephAdm(installer).ceph.smb.share.rm(item["id"], SHARE)
        CephAdm(installer).ceph.smb.cluster.rm(item["id"])
    smb_cleanup(installer, [SHARE], deployed[0]["id"])


def _configure_ad_clusters(clusters, config):
    required = ("realm", "custom_dns", "username", "password")
    ad_config = config.get("ad", {})
    missing = [key for key in required if not ad_config.get(key)]
    if missing:
        raise ConfigError(f"Missing mandatory AD configuration: {', '.join(missing)}")
    for index, item in enumerate(clusters, start=1):
        item.update(
            {
                "auth_mode": "active-directory",
                "realm": ad_config["realm"],
                "custom_dns": ad_config["custom_dns"],
                "user": ad_config["username"],
                "password": ad_config["password"],
                "join_auth_id": f"join-{index}",
            }
        )


def _run_scenario(scenario, installer, smb_node, client, clusters, config):
    file_mount = config.get("file_mount", "/tmp")

    if scenario == "dynamic_bind_network":
        clusters[0]["bind_network"] = _node_network(smb_node)
        log.info(
            f"Using dynamically discovered bind network "
            f"{clusters[0]['bind_network']} on {smb_node.hostname}"
        )
        _apply(installer, clusters[:1], file_mount)
        _verify_access(client, smb_node, clusters[:1])
        return clusters[:1]

    if scenario == "ad_dynamic_bind_network":
        _configure_ad_clusters(clusters, config)
        bind_network = _node_network(smb_node)
        for item in clusters:
            item["bind_network"] = bind_network
        log.info(
            f"Using dynamically discovered bind network "
            f"{bind_network} on {smb_node.hostname}"
        )
        _apply(installer, clusters, file_mount)
        _verify_access(client, smb_node, clusters)
        return clusters

    if scenario == "incremental_scale":
        targets = config.get("scale_targets", [1, 5, 10])
        deployed = []
        for target in targets:
            batch = clusters[len(deployed) : int(target)]
            if not batch:
                continue
            if not deployed:
                _apply(installer, batch, file_mount)
            else:
                _create_additional_subvolumes(installer, batch)
                spec_file = generate_smb_spec(installer, "yaml", _resources(batch))
                apply_smb_spec(installer, spec_file, file_mount)
            deployed.extend(batch)
            _wait_for_services(installer, deployed)
            _verify_access(client, smb_node, deployed)
        return deployed

    if scenario == "ad_integration":
        _configure_ad_clusters(clusters, config)
        _apply(installer, clusters, file_mount)
        _verify_access(client, smb_node, clusters)
        return clusters

    if scenario == "resource_usage":
        initial = clusters[:2]
        remaining = clusters[2:]
        _apply(installer, initial, file_mount)
        _resource_snapshot(installer, smb_node)
        if remaining:
            _create_additional_subvolumes(installer, remaining)
            spec_file = generate_smb_spec(installer, "yaml", _resources(remaining))
            apply_smb_spec(installer, spec_file, file_mount)
            _wait_for_services(installer, clusters)
        _resource_snapshot(installer, smb_node)
        _verify_access(client, smb_node, clusters)
        return clusters

    _apply(installer, clusters, file_mount)
    if scenario == "custom_ports":
        _verify_custom_ports(smb_node, clusters)
        _verify_access(client, smb_node, clusters)
    elif scenario == "node_reboot":
        _verify_access(client, smb_node, clusters)
        reboot_node(smb_node)
        _wait_for_services(installer, clusters)
        _wait_for_access(client, smb_node, clusters)
    elif scenario == "mount_both":
        _verify_mounts(client, smb_node, clusters)
    elif scenario == "independent_auth":
        _verify_independent_auth(client, smb_node, clusters)
    elif scenario == "simultaneous_io":
        _verify_simultaneous_io(client, smb_node, clusters)
    else:
        raise ConfigError(f"Unsupported colocation scenario: {scenario}")
    return clusters


def run(ceph_cluster, **kw):
    """Run a configured SMB colocation scenario."""
    config = kw.get("config", {})
    scenario = config.get("scenario")
    if not scenario:
        raise ConfigError("Mandatory config 'scenario' not provided")

    installer = ceph_cluster.get_nodes(role="installer")[0]
    smb_nodes = ceph_cluster.get_nodes(role="smb")
    clients = ceph_cluster.get_nodes(role="client")
    if not smb_nodes or not clients:
        raise ConfigError("The test requires smb and client nodes")

    count = int(config.get("cluster_count", 2))
    base_port = int(config.get("base_port", 4450))
    clusters = [
        _cluster(index, smb_nodes[0].hostname, base_port)
        for index in range(1, count + 1)
    ]
    failed = False
    try:
        _run_scenario(scenario, installer, smb_nodes[0], clients[0], clusters, config)
    except Exception as error:
        log.error(f"SMB colocation scenario '{scenario}' failed: {error}")
        failed = True
    finally:
        if config.get("cleanup", True):
            try:
                _cleanup(installer, clusters)
            except Exception as error:
                log.error(f"SMB colocation cleanup failed: {error}")
                failed = True
    return 1 if failed else 0
