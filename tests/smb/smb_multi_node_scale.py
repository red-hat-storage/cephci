"""Scale one SMB cluster from three to twelve hosts and exercise every host."""

import json
import re

from smb_operations import (
    deploy_smb_service_imperative,
    smb_cifs_mount,
    smb_cleanup,
    smbclient_check_shares,
)

from ceph.waiter import WaitUntil
from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)
INITIAL_SMB_NODE_COUNT = 3


def _node_names(node):
    names = {node.shortname, node.hostname, node.hostname.split(".")[0]}
    for hostname in (node.shortname, node.hostname):
        match = re.search(r"(?:^|[-_])(node\d+)(?:[-_.]|$)", hostname)
        if match:
            names.add(match.group(1))
    return names


def _resolve_node_groups(ceph_cluster, configured_groups):
    nodes_by_name = {}
    for node in ceph_cluster.get_nodes():
        for name in _node_names(node):
            nodes_by_name[name] = node

    groups = []
    for group in configured_groups:
        resolved = []
        for name in group:
            node = nodes_by_name.get(name)
            if node is None:
                raise OperationFailedError(
                    f"Configured SMB node '{name}' was not found"
                )
            resolved.append(node)
        groups.append(resolved)
    return groups


def _wait_for_daemon_count(installer, service_name, expected_count):
    """Wait until cephadm reports the expected number of running SMB daemons."""
    command = (
        "cephadm shell -- ceph orch ps " f"--service_name {service_name} --format json"
    )
    timeout, interval = 600, 15
    for wait in WaitUntil(timeout=timeout, interval=interval):
        try:
            output, _ = installer.exec_command(sudo=True, cmd=command)
            daemons = json.loads(output)
            running = [
                daemon
                for daemon in daemons
                if str(daemon.get("status_desc", "")).strip().lower() == "running"
            ]
            if len(running) == expected_count:
                log.info(
                    f"SMB service {service_name} has {expected_count} running daemons"
                )
                return
            log.info(
                f"Waiting for {expected_count} running SMB daemons; "
                f"currently found {len(running)}"
            )
        except (ValueError, TypeError, KeyError) as error:
            log.info(f"Unable to read SMB daemon status yet: {error}")
    if wait.expired:
        raise OperationFailedError(
            f"SMB service {service_name} did not reach {expected_count} running daemons"
        )


def _add_smb_label(installer, node):
    installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph orch host label add {node.shortname} smb",
    )


def _exercise_nodes(
    client,
    nodes,
    shares,
    username,
    password,
    auth_mode,
    domain_realm,
    mount_root,
    fio_config,
):
    smbclient_check_shares(
        nodes, client, shares, username, password, auth_mode, domain_realm
    )

    mounted = []
    try:
        for index, node in enumerate(nodes, start=1):
            mount_point = f"{mount_root}/node-{index}"
            smb_cifs_mount(
                node,
                client,
                shares[0],
                username,
                password,
                auth_mode,
                domain_realm,
                mount_point,
            )
            mounted.append(mount_point)
            size = fio_config.get("size", "16m")
            runtime = int(fio_config.get("runtime", 10))
            fio_name = re.sub(r"[^A-Za-z0-9_-]", "_", node.shortname)
            command = (
                f"fio --name=smb-{fio_name} "
                f"--filename={mount_point}/fio-test-{fio_name} "
                f"--size={size} --rw=write --bs=4k --iodepth=1 --numjobs=1 "
                f"--runtime={runtime} --time_based --direct=0 --end_fsync=1"
            )
            client.exec_command(sudo=True, cmd=command, long_running=True)
    finally:
        for mount_point in reversed(mounted):
            client.exec_command(sudo=True, cmd=f"umount {mount_point}")
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}")


def run(ceph_cluster, **kw):
    """Deploy SMB on three hosts, then add three hosts at each checkpoint."""
    config = kw.get("config") or {}
    groups = config.get("smb_node_groups", [])
    if len(groups) != 4 or any(len(group) != 3 for group in groups):
        raise OperationFailedError(
            "smb_node_groups must contain four groups of three host names"
        )

    installer = ceph_cluster.get_nodes(role="installer")[0]
    client = ceph_cluster.get_nodes(role="client")[0]
    node_groups = _resolve_node_groups(ceph_cluster, groups)
    expected_initial = {node.shortname for node in node_groups[0]}
    initially_labeled = {node.shortname for node in ceph_cluster.get_nodes("smb")}
    if len(expected_initial) != INITIAL_SMB_NODE_COUNT:
        raise OperationFailedError(
            "The initial SMB node group must contain exactly "
            f"{INITIAL_SMB_NODE_COUNT} nodes; found {len(expected_initial)}"
        )
    if len(initially_labeled) != INITIAL_SMB_NODE_COUNT:
        raise OperationFailedError(
            f"Expected {INITIAL_SMB_NODE_COUNT} SMB nodes, found "
            f"{len(initially_labeled)}: {sorted(initially_labeled)}"
        )
    if initially_labeled != expected_initial:
        raise OperationFailedError(
            "The initial SMB label must be present on exactly the first three nodes; "
            f"expected {sorted(expected_initial)}, found {sorted(initially_labeled)}"
        )

    volume = config.get("cephfs_volume", "cephfs")
    group = config.get("smb_subvolume_group", "smb")
    subvolumes = config.get("smb_subvolumes", ["sv1"])
    cluster_id = config.get("smb_cluster_id", "smb1")
    shares = config.get("smb_shares", ["share1"])
    username = config.get("smb_user_name", "user1")
    password = config.get("smb_user_password", "passwd")
    auth_mode = config.get("auth_mode", "user")
    domain_realm = config.get("domain_realm")
    deployed = False
    mount_root = config.get("cifs_mount_root", "/mnt/smb-multi-node")

    try:
        deploy_smb_service_imperative(
            installer,
            volume,
            group,
            subvolumes,
            config.get("smb_subvolume_mode", "0777"),
            cluster_id,
            auth_mode,
            username,
            password,
            shares,
            config.get("path", "/"),
            domain_realm,
            config.get("custom_dns"),
        )
        deployed = True

        active_nodes = []
        for stage, node_group in enumerate(node_groups):
            if stage:
                for node in node_group:
                    _add_smb_label(installer, node)
            active_nodes.extend(node_group)

            _wait_for_daemon_count(installer, f"smb.{cluster_id}", len(active_nodes))
            log.info(
                f"Testing SMB cluster with {len(active_nodes)} nodes: "
                f"{[node.shortname for node in active_nodes]}"
            )
            _exercise_nodes(
                client,
                active_nodes,
                shares,
                username,
                password,
                auth_mode,
                domain_realm,
                f"{mount_root}/{len(active_nodes)}-nodes",
                config.get("fio", {}),
            )
    except Exception as error:
        log.error(f"SMB multi-node scale test failed: {error}")
        return 1
    finally:
        if deployed:
            smb_cleanup(installer, shares, cluster_id, volume=volume, group_name=group)
    return 0
