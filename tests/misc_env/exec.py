# Module to assist in executing a sequence of command.
import json
import re
from typing import Any, Tuple

from ceph.utils import get_node_by_id, translate_to_ip
from utility.log import Log

LOG = Log(__name__)


def exec_command(node, **kwargs: Any) -> Tuple:
    """
    Return the results of the command executed.

    Args:
        node:   The node to be used for executing the given command.
        kwargs: The supported keys are
                sudo    - Default false, root privilege to be used or not
                cmd     - The command to be executed
                check_ec - Evaluate the error code returned.

    Return:
        out, err    String values

    Raises:
        CommandFailed   when there is an execution failure
    """
    out, err = node.exec_command(**kwargs)
    return out, err


def translate_to_hostname(cluster, string: str) -> str:
    """
    Return the string with node ID replaced with shortname.

    In this method, the pattern {node:node1} would be replaced with the value of
    node.shortname.

    Args:
        cluster:    Ceph cluster instance
        string:    String to be searched for node ID pattern

    Return:
        String whose node ID's are replaced with shortnames
    """
    replaced_string = string
    node_list = re.findall("{node:(.+?)}", string)

    for node in node_list:
        node_name = get_node_by_id(cluster, node).shortname
        replacement_pattern = "{node:" + node + "}"
        replaced_string = re.sub(replacement_pattern, node_name, replaced_string)

    return replaced_string


def translate_to_service_name(node, string: str) -> str:
    """
    Return the string after replacing the service id with it's service name.

    If the given string contains {service_name:<value>} then this substring is replaced
    with the name of the service that matches the given <value>.

    The service name is determined by iterating through the output of

        ceph orch ls --format json

    and matching the value of the key service_id.

    Args:
        node:       System to be used to gather the service details.
        string:     The text that needs to be searched for the given pattern.

    Return:
        String after replacing the pattern with the service name
    """
    replaced_string = string
    names = re.findall("{service_name:(.+?)}", string)
    if not names:
        return replaced_string
    cmd = "ceph orch ls --format json"

    if "installer" in node.role:
        cmd = f"cephadm shell {cmd}"

    out, _ = exec_command(node, sudo=True, cmd=cmd)
    services_dict = json.loads(out)

    for name in names:
        for service_info in services_dict:
            service_id = service_info.get("service_id")
            if service_id and name == service_id:
                pattern = "{service_name:" + name + "}"
                replaced_string = re.sub(
                    pattern, service_info["service_name"], replaced_string
                )

    return replaced_string


def translate_to_daemon_id(node, string: str) -> str:
    """
    Return the given string after replacing the service id with it's daemon id.

    If the given string contains {daemon_id:<value>} then this substring is replaced
    with the Daemon Id that starts with the given <value>.

    Args:
        node:       System to be used to gather the information.
        string:     The ID of the service that needs to be translated.

    Return:
        String after replacing the pattern with the daemon id.
    """
    replaced_string = string
    ids_ = re.findall("{daemon_id:(.+?)}", string)
    if not ids_:
        return replaced_string
    cmd = "ceph orch ps --format json"

    if "installer" in node.role:
        cmd = f"cephadm shell {cmd}"

    out, _ = exec_command(node, sudo=True, cmd=cmd)
    daemon_details = json.loads(out)

    for id_ in ids_:
        for daemon in daemon_details:
            if daemon["daemon_id"].startswith(id_):
                pattern = "{daemon_id:" + id_ + "}"
                replaced_string = re.sub(pattern, daemon["daemon_id"], replaced_string)

    return replaced_string


def get_systemctl_service_name(node, service_name, command):
    """
    Return the systemctl service name matching the service_name parameter given.

    Args:
        node:       System to be used to gather the information.
        service_name:     The string pattern to be used to match the service name.
        command:        Command to be executed.

    Return:
        command after adding the service name.
    """
    cmd = f"systemctl list-units --type=service | grep ceph | grep {service_name}"
    out, err = exec_command(node, sudo=True, cmd=cmd)
    service_full_name = out.split()[0].strip()
    return f"{command} {service_full_name}"


def run(ceph_cluster, **kwargs: Any) -> int:
    """
    Test module for executing a sequence of commands using the CephAdm shell interface.

    The provided Ceph object is used to determine the installer node for executing the
    provided list of commands.

    Args:
        ceph_cluster:   The participating Ceph cluster object
        kwargs:         Supported key value pairs for the key config are
                        cmd             | Depreciated - command to be executed
                        idx             | Default 0, index to be used
                        sudo            | Execute in the privileged mode
                        cephadm         | Default False, use CephADM shell
                        commands        | List of commands to be executed
                        check_status    | Default True, if disabled does not raise error
                        role            | The role of the node to be used for exec
                        service_name    | The service on which the given systemctl command needs to be executed
    Returns:
        0 - on success
        1 - on failure

    Raises:
        CommandError

    Example:
        - test:
            abort-on-fail: true
            config:
                check_status: true
                commands:
                  - "radosgw-admin zone create --rgw-zone south"
                  - "radosgw-admin realm create --rgw-realm india"
            desc: Execute radosgw-admin commands for zone and
            module: exec.py

    Notes:
        {node:node1} -> would be replaced with the shortname of node1
    """
    LOG.info("Executing command")
    config = kwargs["config"]
    build = config.get("build", config.get("rhbuild"))
    timeout = config.get("timeout")
    command = config.get("cmd")
    service_name = config.get("service_name", "")
    LOG.warning("Usage of cmd is deprecated instead use commands.")
    commands = [command] if command else config["commands"]
    cephadm = config.get("cephadm", False)

    check_status = config.get("check_status", True)
    sudo = config.get("sudo", False)
    long_running = config.get("long_running", False)

    role = config.get("role", "client")
    if cephadm:
        role = "installer"

    nodes = ceph_cluster.get_nodes(role=role)
    node = nodes[config.get("idx", 0)]

    for cmd in commands:
        # Reconstruct user values into dynamic values based on run instance
        cmd = translate_to_hostname(ceph_cluster, cmd)
        cmd = translate_to_ip(kwargs["ceph_cluster_dict"], ceph_cluster.name, cmd)

        if cephadm:
            cmd = f"cephadm shell -- {cmd}"
            sudo = True
            long_running = False

        # Reconstruct cephadm specific lookups
        if not build.startswith("4"):
            cmd = translate_to_daemon_id(node, cmd)
            cmd = translate_to_service_name(node, cmd)

        if "systemctl" in cmd:
            cmd = get_systemctl_service_name(node, service_name, cmd)

        try:
            out, err = exec_command(
                node,
                sudo=sudo,
                cmd=cmd,
                check_ec=check_status,
                long_running=long_running,
                timeout=timeout,
            )
            LOG.info(out)
        except BaseException as be:
            LOG.error(be)
            return 1

    return 0
