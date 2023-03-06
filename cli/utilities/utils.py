import os
import re

from ceph.waiter import WaitUntil
from utility.log import Log

log = Log(__name__)

CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
RHBUILD_PATTERN = r"(\d\.\d)-(rhel-\d)"


def get_disk_list(node, expr=None, **kw):
    """Get disks used by ceph cluster

    Args:
        node (ceph): ceph node object
        expr (str): expression to filter disks
        kw (dict): execute command parameters
    """
    cmd = "lsblk -ln -o name"
    if expr:
        cmd += f" | grep {expr}"

    return node.exec_command(cmd=cmd, **kw)


def get_running_containers(node, expr=None, **kw):
    """Get containers running on nodes

    Args:
        node (ceph): ceph node object
        expr (str): expression to filter containers
        kw (dict): execute command parameters
    """
    cmd = "podman ps --noheading"
    if expr:
        cmd += f" --filter {expr}"

    return node.exec_command(cmd=cmd, **kw)


def os_major_version(node, **kw):
    """Get OS major release version
    Args:
        node (ceph): Ceph node object
    """
    cmd = r"cat /etc/os-release | tr -dc '0-9.'| cut -d \. -f1"
    return node.exec_command(cmd=cmd, **kw)


def config_dict_to_string(data):
    """Convert the provided data to a string of optional arguments.

    Args:
        data (dict):  key/value pairs that are CLI optional arguments
    """
    rtn = ""
    for key, value in data.items():
        if isinstance(value, bool) and value is False:
            continue

        rtn += f" -{key}" if len(key) == 1 else f" --{key}"

        if not isinstance(value, bool):
            rtn += f" {value}"

    return rtn


def get_node_ip(nodes, node_name):
    """Get IP address of node_name
    Args:
        node (ceph): ceph node object
        node_name (str): node name for ceph object
    """
    for node in nodes:
        if (
            node.shortname == node_name
            or node.shortname.endswith(node_name)
            or f"{node_name}-" in node.shortname
        ):
            return node.ip_address

    return False


def get_custom_repo_url(base_url, cloud_type="openstack"):
    """Add the given custom repo on every node part of the cluster.

    Args:
        cloud_type (str): cloudtype (openstack|ibmc)
        base_url (str): base URL of repository
    """
    if base_url.endswith(".repo"):
        return base_url

    if not base_url.endswith("/"):
        base_url += "/"

    if cloud_type == "ibmc":
        base_url += "Tools"
    else:
        base_url += "compose/Tools/x86_64/os/"

    return base_url


def get_nodes_by_ids(nodes, ids):
    """
    Fetch nodes using provided substring of nodes

    Args:
        nodes (list|tuple): CephNode objects
        ids (list|tuple): node name list (eg., ['node1'])

    Returns:
        list of nodes identified by given ids
    """
    node_details = []
    for name in ids:
        node = get_node_by_id(nodes, name)
        if node:
            node_details.append(node)
    return node_details


def get_node_by_id(nodes, id):
    """
    Fetch node using provided node substring::
        As per the naming convention used at VM creation, where each node.shortname
        is framed with hyphen("-") separated string as below, please refer
        ceph.utils.create_ceph_nodes definition
            "ceph-<name>-node1-<roles>"
            name: RHOS-D username or provided --instances-name
            roles: roles attached to node in inventory file
        In this method we use hyphen("-") appended to node_name string to try fetch exact node.
        for example,
            "node1" ----> "node1-"
        Note: But however if node_name doesn't follow naming convention as mentioned in
        inventory, the first searched node will be returned.
        for example,
            "node" ----> it might return any node which matched first, like node11.
        return None, If this cluster has no nodes with this substring.

    Args:
        nodes: Cli obj containing node details
        id: node1        # try to use node<Id>

    Returns:
        node instance (CephVMNode)
    """
    for node in nodes:
        searches = re.findall(rf"{id}?\d*", node.hostname, re.IGNORECASE)
        for ele in searches:
            if ele == id:
                return node
    return None


def build_cmd_from_args(seperator="=", **kw):
    """This method checks from the dictionary the optional arguments
    if present it adds them in "cmd" and returns cmd.

    Args:
        seperator: the separator for parameters, '=' by default
        kw (dict) : takes a dictionary as an input.

    Returns:
        cmd (str): returns a command string.

    eg:
        Args:
            kw={"uid": "<uid>", "purge-keys": True, "purge-data": True}
            kw={"placement=": "<placement_groups>", "purge-data": True}
        Returns:
            " --uid <uid> --purge-keys --purge-data"
            " --placment=<placement_groups> --purge-data"
    """
    if not kw:
        return ""

    cmd = ""
    for k, v in kw.items():
        if v is True:
            cmd += f" {k}"
        else:
            if seperator and seperator in k:
                cmd += f" --{k}{v}"
            else:
                cmd += f" --{k} {v}"
    return cmd


def put_cephadm_ansible_playbook(
    node, playbook, cephadm_ansible_path=CEPHADM_ANSIBLE_PATH
):
    """Put playbook to cephadm ansible location.
    Args:
        playbook (str): Playbook need to be copied to cephadm ansible path
        cephadm_ansible_path (str): Path to where the playbook has to be copied
    """
    dst = os.path.join(cephadm_ansible_path, os.path.basename(playbook))

    node.upload_file(sudo=True, src=playbook, dst=dst)
    log.info(f"Uploaded playbook '{playbook}' to '{dst}' on node.")


def get_builds_by_rhbuild(rhbuild):
    """
    Get RHCS and RHEL version from rhbuild

    Args:
        rhbuild (str): build version
    """
    match = re.search(RHBUILD_PATTERN, rhbuild)
    if match:
        return match.group(1, 2)

    return


def verify_execution_status(out, cmd):
    """
    Helper method to verify whether the installation or upgrade happened
    on all the nodes specified
    """
    for node, result in out.items():
        if result != 0:
            log.error(f"Execution failed for '{cmd}' on '{node}'")
            return False
    return True


def set_selinux_mode(nodes, enforcing_mode):
    """
    Sets the selinux mode to the specified value and validate whether the selinux mode is set
    Args:
        nodes (ceph): ceph node objects
        enforcing_mode (str): enforcing/permissive
    """
    if enforcing_mode == "permissive":
        mode = "0"
    elif enforcing_mode == "enforcing":
        mode = "1"
    else:
        log.error(
            f"Only permissive and enforcing modes accepted, given {enforcing_mode}"
        )
        return False

    # Set selinux to the given mode
    for node in nodes:
        _, err = node.exec_command(cmd=f"setenforce {mode}", sudo=True)
        if err:
            log.error(
                f"Failed to set selinux mode to {enforcing_mode} on {node.hostname}"
            )
            return False

    # Verify the selinux mode is as expected
    for node in nodes:
        out, _ = node.exec_command(cmd="getenforce")
        if str(out.strip()).lower() != enforcing_mode:
            log.error(f"Setenforce failed on {node.hostname}")
            return False

    return True


def reboot_node(node):
    """
    Reboots a given node and waits till the reboot complete
    Args:
        node (ceph): Node to reboot

    Returns (bool): Based on reboot status
    """
    reboot_cmd = "sleep 3; /sbin/shutdown -r now 'Reboot triggered by CephCI'"
    node.exec_command(sudo=True, cmd=reboot_cmd, check_ec=False)
    # If service was removed, wait for a timeout to check whether its removed
    timeout, interval = 300, 10
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            node.reconnect()
            log.info(f"Node {node.hostname} reconnected after reboot")
            return True
        except Exception:
            log.warning(f"Node {node.hostname} is not back after reboot")
    if w.expired:
        return False


def get_service_id(node, service_name):
    """
    Returns the service id of a given service
    Args:
        node (ceph): Node to execute the cmd
        service_name: Service name

    Returns (str): Service ID
    """
    out, err = node.exec_command(cmd=f"systemctl --type=service | grep {service_name}")
    if err:
        return None
    return out.split(" ")[0]


def set_service_state(node, service_id, state):
    """
    Sets the service to given state using systemctl
    Args:
        node (ceph): Node to execute the cmd
        service_id: Service id
        state: state [start /stop/ restart]

    Returns (str): Service ID
    """
    _, err = node.exec_command(cmd=f"systemctl {state} {service_id}", sudo=True)
    if err:
        return False
    return True
