import os
import re

from utility.log import Log

log = Log(__name__)

CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"


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
