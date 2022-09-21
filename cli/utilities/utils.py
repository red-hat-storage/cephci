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


def get_running_containers(node, expr=None, template=None, **kw):
    """Get containers running on nodes

    Args:
        node (ceph): ceph node object
        expr (str): expression to filter containers
        template (str): template to print result
        kw (dict): execute command parameters
    """
    cmd = "podman ps --noheading"
    if expr:
        cmd += f" --filter {expr}"

    if template:
        cmd += f" --format {template}"

    return node.exec_command(cmd=cmd, **kw)


def podman_exec(node, cmd, ctr_id, user=None, workdir=None, **kw):
    """Execute podman exec with iteractive mode

    Args:
        node (ceph): Ceph node object
        ctr_id (str): Container ID
        cmd (str): Command to be executed inside user
        user (str): Sets the username or UID used
        workdir (str): Working directory inside the container.
    """
    execute = f"podman exec {ctr_id} "
    if user:
        execute += f"--user {user} "
    if workdir:
        execute += f"--workdir {workdir} "

    execute += cmd
    return node.exec_command(cmd=execute, **kw)


def podman_copy_to_localhost(node, ctr_id, source, destination, **kw):
    """Copy source directory from container to localhost

    Args:
        node (ceph): Ceph node object
        ctr_id (str): Container ID
        source (str): Location from container to copy file
        destination (str): Location from localhost to copy file
    """
    cmd = f"podman cp {ctr_id}:{source} {destination}"
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
