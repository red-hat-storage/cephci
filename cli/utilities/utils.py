def get_disk_list(node, expr=None):
    """Get disks used by ceph cluster

    Args:
        node (ceph): Ceph node object
        expr (str): Expression to filter disks
    """
    cmd = "lsblk -ln -o name"
    if expr:
        cmd += f" | grep {expr}"
    stdout, _ = node.exec_command(sudo=True, cmd=cmd)

    return stdout


def get_running_containers(node, expr=None):
    """Get containers running on nodes

    Args:
        node (ceph): Ceph node object
        expr (str): Expression to filter containers
    """
    cmd = "podman ps --noheading"
    if expr:
        cmd += f" --filter {expr}"

    stdout, _ = node.exec_command(sudo=True, cmd=cmd)

    return stdout


def config_dict_to_string(data):
    """
    Convert the provided data to a string of optional arguments.

    Args:
        data (dict):  Key/value pairs that are CLI optional arguments
    """
    rtn = ""
    for key, value in data.items():
        if isinstance(value, bool) and value is False:
            continue

        rtn += f" -{key}" if len(key) == 1 else f" --{key}"

        if not isinstance(value, bool):
            rtn += f" {value}"

    return rtn
