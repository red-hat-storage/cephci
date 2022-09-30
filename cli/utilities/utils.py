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


def config_dict_to_string(data):
    """Convert the provided data to a string of optional arguments.

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
