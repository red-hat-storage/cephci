import os
import random
import re
import string
from datetime import datetime
from json import loads
from subprocess import PIPE, Popen
from threading import Thread
from time import sleep

import yaml

from ceph.waiter import WaitUntil
from cli.exceptions import OperationFailedError
from cli.utilities.containers import Container
from utility.log import Log

log = Log(__name__)

SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"

CEPHADM_ANSIBLE_PATH = "/usr/share/cephadm-ansible"
RHBUILD_PATTERN = r"(\d\.\d)-(rhel-\d)"

CEPH_PUB_KEY = "/etc/ceph/ceph.pub"


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


def get_container_images(node, name=None, tag=None, expr=None, format=None, **kw):
    """Get container images on node

    Args:
        node (ceph): ceph node object
        name (str): name of the image
        tag (str): tag expression
        expr (str): expression to filter containers
        kw (dict): execute command parameters
    """
    cmd = "podman images --noheading"

    if name:
        cmd += f" {name}"

    if name and tag:
        cmd += f":{tag}"

    if expr:
        cmd += f" --filter {expr}"

    if format:
        cmd += f' --format "{format}"'

    return node.exec_command(cmd=cmd, **kw)


def get_running_containers(node, expr=None, format=None, **kw):
    """Get containers running on nodes

    Args:
        node (ceph): ceph node object
        expr (str): expression to filter containers
        kw (dict): execute command parameters
    """
    cmd = "podman ps --noheading"
    if expr:
        cmd += f" --filter {expr}"

    if format:
        cmd += f' --format "{format}"'

    return node.exec_command(cmd=cmd, **kw)


def stop_container(node, container_id):
    """
    Stops a given container
    Args:
        node (ceph): ceph node object
        container_id: id/name of the container
    """
    cmd = f"podman stop {container_id}"

    return node.exec_command(cmd=cmd, sudo=True)


def start_container(node, container_id):
    """
    Starts a given container
    Args:
        node (ceph): ceph node object
        container_id: id/name of the container
    """
    cmd = f"podman start {container_id}"

    return node.exec_command(cmd=cmd, sudo=True)


def restart_container(node, container_id):
    """
    Restarts a given container
    Args:
        node (ceph): ceph node object
        container_id: id/name of the container
    """
    cmd = f"podman restart {container_id}"

    return node.exec_command(cmd=cmd, sudo=True)


def exec_command_on_container(node, ctr, cmd, **kw):
    """Execute command on container

    Args:
        node (ceph): ceph node object
        ctr (str): container id
        cmd (str): command to be expected
    """
    cmd = f'podman exec {ctr} /bin/sh -c "{cmd}"'
    return node.exec_command(cmd=cmd, **kw)


def os_major_version(node, **kw):
    """Get OS major release version
    Args:
        node (ceph): Ceph node object
    """
    cmd = r"cat /etc/os-release | tr -dc '0-9.'| cut -d \. -f1"
    version, _ = node.exec_command(cmd=cmd, **kw)
    return version.strip()


def get_release_info(node, **kw):
    """Get release info from node

    Args:
        node (ceph): ceph node object
        kw (dict): execute command parameters
    """
    cmd = "cat /etc/redhat-release"
    return node.exec_command(cmd=cmd, **kw)


def get_kernel_version(node, **kw):
    """Get kernel version from node

    Args:
        node (ceph): ceph node object
        kw (dict): execute command parameters
    """
    cmd = "uname -a"
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
            cmd += f" --{k}"
        elif isinstance(v, list):
            for val in v:
                cmd += build_cmd_from_args(**val)
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


def enable_fips_mode(node):
    """Enable FIPS mode on node

    Args:
        node (CephNode): Ceph Node Object
    """
    # Set FIPS commands
    enable_fips = "fips-mode-setup --enable"
    finish_fips_setup = "fips-finish-install --complete"

    # Enable FIPS mode
    out, err = node.exec_command(cmd=enable_fips, sudo=True)
    if "FIPS mode will be enabled." not in out:
        log.error(f"Failed to setup FIPS mode config. Error -\n{err}")
        return False

    # Finish FIPS mode
    _, err = node.exec_command(cmd=finish_fips_setup, sudo=True)
    if err:
        log.error(f"Failed to setup enable mode. Error -\n{err}")
        return False

    return True


def is_fips_mode_enabled(node):
    """Check for FIPS mode on node

    Args:
        node (CephNode): Ceph Node Object
    """
    # Check FIPS command
    check_fips_setup = "fips-mode-setup --check"

    # Check for FIPS status
    out, _ = node.exec_command(cmd=check_fips_setup, sudo=True)
    if "FIPS mode is enabled" not in out:
        log.error(f"FIPS mode is disabled on node '{node.hostname}'")
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

    # Add sleep for 5 seconds as node will be rebooted after 3 seconds
    log.info("Sleeping for 5 sec to get reboot command executed ...")
    sleep(5)

    # If service was removed, wait for a timeout to check whether its removed
    log.info(f"Checking for node '{node.hostname}' connection")
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
        service_name: Service name (mon/osd/mgr etc)

    Returns (str/list): Service ID /ID's
    """
    out, err = node.exec_command(
        cmd=f"systemctl --type=service | grep {service_name} | awk '{{print $1}}'"
    )
    if err:
        return None

    service_ids = []
    for item in out.strip().split("\n"):
        service_ids.append(item.strip())
    return service_ids


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


def get_service_state(node, service):
    """
    Returns the state of a given service
    Args:
        node (ceph): Node to execute the cmd
        service: Service id / name

    Returns (str): Status of service (active/inactive)
    """
    try:
        res, _ = node.exec_command(cmd=f"systemctl is-active {service}", sudo=True)
    except Exception:
        return "inactive"
    return res


def bring_node_offline(node, interface="eth0", timeout=120):
    """
    Brings a node offlibe by making network interface down for a defined time
        Args:
            node (ceph): Node at which the interface has to be bought down
            interface (str): Node interface name ens3/eth0 etc.
            timeout (int): Time duration (in secs) for which network has to
                           be down
        Returns (Thread): Thread object, None if failed
        Example:
            bring_node_offline(mon_node, installer_node, timout=120)
    """
    cmd = f"ifconfig {interface} down;sleep {timeout};ifconfig {interface} up"
    thread = Thread(target=(lambda: node.exec_command(cmd=cmd, sudo=True)))
    thread.start()

    # Adding a sleep for network interface update to take place
    sleep(3)

    if is_node_online(node):
        log.error(
            f"{node.hostname} is online even after bringing the n/w interface down"
        )
        log.info("Waiting for the interface update command to complete")
        thread.join()
        return None

    log.info(f"{node.hostname} is made offline for {timeout} seconds")
    return thread


def is_node_online(node):
    """
    Checks whether the given node is online. Uses ping to verify the node status
    Args:
        node (ceph): Node to be checked for online/offline status

    Returns (bool): Based on if the node is online or not

    """
    cmd = ["ping", node.ip_address, "-c1"]
    ping_proc = Popen(cmd, stdout=PIPE)
    _, _ = ping_proc.communicate()
    if not ping_proc.returncode:
        log.info("{} is UP".format(node.ip_address))
        return True
    log.error(f"{node.hostname} is DOWN, Ping failed")
    return False


def get_lvm_on_osd_container(container_id, node, format="json"):
    """
    Returns the lvm list for the given container id
    Args:
        container_id (str): ID of the container
        node (ceph): Node to execute the cmd

    Returns (str): output of the command
    """
    cmd = f"ceph-volume lvm list --format {format}"
    out, _ = exec_command_on_container(node=node, ctr=container_id, cmd=cmd, sudo=True)
    return loads(out)


def get_disk_devlinks(node, disk):
    """
    Parses the devlink data to identify the by-id and by-path values
    Args:
        node (ceph): Node to execute cmd
        disk (str): disk nam. e.g: /dev/vdb

    Returns (list, list): by-id and by-path values
    """

    cmd = f"udevadm info --query=property {disk} | grep DEVLINKS"
    out, _ = node.exec_command(cmd=cmd)
    by_id = []
    by_path = []
    for entry in out.split(" "):
        path = entry.split("=")[-1]
        if "by-id" in path:
            by_id.append(path)
        elif "by-path" in path:
            by_path.append(path)
    return by_id, by_path


def get_process_id(node, process):
    """
    Returns the process of the given process
    Args:
        node (ceph): Node where the cmd has to be executed
        process (str): Name of the process
    """
    cmd = f"pidof {process}"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    return out


def check_coredump_generated(node, coredump_path, created_after):
    """
    Checks if the coredump file is generated in the given location
    Args:
        node (ceph): Node where coredump has to be checked
        coredump_path (str): Path where coredump is found
        created_after (datetime): Time from when coredump has to be checked.
    """
    # Get the latest file in the cordump folder
    cmd = f"ls -Art {coredump_path} | tail -n 1"
    file_name, _ = node.exec_command(cmd=cmd, sudo=True)

    # Get the file creation time
    cmd = f"stat -c '%w' {file_name}"
    created_time, _ = node.exec_command(cmd=cmd, sudo=True)

    # Remove timezone and fractional seconds from time
    # E.g : => 2023-06-16 10:15:38.186183240 +0530
    created_time = created_time.split(".")[0]
    created_time = datetime.strptime(created_time, "%Y-%m-%d %H:%M:%S")

    # Verify if the file is created after the given time
    if created_time > created_after:
        return False
    return True


def create_files(client, mount_point, file_count):
    """
    Create files
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/urandom of={mount_point}/file{i} bs=1 count=1",
            )
        except Exception:
            raise OperationFailedError(f"failed to create file file{i}")


def perform_lookups(client, mount_point, num_files):
    """
    Perform lookups
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        num_files (int): total file count
    """
    for _ in range(num_files):
        try:
            log.info(
                client.exec_command(
                    sudo=True,
                    cmd=f"ls -laRt {mount_point}/",
                )
            )
        except FileNotFoundError as e:
            error_message = str(e)
            if "No such file or directory" not in error_message:
                raise OperationFailedError("failed to perform lookups")
            log.warning(f"Ignoring error: {error_message}")
        except Exception:
            raise OperationFailedError("failed to perform lookups")


def change_ownership(client, mount_point, file_count, user):
    """
    Perform lookups
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        num_files (int): total file count
        user (str): user name
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"chown {user} {mount_point}/file{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to change ownership for file{i}")


def change_permission(client, mount_point, file_count, permissions):
    """
    Perform lookups
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        num_files (int): total file count
        permissions (str): file permissions
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"chmod {permissions} {mount_point}/file{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to change permission for file{i}")


def generate_random_string(**kw):
    """
    Generates a random string and returns it

    Args:
        kw: {
            "len": 20
        }

    Returns:
        The generated string
    """
    length = kw.get("len", 5)
    temp_str = "".join([random.choice(string.ascii_letters) for _ in range(length)])
    return temp_str


def get_file_owner(filepath, clients):
    """
    Get file owner
    Args:
        filepath (str): file path
        clients (ceph): Client nodes
    """
    out = clients[0].exec_command(sudo=True, cmd=f"ls -n {filepath}")
    uid = int(out[0].split()[2])
    gid = int(out[0].split()[3])

    # uid and gid for rootuser
    if uid == 0 and gid == 0:
        log.info(f"The file '{filepath}' is created by the root user.")
        return "rootuser"
    # uid and gid for squashuser
    elif uid == 4294967294 and gid == 4294967294:
        log.info(f"The file '{filepath}' is created by the squashed user.")
        return "squashuser"
    else:
        log.info(
            f"The file '{filepath}' is created by an unknown user with UID: {uid} and GID: {gid}."
        )
        return None


def remove_files(client, mount_point, file_count):
    """
    Create files
    Args:
        clients (ceph): Client nodes
        mount_point (str): mount path
        file_count (int): total file count
    """
    for i in range(1, file_count + 1):
        try:
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {mount_point}/file{i}",
            )
        except Exception:
            raise OperationFailedError(f"failed to remove file{i}")


def get_ip_from_node(node):
    """
    Returns the list of ip addresses assigned to the given node
    Args:
        node (ceph): Ceph node
    """
    pattern = r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"
    cmd = "ip addr"
    out = node.exec_command(cmd=cmd, sudo=True)
    return re.findall(pattern=pattern, string=out[0])


def copy_ceph_sshkey_to_host(installer, node, user="root", key=CEPH_PUB_KEY):
    """Copy ceph ssh key to host

    Args:
        installer (CephInstallerNode): Ceph installer node object
        node (CephNode): Ceph Node object to copy ceph key
        user (str): User name
        key (str): Ceph key path
    """
    # Copy ceph key to host
    installer.exec_command(sudo=True, cmd=SSH_COPYID.format(key, user, node.hostname))


def kill_process(node, pid_to_kill):
    """
    Kills the process using process id
    Args:
        node (ceph): Ceph node
        pid_to_kill (str/list): Pid to kill
    """
    pids = pid_to_kill if isinstance(pid_to_kill, list) else [pid_to_kill]
    pids = " ".join(pids)
    cmd = f"kill -s SIGSEGV {pids}"
    node.exec_command(cmd=cmd, sudo=True)


def get_all_running_pids(node, process):
    """
    Returns the pid(s) of the given process
    Args:
        node (ceph): Ceph node
        process (str): Process name (rgw, mon, mds)
    """
    cmd = f"pgrep {process}"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    pids = [s.strip() for s in out.split("\n") if s]
    return pids


def get_pid_limit(node, service):
    """
    Gets the pids limit of a given service
    Args:
        node (CephNode): Ceph node where the service is running
        service (str): Name of the service (rgw, mon, mds)
    """
    # Identify the process name of the given service
    out = Container(node).ps(filter={"name": service})[0]
    process_name = out.split("\n")[1]
    process_name = process_name.split(" ")[0].strip()

    # Check the pids-limit for identified service container
    inspect_data = Container(node).inspect(image=process_name)[0]
    inspect_data = yaml.safe_load(inspect_data)[0]
    pid_limit = [
        entry
        for entry in inspect_data["Config"]["CreateCommand"]
        if "--pids-limit" in entry
    ]
    if not pid_limit:
        return None
    pid_limit = re.findall(r"-?\d+", pid_limit[0])[0]
    return pid_limit
