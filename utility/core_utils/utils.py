import copy
import datetime
import logging
import re
import time
from copy import deepcopy

import yaml
from fabric.tasks import execute
from src.utilities.metadata import Metadata
from src.utilities.retry import RetryMixin

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("startup.log", mode="a")
file_handler.setLevel(logging.ERROR)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

import core.cli.fabfile as fabfile
from core.utilities import core_utils


def get_module(**kw):
    """Returns required module.
    Args:
        kw: Key value pair of method arguments
        Example::
            Supported keys:
                service(str): name of the service.
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    module = kw.get("service")
    return globals()[module]


def check_and_raise_fabfile_command_failure(output, parallel=False, fail=True, msg=""):
    """Checks and raises Exception for fabfile commands.
    Args:
        output          : output of the command that is dict if parallel is set
                          else an object of fabric.operations._AttributeString
        parallel (bool) : parallel true or false
        fail (bool)     : to throw exception based on fail status
        msg (str)       : message to be thrown
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run

    Raises:
        Exception
    """
    if output is None:
        return True
    if not parallel:
        output = {"ip": output}
    for ip, result in output.items():
        status = result.__dict__
        if status.get("failed") == fail:
            return False
    return True


def rgw_period_update(**kw):
    """Updates the period.
    Args:
        None
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    cmd = "radosgw-admin period update --commit"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def service_list(**kw):
    """To list a service in systemd.
    Args:
        None
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    cmd = "systemctl list-units | grep ceph"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def service_start(**kw):
    """To start a service in systemd.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service")
    env_config = kw.get("env_config")
    cmd = f"systemctl start {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, env_config)
    except Exception as e:
        logger.error(f"{e}")


def service_enable(**kw):
    """Enable a service, without starting it.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service")
    env_config = kw.get("env_config")
    cmd = f"systemctl enable {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, env_config)
    except Exception as e:
        logger.error(f"{e}")


def service_stop(**kw):
    """To stop a service in systemd.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service")
    env_config = kw.get("env_config")
    cmd = f"systemctl stop {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, env_config)
    except Exception as e:
        logger.error(f"{e}")


def service_disable(**kw):
    """To disable the service from starting automatically.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service_name")
    cmd = f"systemctl disable {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def service_restart(**kw):
    """To restart a running service.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service_name")
    cmd = f"systemctl restart {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def service_status(**kw):
    """To display a status of a running service.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                service_name(str): takes the service name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    service_name = kw.get("service")
    env_config = kw.get("env_config")
    cmd = f"systemctl is-active {service_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=env_config)
    except Exception as e:
        logger.error(f"{e}")


def set_firewall_config(**kw):
    """Shows the whole firewall configuration.
    Args:
            zone(str): name of the zone(optional)
            add_port(str): name of the port(optional)
            list-all(bool): to list all the relevant information for the default zone(optional)
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    env_conf = kw.pop("env_config")
    cmd = "firewall-cmd" + core_utils.build_cmd_args(kw=kw)
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=env_conf)
    except Exception as e:
        logger.error(f"{e}")


def write_to_file(**kw):
    """writes in a specified file.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                path(str): path to the file
                content(str): content to be written in the file
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    path = kw.get("path", "~")
    content = kw.get("content")
    env_config = kw.get("env_config")
    logger.info("Writing the content to the file")
    try:
        return fabfile.run_command(f"echo -e '{content}' >> {path}", env_config)
    except Exception as e:
        logger.error(f"{e}")


def create_file(**kw):
    """
    This method is used to create an empty file.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          file_name(str): Name of the file.
          path(str): Path at which the file need to be created.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run.
    """
    kw = kw.get("kw")
    file_name = kw.get("file_name")
    path = kw.get("path", "~")
    env_config = kw.get("env_config")
    cmd = "touch" + f" {path}/{file_name}"
    logger.info(f"Running command {cmd}")
    try:
        output = fabfile.run_command(cmd, config=env_config)
        check_and_raise_fabfile_command_failure(
            output=output, parallel=env_config.get("parallel")
        )
        return output
    except Exception as e:
        logger.error(f"{e}")


def wait_for_status(func):
    """
    This method is used to wait till we get the given string in command output.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
        This method is used to wait till we get the desired string in the output.
        timeout(int): value of timeout in seconds
        interval(int): value of interval.
        cmd(str): Command that needs to be checked.
        status_string(str): Desired string which the command should contain in the output.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run.

    """

    def inner_func(**kw):
        timeout = kw.get("timeout")
        cmd = kw.get("cmd")
        status_string = kw.get("status_string")
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        try:
            while end_time > datetime.datetime.now():
                out, rc = execute(fabfile.run_cmd_in_serial, cmd)
                if status_string in out:
                    return True
                func(**kw)
        except Exception as e:
            logger.error(f"{e}")

    return inner_func


@wait_for_status
def wait_status(**kw):
    interval = kw.get("interval")
    sleep(interval)


def retry(kw):
    """
    This method is used to wait till we get the given string in command output.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
        This method is used to wait till we get the desired string in the output.
        runner(Runner): object of Runner.
        retry_args(str): arguments of the retry.

    Returns:
      output of the command.

    """
    out = RetryMixin().retry(kw)
    return out


def transfer_remote_files(**kw):
    """
    This method is used to transfer files between two remote machines we get the given string in command output.
    Args:
      args(dict): Key/value pairs that needs to be provided to the installer.

    Returns:
      None.
    """
    kw = kw.get("kw")
    source_role = kw.get("source_role")
    source_path = kw.get("source_path")
    destination_path = kw.get("destination_path")
    destination_role = kw.get("destination_role")
    cluster_name = kw.get("cluster_name")
    env_config = kw.get("env_config")
    metadata = Metadata()
    ceph = metadata.__dict__.get("ceph")
    ceph_role = ceph.ceph_role
    source_ips = ceph_role.get_ips_by_role(source_role, cluster_name)
    destination_ips = ceph_role.get_ips_by_role(destination_role, cluster_name)
    env_config["hosts"] = source_ips
    out = fabfile.run_command(f"cat {source_path}", env_config)
    out = list(out.values())[0]
    env_config["hosts"] = destination_ips
    fabfile.run_command(f"echo -e '{out}' >> {destination_path}", env_config)
    return


def wait_till(**kw):
    """
    This method is used to wait till we get the given string in command output.
    Args:
      args(dict): Key/value pairs that needs to be provided to the installer.

    Returns:
      None.
    """
    return


def read_from_file(**kw):
    """
    This method is used to read the contents of the file.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          file_name(str): Name of the file.
          path(str): Path at which the file need to be created.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    file_name = kw.get("file_name")
    path = kw.get("path", "~")
    env_config = kw.get("env_config")
    cmd = "cat" + f" {path}/{file_name}"
    logger.info(f"Running command {cmd}")
    try:
        out = fabfile.run_command(cmd, config=env_config)
        return out
    except Exception as e:
        logger.error(f"{e}")


def edit_permission(**kw):
    """
    This method is used to edit the permissions of a specified file.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          file_name(str): Name of the file.
          path(str): Path at which the file need to be created.
          permissions: The file permissions in Linux are the following three types:
                         [Type]      [Absolute Mode equivalent]
                       read    (r)        -> 4
                       write   (w)        -> 2
                       execute (x)        -> 1

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    permissions = kw.get("permissions")
    file_name = kw.get("file_name")
    path = kw.get("path", "~")
    env_config = kw.get("env_config")
    cmd = "chmod" + f" {permissions} {path}/{file_name}"
    logger.info(f"Running command {cmd}")
    try:
        output = fabfile.run_command(cmd, config=env_config)
        check_and_raise_fabfile_command_failure(
            output=output, parallel=env_config.get("parallel")
        )
        return output
    except Exception as e:
        logger.error(f"{e}")


def copy_content(**kw):
    """
    This method is used to copy the file(source) into another file(destination).
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          source(str): File that needs to be copied.
          destination(str): File into which the source file needs to copied.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    source = kw.get("source")
    destination = kw.get("destination")
    env_config = kw.get("env_config")
    cmd = "cp" + f" {source} {destination}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=env_config)
    except Exception as e:
        logger.error(f"{e}")


def sleep(**kw):
    """
    This method is used to pause execution for a given period.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          number(seconds): Amount of time the command need to be paused.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    number = kw.get("sleep_time")
    logger.info(f"Pausing the execution for {number} seconds")
    try:
        time.sleep(number)
    except Exception as e:
        logger.error(f"{e}")


def create_partition(**kw):
    """
    This method is used for creating and deleting partitions.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          device_name(str): name of the hardisk.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    device_name = kw.get("device_name")
    cmd = "parted /dev/" + f"{device_name}" + " mklabel msdos"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def create_directory(**kw):
    """
    This method is used to create a new directory.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          name(str): name of the directory.
          path(str): Path at which the file need to be created.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    name = kw.get("name", "")
    path = kw.get("path", "~")
    env_config = kw.get("env_config")
    cmd = "mkdir" f" {path}/{name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=env_config)
    except Exception as e:
        logger.error(f"{e}")


def mount_system(**kw):
    """
    This method is used to mount a filesystem.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          device(str): Name of the device.
          mount_point(str): Path for the mount point.
    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    device_name = kw.get("device_name")
    mount_point = kw.get("mount_point")
    cmd = "mount" + f" {device_name} {mount_point}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def format_partition(**kw):
    """
    This method is used to format a block storage device with a specific file system.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          fstype(str): It is the type of the filesystem.
          device(str): It is the target UNIX device to write the filesystem data to.

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    fstype = kw.get("fstype")
    device = kw.get("device")
    cmd = "mkfs -t" + f" {fstype} {device}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def check_for_status(**kw):
    """
    This method is used to check the status of a particular service
    Ex:- ceph, rbd.
    Args:
      kw(dict): Key/value pairs that needs to be provided to the installer
      Example:
        Supported keys:
          service(str): Name of the service.
          target_pool_name(str): name of the target pool(optional).
          source_image_name(str): name of the source image(optional).

    Returns:
      Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run.
    """
    kw = kw.get("kw")
    kw_copy = deepcopy(kw)
    service = kw_copy.pop("service")
    cmd_args = core_utils.build_cmd_args(kw=kw_copy)
    cmd = f"{service}" + " status" + cmd_args
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def write_to_remote_file(**kw):
    """writes in a file present at remote location.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                content(str): content to be written in the file
                path(str): path to the file
    """
    kw = kw.get("kw")
    source_path = kw.get("source_path")
    destination_path = kw.get("destination_path")
    env_config = kw.get("env_config")
    logger.info("Writing in a file at remote location")
    try:
        cmd = f"cat {source_path} > {destination_path}"
        output = fabfile.run_command(cmd, config=env_config)
        check_and_raise_fabfile_command_failure(
            output=output, parallel=env_config.get("parallel")
        )
        return output
    except Exception as e:
        logger.error(f"{e}")


def check_cluster_running(**kw):
    """checks if the ceph cluster is running.
    Args:
            None
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    cmd = "ceph status"
    logger.info(f"Running command {cmd}")
    out, err = fabfile.run_command(cmd, config=kw.get("env_config"))
    health_status = out.get("health")
    osds = out.get("services").get("osd")
    pgs = out.get("services").get("pgs")
    match = re.search(r"(\d+)\s+osds:\s+(\d+)\s+up,\s+(\d+)\s+in", osds)
    up_osds = int(match.group(2))
    in_osds = int(match.group(3))
    if health_status == "HEALTH_OK":
        if up_osds != in_osds:
            logger.error("osd is down")
            raise Exception("osd is down")
        if "active+clean" not in pgs:
            logger.error("pg in stale/stuck state")
            raise Exception("pg in stale/stuck state")
    else:
        logger.error("Ceph is unhealthy")
        raise Exception("Ceph is unhealthy")


def check_for_root_privileges(**kw):
    """Checks if the user has root privileges.
    Args:
            None
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    cmd = "sudo --list"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def configure_sudo(**kw):
    """Configure sudo access for the newly created user.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                user_name(str): takes the user name
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    user_name = kw.get("user_name")
    cmd = f"cat << EOF >/etc/sudoers.d/{user_name}"
    logger.info(f"Running command {cmd}")
    try:
        return fabfile.run_command(cmd, config=kw.get("env_config"))
    except Exception as e:
        logger.error(f"{e}")


def equals(**kw):
    """
    To check whether content is consistent in two different files.
    Args:
            kw(dict): Key/value pairs that needs to be provided to the installer
        Example::
        Supported keys:
                source_content(str): takes the content from source file
                destination_content(str): takes the content from destination file
    Returns:
        Dict(str)
        A mapping of host strings to the given task’s return value for that host’s execution run
    """
    kw = kw.get("kw")
    source_content = kw.get("source_content")
    destination_content = kw.get("destination_content")
    try:
        if source_content == destination_content:
            print("It is consistent")
    except Exception as e:
        logger.error(f"{e}")


def print_vm_nodes_for_resuming(clusters_dict):
    """
    Stores node details to resume execution from running suites.
    Args:
        clusters_dict(dict): Dictionary containing cluster details

    Returns:
        None
    """
    logger.info(clusters_dict)
    clusters = dict()
    clusters["cloud_type"] = clusters_dict.get("cloud_type")
    clusters["vm_nodes"] = {}
    for node_name, node_dict in clusters_dict.get("vm_nodes").items():
        node_obj = node_dict.pop("node_obj")
        clusters["vm_nodes"][node_name] = copy.deepcopy(node_dict)
        clusters["vm_nodes"][node_name]["uuid"] = node_obj.id
        node_dict["node_obj"] = node_obj
    output_file = open("src/models/outputs/clusters_dict.yaml", "w+")
    yaml.dump(clusters, output_file, allow_unicode=True, default_flow_style=False)
    logger.info(clusters)


def copy_admin_sshkeys(**kw):
    """
    Distribute cephadm generated public key to all nodes in the list.
    Args:
        None

    Returns:
        None
    """
    kw = kw.get("kw")
    core_utils.copy_admin_sshkeys(kw=kw)
