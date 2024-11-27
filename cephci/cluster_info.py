import json
import os
import pickle
import re

import yaml
from docopt import docopt

from cli.cephadm.cephadm import CephAdm
from cli.utilities.packages import Rpm, SubscriptionManager
from cli.utilities.utils import (
    exec_command_on_container,
    get_container_images,
    get_disk_list,
    get_kernel_version,
    get_release_info,
    get_running_containers,
)
from utility.log import Log

log = Log(__name__)

CEPH_VAR_LOG_DIR = "/var/log/ceph"

doc = """
Utility to gather cluster information

    Usage:
       cephci/cluster_info.py (--reuse <FILE>)
           (--output <YAML>)
           (--ceph-logs <BOOL>)

       cephci/cluster_info.py --help

    Options:
       -h --help            Help
       --reuse <FILE>       Use the stored vm state for rerun
       --output <YAML>      Create file with cluster info collected
       --ceph-logs <BOOL>     Collect ceph logs? True or False.
"""


def _load_cluster_config(config):
    """Load cluster configration from Ceph CI object"""
    cluster = None
    with open(config, "rb") as f:
        cluster = pickle.load(f)

    [n.reconnect() for _, c in cluster.items() for n in c]

    return cluster


def get_node_details(node):
    """Gather node details"""
    kernel_ver, _ = get_kernel_version(sudo=True, node=node)
    release_info, _ = get_release_info(sudo=True, node=node)

    repos = SubscriptionManager(node).repos.list("enabled")
    repos = re.findall(r"Repo ID:(.*)(\s+)", repos)

    return {
        "Linux_kernel_version": kernel_ver.strip(),
        "RedHat_release_info": release_info.strip(),
        "Repos_enabled": [repo.strip() for repo, _ in repos],
    }


def get_cluster_details(node):
    """Gather cluster cluster details"""
    ceph, kw = CephAdm(node).ceph, {"format": "json"}
    details = [
        {"Host_list": json.loads(ceph.orch.host.ls(**kw))},
        {"Device_list": json.loads(ceph.orch.device.ls(**kw))},
    ]

    return details


def get_osd_host_disks(node):
    """Gather OSD host disks details"""
    disks, _ = get_disk_list(sudo=True, check_ec=False, node=node, expr="ceph-")
    if disks:
        disks = disks.strip().split("\n")

    return disks


def get_installed_packages(node):
    """Gether ceph installed package details"""
    packages = ["cephadm", "ansible", "cephadm-ansible", "podman"]
    rpm, installed = Rpm(node), []
    for pkg in packages:
        version = rpm.query(pkg)
        if version:
            installed.append(version)

    return installed


def get_container_images_details(node):
    """Gather container image details"""
    images, _ = get_container_images(
        sudo=True, node=node, format="{{.Repository}} {{.Tag}}"
    )

    if not images:
        return []

    details = []
    for image in images.strip().split("\n"):
        name, tag = image.split()
        details.append({"Name": name, "Tag": tag})

    return details


def get_container_details(node):
    """Gather container information"""
    ctrs, _ = get_running_containers(
        sudo=True, node=node, format="{{.ID}} {{.Image}} {{.Names}}"
    )
    if not ctrs:
        return []

    _info = []
    for ctr in ctrs.strip().split("\n"):
        id, image, name = ctr.split()
        details = {"ID": id, "Image": image, "Name": name}

        pkgs, _ = exec_command_on_container(
            sudo=True, check_ec=False, node=node, ctr=id, cmd="rpm -qa | grep ceph"
        )
        if pkgs:
            details["Packages"] = pkgs.strip().split("\n")

        _info.append(details)

    return _info


def gather_info(cluster):
    """Gather cluster configuration info"""
    info, nodes = {}, cluster.get_nodes()
    for node in nodes:
        _info = get_node_details(node)
        _info.update({"List_of_packages": get_installed_packages(node)})
        if node.role == "installer":
            _info.update({"Cluster_details": get_cluster_details(node)})

        if node.role != "client":
            _info.update({"List_of_images": get_container_images_details(node)})
            _info.update({"List_of_containers": get_container_details(node)})

        if node.role == "osd":
            _info.update({"List_of_host_ceph_disks": get_osd_host_disks(node)})

        info.update({node.hostname: _info})

    return info


def get_ceph_var_logs(cluster, log_dir):
    """
    This method is to download and store
    ceph cluster var logs into log directory.
    """
    download_dir = os.path.join(log_dir, "ceph_logs")
    os.makedirs(download_dir, exist_ok=True)
    for node in cluster.get_nodes():
        tar_file = f"{node.hostname}-cephlog.tar"
        node.exec_command(cmd=f"tar -cvzf {tar_file} {CEPH_VAR_LOG_DIR}", sudo=True)

        node.download_file(
            src=tar_file,
            dst=os.path.join(download_dir, tar_file),
            sudo=True,
        )
        log.info(f"Downloading {tar_file} from {node.hostname} to {download_dir}")


def write_output(data, output):
    """Write output data to stream"""
    with open(output, "w") as fp:
        yaml.dump(data, fp, default_flow_style=False, indent=2)


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    config = args.get("--reuse")
    output = args.get("--output")
    ceph_logs = args.get("--ceph-logs")

    # Collect cluster information
    _dict, _info = _load_cluster_config(config), {}
    for name in _dict:
        if ceph_logs in ["true", True]:
            get_ceph_var_logs(_dict.get(name), output)

        _info = gather_info(_dict.get(name))

        # Write data to stream
        _output = os.path.join(output, f"cluster_info_{name}.yaml")
        write_output(_info, _output)
