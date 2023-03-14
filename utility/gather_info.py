import json
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

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
    utilities/gather_info.py --reuse FILE
    utilities/gather_info.py --reuse FILE --output YAML

 Options:
    --reuse <FILE>                    Use the stored vm state for rerun
    -o --output <YAML>                Create file with cluster info collected
"""


def get_node_details(node):
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
    ceph, kw = CephAdm(node).ceph, {"format": "json"}
    details = [
        {"Host_list": json.loads(ceph.orch.host.ls(**kw))},
        {"Device_list": json.loads(ceph.orch.device.ls(**kw))},
    ]

    return details


def get_osd_host_disks(node):
    disks, _ = get_disk_list(sudo=True, check_ec=False, node=node, expr="ceph-")
    if disks:
        disks = disks.strip().split("\n")

    return []


def get_installed_packages(node):
    packages = ["cephadm", "ansible", "cephadm-ansible", "podman"]
    rpm, installed = Rpm(node), []
    for pkg in packages:
        version = rpm.query(pkg)
        if version:
            installed.append(version)

    return installed


def get_container_images_details(node):
    images, _ = get_container_images(
        sudo=True, node=node, format="{{.Repository}} {{.Tag}}"
    )

    if not images:
        return []

    image_details = []
    for image in images.strip().split("\n"):
        name, tag = image.split()
        image_details.append({"Name": name, "Tag": tag})

    return image_details


def get_container_details(node):
    ctrs, _ = get_running_containers(
        sudo=True, node=node, format="{{.ID}} {{.Image}} {{.Names}}"
    )
    if not ctrs:
        return []
    ctrs_info = []
    for ctr in ctrs.strip().split("\n"):
        id, image, name = ctr.split()
        details = {"ID": id, "Image": image, "Name": name}

        pkgs, _ = exec_command_on_container(
            sudo=True, check_ec=False, node=node, ctr=id, cmd="rpm -qa | grep ceph"
        )
        if pkgs:
            details["Packages"] = pkgs.strip().split("\n")

        ctrs_info.append(details)

    return ctrs_info


def gather_info(cluster):
    info, nodes = {}, cluster.get_nodes()
    for node in nodes:
        node_info = get_node_details(node)
        node_info.update({"List_of_packages": get_installed_packages(node)})
        if node.role == "installer":
            node_info.update({"Cluster_details": get_cluster_details(node)})

        if node.role != "client":
            node_info.update({"List_of_images": get_container_images_details(node)})
            node_info.update({"List_of_containers": get_container_details(node)})

        if node.role == "osd":
            node_info.update({"List_of_host_ceph_disks": get_osd_host_disks(node)})

        info.update({node.hostname: node_info})

    return info


def load_cluster_config(config):
    cluster_dict = None
    with open(config, "rb") as f:
        cluster_dict = pickle.load(f)

    for _, cluster in cluster_dict.items():
        for node in cluster:
            node.reconnect()

    return cluster_dict


def write_output(data, output):
    if not output:
        print(data)
        return

    with open(output, "w") as fp:
        yaml.dump(data, fp, default_flow_style=False, indent=2)


if __name__ == "__main__":
    args = docopt(doc)
    config, output = args.get("--reuse"), args.get("--output")

    cluster_dict, cluster_info = load_cluster_config(config), {}
    for cluster_name in cluster_dict:
        cluster_info[cluster_name] = gather_info(cluster_dict.get(cluster_name))

    write_output(cluster_info, output)
