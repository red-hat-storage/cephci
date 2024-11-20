#! /usr/bin/env python3

# -*- code: utf-8 -*-

from logging import getLogger

from cluster_conf import collect_conf, write_output
from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.cluster.node import Node
from cli.cluster.volume import Volume
from cli.exceptions import ConfigError
from cli.utilities.utils import load_config

# Set logging env
LOG = getLogger(__name__)

# Set default configs
DEFAULT_VMSIZE = "ci.standard.medium"
DEFAULT_IMAGE = "{}.0-x86_64-ga-latest"

doc = """
Utility to provision cluster on cloud

    Usage:
        cephci/provision.py --cloud-type <CLOUD>
            (--global-conf <YAML>)
            (--platform <STR>)
            [--prefix <STR>]
            [--owner <STR>]
            [--cluster-conf <YAML>]
            [--image <STR>]
            [--vmsize <STR>]
            [--network <STR>...]
            [--config <YAML>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/provision.py --help

    Options:
        -h --help               Help
        --cloud-type <CLOUD>    Cloud type [openstack|ibmc|baremetal]
        --global-conf <YAML>    Global config file with node details
        --platform <STR>        Node OS Type & Version
        --prefix <STR>          Resource name prefix
        --owner <STR>           Baremetal node owner
        --image <STR>           Cloud images to be used for node
        --network <STR>         Cloud network to be attached to node
        --vmsize <STR>          Cloud image flavor
        --cluster-conf <YAML>   Cluster config file path
        --config <YAML>         Config file with cloud credentials
        --log-level <LOG>       Log level for log utility Default: DEBUG
        --log-dir <PATH>        Log directory for logs
"""


def provision_osp_volume(name, volcount, volsize):
    """Provision volumes on OSP"""
    # Provision volumes to be attached to node
    volumes = []
    for i in range(volcount):
        # Create volume object
        volname = f"{name}-vol-{i}"
        volume = Volume(volname, cloud)

        # Provision volume
        LOG.info(f"Provisioning volume '{volname}'")
        volume.create(volsize, timeout, interval)
        LOG.info(
            f"Volume '{volume.name}' with id '{volume.id}' provisioned successfully"
        )

        volumes.append(volname)

    return volumes


def provision_node(name, cloud, config, node, timeout=600, interval=10):
    """Provision node on cloud"""
    # Provision OSP volumes
    if str(cloud) == "openstack":
        volcount, volsize = node.get("no-of-volumes", 0), node.get("disk-size", 0)
        config["volumes"] = provision_osp_volume(name, volcount, volsize)

    # Create node object
    node = Node(name, cloud)

    # Provision node
    node.create(timeout=timeout, interval=interval, **config)
    LOG.info(f"Node '{node.name}' with id '{node.id}' provisioned successfully")

    # Attach volumes provisioned to node
    if str(cloud) == "openstack":
        node.attach_volume(config.get("volumes"))

    return True


def provision(
    cloud,
    prefix,
    clusters,
    platform,
    image=None,
    vmsize=None,
    networks=None,
    timeout=600,
    interval=10,
):
    """Provision cluster with global configs"""
    # Get platform details
    _type, _version = platform.split("-")
    configs = {
        "os_type": _type,
        "os_version": _version,
        "image": image,
        "size": vmsize,
    }

    # Create cluster configs
    for _c in clusters:
        cluster = _c.get("ceph-cluster")
        name = cluster.get("name")

        LOG.info(f"Provisioning cluster '{name}'")
        for key in cluster.keys():
            if "node" not in key:
                continue

            # Create node name
            node_name = ""
            if str(cloud) == "openstack":
                node_name = f"{name}-{prefix}-{key}"
            elif str(cloud) == "baremetal":
                node_name = cluster.get(key).get("hostname")

            # Get node details
            node = cluster.get(key)
            if not networks:
                configs["networks"] = node.get("networks")

            provision_node(node_name, cloud, configs, node, timeout, interval)

        LOG.info(f"Cluster '{name}' provisioned successfully")


if __name__ == "__main__":
    # Set configs
    cloud_configs, global_configs = {}, []

    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cloud = args.get("--cloud-type")
    global_conf = args.get("--global-conf")
    platform = args.get("--platform")
    prefix = args.get("--prefix")
    owner = args.get("--owner")
    network = args.get("--network")
    image = args.get("--image")
    vmsize = args.get("--vmsize")
    cluster_conf = args.get("--cluster-conf")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Read configuration for cloud
    get_configs(config)

    # Check for mandatory OSP paramters
    if cloud == "openstack":
        if not prefix:
            raise ConfigError("Mandatory parameter prefix are not provided")

        vmsize = vmsize if vmsize else DEFAULT_VMSIZE
        image = image if image else DEFAULT_IMAGE.format(platform.upper())

    # Check for mandatory baremetal paramters
    elif cloud == "baremetal":
        if not owner:
            raise ConfigError("Mandatory owner are not provided")

        # Set cloud owner
        cloud_configs["owner"] = owner

    # Read cloud configurations
    cloud_configs.update(get_cloud_credentials(cloud))

    # Read cluster configurations
    global_configs = load_config(global_conf).get("globals")

    # Get timeout and interval
    timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")
    interval = int(timeout / retry) if timeout and retry else None

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    # Provision cluster
    provision(
        cloud,
        prefix,
        global_configs,
        platform,
        image,
        vmsize,
        network,
        timeout,
        interval,
    )

    # Collect cluster config
    data = collect_conf(cloud, prefix, global_configs)

    # Log cluster configuration
    LOG.info(f"\nCluster configuration details -\n{data}")

    # write output to yaml
    write_output(cluster_conf, data) if cluster_conf else None
