from copy import deepcopy
from random import random
from string import ascii_uppercase, ascii_lowercase, digits
from time import sleep
import yaml
from docopt import docopt

from cli.cluster.node import Node
from cli.exceptions import ConfigError
from cli.exceptions import TestSetupFailure
from utility.log import Log
from cephci.cephci.cleanup import cleanup

log = Log(__name__)
cloud = None
configs = None
node_conf = None
cluster_conf = None


"""ToDo : are these params sufficient
cloud - [openstack|ibmc|baremetal], node-conf, cluster-conf, credentials, log-dir, log-level
"""

doc = """
Utility to provision cluster from the given cloud

"""


def _set_log(level):
    log.logger.setLevel(level.upper())


def _get_configs(creds):
    with open(creds, "r") as _stream:
        try:
            _yaml = yaml.safe_load(_stream)
        except yaml.YAMLError:
            raise ConfigError(f"Invalid credentials file '{creds}'")

    try:
        return _yaml["credentials"]
    except KeyError:
        raise ConfigError("Config file doesn't include credentials for Cloud provider")


def _validate_conf():
    # checks node_conf and perform validation
    """
    Validates the global conf by checking unique ID for nodes.
    Rules:
    1. If id is provided, then it will take the highest precedence.
    2. If id not provided, then framework will add the node IDs based on its appearance index.
    Note : ID should not follow node{i} which will conflict with the dynamic ID generator. like node1,node2..etc
    """
    log.info("Validate global configuration file")
    for cluster in cluster_conf.get("globals"):
        nodes = cluster.get("ceph-cluster").get("nodes", [])
        if not nodes:
            nodes_id = []
            ceph_cluster = cluster.get("ceph-cluster")
            for node in sorted(ceph_cluster.keys()):
                if not node.startswith("node"):
                    continue
                nodes_id.append(ceph_cluster[node].get("id") or f"{node}")
        else:
            nodes_id = [
                node.get("id") or f"node{idx + 1}" for idx, node in enumerate(nodes)
            ]
        log.info(f"List of Node IDs : {nodes_id}")
        if not (len(nodes_id) == len(set(nodes_id))):
            raise TestSetupFailure(
                f"Nodes does not have Unique Identifiers, "
                f"Please set the unique node Ids in global conf {_validate_conf.__doc__}"
            )


def _validate_image():
    """Validate the global conf for image_name.
    This module validates the global conf, by checking if the user has provided image-name.
    If image-name key is provided, then it should have the specific image specified along with it.
    Note:
        for psi based, "openstack" is the key, followed by required image name, similarly, "ibmc" for ibmc env.
        This check is required with the introduction of multi-version ceph clients.
    Args:
        conf (dict): cluster global configuration provided
        cloud_type (str): underlying deployment infrastructure used
    example::
      node7:
        image-name:
          openstack: RHEL-8.6.0-x86_64-ga-latest
          ibmc: rhel-86-server-released
    """
    log.info("Validate global configuration file")
    if cloud == "baremetal":
        return
    for cluster in cluster_conf.get("globals"):
        nodes = cluster.get("ceph-cluster")
        for node in nodes.keys():
            if "node" in node:
                attrs = nodes[node].get("image-name")
                if attrs:
                    log.info(f"Image attributes provided for node {node} : {attrs} ")
                    if cloud not in attrs.keys():
                        raise TestSetupFailure(
                            f"Node {node} has image-name provided , but no corresponding image given for {cloud} "
                            f"Please set the {cloud}:image in global conf {_validate_image.__doc__}"
                        )


def provision(**args):
    _cloud = Node(name="", cloud="openstack")

    # Validate the configs and image
    _validate_conf()
    _validate_image()

    # No checks are required as cleanup takes care of openshift and ibmc cases
    # fetch the patter and pass to cleanup
    prefix = args.get("--pattern")
    cleanup(prefix)

    # Generate the run_id
    run_id = "".join(random.choices(ascii_lowercase + ascii_uppercase + digits, k=6))
    ceph_cluster_dict = {}
    clients = []

    log.info("Creating osp instances")
    for cluster in cluster_conf.get("globals"):
        ceph_vmnodes = _cloud._create(
            cluster,
            node_conf,
            creds,
            run_id,
            prefix,
        )

        ceph_nodes = []
        root_password = None
        for node in ceph_vmnodes.values():
            look_for_key = False
            private_key_path = ""

            # these will be redirected to openstack.methods
            if cloud == "openstack":
                private_ip = _cloud.private_ips()
            # ToDo: Add for other cloudtypes

            # ToDo: How to handle the CephNode class and Ceph Class calls here
            ceph = CephNode(
                username="cephuser",
                password="cephuser",
                root_password="passwd" if not root_password else root_password,
                look_for_key=look_for_key,
                private_key_path=private_key_path,
                root_login=node.root_login,
                role=node.role,
                no_of_volumes=node.no_of_volumes,
                ip_address=node.ip_address,
                subnet=node.subnet,
                private_ip=private_ip,
                hostname=node.hostname,
                ceph_vmnode=node,
                id=node.id,
            )
            ceph_nodes.append(ceph)

        cluster_name = cluster.get("ceph-cluster").get("name", "ceph")

        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)

        # Set the network attributes of the cluster
        # ToDo: Support other providers like openstack and IBM-C
        if "baremetal" in cloud:
            ceph_cluster_dict[cluster_name].networks = deepcopy(
                cluster.get("ceph-cluster", {}).get("networks", {})
            )

    # TODO: refactor cluster dict to cluster list
    log.info("Done creating osp instances")
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    sleep(15)

    # ToDo: can we exclude the rp_logger?
    rp_logger = None
    for cluster_name, cluster in ceph_cluster_dict.items():
        for instance in cluster:
            try:
                instance.connect()
            except BaseException:
                if rp_logger:
                    rp_logger.finish_test_item(status="FAILED")
                raise

    if rp_logger:
        rp_logger.finish_test_item(status="PASSED")

    return ceph_cluster_dict, clients


if __name__ == "__main__":
    args = docopt(doc)

    log_level = args.get("--log-level")
    cloud = args.get("--cloud-type")
    creds = args.get("--cloud-creds")
    cluster_conf = args.get("--cluster-conf")
    node_conf = args.get("--node-conf")

    _set_log(log_level)

    configs = _get_configs(creds)

    provision(**args)
