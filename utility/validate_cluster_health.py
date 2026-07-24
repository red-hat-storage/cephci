import json
import pickle

import paramiko
import yaml
from docopt import docopt

from ceph.ceph import CommandFailed
from utility.log import Log
from utility.retry import retry

LOG = Log(__name__)


doc = """
Standard script to Check the cluster health state

    Usage:
        validate_cluster_health.py --conf <str> [--pickle]
        validate_cluster_health.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        --conf <str>       conffile path if baremetal or pickle file path
        --pickle           boolean
    """
cluster_status_cmds = [
    "ceph -s -f json",
    "ceph health detail -f json",
    "ceph orch ps -f json",
    "ceph osd tree -f json",
]


@retry(CommandFailed, tries=3, delay=10)
def get_cluster_status(client):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(client["ip"], username="root", password=client["root_password"])
    for cmd in cluster_status_cmds:
        stdin, stdout, stderr = ssh_client.exec_command(cmd)
        cluster_stat = json.loads(stdout.read())
        LOG.info(
            f"\n{cmd}:\n--------------------------------------------------------------------------------------------- "
            f"\n{json.dumps(cluster_stat, indent=4)}"
            f"\n--------------------------------------------------------------------------------------------- "
        )

    stdin, stdout, stderr = ssh_client.exec_command("ceph -s -f json")
    health_status = json.loads(stdout.read())["health"]["status"]
    LOG.info(health_status)
    if health_status not in ["HEALTH_OK", "HEALTH_WARN"]:
        raise CommandFailed("Health is not OK")
    print("Cluster is in a Healthy state")
    return health_status


def load_cluster_config(config):
    cluster_dict = None
    with open(config, "rb") as f:
        cluster_dict = pickle.load(f)

    for _, cluster in cluster_dict.items():
        for node in cluster:
            node.reconnect()

    return cluster_dict


def load_cluster_from_file(config):
    with open(config, "r") as file:
        yaml_data = file.read()

    parsed_data = yaml.safe_load(yaml_data)
    return parsed_data


def get_nodes_with_role(parsed_data, target_role="client"):
    nodes_with_target_role = []

    # Check if 'globals' key exists and is a list
    if "globals" in parsed_data and isinstance(parsed_data["globals"], list):
        for item in parsed_data["globals"]:
            if isinstance(item, dict):
                # Check if 'ceph-cluster' key exists and is a dictionary
                ceph_cluster = item.get("ceph-cluster")
                if ceph_cluster and isinstance(ceph_cluster, dict):
                    # Check if 'nodes' key exists and is a list
                    nodes = ceph_cluster.get("nodes")
                    if nodes and isinstance(nodes, list):
                        for node in nodes:
                            # Check if 'role' key exists and contains the target role
                            if "role" in node and target_role in node["role"]:
                                nodes_with_target_role.append(node)

    return nodes_with_target_role


if __name__ == "__main__":
    arguments = docopt(doc)
    conf = arguments.get("--conf")

    if arguments.get("--pickle"):
        cluster_dict = load_cluster_config(conf)
        client = cluster_dict.get("ceph").get_ceph_objects("client")[0]
        client_dict = {"ip": client.ip_address, "root_password": client.root_password}
    else:
        parsed_data = load_cluster_from_file(conf)
        client_nodes = get_nodes_with_role(parsed_data)
        if not client_nodes:
            raise ValueError("No nodes with the specified role found.")
        client_dict = {
            "ip": client_nodes[0].get("ip"),
            "root_password": client_nodes[0].get("root_password"),
        }
    try:
        get_cluster_status(client_dict)
    except Exception as e:
        print(e)
