"""
This module contains the workflows for installing and configuring WARP - Cloud
Object Storage Benchmark tool on the provided nodes. It supports

    - deploying WARP on the provided systems
    - Configuring WARP clients on the given nodes

Sample test script

    - test:
        abort-on-fail: true
        config:
          warp_server:
            - node6
          warp_client:
            bucket_count: 5
            hosts:
              - node6
              - node7
        desc: Start WARP clients and server
        module: configure_warp.py
        name: Configure WARP
"""
from json import loads
from typing import Dict, List

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.utils import get_nodes_by_ids
from utility.log import Log

LOG = Log(__name__)
RPMS = ["firewalld"]
WR_FILE = "warp_Linux_x86_64.rpm"
WR_URL = f"https://github.com/minio/warp/releases/download/v0.6.8/{WR_FILE}"


def install(nodes: List[CephNode]) -> None:
    """
    Installs WARP along with its pre-requisites.

    Args:
        nodes (list):   The list of nodes on which the packages are installed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    pre_req_pkgs = " ".join(RPMS)
    for node in nodes:
        node.exec_command(sudo=True, cmd=f"yum install -y {pre_req_pkgs}")
        try:
            node.exec_command(cmd="ls -l /usr/bin/warp")
            continue
        except CommandFailed:
            pass
        node.exec_command(sudo=True, cmd=f"curl -L {WR_URL} -O")
        node.exec_command(sudo=True, cmd=f"rpm -ivh {WR_FILE}")

    LOG.info("Successfully installed WARP!!!")


def enable_ports(node: CephNode, port: int = 7001) -> None:
    """
    Opens the required firewall ports on the WARP role type nodes.

    Args:
        node (CephNode):    The list of nodes for which the port has to be opened.
        port (int):         The network port that needs to be opened

    Returns:
        None

    Raises:
        CommandFailed
    """
    LOG.debug("Opening the required network ports if firewall is configured.")

    try:
        out, err = node.exec_command(sudo=True, cmd="firewall-cmd --state")

        if out.lower() != "running":
            return
    except CommandFailed:
        LOG.debug(f"{node.shortname} has no firewall configuration.")
        return

    node.exec_command(
        sudo=True, cmd=f"firewall-cmd --zone public --permanent --add-port {port}/tcp"
    )


def start_warp_client(node: CephNode, port: int = 7001) -> None:
    """Executes the given script on all provided nodes.

    Args:
        nodes (list):   The list of nodes on which the script needs to be executed.
        script (str):   The script file that needs to be executed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    LOG.debug("Starting WARP client on client node")
    node.exec_command(
        cmd=f"nohup warp client {node.ip_address}:{port} > /dev/null 2>&1 &"
    )


def get_or_create_user(node: CephNode) -> Dict:
    """Creates or retrieves a RADOS user.

    Returns:
         Dictionary holding the keys user, access_key & secret_key
    """
    LOG.debug("Get or Create cosbench01 user using radosgw-admin.")
    user = "warp_usr"
    try:
        out, err = node.exec_command(cmd=f"sudo radosgw-admin user info --uid {user}")
        out = loads(out)
        return out["keys"][0]
    except CommandFailed:
        out, err = node.exec_command(
            cmd=f"sudo radosgw-admin user create --uid {user} --display-name {user}"
            f" --email {user}@noreply.com"
        )
        out = loads(out)
        return out["keys"][0]


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that executes the set of workflows.

    Here, Cloud Object Store Benchmark tool (WARP) is installed on the nodes in the
    cluster

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        0 on Success and 1 on Failure.
    """
    LOG.info("Being WARP deploy and configuration workflow.")
    client = ceph_cluster.get_nodes(role="installer")[0]
    warp_server = get_nodes_by_ids(ceph_cluster, kwargs["config"]["warp_server"])
    warp_clients = get_nodes_by_ids(
        ceph_cluster, kwargs["config"]["warp_client"]["hosts"]
    )

    try:
        client.exec_command(cmd="sudo yum install -y --nogpgcheck ceph-common")
        install(warp_server)
        install(warp_clients)
        get_or_create_user(client)
        driver_count = kwargs["config"]["warp_client"].get("bucket_count", 1)
        for client in warp_clients:
            for i in range(driver_count):
                port = 7001 + i
                enable_ports(client, port)
                start_warp_client(client, port)

    except BaseException as be:  # noqa
        LOG.error(be)
        return 1

    LOG.info("Successfully deployed WARP!!!")
    return 0
