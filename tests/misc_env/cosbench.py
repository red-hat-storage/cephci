"""
This module contains the workflows for installing and configuring COSBench - Cloud
Object Storage Benchmark tool on the provided nodes. It supports

    - deploying COSBench on the provided systems
    - Configuring COSBench based on the given nodes

Sample test script

    - test:
        abort-on-fail: true
        config:
          controllers:
            - node6
          drivers:
            count: 1
            hosts:
              - node6
              - node7
        desc: Start COS Bench controller and driver
        module: cosbench.py
        name: deploy cosbench
"""
from json import loads
from typing import Dict, List

from jinja2 import Template

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.utils import get_nodes_by_ids
from utility.log import Log

LOG = Log(__name__)
RPMS = ["java-1.8.0-openjdk", "unzip", "nmap-ncat"]
CB_VER = "0.4.2.c4"
CB_FILE = f"{CB_VER}.zip"
CB_URL = (
    f"https://github.com/intel-cloud/cosbench/releases/download/v{CB_VER}/{CB_FILE}"
)

CTRL_CONF = """[controller]
name = CephCI COS
log_level = INFO
log_file = log/controller.log
archive_dir = archive
drivers = {{ data|length }}

{% for item in data %}
[driver{{ loop.index }}]
name = {{ item.name }}-{{ loop.index }}
url = http://{{ item.ip_address }}:{{ item.port }}/driver
{%- endfor %}
"""


def install(nodes: List[CephNode]) -> None:
    """
    Installs COS Bench along with its pre-requisites.

    Args:
        nodes (list):   The list of nodes on which the packages are installed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    pre_req_pkgs = " ".join(RPMS)
    for node in nodes:
        try:
            node.exec_command(cmd="ls -l /opt/cosbench")
            continue
        except CommandFailed:
            pass
        node.exec_command(sudo=True, cmd=f"yum install -y {pre_req_pkgs}")
        node.exec_command(cmd=f"curl -L {CB_URL} -O")
        node.exec_command(cmd=f"unzip {CB_FILE}")
        node.exec_command(cmd=f"sudo mv {CB_VER} /opt/cosbench")
        node.exec_command(cmd="chmod +x /opt/cosbench/*.sh")

    LOG.info("Successfully install COSBench!!!")


def enable_ports(node: CephNode, port: int = 18088) -> None:
    """
    Opens the required firewall ports on the COSBench role type nodes.

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
        sudo=True, cmd=f"firewall-cmd --zone public --permanent --port {port}/tcp"
    )


def config(node: CephNode, data: List) -> None:
    """
    Writes the COS Bench controller configuration file.

    Args:
        node:   The node that is designated to be a COS controller
        data:   A list of dictionaries having driver details (name & ip_address)
    Returns:
        None
    Raises:
        CommandFailed
    """
    LOG.info("Generating the COS Bench controller file.")
    templ = Template(CTRL_CONF)
    conf = templ.render(data=data)

    conf_file = node.remote_file(
        file_name="/opt/cosbench/conf/controller.conf", file_mode="w"
    )
    conf_file.write(conf)
    conf_file.flush()


def execute_cosbench_script(nodes: List[CephNode], script: str) -> None:
    """Executes the given script on all provided nodes.

    Args:
        nodes (list):   The list of nodes on which the script needs to be executed.
        script (str):   The script file that needs to be executed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    LOG.debug(f"Executing COS Bench script: {script}")
    for node in nodes:
        node.exec_command(cmd=f"cd /opt/cosbench && ./{script}")


def get_or_create_user(node: CephNode) -> Dict:
    """Creates or retrieves a RADOS user.

    Returns:
         Dictionary holding the keys user, access_key & secret_key
    """
    LOG.debug("Get or Create cosbench01 user using radosgw-admin.")
    user = "cosbench01"
    try:
        out, err = node.exec_command(cmd=f"radosgw-admin user info --uid {user}")
        out = loads(out)
        return out["keys"][0]
    except CommandFailed:
        out, err = node.exec_command(
            cmd=f"radosgw-admin user create --uid {user} --display-name {user}"
            f" --email {user}@noreply.com"
        )
        out = loads(out)
        return out["keys"][0]


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that executes the set of workflows.

    Here, Cloud Object Store Benchmark tool (COSBench) is installed on the nodes in the
    cluster having the following roles

        - cosbench-controller

    Args:
        ceph_cluster:   Cluster participating in the test.

    Returns:
        0 on Success and 1 on Failure.
    """
    LOG.info("Being COSBench deploy and configuration workflow.")
    client = ceph_cluster.get_nodes(role="client")[0]
    controllers = get_nodes_by_ids(ceph_cluster, kwargs["config"]["controllers"])
    drivers = get_nodes_by_ids(ceph_cluster, kwargs["config"]["drivers"]["hosts"])

    try:
        install(controllers)
        for ctrl in controllers:
            enable_ports(ctrl, port=19088)

        install(drivers)
        data = list()
        driver_count = kwargs["config"]["drivers"].get("count", 1)
        for driver in drivers:
            for i in range(driver_count):
                port = 18088 + 100 * i
                enable_ports(driver, port)
                data.append(
                    {
                        "name": driver.shortname,
                        "ip_address": driver.ip_address,
                        "port": port,
                    }
                )

        config(controllers[0], data=data)

        execute_cosbench_script(drivers, script=f"start-driver.sh {driver_count}")
        execute_cosbench_script(controllers, script="start-controller.sh")

        get_or_create_user(client)
    except BaseException as be:  # noqa
        LOG.error(be)
        return 1

    LOG.info("Successfully deployed COSBench!!!")
    return 0
