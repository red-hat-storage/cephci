"""
Module that executes the external RBD test suites as stated by the user.

The purpose of this module is to execute the upstream test suite against the downstream
code. The test suites are available at

    https://github.com/ceph/ceph/tree/master/qa/workunits/rbd

This module will not install any pre-requisites of the repo.

This module returns 0 on success else 1.
"""
import logging
from time import sleep

from ceph.utils import get_nodes_by_ids

LOG = logging.getLogger(__name__)
TEST_REPO = "https://github.com/ceph/ceph.git"
SCRIPT_PATH = "qa/workunits/rbd"


def one_time_setup(node, rhbuild, branch: str) -> None:
    """
    Installs the pre-requisites for executing the tests.

    Args:
        node:   The node object participating in the test
        branch: The branch that needs to be cloned
        rhbuild: specification of rhbuild. ex: 4.3-rhel-7
    """

    os_ver = rhbuild.split("-")[-1]

    EPEL_RPM = (
        f"https://dl.fedoraproject.org/pub/epel/epel-release-latest-{os_ver}.noarch.rpm"
    )

    commands = [
        {"cmd": "sudo rm -rf ceph"},
        {"cmd": f"git clone --branch {branch} --single-branch --depth 1 {TEST_REPO}"},
        {"cmd": f"yum install -y {EPEL_RPM} --nogpgcheck", "sudo": True},
        {
            "cmd": "yum install -y xmlstarlet rbd-nbd qemu-img cryptsetup --nogpgcheck",
            "sudo": True,
        },
    ]
    for command in commands:
        node.exec_command(**command)

    # Blind sleep to ensure the Mon service has restarted.
    # TODO: Identify a way to check the service is running
    sleep(5)


def run(ceph_cluster, **kwargs) -> int:
    """
    Method that executes the external test suite.

    Args:
        ceph_cluster    The storage cluster participating in the test.
        kwargs          The supported keys are
                        config  contains the test configuration

    Returns:
        0 - Success
        1 - Failure
    """
    LOG.info("Running RBD Sanity tests.")

    config = kwargs["config"]
    script_dir = config["script_path"]
    script = config["script"]

    branch = config.get("branch", "pacific")
    nodes = config.get("nodes", [])
    rhbuild = config.get("rhbuild")

    if nodes:
        nodes = get_nodes_by_ids(ceph_cluster, nodes)
    else:
        # By default, tests would be executed on a single client node
        nodes = [ceph_cluster.get_nodes(role="client")[0]]

    if "5." in rhbuild:
        nodes[0].exec_command(cmd="ceph config set mon mon_allow_pool_delete true")
        nodes[0].exec_command(cmd="ceph orch restart mon")
    else:
        nodes[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        mon_nodes = ceph_cluster.get_nodes(role="mon")
        for ceph_mon in mon_nodes:
            ceph_mon.exec_command(
                sudo=True,
                cmd=f"systemctl restart ceph-mon@{ceph_mon.hostname}",
                long_running=True,
            )

    for node in nodes:
        one_time_setup(node, rhbuild, branch=branch)

        cmd = f"cd ceph/{script_dir}; sudo bash {script}"
        if script == "*":
            cmd = f"cd ceph/{script_dir}; for test in $(ls); do sudo bash $test; done"

        node.exec_command(cmd=cmd, check_ec=True, timeout=1200)

    return 0
