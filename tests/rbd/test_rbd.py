"""
Module that executes the external RBD test suites as stated by the user.

The purpose of this module is to execute the upstream test suite against the downstream
code. The test suites are available at

    https://github.com/ceph/ceph/tree/master/qa/workunits/rbd

This module will not install any pre-requisites of the repo.

This module returns 0 on success else 1.
"""
import json
import re
from time import sleep

from ceph.utils import get_nodes_by_ids
from utility.log import Log

log = Log(__name__)

TEST_REPO = "https://github.com/ceph/ceph.git"
SCRIPT_PATH = "qa/workunits/rbd"


def get_tag(node) -> str:
    """Get upstream tag that to be picked up for test.

    Args:
        node: ceph node object
    Returns:
        str: tag to be used for upstream script
    """
    version, err = node.exec_command(cmd="ceph -v")
    return "v" + re.search(r"[0-9]+(\.[0-9]+)+", version).group(0)


def one_time_setup(node, rhbuild, branch: str) -> None:
    """
    Installs the pre-requisites for executing the tests.

    Args:
        node:   The node object participating in the test
        branch: The branch that needs to be cloned
        rhbuild: specification of rhbuild. ex: 4.3-rhel-7
    """
    node.exec_command(
        cmd=f"sudo rm -rf ceph && git clone --branch {branch} --single-branch --depth 1 {TEST_REPO}"
    )
    os_ver = rhbuild.split("-")[-1]
    ceph_ver = rhbuild.split("-")[0]

    if os_ver == "7":
        node.exec_command(
            cmd="sed -i '49 a rbd feature disable testimg1 object-map fast-diff deep-flatten' "
            "ceph/qa/workunits/rbd/kernel.sh"
        )

    if "4." in ceph_ver:
        node.exec_command(
            cmd="sed -i 's/blocklist/blacklist/g' "
            "ceph/qa/workunits/rbd/krbd_exclusive_option.sh"
        )

    try:
        node.exec_command(cmd="rpm -qa | grep xmlstarlet")
        return
    except BaseException:  # noqa
        pass

    EPEL_RPM = (
        f"https://dl.fedoraproject.org/pub/epel/epel-release-latest-{os_ver}.noarch.rpm"
    )

    commands = [
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
    log.info("Running RBD Sanity tests.")

    config = kwargs["config"]
    script_dir = config["script_path"]
    script = config["script"]

    nodes = config.get("nodes", [])
    rhbuild = config.get("rhbuild")

    if nodes:
        nodes = get_nodes_by_ids(ceph_cluster, nodes)
    else:
        # By default, tests would be executed on a single client node
        nodes = [ceph_cluster.get_nodes(role="client")[0]]

    os_ver = rhbuild.split("-")[-1]
    if "4." in rhbuild and os_ver == "8":
        nodes[0].exec_command(
            cmd="sudo /usr/sbin/alternatives --set python /usr/bin/python3"
        )

    if rhbuild[0] > "4":
        out, err = nodes[0].exec_command(
            sudo=True, cmd="ceph config get mon mon_allow_pool_delete --format json"
        )

        if not json.loads(out):
            nodes[0].exec_command(
                sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
            )

    for node in nodes:
        branch = config.get("branch", get_tag(node))
        one_time_setup(node, rhbuild, branch=branch)

        cmd = f"cd ceph/{script_dir}; sudo bash {script}"
        if script == "*":
            cmd = f"cd ceph/{script_dir}; for test in $(ls); do sudo bash $test; done"

        node.exec_command(cmd=cmd, check_ec=True, timeout=1200)

    return 0
