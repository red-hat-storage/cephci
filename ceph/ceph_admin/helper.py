"""
Contains helper functions that can used across the module.
"""
import logging

LOG = logging.getLogger()


def get_cluster_state(cls, commands=[]):
    """
    fetch cluster state using commands provided along
    with the default set of commands

    - ceph status
    - ceph orch ls -f json-pretty
    - ceph orch ps -f json-pretty
    - ceph health detail -f yaml

    Args:
        cls: ceph.ceph_admin instance with shell access
        commands: list of commands

    """
    __CLUSTER_STATE_COMMANDS = [
        "ceph status",
        "ceph orch ls -f yaml",
        "ceph orch ps -f json-pretty",
        "ceph health detail -f yaml",
    ]

    __CLUSTER_STATE_COMMANDS.extend(commands)

    for cmd in __CLUSTER_STATE_COMMANDS:
        out, err = cls.shell(args=[cmd])
        LOG.info("STDOUT:\n %s" % out)
        LOG.error("STDERR:\n %s" % err)
