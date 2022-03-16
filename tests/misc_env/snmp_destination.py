# Module to setup snmp destination node and test
from typing import Any

from ceph.ceph_admin.helper import GenerateServiceSpec
from ceph.utils import get_node_by_id
from utility.log import Log

LOG = Log(__name__)


def configure_firewalld(node):
    """
    Module for configuring firewalld on snmp destination node.

    Args:
        node: node on which firewalld to be configured

    """
    commands = [
        "yum install -y firewalld",
        "systemctl start firewalld.service",
        "firewall-cmd --zone=public --add-port=162/udp",
        "firewall-cmd --zone=public --add-port=162/udp --permanent",
    ]
    LOG.info(commands)
    for cmd in commands:
        out, err = node.exec_command(sudo=True, cmd=cmd)
        LOG.info(out)


def configure_snmptrapd(node, ceph_cluster, config):
    """
    Module for configuring mib and snmptrapd on snmp destination node.

    Args:
        node:           node to be configured
        ceph_cluster:   The participating Ceph cluster object
        config:         configuration

    """
    spec_cls = GenerateServiceSpec(
        node=node, cluster=ceph_cluster, specs=config["specs"]
    )
    conf_filename = spec_cls.create_snmp_conf_file()
    LOG.info(f"conf_file_name:{conf_filename}")
    conf_copy = f"cp {conf_filename} /root/snmptrapd/snmptrapd_auth.conf"
    commands = [
        "curl -o CEPH_MIB.txt -L https://raw.githubusercontent.com/ceph/ceph/master/monitoring/snmp/CEPH-MIB.txt",
        "cp CEPH_MIB.txt /usr/share/snmp/mibs/",
        "mkdir /root/snmptrapd/",
        conf_copy,
    ]
    LOG.info(commands)
    for cmd in commands:
        out, err = node.exec_command(sudo=True, cmd=cmd)
        LOG.info(out)


def run(ceph_cluster, **kwargs: Any) -> int:
    """
    Test module for setting up snmp destination node.

    Args:
        ceph_cluster:   The participating Ceph cluster object
        kwargs:         Supported key value pairs for the key config are
                        node            | node on which commands to be executed
                        cmd             | Set of commands to be executed
    Returns:
        0 - on success
        1 - on failure

    Raises:
        CommandError

    """
    config = kwargs["config"]
    nodes = config.get("node")
    node = get_node_by_id(ceph_cluster, nodes)
    LOG.info(f"node: {node.shortname}")
    try:
        configure_firewalld(node)
        configure_snmptrapd(node, ceph_cluster, config)
    except BaseException as be:
        LOG.error(be)
        return 1
    return 0
