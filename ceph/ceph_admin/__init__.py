"""
Ceph Administrator aka cephadm module is a configuration tool for Ceph cluster.

It allows the users to deploy and manage their Ceph cluster. It also supports all the
operations part of the cluster lifecycle.

Over here, we create a glue between the CLI and CephCI to allow the QE to write test
scenarios for verifying and validating cephadm.
"""
import logging
from typing import Dict

from .bootstrap import BootstrapMixin
from .shell import ShellMixin

logger = logging.getLogger(__name__)


class CephAdmin(BootstrapMixin, ShellMixin):
    """
    Ceph administrator base class which enables ceph pre-requisites
    and Inherits HostMixin and BootstrapMixin classes to support
    host and bootstrap operations respectively
    """

    TIMEOUT = 300

    direct_calls = ["bootstrap", "shell"]

    def __init__(self, cluster, **config):
        """
        Initialize Cephadm with ceph_cluster object

        Args:
            cluster: Ceph cluster object
            config: test data configuration

        config:
            base_url: ceph compose URL
            container_image: custom ceph container image
        """
        self.cluster = cluster
        self.config = config
        self.installer = self.cluster.get_ceph_object("installer")

    def read_cephadm_gen_pub_key(self):
        """
        Read cephadm generated public key.

        Arg:
            Installer node

        Returns:
            Public Key string
        """
        ceph_pub_key, _ = self.installer.exec_command(
            sudo=True, cmd="cat /etc/ceph/ceph.pub"
        )
        return ceph_pub_key.read().decode().strip()

    def distribute_cephadm_gen_pub_key(self, nodes=None):
        """
        Distribute cephadm generated public key to all nodes in the list.

        Args:
            nodes: node list to add ceph public key(default: None)
        """
        ceph_pub_key = self.read_cephadm_gen_pub_key()

        if nodes is None:
            nodes = self.cluster.get_nodes()

        nodes = nodes if isinstance(nodes, list) else [nodes]

        for each_node in nodes:
            each_node.exec_command(sudo=True, cmd="install -d -m 0700 /root/.ssh")
            keys_file = each_node.write_file(
                sudo=True, file_name="/root/.ssh/authorized_keys", file_mode="a"
            )
            keys_file.write(ceph_pub_key)
            keys_file.flush()
            each_node.exec_command(
                sudo=True, cmd="chmod 0600 /root/.ssh/authorized_keys"
            )

    def set_tool_repo(self):
        """Add the given repo on every node part of the cluster."""
        for node in self.cluster.get_nodes():
            node.exec_command(
                sudo=True,
                cmd="yum-config-manager --add-repo"
                " {}compose/Tools/x86_64/os/".format(self.config.get("base_url")),
            )

    def install(self, **kwargs: Dict) -> None:
        """
        Install the cephadm package in the installer node.

        Args:
          kwargs: Key/value pairs that needs to be provided to the installer

        Supported keys:
            Note: At present, they are prefixed with -- hence use long options
          upgrade: boolean # to upgrade cephadm RPM package
          gpgcheck: boolean

        """
        cmd = "yum install cephadm -y"

        if kwargs.get("nogpgcheck", True):
            cmd += " --nogpghceck"

        self.installer.exec_command(
            sudo=True,
            cmd="yum install cephadm -y --nogpgcheck",
            long_running=True,
        )

        if kwargs.get("upgrade", False):
            self.installer.exec_command(sudo=True, cmd="yum update metadata")
            self.installer.exec_command(sudo=True, cmd="yum update -y cephadm")

        self.installer.exec_command(cmd="rpm -qa | grep cephadm")
