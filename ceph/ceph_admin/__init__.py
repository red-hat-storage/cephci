"""
Ceph Administrator aka cephadm module is a configuration tool for Ceph cluster.

It allows the users to deploy and manage their Ceph cluster. It also supports all the
operations part of the cluster lifecycle.

Over here, we create a glue between the CLI and CephCI to allow the QE to write test
scenarios for verifying and validating cephadm.
"""
from typing import Dict

from utility.log import Log

from .bootstrap import BootstrapMixin
from .shell import ShellMixin

logger = Log(__name__)


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
            cluster (Ceph.Ceph): Ceph cluster object
            config (Dict): test data configuration

        Example::

            config:
                base_url (Str): ceph compose URL
                container_image (Str): custom ceph container image
        """
        self.cluster = cluster
        self.config = config
        self.installer = self.cluster.get_ceph_object("installer")

    def read_cephadm_gen_pub_key(self, ssh_key_path=None):
        """
        Read cephadm generated public key.

        Arg:
            ssh_key_path ( Str ): custom ssh public key path

        Returns:
            Public Key string (Str)
        """
        path = ssh_key_path if ssh_key_path else "/etc/ceph/ceph.pub"
        ceph_pub_key, _ = self.installer.exec_command(sudo=True, cmd=f"cat {path}")
        return ceph_pub_key.strip()

    def distribute_cephadm_gen_pub_key(self, ssh_key_path=None, nodes=None):
        """
        Distribute cephadm generated public key to all nodes in the list.

        Args:
            ssh_key_path (Str): custom SSH ceph public key path (default: None)
            nodes (List): node list to add ceph public key (default: None)
        """
        ceph_pub_key = self.read_cephadm_gen_pub_key(ssh_key_path)

        # Add with new line, so avoiding append to earlier entry
        ceph_pub_key = f"\n{ceph_pub_key}"

        if nodes is None:
            nodes = self.cluster.get_nodes()

        nodes = nodes if isinstance(nodes, list) else [nodes]

        for each_node in nodes:
            each_node.exec_command(sudo=True, cmd="install -d -m 0700 /root/.ssh")
            keys_file = each_node.remote_file(
                sudo=True, file_name="/root/.ssh/authorized_keys", file_mode="a"
            )
            keys_file.write(ceph_pub_key)
            keys_file.flush()
            each_node.exec_command(
                sudo=True, cmd="chmod 0600 /root/.ssh/authorized_keys"
            )

    def set_tool_repo(self, repo=None):
        """
        Add the given repo on every node part of the cluster.

        Args:
            repo (Str): repository (default: None)

        """
        cloud_type = self.config.get("cloud-type", "openstack")
        logger.info(f"cloud type is {cloud_type}")
        hotfix_repo = self.config.get("hotfix_repo")
        if hotfix_repo:
            for node in self.cluster.get_nodes():
                logger.info(
                    "Adding hotfix repo {repo} to {sn}".format(
                        repo=hotfix_repo,
                        sn=node.shortname,
                    )
                )
                node.exec_command(
                    sudo=True,
                    cmd="curl -o /etc/yum.repos.d/rh_hotfix_repo.repo {repo}".format(
                        repo=hotfix_repo,
                    ),
                )
                node.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)
        else:
            base_url = self.config["base_url"]
            if not base_url.endswith("/"):
                base_url += "/"
            if cloud_type == "ibmc":
                base_url += "Tools"
            else:
                base_url += "compose/Tools/x86_64/os/"

            if repo:
                # provide whole path till "/x86_64/os/" for openstack,
                # "/Tools" for IBM
                base_url = repo
            cmd = f"yum-config-manager --add-repo {base_url}"
            for node in self.cluster.get_nodes():
                node.exec_command(sudo=True, cmd=cmd)

    def set_cdn_tool_repo(self):
        """
        Enable the cdn Tools repo on all ceph node.
        """
        cdn_repo = "rhceph-5-tools-for-rhel-8-x86_64-rpms"
        cmd = f"subscription-manager repos --enable={cdn_repo}"
        for node in self.cluster.get_nodes():
            node.exec_command(sudo=True, cmd=cmd)

    def setup_upstream_repository(self, repo_url=None):
        """Download upstream repository to inidividual nodes.

        Args:
            repo_url: repo file URL link (default: None)
        """
        if not repo_url:
            repo_url = self.config["base_url"]

        for node in self.cluster.get_nodes():
            node.exec_command(
                sudo=True, cmd=f"curl -o /etc/yum.repos.d/upstream_ceph.repo {repo_url}"
            )
            node.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)
            node.exec_command(
                sudo=True,
                cmd="dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm -y",
                check_ec=False,
            )

    def install(self, **kwargs: Dict) -> None:
        """Install the cephadm package in all node(s).

        Args:
          kwargs (Dict): Key/value pairs that needs to be provided to the installer


        Example::

            Supported keys:
              upgrade: boolean # to upgrade cephadm RPM package
              gpgcheck: boolean


        :Note: At present, they are prefixed with -- hence use long options

        """
        cmd = "yum install cephadm -y"

        if kwargs.get("nogpgcheck", True):
            cmd += " --nogpghceck"

        for node in self.cluster.get_nodes():
            node.exec_command(
                sudo=True,
                cmd="yum install cephadm -y --nogpgcheck",
                long_running=True,
            )

            if kwargs.get("upgrade", False):
                node.exec_command(sudo=True, cmd="yum update metadata")
                node.exec_command(sudo=True, cmd="yum update -y cephadm")

            node.exec_command(cmd="rpm -qa | grep cephadm")

    def get_cluster_state(self, commands):
        """
        Display cluster state by executing commands provided
        Just used for sanity.

        Args:
            commands (List): list of commands
        """
        for cmd in commands:
            out, err = self.shell(args=[cmd])
            logger.info(out)
            if err:
                logger.error(err)
