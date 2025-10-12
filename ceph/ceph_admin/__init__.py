"""
Ceph Administrator aka cephadm module is a configuration tool for Ceph cluster.

It allows the users to deploy and manage their Ceph cluster. It also supports all the
operations part of the cluster lifecycle.

Over here, we create a glue between the CLI and CephCI to allow the QE to write test
scenarios for verifying and validating cephadm.
"""

from typing import Dict, Optional

from cephci.utils.build_info import CephTestManifest
from cli.utilities.configure import setup_ibm_licence
from utility.log import Log

from .bootstrap import BootstrapMixin
from .registry_login import RegistryLoginMixin
from .shell import ShellMixin

logger = Log(__name__)


class CephAdmin(BootstrapMixin, ShellMixin, RegistryLoginMixin):
    """
    Ceph administrator base class which enables ceph pre-requisites
    and Inherits HostMixin and BootstrapMixin classes to support
    host and bootstrap operations respectively
    """

    TIMEOUT = 300

    direct_calls = ["bootstrap", "shell"]

    def __init__(self, cluster, **config):
        """Initialize Cephadm with ceph_cluster object

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
        """Read cephadm generated public key.

        Arg:
            ssh_key_path ( Str ): custom ssh public key path

        Returns:
            Public Key string (Str)
        """
        path = ssh_key_path if ssh_key_path else "/etc/ceph/ceph.pub"
        ceph_pub_key, _ = self.installer.exec_command(sudo=True, cmd=f"cat {path}")

        return ceph_pub_key.strip()

    def distribute_cephadm_gen_pub_key(self, ssh_key_path=None, nodes=None):
        """Distribute cephadm generated public key to all nodes in the list.

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
        """Add the given repo on every node part of the cluster.

        Args:
            repo (Str): repository (default: None)

        """
        cloud_type = self.config.get("cloud-type", "openstack")
        logger.info(f"cloud type is {cloud_type}")

        hotfix_repo = self.config.get("hotfix_repo")
        base_url = self.config["base_url"]
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
        elif repo:
            base_url = repo
            cmd = f"yum-config-manager --add-repo {base_url}"
            for node in self.cluster.get_nodes():
                node.exec_command(sudo=True, cmd=cmd)

        elif base_url.endswith(".repo"):
            cmd = f"yum-config-manager --add-repo {base_url}"
            for node in self.cluster.get_nodes():
                node.exec_command(sudo=True, cmd=cmd)
        else:
            if not base_url.endswith("/"):
                base_url += "/"
            if cloud_type == "ibmc":
                base_url += "Tools"
            else:
                base_url += "compose/Tools/x86_64/os/"
            cmd = f"yum-config-manager --add-repo {base_url}"
            for node in self.cluster.get_nodes():
                node.exec_command(sudo=True, cmd=cmd)

    def set_cdn_tool_repo(
        self, release: Optional[str] = None, ctm: Optional[CephTestManifest] = None
    ) -> None:
        """Enable customer facing CDN.

        Args:
            release (Str): Ceph Release Version (default: None)
            ctm (CephTestManifest): Details of the build.
        """
        if release:
            logger.warning("[Deprecated] Avoid setting release.")

        if ctm is None:
            ctm = self.config["manifest"]

        cmd = f"subscription-manager repos --enable={ctm.repo_id}"
        if ctm.product == "ibm":
            # Pick the customer facing repositories as it would be
            # CDN testing.
            _repo = ctm.build_info["released"]["repositories"]["default"]
            cmd = f"dnf config-manager --add-repo {_repo}"

        for node in self.cluster.get_nodes(ignore="client"):
            node.exec_command(sudo=True, cmd=cmd)

    def setup_upstream_repository(self, repo_url=None):
        """Download upstream repository to inidividual nodes.

        Args:
            repo_url: repo file URL link (default: None)
        """
        EPEL_REPOS = {
            "7": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm",
            "8": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm",
            "9": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm",
        }

        if not repo_url:
            repo_url = self.config["base_url"]

        for node in self.cluster.get_nodes():
            # Ceph Repo
            node.exec_command(
                sudo=True, cmd=f"curl -o /etc/yum.repos.d/upstream_ceph.repo {repo_url}"
            )
            node.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)

            # Epel Repo
            node.exec_command(
                sudo=True,
                cmd=f"dnf install {EPEL_REPOS[node.distro_info['VERSION_ID'][0]]} -y",
                check_ec=False,
            )

            # public repo: needed to compensate for dependencies required during
            # installation of ceph-common and other pkg RPMs
            public_repo_url = (
                f"https://dl.fedoraproject.org/pub/epel/"
                f"{node.distro_info['VERSION_ID'][0]}/Everything/x86_64/"
            )
            node.exec_command(
                sudo=True,
                cmd=f"yum-config-manager --add-repo {public_repo_url}",
                check_ec=False,
            )

    def install(self, **kwargs: Dict) -> None:
        """Install the cephadm package in all node(s).

        Args:
          kwargs (Dict): Key/value pairs that needs to be provided to the installer

        Example::

            Supported keys:
              upgrade: boolean # to upgrade cephadm RPM package
              nogpgcheck: boolean
              upgrade_client: boolean # to upgrade ceph client RPM packages (default: true)
              rpm_version: specific rpm version to be installed


        :Note: At present, they are prefixed with -- hence use long options
        """
        cmd = "yum -y install cephadm"
        if kwargs.get("rpm_version", None):
            cmd = f"{cmd}-{kwargs['rpm_version']}"

        if kwargs.get("nogpgcheck", True):
            cmd += " --nogpgcheck"

        nodes = self.cluster.get_nodes(ignore="client")  # list of only cluster nodes
        if kwargs.get("upgrade", False):
            if kwargs.get("upgrade_client", True):
                nodes = self.cluster.get_nodes()  # list of all nodes, includes clients

            for node in nodes:
                if self.config.get("ibm_build"):
                    setup_ibm_licence(node, build_type=None)

                node.exec_command(sudo=True, cmd="yum update metadata", check_ec=False)
                upd_cmd = "yum update --nogpgcheck -y ceph*"
                if kwargs.get("rpm_version", None):
                    upd_cmd = f"{upd_cmd}-{kwargs['rpm_version']}"

                node.exec_command(sudo=True, cmd=upd_cmd)
                node.exec_command(cmd="rpm -qa | grep ceph")

        else:
            for node in nodes:
                if self.config["product"] == "ibm":
                    setup_ibm_licence(node, build_type=None)

                node.exec_command(
                    sudo=True,
                    cmd=cmd,
                    long_running=True,
                )

                node.exec_command(cmd="rpm -qa | grep cephadm")

    def get_cluster_state(self, commands):
        """Retrieve the state of Ceph Cluster.

        Args:
            commands (List): list of commands
        """
        for cmd in commands:
            out, err = self.shell(args=[cmd])
            logger.info(out)

            if err:
                logger.error(err)
