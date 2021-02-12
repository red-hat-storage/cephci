"""Module that allows QE's to interface with cephadm bootstrap CLI."""
import logging
from typing import Dict

from ceph.ceph import ResourcesNotFoundError
from utility.utils import get_cephci_config

from .typing_ import CephAdmProtocol

logger = logging.getLogger(__name__)


class BootstrapMixin:
    """Provide cephadm bootstrap CLI execution."""

    def bootstrap(self: CephAdmProtocol, **kwargs: Dict):
        """
        Execute cephadm bootstrap with the passed kwargs on the installer node.

        Bootstrap involves,
          - Creates /etc/ceph directory with permissions
          - CLI creation with bootstrap options with custom/default image
          - Execution of bootstrap command

        Args:
            kwargs: Key/value pairs supported in the CLI
        """
        # copy ssh keys to other hosts
        self.cluster.setup_ssh_keys()

        # set tool download repository
        self.set_tool_repo()

        # install/download cephadm package on installer
        self.install()

        # Create and set permission to ceph directory
        self.installer.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
        self.installer.exec_command(sudo=True, cmd="chmod 777 /etc/ceph")

        # Execute bootstrap with MON ip-address
        # Construct bootstrap command
        # 1) Skip default mon, mgr & crash specs
        # 2) Skip automatic dashboard provisioning
        cdn_cred = get_cephci_config().get("cdn_credentials")

        cmd = "cephadm -v bootstrap"

        # Exception, custom_image is a boolean however the information about it comes
        #            from the command line to allow frequent changes.

        custom_image = kwargs.pop("custom_image")

        if custom_image:
            custom_image_args = (
                " --registry-url registry.redhat.io"
                " --registry-username {user}"
                " --registry-password {password}"
                " --image {image}"
            )
            cmd += custom_image_args.format(
                user=cdn_cred.get("username"),
                password=cdn_cred.get("password"),
                image=self.config["container_image"],
            )

        mon_node = kwargs.pop("mon-ip")
        if mon_node:
            # get the node IP address
            nodes = self.cluster.get_nodes()
            for node in nodes:
                if mon_node in node.shortname:
                    cmd += f" --mon-ip {node.ip_address}"
                    break
            else:
                raise ResourcesNotFoundError(f"Unknown {mon_node} node name.")

        for arg in kwargs:
            cmd += f" --{arg}"
            if not isinstance(kwargs[arg], bool):
                cmd += f" {kwargs[arg]}"

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out.read().decode())
        logger.error("Bootstrap error: %s", err.read().decode())

        self.distribute_cephadm_gen_pub_key()
