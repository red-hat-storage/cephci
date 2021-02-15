"""Module that allows QE to interface with cephadm bootstrap CLI."""
import logging
from typing import Dict, Optional

from ceph.ceph import ResourcesNotFoundError
from utility.utils import get_cephci_config

from .typing_ import CephAdmProtocol

logger = logging.getLogger(__name__)


class BootstrapMixin:
    """Add bootstrap support to the child class."""

    def bootstrap(
        self: CephAdmProtocol,
        prefix_args: Optional[Dict] = None,
        args: Optional[Dict] = None,
    ) -> None:
        """
        Execute cephadm bootstrap with the passed kwargs on the installer node.

        Bootstrap involves,
          - Creates /etc/ceph directory with permissions
          - CLI creation with bootstrap options with custom/default image
          - Execution of bootstrap command

        Args:
            prefix_args:    Optional arguments to the command.
            args:           Optional arguments to the action.

        Example:
            config:
                command: bootstrap
                prefix_args:
                    verbose: true
                    image: <image_name>
                args:
                    mon-ip: <node_name>
                    mgr-id: <mgr_id>
                    fsid: <id>
        """
        self.cluster.setup_ssh_keys()
        self.set_tool_repo()
        self.install()

        # Create and set permission to ceph directory
        self.installer.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
        self.installer.exec_command(sudo=True, cmd="chmod 777 /etc/ceph")

        # Execute bootstrap with MON ip-address
        # Construct bootstrap command
        # 1) Skip default mon, mgr & crash specs
        # 2) Skip automatic dashboard provisioning
        cdn_cred = get_cephci_config().get("cdn_credentials")

        cmd = "cephadm"

        for key, value in prefix_args:
            cmd += f" --{key}"

            # CLI enable/disable are the long option without any value. Support others
            if not isinstance(value, bool):
                cmd += f" {value}"

        # Exception, custom_image is a boolean however the information about it comes
        #            from the command line to allow frequent changes.
        custom_image = args.pop("custom_image")

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

        # To be generic, the mon-ip contains the global node name. Here, we replace the
        # name with the IP address. The replacement allows us to be inline with the
        # CLI option.
        mon_node = args.pop("mon-ip")
        if mon_node:
            nodes = self.cluster.get_nodes()
            for node in nodes:
                if mon_node in node.shortname:
                    cmd += f" --mon-ip {node.ip_address}"
                    break
            else:
                raise ResourcesNotFoundError(f"Unknown {mon_node} node name.")

        for key, value in args:
            cmd += f" --{key}"
            if not isinstance(value, bool):
                cmd += f" {value}"

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out.read().decode())
        logger.error("Bootstrap error: %s", err.read().decode())

        self.distribute_cephadm_gen_pub_key()
