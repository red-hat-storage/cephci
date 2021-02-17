"""Module that allows QE to interface with cephadm bootstrap CLI."""
import logging
from typing import Dict

from ceph.ceph import ResourceNotFoundError
from utility.utils import get_cephci_config

from .common import config_dict_to_string
from .typing_ import CephAdmProtocol

logger = logging.getLogger(__name__)


class BootstrapMixin:
    """Add bootstrap support to the child class."""

    def bootstrap(self: CephAdmProtocol, config: Dict) -> None:
        """
        Execute cephadm bootstrap with the passed kwargs on the installer node.

        Bootstrap involves,
          - Creates /etc/ceph directory with permissions
          - CLI creation with bootstrap options with custom/default image
          - Execution of bootstrap command

        Args:
            config: Key/value pairs passed from the test case.

        Example:
            config:
                command: bootstrap
                base_cmd_args:
                    verbose: true
                args:
                    custom_image: true | false
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

        if config.get("base_cmd_args"):
            cmd += config_dict_to_string(config["base_cmd_args"])

        args = config.get("args")
        custom_image = args.pop("custom_image", True)

        if custom_image:
            cmd += f" --image {self.config['container_image']}"

        cmd += " bootstrap"
        custom_image_args = (
            " --registry-url registry.redhat.io"
            " --registry-username {user}"
            " --registry-password {password}"
        )
        cmd += custom_image_args.format(
            user=cdn_cred.get("username"),
            password=cdn_cred.get("password"),
        )

        # To be generic, the mon-ip contains the global node name. Here, we replace the
        # name with the IP address. The replacement allows us to be inline with the
        # CLI option.
        mon_node = args.pop("mon-ip")
        if mon_node:
            for node in self.cluster.get_nodes():
                if mon_node in node.shortname:
                    cmd += f" --mon-ip {node.ip_address}"
                    break
            else:
                raise ResourceNotFoundError(f"Unknown {mon_node} node name.")

        cmd += config_dict_to_string(args)

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out.read().decode())
        logger.error("Bootstrap error: %s", err.read().decode())

        self.distribute_cephadm_gen_pub_key()
