"""Module that allows QE to interface with cephadm bootstrap CLI."""
import json
import logging
import tempfile
from typing import Dict

from ceph.ceph import ResourceNotFoundError
from utility.utils import get_cephci_config

from .common import config_dict_to_string
from .typing_ import CephAdmProtocol

logger = logging.getLogger(__name__)


def construct_registry(cls, registry: str, json_file: bool = False):
    """
    Construct registry credentials for bootstrapping cluster

    Args:
        cls: class object
        registry: registry name
        json_file: registry credentials in file with JSON format

    json_file(default=false):
    - False : Constructs registry credentials for bootstrap
    - True  : Creates file with registry name attached with it,
              and saved as /tmp/<registry>.json file.

    Returns:
        constructed string for registry credentials
    """
    # Todo: Retrieve credentials based on registry name
    cdn_cred = get_cephci_config().get("cdn_credentials")
    reg_args = {
        "registry-url": registry,
        "registry-username": cdn_cred.get("username"),
        "registry-password": cdn_cred.get("password"),
    }
    if json_file:
        reg = dict((k.lstrip("registry-"), v) for k, v in reg_args.items())

        # Create file and return file_path
        temp_file = tempfile.NamedTemporaryFile(suffix=".json")
        reg_args = {"registry-json": temp_file.name}
        reg_file = cls.installer.node.remote_file(
            sudo=True, file_name=temp_file.name, file_mode="w"
        )
        reg_file.write(json.dumps(reg, indent=4))
        reg_file.flush()

    return config_dict_to_string(reg_args)


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
                    registry-url: <registry.url.name>
                    registry-json: <registry.url.name>
        """
        self.cluster.setup_ssh_keys()
        self.set_tool_repo()
        self.install()

        cmd = "cephadm"

        if config.get("base_cmd_args"):
            cmd += config_dict_to_string(config["base_cmd_args"])

        args = config.get("args")
        custom_image = args.pop("custom_image", True)

        if custom_image:
            cmd += f" --image {self.config['container_image']}"

        cmd += " bootstrap"

        # Construct registry credentials as string or json.
        registry_url = args.pop("registry-url", None)
        if registry_url:
            cmd += construct_registry(self, registry_url)

        registry_json = args.pop("registry-json", None)
        if registry_json:
            cmd += construct_registry(self, registry_json, json_file=True)

        # To be generic, the mon-ip contains the global node name. Here, we replace the
        # name with the IP address. The replacement allows us to be inline with the
        # CLI option.

        # Todo: need to switch installer node on any other node name provided
        #       other than installer node
        mon_node = args.pop("mon-ip", self.installer.node.shortname)
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
