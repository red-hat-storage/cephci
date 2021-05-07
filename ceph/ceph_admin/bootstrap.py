"""Module that allows QE to interface with cephadm bootstrap CLI."""
import json
import logging
import tempfile
from typing import Dict

from ceph.ceph import ResourceNotFoundError
from utility.utils import get_cephci_config

from .common import config_dict_to_string
from .helper import GenerateServiceSpec
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

    def bootstrap(self: CephAdmProtocol, config: Dict):
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
                    custom_image: <image path> or <boolean>
                    mon-ip: <node_name>
                    mgr-id: <mgr_id>
                    fsid: <id>
                    registry-url: <registry.url.name>
                    registry-json: <registry.url.name>
                    initial-dashboard-user: <admin123>
                    initial-dashboard-password: <admin123>

        custom_image:
          image path: compose path for example alpha build,
            ftp://partners.redhat.com/d960e6f2052ade028fa16dfc24a827f5/rhel-8/Tools/x86_64/os/
          boolean:
            True: use latest image from test config
            False: do not use latest image from test config,
                   and also indicates usage of default image from cephadm source-code.

        """
        self.cluster.setup_ssh_keys()
        args = config.get("args")
        custom_repo = args.pop("custom_repo", None)
        custom_image = args.pop("custom_image", True)

        if custom_repo:
            self.set_tool_repo(repo=custom_repo)
        else:
            self.set_tool_repo()

        self.install()

        cmd = "cephadm"
        if config.get("base_cmd_args"):
            cmd += config_dict_to_string(config["base_cmd_args"])

        if custom_image:
            if isinstance(custom_image, str):
                cmd += f" --image {custom_image}"
            else:
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

        # apply-spec
        specs = args.get("apply-spec")
        if specs:
            spec_cls = GenerateServiceSpec(
                node=self.installer, cluster=self.cluster, specs=specs
            )
            args["apply-spec"] = spec_cls.create_spec_file()

        cmd += config_dict_to_string(args)
        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        out, err = out.read().decode(), err.read().decode()
        logger.info("Bootstrap output : %s", out)
        logger.error("Bootstrap error: %s", err)

        self.distribute_cephadm_gen_pub_key(args.get("output-pub-ssh-key"))

        # The provided image is used by Grafana service only when
        # --skip-monitoring-stack is set to True during bootstrap.
        if self.config.get("grafana_image"):
            cmd = "cephadm shell --"
            cmd += " ceph config set mgr mgr/cephadm/container_image_grafana"
            cmd += f" {self.config['grafana_image']}"
            self.installer.exec_command(sudo=True, cmd=cmd)

        return out, err
