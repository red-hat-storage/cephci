"""Module that allows QE to interface with cephadm bootstrap CLI."""
import json
import logging
import re
import tempfile
from typing import Dict

import requests

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


def validate_fsid(cls, fsid: str, out: str) -> bool:
    """
    Method is used to validate fsid
    Args:
        cls: class object
        fsid: <ID>
        out: bootstrap response logs

    Returns: Boolean value whether fsid is valid

    """
    out, err = cls.shell(args=["ceph", "fsid"])
    return out == fsid


def validate_skip_monitoring_stack(cls, stack, out: str) -> bool:
    """
    Method to validate monitoring service(s) has been skipped during bootstrap
    monitoring services - grafana, prometheus, alertmanager, node-exporter

    Args:
        cls: class object
        stack: skip monitoring stack
        out: bootstrap response logs

    Returns: Boolean

    """
    out, _ = cls.shell(args=["ceph", "orch", "ls", "-f json"])
    monitoring = ["prometheus", "grafana", "alertmanager", "node-exporter"]
    for svc in json.loads(out):
        if svc["service_type"] in monitoring:
            return False
    return True


def validate_dashboard(cls, out, user=None, password=None):
    """
    Method to validate dashboard login using provided user/password
    and Also validates user/passwd in bootstrap console output

    Args:
        cls: class object
        out: bootstrap response log
        user: dashboard user
        password: dashbard password

    Returns:
        boolean
    """
    _, _, db_log_part = out.partition("Ceph Dashboard is now available at:")

    if not db_log_part:
        logger.error("Dashboard log part not found")
        return False

    def parse(string):
        str_ = r"%s:\s(.+)" % string
        return re.search(str_, db_log_part).group(1)

    host = parse("URL")
    user_ = parse("User")
    passwd_ = parse("Password")

    if user:
        assert user == user_, f"user {user} did not match"
    if password:
        assert password == passwd_, f"password {password} did not match"

    host_ = re.search(r"https?:[/]{2}(.*):", host).group(1)
    for node in cls.cluster.get_nodes():
        if host_ in node.hostname:
            host = host.replace(host_, node.ip_address)
            break
    else:
        return False

    data = json.dumps({"password": passwd_, "username": user_})
    url_ = f"{host}/api/auth" if not host.endswith("/") else f"{host}api/auth"

    session = requests.Session()
    session.headers = {
        "accept": "application/vnd.ceph.api.v1.0+json",
        "content-type": "application/json",
    }

    resp = session.post(url_, data=data, verify=False)
    if not resp.ok:
        logger.info("Response: %s, Status code: %s" % (resp.text, resp.status_code))
        return False
    return True


def validate_dashboard_user(cls, user: str, out: str) -> bool:
    """
    Method is used to validate fsid
    Args:
        cls: class object
        user: dashboard user
        out: bootstrap console response

    Returns: Boolean value if user exists and able to login using username
    """
    return validate_dashboard(cls, out, user=user)


def validate_dashboard_passwd(cls, password: str, out: str) -> bool:
    """
    Method is used to validate fsid
    Args:
        cls: class object
        password: dashboard password
        out: bootstrap console response

    Returns:
         Boolean
    """
    return validate_dashboard(cls, out, password=password)


def fetch_method(key: str):
    """
    Fetch method using the Key

    Args:
        key: bootstrap argument

    Returns:
        Returns the method based on the Key
    """
    verify_map = {
        "fsid": validate_fsid,
        "initial-dashboard-user": validate_dashboard_user,
        "initial-dashboard-password": validate_dashboard_passwd,
        "skip-monitoring-stack": validate_skip_monitoring_stack,
    }
    return verify_map.get(key)


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
                    initial-dashboard-user: <admin123>
                    initial-dashboard-password: <admin123>
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

        out, err = out.read().decode(), err.read().decode()
        logger.info("Bootstrap output : %s", out)
        logger.error("Bootstrap error: %s", err)

        self.distribute_cephadm_gen_pub_key()

        # Verification of arguments
        args = self.config.get("args", {})
        for key, value in args.items():
            func = fetch_method(key)
            if not func:
                continue
            # bootstrap response through stdout & stderr are combined here
            # currently console response coming through stderr.
            if not func(self, value, out + err):
                raise AssertionError
