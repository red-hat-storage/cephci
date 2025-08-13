from json import loads

from ceph.nvmeof.cli.v1.connection import Connection
from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin
from ceph.nvmeof.cli.v1.gateway import Gateway
from ceph.nvmeof.cli.v1.host import Host
from ceph.nvmeof.cli.v1.listener import Listener
from ceph.nvmeof.cli.v1.log_level import LogLevel
from ceph.nvmeof.cli.v1.namespace import Namespace
from ceph.nvmeof.cli.v1.subsystem import Subsystem
from ceph.nvmeof.cli.v1.version import Version
from cephci.utils.configs import get_configs, get_registry_credentials
from cli.utilities.containers import Registry


class NVMeGWCLI(ExecuteCommandMixin):
    cli_version = "v1"

    def __init__(self, node, **kwargs) -> None:
        self.node = node
        self.port = kwargs.get("port", 5500)
        self._mtls = kwargs.get("mtls", False)
        self.connection = Connection(self)
        self.gateway = Gateway(self)
        self.host = Host(self)
        self.loglevel = LogLevel(self)
        self.listener = Listener(self)
        self.namespace = Namespace(self)
        self.subsystem = Subsystem(self)
        self.version = Version(self)
        self.v1 = True

        self.name = " "
        for clas in [
            self.loglevel,
            self.gateway,
            self.version,
            self.connection,
            self.host,
            self.subsystem,
            self.namespace,
            self.listener,
        ]:
            clas.NVMEOF_CLI_IMAGE = self.NVMEOF_CLI_IMAGE

        if "icr.io" in self.NVMEOF_CLI_IMAGE:
            get_configs()
            registry = get_registry_credentials("cdn", "ibm")
            if "stg" in self.NVMEOF_CLI_IMAGE:
                registry = get_registry_credentials("stage", "ibm")
            url = registry["registry"]
            username = registry["username"]
            password = registry["password"]
            Registry(self.node).login(url, username, password)

    def setter(self, attribute, value):
        """Method to set lower class attributes.

        Ensure lower class has these attributes and setter method.

        Args:
            attribute: attribute which has to be set
            value: value to be set on attribute.
        """
        for clas in [
            self.loglevel,
            self.gateway,
            self.version,
            self.connection,
            self.host,
            self.subsystem,
            self.namespace,
            self.listener,
        ]:
            setattr(clas, attribute, value)

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value
        self.setter("mtls", value)

    def fetch_gateway(self):
        """Return Gateway info"""
        gwinfo = {"base_cmd_args": {"format": "json"}}
        out, _ = self.gateway.info(**gwinfo)
        out = loads(out)
        return out

    def fetch_gateway_client_name(self):
        """Return Gateway Client name/id."""
        out = self.fetch_gateway()
        return out["name"]

    def fetch_gateway_lb_group_id(self):
        """Return Gateway Load balancing group Id."""
        out = self.fetch_gateway()
        return out["load_balancing_group"]

    def fetch_gateway_hostname(self):
        """Return Gateway load balancing group host name"""
        out = self.fetch_gateway()
        return out["hostname"]

    def get_subsystems(self, **kwargs):
        """Nvme CLI get_subsystems"""
        return self.run_nvme_cli("get_subsystems", **kwargs)
