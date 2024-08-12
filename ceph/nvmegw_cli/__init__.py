from json import loads

from ceph.nvmegw_cli.connection import Connection
from ceph.nvmegw_cli.execute import ExecuteCommandMixin
from ceph.nvmegw_cli.gateway import Gateway
from ceph.nvmegw_cli.host import Host
from ceph.nvmegw_cli.listener import Listener
from ceph.nvmegw_cli.log_level import LogLevel
from ceph.nvmegw_cli.namespace import Namespace
from ceph.nvmegw_cli.subsystem import Subsystem
from ceph.nvmegw_cli.version import Version
from cephci.utils.configs import get_configs, get_registry_credentials
from cli.utilities.containers import Registry


class NVMeGWCLI(ExecuteCommandMixin):
    def __init__(self, node, port=5500, mtls=False) -> None:
        self.node = node
        self.port = port
        self._mtls = mtls
        self.connection = Connection(node, port, mtls)
        self.gateway = Gateway(node, port, mtls)
        self.host = Host(node, port, mtls)
        self.loglevel = LogLevel(node, port, mtls)
        self.listener = Listener(node, port, mtls)
        self.namespace = Namespace(node, port, mtls)
        self.subsystem = Subsystem(node, port, mtls)
        self.version = Version(node, port, mtls)

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
        _, out = self.gateway.info(**gwinfo)
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
