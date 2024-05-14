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
from cli.utilities.configs import get_registry_details
from cli.utilities.containers import Registry


class NVMeGWCLI(ExecuteCommandMixin):
    def __init__(self, node, port=5500) -> None:
        super().__init__(node, port)
        self.connection = Connection(node, port)
        self.gateway = Gateway(node, port)
        self.host = Host(node, port)
        self.loglevel = LogLevel(node, port)
        self.listener = Listener(node, port)
        self.namespace = Namespace(node, port)
        self.subsystem = Subsystem(node, port)
        self.version = Version(node, port)

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
            registry_details = get_registry_details(ibm_build=True)
            url = registry_details.get("registry-url")
            username = registry_details.get("registry-username")
            password = registry_details.get("registry-password")
            Registry(self.node).login(url, username, password)

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
