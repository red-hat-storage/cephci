from json import loads

from ceph.nvmeof.cli.v2.base_cli import BaseCLI
from ceph.nvmeof.cli.v2.connection import Connection
from ceph.nvmeof.cli.v2.gateway import Gateway
from ceph.nvmeof.cli.v2.host import Host
from ceph.nvmeof.cli.v2.listener import Listener
from ceph.nvmeof.cli.v2.log_level import LogLevel
from ceph.nvmeof.cli.v2.namespace import Namespace
from ceph.nvmeof.cli.v2.subsystem import Subsystem
from ceph.nvmeof.cli.v2.version import Version


class NVMeGWCLIV2(BaseCLI):
    cli_version = "v2"

    def __init__(self, node, **kwargs) -> None:
        """Initialize NVMe Gateway CLI (v2).

        Args:
            node (CephNode): Gateway node instance.
            shell: Cephadm shell instance (orch.shell or cephadm.shell).
            mtls (bool, optional): Enable/disable mTLS. Defaults to False.
        """
        shell = kwargs.get("shell")
        if shell is None:
            raise ValueError("`cephadm.shell` is required for NVMeGWCLIV2")

        super().__init__(node, shell)

        self.node = node
        self.shell = shell
        self._mtls: bool = kwargs.get("mtls", False)

        self.connection = Connection(self)
        self.gateway = Gateway(self)
        self.host = Host(self)
        self.loglevel = LogLevel(self)
        self.listener = Listener(self)
        self.namespace = Namespace(self)
        self.subsystem = Subsystem(self)
        self.version = Version(self)
        self.name = " "
        self.v2 = True

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
        return self.run_nvme.cli(self.name, "get_subsystems", **kwargs)
