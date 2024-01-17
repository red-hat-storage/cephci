from ceph.nvmegw_cli.connection import Connection
from ceph.nvmegw_cli.execute import ExecuteCommandMixin
from ceph.nvmegw_cli.gateway import Gateway
from ceph.nvmegw_cli.log_level import LogLevel
from ceph.nvmegw_cli.version import Version


class NVMeGWCLI(ExecuteCommandMixin):
    def __init__(self, node, port=5500) -> None:
        super().__init__(node, port)
        self.loglevel = LogLevel(node, port)
        self.gateway = Gateway(node, port)
        self.version = Version(node, port)
        self.connection = Connection(node, port)
        self.name = " "
        for clas in [self.loglevel, self.gateway, self.version, self.connection]:
            clas.NVMEOF_CLI_IMAGE = self.NVMEOF_CLI_IMAGE
