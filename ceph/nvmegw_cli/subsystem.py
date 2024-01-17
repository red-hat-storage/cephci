from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmegw_cli.host import Host
from ceph.nvmegw_cli.listener import Listener
from ceph.nvmegw_cli.namespace import Namespace


class Subsystem(NVMeGWCLI):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = "subsystem"
        self.listener = Listener(node, port)
        self.namespace = Namespace(node, port)
        self.host = Host(node, port)
        for clas in [self.listener, self.namespace, self.host]:
            clas.NVMEOF_CLI_IMAGE = self.NVMEOF_CLI_IMAGE

    def add(self, **kwargs):
        return self.run_nvme_cli("add", **kwargs)

    def delete(self, **kwargs):
        return self.run_nvme_cli("del", **kwargs)

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
