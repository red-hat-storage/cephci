from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Connection(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self._mtls = mtls
        self.name = "connection"

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
