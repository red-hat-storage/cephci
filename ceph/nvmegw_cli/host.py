from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Host(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self.name = "host"
        self._mtls = mtls

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value

    def add(self, **kwargs):
        return self.run_nvme_cli("add", **kwargs)

    def delete(self, **kwargs):
        return self.run_nvme_cli("del", **kwargs)

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
