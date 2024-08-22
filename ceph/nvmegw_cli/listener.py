from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Listener(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self._mtls = mtls
        self.name = "listener"

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
