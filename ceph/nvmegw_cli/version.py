from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Version(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self.name = str()
        self._mtls = mtls

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value

    def version(self, **kwargs):
        return self.run_nvme_cli("version", **kwargs)
