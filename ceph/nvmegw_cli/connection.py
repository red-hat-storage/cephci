from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Connection(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False, encryption=False) -> None:
        self.node = node
        self.port = port
        self._mtls = mtls
        self._encryption = encryption
        self.name = "connection"

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value
        
    @property
    def encryption(self):
        return self._encryption

    @encryption.setter
    def encryption(self, value):
        self._encryption = value

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
