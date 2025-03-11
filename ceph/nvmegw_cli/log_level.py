from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class LogLevel(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False, encryption=False) -> None:
        self.node = node
        self.port = port
        self.name = "spdk_log_level"
        self._mtls = mtls
        self._encryption = encryption

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

    def get(self, **kwargs):
        return self.run_nvme_cli("get", **kwargs)

    def set(self, **kwargs):
        return self.run_nvme_cli("set", **kwargs)

    def disable(self, **kwargs):
        return self.run_nvme_cli("disable", **kwargs)
