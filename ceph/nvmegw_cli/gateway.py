from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Gateway(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False, encryption=False) -> None:
        self.node = node
        self.port = port
        self.name = "gw"
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

    def info(self, **kwargs):
        return self.run_nvme_cli("info", **kwargs)

    def version(self, **kwargs):
        return self.run_nvme_cli("version", **kwargs)

    def set_log_level(self, **kwargs):
        return self.run_nvme_cli("set_log_level", **kwargs)

    def get_log_level(self, **kwargs):
        return self.run_nvme_cli("get_log_level", **kwargs)
