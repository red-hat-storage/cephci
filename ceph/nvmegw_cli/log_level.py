from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class LogLevel(ExecuteCommandMixin):
    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self.name = "spdk_log_level"
        self._mtls = mtls

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value

    def get(self, **kwargs):
        return self.run_nvme_cli("get", **kwargs)

    def set(self, **kwargs):
        return self.run_nvme_cli("set", **kwargs)

    def disable(self, **kwargs):
        return self.run_nvme_cli("disable", **kwargs)
