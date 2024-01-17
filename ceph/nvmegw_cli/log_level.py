from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class LogLevel(ExecuteCommandMixin):
    def __init__(self, port, node) -> None:
        super().__init__(port, node)
        self.name = "log_level"

    def get(self, **kwargs):
        return self.run_nvme_cli("get", **kwargs)

    def set(self, **kwargs):
        return self.run_nvme_cli("set", **kwargs)

    def disable(self, **kwargs):
        return self.run_nvme_cli("disable", **kwargs)
