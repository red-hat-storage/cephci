from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class LogLevel:
    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "spdk_log_level"

    def get(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get", **kwargs)

    def set(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "set", **kwargs)

    def disable(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "disable", **kwargs)
