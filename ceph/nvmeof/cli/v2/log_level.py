from ceph.nvmeof.cli.v2.base_cli import BaseCLI


class LogLevel:

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "log_level"

    def get(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get", **kwargs)

    def set(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "set", **kwargs)

    def disable(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "disable", **kwargs)
