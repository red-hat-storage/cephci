from ceph.nvmeof.cli.v2.base_cli import BaseCLI


class Gateway:

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "gateway"

    def get_log_level(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get_log_level", **kwargs)

    def info(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "info", **kwargs)

    def listener_info(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "listener_info", **kwargs)

    def set_log_level(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "set_log_level", **kwargs)

    def version(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "version", **kwargs)
