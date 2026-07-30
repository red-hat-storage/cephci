from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class Gateway:
    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "gw"

    def get_log_level(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get_log_level", **kwargs)

    def get_stats(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get_stats", **kwargs)

    def get_thread_stats(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "get_thread_stats", **kwargs)

    def info(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "info", **kwargs)

    def listener_info(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "listener_info", **kwargs)

    def set_log_level(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "set_log_level", **kwargs)

    def version(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "version", **kwargs)
