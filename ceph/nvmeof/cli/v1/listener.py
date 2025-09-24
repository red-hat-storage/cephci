from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class Listener:
    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "listener"

    def add(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "add", **kwargs)

    def delete(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "del", **kwargs)

    def list(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "list", **kwargs)
