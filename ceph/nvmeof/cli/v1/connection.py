from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class Connection:
    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "connection"

    def list(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "list", **kwargs)
