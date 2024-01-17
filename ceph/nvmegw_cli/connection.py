from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Connection(ExecuteCommandMixin):
    def __init__(self, port, node) -> None:
        super().__init__(port, node)
        self.name = "connection"

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
