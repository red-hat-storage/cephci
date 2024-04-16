from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Connection(ExecuteCommandMixin):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = "connection"

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
