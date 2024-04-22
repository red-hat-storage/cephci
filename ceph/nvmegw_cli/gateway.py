from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Gateway(ExecuteCommandMixin):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = "gw"

    def info(self, **kwargs):
        return self.run_nvme_cli("info", **kwargs)

    def version(self, **kwargs):
        return self.run_nvme_cli("version", **kwargs)
