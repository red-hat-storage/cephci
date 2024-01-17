from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Version(ExecuteCommandMixin):
    def __init__(self, port, node) -> None:
        super().__init__(port, node)
        self.name = str()

    def version(self, **kwargs):
        return self.run_nvme_cli("version", **kwargs)
