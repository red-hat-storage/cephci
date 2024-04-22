from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Version(ExecuteCommandMixin):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = str()

    def version(self, **kwargs):
        return self.run_nvme_cli("version", **kwargs)
