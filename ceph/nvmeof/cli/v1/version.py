from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class Version:
    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = str()

    def version(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "version", **kwargs)
