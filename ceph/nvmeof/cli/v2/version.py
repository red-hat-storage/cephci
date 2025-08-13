from ceph.nvmeof.cli.v2.base_cli import BaseCLI


class Version:

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "connection"

    def version(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "version", **kwargs)
