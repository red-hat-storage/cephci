from ceph.nvmeof.cli.v2.base_cli import BaseCLI


class Host:

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "host"

    def add(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "add", **kwargs)

    def change_key(self, **kwargs):
        """Change DHCHAP key for host"""
        return self.base.run_nvme_cli(self.name, "change_key", **kwargs)

    def delete(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "del", **kwargs)

    def list(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "list", **kwargs)
