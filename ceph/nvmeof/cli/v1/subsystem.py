from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin


class Subsystem:

    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "subsystem"

    def add(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "add", **kwargs)

    def change_key(self, **kwargs):
        """Change DHCHAP key for subsystem."""
        return self.base.run_nvme_cli(self.name, "change_key", **kwargs)

    def delete(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "del", **kwargs)

    def list(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "list", **kwargs)
