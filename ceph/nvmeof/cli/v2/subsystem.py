from ceph.nvmeof.cli.v2.base_cli import BaseCLI

from .common import substitute_keys

KEY_MAP = {
    # TODO: Temporary solution until https://tracker.ceph.com/issues/72636
    #       is fixed.
    "force": "force=true",
}


class Subsystem:

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "subsystem"

    def add(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "add", **kwargs)

    def change_key(self, **kwargs):
        """Change DHCHAP key for subsystem."""
        return self.base.run_nvme_cli(self.name, "change_key", **kwargs)

    @substitute_keys(KEY_MAP)
    def delete(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "del", **kwargs)

    def list(self, **kwargs):
        return self.base.run_nvme_cli(self.name, "list", **kwargs)
