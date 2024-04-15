from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Listener(ExecuteCommandMixin):
    def __init__(self, port, node) -> None:
        super().__init__(port, node)
        self.name = "listener"

    def add(self, **kwargs):
        return self.run_nvme_cli("add", **kwargs)

    def delete(self, **kwargs):
        return self.run_nvme_cli("del", **kwargs)

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
