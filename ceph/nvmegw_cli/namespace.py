from ceph.nvmegw_cli import NVMeGWCLI


class Namespace(NVMeGWCLI):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = "namespace"

    def set_qos(self, **kwargs):
        return self.run_nvme_cli("set_qos", **kwargs)

    def add(self, **kwargs):
        return self.run_nvme_cli("add", **kwargs)

    def delete(self, **kwargs):
        return self.run_nvme_cli("del", **kwargs)

    def list(self, **kwargs):
        return self.run_nvme_cli("list", **kwargs)
