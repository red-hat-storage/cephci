import re

from ceph.nvmegw_cli import NVMeGWCLI


class Namespace(NVMeGWCLI):
    def __init__(self, node, port) -> None:
        super().__init__(node, port)
        self.name = "namespace"

    def set_qos(self, **kwargs):
        """Set QoS for namespace."""
        subsystem = kwargs.get("args", {}).get("subsystem")
        _, namespaces = self.list(args={"subsystem": subsystem})

        pattern = r"\│\s*(\d+)\s*│"
        nsid = [int(match) for match in re.findall(pattern, namespaces)]

        kwargs.setdefault("args", {}).update({"nsid": nsid[0]})

        return self.run_nvme_cli("set_qos", **kwargs)

    def add(self, **kwargs):
        """Adds namespace for subsystem."""
        return self.run_nvme_cli("add", **kwargs)

    def delete(self, **kwargs):
        """Deletes  namespace."""
        return self.run_nvme_cli("del", **kwargs)

    def list(self, **kwargs):
        """Lists namespaces under subsystem."""
        return self.run_nvme_cli("list", **kwargs)

    def resize(self, **kwargs):
        """Resize namespaces under subsystem."""
        return self.run_nvme_cli("resize", **kwargs)

    def change_load_balancing_group(self, **kwargs):
        """change LB Group Id for namespaces under subsystem."""
        return self.run_nvme_cli("change_load_balancing_group", **kwargs)

    def get_io_stats(self, **kwargs):
        """Get IO Stats for namespace."""
        return self.run_nvme_cli("get_io_stats", **kwargs)
