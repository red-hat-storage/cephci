from ceph.nvmegw_cli.execute import ExecuteCommandMixin


class Namespace(ExecuteCommandMixin):
    """NVMeoF Namespace operations."""

    def __init__(self, node, port, mtls=False) -> None:
        self.node = node
        self.port = port
        self.name = "namespace"
        self._mtls = mtls

    @property
    def mtls(self):
        return self._mtls

    @mtls.setter
    def mtls(self, value):
        self._mtls = value

    def add(self, **kwargs):
        """Adds namespace for subsystem."""
        return self.run_nvme_cli("add", **kwargs)

    def add_host(self, **kwargs):
        """Add a host to a namespace."""
        return self.run_nvme_cli("add_host", **kwargs)

    def change_load_balancing_group(self, **kwargs):
        """Change LB Group Id for namespace under subsystem."""
        return self.run_nvme_cli("change_load_balancing_group", **kwargs)

    def change_visibility(self, **kwargs):
        """Change visibility for namespace under subsystem."""
        return self.run_nvme_cli("change_visibility", **kwargs)

    def delete(self, **kwargs):
        """Delete a namespace from a subsystem."""
        return self.run_nvme_cli("del", **kwargs)

    def delete_host(self, **kwargs):
        """Delete host from a namespace."""
        return self.run_nvme_cli("del_host", **kwargs)

    def get_io_stats(self, **kwargs):
        """Get IO Stats for namespace."""
        return self.run_nvme_cli("get_io_stats", **kwargs)

    def list(self, **kwargs):
        """Lists namespaces under subsystem."""
        return self.run_nvme_cli("list", **kwargs)

    def resize(self, **kwargs):
        """Resize namespace under subsystem."""
        return self.run_nvme_cli("resize", **kwargs)

    def set_qos(self, **kwargs):
        """Set QOS for a namespace."""
        return self.run_nvme_cli("set_qos", **kwargs)
