from ceph.nvmeof.cli.v1.execute import ExecuteCommandMixin

from .common import substitute_keys

KEY_MAP = {"rbd-image-size": "size"}


class Namespace:
    """NVMeoF Namespace operations."""

    def __init__(self, base: ExecuteCommandMixin) -> None:
        self.base = base
        self.name = "namespace"

    @substitute_keys(KEY_MAP)
    def add(self, **kwargs):
        """Adds namespace for subsystem."""
        return self.base.run_nvme_cli(self.name, "add", **kwargs)

    def add_host(self, **kwargs):
        """Add a host to a namespace."""
        return self.base.run_nvme_cli(self.name, "add_host", **kwargs)

    def change_load_balancing_group(self, **kwargs):
        """Change LB Group Id for namespace under subsystem."""
        return self.base.run_nvme_cli(
            self.name, "change_load_balancing_group", **kwargs
        )

    def change_location(self, **kwargs):
        """Change location for namespace under subsystem."""
        return self.base.run_nvme_cli(self.name, "change_location", **kwargs)

    def change_visibility(self, **kwargs):
        """Change visibility for namespace under subsystem."""
        return self.base.run_nvme_cli(self.name, "change_visibility", **kwargs)

    def delete(self, **kwargs):
        """Delete a namespace from a subsystem."""
        return self.base.run_nvme_cli(self.name, "del", **kwargs)

    def del_host(self, **kwargs):
        """Delete host from a namespace."""
        return self.base.run_nvme_cli(self.name, "del_host", **kwargs)

    def get_io_stats(self, **kwargs):
        """Get IO Stats for namespace."""
        return self.base.run_nvme_cli(self.name, "get_io_stats", **kwargs)

    def get(self, **kwargs):
        """Get namespace details."""
        return self.base.run_nvme_cli(self.name, "get", **kwargs)

    def list(self, **kwargs):
        """Lists namespaces under subsystem."""
        return self.base.run_nvme_cli(self.name, "list", **kwargs)

    def list_hosts(self, **kwargs):
        """List hosts allowed for a namespace."""
        return self.base.run_nvme_cli(self.name, "list_hosts", **kwargs)

    @substitute_keys(KEY_MAP)
    def resize(self, **kwargs):
        """Resize namespace under subsystem."""
        return self.base.run_nvme_cli(self.name, "resize", **kwargs)

    def set_qos(self, **kwargs):
        """Set QoS for a namespace."""
        return self.base.run_nvme_cli(self.name, "set_qos", **kwargs)

    @substitute_keys(KEY_MAP)
    def set_auto_resize(self, **kwargs):
        """Set auto-resize for a namespace."""
        return self.base.run_nvme_cli(self.name, "set_auto_resize", **kwargs)

    def refresh_size(self, **kwargs):
        """Refresh size for a namespace."""
        return self.base.run_nvme_cli(self.name, "refresh_size", **kwargs)
