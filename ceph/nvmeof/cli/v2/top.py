"""
NVMeoF ``top`` CLI wrappers (IBMCEPH-15738 / CEPH-83633026).

Same thin pattern as ``subsystem.py`` / ``namespace.py``: delegate to
``BaseCLI.run_nvme_cli``. Live-polling sample window is passed via
``live_timeout`` (handled in ``base_cli``).
"""

from ceph.nvmeof.cli.v2.base_cli import BaseCLI

# Default sample window: ≥2 refresh cycles at period=1 (first batch is often garbage).
DEFAULT_LIVE_TIMEOUT = 8


class Top:
    """NVMeoF ``top`` command group."""

    def __init__(self, base: BaseCLI) -> None:
        self.base = base
        self.name = "top"

    def _live_timeout(self, kwargs):
        """Resolve sample window: explicit live_timeout / sample_timeout, else default."""
        if kwargs.get("live_timeout") is not None:
            return kwargs["live_timeout"]
        if kwargs.get("sample_timeout") is not None:
            return kwargs["sample_timeout"]
        args = kwargs.get("args") or {}
        period = args.get("period", 1)
        try:
            period = float(period)
        except (TypeError, ValueError):
            period = 1.0
        return max(DEFAULT_LIVE_TIMEOUT, int(period * 2) + 3)

    def connection(self, **kwargs):
        """Run ``ceph nvmeof top connection --nqn ... --host_nqn ...``."""
        kwargs["live_timeout"] = self._live_timeout(kwargs)
        return self.base.run_nvme_cli(self.name, "connection", **kwargs)

    def io(self, **kwargs):
        """Run ``ceph nvmeof top io``."""
        kwargs["live_timeout"] = self._live_timeout(kwargs)
        return self.base.run_nvme_cli(self.name, "io", **kwargs)

    def cpu(self, **kwargs):
        """Run ``ceph nvmeof top cpu``."""
        kwargs["live_timeout"] = self._live_timeout(kwargs)
        return self.base.run_nvme_cli(self.name, "cpu", **kwargs)
