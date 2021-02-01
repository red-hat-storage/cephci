"""
Module to deploy below listed service and daemon(s).

- Daemon:
    (mon|mgr|rbd-mirror|crash|alertmanager|grafana|node-exporter|
    prometheus|cephadm-exporter)

    mon|mgr|alertmanager|grafana|node-exporter|
    prometheus|cephadm-exporter

ceph orch daemon add

this module inherited for deploying daemon(s) using "orch daemon" operation(s)
"""


class DaemonMixin:

    daemon_cmd = ["ceph", "orch", "daemon"]

    def add(self, service, **config):
        """
        Add daemon using cephadm orchestration

        Args:
            service: service type
            config: test arguments
        """
        pass

    def remove(self, service, **config):
        """
        Remove daemon using cephadm orchestration

        Args:
            service: service type
            config: test arguments
        """
        pass
