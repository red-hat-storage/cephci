"""
Module to deploy below listed service(s).

# Service:
    (mon|mgr|rbd-mirror|crash|alertmanager|grafana|node-exporter|
    prometheus|cephadm-exporter)

this module inherited wherever service deployed using "apply" operation
"""


class Apply:

    apply_cmd = ["ceph", "orch", "apply"]

    def apply(self, service, **args):
        """
        Cephadm service deployment using apply command

        Args:
            service: daemon name
            args: test arguments
        """
        pass
