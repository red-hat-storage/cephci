"""Manage the NVMeoF service via ceph-adm CLI."""

from copy import deepcopy
from typing import Dict

from looseversion import LooseVersion

from utility.utils import get_ceph_version_from_cluster

from .apply import ApplyMixin
from .orch import Orch

# Ceph version from which nvmeof apply uses --pool/--group (args) instead of pos_args
NVMEOF_ARGS_VERSION = "20.2.1"


class NVMeoF(ApplyMixin, Orch):
    """Interface to ceph orch <action> nvmeof."""

    SERVICE_NAME = "nvmeof"

    def apply(self, config: Dict) -> None:
        """
        Deploy the NVMEoF service using the provided configuration.

        Supports both formats. For Ceph >= 20.2.1 uses --pool/--group (args).
        For Ceph < 20.2.1 uses positional args. Converts between formats as needed.

        Args:
            config (Dict): Key/value pairs provided by the test case to create the service.

        Example (pos_args format, legacy)::

            config:
                command: apply
                service: nvmeof
                pos_args: [rbd, gw_group1]
                args:
                    placement:
                        label: nvmeof-gw

        Example (args format)::

            config:
                command: apply
                service: nvmeof
                args:
                    placement:
                        label: nvmeof-gw
                    pool: rbd
                    group: gw_group1
        """
        config = deepcopy(config)
        args = config.get("args") or {}
        ceph_version = get_ceph_version_from_cluster(
            self.cluster.get_nodes(role="client")[0]
        )
        use_args_format = LooseVersion(ceph_version) >= LooseVersion(
            NVMEOF_ARGS_VERSION
        )

        pos_args = config.get("pos_args")
        pool = args.pop("pool", None)
        group = args.pop("group", None)

        if pos_args:
            # Legacy format: pos_args = [pool, group]
            pool_from_pos = pos_args[0] if pos_args else None
            group_from_pos = pos_args[1] if len(pos_args) > 1 else None
            if use_args_format:
                # Ceph >= 20.2.1: convert pos_args to args
                if pool_from_pos:
                    args["pool"] = pool_from_pos
                if group_from_pos is not None:
                    args["group"] = group_from_pos
                config.pop("pos_args", None)
            # else: keep pos_args as-is for older ceph
        elif pool or group is not None:
            # New format: pool/group in args
            if use_args_format:
                if pool:
                    args["pool"] = pool
                if group is not None:
                    args["group"] = group
            else:
                # Ceph < 20.2.1: convert to pos_args
                if pool:
                    config["pos_args"] = [pool]
                    if group is not None:
                        config["pos_args"].append(group)

        config["args"] = args
        super().apply(config=config)
