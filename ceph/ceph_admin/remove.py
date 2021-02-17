"""
Represent the ceph orch CLI action 'remove'.

Module to remove ceph role service(s) using orchestration command
"ceph orch remove service.name "

This module inherited where service deleted using "remove" operation.
"""
from typing import Dict

from .typing_ import ServiceProtocol


class RemoveMixin:
    """Add Orch remove support to the base class."""

    def remove(self: ServiceProtocol, config: Dict) -> None:
        """
        Execute the remove method using the object's service name.

        Args:
            config:     Key/value pairs passed from the test suite.
                        pos_args        - List to be added as positional params

        Example:
            config:
                command: remove
                service: rgw
                base_cmd_args:
                    service_name: rgw.realm.zone
                    verify: true
        """
        base_cmd = ["ceph", "orch", "rm"]
        cmd_args = config.pop("base_cmd_args")
        service_name = cmd_args.pop("service_name")
        base_cmd.append(service_name)

        self.shell(args=base_cmd)

        verify = cmd_args.pop("verify", True)

        if verify:
            self.check_service(
                service_name=service_name,
                exist=False,
            )
