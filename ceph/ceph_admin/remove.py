"""
Represent the ceph orch CLI action 'remove'.
Module to remove ceph role service(s) using orchestration command
"ceph orch remove service.name "
This module inherited where service deleted using "remove" operation.
"""

from typing import Dict

from .common import config_dict_to_string
from .typing_ import ServiceProtocol


class RemoveMixin:
    """Add Orch remove support to the base class."""

    def remove(self: ServiceProtocol, config: Dict) -> None:
        """
        Execute the remove method using the object's service name.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            pos_args - List to be added as positional params

            config:
                command: remove
                service: rgw
                base_cmd_args:
                    verbose: true
                args:
                    service_name: rgw.realm.zone
                    verify: true

        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        args = config["args"]

        service_name = args.pop("service_name")
        cmd.extend(["rm", service_name])

        self.shell(args=cmd)

        verify = args.pop("verify", True)

        if verify:
            self.check_service(
                service_name=service_name,
                exist=False,
            )
