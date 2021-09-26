"""Represents the ceph orch CLI action 'pause'.

This module enables the user to perform ceph pause orchestrator operations
using orchestration command

Example:
    The orchestrator operations can be paused by using the following command::

        ceph orch pause

This module is inherited where orchestrator operations are paused using pause command.
"""
import logging
from typing import Dict

from .common import config_dict_to_string
from .typing_ import ServiceProtocol

LOG = logging.getLogger()


class PauseFailure(Exception):
    pass


class PauseMixin:
    """Add Orch pause support to the base class."""

    def pause(self: ServiceProtocol, config: Dict) -> None:
        """Execute the pause method using the orchestrator.

        Args:
            config (Dict):     Key/value pairs passed from the test suite.

        Example::

            config:
                command: pause
                verify: true
                base_cmd_args:
                    verbose: true
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        cmd.append("pause")

        self.shell(args=cmd)

        verify = config.pop("verify", True)

        if verify and not self.verify_status("pause"):
            raise PauseFailure("Orchestrator operations could not be paused")
