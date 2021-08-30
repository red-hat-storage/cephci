"""Represents the ceph orch CLI action 'resume'.

This module enables the user to perform ceph resume orchestrator operations
using orchestration command

Example:
    The orchestrator operations can be resumed by using the following command::

        ceph orch resume

This module is inherited where orchestrator operations are resumed using resume command.
"""
import logging
from typing import Dict

from .common import config_dict_to_string
from .typing_ import ServiceProtocol

LOG = logging.getLogger()


class ResumeFailure(Exception):
    pass


class ResumeMixin:
    """Add Orch resume support to the base class."""

    def resume(self: ServiceProtocol, config: Dict) -> None:
        """Execute the resume method using the orchestrator.

        Args:
            config:     Key/value pairs passed from the test suite.

        Example::

            config:
                command: resume
                verify: true
                base_cmd_args:
                    verbose: true
        """
        cmd = ["ceph", "orch"]
        if config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        cmd.append("resume")

        self.shell(args=cmd)

        verify = config.pop("verify", True)

        if verify and not self.verify_pause_status("resume"):
            raise ResumeFailure("Orchestrator operations could not be resumed")
