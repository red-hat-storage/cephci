"""Module that interfaces with ceph orch ls CLI."""

from typing import Dict, Optional, Tuple

from .common import config_dict_to_string
from .typing_ import OrchProtocol


class LSMixin:
    """CLI that list known services to orchestrator."""

    def ls(self: OrchProtocol, config: Optional[Dict] = None) -> Tuple:
        """
        Execute the command ceph orch ls <args>.

        Args:
            config (Dict): The key/value pairs passed from the test case.

        Returns:
            output (Str), error (Str)   returned by the command.

        Example::

            Testing ceph orch ls

            config:
                command: ls
                base_cmd_args:
                    verbose: true
                    format: json | json-pretty | xml | xml-pretty | plain | yaml
                args:
                    host: <hostname>
                    service_type: <type of service>
                    service_name: <name of the service>
                    export: true
                    refresh: true

        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            cmd.append(config_dict_to_string(config["base_cmd_args"]))

        cmd.append("ls")

        if config and config.get("args"):
            args = config.get("args")
            cmd.append(config_dict_to_string(args))

        return self.shell(args=cmd)
