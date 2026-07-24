"""Module that interfaces with ceph orch ps CLI."""

from typing import Dict, Optional, Tuple

from .common import config_dict_to_string
from .typing_ import OrchProtocol


class PSMixin:
    """CLI that list daemons known to orchestrator."""

    def ps(self: OrchProtocol, config: Optional[Dict] = None) -> Tuple:
        """
        Execute the command ceph orch ps <args>.

        Args:
            config (Dict): The key/value pairs passed from the test case.

        Returns:
            output, error   returned by the command.

        Example::

            Testing ceph orch ps

            config:
                command: ps
                base_cmd_args:
                    verbose: true
                    format: json | json-pretty | xml | xml-pretty | plain | yaml
                args:
                    hostname: <hostname>
                    service_name: <name of service>
                    daemon_type: <type of daemon>
                    daemon_id: <id of the daemon>
                    refresh: true

        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("ps")

        if config and config.get("args"):
            args = config.get("args")
            cmd.append(config_dict_to_string(args))

        return self.shell(args=cmd)
