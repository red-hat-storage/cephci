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
            config: The key/value pairs passed from the test case.

        Example:
            Testing ceph orch ps

            config:
                command: ps
                base_cmd_args:
                    verbose: true
                    format: json | json-pretty | xml | xml-pretty | plain | yaml
                args:
                    host: <hostname>
                    service_name: <name of service>
                    daemon_type: <type of daemon>
                    daemon_id: <id of the daemon>
                    refresh: true

        Returns:
            output, error   returned by the command.
        """
        cmd = ["ceph", "orch"]

        if config and config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("ps")

        args = None
        if config and config.get("args"):
            args = config.get("args")

        if args:
            # Export key has to be dealt differently
            refresh = args.pop("refresh")

            # Ideally, there is only one argument along
            for key, value in args:
                cmd.append(value)

            if refresh:
                cmd.append("--refresh")

        return self.shell(args=cmd)
