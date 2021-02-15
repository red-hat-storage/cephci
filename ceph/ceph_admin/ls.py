"""Module that interfaces with ceph orch ls CLI."""
from typing import Dict, Optional, Tuple

from .common import config_dict_to_string
from .typing_ import OrchProtocol


class LSMixin:
    """CLI that list known services to orchestrator."""

    def ls(self: OrchProtocol, config: Dict, args: Optional[Dict] = None) -> Tuple:
        """
        Execute the command ceph orch ls <args>.

        Args:
            config: The key/value pairs passed from the test case.
            args:   The optional key/value pairs that is passed to the command.

        Example:
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

        Returns:
            output, error   returned by the command.
        """
        cmd = ["ceph", "orch"]

        if config.get("base_cmd_args"):
            base_cmd_args = config_dict_to_string(config["base_cmd_args"])
            cmd.append(base_cmd_args)

        cmd.append("ls")

        # Export key has to be dealt differently
        export_ = args.pop("export")
        refresh = args.pop("refresh")

        # Ideally, there is only one argument along
        for key, value in args:
            cmd.append(value)

        if export_:
            cmd.append("--export")

        if refresh:
            cmd.append("--refresh")

        return self.shell(args=cmd)
