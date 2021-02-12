"""Module that interfaces with ceph orch ls CLI."""
from typing import Dict, Optional, Tuple

from .typing_ import OrchProtocol


class LSMixin:
    """CLI that list known services to orchestrator."""

    def ls(
            self: OrchProtocol,
            prefix_args: Optional[Dict] = None,
            args: Optional[Dict] = None,
    ) -> Tuple:
        """
        Execute the command ceph orch ls <args>.

        Args:
            prefix_args:    The key/value pairs to be passed to the base command.
            args:           the key/value pairs to be passed to the command.

        Example:
            Testing ceph orch ls

            config:
                command: ls
                prefix_args:
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

        for key, value in prefix_args:
            if len(key) == 1:
                cmd.append(f"-{key}")
                continue

            cmd.append(f"--{key}")
            if not isinstance(value, bool):
                cmd.append(value)

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
