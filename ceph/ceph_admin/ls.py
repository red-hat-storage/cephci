"""Module that interfaces with ceph orch ls CLI."""
from copy import deepcopy
from typing import List, Optional, Tuple

from .typing_ import OrchProtocol

BASE_CMD = ["ceph", "orch", "ls"]


class LSMixin:
    """CLI that list known services to orchestrator."""

    def ls(
        self: OrchProtocol, args: Optional[List[str]] = None, pretty_json: bool = True
    ) -> Tuple:
        """
        Execute the command ceph orch ls <args>.

        Args:
            args:           A list of arguments to pass to ls command
            pretty_json:    output in JSON format.

        Returns:
            output, error   returned by the command.
        """
        cmd = deepcopy(BASE_CMD)

        if args:
            cmd += args

        if pretty_json:
            cmd += ["-f", "json-pretty"]

        return self.shell(remote=self.cephadm.installer, args=cmd)
