"""Module that interfaces with ceph orch ps CLI."""
from copy import deepcopy
from typing import List, Optional, Tuple

from .typing_ import OrchProtocol

BASE_CMD = ["ceph", "orch", "ps"]


class PSMixin:
    """CLI that list daemons known to orchestrator."""

    def ps(
        self: OrchProtocol, args: Optional[List[str]] = None, pretty_json: bool = True
    ) -> Tuple:
        """
        Execute the command ceph orch ps <args>.

        Args:
            args:   A list of arguments to pass to ps command
            pretty_json:    output in JSON format.

        Returns:
            output, error   returned by the command.
        """
        cmd = deepcopy(BASE_CMD)

        if args:
            cmd += args

        if pretty_json:
            cmd += ["-f", "json-pretty"]

        return self.cephadm.shell(remote=self.cephadm.installer, args=cmd)
