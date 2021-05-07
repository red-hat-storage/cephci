"""Interface to cephadm shell CLI."""
import logging
from copy import deepcopy
from typing import Dict, List

from .common import config_dict_to_string
from .typing_ import CephAdmProtocol

LOG = logging.getLogger()
BASE_CMD = ["cephadm", "-v", "shell"]


class ShellMixin:
    """Interface to shell CLI."""

    def shell(
        self: CephAdmProtocol,
        args: List[str],
        base_cmd_args: Dict = None,
        check_status: bool = True,
        timeout: int = 300,
    ):
        """
        Ceph orchestrator shell interface to run ceph commands.

        Args:
            args: list arguments
            base_cmd_args: cephadm base command options
            check_status: check command status
            timeout: Maximum time allowed for execution.

        Returns:
            out: stdout
            err: stderr
        """
        cmd = deepcopy(BASE_CMD)

        if base_cmd_args:
            cmd.append(config_dict_to_string(base_cmd_args))

        cmd.append("--")
        cmd.extend(args)

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=" ".join(cmd),
            timeout=timeout,
            check_ec=check_status,
        )

        out = out.read().decode()
        err = err.read().decode()
        LOG.debug(out)
        return out, err
