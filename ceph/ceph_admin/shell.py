"""Interface to cephadm shell CLI."""
import logging
from typing import List

from .typing_ import CephAdmProtocol

LOG = logging.getLogger()
BASE_CMD = ["cephadm", "-v", "shell", "--"]


class ShellMixin:
    """Interface to shell CLI."""

    def shell(
        self: CephAdmProtocol,
        args: List[str],
        check_status: bool = True,
        timeout: int = 300,
    ):
        """
        Ceph orchestrator shell interface to run ceph commands.

        Args:
            args: list arguments
            check_status: check command status
            timeout:    Maximum time allowed for execution.

        Returns:
            out: stdout
            err: stderr
        """
        cmd = BASE_CMD + args

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=" ".join(cmd),
            timeout=timeout,
            check_ec=check_status,
        )

        out = out.read().decode().strip()
        err = err.read().decode().strip()
        LOG.debug(out)
        return out, err
