"""Interface to cephadm shell CLI."""

from copy import deepcopy
from logging import getLogger
from typing import Dict, List

from .common import config_dict_to_string
from .typing_ import CephAdmProtocol

LOG = getLogger(__name__)
BASE_CMD = ["cephadm", "shell"]


class ShellMixin:
    """Interface to shell CLI."""

    def shell(
        self: CephAdmProtocol,
        args: List[str],
        base_cmd_args: Dict = None,
        check_status: bool = True,
        timeout: int = 600,
        long_running: bool = False,
        print_output: bool = True,
        pretty_print: bool = False,
    ):
        """
        Ceph orchestrator shell interface to run ceph commands.

        Args:
            args (List): list arguments
            base_cmd_args (Dict)): cephadm base command options
            check_status (Bool): check command status
            timeout (Int): Maximum time allowed for execution.
            long_running (Bool): Long running command (default: False)
            print_output (Bool): Flag to decide whether the output should be printed in log or not
            pretty_print (Bool): When enabled, output/error will be pretty printed. Default false.

        Returns:
            out (Str), err (Str) stdout and stderr response
            rc (Int) exit status code if long_running command

        """
        cmd = deepcopy(BASE_CMD)

        if base_cmd_args:
            cmd.append(config_dict_to_string(base_cmd_args))

        cmd.append("--")
        cmd.extend(args)

        out = self.installer.exec_command(
            sudo=True,
            cmd=" ".join(cmd),
            timeout=timeout,
            check_ec=check_status,
            long_running=long_running,
            pretty_print=pretty_print,
        )

        if isinstance(out, tuple):
            if print_output:
                LOG.debug(out[0])
        return out
