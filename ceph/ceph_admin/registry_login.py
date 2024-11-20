"""Cephadm registry login CLI Command."""

from copy import deepcopy
from logging import getLogger
from typing import Dict

from .common import config_dict_to_string
from .typing_ import CephAdmProtocol

LOG = getLogger(__name__)
BASE_CMD = ["cephadm"]


class RegistryLoginMixin:
    """Cephadm registry-login CLI."""

    def registry_login(
        self: CephAdmProtocol,
        node,
        args: Dict = None,
        base_cmd_args: Dict = None,
        check_status: bool = True,
        timeout: int = 600,
        long_running: bool = False,
    ):
        """
        Cephadm registry login command.

         To store registry credentials under /etc/ceph/podman-auth.json

        Args:
            args (Dict): registry-login command options
            base_cmd_args (Dict)): cephadm base command options
            check_status (Bool): check command status
            timeout (Int): Maximum time allowed for execution.
            long_running (Bool): Long running command (default: False)

        Returns:
            out (Str), err (Str) stdout and stderr response
            rc (Int) exit status code if long_running command

        """
        cmd = deepcopy(BASE_CMD)

        if base_cmd_args:
            cmd.append(config_dict_to_string(base_cmd_args))

        cmd.append("registry-login")
        cmd.append(config_dict_to_string(args))

        out = node.exec_command(
            sudo=True,
            cmd=" ".join(cmd),
            timeout=timeout,
            check_ec=check_status,
            long_running=long_running,
        )

        if isinstance(out, tuple):
            LOG.debug(out[0])
        return out
