from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Host(Cli):
    def __init__(self, nodes, base_cmd):
        super(Host, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} host"

    def ls(self, **kw):
        """
        List hosts
        Args:
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported Keys:
                format (str): the type to be formatted(yaml)
                host_pattern (str) : host name
                label (str) : label
                host_status (str) : host status
        """
        cmd = f"{self.base_cmd} ls {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def maintenance(self, hostname, operation, force=False, yes_i_really_mean_it=False):
        """
        Add/Remove the specified host from maintenance mode
        Args:
            hostname(str): name of the host which needs to be added into maintenance mode
            operation(str): enter/exit maintenance mode
            force (bool): Whether to append force with maintenance enter command
            yes-i-really-mean-it (bool) : Whether to append --yes-i-really-mean-it with maintenance enter command
        """
        cmd = f"{self.base_cmd} maintenance {operation} {hostname}"
        if force:
            cmd += " --force"
        if yes_i_really_mean_it:
            cmd += " --yes-i-really-mean-it"
        out = self.execute(cmd=cmd, sudo=True)
        if not out:
            return False
        return True
