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
