from cli import Cli


class Apply(Cli):
    """This module provides CLI interface for smb cluster related operations"""

    def __init__(self, nodes, base_cmd):
        super(Apply, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} apply"

    def apply(self, spec_file):
        """Deploy smb service using spec file

        Args:
            spec_file (str): Smb spec file path
        """
        cmd = f"{self.base_cmd} -i {spec_file}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
