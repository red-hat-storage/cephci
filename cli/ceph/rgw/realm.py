from cli import Cli
from cli.utilities.utils import build_cmd_from_args


class Realm(Cli):
    """This module provides CLI interface to manage the RGW realm operations"""

    def __init__(self, nodes, base_cmd):
        super(Realm, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} realm"

    def bootstrap(self, **kw):
        """
        Performs rgw realm bootstrap
        Args:
            kw: Key/value pairs of configuration information to be used in the test.
        """
        cmd = f"{self.base_cmd} bootstrap {build_cmd_from_args(**kw)}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def tokens(self):
        """
        Returns the existing tokens
        """
        cmd = f"{self.base_cmd} tokens"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
