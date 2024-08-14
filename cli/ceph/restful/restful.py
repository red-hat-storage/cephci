from cli import Cli


class RestFul(Cli):
    """This module provides CLI interface for OSD related operations"""

    def __init__(self, nodes, base_cmd):
        super(RestFul, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} restful"

    def create_self_signed_cert(self):
        """Generate self signed certificate"""
        cmd = f"{self.base_cmd} create-self-signed-cert"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def create_key(self, username):
        """Create an API user"""
        cmd = f"{self.base_cmd} create-key {username}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def list_key(self):
        """List API keys"""
        cmd = f"{self.base_cmd} list-keys"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
