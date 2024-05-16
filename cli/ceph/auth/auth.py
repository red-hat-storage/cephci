from cli import Cli


class Auth(Cli):
    def __init__(self, nodes, base_cmd):
        super(Auth, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} auth"

    def list(self):
        """
        list authentication state in the Ceph cluster.
        """
        cmd = f"{self.base_cmd} list"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def get(self, entity):
        """
        Display requested key.

        Args:
            entity (str): auth key
        """
        cmd = f"{self.base_cmd} get {entity}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, entity):
        """
        Removes an auth key.
        Args:
          entity (str): auth key
        """
        cmd = f"{self.base_cmd} rm {entity}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
