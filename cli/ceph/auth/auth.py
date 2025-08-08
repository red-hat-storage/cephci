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

    def get_or_create_client_keyring(self, client_hostname: str) -> str:
        """
        Get or create a Ceph auth keyring for the given client hostname.

        Args:
            client_hostname (str): Name of the Ceph client (e.g., hostname of the host).

        Returns:
            str: CLI output from Ceph auth command (the path to the keyring or stdout).

        Raises:
            FuseMountError: If keyring creation fails.
        """
        keyring_path = f"/etc/ceph/ceph.client.{client_hostname}.keyring"
        cmd = (
            f"{self.base_cmd} get-or-create client.{client_hostname} "
            f"mon 'allow *' mds 'allow * path=/' osd 'allow *' "
            f"-o {keyring_path}"
        )

        out = self.execute(sudo=True, cmd=cmd)
        return out[0].strip() if isinstance(out, tuple) else out
