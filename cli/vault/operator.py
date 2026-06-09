import json
from copy import deepcopy


class Operator(object):
    """Vault operator commands via REST API.

    Handles initialization, unsealing, and seal status operations.
    """

    def __init__(self, parent):
        self.parent = parent

    def init(self, **kw):
        """Initialize the Vault server.

        Args:
            kw(dict): Key/value pairs for initialization.
                Supported keys:
                    secret-shares(int): Number of key shares (default: 5)
                    secret-threshold(int): Unseal threshold (default: 3)

        Returns:
            JSON response with unseal_keys_b64 and root_token.
        """
        kw_copy = deepcopy(kw)
        secret_shares = kw_copy.pop("secret-shares", 5)
        secret_threshold = kw_copy.pop("secret-threshold", 3)
        data = json.dumps(
            {
                "secret_shares": secret_shares,
                "secret_threshold": secret_threshold,
            }
        )
        cmd = (
            f"curl -s -X POST"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/sys/init"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def init_status(self, **kw):
        """Check if the Vault server is initialized.

        Returns:
            JSON response with initialized status.
        """
        cmd = f"curl -s {self.parent.vault_url}/v1/sys/init"
        return self.parent.execute_as_sudo(cmd=cmd)

    def unseal(self, **kw):
        """Unseal the Vault server with a single key share.

        Args:
            kw(dict): Key/value pairs for unsealing.
                Supported keys:
                    key(str): Unseal key share
        """
        kw_copy = deepcopy(kw)
        key = kw_copy.pop("key", "")
        data = json.dumps({"key": key})
        cmd = (
            f"curl -s -X POST"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/sys/unseal"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def seal_status(self, **kw):
        """Check the seal status of the Vault server.

        Returns:
            JSON response with seal status details.
        """
        cmd = f"curl -s {self.parent.vault_url}/v1/sys/seal-status"
        return self.parent.execute_as_sudo(cmd=cmd)
