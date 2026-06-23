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
            dict: Response with unseal_keys_b64 and root_token.
        """
        data = {
            "secret_shares": kw.get("secret-shares", 5),
            "secret_threshold": kw.get("secret-threshold", 3),
        }
        return self.parent._request(
            "POST", "/v1/sys/init", data=data, token_required=False
        )

    def init_status(self, **kw):
        """Check if the Vault server is initialized.

        Returns:
            dict: Response with initialized status.
        """
        return self.parent._request("GET", "/v1/sys/init", token_required=False)

    def unseal(self, **kw):
        """Unseal the Vault server with a single key share.

        Args:
            kw(dict): Key/value pairs for unsealing.
                Supported keys:
                    key(str): Unseal key share
        """
        data = {"key": kw.get("key", "")}
        return self.parent._request(
            "POST", "/v1/sys/unseal", data=data, token_required=False
        )

    def seal_status(self, **kw):
        """Check the seal status of the Vault server.

        Returns:
            dict: Seal status details.
        """
        return self.parent._request("GET", "/v1/sys/seal-status", token_required=False)
