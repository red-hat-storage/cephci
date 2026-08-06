class Auth(object):
    """Vault authentication method management via REST API.

    Handles enabling auth methods and managing AppRole credentials.
    """

    def __init__(self, parent):
        self.parent = parent
        self.approle = self.AppRole(parent=self)

    def enable(self, **kw):
        """Enable an authentication method.

        Args:
            kw(dict): Key/value pairs for enabling an auth method.
                Supported keys:
                    path(str): Mount path (e.g., "approle", "userpass")
                    type(str): Auth method type
        """
        path = kw.get("path", "")
        auth_type = kw.get("type", "")
        return self.parent._request(
            "POST", f"/v1/sys/auth/{path}", data={"type": auth_type}
        )

    class AppRole(object):
        """AppRole authentication operations."""

        def __init__(self, parent):
            self.parent = parent

        def create_role(self, **kw):
            """Create an AppRole role with policies and TTL.

            Args:
                kw(dict): Key/value pairs for role creation.
                    Supported keys:
                        role-name(str): Name of the AppRole role
                        token-policies(str): Comma-separated policy names
                        token-ttl(str): Token TTL (e.g., "1h")
                        token-max-ttl(str): Maximum token TTL (e.g., "24h")
            """
            role_name = kw.get("role-name", "")
            payload = {}
            token_policies = kw.get("token-policies", "")
            if token_policies:
                payload["token_policies"] = token_policies
            token_ttl = kw.get("token-ttl", "")
            if token_ttl:
                payload["token_ttl"] = token_ttl
            token_max_ttl = kw.get("token-max-ttl", "")
            if token_max_ttl:
                payload["token_max_ttl"] = token_max_ttl
            return self.parent.parent._request(
                "POST", f"/v1/auth/approle/role/{role_name}", data=payload
            )

        def read_role_id(self, **kw):
            """Read the role ID for an AppRole role.

            Args:
                kw(dict): Key/value pairs for reading the role ID.
                    Supported keys:
                        role-name(str): Name of the AppRole role

            Returns:
                dict: Response containing the role_id.
            """
            role_name = kw.get("role-name", "")
            return self.parent.parent._request(
                "GET", f"/v1/auth/approle/role/{role_name}/role-id"
            )

        def generate_secret_id(self, **kw):
            """Generate a new secret ID for an AppRole role.

            Args:
                kw(dict): Key/value pairs for generating a secret ID.
                    Supported keys:
                        role-name(str): Name of the AppRole role

            Returns:
                dict: Response containing the secret_id.
            """
            role_name = kw.get("role-name", "")
            return self.parent.parent._request(
                "POST", f"/v1/auth/approle/role/{role_name}/secret-id", data={}
            )
