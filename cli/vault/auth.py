import json
from copy import deepcopy


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
        kw_copy = deepcopy(kw)
        path = kw_copy.pop("path", "")
        auth_type = kw_copy.pop("type", "")
        data = json.dumps({"type": auth_type})
        cmd = (
            f"curl -s -X POST"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/sys/auth/{path}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

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
            kw_copy = deepcopy(kw)
            role_name = kw_copy.pop("role-name", "")
            payload = {}
            token_policies = kw_copy.pop("token-policies", "")
            if token_policies:
                payload["token_policies"] = token_policies
            token_ttl = kw_copy.pop("token-ttl", "")
            if token_ttl:
                payload["token_ttl"] = token_ttl
            token_max_ttl = kw_copy.pop("token-max-ttl", "")
            if token_max_ttl:
                payload["token_max_ttl"] = token_max_ttl
            data = json.dumps(payload)
            cmd = (
                f"curl -s -X POST"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" -H 'Content-Type: application/json'"
                f" -d '{data}'"
                f" {self.parent.parent.vault_url}"
                f"/v1/auth/approle/role/{role_name}"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)

        def read_role_id(self, **kw):
            """Read the role ID for an AppRole role.

            Args:
                kw(dict): Key/value pairs for reading the role ID.
                    Supported keys:
                        role-name(str): Name of the AppRole role

            Returns:
                JSON response containing the role_id.
            """
            kw_copy = deepcopy(kw)
            role_name = kw_copy.pop("role-name", "")
            cmd = (
                f"curl -s"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" {self.parent.parent.vault_url}"
                f"/v1/auth/approle/role/{role_name}/role-id"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)

        def generate_secret_id(self, **kw):
            """Generate a new secret ID for an AppRole role.

            Args:
                kw(dict): Key/value pairs for generating a secret ID.
                    Supported keys:
                        role-name(str): Name of the AppRole role

            Returns:
                JSON response containing the secret_id.
            """
            kw_copy = deepcopy(kw)
            role_name = kw_copy.pop("role-name", "")
            cmd = (
                f"curl -s -X POST"
                f" -H 'X-Vault-Token: {self.parent.parent.token}'"
                f" -H 'Content-Type: application/json'"
                f" -d '{{}}'"
                f" {self.parent.parent.vault_url}"
                f"/v1/auth/approle/role/{role_name}/secret-id"
            )
            return self.parent.parent.execute_as_sudo(cmd=cmd)
