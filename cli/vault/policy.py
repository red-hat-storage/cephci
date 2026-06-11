import json
from copy import deepcopy


class Policy(object):
    """Vault policy management via REST API.

    Handles creating, reading, deleting, and listing ACL policies.
    """

    def __init__(self, parent):
        self.parent = parent

    def write(self, **kw):
        """Create or update a named ACL policy.

        Args:
            kw(dict): Key/value pairs for writing a policy.
                Supported keys:
                    name(str): Policy name
                    policy(str): HCL policy content
        """
        kw_copy = deepcopy(kw)
        name = kw_copy.pop("name", "")
        policy = kw_copy.pop("policy", "")
        data = json.dumps({"policy": policy})
        cmd = (
            f"curl -s -X POST"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" -H 'Content-Type: application/json'"
            f" -d '{data}'"
            f" {self.parent.vault_url}/v1/sys/policies/acl/{name}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def read(self, **kw):
        """Read an existing ACL policy.

        Args:
            kw(dict): Key/value pairs for reading a policy.
                Supported keys:
                    name(str): Policy name
        """
        kw_copy = deepcopy(kw)
        name = kw_copy.pop("name", "")
        cmd = (
            f"curl -s"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" {self.parent.vault_url}/v1/sys/policies/acl/{name}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def delete(self, **kw):
        """Delete an ACL policy.

        Args:
            kw(dict): Key/value pairs for deleting a policy.
                Supported keys:
                    name(str): Policy name
        """
        kw_copy = deepcopy(kw)
        name = kw_copy.pop("name", "")
        cmd = (
            f"curl -s -X DELETE"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" {self.parent.vault_url}/v1/sys/policies/acl/{name}"
        )
        return self.parent.execute_as_sudo(cmd=cmd)

    def list(self, **kw):
        """List all ACL policies.

        Returns:
            JSON response with the list of policy names.
        """
        cmd = (
            f"curl -s -X LIST"
            f" -H 'X-Vault-Token: {self.parent.token}'"
            f" {self.parent.vault_url}/v1/sys/policies/acl"
        )
        return self.parent.execute_as_sudo(cmd=cmd)
