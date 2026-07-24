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
        name = kw.get("name", "")
        policy = kw.get("policy", "")
        return self.parent._request(
            "POST", f"/v1/sys/policies/acl/{name}", data={"policy": policy}
        )

    def read(self, **kw):
        """Read an existing ACL policy.

        Args:
            kw(dict): Key/value pairs for reading a policy.
                Supported keys:
                    name(str): Policy name
        """
        name = kw.get("name", "")
        return self.parent._request("GET", f"/v1/sys/policies/acl/{name}")

    def delete(self, **kw):
        """Delete an ACL policy.

        Args:
            kw(dict): Key/value pairs for deleting a policy.
                Supported keys:
                    name(str): Policy name
        """
        name = kw.get("name", "")
        return self.parent._request("DELETE", f"/v1/sys/policies/acl/{name}")

    def list(self, **kw):
        """List all ACL policies.

        Returns:
            dict: List of policy names.
        """
        return self.parent._request("LIST", "/v1/sys/policies/acl")
