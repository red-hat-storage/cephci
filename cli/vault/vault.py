from cli import Cli

from .agent import Agent
from .auth import Auth
from .operator import Operator
from .policy import Policy
from .secrets import Secrets
from .server import Server


class Vault(Cli):
    """CLI wrapper for HashiCorp Vault operations.

    Provides a consistent interface for interacting with a containerized
    Vault server via REST API, following the same pattern as cli/rbd/.

    Usage:
        vault = Vault(node, vault_url="http://10.0.0.1:8200")

        # Deploy the server container
        vault.server.deploy(image="docker.io/hashicorp/vault:latest",
                            vault_addr="10.0.0.1")

        # Initialize and unseal
        out, _ = vault.operator.init(secret_shares=5, secret_threshold=3)
        init_data = json.loads(out)
        vault.token = init_data["root_token"]
        for key in init_data["unseal_keys_b64"][:3]:
            vault.operator.unseal(key=key)

        # Enable transit engine and create key
        vault.secrets.enable(path="transit", type="transit")
        vault.secrets.transit.create_key(key_name="testKey01")

        # Configure policies and auth
        vault.policy.write(name="rgw-policy", policy=hcl_content)
        vault.auth.enable(path="approle", type="approle")
        vault.auth.approle.create_role(role_name="ceph-rgw",
                                       token_policies="rgw-policy")

        # Deploy agent on a different node
        agent_vault = Vault(rgw_node, vault_url="http://10.0.0.1:8200")
        agent_vault.agent.install()
        agent_vault.agent.configure(vault_url="http://10.0.0.1:8200",
                                    role_id=role_id, secret_id=secret_id)
        agent_vault.agent.start()
    """

    def __init__(self, nodes, vault_url="http://127.0.0.1:8200"):
        super(Vault, self).__init__(nodes)
        self.vault_url = vault_url
        self.token = None
        self.operator = Operator(parent=self)
        self.secrets = Secrets(parent=self)
        self.auth = Auth(parent=self)
        self.policy = Policy(parent=self)
        self.server = Server(parent=self)
        self.agent = Agent(parent=self)

    def health(self, **kw):
        """Check the health of the Vault server.

        Returns:
            JSON response with Vault health status.
        """
        cmd = f"curl -s {self.vault_url}/v1/sys/health"
        return self.execute_as_sudo(cmd=cmd)
