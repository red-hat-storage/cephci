import requests as _requests

from cli import Cli
from utility.log import Log

from .agent import Agent
from .auth import Auth
from .operator import Operator
from .policy import Policy
from .secrets import Secrets
from .server import Server

LOG = Log(__name__)


class Vault(Cli):
    """CLI wrapper for HashiCorp Vault operations.

    Provides a consistent interface for interacting with a containerized
    Vault server via REST API and SSH for system-level operations.

    REST API calls (init, unseal, secrets, auth, policy) use the Python
    ``requests`` library directly from the executor host.  System-level
    operations (container lifecycle, agent install/config) continue to
    run over SSH on the target nodes.
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

    def _request(self, method, path, data=None, token_required=True):
        """Make an HTTP request to the Vault REST API.

        Args:
            method: HTTP method (GET, POST, DELETE, LIST).
            path: API path (e.g. "/v1/sys/init").
            data: JSON-serialisable payload for POST requests.
            token_required: Include X-Vault-Token header when True.

        Returns:
            Parsed JSON response as a dict, or {} for empty responses.
        """
        url = f"{self.vault_url}{path}"
        headers = {}
        if token_required and self.token:
            headers["X-Vault-Token"] = self.token

        LOG.debug(f"Vault API: {method} {url}")
        resp = _requests.request(method, url, json=data, headers=headers)
        resp.raise_for_status()

        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    def health(self, **kw):
        """Check the health of the Vault server.

        Returns:
            dict: Vault health status.
        """
        return self._request("GET", "/v1/sys/health", token_required=False)
