from copy import deepcopy

from jinja2 import Template

AGENT_HCL = """pid_file = "/run/vault-agent-pid"

auto_auth {
  method "approle" {
    mount_path = "auth/approle"
    config = {
      role_id_file_path = "/usr/local/etc/vault/.app-role-id"
      secret_id_file_path = "/usr/local/etc/vault/.app-secret-id"
      remove_secret_id_file_after_reading = "false"
    }
  }
}

cache {
  use_auto_auth_token = true
}

listener "tcp" {
  address = "127.0.0.1:8100"
  tls_disable = true
}

vault {
  address = "{{ vault_url }}"
}
"""

AGENT_SYSTEMD = """[Unit]
Description=HashiCorp Vault Agent
After=network.target

[Service]
ExecStart=/usr/bin/vault agent -config /usr/local/etc/vault/agent.hcl
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""


class Agent(object):
    """Vault agent lifecycle management.

    Handles installing the Vault binary, configuring the agent
    with AppRole credentials, and managing the systemd service.
    """

    def __init__(self, parent):
        self.parent = parent

    def install(self, **kw):
        """Install the Vault binary on the node.

        Args:
            kw(dict): Key/value pairs for installation.
                Supported keys:
                    repo-url(str): Custom HashiCorp repo URL
        """
        kw_copy = deepcopy(kw)
        repo_url = kw_copy.pop("repo-url", "")
        if repo_url:
            repo_content = (
                "[hashicorp]\n"
                "name=Hashicorp Stable - $basearch\n"
                f"baseurl={repo_url}\n"
                "enabled=1\n"
                "gpgcheck=1\n"
                "gpgkey=https://rpm.releases.hashicorp.com/gpg\n"
            )
            self._write_file("/etc/yum.repos.d/hashicorp.repo", repo_content)
        else:
            repo_content = (
                "[hashicorp]\n"
                "name=Hashicorp Stable - $basearch\n"
                "baseurl=https://rpm.releases.hashicorp.com/RHEL/"
                "$releasever/$basearch/stable\n"
                "enabled=1\n"
                "gpgcheck=1\n"
                "gpgkey=https://rpm.releases.hashicorp.com/gpg\n"
            )
            self._write_file("/etc/yum.repos.d/hashicorp.repo", repo_content)

        return self.parent.execute_as_sudo(cmd="yum install -y vault", check_ec=False)

    def configure(self, **kw):
        """Configure the Vault agent with AppRole credentials.

        Args:
            kw(dict): Key/value pairs for agent configuration.
                Supported keys:
                    vault-url(str): URL of the Vault server
                    role-id(str): AppRole role ID
                    secret-id(str): AppRole secret ID
        """
        kw_copy = deepcopy(kw)
        vault_url = kw_copy.pop("vault-url", "")
        role_id = kw_copy.pop("role-id", "")
        secret_id = kw_copy.pop("secret-id", "")

        self.parent.execute_as_sudo(cmd="mkdir -p /usr/local/etc/vault/")

        self._write_file("/usr/local/etc/vault/.app-role-id", role_id)
        self._write_file("/usr/local/etc/vault/.app-secret-id", secret_id)

        self.parent.execute_as_sudo(
            cmd=(
                "chmod 600"
                " /usr/local/etc/vault/.app-role-id"
                " /usr/local/etc/vault/.app-secret-id"
            )
        )

        agent_conf = Template(AGENT_HCL).render(vault_url=vault_url)
        self._write_file("/usr/local/etc/vault/agent.hcl", agent_conf)

        self._write_file("/etc/systemd/system/vault-agent.service", AGENT_SYSTEMD)

    def start(self, **kw):
        """Start the Vault agent systemd service."""
        self.parent.execute_as_sudo(cmd="systemctl daemon-reload")
        self.parent.execute_as_sudo(cmd="systemctl enable vault-agent")
        return self.parent.execute_as_sudo(
            cmd="systemctl restart vault-agent", check_ec=False
        )

    def stop(self, **kw):
        """Stop the Vault agent systemd service."""
        return self.parent.execute_as_sudo(cmd="systemctl stop vault-agent")

    def status(self, **kw):
        """Check the Vault agent service status."""
        return self.parent.execute_as_sudo(
            cmd="systemctl is-active vault-agent", check_ec=False
        )

    def _write_file(self, file_name, content):
        """Write content to a remote file on the node."""
        if isinstance(self.parent.ctx, list):
            node = self.parent.ctx[0]
        else:
            node = self.parent.ctx
        fh = node.remote_file(sudo=True, file_mode="w", file_name=file_name)
        fh.write(data=content)
        fh.flush()
        fh.close()
