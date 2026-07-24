from copy import deepcopy

from jinja2 import Template

VAULT_CONFIG_HCL = """
storage "file" {
  path = "/vault/data"
}

listener "tcp" {
  address = "0.0.0.0:8200"
  tls_disable = true
}

api_addr = "http://{{ vault_addr }}:8200"
ui = true
"""

VAULT_SYSTEMD = """[Unit]
Description=HashiCorp Vault Server Container
After=network.target podman.service
Requires=podman.service

[Service]
Type=simple
ExecStartPre=-/usr/bin/podman rm -f {{ container_name }}
ExecStart=/usr/bin/podman run --name {{ container_name }} \\
  -p {{ port }}:8200 \\
  -v /vault/config:/vault/config:Z \\
  -v /vault/data:/vault/data:Z \\
  --cap-add=IPC_LOCK \\
  {{ image }} server
ExecStop=/usr/bin/podman stop -t 10 {{ container_name }}
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
"""


class Server(object):
    """Vault server container lifecycle management.

    Handles deploying, starting, stopping, and checking the status
    of the containerized Vault server via podman and systemd.
    """

    def __init__(self, parent):
        self.parent = parent

    def deploy(self, **kw):
        """Deploy Vault server as a podman container with systemd.

        Args:
            kw(dict): Key/value pairs for deployment.
                Supported keys:
                    image(str): Container image (default: docker.io/hashicorp/vault:latest)
                    container-name(str): Container name (default: vault-server)
                    port(int): Host port to bind (default: 8200)
                    vault-addr(str): Vault server IP address
        """
        kw_copy = deepcopy(kw)
        image = kw_copy.pop("image", "docker.io/hashicorp/vault:latest")
        container_name = kw_copy.pop("container-name", "vault-server")
        port = kw_copy.pop("port", 8200)
        vault_addr = kw_copy.pop("vault-addr", "127.0.0.1")

        self.parent.execute_as_sudo(cmd="mkdir -p /vault/config /vault/data")
        self.parent.execute_as_sudo(cmd="chmod 777 /vault/data")

        vault_conf = Template(VAULT_CONFIG_HCL).render(vault_addr=vault_addr)
        self._write_file("/vault/config/vault.hcl", vault_conf)

        self.parent.execute_as_sudo(cmd=f"podman pull {image}", long_running=True)

        systemd_content = Template(VAULT_SYSTEMD).render(
            container_name=container_name, port=port, image=image
        )
        self._write_file(
            f"/etc/systemd/system/{container_name}.service", systemd_content
        )

        self.parent.execute_as_sudo(cmd="systemctl daemon-reload")
        self.parent.execute_as_sudo(cmd=f"systemctl start {container_name}")
        self.parent.execute_as_sudo(cmd=f"systemctl enable {container_name}")

        self.parent.execute_as_sudo(
            cmd=f"firewall-cmd --add-port={port}/tcp --permanent",
            check_ec=False,
        )
        self.parent.execute_as_sudo(cmd="firewall-cmd --reload", check_ec=False)

    def start(self, **kw):
        """Start the Vault server container.

        Args:
            kw(dict): Key/value pairs.
                Supported keys:
                    container-name(str): Container name (default: vault-server)
        """
        kw_copy = deepcopy(kw)
        container_name = kw_copy.pop("container-name", "vault-server")
        cmd = f"systemctl start {container_name}"
        return self.parent.execute_as_sudo(cmd=cmd)

    def stop(self, **kw):
        """Stop the Vault server container.

        Args:
            kw(dict): Key/value pairs.
                Supported keys:
                    container-name(str): Container name (default: vault-server)
        """
        kw_copy = deepcopy(kw)
        container_name = kw_copy.pop("container-name", "vault-server")
        cmd = f"systemctl stop {container_name}"
        return self.parent.execute_as_sudo(cmd=cmd)

    def status(self, **kw):
        """Check the status of the Vault server container.

        Args:
            kw(dict): Key/value pairs.
                Supported keys:
                    container-name(str): Container name (default: vault-server)
        """
        kw_copy = deepcopy(kw)
        container_name = kw_copy.pop("container-name", "vault-server")
        cmd = f"systemctl is-active {container_name}"
        return self.parent.execute_as_sudo(cmd=cmd, check_ec=False)

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
