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

AUTO_UNSEAL_SCRIPT = """#!/bin/bash
# Auto-unseal Vault when sealed (e.g. after container restart).
# Deployed by cephci install_vault.py.

VAULT_ADDR="{{ vault_url }}"
KEYS_FILE="/vault/unseal-keys.json"

if [ ! -f "$KEYS_FILE" ]; then
    echo "No unseal keys file found at $KEYS_FILE"
    exit 1
fi

THRESHOLD=$(jq -r '.threshold' "$KEYS_FILE")
KEYS=$(jq -r '.keys[]' "$KEYS_FILE")

wait_for_api() {
    for i in $(seq 1 60); do
        if curl -sf "$VAULT_ADDR/v1/sys/health" -o /dev/null 2>/dev/null; then
            return 0
        fi
        # 501 = not initialized, 503 = sealed — both mean API is up
        CODE=$(curl -sf -o /dev/null -w '%{http_code}' "$VAULT_ADDR/v1/sys/health" 2>/dev/null)
        if [ "$CODE" = "501" ] || [ "$CODE" = "503" ]; then
            return 0
        fi
        sleep 2
    done
    echo "Vault API not reachable after 120s"
    return 1
}

wait_for_api || exit 1

SEALED=$(curl -sf "$VAULT_ADDR/v1/sys/seal-status" 2>/dev/null | jq -r '.sealed')

if [ "$SEALED" != "true" ]; then
    echo "Vault is already unsealed"
    exit 0
fi

echo "Vault is sealed, unsealing..."
COUNT=0
for KEY in $KEYS; do
    curl -sf -X POST "$VAULT_ADDR/v1/sys/unseal" \
        -d "{\\"key\\": \\"$KEY\\"}" > /dev/null 2>&1
    COUNT=$((COUNT + 1))
    if [ "$COUNT" -ge "$THRESHOLD" ]; then
        break
    fi
done

SEALED=$(curl -sf "$VAULT_ADDR/v1/sys/seal-status" 2>/dev/null | jq -r '.sealed')
if [ "$SEALED" = "false" ]; then
    echo "Vault successfully unsealed"
else
    echo "Failed to unseal Vault"
    exit 1
fi
"""

AUTO_UNSEAL_SYSTEMD = """[Unit]
Description=Auto-unseal Vault after container start
After={{ container_name }}.service
Requires={{ container_name }}.service

[Service]
Type=oneshot
ExecStartPre=/bin/sleep 5
ExecStart=/bin/bash /vault/auto-unseal.sh
Restart=on-failure
RestartSec=15
StartLimitIntervalSec=300
StartLimitBurst=10
StandardOutput=journal
StandardError=journal

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

    def enable_auto_unseal(self, **kw):
        """Deploy an auto-unseal systemd service that unseals Vault on restart.

        Args:
            kw(dict): Key/value pairs for auto-unseal.
                Supported keys:
                    unseal-keys(list): Base64 unseal key shares
                    threshold(int): Number of keys needed to unseal
                    vault-url(str): Vault server URL
                    container-name(str): Container name (default: vault-server)
        """
        import json

        kw_copy = deepcopy(kw)
        unseal_keys = kw_copy.pop("unseal-keys", [])
        threshold = kw_copy.pop("threshold", 3)
        vault_url = kw_copy.pop("vault-url", "")
        container_name = kw_copy.pop("container-name", "vault-server")

        keys_data = json.dumps({"keys": unseal_keys, "threshold": threshold})
        self._write_file("/vault/unseal-keys.json", keys_data)
        self.parent.execute_as_sudo(cmd="chmod 600 /vault/unseal-keys.json")

        script = Template(AUTO_UNSEAL_SCRIPT).render(vault_url=vault_url)
        self._write_file("/vault/auto-unseal.sh", script)
        self.parent.execute_as_sudo(cmd="chmod 700 /vault/auto-unseal.sh")

        unit = Template(AUTO_UNSEAL_SYSTEMD).render(container_name=container_name)
        self._write_file("/etc/systemd/system/vault-auto-unseal.service", unit)

        self.parent.execute_as_sudo(cmd="systemctl daemon-reload")
        self.parent.execute_as_sudo(cmd="systemctl enable vault-auto-unseal")
        self.parent.execute_as_sudo(
            cmd="systemctl start vault-auto-unseal", check_ec=False
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
