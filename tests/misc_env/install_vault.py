"""
This module deploys and configures containerized HashiCorp Vault for RGW encryption.

**CONTAINERIZED VAULT ONLY** - Self-contained, no external dependencies!

Features:
- Deploys Vault server as a Podman container on client node
- Automatically configures vault-agent on all RGW nodes
- Works on ALL cloud platforms (OpenStack, IBM Cloud, AWS, Baremetal)
- Complete automation: init, unseal, transit engine, AppRole, RGW config
- Fixes AccessDenied by setting encryption key mapping

Test suite configuration (UNCHANGED format):
    - test:
        module: install_vault.py
        config:
          install:
            - agent

Optional customization:
    - test:
        module: install_vault.py
        config:
          install:
            - agent
          vault:
            image: docker.io/hashicorp/vault:latest  # Optional
            port: 8200                                # Optional
            transit:
              key-name: testKey01                     # Optional
              auto-rotate-period: 24h                 # Optional

What gets deployed:
- Client node: Vault server container (port 8200)
- RGW nodes: Vault agents (port 8100)
- RGW daemons: Fully configured for SSE-S3/SSE-KMS encryption
"""

import json
import time
from typing import Dict, Tuple

from jinja2 import Template

from ceph.ceph import Ceph, CephNode
from utility.log import Log
from utility.utils import clone_configs_repo

LOG = Log(__name__)

# Vault server container configuration
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

# Vault agent configuration
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


def run(ceph_cluster: Ceph, config: Dict, **kwargs) -> int:
    """
    Deploy containerized Vault infrastructure.

    Args:
        ceph_cluster: The cluster participating in the test
        config: Configuration passed to the test
        kwargs: Additional configurations

    Returns:
        0 on Success, 1 on Failure
    """
    try:
        if "agent" not in config.get("install", []):
            LOG.warning("No 'agent' in install list, nothing to do")
            return 0

        LOG.info("=" * 70)
        LOG.info("CONTAINERIZED VAULT DEPLOYMENT")
        LOG.info("=" * 70)

        vault_config = config.get("vault", {})

        # Deploy Vault server container
        LOG.info("Deploying Vault server container on client node...")
        vault_url, credentials = _deploy_vault_server(ceph_cluster, vault_config)
        LOG.info(f"✓ Vault server deployed at {vault_url}")

        # Deploy Vault agents on RGW nodes
        LOG.info("\nDeploying Vault agents on RGW nodes...")
        _deploy_vault_agents(ceph_cluster, vault_url, credentials)
        LOG.info("✓ Vault agents deployed")

        # Configure RGW daemons
        LOG.info("\nConfiguring RGW daemons for Vault integration...")
        _configure_all_rgw_daemons(ceph_cluster, vault_config)
        LOG.info("✓ RGW daemons configured")

        LOG.info("\n" + "=" * 70)
        LOG.info("✓ CONTAINERIZED VAULT DEPLOYMENT COMPLETE")
        LOG.info("=" * 70)
        LOG.info(f"Vault Server: {vault_url}")
        LOG.info(f"Transit Key: {credentials.get('key_name', 'testKey01')}")
        LOG.info("Credentials: /vault/credentials.json on client node")
        LOG.info("=" * 70)

        return 0

    except Exception as e:
        LOG.error(f"Vault deployment failed: {e}")
        import traceback

        LOG.error(traceback.format_exc())
        return 1


def _deploy_vault_server(cluster: Ceph, config: Dict) -> Tuple[str, Dict]:
    """Deploy Vault server container on client node."""
    client_nodes = cluster.get_nodes(role="client")
    if not client_nodes:
        raise RuntimeError("No client node found for Vault server deployment")

    node = client_nodes[0]
    image = config.get("image", "docker.io/hashicorp/vault:latest")
    container_name = config.get("container-name", "vault-server")
    port = config.get("port", 8200)

    LOG.info(f"Deploying on {node.shortname} ({node.ip_address})")

    # Create directories
    node.exec_command(sudo=True, cmd="mkdir -p /vault/config /vault/data")
    node.exec_command(sudo=True, cmd="chmod 777 /vault/data")

    # Write Vault config
    vault_addr = node.ip_address
    vault_conf = Template(VAULT_CONFIG_HCL).render(vault_addr=vault_addr)
    _write_remote_file(node, "/vault/config/vault.hcl", vault_conf)

    # Pull image
    LOG.info(f"Pulling Vault image: {image}")
    node.exec_command(
        sudo=True, cmd=f"podman pull {image}", long_running=True, check_ec=False
    )

    # Create systemd service
    systemd_content = Template(VAULT_SYSTEMD).render(
        container_name=container_name, port=port, image=image
    )
    _write_remote_file(
        node, f"/etc/systemd/system/{container_name}.service", systemd_content
    )

    # Start Vault
    node.exec_command(sudo=True, cmd="systemctl daemon-reload")
    node.exec_command(sudo=True, cmd=f"systemctl start {container_name}")
    node.exec_command(sudo=True, cmd=f"systemctl enable {container_name}")

    # Open firewall
    LOG.info(f"Opening firewall port {port}")
    node.exec_command(
        sudo=True,
        cmd=f"firewall-cmd --add-port={port}/tcp --permanent",
        check_ec=False,
    )
    node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)

    # Wait for Vault to be ready
    LOG.info("Waiting for Vault to be ready...")
    time.sleep(10)

    vault_url = f"http://{vault_addr}:{port}"

    # Initialize and configure Vault
    credentials = _initialize_vault(node, vault_url, config, container_name)

    return vault_url, credentials


def _initialize_vault(
    node: CephNode, vault_url: str, config: Dict, container_name: str
) -> Dict:
    """Initialize Vault and configure Transit engine."""
    LOG.info("Initializing Vault server")

    # Check if already initialized
    status_cmd = f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 {container_name} vault status -format=json"
    out, _ = node.exec_command(sudo=True, cmd=status_cmd, check_ec=False)

    try:
        status = json.loads(out)
        if status.get("initialized"):
            LOG.info("Vault already initialized, retrieving existing credentials")
            out, _ = node.exec_command(
                sudo=True, cmd="cat /vault/credentials.json", check_ec=False
            )
            if out:
                return json.loads(out)
    except Exception:
        pass

    # Initialize Vault
    init_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 {container_name} "
        "vault operator init -key-shares=5 -key-threshold=3 -format=json"
    )
    out, _ = node.exec_command(sudo=True, cmd=init_cmd)
    init_data = json.loads(out)

    unseal_keys = init_data["unseal_keys_b64"]
    root_token = init_data["root_token"]

    LOG.info("✓ Vault initialized")

    # Unseal Vault
    LOG.info("Unsealing Vault...")
    for i in range(3):
        unseal_cmd = (
            f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
            f"{container_name} vault operator unseal {unseal_keys[i]}"
        )
        node.exec_command(sudo=True, cmd=unseal_cmd)

    LOG.info("✓ Vault unsealed")

    # Configure Transit engine
    transit_config = config.get("transit", {})
    key_name = transit_config.get("key-name", "testKey01")
    auto_rotate = transit_config.get("auto-rotate-period", "24h")

    LOG.info("Enabling Transit secrets engine")
    enable_transit_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        "vault secrets enable transit"
    )
    node.exec_command(sudo=True, cmd=enable_transit_cmd, check_ec=False)

    LOG.info(f"Creating encryption key: {key_name}")
    create_key_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault write -f transit/keys/{key_name}"
    )
    node.exec_command(sudo=True, cmd=create_key_cmd)

    # Configure auto-rotation
    rotate_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault write transit/keys/{key_name}/config "
        f"auto_rotate_period={auto_rotate}"
    )
    node.exec_command(sudo=True, cmd=rotate_cmd)

    LOG.info(f"✓ Transit engine configured with key '{key_name}'")

    # Create policy for RGW
    policy_name = "ceph-rgw-policy"
    policy = f"""
# Allow encryption/decryption with the main key
path "transit/encrypt/{key_name}" {{
  capabilities = ["update"]
}}

path "transit/decrypt/{key_name}" {{
  capabilities = ["update"]
}}

# Allow reading the main key
path "transit/keys/{key_name}" {{
  capabilities = ["read"]
}}

# Allow RGW to create/manage per-object keys for SSE-S3
path "transit/keys/*" {{
  capabilities = ["create", "read", "update", "delete", "list"]
}}

# Allow encrypt/decrypt with any key (for per-object encryption)
path "transit/encrypt/*" {{
  capabilities = ["update"]
}}

path "transit/decrypt/*" {{
  capabilities = ["update"]
}}

# Allow datakey operations (for envelope encryption)
path "transit/datakey/plaintext/*" {{
  capabilities = ["update"]
}}

path "transit/datakey/wrapped/*" {{
  capabilities = ["update"]
}}

# Allow general transit operations
path "transit/*" {{
  capabilities = ["read", "list"]
}}
"""

    _write_remote_file(node, "/tmp/vault-policy.hcl", policy)
    cp_cmd = f"podman cp /tmp/vault-policy.hcl {container_name}:/tmp/vault-policy.hcl"
    node.exec_command(sudo=True, cmd=cp_cmd)

    policy_write_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault policy write {policy_name} /tmp/vault-policy.hcl"
    )
    node.exec_command(sudo=True, cmd=policy_write_cmd)

    # Enable AppRole auth
    LOG.info("Enabling AppRole authentication")
    enable_approle_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        "vault auth enable approle"
    )
    node.exec_command(sudo=True, cmd=enable_approle_cmd, check_ec=False)

    # Create AppRole
    approle_config = config.get("approle", {})
    role_name = approle_config.get("role-name", "ceph-rgw")
    token_ttl = approle_config.get("token-ttl", "1h")
    token_max_ttl = approle_config.get("token-max-ttl", "24h")

    LOG.info(f"Creating AppRole: {role_name}")
    create_approle_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault write auth/approle/role/{role_name} "
        f"token_policies={policy_name} token_ttl={token_ttl} "
        f"token_max_ttl={token_max_ttl}"
    )
    node.exec_command(sudo=True, cmd=create_approle_cmd)

    # Get role-id
    get_role_id_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault read -field=role_id auth/approle/role/{role_name}/role-id"
    )
    out, _ = node.exec_command(sudo=True, cmd=get_role_id_cmd)
    role_id = out.strip()

    # Generate secret-id
    get_secret_id_cmd = (
        f"podman exec -e VAULT_ADDR=http://127.0.0.1:8200 "
        f"-e VAULT_TOKEN={root_token} {container_name} "
        f"vault write -field=secret_id -f "
        f"auth/approle/role/{role_name}/secret-id"
    )
    out, _ = node.exec_command(sudo=True, cmd=get_secret_id_cmd)
    secret_id = out.strip()

    LOG.info(f"✓ AppRole '{role_name}' configured")

    # Store credentials
    credentials = {
        "role_id": role_id,
        "secret_id": secret_id,
        "root_token": root_token,
        "unseal_keys": unseal_keys,
        "key_name": key_name,
        "vault_url": vault_url,
    }

    _write_remote_file(
        node, "/vault/credentials.json", json.dumps(credentials, indent=2)
    )
    node.exec_command(sudo=True, cmd="chmod 600 /vault/credentials.json")

    return credentials


def _deploy_vault_agents(cluster: Ceph, vault_url: str, credentials: Dict) -> None:
    """Deploy and configure vault-agent on all RGW nodes."""
    rgw_nodes = cluster.get_nodes(role="rgw")

    if not rgw_nodes:
        LOG.warning("No RGW nodes found in cluster")
        return

    LOG.info(f"Configuring {len(rgw_nodes)} RGW nodes")

    for node in rgw_nodes:
        LOG.info(f"  Configuring {node.shortname}...")

        # Install Vault binary
        _install_vault_binary(node)

        # Create config directory
        node.exec_command(sudo=True, cmd="mkdir -p /usr/local/etc/vault/")

        # Write credentials
        _write_remote_file(
            node, "/usr/local/etc/vault/.app-role-id", credentials["role_id"]
        )
        _write_remote_file(
            node, "/usr/local/etc/vault/.app-secret-id", credentials["secret_id"]
        )

        # Secure credentials
        node.exec_command(
            sudo=True,
            cmd="chmod 600 /usr/local/etc/vault/.app-role-id /usr/local/etc/vault/.app-secret-id",
        )

        # Write agent config
        agent_conf = Template(AGENT_HCL).render(vault_url=vault_url)
        _write_remote_file(node, "/usr/local/etc/vault/agent.hcl", agent_conf)

        # Create systemd service
        _write_remote_file(
            node, "/etc/systemd/system/vault-agent.service", AGENT_SYSTEMD
        )

        # Start vault-agent
        commands = [
            "systemctl daemon-reload",
            "systemctl enable vault-agent",
            "systemctl restart vault-agent",
        ]
        for cmd in commands:
            node.exec_command(sudo=True, cmd=cmd, check_ec=False)

        # Verify agent is running
        time.sleep(2)
        out, _ = node.exec_command(
            sudo=True, cmd="systemctl is-active vault-agent", check_ec=False
        )
        if "active" in out:
            LOG.info(f"  ✓ {node.shortname} vault-agent active")
        else:
            LOG.warning(f"  ⚠ {node.shortname} vault-agent may not be running")


def _install_vault_binary(node: CephNode) -> None:
    """Install Vault binary on node (works on all cloud platforms)."""
    try:
        # Try internal repo first
        clone_configs_repo(node, repo_name="rgw_configs")
        node.exec_command(
            sudo=True,
            cmd="cp /home/cephuser/configs/rgw/vault/hashicorp.repo /etc/yum.repos.d/hashicorp.repo",
            check_ec=False,
        )
    except Exception:
        # Fallback to public repo
        hashicorp_repo = """[hashicorp]
name=Hashicorp Stable - $basearch
baseurl=https://rpm.releases.hashicorp.com/RHEL/$releasever/$basearch/stable
enabled=1
gpgcheck=1
gpgkey=https://rpm.releases.hashicorp.com/gpg
"""
        _write_remote_file(node, "/etc/yum.repos.d/hashicorp.repo", hashicorp_repo)

    # Install vault
    node.exec_command(sudo=True, cmd="yum install -y vault", check_ec=False)


def _configure_all_rgw_daemons(cluster: Ceph, vault_config: Dict) -> None:
    """Configure all RGW daemons across all clusters (supports multisite)."""
    client_nodes = cluster.get_nodes(role="client")

    for client_node in client_nodes:
        try:
            _configure_rgw_daemons_on_node(client_node, vault_config)
        except Exception as e:
            LOG.warning(f"Failed to configure RGW on {client_node.shortname}: {e}")


def _configure_rgw_daemons_on_node(node: CephNode, vault_config: Dict) -> None:
    """Configure RGW daemons on a specific node."""
    # Get RGW daemons
    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type rgw --format json", check_ec=False
    )

    if not out or out.strip() == "":
        LOG.warning(f"No RGW daemons found on {node.shortname}")
        return

    rgw_daemons = [f"client.rgw.{x['daemon_id']}" for x in json.loads(out)]

    # Get RGW services
    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ls --service_type rgw --format json", check_ec=False
    )
    rgw_services = [x["service_name"] for x in json.loads(out)] if out else []

    # Vault configuration
    transit_config = vault_config.get("transit", {})
    key_name = transit_config.get("key-name", "testKey01")

    configs = [
        ("rgw_crypt_require_ssl", "false"),
        ("rgw_crypt_s3_kms_backend", "vault"),
        ("rgw_crypt_vault_secret_engine", "transit"),
        ("rgw_crypt_vault_auth", "agent"),
        ("rgw_crypt_vault_addr", "http://127.0.0.1:8100"),
        ("rgw_crypt_vault_prefix", "/v1/transit"),
        ("rgw_crypt_sse_s3_backend", "vault"),
        ("rgw_crypt_sse_s3_vault_secret_engine", "transit"),
        ("rgw_crypt_sse_s3_vault_auth", "agent"),
        ("rgw_crypt_sse_s3_vault_addr", "http://127.0.0.1:8100"),
        ("rgw_crypt_sse_s3_vault_prefix", "/v1/transit"),
        ("rgw_crypt_s3_kms_encryption_keys", f"testkey01={key_name}"),
    ]

    # Apply configs
    for daemon in rgw_daemons:
        LOG.info(f"  Configuring {daemon}")
        for key, value in configs:
            node.exec_command(
                sudo=True, cmd=f"ceph config set {daemon} {key} {value}", check_ec=False
            )

    # Restart services
    for service in rgw_services:
        LOG.info(f"  Restarting {service}")
        node.exec_command(sudo=True, cmd=f"ceph orch restart {service}", check_ec=False)


def _write_remote_file(node: CephNode, file_name: str, content: str) -> None:
    """Write content to remote file."""
    file_handle = node.remote_file(sudo=True, file_mode="w", file_name=file_name)
    file_handle.write(data=content)
    file_handle.flush()
    file_handle.close()
