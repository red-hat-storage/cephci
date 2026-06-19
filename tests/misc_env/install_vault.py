"""
This module deploys and configures containerized HashiCorp Vault for RGW encryption.

**CONTAINERIZED VAULT ONLY** - Self-contained, no external dependencies!

Features:
- Deploys Vault server as a Podman container on client node
- Automatically configures vault-agent on all RGW nodes
- Works on ALL cloud platforms (OpenStack, IBM Cloud, AWS, Baremetal)
- Complete automation: init, unseal, transit engine, AppRole, RGW config
- Uses REST API for all Vault operations (consistent with cli/ pattern)

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

from ceph.ceph import Ceph, CephNode
from cli.vault.vault import Vault
from utility.log import Log

LOG = Log(__name__)

# Transit policy template for RGW encryption
TRANSIT_POLICY_HCL = """
path "transit/encrypt/{key_name}" {{
  capabilities = ["update"]
}}

path "transit/decrypt/{key_name}" {{
  capabilities = ["update"]
}}

path "transit/keys/{key_name}" {{
  capabilities = ["read"]
}}

path "transit/keys/*" {{
  capabilities = ["create", "read", "update", "delete", "list"]
}}

path "transit/encrypt/*" {{
  capabilities = ["update"]
}}

path "transit/decrypt/*" {{
  capabilities = ["update"]
}}

path "transit/datakey/plaintext/*" {{
  capabilities = ["update"]
}}

path "transit/datakey/wrapped/*" {{
  capabilities = ["update"]
}}

path "transit/*" {{
  capabilities = ["read", "list"]
}}
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

        LOG.info("Deploying Vault server container on client node...")
        vault_url, credentials = _deploy_vault_server(ceph_cluster, vault_config)
        LOG.info(f"Vault server deployed at {vault_url}")

        LOG.info("Deploying Vault agents on RGW nodes...")
        _deploy_vault_agents(ceph_cluster, vault_url, credentials)
        LOG.info("Vault agents deployed")

        LOG.info("Configuring RGW daemons for Vault integration...")
        _configure_all_rgw_daemons(ceph_cluster, vault_config)
        LOG.info("RGW daemons configured")

        LOG.info("=" * 70)
        LOG.info("CONTAINERIZED VAULT DEPLOYMENT COMPLETE")
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
    port = config.get("port", 8200)

    LOG.info(f"Deploying on {node.shortname} ({node.ip_address})")

    vault_url = f"http://{node.ip_address}:{port}"
    vault = Vault(node, vault_url=vault_url)

    vault.server.deploy(
        image=image,
        port=port,
        **{"container-name": config.get("container-name", "vault-server")},
        **{"vault-addr": node.ip_address},
    )

    LOG.info("Waiting for Vault to be ready...")
    time.sleep(10)

    credentials = _initialize_vault(vault, config)

    return vault_url, credentials


def _initialize_vault(vault: Vault, config: Dict) -> Dict:
    """Initialize Vault and configure Transit engine via REST API."""
    LOG.info("Initializing Vault server")

    status = vault.operator.init_status()
    try:
        if status.get("initialized"):
            LOG.info("Vault already initialized, retrieving existing credentials")
            if isinstance(vault.ctx, list):
                node = vault.ctx[0]
            else:
                node = vault.ctx
            creds_out, _ = node.exec_command(
                sudo=True, cmd="cat /vault/credentials.json", check_ec=False
            )
            if creds_out:
                return json.loads(creds_out)
    except Exception:
        pass

    init_data = vault.operator.init(**{"secret-shares": 5, "secret-threshold": 3})

    unseal_keys = init_data["unseal_keys_b64"]
    root_token = init_data["root_token"]
    LOG.info("Vault initialized")

    LOG.info("Unsealing Vault...")
    for i in range(3):
        vault.operator.unseal(key=unseal_keys[i])
    LOG.info("Vault unsealed")

    vault.token = root_token

    transit_config = config.get("transit", {})
    key_name = transit_config.get("key-name", "testKey01")
    auto_rotate = transit_config.get("auto-rotate-period", "24h")

    LOG.info("Enabling Transit secrets engine")
    vault.secrets.enable(path="transit", type="transit")

    LOG.info(f"Creating encryption key: {key_name}")
    vault.secrets.transit.create_key(**{"key-name": key_name})

    LOG.info("Configuring auto-rotation")
    vault.secrets.transit.configure_key(
        **{"key-name": key_name, "auto-rotate-period": auto_rotate}
    )
    LOG.info(f"Transit engine configured with key '{key_name}'")

    policy_name = "ceph-rgw-policy"
    policy = TRANSIT_POLICY_HCL.format(key_name=key_name)
    LOG.info(f"Writing policy: {policy_name}")
    vault.policy.write(name=policy_name, policy=policy)

    LOG.info("Enabling AppRole authentication")
    vault.auth.enable(path="approle", type="approle")

    approle_config = config.get("approle", {})
    role_name = approle_config.get("role-name", "ceph-rgw")
    token_ttl = approle_config.get("token-ttl", "1h")
    token_max_ttl = approle_config.get("token-max-ttl", "24h")

    LOG.info(f"Creating AppRole: {role_name}")
    vault.auth.approle.create_role(
        **{
            "role-name": role_name,
            "token-policies": policy_name,
            "token-ttl": token_ttl,
            "token-max-ttl": token_max_ttl,
        }
    )

    role_id_data = vault.auth.approle.read_role_id(**{"role-name": role_name})
    role_id = role_id_data["data"]["role_id"]

    secret_id_data = vault.auth.approle.generate_secret_id(**{"role-name": role_name})
    secret_id = secret_id_data["data"]["secret_id"]

    LOG.info(f"AppRole '{role_name}' configured")

    credentials = {
        "role_id": role_id,
        "secret_id": secret_id,
        "root_token": root_token,
        "unseal_keys": unseal_keys,
        "key_name": key_name,
        "vault_url": vault.vault_url,
    }

    _write_remote_file(
        vault.ctx if not isinstance(vault.ctx, list) else vault.ctx[0],
        "/vault/credentials.json",
        json.dumps(credentials, indent=2),
    )
    node = vault.ctx if not isinstance(vault.ctx, list) else vault.ctx[0]
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

        vault_agent = Vault(node, vault_url=vault_url)

        vault_agent.agent.install()

        vault_agent.agent.configure(
            **{
                "vault-url": vault_url,
                "role-id": credentials["role_id"],
                "secret-id": credentials["secret_id"],
            }
        )

        vault_agent.agent.start()

        time.sleep(2)
        out, _ = vault_agent.agent.status()
        if "active" in out:
            LOG.info(f"  {node.shortname} vault-agent active")
        else:
            LOG.warning(f"  {node.shortname} vault-agent may not be running")


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
    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type rgw --format json", check_ec=False
    )

    if not out or out.strip() == "":
        LOG.warning(f"No RGW daemons found on {node.shortname}")
        return

    rgw_daemons = [f"client.rgw.{x['daemon_id']}" for x in json.loads(out)]

    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ls --service_type rgw --format json", check_ec=False
    )
    rgw_services = [x["service_name"] for x in json.loads(out)] if out else []

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

    for daemon in rgw_daemons:
        LOG.info(f"  Configuring {daemon}")
        for key, value in configs:
            node.exec_command(
                sudo=True, cmd=f"ceph config set {daemon} {key} {value}", check_ec=False
            )

    for service in rgw_services:
        LOG.info(f"  Restarting {service}")
        node.exec_command(sudo=True, cmd=f"ceph orch restart {service}", check_ec=False)


def _write_remote_file(node: CephNode, file_name: str, content: str) -> None:
    """Write content to remote file."""
    file_handle = node.remote_file(sudo=True, file_mode="w", file_name=file_name)
    file_handle.write(data=content)
    file_handle.flush()
    file_handle.close()
