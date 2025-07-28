"""
This module installs and configures HashiCorp's Vault on the given node.

Support for configuring vault-agent is also supported in this module. The configuration
is done based on the inputs provided in .cephci.yaml file.

In case of vault-agent configuration, the following information is required in
.cephci.yaml

Example:

    vault:
        url: http://<vault-server>/
        agent:
          auth: agent
          engine: transit
          role-id: <role-id>
          secret-id: <secret-id>
          prefix: /v1/<path>

ToDo:
  - configure server
  - support TLS
  - support token auth method
"""

from json import loads
from typing import Dict

from jinja2 import Template

from ceph.ceph import Ceph, CephNode
from utility.log import Log
from utility.utils import get_cephci_config

LOG = Log(__name__)

AGENT_HCL = """pid_file = "/run/vault-agent-pid"

auto_auth {
  method "AppRole" {
    mount_path = "auth/approle"
    config = {
      role_id_file_path = "/usr/local/etc/vault/.app-role-id"
      secret_id_file_path = "/usr/local/etc/vault/.app-secret-id"
      remove_secret_id_file_after_reading = "false"
    }
  }
}
{%- if data.auth == "token" %}
sink "file" {
  config = {
    path = {{ data.token.file }}
  }
}
{%- endif %}

{%- if data.auth == "agent" %}
cache {
  use_auto_auth_token = true
}

listener "tcp" {
  address = "127.0.0.1:8100"
  tls_disable = true
}
{%- endif %}

vault {
  address = "{{ data.url }}"
}
"""

AGENT_SYSTEMD = """[Unit]
Description=HashiCorp Vault agent

[Service]
ExecStart=/usr/bin/vault-agent
Restart=on-failure

[Install]
WantedBy=multi-user.target
"""

AGENT_LAUNCHER = """#!/bin/sh
/bin/vault agent -config /usr/local/etc/vault/agent.hcl
"""


def run(ceph_cluster: Ceph, config: Dict, **kwargs) -> int:
    """
    Entry point for module execution.

    Args:
        ceph_cluster    The cluster participating in the test.
        config          Configuration passed to the test.
        kwargs          Additional configurations passed to the test.

    Returns:
        0 on Success else 1
    """
    if "agent" not in config.get("install", []):
        return 0

    cephci_cfg = get_cephci_config()
    vault_cfg = cephci_cfg.get("vault", {})

    # Determine the cloud type explicitly or default to 'openstack'
    cloud_type = config.get("cloud_type", "openstack").lower()

    if cloud_type not in vault_cfg:
        raise ValueError(
            f"Invalid or missing Vault config for cloud_type '{cloud_type}' in cephci.yaml. "
            f"Expected one of: {', '.join(vault_cfg.keys())}"
        )

    selected_cfg = vault_cfg[cloud_type]

    if not selected_cfg.get("url"):
        raise ValueError(
            f"Missing 'url' in Vault config for cloud_type '{cloud_type}'."
        )

    _install_agent(ceph_cluster, selected_cfg)

    try:
        client_node = ceph_cluster.get_nodes(role="client")[0]
    except IndexError:
        raise RuntimeError(
            "No client node found in the cluster to configure RGW daemons."
        )

    _configure_rgw_daemons(client_node, selected_cfg)
    return 0


def _write_remote_file(node: CephNode, file_name: str, content: str) -> None:
    """
    Copies the provided content to the specified file on the given node.
    """
    LOG.debug(f"{node.shortname}: Writing to remote file {file_name}")
    file_handle = node.remote_file(sudo=True, file_mode="w", file_name=file_name)
    file_handle.write(data=content)
    file_handle.flush()
    file_handle.close()


def _install_agent(cluster: Ceph, config: Dict) -> None:
    """
    Installs and configures the vault-agent on all RGW nodes.
    """
    rgw_nodes = cluster.get_nodes(role="rgw")
    for node in rgw_nodes:
        LOG.debug(f"{node.shortname}: Installing and configuring Vault agent")
        _install_vault_packages(node)
        _create_agent_config(node, config)
        _create_agent_systemd(node)


def _install_vault_packages(node: CephNode) -> None:
    """
    Installs the required packages for Vault based on IBM-Ceph or RH-Ceph environment.
    """
    try:
        out = node.exec_command(sudo=True, cmd="podman ps", check_ec=False)[1]

        if "ibm-ceph" in out:
            LOG.debug(f"{node.shortname}: Detected IBM-Ceph environment")

            repo_cmd = """
cat <<EOF | sudo tee /etc/yum.repos.d/hashicorp.repo
[hashicorp]
name=Hashicorp Stable - $basearch
baseurl=https://rpm.releases.hashicorp.com/RHEL/9/$basearch/stable
enabled=1
gpgcheck=1
gpgkey=https://rpm.releases.hashicorp.com/gpg

[hashicorp=test]
name=Hashicorp Test - $basearch
baseurl=https://rpm.releases.hashicorp.com/RHEL/9/$basearch/stable
enabled=0
gpgcheck=1
gpgkey=https://rpm.releases.hashicorp.com/gpg
EOF
"""
            node.exec_command(sudo=True, cmd=repo_cmd.strip(), check_ec=False)

        else:
            LOG.debug(f"{node.shortname}: Detected RH-Ceph or default environment")

            wget_cmd = (
                "curl -o /etc/yum.repos.d/hashicorp.repo "
                "http://magna002.ceph.redhat.com/cephci-jenkins/hashicorp.repo"
            )
            node.exec_command(sudo=True, cmd=wget_cmd, check_ec=False)

        install_cmd = "yum install -y vault"
        node.exec_command(sudo=True, cmd=install_cmd, check_ec=False)

    except Exception as e:
        raise RuntimeError(f"{node.shortname}: Failed to install Vault - {str(e)}")


def _create_agent_config(node: CephNode, config: Dict) -> None:
    """
    Writes required agent configuration files to the node.
    """
    node.exec_command(sudo=True, cmd="mkdir -p /usr/local/etc/vault/")

    _write_remote_file(
        node=node,
        file_name="/usr/local/etc/vault/.app-role-id",
        content=config["agent"]["role-id"],
    )
    _write_remote_file(
        node=node,
        file_name="/usr/local/etc/vault/.app-secret-id",
        content=config["agent"]["secret-id"],
    )

    agent_conf = {
        "url": config["url"],
        "auth": config["agent"]["auth"],
        "token": {"file": config["agent"].get("token_file", "")},
    }
    tmpl = Template(AGENT_HCL)
    data = tmpl.render(data=agent_conf)

    _write_remote_file(
        node=node,
        file_name="/usr/local/etc/vault/agent.hcl",
        content=data,
    )


def _create_agent_systemd(node: CephNode) -> None:
    """
    Configures vault-agent as a systemd service.
    """
    _write_remote_file(
        node=node,
        file_name="/usr/bin/vault-agent",
        content=AGENT_LAUNCHER,
    )
    _write_remote_file(
        node=node,
        file_name="/usr/lib/systemd/system/vault-agent.service",
        content=AGENT_SYSTEMD,
    )

    commands = [
        "chmod +x /usr/bin/vault-agent",
        "systemctl start vault-agent.service",
        "systemctl enable vault-agent.service",
    ]
    for command in commands:
        node.exec_command(sudo=True, cmd=command)


def _configure_rgw_daemons(node: CephNode, config: Dict) -> None:
    """
    Updates RGW daemons with Vault config.
    """
    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type rgw --format json"
    )
    rgw_daemons = [f"client.rgw.{x['daemon_id']}" for x in loads(out)]

    out, _ = node.exec_command(
        sudo=True, cmd="ceph orch ls --service_type rgw --format json"
    )
    rgw_services = [x["service_name"] for x in loads(out)]

    configs = [
        ("rgw_crypt_s3_kms_backend", "vault"),
        ("rgw_crypt_vault_secret_engine", config["agent"]["engine"]),
        ("rgw_crypt_vault_auth", config["agent"]["auth"]),
        ("rgw_crypt_sse_s3_backend", "vault"),
        ("rgw_crypt_sse_s3_vault_secret_engine", config["agent"]["engine"]),
        ("rgw_crypt_sse_s3_vault_auth", config["agent"]["auth"]),
    ]

    if config["agent"]["auth"] == "token":
        configs += [
            ("rgw_crypt_vault_token_file", config["agent"]["token_file"]),
            ("rgw_crypt_vault_addr", config["url"]),
            ("rgw_crypt_sse_s3_vault_token_file", config["agent"]["token_file"]),
            ("rgw_crypt_sse_s3_vault_addr", config["url"]),
        ]
    else:
        configs += [
            ("rgw_crypt_vault_prefix", config["agent"]["prefix"]),
            ("rgw_crypt_vault_addr", "http://127.0.0.1:8100"),
            ("rgw_crypt_sse_s3_vault_prefix", config["agent"]["prefix"]),
            ("rgw_crypt_sse_s3_vault_addr", "http://127.0.0.1:8100"),
        ]

    for daemon in rgw_daemons:
        for key, value in configs:
            node.exec_command(sudo=True, cmd=f"ceph config set {daemon} {key} {value}")

    for service in rgw_services:
        node.exec_command(sudo=True, cmd=f"ceph orch restart {service}")
