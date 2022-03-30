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
        config          Configuration passed to the test
        kwargs          Additional configurations passed to the test.

    Returns:
        0 on Success else 1

    Raises:
        CommandFailure

    Example:

        - test:
            abort-on-fail: false
            config:
              install:
                - agent
            desc: Install and configure vault agent
            module: install_vault.py
            name: install vault agent
    """
    if "agent" in config["install"]:
        vault_cfg = get_cephci_config().get("vault")
        _install_agent(ceph_cluster, vault_cfg)

        client = ceph_cluster.get_nodes(role="client")[0]
        _configure_rgw_daemons(client, vault_cfg)

    return 0


# Private methods


def _write_remote_file(node: CephNode, file_name: str, content: str) -> None:
    """
    Copies the provide content to the specified file on the given node.

    Args:
        node        The target system
        file_name   The name of the remote file to which the content needs to be written
        content     The content of the file to be written

    Returns:
          None

    Raises:
          CommandFailed
    """
    LOG.debug(f"Writing to remote file {file_name}")
    file_handle = node.remote_file(sudo=True, file_mode="w", file_name=file_name)
    file_handle.write(data=content)
    file_handle.flush()
    file_handle.close()


def _install_agent(cluster: Ceph, config: Dict) -> None:
    """
    Installs and configures the vault-agent on all RGW nodes

    Args:
        cluster     Ceph cluster participating in the test
        config      key/value pairs useful for customization

    Returns:
        None

    Raises:
        CommandFailed
    """
    rgw_nodes = cluster.get_nodes(role="rgw")
    for node in rgw_nodes:
        LOG.debug(f"Vault install and configuration on {node.shortname}")
        _install_vault_packages(node)
        _create_agent_config(node, config)
        _create_agent_systemd(node)


def _install_vault_packages(node: CephNode) -> None:
    """
    Installs the required packages for vault

    Args:
        node    The system on which the package needs to be installed

    Returns:
        None

    Raises:
        CommandFailed
    """
    vault_repo = "https://rpm.releases.hashicorp.com/RHEL/hashicorp.repo"
    commands = [f"yum-config-manager --add-repo {vault_repo}", "yum install -y vault"]
    for command in commands:
        node.exec_command(sudo=True, cmd=command, check_ec=False)


def _create_agent_config(node: CephNode, config: Dict) -> None:
    """
    Writes the required configuration file to the provided node.

    The following files are created .app-role-id, .app-secret-id and agent.hcl

    Args:
        node    The system on which files have to be copied
        config  Dictionary holding the tokens

    Returns:
        None

    Raises:
        CommandFailed
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
    # hcl file
    agent_conf = {"url": config["url"], "auth": config["agent"]["auth"]}
    tmpl = Template(AGENT_HCL)
    data = tmpl.render(data=agent_conf)
    _write_remote_file(
        node=node,
        file_name="/usr/local/etc/vault/agent.hcl",
        content=data,
    )


def _create_agent_systemd(node: CephNode) -> None:
    """
    Configures and runs the vault-agent as a system daemon.

    This method creates two files i.e. a launcher file and a system service unit. It
    also enables the service to start.

    Args:
        node    The node for which the vault agent needs to be set.

    Returns:
        None

    Raises:
        CommandFailed
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
    Updates the RGW daemons with the provided configuration.

    Args:
         node       Server that has privilege to perform ceph config set commands.
         config     Key/value pairs to be used for configuration
    Returns:
        None
    Raises:
        CommandFailed
    """
    out, err = node.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type rgw --format json"
    )
    rgw_daemons = [f"client.rgw.{x['daemon_id']}" for x in loads(out)]

    out, err = node.exec_command(
        sudo=True, cmd="ceph orch ls --service_type rgw --format json"
    )
    rgw_services = [x["service_name"] for x in loads(out)]

    configs = [
        ("rgw_crypt_s3_kms_backend", "vault"),
        ("rgw_crypt_vault_secret_engine", config["agent"]["engine"]),
        ("rgw_crypt_vault_auth", config["agent"]["auth"]),
    ]

    if config["agent"]["auth"] == "token":
        configs += [
            ("rgw_crypt_vault_token_file", config["agent"]["token_file"]),
            ("rgw_crypt_vault_addr", config["url"]),
        ]
    else:
        configs += [
            ("rgw_crypt_vault_prefix", config["agent"]["prefix"]),
            ("rgw_crypt_vault_addr", "http://127.0.0.1:8100"),
        ]

    for daemon in rgw_daemons:
        for key, value in configs:
            node.exec_command(sudo=True, cmd=f"ceph config set {daemon} {key} {value}")

    for service in rgw_services:
        node.exec_command(sudo=True, cmd=f"ceph orch restart {service}")
