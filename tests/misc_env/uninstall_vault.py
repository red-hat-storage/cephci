"""
Cleanup module for containerized Vault infrastructure.

This module removes all vault components deployed by install_vault.py:
- Stops and removes vault-agent services on RGW nodes
- Removes vault-agent configuration files
- Stops and removes vault-server container
- Removes vault data and configuration
- Closes firewall ports

Usage in test suite:
    - test:
        name: cleanup vault infrastructure
        module: uninstall_vault.py
        config:
          cleanup:
            - server   # Remove vault server container
            - agent    # Remove vault agents
"""

from typing import Dict

from ceph.ceph import Ceph
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster: Ceph, config: Dict, **kwargs) -> int:
    """
    Remove containerized Vault infrastructure.

    Args:
        ceph_cluster: The cluster participating in the test
        config: Configuration passed to the test
        kwargs: Additional configurations

    Returns:
        0 on Success, 1 on Failure
    """
    try:
        cleanup_components = config.get("cleanup", ["agent", "server"])

        LOG.info("=" * 70)
        LOG.info("VAULT INFRASTRUCTURE CLEANUP")
        LOG.info("=" * 70)

        # Remove vault agents first (if requested)
        if "agent" in cleanup_components:
            LOG.info("\nRemoving Vault agents from RGW nodes...")
            _remove_vault_agents(ceph_cluster)
            LOG.info("✓ Vault agents removed")

        # Remove vault server container (if requested)
        if "server" in cleanup_components:
            LOG.info("\nRemoving Vault server container...")
            _remove_vault_server(ceph_cluster)
            LOG.info("✓ Vault server removed")

        LOG.info("\n" + "=" * 70)
        LOG.info("✓ VAULT CLEANUP COMPLETE")
        LOG.info("=" * 70)

        return 0

    except Exception as e:
        LOG.error(f"Vault cleanup failed: {e}")
        import traceback

        LOG.error(traceback.format_exc())
        return 1


def _remove_vault_agents(cluster: Ceph) -> None:
    """Remove vault-agent from all RGW nodes."""
    rgw_nodes = cluster.get_nodes(role="rgw")

    if not rgw_nodes:
        LOG.warning("No RGW nodes found")
        return

    LOG.info(f"Cleaning up {len(rgw_nodes)} RGW nodes")

    for node in rgw_nodes:
        LOG.info(f"  Cleaning {node.shortname}...")

        # Stop and disable vault-agent service
        commands = [
            "systemctl stop vault-agent",
            "systemctl disable vault-agent",
        ]
        for cmd in commands:
            node.exec_command(sudo=True, cmd=cmd, check_ec=False)

        # Remove systemd service file
        node.exec_command(
            sudo=True,
            cmd="rm -f /etc/systemd/system/vault-agent.service",
            check_ec=False,
        )

        # Remove vault configuration directory
        node.exec_command(
            sudo=True,
            cmd="rm -rf /usr/local/etc/vault",
            check_ec=False,
        )

        # Reload systemd
        node.exec_command(
            sudo=True,
            cmd="systemctl daemon-reload",
            check_ec=False,
        )

        # Optionally remove vault binary (commented out - may be needed for other purposes)
        # node.exec_command(
        #     sudo=True,
        #     cmd="yum remove -y vault",
        #     check_ec=False,
        # )

        LOG.info(f"  ✓ {node.shortname} cleaned")


def _remove_vault_server(cluster: Ceph) -> None:
    """Remove vault server container from client node."""
    client_nodes = cluster.get_nodes(role="client")

    if not client_nodes:
        LOG.warning("No client node found")
        return

    node = client_nodes[0]
    LOG.info(f"Cleaning vault server on {node.shortname}")

    # Stop and disable vault-server systemd service
    node.exec_command(
        sudo=True,
        cmd="systemctl stop vault-server",
        check_ec=False,
    )
    node.exec_command(
        sudo=True,
        cmd="systemctl disable vault-server",
        check_ec=False,
    )

    # Remove systemd service file
    node.exec_command(
        sudo=True,
        cmd="rm -f /etc/systemd/system/vault-server.service",
        check_ec=False,
    )

    # Stop and remove vault-server container
    node.exec_command(
        sudo=True,
        cmd="podman stop vault-server",
        check_ec=False,
    )
    node.exec_command(
        sudo=True,
        cmd="podman rm vault-server",
        check_ec=False,
    )

    # Remove vault image (optional - commented out to save time on re-runs)
    # node.exec_command(
    #     sudo=True,
    #     cmd="podman rmi docker.io/hashicorp/vault:latest",
    #     check_ec=False,
    # )

    # Remove vault data and config directories
    node.exec_command(
        sudo=True,
        cmd="rm -rf /vault",
        check_ec=False,
    )

    # Close firewall port 8200
    node.exec_command(
        sudo=True,
        cmd="firewall-cmd --remove-port=8200/tcp --permanent",
        check_ec=False,
    )
    node.exec_command(
        sudo=True,
        cmd="firewall-cmd --reload",
        check_ec=False,
    )

    # Reload systemd
    node.exec_command(
        sudo=True,
        cmd="systemctl daemon-reload",
        check_ec=False,
    )

    LOG.info(f"  ✓ {node.shortname} cleaned")
