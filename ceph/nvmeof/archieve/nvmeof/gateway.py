"""Ceph-NVMeoF gateway module.

- Configure spdk and start spdk.
- Configure nvme-of targets using control.cli.
"""

import re

from ceph.ceph import CephNode
from ceph.ceph_admin.common import config_dict_to_string
from cli.utilities.packages import Package
from utility.log import Log

LOG = Log(__name__)

REPO_PATH = "/tmp/ceph-nvmeof"
CEPH_NVMEOF_REPO = "https://github.com/ceph/ceph-nvmeof.git"
CTRL_DAEMON = "python3 -m control"
CTRL_DAEMON_CLI = f"{CTRL_DAEMON}.cli"


def fetch_spdk_pid(node: CephNode):
    """Fetch SPDK Process id.

    Args:
        node: Gateway Node
    """
    out, _ = node.exec_command(cmd=f"pgrep -f '{CTRL_DAEMON}'", check_ec=False)
    LOG.debug(out)
    return out.strip() if out else False


def kill_spdk_process(node: CephNode):
    """Kill SPDK daemon.

    Args:
        node: Gateway node
    """
    pid = fetch_spdk_pid(node)
    if pid:
        node.exec_command(cmd=f"kill -9 {pid}", check_ec=False, sudo=True)


def fetch_gateway_log(node: CephNode):
    """fetch GW server logs.

    Args:
        node: Gateway node
    """
    out, _ = node.exec_command(
        sudo=True,
        cmd=f"cd {REPO_PATH}; cat output.log",
        check_ec=False,
    )
    LOG.debug(out)


def configure_spdk(node: CephNode, rbd_pool):
    """SPDK installation on Gateway node.

    - Supports only RHEL-9.

    Args:
        node: Ceph Node object.
        rbd_pool: RBD Pool
    """
    pkg_mgr = Package(nodes=[node])
    pkg_mgr.install("git")

    # Clone the ceph-nvmeof repository
    SHA_ID = "70f4728e916346cd59dd335897e53f9a618b98de"
    node.exec_command(
        sudo=True,
        cmd=f"rm -rf {REPO_PATH};"
        f"git clone --recursive {CEPH_NVMEOF_REPO} {REPO_PATH};"
        f"cd {REPO_PATH}; git checkout {SHA_ID}",
    )
    out, _ = node.exec_command(cmd=f"cat {REPO_PATH}/ceph-nvmeof.conf")

    def update_cfg(cfg, key, value):
        return re.sub(f"{key} = (.*)", f"{key} = {value}", cfg)

    conf = update_cfg(out, "spdk_path", REPO_PATH)
    conf = update_cfg(conf, "addr", node.ip_address)
    conf = update_cfg(conf, "pool", rbd_pool)

    _file = node.remote_file(
        sudo=True, file_name=f"{REPO_PATH}/ceph-nvmeof.conf", file_mode="w"
    )
    _file.write(conf)
    _file.flush()

    # Install pre-requisites
    pkg_mgr.install(
        "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm"
    )
    node.exec_command(
        sudo=True,
        cmd="subscription-manager repos --enable=codeready-builder-for-rhel-9-x86_64-rpms",
    )
    pkg_mgr.install(
        "CUnit-devel libiscsi-devel json-c-devel libcmocka-devel python-rados librbd-devel"
    )
    node.exec_command(
        sudo=True,
        cmd="sh -c 'echo 4096 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages'",
    )
    if node.exec_command(
        sudo=True,
        cmd=f"cd {REPO_PATH}/spdk; ./scripts/pkgdep.sh; ./configure --with-rbd; make",
        long_running=True,
        check_ec=False,
    ):
        raise Exception("SPDK pre-requisites installation failed...")
    node.exec_command(
        sudo=True,
        cmd=f"cd {REPO_PATH}; make grpc; nohup {CTRL_DAEMON} > output.log 2>&1 & sleep 20",
    )

    if not fetch_spdk_pid(node):
        raise Exception("Control and SPDK process is not running.")

    node.exec_command(sudo=True, cmd="systemctl stop firewalld", check_ec=False)


def delete_gateway(node: CephNode):
    """Cleanup the gateway.

    - Kill Gateway process
    - remove the repository

    Args:
        node: CephNode (gateway node)
    """
    kill_spdk_process(node)
    node.exec_command(sudo=True, cmd=f"rm -rf {REPO_PATH}", check_ec=False)


def run_control_cli(node: CephNode, action, **cmd_args):
    """Run CLI via control daemon."""
    out = node.exec_command(
        sudo=True,
        cmd=f"cd {REPO_PATH}; {CTRL_DAEMON_CLI} {action} {config_dict_to_string(cmd_args)}",
    )
    LOG.debug(out)
    return out


class Gateway:
    def __init__(self, node):
        self.node = node

    def get_subsystems(self):
        """Get all subsystems."""
        return run_control_cli(self.node, "get_subsystems")

    def create_block_device(self, name, image, pool, block_size=None):
        """Create block device using rbd image."""
        args = {"image": image, "pool": pool, "bdev": name}
        if block_size:
            args["block-size"] = block_size
        return run_control_cli(self.node, "create_bdev", **args)

    def delete_block_device(self, name):
        """Delete block device."""
        args = {"bdev": name}
        return run_control_cli(self.node, "delete_bdev", **args)

    def create_subsystem(self, subnqn, serial_num, max_ns):
        """Create subsystem."""
        args = {"subnqn": subnqn, "serial": serial_num, "max-namespaces": max_ns}
        return run_control_cli(self.node, "create_subsystem", **args)

    def delete_subsystem(self, subnqn):
        """Delete subsystem."""
        return run_control_cli(self.node, "delete_subsystem", **{"subnqn": subnqn})

    def add_namespace(self, subnqn, bdev, nsid=None):
        """Add namespace under subsystem."""
        args = {"subnqn": subnqn, "bdev": bdev}
        if nsid:
            args["nsid"] = nsid
        return run_control_cli(self.node, "add_namespace", **args)

    def remove_namespace(self, subnqn, bdev):
        """Remove namespace under subsystem."""
        args = {"subnqn": subnqn, "nsid": bdev}
        return run_control_cli(self.node, "remove_namespace", **args)

    def add_host(self, subnqn, hostnqn):
        """Add host to subsystem."""
        args = {"subnqn": subnqn, "host": repr(hostnqn)}
        return run_control_cli(self.node, "add_host", **args)

    def remove_host(self, subnqn, hostnqn):
        """Remove host from subsystem."""
        args = {"subnqn": subnqn, "hostnqn": repr(hostnqn)}
        return run_control_cli(self.node, "remove_host", **args)

    def create_listener(self, subnqn, port, **kwargs):
        """Create listener under subsystem.

        Args:
            subnqn: subsystem nqn
            port: transport channel port
            kwargs: other attributes
        """
        args = {
            "subnqn": subnqn,
            "trsvcid": port,
            "gateway-name": kwargs.get("gateway-name", False),
            "trtype": kwargs.get("trtype", False),
            "adrfam": kwargs.get("adrfam", False),
            "traddr": kwargs.get("traddr", False),
        }
        return run_control_cli(self.node, "create_listener", **args)

    def delete_listener(self, subnqn, port, **kwargs):
        """Delete listener under subsystem.

        Args:
            subnqn: subsystem nqn
            port: transport channel port
            kwargs: other attributes
        """
        args = {
            "subnqn": subnqn,
            "trsvcid": port,
            "gateway-name": kwargs.get("gateway-name", False),
            "trtype": kwargs.get("trtype", False),
            "adrfam": kwargs.get("adrfam", False),
            "traddr": kwargs.get("traddr", False),
        }

        return run_control_cli(self.node, "delete_listener", **args)
