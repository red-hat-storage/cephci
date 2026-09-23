"""Cephadm host management tests (DMFG).

Maintenance enter/exit verification is overridden here so DMFG-specific
podman checks (e.g. excluding systemd-managed cephadm agent) do not change
the shared ``ceph.ceph_admin.maintenance`` library used by other FGs.
"""

from json import loads
from time import sleep
from types import MethodType

from ceph.ceph_admin.common import fetch_method
from ceph.ceph_admin.helper import get_cluster_state
from ceph.ceph_admin.host import Host
from utility.log import Log

log = Log(__name__)


CLUSTER_STATE = ["ceph orch host ls -f yaml"]


def _podman_container_names(containers):
    """Normalize podman ``Names`` field (list or string) to a flat name list."""
    names = []
    for container in containers:
        value = container.get("Names")
        if isinstance(value, list):
            if value:
                names.append(value[0])
        elif value:
            names.append(value)
    return names


def check_maintenance_status(self, op, node) -> bool:
    """DMFG verification of host maintenance enter/exit via orch + podman.

    Same contract as ``MaintenanceMixin.check_maintenance_status``, but:
    - excludes ``daemon_type == "agent"`` (systemd, not a ceph-* container)
    - uses a fixed retry budget so exit can wait for containers to return
    - normalizes podman Names and casts daemon_id to str
    """
    status = self.get_host_status(node.hostname)
    out, _ = self.shell(args=["ceph", "fsid"])
    fsid = out.strip()
    config = {
        "command": "ps",
        "base_cmd_args": {"format": "json"},
        "args": {"hostname": node.hostname},
    }
    out, _ = self.ps(config)
    daemons = loads(out)
    # cephadm "agent" runs via systemd, not as a ceph-<fsid>-agent-* container.
    daemon_names = [
        f"ceph-{fsid}-{daemon['daemon_type']}-{str(daemon['daemon_id']).replace('.', '-')}"
        for daemon in daemons
        if daemon.get("daemon_type") != "agent"
    ]
    if not daemon_names:
        return op == "exit" and status != "maintenance"

    retry_count = 20
    count = 0

    if op == "enter" and status == "maintenance":
        active_daemon = True
        while count < retry_count:
            sleep(30)
            stdout, _ = node.exec_command(sudo=True, cmd="podman ps --format json")
            container_out = stdout.replace("\n", "")
            containers = loads(container_out) if container_out else list()
            if not containers:
                active_daemon = False
                break
            container_names = _podman_container_names(containers)
            if any(daemon in container_names for daemon in daemon_names):
                count += 1
            else:
                active_daemon = False
                break
        return not bool(active_daemon)

    if op == "exit" and status != "maintenance":
        daemons_active = False
        while count < retry_count:
            sleep(30)
            stdout, _ = node.exec_command(sudo=True, cmd="podman ps --format json")
            container_out = stdout.replace("\n", "")
            containers = loads(container_out) if container_out else list()
            if containers:
                container_names = _podman_container_names(containers)
                if all(daemon in container_names for daemon in daemon_names):
                    daemons_active = True
                    break
            count += 1
        return daemons_active

    return False


def run(ceph_cluster, **kw):
    """
    Cephadm Bootstrap, Managing hosts with options and
    full cluster deployment at single call are supported.

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    - Manage host operations like,
        - Add hosts with/without labels and IP address
        - Add/Remove labels to/from existing node
        - Set Address to node.
        - Remove hosts

        host_ops keys are definition names are defined under
        CephAdmin.HostMixin should be used to call that respective method.

        supported definition names for host_ops are host_add, attach_label,
        remove_label, set_address and host_remove.

        for example.,
        - test:
            name: Add host
            desc: Add new host node with IP address
            module: test_host.py
            config:
                service: host
                command: add | remove | label_add | label_remove | set_address
                base_cmd_args:
                  nodes:
                    - "node3"
                  attach_address: true
                  add_label: false

    """
    log.info("Running Cephadm Host test")
    config = kw.get("config")

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    service = config.pop("service", "")

    log.info("Executing %s %s" % (service, command))

    host = Host(cluster=ceph_cluster, **config)
    if command in ("enter", "exit"):
        host.check_maintenance_status = MethodType(check_maintenance_status, host)

    try:
        method = fetch_method(host, command)
        method(config)
    finally:
        # Get cluster state
        get_cluster_state(host, CLUSTER_STATE)

    return 0
