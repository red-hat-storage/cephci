"""Shared helpers and constants for cephadm agent tests."""

import json
import time

from utility.log import Log

log = Log(__name__)

AGENT_SERVICE = "agent"
AGENT_HEALTH_WARNING = "CEPHADM_AGENT_DOWN"
DEFAULT_AGENT_REFRESH = 20
DEFAULT_AGENT_DOWN_TIMEOUT = DEFAULT_AGENT_REFRESH * 3
KNOWN_IGNORABLE_WARNINGS = {
    "CEPHADM_APPLY_SPEC_FAIL",
    "TOO_FEW_OSDS",
    "CEPHADM_REFRESH_FAILED",
}


def shell(installer, cmd):
    """Run a cephadm shell command and return (stdout, stderr)."""
    return installer.exec_command(sudo=True, cmd=f"cephadm shell -- {cmd}")


def get_agent_daemons(installer):
    """Return parsed list of agent daemons from `ceph orch ps`."""
    out, _ = shell(installer, "ceph orch ps --daemon-type agent -f json")
    return json.loads(out)


def wait_for_health_warning(
    installer, warning_code, timeout=120, interval=10, expect_present=True
):
    """
    Poll `ceph health detail` until the given warning code appears (or disappears).
    Returns True if condition met within timeout, False otherwise.
    """
    end = time.time() + timeout
    while time.time() < end:
        out, _ = shell(installer, "ceph health detail -f json")
        health = json.loads(out)
        checks = health.get("checks", {})
        present = warning_code in checks
        if present == expect_present:
            return True
        time.sleep(interval)
    return False


def wait_for_agent_running(installer, hostname, timeout=120, interval=10):
    """Wait until agent daemon on a specific host shows status 'running'."""
    end = time.time() + timeout
    while time.time() < end:
        agents = get_agent_daemons(installer)
        for a in agents:
            if a.get("hostname") == hostname and a.get("status_desc") == "running":
                return True
        time.sleep(interval)
    return False


def get_cluster_health_status(installer):
    """Return the overall cluster health status string."""
    out, _ = shell(installer, "ceph health -f json")
    return json.loads(out).get("status", "HEALTH_ERR")


def get_fsid(installer):
    """Return the cluster fsid, stripped."""
    fsid, _ = shell(installer, "ceph fsid")
    return fsid.strip()


def get_node_for_host(ceph_cluster, hostname):
    """Find and return the CephNode matching hostname."""
    for node in ceph_cluster.get_nodes():
        if node.hostname == hostname:
            return node
    return None


def agent_service_name(fsid, hostname):
    """Return the systemd service name for the agent on a host."""
    return f"ceph-{fsid}@agent.{hostname}"


def ensure_agent_running_on_all(installer, ceph_cluster, timeout=180):
    """Wait until agent daemons are running on every cluster host."""
    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    expected = {h["hostname"] for h in json.loads(hosts_out)}
    end = time.time() + timeout
    while time.time() < end:
        agents = get_agent_daemons(installer)
        running = {a["hostname"] for a in agents if a.get("status_desc") == "running"}
        if running >= expected:
            return True
        time.sleep(15)
    return False


def get_host_facts(installer, hostname):
    """Retrieve cached host facts for a hostname."""
    out, _ = shell(installer, f"ceph orch host facts {hostname} -f json")
    return json.loads(out)


def get_orch_ps(installer, daemon_type=None, hostname=None, refresh=False):
    """Get daemon list via `ceph orch ps`, optionally filtered and refreshed."""
    cmd = "ceph orch ps"
    if daemon_type:
        cmd += f" --daemon-type {daemon_type}"
    if hostname:
        cmd += f" --hostname {hostname}"
    if refresh:
        cmd += " --refresh"
    cmd += " -f json"
    out, _ = shell(installer, cmd)
    return json.loads(out)


def get_installer(ceph_cluster):
    """Get the installer node from the cluster."""
    installers = ceph_cluster.get_nodes(role="installer")
    if not installers:
        installers = ceph_cluster.get_nodes(role="_admin")
    return installers[0]


def setup_run(ceph_cluster, kw):
    """Common run() setup: get config, installer, enable agent."""
    config = kw.get("config", {})
    build = config.get("build", config.get("rhbuild"))
    if build:
        ceph_cluster.rhcs_version = build
    installer = get_installer(ceph_cluster)
    shell(installer, "ceph config set mgr mgr/cephadm/use_agent true")
    time.sleep(10)
    return config, installer
