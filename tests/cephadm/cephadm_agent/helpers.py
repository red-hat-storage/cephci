"""Shared helpers and constants for cephadm agent tests."""

import json
import re
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


# cephadm shell has a known transient failure mode where the mon config file
# disappears mid-flight, e.g.
#
#     Inferring fsid <fsid>
#     Inferring config /var/lib/ceph/<fsid>/mon.<host>/config
#     Error: statfs /var/lib/ceph/<fsid>/mon.<host>/config: no such file or directory
#     (exit 125)
#
# Observed ~28x more often on the 20.2.2-234..254 builds than elsewhere, but it
# also shows up as a one-off on healthy builds. It is not caused by the test, so
# a single transient must not abort an entire suite. Retry those specific
# signatures briefly, then give up and let the caller fail for real.
# Only signatures unique to this transient belong here. Do NOT add generic
# fragments such as "cephadm shell -- " — that is the prefix of every command
# and would retry genuine failures (permission denied, EINVAL, command not
# found) three times with a 20s penalty before failing anyway.
TRANSIENT_SHELL_PATTERNS = (
    "statfs ",
    "no such file or directory",
    "Inferring fsid",
    "Inferring config",
)
TRANSIENT_SHELL_ATTEMPTS = 3
TRANSIENT_SHELL_DELAY = 10


def _is_transient_shell_error(exc):
    """Return True when a cephadm shell failure looks like the known transient.

    Requires the exit-125 marker *and* one of the mon-config signatures, so a
    plain non-zero exit with an unrelated error is never retried.
    """
    msg = str(exc)
    if "code 125" not in msg:
        return False
    return any(pat in msg for pat in TRANSIENT_SHELL_PATTERNS)


def shell(installer, cmd):
    """Run a cephadm shell command and return (stdout, stderr).

    Retries the known cephadm-shell transient (exit 125 / missing mon config)
    a few times before surfacing the original error, so an intermittent
    infrastructure hiccup does not fail the whole test.
    """
    last_exc = None
    for attempt in range(1, TRANSIENT_SHELL_ATTEMPTS + 1):
        try:
            return installer.exec_command(sudo=True, cmd=f"cephadm shell -- {cmd}")
        except Exception as exc:  # noqa: BLE001 - classify by message below
            if not _is_transient_shell_error(exc):
                raise
            last_exc = exc
            if attempt == TRANSIENT_SHELL_ATTEMPTS:
                break
            log.warning(
                "cephadm shell transient failure (attempt %d/%d), retrying in %ds: %s",
                attempt,
                TRANSIENT_SHELL_ATTEMPTS,
                TRANSIENT_SHELL_DELAY,
                str(exc).splitlines()[-1][:200] if str(exc) else exc,
            )
            time.sleep(TRANSIENT_SHELL_DELAY)
    raise last_exc


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

    A raising poll (e.g. transient cephadm shell failure) is logged and treated
    as "not yet", so the loop keeps trying until the timeout instead of aborting
    the whole test on the first hiccup.
    """
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        try:
            out, _ = shell(installer, "ceph health detail -f json")
            health = json.loads(out)
        except Exception as exc:  # noqa: BLE001 - keep polling until timeout
            last_err = exc
            log.warning("health poll failed, retrying: %s", str(exc)[:200])
            time.sleep(interval)
            continue
        checks = health.get("checks", {})
        present = warning_code in checks
        if present == expect_present:
            return True
        time.sleep(interval)
    if last_err:
        log.warning(
            "wait_for_health_warning timed out; last error: %s", str(last_err)[:200]
        )
    return False


def wait_for_agent_running(installer, hostname, timeout=120, interval=10):
    """Wait until agent daemon on a specific host shows status 'running'."""
    end = time.time() + timeout
    last_err = None
    while time.time() < end:
        try:
            agents = get_agent_daemons(installer)
        except Exception as exc:  # noqa: BLE001 - keep polling until timeout
            last_err = exc
            log.warning("agent poll failed, retrying: %s", str(exc)[:200])
            time.sleep(interval)
            continue
        for a in agents:
            if a.get("hostname") == hostname and a.get("status_desc") == "running":
                return True
        time.sleep(interval)
    if last_err:
        log.warning(
            "wait_for_agent_running(%s) timed out; last error: %s",
            hostname,
            str(last_err)[:200],
        )
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
    """Wait until agent daemons are running on every cluster host.

    The whole poll body is retried until the timeout so a single transient
    cephadm shell failure does not abort a multi-minute wait.
    """
    last_err = None
    end = time.time() + timeout
    expected = None
    while time.time() < end:
        try:
            if expected is None:
                hosts_out, _ = shell(installer, "ceph orch host ls -f json")
                expected = {h["hostname"] for h in json.loads(hosts_out)}
            agents = get_agent_daemons(installer)
        except Exception as exc:  # noqa: BLE001 - keep polling until timeout
            last_err = exc
            log.warning("agent poll failed, retrying: %s", str(exc)[:200])
            time.sleep(15)
            continue
        running = {a["hostname"] for a in agents if a.get("status_desc") == "running"}
        if running >= expected:
            return True
        time.sleep(15)
    if last_err:
        log.warning(
            "ensure_agent_running_on_all timed out; last error: %s", str(last_err)[:200]
        )
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


def ensure_packet_filter_cli(node):
    """
    Ensure a usable iptables CLI exists (RHEL 10 often has nft only).
    Prefers iptables-nft; installs package if missing.
    """
    check, _ = node.exec_command(
        sudo=True,
        cmd="command -v iptables || command -v iptables-nft || true",
        check_ec=False,
    )
    if check.strip():
        return check.strip().splitlines()[0]

    log.info("iptables not found — installing iptables-nft for agent network tests")
    node.exec_command(
        sudo=True,
        cmd="dnf install -y iptables-nft iptables >/dev/null 2>&1 || "
        "yum install -y iptables-nft iptables >/dev/null 2>&1 || true",
        check_ec=False,
    )
    check2, _ = node.exec_command(
        sudo=True,
        cmd="command -v iptables || command -v iptables-nft || true",
        check_ec=False,
    )
    path = check2.strip().splitlines()[0] if check2.strip() else ""
    assert path, (
        "Neither iptables nor iptables-nft is available after install attempt. "
        "RHEL 10 images need iptables-nft for agent network-partition tests."
    )
    return path


def iptables_cmd(node, args):
    """Run iptables with auto-detected binary (iptables or iptables-nft)."""
    binary = ensure_packet_filter_cli(node)
    return node.exec_command(sudo=True, cmd=f"{binary} {args}")


def get_active_mgr_ip(installer):
    """Return active mgr IP from `ceph mgr dump`."""
    out, _ = shell(installer, "ceph mgr dump -f json")
    dump = json.loads(out)
    addr = dump.get("active_addr", "")
    # formats like 10.0.0.1:6800/123 or [v2:10.0.0.1:6800/123]
    m = re.search(r"(\d+\.\d+\.\d+\.\d+)", addr)
    return m.group(1) if m else addr.split(":")[0].strip("/").strip()


def pick_non_active_mgr_agent_host(installer, ceph_cluster):
    """
    Prefer an agent host that is not the current active mgr host and not the
    installer when possible — avoids failover landing on the stopped-agent host.
    """
    agents = get_agent_daemons(installer)
    assert agents, "No agent daemons found"

    mgr_out, _ = shell(installer, "ceph mgr stat -f json")
    active_name = json.loads(mgr_out).get("active_name", "")
    installer_host = get_installer(ceph_cluster).hostname

    def score(hostname):
        s = 0
        if hostname and hostname in active_name:
            s -= 100
        if hostname == installer_host:
            s -= 50
        return s

    ranked = sorted(agents, key=lambda a: score(a.get("hostname", "")), reverse=True)
    chosen = ranked[0]["hostname"]
    log.info(
        f"Selected agent host {chosen} "
        f"(active_mgr={active_name}, installer={installer_host})"
    )
    return chosen


def wait_for_agent_json_target_ip(
    node, agent_dir, expected_ip, timeout=300, interval=10
):
    """Poll agent.json until target_ip matches expected_ip (or timeout)."""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        out, _ = node.exec_command(sudo=True, cmd=f"cat {agent_dir}/agent.json")
        cfg = json.loads(out)
        last = cfg.get("target_ip")
        if last == expected_ip:
            return True, last
        time.sleep(interval)
    return False, last
