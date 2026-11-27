"""Write dashboard_endpoint.json after a cephadm deploy.

The file is the handoff for ceph_dashboard_api_automation. It holds the live
dashboard host and port plus the user and password this suite configured.
Pytest reads it later; this module does not start pytest.

When the suite omits the password, the password is taken from the bootstrap
log (the value cephadm actually set), not from a built-in default.
"""

import json
import os
import re
import time

from utility.log import Log

LOG = Log(__name__)

ENDPOINT_NAME = "dashboard_endpoint.json"
ACCEPTABLE_HEALTH = ("HEALTH_OK", "HEALTH_WARN")
_URL_RE = re.compile(r"^https://([^:/]+):(\d+)")
_BANNER = "Ceph Dashboard is now available at:"


def parse_dashboard_url(url):
    """Return (host, port) from a ``https://<host>:<port>`` dashboard URL.

    Raises:
        ValueError: the URL is not ``https://<host>:<port>``.
    """
    match = _URL_RE.match((url or "").strip())
    if not match:
        raise ValueError("Cannot parse dashboard URL")
    return match.group(1), int(match.group(2))


def _as_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def load_json_blob(text):
    """Parse the first JSON object in a command transcript."""
    raw = _as_text(text)
    start = raw.find("{")
    if start < 0:
        raise ValueError("Command output has no JSON object")
    return json.loads(raw[start:])


def parse_bootstrap_banner(text):
    """Return url, username, and password from a cephadm bootstrap transcript.

    Returns None when the dashboard banner is absent or incomplete.
    """
    _prefix, _sep, part = (text or "").partition(_BANNER)
    if not part:
        return None

    def field(label):
        found = re.search(r"%s:\s*(.+)" % label, part)
        if not found:
            return None
        return found.group(1).strip()

    url = field("URL")
    username = field("User")
    password = field("Password")
    if not (url and username and password):
        return None
    return {"url": url, "username": username, "password": password}


def find_bootstrap_credentials(log_dir):
    """Read dashboard creds from top-level cephci logs, not from subdirectories."""
    if not log_dir or not os.path.isdir(log_dir):
        return None
    for name in sorted(os.listdir(log_dir)):
        path = os.path.join(log_dir, name)
        if not name.endswith(".log") or not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as handle:
            parsed = parse_bootstrap_banner(handle.read())
        if parsed:
            return parsed
    return None


def host_for_client(host, nodes):
    """Prefer a node's IP when the dashboard URL uses that node's hostname."""
    for node in nodes or []:
        names = [
            getattr(node, "hostname", None),
            getattr(node, "shortname", None),
            getattr(node, "vmname", None),
        ]
        names = [name for name in names if name]
        if host in names or any(host and host in name for name in names):
            ip_address = getattr(node, "ip_address", None)
            if ip_address:
                return ip_address
    return host


def dashboard_url_from_services(services):
    """Return the dashboard URL from ``ceph mgr services`` JSON."""
    if not isinstance(services, dict):
        raise ValueError("mgr services output is not an object")
    url = services.get("dashboard")
    if not url:
        raise ValueError("mgr services has no dashboard URL")
    return url


def wait_until_usable(
    exec_cmd, base_cmd, timeout, interval=15, sleep=time.sleep, now=time.time
):
    """Block until cluster health is HEALTH_OK or HEALTH_WARN.

    HEALTH_ERR while PGs peer should not fail the dashboard suite immediately.
    A cluster that stays in HEALTH_ERR raises RuntimeError.
    """
    timeout = int(timeout)
    if timeout <= 0:
        return None
    deadline = now() + timeout
    last_status = None
    while True:
        payload = load_json_blob(exec_cmd(f"{base_cmd} ceph health -f json"))
        last_status = payload.get("status")
        if last_status in ACCEPTABLE_HEALTH:
            return last_status
        if now() >= deadline:
            raise RuntimeError(
                "Cluster health stayed %s for %ss" % (last_status, timeout)
            )
        sleep(interval)


def write_endpoint(log_dir, endpoint):
    """Write the endpoint file for the pytest subprocess. Mode 0600."""
    path = os.path.join(log_dir, ENDPOINT_NAME)
    blob = json.dumps(endpoint, indent=2) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(descriptor, blob.encode("utf-8"))
    finally:
        os.close(descriptor)
    return path


def _shell_prefix(rhbuild):
    if str(rhbuild or "").startswith("4"):
        return "sudo"
    return "sudo cephadm shell --"


def _run_shell(installer, command):
    out, _err = installer.exec_command(cmd=command)
    return _as_text(out)


def run(ceph_cluster, config, **kwargs):
    """Collect the dashboard endpoint and write it into the cephci log directory.

    Returns:
        0 on success, 1 on failure.
    """
    config = config or {}
    run_config = kwargs.get("run_config") or {}
    log_dir = run_config.get("log_dir")
    if not log_dir:
        LOG.error("run_config.log_dir is not set; cannot write %s", ENDPOINT_NAME)
        return 1

    try:
        installer = ceph_cluster.get_nodes(role="installer")[0]
        base_cmd = _shell_prefix(config.get("rhbuild"))

        def exec_cmd(command):
            return _run_shell(installer, command)

        status = wait_until_usable(
            exec_cmd, base_cmd, config.get("health-wait-seconds", 600)
        )
        if status:
            LOG.info("Cluster health is %s", status)

        banner = None
        try:
            services = load_json_blob(exec_cmd(f"{base_cmd} ceph mgr services -f json"))
            host, port = parse_dashboard_url(dashboard_url_from_services(services))
        except Exception:
            banner = find_bootstrap_credentials(log_dir)
            if not banner:
                raise
            LOG.info("Using the dashboard URL from the bootstrap log")
            host, port = parse_dashboard_url(banner["url"])
        host = host_for_client(host, ceph_cluster.get_nodes())

        username = config.get("username") or config.get("initial-dashboard-user")
        password = config.get("password") or config.get("initial-dashboard-password")
        if not username or not password:
            banner = banner or find_bootstrap_credentials(log_dir)
            if banner:
                username = username or banner["username"]
                password = password or banner["password"]
        if not username:
            username = "admin"
        if not password:
            LOG.error(
                "Dashboard password was not in the suite config or the bootstrap log"
            )
            return 1

        path = write_endpoint(
            log_dir,
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
            },
        )
        LOG.info(
            "Wrote dashboard endpoint host=%s port=%s user=%s",
            host,
            port,
            username,
        )
        LOG.info("Dashboard endpoint file: %s", path)
        return 0
    except Exception as exc:  # noqa: BLE001 - suite step must return 1, not raise
        LOG.error("Failed to export dashboard endpoint: %s", exc)
        return 1
