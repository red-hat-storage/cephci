"""Open port 443 and confirm the RGW HTTPS endpoint accepts connections."""

import shlex
from time import sleep

from utility.log import Log

LOG = Log(__name__)

ATTEMPTS = 24
PAUSE = 10


def rgw_unit(node):
    """Return the systemd unit for the RGW service, if it is installed."""
    out, _ = node.exec_command(
        sudo=True,
        cmd="systemctl list-units --all --no-legend",
        check_ec=False,
    )
    for line in out.splitlines():
        if "rgw.rgw.ssl" not in line:
            continue
        for field in line.split():
            if field.endswith(".service"):
                return field
    return ""


def log_cmd(node, cmd):
    """Run a diagnostic command and keep going if it fails."""
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if out:
        LOG.info(out)
    if err:
        LOG.info(err)


def endpoint_ready(node):
    """Return True when localhost and the node IP both answer on 443."""
    url = (
        "curl -k -sS --connect-timeout 5 -o /dev/null https://127.0.0.1:443"
        f" && curl -k -sS --connect-timeout 5 -o /dev/null https://{node.ip_address}:443"
    )
    _, _, rc, _ = node.exec_command(
        sudo=True,
        cmd="bash -c " + shlex.quote(url),
        check_ec=False,
        verbose=True,
    )
    return rc == 0


def dump_failure(node):
    """Log listeners, the RGW container, and recent journal lines."""
    log_cmd(node, "ss -lntp")
    log_cmd(node, "podman ps -a --filter name=rgw")
    out, _ = node.exec_command(
        sudo=True,
        cmd="podman ps -aq --filter name=rgw",
        check_ec=False,
    )
    cid = out.strip().splitlines()[0] if out.strip() else ""
    if cid:
        log_cmd(node, f"podman logs --tail 150 {shlex.quote(cid)}")
    log_cmd(node, "journalctl --no-pager -n 150 -t ceph-rgw")
    log_cmd(
        node,
        "journalctl --no-pager -n 150 | grep -E 'rgw|ssl_certificate|beast'",
    )


def run(ceph_cluster, **kwargs):
    """Restart RGW and wait until HTTPS port 443 accepts connections."""
    node = ceph_cluster.get_nodes(role="rgw")[0]
    try:
        node.exec_command(sudo=True, cmd="firewall-cmd --add-port=443/tcp --permanent")
        node.exec_command(sudo=True, cmd="firewall-cmd --reload")
        unit = rgw_unit(node)
        LOG.info("rgw unit: %s", unit or "missing")
        if unit:
            quoted = shlex.quote(unit)
            node.exec_command(
                sudo=True, cmd=f"systemctl reset-failed {quoted}", check_ec=False
            )
            node.exec_command(
                sudo=True, cmd=f"systemctl restart {quoted}", check_ec=False
            )
        for _ in range(ATTEMPTS):
            if endpoint_ready(node):
                return 0
            sleep(PAUSE)
        dump_failure(node)
    except Exception as exc:
        LOG.error("Failed to verify the RGW HTTPS endpoint: %s", exc)
        return 1
    LOG.error("RGW did not accept HTTPS connections on port 443")
    return 1
