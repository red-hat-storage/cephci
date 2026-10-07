"""RGW reference-certificate HTTPS helpers for the EC pool suite.

The suite calls this module three times:

    action: reload   rewrite the Beast certificate after apply
    action: verify   open port 443 and confirm the endpoint answers
    action: trust    install the certificate on the client and curl without -k
"""

import json
import re
import shlex
from time import sleep

from copy_cephadm_root_ca_cert import install_cephadm_root_ca_cert
from utility.log import Log

LOG = Log(__name__)

SERVICE = "rgw.rgw.ssl"
CONFIG_KEY = "rgw/cert/rgw.rgw.ssl"
PEM_HOST = "/tmp/rgw_combined.pem"
PEM_CONTAINER = "/etc/ceph/rgw_combined.pem"
FRONTEND = f"beast ssl_port=443 ssl_certificate=config://{CONFIG_KEY}"
REFERENCE_CERT_FILE = "rgw-reference.crt"
TRUST_ROLES = ("client",)
PORT = 443
VERIFY_ATTEMPTS = 24
VERIFY_PAUSE = 10


def ceph(node, *args, mount_pem=False):
    """Run a ceph command inside cephadm shell on the installer."""
    mount = ""
    if mount_pem:
        mount = f"-v {PEM_HOST}:{PEM_CONTAINER} "
    quoted = " ".join(shlex.quote(arg) for arg in args)
    inner = f"cephadm shell {mount}-- ceph {quoted}"
    out, _ = node.exec_command(cmd="bash -c " + shlex.quote(inner), sudo=True)
    return out


def ceph_json(node, *args):
    """Return JSON from a ceph command, ignoring any leading log text."""
    out = ceph(node, *args)
    start = next((i for i, char in enumerate(out) if char in "[{"), None)
    if start is None:
        raise ValueError("ceph command did not return JSON")
    data, _ = json.JSONDecoder().raw_decode(out[start:])
    return data


def reload_certificate(cluster):
    """Publish the reference PEM and point every RGW frontend at it."""
    installer = cluster.get_nodes(role="installer")[0]
    keys = {CONFIG_KEY}
    sections = {"client.rgw"}
    dump = ceph_json(installer, "config", "dump", "--format", "json")
    for item in dump:
        if item.get("name") != "rgw_frontends":
            continue
        LOG.info("FRONT %s %s", item.get("section"), item.get("value"))
        if item.get("section"):
            sections.add(item["section"])
        keys.update(re.findall(r"config://(\S+)", item.get("value") or ""))
    daemons = ceph_json(
        installer, "orch", "ps", "--service_name", SERVICE, "--format", "json"
    )
    LOG.info("PS %s", json.dumps(daemons))
    if isinstance(daemons, dict):
        daemons = daemons.get("daemons", [])
    for daemon in daemons:
        name = daemon.get("daemon_name")
        if not name:
            continue
        sections.add("client." + name)
        keys.add("rgw/cert/" + name)
    for key in sorted(keys):
        LOG.info("SET %s", key)
        ceph(
            installer,
            "config-key",
            "set",
            key,
            "-i",
            PEM_CONTAINER,
            mount_pem=True,
        )
    for section in sorted(sections):
        LOG.info("CONFIG %s", section)
        ceph(installer, "config", "set", section, "rgw_frontends", FRONTEND)
    installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph orch restart {SERVICE}",
        check_ec=False,
    )


def rgw_unit(node):
    """Return the systemd unit for the RGW service, if it is installed."""
    out, _ = node.exec_command(
        sudo=True,
        cmd="systemctl list-units --all --no-legend",
        check_ec=False,
    )
    for line in out.splitlines():
        if SERVICE not in line:
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
        f" && curl -k -sS --connect-timeout 5 -o /dev/null https://{node.ip_address}:{PORT}"
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
    log_cmd(node, "journalctl --no-pager -n 150 | grep -E 'rgw|ssl_certificate|beast'")


def verify_endpoint(cluster):
    """Restart RGW and wait until HTTPS port 443 accepts connections."""
    node = cluster.get_nodes(role="rgw")[0]
    node.exec_command(sudo=True, cmd=f"firewall-cmd --add-port={PORT}/tcp --permanent")
    node.exec_command(sudo=True, cmd="firewall-cmd --reload")
    unit = rgw_unit(node)
    LOG.info("rgw unit: %s", unit or "missing")
    if unit:
        quoted = shlex.quote(unit)
        node.exec_command(sudo=True, cmd=f"systemctl reset-failed {quoted}", check_ec=False)
        node.exec_command(sudo=True, cmd=f"systemctl restart {quoted}", check_ec=False)
    for _ in range(VERIFY_ATTEMPTS):
        if endpoint_ready(node):
            return
        sleep(VERIFY_PAUSE)
    dump_failure(node)
    raise RuntimeError("RGW did not accept HTTPS connections on port 443")


def reference_certificate(installer):
    """Return the certificate from the RGW config-key, without the private key."""
    out, _ = installer.exec_command(
        cmd=f"cephadm shell -- ceph config-key get {CONFIG_KEY}",
        sudo=True,
    )
    match = re.search(
        r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
        out,
        re.DOTALL,
    )
    if not match:
        raise ValueError(f"{CONFIG_KEY} does not contain a certificate")
    return match.group(0) + "\n"


def trust_certificate(cluster):
    """Install the reference certificate on the client and curl without -k."""
    installer = cluster.get_nodes(role="installer")[0]
    cert = reference_certificate(installer)
    seen = set()
    for role in TRUST_ROLES:
        for node in cluster.get_nodes(role=role):
            if node.hostname in seen:
                continue
            seen.add(node.hostname)
            LOG.info("Copying RGW reference certificate to %s", node.hostname)
            install_cephadm_root_ca_cert(node, cert, cert_name=REFERENCE_CERT_FILE)
    client = cluster.get_nodes(role="client")[0]
    rgw = cluster.get_nodes(role="rgw")[0]
    url = f"https://{rgw.ip_address}:{PORT}"
    LOG.info("Checking %s trusts %s", client.hostname, url)
    cmd = (
        "bash -c '"
        "for i in $(seq 1 6); do "
        f"curl -sS --connect-timeout 10 -o /dev/null {url} && exit 0; "
        "sleep 5; "
        "done; "
        "exit 1'"
    )
    client.exec_command(sudo=True, cmd=cmd)


def run(ceph_cluster, **kwargs):
    """Run one reference-certificate step. Config key action selects it."""
    action = (kwargs.get("config") or {}).get("action")
    steps = {
        "reload": reload_certificate,
        "verify": verify_endpoint,
        "trust": trust_certificate,
    }
    if action not in steps:
        LOG.error("Set action to one of: %s", ", ".join(steps))
        return 1
    try:
        steps[action](ceph_cluster)
    except Exception as exc:
        LOG.error("RGW reference HTTPS %s failed: %s", action, exc)
        return 1
    return 0
