"""Rewrite the RGW certificate after apply so Beast binds port 443.

The combined PEM is created on the installer by the register step at
/tmp/rgw_combined.pem. Apply can replace the monitor config-key, so this
writes that PEM again and sets rgw_frontends to the Beast HTTPS endpoint.
"""

import json
import shlex

from utility.log import Log

LOG = Log(__name__)

SERVICE = "rgw.rgw.ssl"
CONFIG_KEY = "rgw/cert/rgw.rgw.ssl"
PEM_HOST = "/tmp/rgw_combined.pem"
PEM_CONTAINER = "/etc/ceph/rgw_combined.pem"
FRONTEND = f"beast ssl_port=443 ssl_certificate=config://{CONFIG_KEY}"


def ceph(node, *args, mount_pem=False):
    """Run a ceph command inside cephadm shell on the installer."""
    mount = ""
    if mount_pem:
        mount = f"-v {PEM_HOST}:{PEM_CONTAINER} "
    inner = "cephadm shell " + mount + "-- ceph " + " ".join(shlex.quote(arg) for arg in args)
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


def reload_certificate(installer):
    """Publish the reference PEM and point every RGW frontend at it."""
    keys = {CONFIG_KEY}
    sections = {"client.rgw"}
    dump = ceph_json(installer, "config", "dump", "--format", "json")
    for item in dump:
        if item.get("name") != "rgw_frontends":
            continue
        LOG.info("FRONT %s %s", item.get("section"), item.get("value"))
        if item.get("section"):
            sections.add(item["section"])
        value = item.get("value") or ""
        for match in value.split():
            marker = "config://"
            if marker in match:
                keys.add(match.split(marker, 1)[1])
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


def run(ceph_cluster, **kwargs):
    """Rewrite the reference certificate and restart RGW."""
    try:
        installer = ceph_cluster.get_nodes(role="installer")[0]
        reload_certificate(installer)
    except Exception as exc:
        LOG.error("Failed to reload the RGW reference certificate: %s", exc)
        return 1
    return 0
