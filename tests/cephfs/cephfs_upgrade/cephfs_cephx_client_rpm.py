"""Suite step: align ceph-common/ceph-fuse on a client to the upgraded cluster.

Used for Client2 and Client3 (config.client selects the entity).

Points Tools at ``config["base_url"]``, then installs host RPMs as
``2:{cluster_VR}.el{client_OS}cp`` (container dist-tag is not used for yum).
Asserts version-release equality, ignoring ``.elNcp``.
"""

import re
import shlex
import traceback

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    _cephadm_shell,
    client_by_entity,
)
from utility.log import Log

log = Log(__name__)

_DIST_TAG_RE = re.compile(r"\.el\d+.*$")


def _version_token(ceph_version_line: str) -> str:
    """Parse ``ceph version <token> ...`` / ``ceph --version`` output."""
    match = re.search(r"ceph\s+version\s+(\S+)", ceph_version_line or "")
    if not match:
        raise OperationFailedError(
            f"Unexpected ceph version line: {ceph_version_line!r}"
        )
    return match.group(1)


def _strip_dist(token: str) -> str:
    """``20.2.2-351.el10cp`` → ``20.2.2-351``."""
    return _DIST_TAG_RE.sub("", token or "")


def _tools_url(base_url: str, cloud_type: str) -> str:
    """Mirror CephAdmin.set_tool_repo Tools path for one client."""
    if not base_url:
        raise OperationFailedError("config base_url is required for client RPM align")
    if base_url.endswith(".repo"):
        return base_url
    if not base_url.endswith("/"):
        base_url += "/"
    if cloud_type == "ibmc":
        return base_url + "Tools"
    return base_url + "compose/Tools/x86_64/os/"


def _client_os_major(client) -> str:
    info = getattr(getattr(client, "node", client), "distro_info", None) or {}
    ver = (info.get("VERSION_ID") or "").strip()
    if not ver:
        out, _ = client.exec_command(
            sudo=True,
            cmd="bash -lc '. /etc/os-release && printf %s \"$VERSION_ID\"'",
        )
        ver = (out or "").strip()
    major = ver.split(".")[0]
    if not major.isdigit():
        raise OperationFailedError(
            f"Could not parse RHEL major on {client.node.hostname}: {ver!r}"
        )
    return major


def run(ceph_cluster, **kw):
    """Config keys: client; also uses base_url and cloud-type from run config."""
    try:
        config = kw.get("config") or {}
        entity = config.get("client", "client.2")
        clients = ceph_cluster.get_ceph_objects("client")
        installer = ceph_cluster.get_nodes("installer")[0]
        client = client_by_entity(clients, entity)

        ver = _cephadm_shell(installer, "ceph version")
        log.info("Cluster ceph version: %s", ver)
        cluster_tok = _version_token(ver)
        cluster_vr = _strip_dist(cluster_tok)
        os_major = _client_os_major(client)
        rpm_nevra = f"2:{cluster_vr}.el{os_major}cp"
        pkgs = " ".join(
            shlex.quote(p)
            for p in (f"ceph-common-{rpm_nevra}", f"ceph-fuse-{rpm_nevra}")
        )

        cloud_type = config.get("cloud-type", "openstack")
        tools = _tools_url(config.get("base_url", ""), cloud_type)
        log.info(
            "=== align ceph-common/ceph-fuse on %s (%s): Tools=%s pin=%s "
            "(cluster version %s → VR %s, host el%scp) ===",
            client.node.hostname,
            entity,
            tools,
            rpm_nevra,
            cluster_tok,
            cluster_vr,
            os_major,
        )
        client.exec_command(
            sudo=True,
            cmd=f"yum-config-manager --add-repo {shlex.quote(tools)}",
            timeout=300,
        )
        client.exec_command(
            sudo=True, cmd="yum update metadata", check_ec=False, timeout=300
        )
        client.exec_command(
            sudo=True,
            cmd=f"yum install -y --nogpgcheck --allowerasing {pkgs}",
            timeout=900,
        )
        cver, _ = client.exec_command(sudo=True, cmd="ceph --version")
        log.info("%s ceph --version: %s", entity, cver)
        client_vr = _strip_dist(_version_token(cver))
        if client_vr != cluster_vr:
            raise OperationFailedError(
                f"{entity} client VR {client_vr!r} != cluster VR {cluster_vr!r}"
            )
        rpm_q, _ = client.exec_command(sudo=True, cmd="rpm -q ceph-common ceph-fuse")
        log.info("%s packages:\n%s", entity, rpm_q)
        log.info("%s CephX client RPM align PASSED: VR %s", entity, client_vr)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
