"""Kerberos helpers for NFS-Ganesha cephci tests."""

import json
import re
import shlex
import time

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, MountFailedError
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import create_nfs_via_file_and_verify, nfs_log_parser
from tests.nfs.security.kerberos_helper import _krb5_conf_content
from tests.nfs.security.security_utils import (
    _nfs_ls_to_cluster_names,
    wait_until_nfs_cluster_teardown,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_NFS_KRB_CLUSTER = "cephfs-nfs-krb"
KRB_KEYTAB_PATH = "/etc/krb5.keytab"
KRB5_CONF_PATH = "/etc/krb5.conf"
CLIENT_KEYTAB_STASH = "/tmp/cephci-krb5.keytab.stash"


def krb_config_get(config, *keys, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def node_fqdn(node, domain):
    """Build a stable FQDN used for Kerberos SPNs and /etc/hosts."""
    host = getattr(node, "hostname", "localhost")
    if "." in host:
        return host
    return "{}.{}".format(host, domain.lower())


def _configure_nfs_conf_gssd_use_proxy(node, use_proxy=False):
    """
    Set ``use-gss-proxy`` under ``[gssd]`` (RHEL reads this section, not ``[general]``).

    NFS clients typically need ``use-gss-proxy=0`` so ``rpc.gssd`` handles user
    tickets from ``kinit`` without gssproxy impersonation quirks.
    """
    value = "1" if use_proxy else "0"
    node.exec_command(
        sudo=True,
        cmd=(
            "test -f /etc/nfs.conf || touch /etc/nfs.conf; "
            "if grep -q '^[[:space:]]*\\[gssd\\]' /etc/nfs.conf; then "
            "if grep -q '^[[:space:]]*use-gss-proxy' /etc/nfs.conf; then "
            "sed -i 's/^[[:space:]]*use-gss-proxy.*/use-gss-proxy={val}/' "
            "/etc/nfs.conf; "
            "else sed -i '/^[[:space:]]*\\[gssd\\]/a use-gss-proxy={val}' "
            "/etc/nfs.conf; fi; "
            "else printf '\\n[gssd]\\nuse-gss-proxy={val}\\n' >> /etc/nfs.conf; fi"
        ).format(val=value),
        check_ec=False,
    )


def install_krb_client_packages(node, nfs_server=False):
    """Install NFS/Kerberos packages and enable RPCSEC_GSS units (no keytab yet)."""
    Package(node).install("krb5-workstation nfs-utils")
    node.exec_command(sudo=True, cmd="modprobe auth_rpcgss", check_ec=False)
    node.exec_command(
        sudo=True,
        cmd="systemctl enable nfs-client.target rpcbind gssproxy rpc-gssd nfs-idmapd",
        check_ec=False,
    )
    _configure_nfs_conf_gssd_use_proxy(node, use_proxy=True if nfs_server else False)
    _remove_stale_nfs_conf_general_gssd(node)


def _remove_stale_nfs_conf_general_gssd(node):
    """Drop mistaken ``[general] use-gss-proxy`` lines from earlier test runs."""
    node.exec_command(
        sudo=True,
        cmd=(
            "test -f /etc/nfs.conf && "
            "sed -i '/^\\[general\\]$/,/^\\[/"
            "{ /^\\[general\\]$/b; /^\\[/b; "
            "/use-gss-proxy/d; }' /etc/nfs.conf || true"
        ),
        check_ec=False,
    )


def open_nfs_client_firewall(node):
    """Allow inbound NFSv4 callbacks from Ganesha (SETCLIENTID confirm)."""
    for cmd in (
        "firewall-cmd --permanent --add-service=nfs",
        "firewall-cmd --reload",
    ):
        node.exec_command(sudo=True, cmd=cmd, check_ec=False)


def ensure_nfs_krb_client_services(node, require_rpc_gssd=True, timeout=90):
    """
    (Re)start NFS client GSS services after ``krb5.conf`` / keytab are in place.

    RHEL ``rpc-gssd`` refuses to stay up unless ``/etc/krb5.keytab`` exists.
    """
    node.exec_command(sudo=True, cmd="modprobe auth_rpcgss", check_ec=False)
    for svc in ("rpcbind", "gssproxy", "nfs-idmapd", "rpc-gssd"):
        node.exec_command(
            sudo=True,
            cmd="systemctl enable --now {}".format(svc),
            check_ec=False,
        )
    node.exec_command(
        sudo=True,
        cmd="systemctl restart nfs-client.target",
        check_ec=False,
    )
    node.exec_command(
        sudo=True,
        cmd="systemctl restart rpc-gssd",
        check_ec=False,
    )

    if not require_rpc_gssd:
        return

    last_status = ""
    for w in WaitUntil(timeout=timeout, interval=3):
        gssd, _ = node.exec_command(
            sudo=True,
            cmd="systemctl is-active rpc-gssd",
            check_ec=False,
        )
        keytab, _ = node.exec_command(
            sudo=True,
            cmd="test -s /etc/krb5.keytab && echo yes",
            check_ec=False,
        )
        if "active" in (gssd or ""):
            log.info(
                "rpc-gssd active on %s (keytab=%s)",
                node.hostname,
                "yes" in (keytab or ""),
            )
            return
        last_status, _ = node.exec_command(
            sudo=True,
            cmd="systemctl status rpc-gssd --no-pager -l | head -20",
            check_ec=False,
        )

    raise OperationFailedError(
        "rpc-gssd not active on {} after {}s (RHEL needs /etc/krb5.keytab). "
        "Status:\n{}".format(node.hostname, timeout, last_status)
    )


def install_client_host_keytab(kdc_setup, client_node, host_fqdn):
    """Install a host/ SPN keytab so ``rpc-gssd`` can start on NFS clients."""
    spn = kdc_setup.add_host_service_principal(host_fqdn)
    remote_kt = "/tmp/cephci-host-{}.keytab".format(
        client_node.hostname.replace(".", "-")
    )
    kdc_setup.export_keytab_for_principal(spn, remote_path=remote_kt)
    keytab_bytes = kdc_setup.fetch_keytab_bytes(remote_path=remote_kt)
    install_nfs_keytab(client_node, keytab_bytes)
    ensure_nfs_krb_client_services(client_node)
    log.info(
        "Installed client host keytab for %s on %s",
        spn,
        client_node.hostname,
    )


def write_krb5_conf(nodes, realm, domain, kdc_hostname, kdc_ip=None):
    content = _krb5_conf_content(
        realm.upper(), domain.lower(), kdc_hostname, kdc_ip=kdc_ip
    )
    for node in nodes:
        handle = node.remote_file(sudo=True, file_name=KRB5_CONF_PATH, file_mode="w")
        handle.write(content)
        handle.flush()
        log.info("Wrote %s on %s", KRB5_CONF_PATH, node.hostname)


def _sed_remove_hosts_names(node, names):
    """Remove any /etc/hosts line mentioning one of ``names`` (stale IPs)."""
    for name in names:
        safe = name.replace(".", r"\.").replace("/", r"\/")
        if name == "kdc":
            # Short alias only — do not match substrings like kdc.ceph.test.
            node.exec_command(
                sudo=True,
                cmd=(
                    "sed -i '/[[:space:]]kdc[[:space:]]/d;"
                    "/[[:space:]]kdc$/d' /etc/hosts"
                ),
                check_ec=False,
            )
        else:
            node.exec_command(
                sudo=True,
                cmd="sed -i '/{}/d' /etc/hosts".format(safe),
                check_ec=False,
            )


def ensure_hosts_entries(node, ip, aliases):
    """
    Ensure ``ip`` is the canonical address for ``aliases`` on ``node``.

    Replaces existing lines that mention any alias so leftover cluster
    reruns do not keep an old KDC or NFS IP.
    """
    names = aliases if isinstance(aliases, (list, tuple)) else [aliases]
    _sed_remove_hosts_names(node, names)
    line = "{} {}".format(ip, " ".join(names))
    node.exec_command(
        sudo=True,
        cmd="echo '{}' >> /etc/hosts".format(line),
    )
    log.info("Ensured hosts entry on %s: %s", node.hostname, line)


def merge_hosts_entries(nodes, hostmap):
    """
    Add or refresh static host entries on every node in ``hostmap``.

    Args:
        nodes: Iterable of CephNode objects.
        hostmap: dict mapping IP -> list of host aliases.
    """
    for node in nodes:
        for ip, names in hostmap.items():
            ensure_hosts_entries(node, ip, names)


def _log_kdc_connectivity_diagnostics(node, kdc_ip, kdc_node=None):
    """Collect krb5/KDC reachability hints after a failed kinit."""
    krb5_snip, _ = node.exec_command(
        sudo=True,
        cmd="grep -E '^[[:space:]]*kdc' /etc/krb5.conf || true",
        check_ec=False,
    )
    hosts_snip, _ = node.exec_command(
        sudo=True,
        cmd="grep -E 'kdc|{}' /etc/hosts || true".format(
            (kdc_ip or "").replace(".", r"\.")
        ),
        check_ec=False,
    )
    log.error(
        "KDC connectivity diagnostics from %s:\n  krb5.conf kdc: %s\n  /etc/hosts: %s",
        node.hostname,
        (krb5_snip or "").strip(),
        (hosts_snip or "").strip().replace("\n", " | "),
    )
    if kdc_node is not None:
        listeners, _ = kdc_node.exec_command(
            sudo=True,
            cmd=(
                "ss -H -lnptu | grep -E ':88 |:749 ' || true; "
                "firewall-cmd --list-ports 2>/dev/null || true"
            ),
            check_ec=False,
        )
        log.error(
            "KDC host %s listeners/firewall: %s",
            kdc_node.hostname,
            (listeners or "").strip().replace("\n", "; "),
        )


def verify_kdc_kinit_from_node(node, kdc_setup, timeout=60, interval=5):
    """
    Confirm the KDC answers krb5 AS-REQ from ``node`` (UDP/TCP 88).

    Uses password kinit for the test user — same path NFS nodes need.
    """
    principal = kdc_setup.test_principal
    pw = kdc_setup.test_password.replace('"', '\\"')
    last_err = ""
    for w in WaitUntil(timeout=timeout, interval=interval):
        node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)
        _, err = node.exec_command(
            sudo=True,
            cmd="bash -c 'echo \"{}\" | kinit {}'".format(pw, principal),
            check_ec=False,
        )
        if node.exit_status == 0:
            log.info(
                "KDC reachable from %s via kinit %s (~%ss)",
                node.hostname,
                principal,
                w._attempt * w.interval,
            )
            node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)
            return True
        last_err = (err or "").strip()
        log.warning(
            "KDC kinit from %s attempt %s failed: %s",
            node.hostname,
            w._attempt,
            last_err,
        )
    raise OperationFailedError(
        "Cannot kinit {} from {} within {}s: {}".format(
            principal, node.hostname, timeout, last_err
        )
    )


def install_nfs_keytab(nfs_node, keytab_bytes, path=KRB_KEYTAB_PATH):
    if isinstance(keytab_bytes, str):
        raise TypeError("keytab_bytes must be binary; use fetch_keytab_bytes()")
    handle = nfs_node.remote_file(sudo=True, file_name=path, file_mode="wb")
    handle.write(keytab_bytes)
    handle.flush()
    handle.close()
    nfs_node.exec_command(sudo=True, cmd="chmod 600 {}".format(path))
    nfs_node.exec_command(sudo=True, cmd="chown root:root {}".format(path))
    log.info("Installed NFS keytab on %s at %s", nfs_node.hostname, path)


def kinit_user(client_node, principal, password):
    client_node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)
    client_node.exec_command(
        sudo=True,
        cmd="bash -c 'echo \"{}\" | kinit {}'".format(
            password.replace('"', '\\"'),
            principal,
        ),
    )
    out, _ = client_node.exec_command(sudo=True, cmd="klist")
    if principal not in (out or ""):
        raise OperationFailedError(
            "kinit did not produce a ticket for {} on {}".format(
                principal, client_node.hostname
            )
        )
    client_node.exec_command(
        sudo=True, cmd="systemctl restart rpc-gssd", check_ec=False
    )
    log.info("kinit succeeded for %s on %s", principal, client_node.hostname)


def kinit_service_keytab(
    nfs_node,
    nfs_fqdn,
    realm,
    kdc_ip=None,
    kdc_node=None,
    timeout=90,
    interval=5,
):
    principal = "nfs/{}@{}".format(nfs_fqdn, realm.upper())
    cmd = "kinit -kt {} {}".format(KRB_KEYTAB_PATH, principal)
    nfs_node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)
    last_err = ""
    for w in WaitUntil(timeout=timeout, interval=interval):
        _, err = nfs_node.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        if nfs_node.exit_status == 0:
            break
        last_err = (err or "").strip()
        log.warning(
            "Service kinit for %s on %s attempt %s failed: %s",
            principal,
            nfs_node.hostname,
            w._attempt,
            last_err,
        )
    else:
        _log_kdc_connectivity_diagnostics(nfs_node, kdc_ip, kdc_node=kdc_node)
        raise OperationFailedError(
            "Service kinit failed for {} on {} within {}s: {}".format(
                principal, nfs_node.hostname, timeout, last_err
            )
        )
    out, _ = nfs_node.exec_command(sudo=True, cmd="klist")
    if principal not in (out or ""):
        raise OperationFailedError(
            "Service kinit failed for {} on {} (klist missing ticket)".format(
                principal, nfs_node.hostname
            )
        )
    log.info("NFS service ticket acquired on %s for %s", nfs_node.hostname, principal)


def kdestroy_all(node):
    node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)


def _mount_stderr(client_node, mount_cmd):
    """Run mount without raising; return combined stdout/stderr text."""
    result = client_node.exec_command(
        sudo=True,
        cmd=mount_cmd,
        check_ec=False,
    )
    if isinstance(result, tuple):
        parts = [result[0] or ""]
        if len(result) > 1 and result[1]:
            parts.append(result[1])
        return "\n".join(parts)
    return result or ""


def _is_invalid_mount_option_error(mount_output):
    return "incorrect mount option" in (mount_output or "").lower()


def _stash_client_host_keytab(client_node):
    """
    Move the client host keytab aside for negative mount tests.

    RHEL ``rpc-gssd`` can authenticate ``sec=krb5`` mounts using machine
    credentials from ``host/`` keytab even when no user ``kinit`` ticket exists.
    """
    out, _ = client_node.exec_command(
        sudo=True,
        cmd="test -s {} && echo yes".format(KRB_KEYTAB_PATH),
        check_ec=False,
    )
    if "yes" not in (out or ""):
        return False
    client_node.exec_command(
        sudo=True,
        cmd=("mv -f {keytab} {stash} && " "install -m 600 /dev/null {keytab}").format(
            keytab=KRB_KEYTAB_PATH, stash=CLIENT_KEYTAB_STASH
        ),
        check_ec=False,
    )
    log.info(
        "Stashed client host keytab on %s for unauthenticated mount check",
        client_node.hostname,
    )
    return True


def _restore_client_host_keytab(client_node):
    """Restore a stashed client host keytab after a negative mount test."""
    client_node.exec_command(
        sudo=True,
        cmd=("test -f {stash} && mv -f {stash} {keytab}").format(
            stash=CLIENT_KEYTAB_STASH, keytab=KRB_KEYTAB_PATH
        ),
        check_ec=False,
    )


def check_mount_fails_without_ticket(client_node, mount_cmd):
    """Mount must not succeed without a user ``kinit`` ticket."""
    client_node.exec_command(sudo=True, cmd="kdestroy -A", check_ec=False)
    stashed = _stash_client_host_keytab(client_node)
    try:
        client_node.exec_command(
            sudo=True, cmd="systemctl restart rpc-gssd", check_ec=False
        )
        client_node.exec_command(
            sudo=True, cmd="systemctl restart gssproxy", check_ec=False
        )
        time.sleep(2)
        mount_path = mount_cmd.strip().split()[-1]
        prepare_client_mount_dir(client_node, mount_path)
        attempts = [mount_cmd]
        if mount_cmd.startswith("mount -t nfs4 -o "):
            attempts.append(
                "mount -t nfs -o nfsvers=4.2,{}".format(
                    mount_cmd[len("mount -t nfs4 -o ") :]
                )
            )
        out = ""
        for cmd in attempts:
            out = _mount_stderr(client_node, cmd)
            if not _is_invalid_mount_option_error(out):
                break
        mnt, _ = client_node.exec_command(
            sudo=True,
            cmd="findmnt -n -o FSTYPE,OPTIONS --target {}".format(mount_path),
            check_ec=False,
        )
        if (mnt or "").strip() and "nfs" in (mnt or "").lower():
            client_node.exec_command(
                sudo=True,
                cmd="umount -l {}".format(mount_path),
                check_ec=False,
            )
            log.error(
                "Mount succeeded without user Kerberos ticket: %s",
                (mnt or "").strip(),
            )
            return False
        if _is_invalid_mount_option_error(out) and not stashed:
            raise OperationFailedError(
                "Mount failed with 'incorrect mount option' before Kerberos auth; "
                "NFS client RPCSEC_GSS is not ready (rpc-gssd needs "
                "/etc/krb5.keytab on RHEL, plus nfs-client.target). Output: {}".format(
                    out.strip()
                )
            )
        log.info(
            "Mount failed without user ticket as expected: %s",
            out.strip()[:200],
        )
        return True
    finally:
        if stashed:
            _restore_client_host_keytab(client_node)
            ensure_nfs_krb_client_services(client_node)


def _parse_orch_nfs_services(client_node):
    """Return ``ceph orch ls --service_type nfs`` as a list of dicts."""
    raw, _ = client_node.exec_command(
        sudo=True,
        cmd="ceph orch ls --service_type nfs --format json",
        timeout=120,
        check_ec=False,
    )
    if raw is None:
        return []
    text = raw.strip() if isinstance(raw, str) else raw.read().decode().strip()
    if not text or "No services reported" in text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        log.warning("Could not parse orch nfs ls JSON: %s", text[:200])
        return []
    return data if isinstance(data, list) else [data]


def _service_placement_hosts(service):
    """Extract hostnames from an orch NFS service entry."""
    placement = service.get("placement") or {}
    hosts = placement.get("hosts")
    if hosts:
        return list(hosts)
    return []


def _parse_nfs_cluster_names(client_node):
    """Return NFS cluster/service ids from mgr ``nfs cluster ls``."""
    try:
        raw = Ceph(client_node).nfs.cluster.ls()
    except Exception as ex:
        log.debug("nfs cluster ls failed: %s", ex)
        return []
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return _nfs_ls_to_cluster_names(raw)
    text = raw.strip() if isinstance(raw, str) else raw.read().decode().strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return _nfs_ls_to_cluster_names([text])
    if isinstance(data, list):
        return _nfs_ls_to_cluster_names(data)
    return _nfs_ls_to_cluster_names([data])


def _collect_nfs_clusters_to_remove(
    client_node, hostnames, keep_service_id=None, extra_remove_names=None
):
    """Build set of NFS cluster ids to delete before a Kerberos deploy."""
    host_set = set(hostnames)
    extra = set(extra_remove_names or [])
    to_delete = set(extra)
    orch_by_sid = {}

    for svc in _parse_orch_nfs_services(client_node):
        sid = svc.get("service_id")
        if not sid:
            name = svc.get("service_name") or ""
            if name.startswith("nfs."):
                sid = name.split("nfs.", 1)[-1]
        if sid:
            orch_by_sid[sid] = set(_service_placement_hosts(svc))

    for sid, placed in orch_by_sid.items():
        if keep_service_id and sid == keep_service_id:
            continue
        if sid in extra or (placed & host_set):
            to_delete.add(sid)

    for name in _parse_nfs_cluster_names(client_node):
        if keep_service_id and name == keep_service_id:
            continue
        if name in extra:
            to_delete.add(name)
            continue
        placed = orch_by_sid.get(name)
        if placed is None:
            log.info(
                "NFS cluster %r listed by mgr but not orch; scheduling delete",
                name,
            )
            to_delete.add(name)
            continue
        if placed & host_set:
            to_delete.add(name)

    if keep_service_id:
        to_delete.discard(keep_service_id)
    return sorted(to_delete)


def _stop_cephadm_nfs_daemons_on_host(client_node, hostname):
    """Ask orch to stop NFS daemons scheduled on ``hostname``."""
    raw, _ = client_node.exec_command(
        sudo=True,
        cmd="ceph orch ps --hostname {} --format json".format(hostname),
        timeout=120,
        check_ec=False,
    )
    text = raw.strip() if isinstance(raw, str) else (raw or "").strip()
    if not text:
        return
    try:
        daemons = json.loads(text)
    except json.JSONDecodeError:
        log.debug("Could not parse orch ps JSON for %s", hostname)
        return
    if not isinstance(daemons, list):
        daemons = [daemons]
    for daemon in daemons:
        svc = str(daemon.get("service_name") or "")
        if "nfs" not in svc.lower():
            continue
        daemon_name = daemon.get("daemon_name") or daemon.get("name")
        if not daemon_name:
            continue
        log.info("Stopping orphan NFS daemon %s on %s", daemon_name, hostname)
        client_node.exec_command(
            sudo=True,
            cmd="ceph orch daemon stop {}".format(daemon_name),
            check_ec=False,
        )


def force_free_nfs_port_on_host(client_node, node, port=2049):
    """
    Release ``port`` on ``node`` when mgr delete left a listener behind.

    Orphan podman/docker Ganesha containers and stale host nfs-server units
    are common after ``--reuse`` runs when orch state no longer lists the cluster.
    """
    port = int(port)
    log.info(
        "Force-releasing TCP port %s on %s (orphan NFS/Ganesha cleanup)",
        port,
        node.hostname,
    )
    _stop_cephadm_nfs_daemons_on_host(client_node, node.hostname)

    node.exec_command(
        sudo=True,
        cmd="systemctl stop nfs-server nfs-kernel-server 2>/dev/null || true",
        check_ec=False,
    )
    for runtime in ("podman", "docker"):
        node.exec_command(
            sudo=True,
            cmd=(
                "for cid in $({rt} ps -q --filter publish={p} 2>/dev/null); do "
                '{rt} rm -f "$cid" 2>/dev/null; done'
            ).format(rt=runtime, p=port),
            check_ec=False,
        )
    node.exec_command(
        sudo=True,
        cmd=(
            "ss -Hlnpt 'sport = :{p}' 2>/dev/null | "
            "sed -n 's/.*pid=\\([0-9]*\\).*/\\1/p' | sort -u | "
            "while read pid; do "
            '[ -n "$pid" ] && kill -9 "$pid" 2>/dev/null; done'
        ).format(p=port),
        check_ec=False,
    )


def wait_for_tcp_port_free(node, port, timeout=180, interval=5):
    """Poll until ``port`` is not listening on ``node``."""
    port = int(port)
    for w in WaitUntil(timeout=timeout, interval=interval):
        out, _ = node.exec_command(
            sudo=True,
            cmd="ss -Hlnpt 'sport = :{}'".format(port),
            check_ec=False,
        )
        if not (out or "").strip():
            log.info(
                "TCP port %s free on %s after ~%s s",
                port,
                node.hostname,
                w._attempt * w.interval,
            )
            return True
        log.debug(
            "Port %s still in use on %s: %s",
            port,
            node.hostname,
            (out or "").strip()[:120],
        )
    log.warning(
        "Timed out after %ss waiting for port %s on %s",
        timeout,
        port,
        node.hostname,
    )
    return False


def remove_conflicting_nfs_on_hosts(
    client_node,
    hostnames,
    keep_service_id=None,
    extra_remove_names=None,
    timeout=300,
):
    """
    Delete NFS orch clusters on ``hostnames`` so a new cluster can bind port 2049.

    Removes any ``nfs`` service whose placement overlaps ``hostnames``, any cluster
    name returned by ``ceph nfs cluster ls``, and ids listed in
    ``extra_remove_names`` (e.g. bootstrap ``cephfs-nfs``).
    """
    to_delete = _collect_nfs_clusters_to_remove(
        client_node,
        hostnames,
        keep_service_id=keep_service_id,
        extra_remove_names=extra_remove_names,
    )

    for sid in to_delete:
        log.info(
            "Removing NFS cluster %r (conflicts with Kerberos deploy on %s)",
            sid,
            sorted(set(hostnames)),
        )
        try:
            Ceph(client_node).nfs.cluster.delete(sid)
        except Exception as ex:
            log.debug("nfs cluster delete %s: %s", sid, ex)
        wait_until_nfs_cluster_teardown(client_node, sid, timeout=timeout)

    for hostname in sorted(set(hostnames)):
        _stop_cephadm_nfs_daemons_on_host(client_node, hostname)


def prepare_nfs_hosts_for_kerberos_deploy(
    client_node,
    nfs_nodes,
    nfs_name,
    port=2049,
    config=None,
):
    """
    Ensure NFS placement hosts can bind ``port`` before applying a Kerberos spec.

    Drops leftover ``nfs_name`` state, removes bootstrap clusters (e.g.
    ``cephfs-nfs``) on the same nodes, and waits for the TCP port to be free.
    """
    config = config or {}
    hostnames = [n.hostname for n in nfs_nodes]
    strip = config.get("strip_conflicting_nfs_on_hosts", True)
    extra_remove = krb_config_get(
        config,
        "remove_nfs_clusters",
        "conflicting_nfs_clusters",
        default=["cephfs-nfs"],
    )
    if isinstance(extra_remove, str):
        extra_remove = [extra_remove]

    cleanup_kerberos_nfs_stack(client_node, nfs_name)

    if strip:
        remove_conflicting_nfs_on_hosts(
            client_node,
            hostnames,
            keep_service_id=nfs_name,
            extra_remove_names=extra_remove,
        )

    port_wait = int(config.get("nfs_port_free_timeout_sec", 180))
    for nfs_node in nfs_nodes:
        if wait_for_tcp_port_free(nfs_node, port, timeout=30):
            continue
        force_free_nfs_port_on_host(client_node, nfs_node, port=port)
        if wait_for_tcp_port_free(nfs_node, port, timeout=port_wait):
            continue
        holder, _ = nfs_node.exec_command(
            sudo=True,
            cmd="ss -lnpt 'sport = :{}'".format(port),
            check_ec=False,
        )
        raise OperationFailedError(
            "Port {} still in use on {} after orch delete and force cleanup "
            "(~{}s): {}".format(
                port,
                nfs_node.hostname,
                port_wait,
                (holder or "").strip().replace("\n", " | "),
            )
        )


def wait_for_no_keytab_errors_in_nfs_logs(
    client_node, nfs_nodes, nfs_cluster_service_id, timeout=180, interval=5
):
    bad = [
        "no usable keytab entry found in keytab",
        "gssd_refresh_krb5_machine_credential",
    ]
    for w in WaitUntil(timeout=timeout, interval=interval):
        logs_ok = True
        for needle in bad:
            if (
                nfs_log_parser(
                    client_node,
                    nfs_nodes,
                    nfs_cluster_service_id,
                    expect_list=[needle],
                    expect_quiet=True,
                )
                == 0
            ):
                logs_ok = False
                break
        if logs_ok:
            log.info(
                "No keytab CRIT patterns in NFS logs after ~%s s",
                w._attempt * w.interval,
            )
            return True
    log.warning("Timed out after %ss waiting for clean NFS Kerberos logs", timeout)
    return False


def deploy_kerberos_nfs_cluster(
    installer_node,
    client_node,
    nfs_nodes,
    nfs_name,
    nfs_server_hostname,
    port=2049,
    config=None,
):
    """
    Deploy NFS-Ganesha via orch spec with host krb5.conf and krb5.keytab bind-mounts.
    """
    prepare_nfs_hosts_for_kerberos_deploy(
        client_node, nfs_nodes, nfs_name, port=port, config=config
    )

    Ceph(client_node).mgr.module.enable(module="nfs", force=True)
    time.sleep(3)

    placement_hosts = [n.hostname for n in nfs_nodes]
    nfs_spec = {
        "service_type": "nfs",
        "service_id": nfs_name,
        "placement": {"hosts": placement_hosts},
        "extra_container_args": [
            "-v /etc/krb5.conf:/etc/krb5.conf:ro",
            "-v /etc/krb5.keytab:/etc/krb5.keytab:ro",
        ],
        "spec": {
            "port": int(port),
        },
    }
    log.info(
        "Applying Kerberos-enabled NFS spec for %s on %s",
        nfs_name,
        placement_hosts,
    )
    if not create_nfs_via_file_and_verify(
        installer_node,
        [nfs_spec],
        timeout=300,
        nfs_nodes=nfs_nodes,
        cluster_nodes=None,
        service_id=nfs_name,
    ):
        raise OperationFailedError(
            "Failed to deploy Kerberos NFS cluster {}".format(nfs_name)
        )
    pass


def get_export_json(client_node, nfs_name, export_path):
    """Return parsed ``ceph nfs export get`` JSON for an export."""
    raw = Ceph(client_node).nfs.export.get(nfs_name, export_path, format="json")
    if isinstance(raw, tuple):
        raw = raw[0]
    return json.loads(raw or "{}")


def verify_export_sectypes(client_node, nfs_name, export_path, expected_sectypes):
    """
    Assert export configuration includes each requested RPCSEC_GSS flavor.

    Sectype may appear under different keys across Ceph releases; match
    case-insensitively anywhere in the export JSON blob.
    """
    if isinstance(expected_sectypes, str):
        expected_sectypes = [expected_sectypes]
    data = get_export_json(client_node, nfs_name, export_path)
    blob = json.dumps(data).lower()
    missing = [s for s in expected_sectypes if s.lower() not in blob]
    if missing:
        raise OperationFailedError(
            "Export {} on {} missing sectype(s) {} in JSON: {}".format(
                export_path, nfs_name, missing, data
            )
        )
    log.info("Export %s sectypes verified: %s", export_path, list(expected_sectypes))


def add_client_hosts_alias(client_node, ip_address, alias):
    """Append a single /etc/hosts alias on ``client_node`` (for negative SPN tests)."""
    out, _ = client_node.exec_command(
        sudo=True,
        cmd="grep -qF '{}' /etc/hosts && echo present".format(alias),
        check_ec=False,
    )
    if "present" in (out or ""):
        return
    client_node.exec_command(
        sudo=True,
        cmd="echo '{} {}' >> /etc/hosts".format(ip_address, alias),
    )
    log.info(
        "Added hosts alias on %s: %s -> %s",
        client_node.hostname,
        alias,
        ip_address,
    )


def isolate_client_hosts_alias(client_node, ip_address, alias, remove_names):
    """
    Map ``ip_address`` to ``alias`` only on the client.

    Removes ``remove_names`` from ``/etc/hosts`` so reverse lookups cannot
    fall back to the real NFS FQDN during wrong-SPN negative tests.
    """
    names = list(remove_names or [])
    if alias not in names:
        names.append(alias)
    _sed_remove_hosts_names(client_node, names)
    ensure_hosts_entries(client_node, ip_address, [alias])
    log.info(
        "Isolated hosts alias on %s: %s -> %s (removed %s)",
        client_node.hostname,
        alias,
        ip_address,
        remove_names,
    )


_DEFAULT_KRB_CLIENT_MOUNT_PATHS = (
    "/mnt/nfs_krb",
    "/mnt/nfs_krb_plain",
    "/mnt/nfs_krb_strict",
    "/mnt/nfs_krb_wrong_spn",
    "/mnt/nfs_krb_spn_ok",
    "/mnt/nfs_krb_c0",
    "/mnt/nfs_krb_c1",
    "/mnt/nfs_krb_flavor_krb5i",
    "/mnt/nfs_krb_flavor_krb5p",
    "/mnt/nfs_krb_msec",
    "/mnt/nfs_krb_msec/krb5",
    "/mnt/nfs_krb_msec/krb5i",
    "/mnt/nfs_krb_msec/krb5p",
)


def collect_kerberos_client_mount_paths(config):
    """Return mount paths used by Kerberos suite tests (for cleanup)."""
    paths = []

    def add(path):
        if path and path not in paths:
            paths.append(path)

    for path in _DEFAULT_KRB_CLIENT_MOUNT_PATHS:
        add(path)

    add(krb_config_get(config, "krb_client_mount", "nfs_mount", default=None))
    for path in krb_config_get(config, "krb_client_mounts", default=[]):
        add(path)

    mount_base = krb_config_get(config, "krb_client_mount_base", default=None)
    if mount_base:
        add(mount_base)
        for flavor in krb_config_get(config, "sec_flavors", default=["krb5i", "krb5p"]):
            add("{}_{}".format(mount_base.rstrip("/"), flavor))
        sectypes = krb_config_get(
            config, "export_sectypes", default=["krb5", "krb5i", "krb5p"]
        )
        if isinstance(sectypes, str):
            sectypes = [sectypes]
        for flavor in sectypes:
            add("{}/{}".format(mount_base.rstrip("/"), flavor))

    add(krb_config_get(config, "plain_krb_mount", default=None))
    add(krb_config_get(config, "krb_only_mount", default=None))
    add(krb_config_get(config, "wrong_spn_mount", default=None))

    return paths


def prepare_client_mount_dir(client_node, mount_path):
    """
    Clear stale NFS mounts and recreate a local mount directory.

    Reused clusters often leave orphan mount points that cause
    ``Stale file handle`` on ``mkdir``.
    """
    if not mount_path:
        return
    client_node.exec_command(
        sudo=True,
        cmd="umount -l {} 2>/dev/null || true".format(mount_path),
        check_ec=False,
    )
    try:
        client_node.exec_command(sudo=True, cmd="stat {}".format(mount_path))
    except CommandFailed:
        pass
    else:
        client_node.exec_command(
            sudo=True,
            cmd="rm -rf {}".format(mount_path),
            check_ec=False,
        )
        log.info(
            "Cleared stale/leftover mount at %s on %s",
            mount_path,
            client_node.hostname,
        )
    client_node.exec_command(
        sudo=True,
        cmd="mkdir -p {}".format(mount_path),
    )


def check_krb_mount_fails(client_node, mount_cmd):
    """
    Run a Kerberos mount expected to fail (wrong SPN, no ticket, etc.).

    Returns True on failure. Raises if the client stack is misconfigured
    (``incorrect mount option`` before auth is attempted).
    """
    ensure_nfs_krb_client_services(client_node)
    mount_path = mount_cmd.strip().split()[-1]
    prepare_client_mount_dir(client_node, mount_path)
    out = _mount_stderr(client_node, mount_cmd)
    mnt, _ = client_node.exec_command(
        sudo=True,
        cmd="findmnt -n -o FSTYPE,OPTIONS --target {}".format(mount_path),
        check_ec=False,
    )
    if (mnt or "").strip() and "nfs" in (mnt or "").lower():
        client_node.exec_command(
            sudo=True,
            cmd="umount -l {}".format(mount_path),
            check_ec=False,
        )
        log.error("Kerberos mount succeeded but was expected to fail: %s", mount_cmd)
        return False
    if _is_invalid_mount_option_error(out):
        raise OperationFailedError(
            "Mount failed with 'incorrect mount option' before Kerberos auth; "
            "fix client RPCSEC_GSS setup. Output: {}".format(out.strip())
        )
    log.info("Kerberos mount failed as expected: %s", out.strip()[:200])
    return True


def create_kerberos_export(
    client_node,
    fs_name,
    nfs_name,
    nfs_export,
    fs,
    sectype=None,
    export_settle_sec=30,
):
    """
    Create export with RPCSEC_GSS sectype (defaults to krb5 only).

    Polls for export to appear in listing rather than using fixed sleep.
    """
    if sectype is None:
        sectype = ["krb5"]
    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=nfs_export,
        fs=fs,
        sectype=sectype,
    )

    # Poll for export to appear with timeout
    max_wait = int(export_settle_sec)
    start = time.time()
    exports = []
    while time.time() - start < max_wait:
        raw = Ceph(client_node).nfs.export.ls(nfs_name)
        exports = json.loads(raw) if raw else []
        if nfs_export in exports:
            log.info("Created Kerberos export %s on cluster %s", nfs_export, nfs_name)
            return
        time.sleep(2)

    raise OperationFailedError(
        "Export {} not listed after {}s: {}".format(nfs_export, max_wait, exports)
    )


def delete_kerberos_export(client_node, nfs_name, export_path):
    """Remove a single Kerberos export."""
    Ceph(client_node).nfs.export.delete(nfs_name, export_path)
    log.info("Deleted Kerberos export %s on %s", export_path, nfs_name)


def build_sys_mount_cmd(server, export_path, mount_path, version="4.2"):
    """Mount with explicit ``sec=sys`` (AUTH_SYS).

    Omitting ``sec=`` lets rpc-gssd negotiate krb5 when tickets/keytabs exist,
    which would falsely pass negative enforcement tests on krb-only exports.
    """
    ver = str(version)
    if ver.startswith("4"):
        return "mount -t nfs4 -o sec=sys {server}:{export} {mount}".format(
            server=server,
            export=export_path,
            mount=mount_path,
        )
    return "mount -t nfs -o nfsvers={vers},sec=sys {server}:{export} {mount}".format(
        vers=version,
        server=server,
        export=export_path,
        mount=mount_path,
    )


def unmount_and_remove(client_node, mount_path):
    """Lazy umount and remove mount directory."""
    from cli.utilities.filesys import Unmount

    Unmount(client_node).unmount(mount_path)
    client_node.exec_command(
        sudo=True, cmd="rm -rf {}".format(mount_path), check_ec=False
    )


def build_krb_mount_cmd(server, export_path, mount_path, version, sec="krb5"):
    """
    Assemble an NFS RPCSEC_GSS mount command.

    Uses ``mount -t nfs4`` with ``sec=`` only (RHEL-documented style). ``vers=``
    with ``sec=krb5`` is rejected on some nfs-utils builds.
    """
    ver = str(version)
    opt_str = "sec={}".format(sec)
    if ver.startswith("4"):
        return "mount -t nfs4 -o {opts} {server}:{export} {mount}".format(
            opts=opt_str,
            server=server,
            export=export_path,
            mount=mount_path,
        )
    return "mount -t nfs -o nfsvers={vers},{opts} {server}:{export} {mount}".format(
        vers=version,
        opts=opt_str,
        server=server,
        export=export_path,
        mount=mount_path,
    )


def cleanup_kerberos_nfs_stack(client_node, nfs_name):
    """
    Remove exports and the NFS cluster created by Kerberos tests.

    Unlike ``cleanup_cluster``, does not delete ``ganeshagroup`` or unrelated
    subvolumes so the next case can create exports normally.
    """
    exports = []
    cleanup_errors = []

    try:
        raw = Ceph(client_node).nfs.export.ls(nfs_name)
        exports = json.loads(raw) if raw else []
    except (CommandFailed, OperationFailedError) as ex:
        log.warning("Could not list exports for %s: %s", nfs_name, ex)
        cleanup_errors.append("export list: {}".format(ex))

    for export_path in exports:
        try:
            Ceph(client_node).nfs.export.delete(nfs_name, export_path)
            log.info("Deleted export %s on %s", export_path, nfs_name)
        except (CommandFailed, OperationFailedError) as ex:
            log.warning("Export delete %s: %s", export_path, ex)
            cleanup_errors.append("export {}: {}".format(export_path, ex))

    try:
        Ceph(client_node).nfs.cluster.delete(nfs_name)
        log.info("Deleted NFS cluster %s", nfs_name)
    except (CommandFailed, OperationFailedError) as ex:
        log.warning("Cluster delete %s: %s", nfs_name, ex)
        cleanup_errors.append("cluster: {}".format(ex))

    wait_until_nfs_cluster_teardown(client_node, nfs_name)

    for export_path in exports:
        subvol_name = export_path.replace("/", "")
        if not subvol_name:
            continue
        try:
            safe_subvol = shlex.quote(subvol_name)
            client_node.exec_command(
                sudo=True,
                cmd="ceph fs subvolume rm cephfs {} --group_name ganeshagroup".format(
                    safe_subvol
                ),
                check_ec=False,
            )
        except (CommandFailed, OperationFailedError) as ex:
            log.warning("Subvolume delete %s: %s", subvol_name, ex)
            cleanup_errors.append("subvolume {}: {}".format(subvol_name, ex))

    if cleanup_errors:
        log.error("Cleanup completed with errors: %s", "; ".join(cleanup_errors))


def _mount_krb5_once(
    client_node,
    mount_path,
    version,
    port,
    server,
    export_path,
    sec="krb5",
    fstype="nfs4",
):
    """Single mount attempt with explicit ``fstype`` (``nfs4`` or ``nfs``)."""
    ver = str(version)
    kwargs = {"sec": sec}
    if fstype == "nfs4" and ver.startswith("4"):
        kwargs["fstype"] = "nfs4"
    else:
        kwargs["fstype"] = "nfs"
        kwargs["use_nfsvers"] = True
    Mount(client_node).nfs(
        mount=mount_path,
        version=version,
        port=str(port) if port is not None else "2049",
        server=server,
        export=export_path,
        **kwargs,
    )


def mount_krb5(
    client_node,
    mount_path,
    version,
    port,
    server,
    export_path,
    sec="krb5",
):
    ensure_nfs_krb_client_services(client_node)
    open_nfs_client_firewall(client_node)

    ver = str(version)
    last_err = None
    for fstype in ("nfs4", "nfs"):
        try:
            _mount_krb5_once(
                client_node,
                mount_path,
                version,
                port,
                server,
                export_path,
                sec=sec,
                fstype=fstype if ver.startswith("4") else "nfs",
            )
            log.info(
                "Kerberos mount %s:%s -> %s (fstype=%s sec=%s) on %s",
                server,
                export_path,
                mount_path,
                fstype,
                sec,
                client_node.hostname,
            )
            return
        except MountFailedError as err:
            last_err = err
            log.warning(
                "Kerberos mount with fstype=%s failed on %s: %s",
                fstype,
                client_node.hostname,
                err,
            )
            # Cleanup failed mount attempt
            client_node.exec_command(
                sudo=True,
                cmd="umount -l {} 2>/dev/null || true".format(mount_path),
                check_ec=False,
            )

    raise MountFailedError(
        "Kerberos mount failed on {} (tried nfs4 and nfs): {}".format(
            client_node.hostname, last_err
        )
    )


def provision_kerberos_environment(
    kdc_setup,
    kdc_node,
    nfs_nodes,
    client_nodes,
    installer_node,
    domain,
    kdc_hostname,
):
    """
    Configure krb5, hosts, principals, and keytabs across the cluster.
    """
    realm = kdc_setup.realm
    all_nodes = list(
        {
            n.hostname: n for n in [kdc_node, installer_node] + nfs_nodes + client_nodes
        }.values()
    )

    install_krb_client_packages(kdc_node)
    for nfs_node in nfs_nodes:
        install_krb_client_packages(nfs_node, nfs_server=True)
    for node in client_nodes:
        install_krb_client_packages(node, nfs_server=False)

    kdc_setup.setup_kdc()
    kdc_ip = kdc_node.ip_address
    write_krb5_conf(all_nodes, realm, domain, kdc_hostname, kdc_ip=kdc_ip)

    hostmap = {
        kdc_ip: [kdc_hostname, "kdc"],
    }
    for nfs_node in nfs_nodes:
        fqdn = node_fqdn(nfs_node, domain)
        hostmap[nfs_node.ip_address] = [fqdn, nfs_node.hostname]
    for client_node in client_nodes:
        client_fqdn = node_fqdn(client_node, domain)
        hostmap[client_node.ip_address] = [client_fqdn, client_node.hostname]
    merge_hosts_entries(all_nodes, hostmap)

    kdc_setup.add_user_principal()

    for client_node in client_nodes:
        client_fqdn = node_fqdn(client_node, domain)
        install_client_host_keytab(kdc_setup, client_node, client_fqdn)
        open_nfs_client_firewall(client_node)

    for nfs_node in nfs_nodes:
        fqdn = node_fqdn(nfs_node, domain)
        verify_kdc_kinit_from_node(nfs_node, kdc_setup)
        spn = kdc_setup.add_nfs_service_principal(fqdn)
        safe_hostname = re.sub(r"[^a-zA-Z0-9._-]", "_", nfs_node.hostname)
        remote_kt = "/tmp/cephci-nfs-{}.keytab".format(safe_hostname.replace(".", "-"))
        kdc_setup.export_keytab_for_principal(spn, remote_path=remote_kt)
        keytab_bytes = kdc_setup.fetch_keytab_bytes(remote_path=remote_kt)
        install_nfs_keytab(nfs_node, keytab_bytes)
        kinit_service_keytab(
            nfs_node,
            fqdn,
            realm,
            kdc_ip=kdc_ip,
            kdc_node=kdc_node,
        )
        ensure_nfs_krb_client_services(nfs_node)

    return realm
