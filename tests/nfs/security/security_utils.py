import json
import shlex
import time

from ceph.ceph import CommandFailed
from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.packages import Package
from tests.nfs.nfs_operations import create_nfs_via_file_and_verify, nfs_log_parser
from utility.log import Log
from utility.utils import generate_self_signed_certificate

log = Log(__name__)

# Default client mount points TLS tests may leave mounted (for teardown helpers).
TLS_TEST_MOUNT_CANDIDATES = [
    "/mnt/plain_tc2",
    "/mnt/tls_tc2",
    "/mnt/tls_tc1_0",
    "/mnt/plain_tc4_0",
    "/mnt/tls_tc4_0",
]


def _nfs_ls_to_cluster_names(clusters):
    """Normalize ``ceph nfs cluster ls`` JSON to a list of cluster/service id strings."""
    if not clusters:
        return []
    names = []
    for c in clusters:
        if isinstance(c, str):
            names.append(c)
        elif isinstance(c, dict):
            for key in ("name", "service_id", "nfs_cluster"):
                if c.get(key):
                    names.append(str(c[key]))
                    break
    return names


def _nfs_cluster_names(client_node):
    try:
        clusters = Ceph(client_node).nfs.cluster.ls()
    except Exception as ex:
        log.debug("nfs cluster ls failed: %s", ex)
        return None
    return _nfs_ls_to_cluster_names(clusters)


def _orch_nfs_daemon_count(client_node, nfs_name):
    """Return count of cephadm NFS daemons for ``nfs.<nfs_name>``, or -1 if query failed."""
    try:
        raw, _ = client_node.exec_command(
            sudo=True,
            cmd=f"ceph orch ps --service_name nfs.{nfs_name} --format json",
            timeout=60,
            check_ec=False,
        )
        if raw is None:
            text = ""
        elif isinstance(raw, str):
            text = raw.strip()
        else:
            text = raw.read().decode().strip()
        if not text:
            return 0
        data = json.loads(text)
        if isinstance(data, list):
            return len(data)
        return 0
    except Exception as ex:
        log.debug("orch ps nfs.%s: %s", nfs_name, ex)
        return -1


def wait_until_nfs_cluster_teardown(client_node, nfs_name, timeout=300, interval=5):
    """
    Wait until the NFS cluster is gone from ``ceph nfs cluster ls`` and no daemons remain
    in ``ceph orch ps`` for ``nfs.<nfs_name>`` (replaces a fixed sleep after cluster delete).
    """
    for w in WaitUntil(timeout=timeout, interval=interval):
        names = _nfs_cluster_names(client_node)
        if names is None:
            continue
        n_daemons = _orch_nfs_daemon_count(client_node, nfs_name)
        if nfs_name not in names and n_daemons == 0:
            log.info(
                "NFS cluster %r teardown complete (mgr + orch) after ~%s s",
                nfs_name,
                w._attempt * w.interval,
            )
            return True
        if nfs_name not in names and n_daemons < 0:
            log.info(
                "NFS cluster %r absent from mgr ls; orch ps unavailable after ~%s s",
                nfs_name,
                w._attempt * w.interval,
            )
            return True
        log.debug(
            "Waiting for nfs.%s teardown (in_ls=%s, daemon_count=%s)",
            nfs_name,
            nfs_name in names,
            n_daemons,
        )
    log.warning(
        "Timed out after %ss waiting for NFS cluster %r to drain; continuing cleanup.",
        timeout,
        nfs_name,
    )
    return False


def wait_until_nfs_export_visible(
    client_node, nfs_name, export_path, timeout=180, interval=3
):
    """
    Poll until ``export_path`` appears in ``ceph nfs export ls <nfs_name>``.
    """
    for w in WaitUntil(timeout=timeout, interval=interval):
        try:
            raw = Ceph(client_node).nfs.export.ls(nfs_name)
            if not raw or not isinstance(raw, str):
                continue
            try:
                exports = json.loads(raw)
            except json.JSONDecodeError:
                if export_path in raw:
                    log.info(
                        "Export %r visible for %r after ~%s s (non-JSON ls)",
                        export_path,
                        nfs_name,
                        w._attempt * w.interval,
                    )
                    return True
                continue
            if isinstance(exports, list):
                if export_path in exports:
                    log.info(
                        "Export %r listed for %r after ~%s s",
                        export_path,
                        nfs_name,
                        w._attempt * w.interval,
                    )
                    return True
                for e in exports:
                    if e == export_path or export_path in str(e):
                        log.info(
                            "Export %r listed for %r after ~%s s",
                            export_path,
                            nfs_name,
                            w._attempt * w.interval,
                        )
                        return True
        except Exception as ex:
            log.debug("export ls wait: %s", ex)
    raise OperationFailedError(
        f"Timed out after {timeout}s waiting for export {export_path!r} on {nfs_name!r}"
    )


def wait_for_tls_strings_in_nfs_logs(
    client,
    nfs_node,
    nfs_cluster_service_id,
    expect_list,
    timeout=120,
    interval=3,
):
    """
    Poll Ganesha logs until all ``expect_list`` substrings are found (same check as
    ``verify_tls_strings_in_nfs_logs``), instead of sleeping before a single check.
    """
    if not expect_list:
        return True
    for w in WaitUntil(timeout=timeout, interval=interval):
        if verify_tls_strings_in_nfs_logs(
            client,
            nfs_node,
            nfs_cluster_service_id,
            expect_list,
            expect_quiet=True,
        ):
            log.info(
                "TLS log substrings matched after ~%s s",
                w._attempt * w.interval,
            )
            return True
    return False


def wait_until_ceph_orch_nfs_ps_ready(
    client_node, nfs_name_substring, timeout=300, interval=5
):
    """
    Poll ``ceph orch ps | grep <substring>`` until NFS service lines appear (e.g. after redeploy).

    Uses the same success criteria as ``verify_ceph_orch_nfs_running`` without logging an error
    on every poll.
    """
    for w in WaitUntil(timeout=timeout, interval=interval):
        out, _ = client_node.exec_command(
            sudo=True,
            cmd=f"ceph orch ps | grep {nfs_name_substring}",
            check_ec=False,
        )
        if out and "nfs" in out.lower():
            log.info(
                "ceph orch ps shows NFS (%r) after ~%s s",
                nfs_name_substring,
                w._attempt * w.interval,
            )
            log.info("NFS orch ps (filtered):\n%s", out)
            return True
    log.warning(
        "Timed out after %ss waiting for NFS in `ceph orch ps | grep %s`",
        timeout,
        nfs_name_substring,
    )
    return False


def tls_config_get(config, *keys, default=None):
    """
    Return the first present value for any of ``keys`` in dict-like ``config``.
    Used for suite YAML with preferred names plus legacy tc_* aliases.
    """
    for k in keys:
        if k in config and config[k] is not None:
            return config[k]
    return default


def setup_tls_client(client_node, ca_cert):
    """Install ktls-utils, drop CA trust for tlshd, and restart tlshd on the client."""
    log.info("Installing ktls-utils on client node %s", client_node.hostname)
    Package(client_node).install("ktls-utils")

    log.info("Configuring tlshd on client node %s", client_node.hostname)
    cert_dir = "/etc/pki/ca-trust/source/anchors"
    cert_path = f"{cert_dir}/tls_ca_cert.pem"
    client_node.exec_command(sudo=True, cmd=f"mkdir -p {cert_dir}")

    try:
        cert_file = client_node.remote_file(
            sudo=True, file_name=cert_path, file_mode="w"
        )
        cert_file.write(ca_cert)
        cert_file.flush()
    except AttributeError:
        cert_file = client_node.remote_file(
            sudo=True, file_name=cert_path, file_mode="wb"
        )
        cert_file.write(ca_cert.encode("utf-8"))
        cert_file.flush()

    client_node.exec_command(
        sudo=True,
        cmd="sed -i '/\\[authenticate.client\\]/d' /etc/tlshd.conf",
        check_ec=False,
    )
    client_node.exec_command(
        sudo=True, cmd="sed -i '/x509.truststore/d' /etc/tlshd.conf", check_ec=False
    )
    # keep strict formatting for tlshd parser: no leading spaces around '='
    tlshd_conf_adds = f"\n[authenticate.client]\nx509.truststore={cert_path}\n"
    client_node.exec_command(
        sudo=True, cmd=f"echo '{tlshd_conf_adds}' >> /etc/tlshd.conf"
    )
    client_node.exec_command(
        sudo=True, cmd=f"restorecon -Rv {cert_dir}", check_ec=False
    )

    log.info("Restarting tlshd on %s", client_node.hostname)
    client_node.exec_command(sudo=True, cmd="systemctl enable --now tlshd")
    client_node.exec_command(sudo=True, cmd="systemctl restart tlshd")


def check_mount_fails(client_node, mount_cmd):
    """
    Run a mount command that is expected to fail (e.g. non-TLS mount to TLS-only export).

    Returns True if the command failed as expected, False if it succeeded.
    """
    log.info("Attempting intentionally failing mount: %s", mount_cmd)
    try:
        client_node.exec_command(sudo=True, cmd=mount_cmd)
        log.error("Mount succeeded but was expected to fail!")
        return False
    except CommandFailed:
        log.info("Mount failed as expected.")
        return True


def full_tls_stack_cleanup(
    client_node,
    nfs_name,
    mount_candidates=None,
    fs_name="cephfs",
    subvolume_group="ganeshagroup",
):
    """
    Umount known TLS/plain mounts, delete all exports, delete NFS cluster,
    remove CephFS subvolumes in the Ganesha group (admin/sudo).

    Args:
        client_node: Node with ceph admin.
        nfs_name: NFS cluster service_id.
        mount_candidates: Paths to lazy-umount and remove; defaults to TLS_TEST_MOUNT_CANDIDATES.
        fs_name: CephFS name for subvolume cleanup.
        subvolume_group: FS subvolume group used by NFS exports.
    """
    paths = (
        mount_candidates if mount_candidates is not None else TLS_TEST_MOUNT_CANDIDATES
    )
    for m in paths:
        client_node.exec_command(sudo=True, cmd=f"umount -l {m}", check_ec=False)
        client_node.exec_command(sudo=True, cmd=f"rm -rf {m}", check_ec=False)

    try:
        raw = Ceph(client_node).nfs.export.ls(nfs_name)
        exports = json.loads(raw) if raw else []
    except (json.JSONDecodeError, TypeError, Exception) as ex:
        log.warning("Could not list exports for %s: %s", nfs_name, ex)
        exports = []

    for export in exports:
        try:
            Ceph(client_node).nfs.export.delete(nfs_name, export)
            log.info("Deleted export %s", export)
        except Exception as ex:
            log.warning("Export delete %s: %s", export, ex)

    try:
        Ceph(client_node).nfs.cluster.delete(nfs_name)
        log.info("Deleted NFS cluster %s", nfs_name)
    except Exception as ex:
        log.warning("NFS cluster delete %s: %s", nfs_name, ex)

    wait_until_nfs_cluster_teardown(client_node, nfs_name)

    ceph_cli = Ceph(client_node)
    try:
        raw = ceph_cli.fs.sub_volume.ls(fs_name, group_name=subvolume_group)
        items = json.loads(raw) if raw else []
        for item in items:
            subvol = item["name"]
            ceph_cli.fs.sub_volume.rm(fs_name, subvol, group_name=subvolume_group)
    except Exception as ex:
        log.warning("Subvolume cleanup: %s", ex)

    ceph_cli.fs.sub_volume_group.rm(fs_name, subvolume_group, force=True)


def verify_ceph_orch_nfs_running(client, nfs_name_substring="nfs"):
    """
    Verify at least one NFS orch daemon is reported (e.g. after TLS deploy).
    Returns (True, stdout) on success.
    """
    out, _ = client.exec_command(
        sudo=True, cmd=f"ceph orch ps | grep {nfs_name_substring}", check_ec=False
    )
    if not out or "nfs" not in out.lower():
        log.error(
            "No NFS service lines in `ceph orch ps | grep %s`", nfs_name_substring
        )
        return False, out or ""
    log.info("NFS orch ps (filtered):\n%s", out)
    return True, out


def verify_tls_strings_in_nfs_logs(
    client, nfs_node, nfs_cluster_service_id, expect_list, expect_quiet=False
):
    """
    Wrapper around nfs_operations.nfs_log_parser for TLS-related log assertions.

    Args:
        client: Any node with ceph admin (typically installer or client).
        nfs_node: Host where the nfs container runs.
        nfs_cluster_service_id: Ceph NFS cluster name (service_id), e.g. cephfs-nfs-tls.
        expect_list: Substrings that must appear in cephadm nfs daemon logs.
        expect_quiet: If True, omit summary error log (for polling waiters).

    Returns:
        True if all strings found, False otherwise.
    """
    if not expect_list:
        return True
    rc = nfs_log_parser(
        client,
        nfs_node,
        nfs_cluster_service_id,
        expect_list=expect_list,
        expect_quiet=expect_quiet,
    )
    if rc != 0:
        if not expect_quiet:
            log.error(
                "TLS log verification failed for cluster %s; expected substrings: %s",
                nfs_cluster_service_id,
                expect_list,
            )
        return False
    return True


def probe_tls_handshake_with_openssl(
    client_node, nfs_host, port=2049, tls_version_flag="-tls1_3"
):
    """
    Optional probe: openssl s_client against NFS port (may not always negotiate like HTTPS).

    Returns (success: bool, output_snippet: str).
    """
    cmd = (
        f"bash -c 'echo | timeout 15 openssl s_client -connect {nfs_host}:{port} "
        f"{tls_version_flag} 2>&1 | head -40'"
    )
    try:
        out, _ = client_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        log.info("openssl s_client probe output (truncated):\n%s", out[:2000])
        ok = bool(
            out and ("TLSv1.3" in out or "CONNECTED" in out or "SSL-Session" in out)
        )
        return ok, out or ""
    except Exception as ex:
        log.warning("openssl probe skipped/failed: %s", ex)
        return False, str(ex)


def _quote_shell_path(path):
    """Return ``path`` safely escaped for use in remote shell commands."""
    return shlex.quote(str(path))


def _validate_max_packets(max_packets):
    if not isinstance(max_packets, int) or max_packets < 0:
        raise OperationFailedError(
            f"max_packets must be a non-negative integer, got {max_packets!r}"
        )


def _wait_for_pcap_closed(client_node, pcap_path, max_wait=5):
    """Wait until no tcpdump process holds ``pcap_path`` open."""
    quoted = _quote_shell_path(pcap_path)
    for _ in range(max_wait):
        out, _ = client_node.exec_command(
            sudo=True,
            cmd=f"lsof {quoted} 2>/dev/null",
            check_ec=False,
        )
        if not out or "tcpdump" not in str(out):
            return
        time.sleep(1)
    log.warning("pcap file %s still open after %ss", pcap_path, max_wait)


def normalize_fio_buffer_pattern(pattern):
    """
    Return a fio-compatible ``--buffer_pattern`` value.

    fio 3.x requires a hex literal (e.g. ``0xCEFACEFE``); plain ASCII strings
    fail with "failed parsing buffer_pattern".
    """
    raw = str(pattern).strip()
    if raw.lower().startswith("0x"):
        hex_body = raw[2:]
    else:
        hex_body = raw
    if not hex_body or len(hex_body) % 2:
        raise OperationFailedError(
            f"fio buffer_pattern must be even-length hex (e.g. 0xCEFACEFE), got {pattern!r}"
        )
    try:
        int(hex_body, 16)
    except ValueError as err:
        raise OperationFailedError(
            f"Invalid fio buffer_pattern hex {pattern!r}: {err}"
        ) from err
    return f"0x{hex_body.lower()}"


def fio_pattern_to_pcap_hex_substring(pattern):
    """Map fio ``--buffer_pattern`` bytes to a spaced hex substring for tcpdump -xx."""
    normalized = normalize_fio_buffer_pattern(pattern)
    byte_pairs = [normalized[i : i + 2] for i in range(2, len(normalized), 2)]
    return " ".join(byte_pairs)


def fio_pattern_to_binary_grep_literal(pattern):
    """Return a bash ``$'\\x..\\x..'` literal for ``grep -a -F`` on a pcap file."""
    normalized = normalize_fio_buffer_pattern(pattern)
    byte_pairs = [normalized[i : i + 2] for i in range(2, len(normalized), 2)]
    return "".join(f"\\x{b}" for b in byte_pairs)


def _pcap_count_tls_records(client_node, pcap_path, tls_regex, max_packets, timeout):
    """Count TLS record headers in the first ``max_packets`` of a pcap via on-node grep."""
    _validate_max_packets(max_packets)
    quoted_pcap = _quote_shell_path(pcap_path)
    cmd = (
        f"timeout {timeout} tcpdump -r {quoted_pcap} -xx -n -c {max_packets} "
        f"2>/dev/null | grep -cE '{tls_regex}' || true"
    )
    out, _ = client_node.exec_command(
        sudo=True,
        cmd=cmd,
        check_ec=False,
        timeout=timeout + 5,
    )
    try:
        return int(str(out).strip() or "0")
    except ValueError:
        return 0


def _pcap_count_binary_byte_pattern(client_node, pcap_path, bash_literal, timeout):
    """Count occurrences of a raw byte pattern anywhere in a pcap via ``grep -a -o``."""
    quoted_pcap = _quote_shell_path(pcap_path)
    out, _ = client_node.exec_command(
        sudo=True,
        cmd=(
            f"timeout {timeout} grep -a -o $'{bash_literal}' {quoted_pcap} "
            f"2>/dev/null | wc -l"
        ),
        check_ec=False,
        timeout=timeout + 5,
    )
    try:
        return int(str(out).strip() or "0")
    except ValueError:
        return 0


def ensure_package_installed(client_node, package_name, manager="yum"):
    """Install ``package_name`` when ``rpm -q`` reports it is missing."""
    out, _ = client_node.exec_command(
        sudo=True,
        cmd=f"rpm -q {package_name}",
        check_ec=False,
    )
    if out and "is not installed" not in out.lower():
        log.info("%s already installed on %s", package_name, client_node.hostname)
        return
    log.info("Installing %s on %s", package_name, client_node.hostname)
    Package(client_node, manager=manager).install(package_name)


def start_tcpdump_capture(client_node, server_host, port, pcap_path, snaplen=512):
    """
    Start ``tcpdump`` in the background on ``client_node`` for NFS TLS traffic.

    Returns the background shell pid printed by ``nohup ... & echo $!``.
    """
    pid = None
    quoted_pcap = _quote_shell_path(pcap_path)
    try:
        ensure_package_installed(client_node, "tcpdump")
        client_node.exec_command(
            sudo=True, cmd="mkdir -p /tmp/tls_pcap", check_ec=False
        )
        client_node.exec_command(
            sudo=True,
            cmd=f"pkill -f 'tcpdump.*{quoted_pcap}'",
            check_ec=False,
        )
        bpf = f"host {server_host} and tcp port {port}"
        log_name = pcap_path.rsplit("/", 1)[-1]
        cmd = (
            f"nohup tcpdump -i any -s {snaplen} -w {quoted_pcap} '{bpf}' "
            f">/tmp/tcpdump_{log_name}.log 2>&1 & echo $!"
        )
        out, _ = client_node.exec_command(sudo=True, cmd=cmd, timeout=30)
        pid = (out or "").strip().splitlines()[-1].strip()
        if not pid or not pid.isdigit():
            raise OperationFailedError(f"Failed to start tcpdump: invalid pid {pid!r}")
        time.sleep(2)
        log.info(
            "tcpdump capture started on %s (pid=%s, bpf=%r, pcap=%s, snaplen=%s)",
            client_node.hostname,
            pid,
            bpf,
            pcap_path,
            snaplen,
        )
        return pid
    except Exception:
        if pid and pid.isdigit():
            stop_tcpdump_capture(client_node, pcap_path, pid=pid)
        else:
            client_node.exec_command(
                sudo=True,
                cmd=f"pkill -f 'tcpdump.*{quoted_pcap}'",
                check_ec=False,
            )
        raise


def stop_tcpdump_capture(client_node, pcap_path, pid=None):
    """Stop a background ``tcpdump`` and wait briefly for the pcap to flush."""
    quoted_pcap = _quote_shell_path(pcap_path)
    if pid:
        log.info("Stopping tcpdump pid=%s", pid)
        client_node.exec_command(
            sudo=True,
            cmd=f"kill -TERM {pid}",
            check_ec=False,
        )
        time.sleep(1)
        out, _ = client_node.exec_command(
            sudo=True,
            cmd=f"ps -p {pid} -o comm=",
            check_ec=False,
        )
        if out and "tcpdump" in str(out):
            log.warning("tcpdump %s did not stop with TERM, using KILL", pid)
            client_node.exec_command(
                sudo=True,
                cmd=f"kill -KILL {pid}",
                check_ec=False,
            )
    client_node.exec_command(
        sudo=True,
        cmd=f"pkill -9 -f 'tcpdump.*{quoted_pcap}'",
        check_ec=False,
    )
    _wait_for_pcap_closed(client_node, pcap_path)
    time.sleep(1)
    out, _ = client_node.exec_command(
        sudo=True,
        cmd=f"test -s {quoted_pcap} && echo exists || echo missing",
        check_ec=False,
    )
    if "missing" in str(out or ""):
        log.warning("pcap file %s is missing or empty after capture", pcap_path)


def analyze_pcap_traffic(
    client_node,
    pcap_path,
    cleartext_marker,
    min_total_packets=10,
    min_tls_app_records=5,
    max_packets_to_analyze=10000,
    pcap_analyze_timeout=300,
):
    """
    Analyze a pcap for TLS record headers and an optional fio cleartext marker.

    Returns a dict with packet_count, tls/binary TLS hit counts, cleartext_marker_hits,
    cleartext_marker_found, and tls_record_signal.
    """
    quoted_pcap = _quote_shell_path(pcap_path)
    _wait_for_pcap_closed(client_node, pcap_path)
    size_out, _ = client_node.exec_command(
        sudo=True,
        cmd=f"test -s {quoted_pcap} && wc -c < {quoted_pcap}",
        check_ec=False,
    )
    if not size_out or not str(size_out).strip().isdigit():
        raise OperationFailedError(f"Capture file missing or empty: {pcap_path}")

    pcap_bytes = int(str(size_out).strip())
    log.info("pcap %s size=%s bytes", pcap_path, pcap_bytes)

    pkt_out, _ = client_node.exec_command(
        sudo=True,
        cmd=(
            f"timeout {pcap_analyze_timeout} tcpdump -r {quoted_pcap} -n "
            f"2>/dev/null | wc -l"
        ),
        check_ec=False,
        timeout=pcap_analyze_timeout + 5,
    )
    packet_count = int(str(pkt_out).strip().splitlines()[-1].strip() or "0")
    if packet_count < min_total_packets:
        raise OperationFailedError(
            f"Expected at least {min_total_packets} packets in {pcap_path}, "
            f"got {packet_count}"
        )

    tls_handshake_hits = _pcap_count_tls_records(
        client_node,
        pcap_path,
        r" 16 03 0[34] ",
        max_packets_to_analyze,
        pcap_analyze_timeout,
    )
    tls_app_data_hits = _pcap_count_tls_records(
        client_node,
        pcap_path,
        r" 17 03 0[34] ",
        max_packets_to_analyze,
        pcap_analyze_timeout,
    )
    binary_tls_handshake_hits = _pcap_count_binary_byte_pattern(
        client_node, pcap_path, r"\x16\x03", pcap_analyze_timeout
    )
    binary_tls_app_hits = _pcap_count_binary_byte_pattern(
        client_node, pcap_path, r"\x17\x03", pcap_analyze_timeout
    )

    cleartext_marker_hits = 0
    marker_hex_substring = ""
    if cleartext_marker:
        marker_hex_substring = fio_pattern_to_pcap_hex_substring(cleartext_marker)
        grep_literal = fio_pattern_to_binary_grep_literal(cleartext_marker)
        leak_out, _ = client_node.exec_command(
            sudo=True,
            cmd=(
                f"timeout {pcap_analyze_timeout} grep -a -F $'{grep_literal}' "
                f"{quoted_pcap} 2>/dev/null | wc -l"
            ),
            check_ec=False,
            timeout=pcap_analyze_timeout + 5,
        )
        cleartext_marker_hits = int(str(leak_out).strip() or "0")

    cleartext_marker_found = cleartext_marker_hits > 0
    has_tls_record_signal = (
        tls_handshake_hits >= 1
        or tls_app_data_hits >= min_tls_app_records
        or binary_tls_handshake_hits >= 1
        or binary_tls_app_hits >= min_tls_app_records
    )

    log.info(
        "pcap analysis for %s: packets=%s analyzed_packets<=%s "
        "handshake_tls_records=%s app_data_tls_records=%s "
        "binary_handshake_hits=%s binary_app_hits=%s "
        "cleartext_marker_hex=%r cleartext_marker_hits=%s "
        "cleartext_marker_found=%s tls_record_signal=%s",
        pcap_path,
        packet_count,
        max_packets_to_analyze,
        tls_handshake_hits,
        tls_app_data_hits,
        binary_tls_handshake_hits,
        binary_tls_app_hits,
        marker_hex_substring,
        cleartext_marker_hits,
        cleartext_marker_found,
        has_tls_record_signal,
    )

    return {
        "pcap_bytes": pcap_bytes,
        "packet_count": packet_count,
        "tls_handshake_hits": tls_handshake_hits,
        "tls_app_data_hits": tls_app_data_hits,
        "binary_tls_handshake_hits": binary_tls_handshake_hits,
        "binary_tls_app_hits": binary_tls_app_hits,
        "cleartext_marker_hits": cleartext_marker_hits,
        "cleartext_marker_found": cleartext_marker_found,
        "tls_record_signal": has_tls_record_signal,
    }


def verify_cleartext_traffic_in_pcap(
    client_node,
    pcap_path,
    cleartext_marker,
    min_total_packets=10,
    min_cleartext_marker_hits=1,
    max_packets_to_analyze=10000,
    pcap_analyze_timeout=300,
):
    """
    Verify a plain (non-TLS) NFS capture exposes the fio buffer pattern on the wire.

    Used as a control baseline before the TLS capture phase: sustained fio I/O with
    ``--buffer_pattern`` must leave that byte sequence visible in the pcap when NFS is
    not encrypted.
    """
    analysis = analyze_pcap_traffic(
        client_node,
        pcap_path,
        cleartext_marker,
        min_total_packets=min_total_packets,
        max_packets_to_analyze=max_packets_to_analyze,
        pcap_analyze_timeout=pcap_analyze_timeout,
    )
    if analysis["cleartext_marker_hits"] < min_cleartext_marker_hits:
        raise OperationFailedError(
            f"Plain NFS control capture did not show fio cleartext marker "
            f"{cleartext_marker!r} on the wire (hits={analysis['cleartext_marker_hits']}, "
            f"expected>={min_cleartext_marker_hits}); cannot confirm unencrypted baseline."
        )
    if analysis["tls_record_signal"]:
        log.warning(
            "TLS record headers present in plain NFS capture (unexpected); "
            "cleartext marker was still observed."
        )
    log.info(
        "Plain NFS control confirmed: fio pattern visible in cleartext "
        "(marker_hits=%s, packets=%s)",
        analysis["cleartext_marker_hits"],
        analysis["packet_count"],
    )
    return analysis


def verify_tls_encrypted_traffic_in_pcap(
    client_node,
    pcap_path,
    cleartext_marker,
    min_total_packets=10,
    min_tls_app_records=5,
    max_packets_to_analyze=10000,
    pcap_analyze_timeout=300,
    max_cleartext_marker_hits=50,
    plain_cleartext_marker_hits=None,
    max_cleartext_leak_ratio=0.0001,
):
    """
    Verify a pcap captured during NFS-over-TLS I/O shows encrypted wire traffic.

    Primary pass criterion: the fio ``--buffer_pattern`` must not appear at the
    same rate as the plain-NFS control capture. A small number of grep hits can
    occur by chance in large pcaps (encrypted ciphertext may contain the byte
    sequence), so when ``plain_cleartext_marker_hits`` is provided the allowed
    TLS hit count scales with that baseline.

    TLS record headers (``\\x16\\x03`` handshake, ``\\x17\\x03`` application data) are
    counted for positive signal. With kernel TLS (kTLS), post-handshake traffic may not
    expose TLS record framing on the wire, so missing headers does not fail the test
    when cleartext leakage is below threshold and enough packets were captured.
    """
    analysis = analyze_pcap_traffic(
        client_node,
        pcap_path,
        cleartext_marker,
        min_total_packets=min_total_packets,
        min_tls_app_records=min_tls_app_records,
        max_packets_to_analyze=max_packets_to_analyze,
        pcap_analyze_timeout=pcap_analyze_timeout,
    )

    tls_hits = analysis["cleartext_marker_hits"]
    allowed_hits = max_cleartext_marker_hits
    if plain_cleartext_marker_hits is not None and plain_cleartext_marker_hits > 0:
        ratio_cap = int(plain_cleartext_marker_hits * max_cleartext_leak_ratio)
        allowed_hits = max(allowed_hits, ratio_cap)

    cleartext_leak = tls_hits > allowed_hits
    analysis["cleartext_marker_found"] = cleartext_leak
    analysis["allowed_cleartext_marker_hits"] = allowed_hits

    if cleartext_leak:
        raise OperationFailedError(
            f"Cleartext marker {cleartext_marker!r} visible in TLS capture "
            f"(hits={tls_hits}, allowed<={allowed_hits}, "
            f"plain_baseline={plain_cleartext_marker_hits}); "
            "NFS payload was not encrypted on the wire."
        )

    if tls_hits > 0:
        log.warning(
            "TLS capture had %s cleartext marker hit(s) within allowed threshold "
            "(allowed<=%s, plain_baseline=%s); treating as grep false-positive",
            tls_hits,
            allowed_hits,
            plain_cleartext_marker_hits,
        )

    if not analysis["tls_record_signal"]:
        log.warning(
            "No TLS record headers in capture (kTLS may omit TLS framing on wire); "
            "encryption verified via low fio cleartext marker hits (%s) during %s packets",
            tls_hits,
            analysis["packet_count"],
        )

    return analysis


def setup_plain_nfs_cluster(installer_node, nfs_node, nfs_name):
    """Deploy a non-TLS NFS-Ganesha cluster (no ssl / xprtsec on the service)."""
    nfs_spec = {
        "service_type": "nfs",
        "service_id": nfs_name,
        "placement": {"hosts": [nfs_node.hostname]},
        "spec": {},
    }
    log.info("Deploying plain (non-TLS) NFS cluster %s via spec file", nfs_name)
    if not create_nfs_via_file_and_verify(installer_node, [nfs_spec], timeout=300):
        raise OperationFailedError(f"Failed to create plain NFS cluster {nfs_name}")
    log.info("Successfully deployed plain NFS cluster %s", nfs_name)


def setup_tls_nfs_cluster(
    installer_node,
    nfs_node,
    nfs_name,
    tls_min_version="TLSv1.3",
    tls_ciphers="ALL",
    tls_ktls=True,
    tls_debug=True,
):
    """
    Creates an NFS-Ganesha cluster with TLS enabled by generating a self-signed
    certificate and applying the custom config via Ceph orchestrator spec.

    Args:
        installer: The installer node.
        nfs_node: The NFS node where the service will be placed.
        nfs_name: Name of the NFS cluster.
        tls_min_version: Minimum TLS version.
        tls_ciphers: TLS ciphers string.
        tls_ktls: Boolean flag for ktls.
        tls_debug: Boolean flag for tls debug.
    """
    log.info(f"Generating self-signed certificate for {nfs_node.hostname}")
    subject = {
        "common_name": nfs_node.hostname,
        "ip_address": nfs_node.ip_address,
    }
    cert_key, cert, ca_cert = generate_self_signed_certificate(subject)

    # Note: `generate_self_signed_certificate` returns cephqe-signed cert + CA PEM when
    # `get_cephqe_ca()` can download from root-ca-location; otherwise ca_cert is None.
    # Orchestrator rejects nfs specs with ssl:true but no ssl_ca_cert (EINVAL: CA required).
    if not ca_cert:
        ca_cert = cert
        log.warning(
            "No cephqe CA available (offline DNS/network to root-ca-location); "
            "using self-signed server cert PEM as ssl_ca_cert for NFS orch apply."
        )

    # Construct the TLS cluster spec
    nfs_spec = {
        "service_type": "nfs",
        "service_id": nfs_name,
        "placement": {"hosts": [nfs_node.hostname]},
        "spec": {
            "certificate_source": "inline",
            "ssl": True,
            "ssl_cert": cert.rstrip("\n"),
            "ssl_key": cert_key.rstrip("\n"),
            "ssl_ca_cert": ca_cert.rstrip("\n"),
            "tls_ktls": tls_ktls,
            "tls_debug": tls_debug,
            "tls_min_version": tls_min_version,
            "tls_ciphers": tls_ciphers,
        },
    }

    log.info(f"Deploying TLS-enabled NFS cluster {nfs_name} via spec file")
    if not create_nfs_via_file_and_verify(installer_node, [nfs_spec], timeout=300):
        raise OperationFailedError(f"Failed to create TLS NFS cluster {nfs_name}")

    log.info(f"Successfully deployed TLS NFS cluster {nfs_name}")
    return ca_cert or cert
