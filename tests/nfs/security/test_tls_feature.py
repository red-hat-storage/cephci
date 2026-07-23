import json
import re
import traceback
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, MountFailedError, Unmount
from tests.nfs.nfs_operations import (
    _get_client_specific_mount_versions,
    exports_mounts_perclient,
    mount_retry,
)
from tests.nfs.security.security_utils import (
    assert_mount_failed,
    assert_tlshd_handshake_failure,
    attempt_nfs_mount_expect_failure,
    build_tls_nfs_mount_cmd,
    capture_tlshd_status,
    check_mount_fails,
    corrupt_pem_material,
    deploy_tls_nfs_cluster_with_certs,
    ensure_package_installed,
    full_tls_stack_cleanup,
    generate_mismatched_tls_trust_cert,
    generate_tls_cert_bundle,
    mount_export_via_stunnel,
    normalize_fio_buffer_pattern,
    probe_tls_handshake_with_openssl,
    setup_plain_nfs_cluster,
    setup_stunnel_client,
    setup_tls_client,
    setup_tls_nfs_cluster,
    start_tcpdump_capture,
    stop_stunnel_client,
    stop_tcpdump_capture,
    tls_config_get,
    verify_ceph_orch_nfs_running,
    verify_cleartext_traffic_in_pcap,
    verify_tls_encrypted_traffic_in_pcap,
    verify_tls_strings_in_nfs_logs,
    wait_for_tls_strings_in_nfs_logs,
    wait_until_ceph_orch_nfs_ps_ready,
    wait_until_nfs_export_visible,
    write_client_tls_ca_cert,
)
from tests.nfs.test_nfs_io_operations_during_upgrade import (
    create_export_and_mount_for_existing_nfs_cluster,
)
from tests.nfs.test_nfs_multiple_operations_for_upgrade import (
    create_file,
    delete_file,
    lookup_in_directory,
    read_from_file_using_dd_command,
    rename_file,
    write_to_file_using_dd_command,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_NFS_TLS_CLUSTER = "cephfs-nfs-tls"
DEFAULT_NFS_PLAIN_CLUSTER = "cephfs-nfs-plain"

# Single-operation names and the combined workflow
OP_TLS_DEPLOY_MOUNT_VERIFY = "tls_deploy_mount_verify"
OP_TLS_EXPORT_ENFORCEMENT = "tls_export_enforcement"
OP_TLS_LOGS_OPENSSL_PROBE = "tls_logs_openssl_probe"
OP_TLS_IO_TCPDUMP_ENCRYPTION = "tls_io_tcpdump_encryption"
OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION = "tls_stunnel_io_tcpdump_encryption"
OP_TLS_NEGATIVE_INVALID_CERTS = "tls_negative_invalid_certs"
OP_TLS_FULL_WORKFLOW = "tls_full_workflow"
_TLS_FULL_SEQUENCE = [
    OP_TLS_DEPLOY_MOUNT_VERIFY,
    OP_TLS_EXPORT_ENFORCEMENT,
    OP_TLS_LOGS_OPENSSL_PROBE,
]
_TLS_STANDALONE_OPERATIONS = _TLS_FULL_SEQUENCE + [
    OP_TLS_IO_TCPDUMP_ENCRYPTION,
    OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION,
    OP_TLS_NEGATIVE_INVALID_CERTS,
]
# fio --buffer_pattern requires hex (e.g. 0xCEFACEFE), not ASCII strings.
DEFAULT_TLS_IO_MARKER = "0xCEFACEFE"
_FIO_SIZE_RE = re.compile(r"^\d+[kKmMgGtT]?$")


def _validate_fio_config(fio_size, fio_bs, fio_runtime):
    if not _FIO_SIZE_RE.match(str(fio_size)):
        raise OperationFailedError(
            f"Invalid fio_size format: {fio_size!r}. Expected format: 32M, 1G, etc."
        )
    if not _FIO_SIZE_RE.match(str(fio_bs)):
        raise OperationFailedError(
            f"Invalid fio_bs format: {fio_bs!r}. Expected format: 64k, 4M, etc."
        )
    if fio_runtime <= 0:
        raise OperationFailedError(f"fio_runtime must be positive, got {fio_runtime}")


def _fio_timeout_from_config(config, fio_runtime):
    fio_timeout_buffer = int(
        tls_config_get(
            config, "fio_timeout_buffer", "tc_04_fio_timeout_buffer", default=120
        )
    )
    fio_min_timeout = int(
        tls_config_get(config, "fio_min_timeout", "tc_04_fio_min_timeout", default=600)
    )
    # Each phase runs write then read; allow up to ~2x runtime plus buffer per command.
    fio_timeout = max((fio_runtime * 2) + fio_timeout_buffer, fio_min_timeout)
    log.info(
        "fio timeout set to %ss (2*runtime=%ss + buffer=%ss, min=%ss)",
        fio_timeout,
        fio_runtime * 2,
        fio_timeout_buffer,
        fio_min_timeout,
    )
    return fio_timeout


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    """
    Resolve config.operation into an ordered list of operation ids.
    Accepts tls_full_workflow / tls_all_in_one for the full sequence.
    """
    raw = config.get("operation")
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: "
            f"{OP_TLS_DEPLOY_MOUNT_VERIFY}, {OP_TLS_EXPORT_ENFORCEMENT}, "
            f"{OP_TLS_LOGS_OPENSSL_PROBE}, {OP_TLS_IO_TCPDUMP_ENCRYPTION}, "
            f"{OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION}, "
            f"{OP_TLS_NEGATIVE_INVALID_CERTS}, "
            f"{OP_TLS_FULL_WORKFLOW} (or tls_all_in_one)."
        )
    op = _normalize_operation(raw)
    if op in (OP_TLS_FULL_WORKFLOW, "tls_all_in_one", "tls_full"):
        return list(_TLS_FULL_SEQUENCE)
    if op in _TLS_STANDALONE_OPERATIONS:
        return [op]
    raise OperationFailedError(
        f"Unknown operation {raw!r}. Expected one of: {', '.join(_TLS_STANDALONE_OPERATIONS)} "
        f"or {OP_TLS_FULL_WORKFLOW}."
    )


def _parse_fio_json(fio_stdout, rw_type):
    """Return (bandwidth_mib_per_sec, iops) from fio JSON output."""
    raw = str(fio_stdout).strip()
    try:
        data = json.loads(raw)
        if not isinstance(data, dict) or "jobs" not in data:
            raise ValueError("Missing 'jobs' key in fio output")
        if not isinstance(data["jobs"], list) or not data["jobs"]:
            raise ValueError("'jobs' is not a non-empty list")
        rw_stats = data["jobs"][0].get(rw_type)
        if not rw_stats:
            raise ValueError(f"Missing '{rw_type}' in job stats")
        bw = rw_stats.get("bw")
        iops = rw_stats.get("iops")
        if bw is None or iops is None:
            raise ValueError(f"Missing 'bw' or 'iops' in {rw_type} stats")
        return float(bw) / 1024.0, float(iops)
    except (json.JSONDecodeError, KeyError, IndexError, ValueError, TypeError) as err:
        raise OperationFailedError(
            f"Could not parse fio {rw_type} JSON output: {err}\n"
            f"Raw (first 500 chars): {raw[:500]}"
        ) from err


def _run_fio_write_read_on_mount(client_node, mount_path, config):
    """Run sequential fio write/read on ``mount_path``; return combined stats dict."""
    ensure_package_installed(client_node, "fio")
    fio_size = tls_config_get(config, "fio_size", "tc_04_fio_size", default="32M")
    fio_bs = tls_config_get(config, "fio_bs", "tc_04_fio_bs", default="64k")
    fio_runtime = int(
        tls_config_get(config, "fio_runtime", "tc_04_fio_runtime", default=20)
    )
    _validate_fio_config(fio_size, fio_bs, fio_runtime)
    log.info(
        "fio config: size=%s, bs=%s, runtime=%ss",
        fio_size,
        fio_bs,
        fio_runtime,
    )
    marker = normalize_fio_buffer_pattern(
        tls_config_get(
            config,
            "fio_cleartext_marker",
            "tc_04_cleartext_marker",
            default=DEFAULT_TLS_IO_MARKER,
        )
    )
    fio_timeout = _fio_timeout_from_config(config, fio_runtime)
    base_args = (
        f"--filename={mount_path}/tls_fio_encrypt.dat "
        f"--bs={fio_bs} --size={fio_size} --runtime={fio_runtime} "
        f"--time_based --direct=1 --ioengine=libaio "
        f"--group_reporting --output-format=json "
        f"--buffer_pattern={marker}"
    )

    write_cmd = f"fio --name=tls_encrypt_verify --rw=write {base_args}"
    log.info("Running fio WRITE command: %s", write_cmd)
    write_out, write_err = client_node.exec_command(
        sudo=True,
        cmd=write_cmd,
        timeout=fio_timeout,
    )
    if write_err:
        log.warning("fio write stderr: %s", write_err)
    if not write_out:
        raise OperationFailedError(
            f"fio write produced no output. Command: {write_cmd}\nStderr: {write_err}"
        )
    try:
        write_mbps, write_iops = _parse_fio_json(write_out, "write")
    except Exception as err:
        log.error("Failed to parse fio write output. Command: %s", write_cmd)
        log.error("fio stdout (first 500 chars): %s", str(write_out)[:500])
        log.error("fio stderr: %s", write_err)
        raise OperationFailedError(f"fio write parsing failed: {err}") from err
    log.info(
        "fio write completed: %.1f MiB/s, %.0f IOPS",
        write_mbps,
        write_iops,
    )

    client_node.exec_command(
        sudo=True, cmd="echo 3 > /proc/sys/vm/drop_caches", check_ec=False
    )

    read_cmd = f"fio --name=tls_encrypt_verify --rw=read {base_args}"
    log.info("Running fio READ command: %s", read_cmd)
    read_out, read_err = client_node.exec_command(
        sudo=True,
        cmd=read_cmd,
        timeout=fio_timeout,
    )
    if read_err:
        log.warning("fio read stderr: %s", read_err)
    if not read_out:
        raise OperationFailedError(
            f"fio read produced no output. Command: {read_cmd}\nStderr: {read_err}"
        )
    try:
        read_mbps, read_iops = _parse_fio_json(read_out, "read")
    except Exception as err:
        log.error("Failed to parse fio read output. Command: %s", read_cmd)
        log.error("fio stdout (first 500 chars): %s", str(read_out)[:500])
        log.error("fio stderr: %s", read_err)
        raise OperationFailedError(f"fio read parsing failed: {err}") from err
    log.info(
        "fio read completed: %.1f MiB/s, %.0f IOPS",
        read_mbps,
        read_iops,
    )

    client_node.exec_command(
        sudo=True,
        cmd=f"rm -f {mount_path}/tls_fio_encrypt.dat",
        check_ec=False,
    )
    log.info(
        "fio on TLS mount succeeded: write=%.3f MiB/s (%.1f IOPS), "
        "read=%.3f MiB/s (%.1f IOPS)",
        write_mbps,
        write_iops,
        read_mbps,
        read_iops,
    )
    return {
        "write_mbps": write_mbps,
        "write_iops": write_iops,
        "read_mbps": read_mbps,
        "read_iops": read_iops,
        "cleartext_marker": marker,
    }


def op_tls_deploy_mount_verify(client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_deploy_mount_verify ===")
    fs_name = config.get("fs_name", "cephfs")
    export_name = tls_config_get(
        config, "tls_export_path", "tc_01_export", default="/tls_export1"
    )
    mount_name = tls_config_get(
        config, "tls_client_mount", "tc_01_mount", default="/mnt/tls_tc1"
    )
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))

    ok, _ = verify_ceph_orch_nfs_running(client_node, nfs_name)
    if not ok:
        raise OperationFailedError("NFS service not reported in ceph orch ps")

    expect_logs = tls_config_get(
        config,
        "tls_log_substrings",
        "tc_01_log_expect",
        default=["AUTH_TLS", "TLS"],
    )

    client_export_mount_dict = create_export_and_mount_for_existing_nfs_cluster(
        clients=[client_node],
        nfs_export=export_name,
        nfs_mount=mount_name,
        export_num=1,
        fs_name=fs_name,
        nfs_name=nfs_name,
        fs=fs_name,
        port=port,
        version=version,
        nfs_server=nfs_node.hostname,
        xprtsec="tls",
    )
    # exports_mounts_perclient uses "{nfs_mount}_{i}" e.g. /mnt/tls_tc1_0
    mount_path = client_export_mount_dict[client_node]["mount"][0]

    wait_timeout = int(
        tls_config_get(
            config,
            "post_mount_tls_log_wait_timeout_sec",
            "tc_01_post_mount_tls_log_wait_timeout_sec",
            "post_mount_sleep_sec",
            "tc_01_post_mount_sleep",
            default=120,
        )
    )
    wait_interval = int(
        tls_config_get(
            config,
            "post_mount_tls_log_wait_interval_sec",
            "tc_01_post_mount_tls_log_wait_interval_sec",
            default=3,
        )
    )
    if not wait_for_tls_strings_in_nfs_logs(
        client_node,
        nfs_node,
        nfs_name,
        expect_logs,
        timeout=wait_timeout,
        interval=wait_interval,
    ):
        if tls_config_get(
            config, "strict_tls_log_check", "tc_01_strict_logs", default=True
        ):
            raise OperationFailedError(
                f"Expected TLS-related log substrings not found within {wait_timeout}s "
                f"after mount: {expect_logs}"
            )
        log.warning("Post-mount TLS log verification failed (non-strict); continuing.")

    lookup_in_directory(client_node, mount_path)
    log.info("Performing basic IO on TLS mount (sudo) via upgrade-test helpers")
    dd_mb = 5
    base = "tc1_file"
    renamed = "tc1_renamed"
    create_file(client_node, mount_path, base)
    write_to_file_using_dd_command(client_node, mount_path, base, dd_mb)
    read_from_file_using_dd_command(client_node, mount_path, base, dd_mb)
    rename_file(client_node, mount_path, base, renamed)
    delete_file(client_node, mount_path, renamed)

    log.info("tls_deploy_mount_verify completed successfully.")


def op_tls_export_enforcement(client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_export_enforcement ===")
    fs_name = config.get("fs_name", "cephfs")
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))

    plain_export = tls_config_get(
        config, "plain_nfs_export", "tc_02_plain_export", default="/plain_export_tc2"
    )
    tls_export = tls_config_get(
        config, "tls_only_export", "tc_02_tls_export", default="/tls_export_tc2"
    )
    plain_mount = tls_config_get(
        config, "plain_export_mount", "tc_02_plain_mount", default="/mnt/plain_tc2"
    )
    tls_mount = tls_config_get(
        config, "tls_export_mount", "tc_02_tls_mount", default="/mnt/tls_tc2"
    )
    # Insecure mount attempt uses vers+port only (no xprtsec); see check_mount_fails below.
    mount_opts = f"-t nfs -o vers={version},port={port}"

    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=plain_export,
        fs=fs_name,
    )
    wait_until_nfs_export_visible(client_node, nfs_name, plain_export)

    client_node.create_dirs(dir_path=plain_mount, sudo=True)
    # TLS-enabled Ganesha still negotiates TLS on the wire; omitting xprtsec fails with e.g.
    # "mount.nfs: failed to apply fstab options" (exit 32). "Plain" here means the export
    # was created without --xprtsec tls (not TLS-mandatory at export policy), not cleartext NFS.
    Mount(client_node).nfs(
        mount=plain_mount,
        version=version,
        port=port,
        server=nfs_node.hostname,
        export=plain_export,
        xprtsec="tls",
    )
    log.info("Plain export mounted with TLS transport (xprtsec=tls); IO with sudo")
    lookup_in_directory(client_node, plain_mount)
    create_file(client_node, plain_mount, "plain_ok")
    # Strict: we know this mount succeeded; fail the step if umount errors.
    client_node.exec_command(sudo=True, cmd=f"umount {plain_mount}")

    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=tls_export,
        fs=fs_name,
        xprtsec="tls",
    )
    wait_until_nfs_export_visible(client_node, nfs_name, tls_export)

    client_node.create_dirs(dir_path=tls_mount, sudo=True)
    cmd_insecure = f"mount {mount_opts} {nfs_node.hostname}:{tls_export} {tls_mount}"
    if not check_mount_fails(client_node, cmd_insecure):
        log.error("TLS export allowed insecure mount (expected failure).")
        raise MountFailedError("TLS export allowed insecure mount (expected failure).")

    try:
        Mount(client_node).nfs(
            mount=tls_mount,
            version=version,
            port=port,
            server=nfs_node.hostname,
            export=tls_export,
            xprtsec="tls",
        )
    except MountFailedError as err:
        log.error("TLS mount with xprtsec=tls failed: %s", err)
        raise
    lookup_in_directory(client_node, tls_mount)
    create_file(client_node, tls_mount, "tls_ok")
    log.info("TLS export: IO with sudo completed.")
    # Best-effort teardown: mount_retry is for *mount* only, not umount. If the TLS
    # mount step failed earlier or cleanup already ran, plain umount can fail; use
    # Unmount (lazy umount -l, Cli.execute check_ec=False) like other NFS tests.
    Unmount(client_node).unmount(tls_mount)

    log.info("tls_export_enforcement completed successfully.")


def op_tls_logs_openssl_probe(installer_node, client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_logs_openssl_probe ===")

    expect_logs = tls_config_get(
        config,
        "tls_log_substrings",
        "tc_03_log_expect",
        "tc_01_log_expect",
        default=["AUTH_TLS", "TLS"],
    )
    verify_tls_strings_in_nfs_logs(client_node, nfs_node, nfs_name, expect_logs)

    host = nfs_node.ip_address or nfs_node.hostname
    ok_13, out_13 = probe_tls_handshake_with_openssl(
        client_node,
        host,
        port=int(config.get("port", 2049)),
        tls_version_flag="-tls1_3",
    )
    if ok_13:
        log.info("openssl TLSv1.3 probe reported a positive signal.")
    else:
        log.warning(
            "openssl TLSv1.3 probe inconclusive (NFS is not HTTPS). Snippet: %s",
            (out_13 or "")[:500],
        )

    ok_12, _ = probe_tls_handshake_with_openssl(
        client_node,
        host,
        port=int(config.get("port", 2049)),
        tls_version_flag="-tls1_2",
    )
    log.info("openssl TLSv1.2 probe ok=%s (informational)", ok_12)

    if tls_config_get(
        config, "redeploy_cluster_min_tls12", "tc_03_redeploy_tls12", default=False
    ):
        log.info(
            "redeploy_cluster_min_tls12: redeploying NFS with tls_min_version=TLSv1.2"
        )
        setup_tls_nfs_cluster(
            installer_node=installer_node,
            nfs_node=nfs_node,
            nfs_name=nfs_name,
            tls_min_version="TLSv1.2",
            tls_ciphers=tls_config_get(
                config, "nfs_tls12_ciphers", "tc_03_tls_ciphers", default="ALL"
            ),
            tls_ktls=config.get("tls_ktls", True),
            tls_debug=False,
        )
        redeploy_wait = int(
            tls_config_get(
                config,
                "redeploy_orch_wait_timeout_sec",
                "tc_03_redeploy_orch_wait_timeout_sec",
                default=300,
            )
        )
        if not wait_until_ceph_orch_nfs_ps_ready(
            client_node, nfs_name, timeout=redeploy_wait, interval=5
        ):
            raise OperationFailedError(
                f"NFS service {nfs_name} not visible in ceph orch ps after TLSv1.2 redeploy "
                f"(waited {redeploy_wait}s)"
            )

    log.info("tls_logs_openssl_probe completed.")


_NEG_SCENARIO_SERVER = "Scenario-1 invalid-server-certs"
_NEG_SCENARIO_CLIENT = "Scenario-2 invalid-client-certs"


def _log_neg_step(scenario, step, total, message):
    """Emit a numbered step line for negative TLS scenario execution."""
    log.info("[%s] Step %s/%s: %s", scenario, step, total, message)


def _log_neg_step_done(scenario, step, total, message):
    """Emit completion of a numbered negative TLS scenario step."""
    log.info("[%s] Step %s/%s complete: %s", scenario, step, total, message)


def _negative_cert_scenarios_to_run(config):
    """Resolve which negative TLS cert scenarios to run (server, client, or both)."""
    raw = tls_config_get(
        config, "negative_cert_scenario", "tc_neg_scenario", default="both"
    )
    op = _normalize_operation(raw)
    if op in ("both", "all"):
        return ["server", "client"]
    if op in ("server", "invalid_server", "server_certs"):
        return ["server"]
    if op in ("client", "invalid_client", "client_certs"):
        return ["client"]
    raise OperationFailedError(
        f"Unknown negative_cert_scenario {raw!r}; expected server, client, or both"
    )


def _tls_cluster_tls_options(config):
    return {
        "tls_min_version": config.get("tls_min_version", "TLSv1.3"),
        "tls_ciphers": config.get("tls_ciphers", "ALL"),
        "tls_ktls": config.get("tls_ktls", True),
        "tls_debug": config.get("tls_debug", True),
        "orch_wait_timeout": int(
            tls_config_get(
                config,
                "negative_redeploy_orch_wait_timeout_sec",
                "tc_neg_redeploy_orch_wait_timeout_sec",
                default=300,
            )
        ),
    }


def _restore_valid_tls_stack(
    installer, client_node, nfs_node, nfs_name, config, scenario_label="inter-scenario"
):
    """Redeploy valid server certs and restore the client tlshd trust anchor."""
    log.info(
        "[%s] Restoring valid TLS server certs and client trust before next scenario",
        scenario_label,
    )
    _log_neg_step(scenario_label, 1, 3, "Generate fresh valid TLS certificate bundle")
    cert_key, cert, ca_cert = generate_tls_cert_bundle(nfs_node)
    _log_neg_step_done(scenario_label, 1, 3, "Valid cert bundle generated")

    _log_neg_step(
        scenario_label,
        2,
        3,
        f"Redeploy NFS cluster {nfs_name!r} with valid server certificates",
    )
    deploy_tls_nfs_cluster_with_certs(
        installer_node=installer,
        nfs_node=nfs_node,
        nfs_name=nfs_name,
        cert_key=cert_key,
        cert=cert,
        ca_cert=ca_cert,
        **_tls_cluster_tls_options(config),
    )
    _log_neg_step_done(scenario_label, 2, 3, "Valid server TLS redeploy applied")

    _log_neg_step(
        scenario_label,
        3,
        3,
        f"Restore client tlshd trust anchor on {client_node.hostname}",
    )
    setup_tls_client(client_node, ca_cert)
    redeploy_wait = int(
        tls_config_get(
            config,
            "negative_redeploy_orch_wait_timeout_sec",
            "tc_neg_redeploy_orch_wait_timeout_sec",
            default=300,
        )
    )
    if not wait_until_ceph_orch_nfs_ps_ready(
        client_node, nfs_name, timeout=redeploy_wait, interval=5
    ):
        raise OperationFailedError(
            f"NFS service {nfs_name} not ready after valid TLS redeploy"
        )
    _log_neg_step_done(
        scenario_label,
        3,
        3,
        f"Client trust restored; nfs.{nfs_name} ready in ceph orch ps",
    )


def _run_small_io_on_tls_mount(client_node, mount_path, label="neg_tls", scenario=None):
    """Quick IO sanity check on a TLS mount (lookup, dd write/read, delete)."""
    if scenario:
        log.info("[%s] Running small IO (lookup, 1MiB dd write/read, delete)", scenario)
    lookup_in_directory(client_node, mount_path)
    io_file = f"{label}_io"
    create_file(client_node, mount_path, io_file)
    write_to_file_using_dd_command(client_node, mount_path, io_file, 1)
    read_from_file_using_dd_command(client_node, mount_path, io_file, 1)
    delete_file(client_node, mount_path, io_file)
    log.info(
        "Small IO on %s completed successfully%s",
        mount_path,
        f" [{scenario}]" if scenario else "",
    )


def _mount_tls_export_expect_success(
    client_node,
    nfs_node,
    export_path,
    mount_path,
    version,
    port,
    scenario=None,
):
    """Mount a TLS export and verify it appears in the mount table."""
    client_node.create_dirs(dir_path=mount_path, sudo=True)
    mount_cmd = build_tls_nfs_mount_cmd(
        nfs_node.hostname, export_path, mount_path, version, port
    )
    if scenario:
        log.info("[%s] Mount command: %s", scenario, mount_cmd)
    result = attempt_nfs_mount_expect_failure(client_node, mount_cmd)
    if result["exit_code"] != 0:
        raise OperationFailedError(
            f"TLS mount failed unexpectedly: {result['stderr'] or result['stdout']}"
        )
    out, _ = client_node.exec_command(sudo=True, cmd="mount", check_ec=False)
    if mount_path.rstrip("/") not in str(out or ""):
        raise OperationFailedError(f"TLS mount missing from mount table: {mount_path}")
    log.info(
        "TLS mount succeeded on %s at %s%s",
        client_node.hostname,
        mount_path,
        f" [{scenario}]" if scenario else "",
    )


def _log_negative_mount_failure(context, mount_result, tlshd_status):
    """Log mount failure details and full tlshd status for negative TLS scenarios."""
    log.info(
        "%s: mount failed as expected (exit=%s, cmd=%r)",
        context,
        mount_result.get("exit_code"),
        mount_result.get("mount_cmd"),
    )
    if mount_result.get("stderr"):
        log.info("%s mount stderr:\n%s", context, mount_result["stderr"])
    if mount_result.get("stdout"):
        log.info("%s mount stdout:\n%s", context, mount_result["stdout"])
    log.info("%s tlshd status:\n%s", context, tlshd_status)


def _attempt_tls_mount_and_capture_tlshd(
    client_node,
    nfs_node,
    export_path,
    mount_path,
    version,
    port,
    context,
    scenario=None,
    mount_step=None,
    tlshd_step=None,
    total_steps=None,
):
    """Attempt a TLS mount expecting failure; return mount result and tlshd status."""
    if scenario and mount_step and total_steps:
        _log_neg_step(
            scenario,
            mount_step,
            total_steps,
            "Attempt TLS mount (expect failure); capture mount error output",
        )
    client_node.create_dirs(dir_path=mount_path, sudo=True)
    mount_cmd = build_tls_nfs_mount_cmd(
        nfs_node.hostname, export_path, mount_path, version, port
    )
    log.info("[%s] Mount command: %s", context, mount_cmd)
    mount_result = attempt_nfs_mount_expect_failure(client_node, mount_cmd)
    if scenario and mount_step and total_steps:
        _log_neg_step_done(
            scenario,
            mount_step,
            total_steps,
            f"Mount attempt finished (exit={mount_result.get('exit_code')})",
        )

    if scenario and tlshd_step and total_steps:
        _log_neg_step(
            scenario,
            tlshd_step,
            total_steps,
            "Capture full systemctl status tlshd output",
        )
    tlshd_status = capture_tlshd_status(client_node)
    if scenario and tlshd_step and total_steps:
        _log_neg_step_done(
            scenario,
            tlshd_step,
            total_steps,
            "tlshd status captured; verifying handshake failure in journal",
        )

    _log_negative_mount_failure(context, mount_result, tlshd_status)
    assert_mount_failed(
        mount_result, context, client_node=client_node, mount_path=mount_path
    )
    assert_tlshd_handshake_failure(tlshd_status, context)
    return mount_result, tlshd_status


def _scenario_invalid_server_certs(
    installer, client_node, nfs_node, config, nfs_name, export_path, mount_path
):
    """Valid TLS IO, redeploy Ganesha with corrupted certs, mount must fail."""
    scenario = _NEG_SCENARIO_SERVER
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    total = 7
    log.info("=== %s: invalid server certificates on Ganesha ===", scenario)
    log.info(
        "[%s] Plan: valid mount+IO baseline → corrupt Ganesha certs → "
        "expect mount failure + tlshd handshake error",
        scenario,
    )

    _log_neg_step(
        scenario,
        1,
        total,
        f"Mount TLS export {export_path!r} at {mount_path!r} "
        f"(valid Ganesha certs, xprtsec=tls)",
    )
    _mount_tls_export_expect_success(
        client_node,
        nfs_node,
        export_path,
        mount_path,
        version,
        port,
        scenario=scenario,
    )
    _log_neg_step_done(scenario, 1, total, "Baseline TLS mount succeeded")

    _log_neg_step(scenario, 2, total, "Run small IO on mount to verify TLS path works")
    _run_small_io_on_tls_mount(
        client_node, mount_path, label="valid_server", scenario=scenario
    )
    _log_neg_step_done(scenario, 2, total, "Baseline IO succeeded")

    _log_neg_step(scenario, 3, total, f"Umount {mount_path!r} before cert redeploy")
    Unmount(client_node).unmount(mount_path)
    _log_neg_step_done(scenario, 3, total, "Mount point released")

    _log_neg_step(
        scenario,
        4,
        total,
        "Generate corrupted server ssl_key/ssl_cert/ssl_ca_cert and redeploy Ganesha",
    )
    cert_key, cert, ca_cert = generate_tls_cert_bundle(nfs_node)
    bad_key = corrupt_pem_material(cert_key)
    bad_cert = corrupt_pem_material(cert)
    bad_ca = corrupt_pem_material(ca_cert)
    deploy_tls_nfs_cluster_with_certs(
        installer_node=installer,
        nfs_node=nfs_node,
        nfs_name=nfs_name,
        cert_key=bad_key,
        cert=bad_cert,
        ca_cert=bad_ca,
        **_tls_cluster_tls_options(config),
    )
    _log_neg_step_done(
        scenario, 4, total, f"NFS cluster {nfs_name!r} redeployed with invalid PEMs"
    )

    _log_neg_step(
        scenario,
        5,
        total,
        f"Wait for nfs.{nfs_name} daemon ready in ceph orch ps after redeploy",
    )
    redeploy_wait = int(
        tls_config_get(
            config,
            "negative_redeploy_orch_wait_timeout_sec",
            "tc_neg_redeploy_orch_wait_timeout_sec",
            default=300,
        )
    )
    if not wait_until_ceph_orch_nfs_ps_ready(
        client_node, nfs_name, timeout=redeploy_wait, interval=5
    ):
        raise OperationFailedError(
            f"NFS service {nfs_name} not ready after invalid-cert redeploy"
        )
    _log_neg_step_done(scenario, 5, total, "Ganesha daemon running with invalid certs")

    _attempt_tls_mount_and_capture_tlshd(
        client_node,
        nfs_node,
        export_path,
        mount_path,
        version,
        port,
        "invalid_server_certs",
        scenario=scenario,
        mount_step=6,
        tlshd_step=7,
        total_steps=total,
    )
    log.info(
        "[%s] PASSED — mount and tlshd correctly rejected invalid server certs",
        scenario,
    )


def _scenario_invalid_client_certs(
    client_node, nfs_node, config, export_path, mount_path
):
    """Valid TLS IO, install mismatched client trust anchor, mount must fail."""
    scenario = _NEG_SCENARIO_CLIENT
    version = config.get("nfs_version", "4.2")
    port = str(config.get("port", "2049"))
    total = 7
    log.info("=== %s: invalid client trust anchor (tlshd) ===", scenario)
    log.info(
        "[%s] Plan: valid mount+IO baseline → install mismatched client CA → "
        "expect mount failure + tlshd handshake error",
        scenario,
    )

    _log_neg_step(
        scenario,
        1,
        total,
        f"Mount TLS export {export_path!r} at {mount_path!r} "
        f"(valid client trust anchor, xprtsec=tls)",
    )
    _mount_tls_export_expect_success(
        client_node,
        nfs_node,
        export_path,
        mount_path,
        version,
        port,
        scenario=scenario,
    )
    _log_neg_step_done(scenario, 1, total, "Baseline TLS mount succeeded")

    _log_neg_step(scenario, 2, total, "Run small IO on mount to verify TLS path works")
    _run_small_io_on_tls_mount(
        client_node, mount_path, label="valid_client", scenario=scenario
    )
    _log_neg_step_done(scenario, 2, total, "Baseline IO succeeded")

    _log_neg_step(
        scenario, 3, total, f"Umount {mount_path!r} before client cert change"
    )
    Unmount(client_node).unmount(mount_path)
    _log_neg_step_done(scenario, 3, total, "Mount point released")

    _log_neg_step(
        scenario,
        4,
        total,
        f"Install mismatched trust anchor on {client_node.hostname} "
        f"(valid PEM, wrong CN/IP — does not match Ganesha server cert)",
    )
    invalid_ca = generate_mismatched_tls_trust_cert(nfs_node)
    write_client_tls_ca_cert(client_node, invalid_ca)
    _log_neg_step_done(
        scenario,
        4,
        total,
        "Client trust anchor replaced; tlshd restarted",
    )

    _log_neg_step(scenario, 5, total, "Wait for tlshd to settle after trust change")
    sleep(2)
    _log_neg_step_done(scenario, 5, total, "Settle wait complete")

    _attempt_tls_mount_and_capture_tlshd(
        client_node,
        nfs_node,
        export_path,
        mount_path,
        version,
        port,
        "invalid_client_certs",
        scenario=scenario,
        mount_step=6,
        tlshd_step=7,
        total_steps=total,
    )
    log.info(
        "[%s] PASSED — mount and tlshd correctly rejected invalid client trust",
        scenario,
    )


def op_tls_negative_invalid_certs(installer, client_node, nfs_node, config, nfs_name):
    log.info("=== Operation: tls_negative_invalid_certs ===")
    fs_name = config.get("fs_name", "cephfs")
    export_name = tls_config_get(
        config, "tls_negative_export", "tc_neg_export", default="/tls_export_neg"
    )
    mount_path = tls_config_get(
        config, "tls_negative_mount", "tc_neg_mount", default="/mnt/tls_neg_tc"
    )
    scenarios = _negative_cert_scenarios_to_run(config)
    log.info(
        "Negative TLS cert test layout: export=%r mount=%r scenarios=%s",
        export_name,
        mount_path,
        scenarios,
    )
    log.info(
        "Pre-run: TLS cluster %r and client tlshd were configured by run() before "
        "this operation",
        nfs_name,
    )

    log.info(
        "[Setup] Step 1/2: Create TLS export %r on cluster %r (--xprtsec tls)",
        export_name,
        nfs_name,
    )
    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=export_name,
        fs=fs_name,
        xprtsec="tls",
    )
    log.info("[Setup] Step 2/2: Wait until export is visible in ceph nfs export ls")
    wait_until_nfs_export_visible(client_node, nfs_name, export_name)
    log.info("[Setup] Export %r ready for negative TLS scenarios", export_name)

    if "server" in scenarios:
        _scenario_invalid_server_certs(
            installer,
            client_node,
            nfs_node,
            config,
            nfs_name,
            export_name,
            mount_path,
        )

    if "client" in scenarios:
        if "server" in scenarios:
            _restore_valid_tls_stack(
                installer,
                client_node,
                nfs_node,
                nfs_name,
                config,
                scenario_label="inter-scenario-restore",
            )
        _scenario_invalid_client_certs(
            client_node, nfs_node, config, export_name, mount_path
        )

    log.info("[Cleanup] Lazy-umount %r if still mounted", mount_path)
    client_node.exec_command(sudo=True, cmd=f"umount -l {mount_path}", check_ec=False)
    log.info("tls_negative_invalid_certs completed successfully.")


def _setup_tls_io_capture_prerequisites(client_node):
    """Install packages required for tcpdump capture and fio I/O on the client."""
    log.info("=== Setup: install tcpdump and fio on NFS TLS client ===")
    ensure_package_installed(client_node, "tcpdump")
    ensure_package_installed(client_node, "fio")
    log.info(
        "Client %s ready for TLS I/O capture (tcpdump + fio installed)",
        client_node.hostname,
    )


def _create_nfs_export_for_capture(
    client_node, fs_name, nfs_name, export_name, mount_name, xprtsec=None
):
    """Create export (subvolume + export) without mounting — mount follows capture start."""
    client_export_mount_dict = exports_mounts_perclient(
        [client_node], export_name, mount_name, 1
    )
    export_path = client_export_mount_dict[client_node]["export"][0]
    mount_path = client_export_mount_dict[client_node]["mount"][0]
    export_kwargs = {"xprtsec": xprtsec} if xprtsec else {}
    Ceph(client_node).nfs.export.create(
        fs_name=fs_name,
        nfs_name=nfs_name,
        nfs_export=export_path,
        fs=fs_name,
        **export_kwargs,
    )
    wait_until_nfs_export_visible(client_node, nfs_name, export_path)
    return export_path, mount_path


def _mount_export_for_capture(
    client_node, nfs_node, export_path, mount_path, version, port, xprtsec=None
):
    """Mount an export after tcpdump has started (TLS handshake included when xprtsec=tls)."""
    mount_versions = _get_client_specific_mount_versions(version, [client_node])
    mount_kwargs = {"xprtsec": xprtsec} if xprtsec else {}
    for mount_version, clients in mount_versions.items():
        clients[0].create_dirs(dir_path=mount_path, sudo=True)
        if not mount_retry(
            client=clients[0],
            mount_name=mount_path,
            version=mount_version,
            port=port,
            nfs_server=nfs_node.hostname,
            export_name=export_path,
            **mount_kwargs,
        ):
            label = "TLS" if xprtsec else "plain"
            raise OperationFailedError(
                f"Failed to mount {label} export {export_path} on {client_node.hostname}"
            )
        sleep(1)
    log.info(
        "%s mount succeeded on %s at %s",
        "TLS" if xprtsec else "Plain",
        client_node.hostname,
        mount_path,
    )


def _pcap_analysis_kwargs(config):
    """Shared pcap verification options from suite config."""
    return {
        "min_total_packets": int(
            tls_config_get(
                config, "min_capture_packets", "tc_04_min_capture_packets", default=10
            )
        ),
        "max_packets_to_analyze": int(
            tls_config_get(
                config,
                "max_pcap_packets_to_analyze",
                "tc_04_max_pcap_packets",
                default=10000,
            )
        ),
        "pcap_analyze_timeout": int(
            tls_config_get(
                config,
                "pcap_analyze_timeout_sec",
                "tc_04_pcap_analyze_timeout_sec",
                default=300,
            )
        ),
    }


def _run_capture_fio_phase(
    client_node,
    nfs_node,
    export_path,
    mount_path,
    version,
    port,
    server_host,
    pcap_path,
    snaplen,
    config,
    xprtsec=None,
):
    """Start tcpdump, mount, run fio write/read, stop capture, and lazy-umount."""
    capture_pid = None
    mount_created = False
    try:
        capture_pid = start_tcpdump_capture(
            client_node,
            server_host=server_host,
            port=port,
            pcap_path=pcap_path,
            snaplen=snaplen,
        )
        _mount_export_for_capture(
            client_node,
            nfs_node,
            export_path,
            mount_path,
            version,
            port,
            xprtsec=xprtsec,
        )
        mount_created = True
        return _run_fio_write_read_on_mount(client_node, mount_path, config)
    finally:
        if capture_pid:
            try:
                stop_tcpdump_capture(client_node, pcap_path, pid=capture_pid)
            except Exception as err:
                log.error("Failed to stop tcpdump: %s", err)
        if mount_created:
            try:
                client_node.exec_command(
                    sudo=True,
                    cmd=f"umount -l {mount_path}",
                    check_ec=False,
                )
                log.info("Unmounted %s", mount_path)
            except Exception as err:
                log.error("Failed to unmount %s: %s", mount_path, err)
        try:
            client_node.exec_command(
                sudo=True,
                cmd=f"rm -f {mount_path}/tls_fio_encrypt.dat",
                check_ec=False,
            )
        except Exception as err:
            log.error("Failed to cleanup temp files on %s: %s", mount_path, err)


def _run_stunnel_capture_fio_phase(
    client_node,
    export_path,
    mount_path,
    version,
    accept_port,
    server_host,
    server_port,
    pcap_path,
    snaplen,
    config,
):
    """Start tcpdump on server:port, mount via stunnel, run fio, stop capture."""
    capture_pid = None
    mount_created = False
    try:
        log.info(
            "[Step 4/6] Start tcpdump on %s:%s (capture TLS wire to Ganesha)",
            server_host,
            server_port,
        )
        capture_pid = start_tcpdump_capture(
            client_node,
            server_host=server_host,
            port=server_port,
            pcap_path=pcap_path,
            snaplen=snaplen,
        )
        log.info(
            "[Step 5/6] Mount export via stunnel local proxy (port=%s)", accept_port
        )
        mount_export_via_stunnel(
            client_node, export_path, mount_path, version, accept_port
        )
        mount_created = True
        log.info("[Step 6/6] Run fio write/read while capture is active")
        return _run_fio_write_read_on_mount(client_node, mount_path, config)
    finally:
        if capture_pid:
            try:
                stop_tcpdump_capture(client_node, pcap_path, pid=capture_pid)
            except Exception as err:
                log.error("Failed to stop tcpdump: %s", err)
        if mount_created:
            try:
                client_node.exec_command(
                    sudo=True,
                    cmd=f"umount -l {mount_path}",
                    check_ec=False,
                )
                log.info("Unmounted %s", mount_path)
            except Exception as err:
                log.error("Failed to unmount %s: %s", mount_path, err)


def op_tls_stunnel_io_tcpdump_encryption(
    installer_node, client_node, nfs_node, config, nfs_name
):
    """
    TLS Ganesha + stunnel client (no kTLS/tlshd): mount through local stunnel proxy,
    run fio I/O under tcpdump, verify wire traffic is encrypted (no cleartext pattern).
    """
    log.info("=== Operation: tls_stunnel_io_tcpdump_encryption ===")
    fs_name = config.get("fs_name", "cephfs")
    tls_nfs_name = nfs_name or config.get("nfs_cluster_name", DEFAULT_NFS_TLS_CLUSTER)
    export_name = tls_config_get(
        config,
        "stunnel_export_path",
        "tc_stunnel_export",
        default="/tls_export_stunnel",
    )
    mount_name = tls_config_get(
        config,
        "stunnel_mount_path",
        "tc_stunnel_mount",
        default="/mnt/tls_stunnel_tc",
    )
    pcap_path = tls_config_get(
        config,
        "stunnel_pcap_path",
        "tc_stunnel_pcap_path",
        default="/tmp/tls_pcap/stunnel_tc.pcap",
    )
    version = config.get("nfs_version", "4.2")
    server_port = int(config.get("port", 2049))
    accept_port = int(
        tls_config_get(
            config,
            "stunnel_accept_port",
            "tc_stunnel_accept_port",
            default=49152,
        )
    )
    server_host = nfs_node.ip_address or nfs_node.hostname
    snaplen = int(
        tls_config_get(
            config, "tcpdump_snaplen", "tc_stunnel_tcpdump_snaplen", default=512
        )
    )
    min_tls_app_records = int(
        tls_config_get(
            config,
            "min_tls_app_records",
            "tc_stunnel_min_tls_app_records",
            default=5,
        )
    )
    pcap_kwargs = _pcap_analysis_kwargs(config)
    ca_cert = config.get("_tls_ca_cert")
    if not ca_cert:
        raise OperationFailedError(
            "Missing TLS CA (run() should deploy TLS cluster before this operation)"
        )

    log.info("[Step 1/6] Install tcpdump, fio, and stunnel on client")
    _setup_tls_io_capture_prerequisites(client_node)
    ensure_package_installed(client_node, "stunnel")

    log.info("[Step 2/6] Configure stunnel on client (no kTLS/tlshd)")
    setup_stunnel_client(
        client_node,
        ca_cert,
        server_host=server_host,
        server_port=server_port,
        accept_port=accept_port,
        ssl_version=config.get("stunnel_ssl_version", "TLSv1.3"),
        ciphers=config.get("stunnel_ciphers", "ALL"),
        verify_level=int(config.get("stunnel_verify", 2)),
    )

    ok, _ = verify_ceph_orch_nfs_running(client_node, tls_nfs_name)
    if not ok:
        raise OperationFailedError(
            f"TLS NFS service {tls_nfs_name} not reported in ceph orch ps"
        )

    log.info("[Step 3/6] Create TLS export %r on cluster %r", export_name, tls_nfs_name)
    export_path, mount_path = _create_nfs_export_for_capture(
        client_node,
        fs_name,
        tls_nfs_name,
        export_name,
        mount_name,
        xprtsec="tls",
    )

    fio_stats = _run_stunnel_capture_fio_phase(
        client_node,
        export_path,
        mount_path,
        version,
        accept_port,
        server_host,
        server_port,
        pcap_path,
        snaplen,
        config,
    )

    log.info("Analyzing pcap %s for encrypted wire traffic", pcap_path)
    tls_analysis = verify_tls_encrypted_traffic_in_pcap(
        client_node,
        pcap_path,
        cleartext_marker=fio_stats["cleartext_marker"],
        min_tls_app_records=min_tls_app_records,
        max_cleartext_marker_hits=int(
            tls_config_get(
                config,
                "max_tls_cleartext_hits",
                "tc_stunnel_max_cleartext_hits",
                default=50,
            )
        ),
        **pcap_kwargs,
    )
    log.info(
        "tls_stunnel_io_tcpdump_encryption completed: cleartext_hits=%s "
        "(allowed<=%s) tls_record_signal=%s packets=%s",
        tls_analysis["cleartext_marker_hits"],
        tls_analysis["allowed_cleartext_marker_hits"],
        tls_analysis["tls_record_signal"],
        tls_analysis["packet_count"],
    )
    stop_stunnel_client(client_node)


def op_tls_io_tcpdump_encryption(
    installer_node, client_node, nfs_node, config, nfs_name
):
    """
    Two-phase wire encryption test:

    1. Plain NFS cluster + export: fio with known buffer pattern; pcap must show
       the pattern in cleartext (unencrypted control baseline).
    2. TLS NFS cluster + export: same fio; pcap must not show the pattern
       (encrypted traffic).
    """
    log.info("=== Operation: tls_io_tcpdump_encryption ===")
    _setup_tls_io_capture_prerequisites(client_node)
    fs_name = config.get("fs_name", "cephfs")
    plain_nfs_name = tls_config_get(
        config,
        "plain_nfs_cluster_name",
        "tc_04_plain_cluster",
        default=DEFAULT_NFS_PLAIN_CLUSTER,
    )
    tls_nfs_name = nfs_name or config.get("nfs_cluster_name", DEFAULT_NFS_TLS_CLUSTER)
    plain_export_name = tls_config_get(
        config,
        "plain_nfs_export",
        "tc_04_plain_export",
        default="/plain_export_tc4",
    )
    plain_mount_name = tls_config_get(
        config,
        "plain_export_mount",
        "tc_04_plain_mount",
        default="/mnt/plain_tc4",
    )
    tls_export_name = tls_config_get(
        config, "tls_export_path", "tc_04_export", default="/tls_export_tc4"
    )
    tls_mount_name = tls_config_get(
        config, "tls_client_mount", "tc_04_mount", default="/mnt/tls_tc4"
    )
    plain_pcap_path = tls_config_get(
        config,
        "plain_tcpdump_pcap_path",
        "tc_04_plain_pcap_path",
        default="/tmp/tls_pcap/tc04_plain.pcap",
    )
    tls_pcap_path = tls_config_get(
        config,
        "tcpdump_pcap_path",
        "tc_04_pcap_path",
        default="/tmp/tls_pcap/tc04.pcap",
    )
    version = config.get("nfs_version", "4.2")
    port = int(config.get("port", 2049))
    server_host = nfs_node.ip_address or nfs_node.hostname
    snaplen = int(
        tls_config_get(config, "tcpdump_snaplen", "tc_04_tcpdump_snaplen", default=512)
    )
    pcap_kwargs = _pcap_analysis_kwargs(config)
    min_tls_app_records = int(
        tls_config_get(
            config,
            "min_tls_app_records",
            "tc_04_min_tls_app_records",
            default=5,
        )
    )

    # --- Phase 1: plain NFS control (expect cleartext fio pattern on wire) ---
    log.info(
        "=== Phase 1: plain NFS baseline (non-TLS cluster, expect cleartext pattern) ==="
    )
    setup_plain_nfs_cluster(installer_node, nfs_node, plain_nfs_name)
    ok, _ = verify_ceph_orch_nfs_running(client_node, plain_nfs_name)
    if not ok:
        raise OperationFailedError(
            f"Plain NFS service {plain_nfs_name} not reported in ceph orch ps"
        )

    plain_export_path, plain_mount_path = _create_nfs_export_for_capture(
        client_node, fs_name, plain_nfs_name, plain_export_name, plain_mount_name
    )
    plain_fio_stats = _run_capture_fio_phase(
        client_node,
        nfs_node,
        plain_export_path,
        plain_mount_path,
        version,
        port,
        server_host,
        plain_pcap_path,
        snaplen,
        config,
        xprtsec=None,
    )
    plain_analysis = verify_cleartext_traffic_in_pcap(
        client_node,
        plain_pcap_path,
        cleartext_marker=plain_fio_stats["cleartext_marker"],
        min_cleartext_marker_hits=int(
            tls_config_get(
                config,
                "min_plain_cleartext_hits",
                "tc_04_min_plain_cleartext_hits",
                default=1,
            )
        ),
        **pcap_kwargs,
    )
    log.info(
        "Phase 1 plain baseline ok: marker_hits=%s packets=%s (traffic not encrypted)",
        plain_analysis["cleartext_marker_hits"],
        plain_analysis["packet_count"],
    )

    log.info("Tearing down plain NFS cluster before TLS deploy")
    full_tls_stack_cleanup(
        client_node,
        plain_nfs_name,
        mount_candidates=[plain_mount_path],
        fs_name=fs_name,
    )

    # --- Phase 2: TLS NFS (expect encrypted wire, no cleartext pattern) ---
    log.info("=== Phase 2: TLS NFS (TLS cluster + export, expect encrypted wire) ===")
    ca_cert = setup_tls_nfs_cluster(
        installer_node,
        nfs_node,
        tls_nfs_name,
        tls_min_version=config.get("tls_min_version", "TLSv1.3"),
        tls_ciphers=config.get("tls_ciphers", "ALL"),
        tls_ktls=config.get("tls_ktls", True),
        tls_debug=False,
    )
    setup_tls_client(client_node, ca_cert)

    ok, _ = verify_ceph_orch_nfs_running(client_node, tls_nfs_name)
    if not ok:
        raise OperationFailedError("TLS NFS service not reported in ceph orch ps")

    tls_export_path, tls_mount_path = _create_nfs_export_for_capture(
        client_node,
        fs_name,
        tls_nfs_name,
        tls_export_name,
        tls_mount_name,
        xprtsec="tls",
    )
    tls_fio_stats = _run_capture_fio_phase(
        client_node,
        nfs_node,
        tls_export_path,
        tls_mount_path,
        version,
        port,
        server_host,
        tls_pcap_path,
        snaplen,
        config,
        xprtsec="tls",
    )
    tls_analysis = verify_tls_encrypted_traffic_in_pcap(
        client_node,
        tls_pcap_path,
        cleartext_marker=tls_fio_stats["cleartext_marker"],
        min_tls_app_records=min_tls_app_records,
        max_cleartext_marker_hits=int(
            tls_config_get(
                config,
                "max_tls_cleartext_hits",
                "tc_04_max_tls_cleartext_hits",
                default=50,
            )
        ),
        plain_cleartext_marker_hits=plain_analysis["cleartext_marker_hits"],
        max_cleartext_leak_ratio=float(
            tls_config_get(
                config,
                "max_tls_cleartext_leak_ratio",
                "tc_04_max_tls_cleartext_leak_ratio",
                default=0.0001,
            )
        ),
        **pcap_kwargs,
    )
    log.info(
        "tls_io_tcpdump_encryption completed: plain cleartext_hits=%s, "
        "TLS cleartext_hits=%s (allowed<=%s) tls_record_signal=%s (packets=%s)",
        plain_analysis["cleartext_marker_hits"],
        tls_analysis["cleartext_marker_hits"],
        tls_analysis["allowed_cleartext_marker_hits"],
        tls_analysis["tls_record_signal"],
        tls_analysis["packet_count"],
    )


_OP_DISPATCH = {
    OP_TLS_DEPLOY_MOUNT_VERIFY: lambda inst, c, n, cfg, name: op_tls_deploy_mount_verify(
        c, n, cfg, name
    ),
    OP_TLS_EXPORT_ENFORCEMENT: lambda inst, c, n, cfg, name: op_tls_export_enforcement(
        c, n, cfg, name
    ),
    OP_TLS_LOGS_OPENSSL_PROBE: lambda inst, c, n, cfg, name: op_tls_logs_openssl_probe(
        inst, c, n, cfg, name
    ),
    OP_TLS_IO_TCPDUMP_ENCRYPTION: lambda inst, c, n, cfg, name: op_tls_io_tcpdump_encryption(
        inst, c, n, cfg, name
    ),
    OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION: lambda inst, c, n, cfg, name: op_tls_stunnel_io_tcpdump_encryption(
        inst, c, n, cfg, name
    ),
    OP_TLS_NEGATIVE_INVALID_CERTS: lambda inst, c, n, cfg, name: op_tls_negative_invalid_certs(
        inst, c, n, cfg, name
    ),
}


def run(ceph_cluster, **kw):
    """
    Each invocation is independent: deploy TLS NFS + tlshd, run config.operation, full cleanup.

    config.operation: tls_deploy_mount_verify | tls_export_enforcement |
        tls_logs_openssl_probe | tls_io_tcpdump_encryption |
        tls_stunnel_io_tcpdump_encryption | tls_negative_invalid_certs |
        tls_full_workflow (or tls_all_in_one)
    """
    log.info("Starting NFS TLS feature tests (independent mode)")
    config = kw.get("config", {})

    steps = _operations_to_run(config)

    installer = ceph_cluster.get_nodes(role="installer")[0]
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")
    clients = ceph_cluster.get_nodes(role="client")
    no_clients = int(config.get("clients", 1))
    clients = clients[:no_clients]
    client_node = clients[0]

    if not nfs_nodes or not clients:
        raise OperationFailedError("Requires at least one NFS node and one client")

    nfs_node = nfs_nodes[0]
    nfs_name = config.get("nfs_cluster_name", DEFAULT_NFS_TLS_CLUSTER)
    plain_nfs_name = tls_config_get(
        config,
        "plain_nfs_cluster_name",
        "tc_04_plain_cluster",
        default=DEFAULT_NFS_PLAIN_CLUSTER,
    )
    fs_name = config.get("fs_name", "cephfs")
    tcpdump_self_contained = steps == [OP_TLS_IO_TCPDUMP_ENCRYPTION]
    stunnel_op = OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION in steps

    try:
        if not tcpdump_self_contained:
            ca_cert = setup_tls_nfs_cluster(
                installer_node=installer,
                nfs_node=nfs_node,
                nfs_name=nfs_name,
                tls_min_version=config.get("tls_min_version", "TLSv1.3"),
                tls_ciphers=config.get("tls_ciphers", "ALL"),
                tls_ktls=config.get("tls_ktls", True),
                tls_debug=False,
            )
            config["_tls_ca_cert"] = ca_cert
            if not stunnel_op:
                setup_tls_client(client_node, ca_cert)
            else:
                log.info(
                    "Skipping kTLS/tlshd client setup; %s uses stunnel",
                    OP_TLS_STUNNEL_IO_TCPDUMP_ENCRYPTION,
                )

        for step in steps:
            _OP_DISPATCH[step](installer, client_node, nfs_node, config, nfs_name)

        log.info("NFS TLS operations %s completed successfully.", steps)
        return 0

    except Exception as e:
        log.error("Test failed: %s", e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Full TLS stack cleanup (umount, exports, cluster, subvolumes)")
        try:
            if stunnel_op:
                stop_stunnel_client(client_node)
            if tcpdump_self_contained:
                full_tls_stack_cleanup(client_node, plain_nfs_name, fs_name=fs_name)
            full_tls_stack_cleanup(client_node, nfs_name, fs_name=fs_name)
        except Exception as ex:
            log.error("Cleanup failed: %s", ex)
