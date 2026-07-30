"""
NFS-Ganesha conditional logging automation.

Operations (config.operation):
  tc_cl_config_01  — Static ANY policy (Part A baseline + Part B Conditional)
  tc_cl_config_02  — Static MATCH_ALL (Part A baseline + Part B1–B4)
  tc_cl_config_03  — Invalid/malformed config handling
  tc_cl_dynamic_01 — ganesha_mgr CRUD
  tc_cl_dynamic_02 — DBus persistence / hot-reload
  conditional_logging_all — run all of the above in order
"""

from __future__ import annotations

import traceback
from time import sleep

from conditional_logging.conditional_logging_utils import (
    CONDITIONAL_LOG_BACKUP_SUFFIX,
    TestCaseResult,
    TestRunReport,
    _cl_tmp_path,
    apply_conditional_log_template,
    build_baseline_log_block,
    build_conditional_log_block,
    capture_ganesha_log_window,
    cl_config_get,
    client_ip,
    create_nfs_exports,
    ganesha_mgr,
    is_ganesha_running,
    log_contains_any,
    log_contains_fatal,
    malformed_log_block_cases,
    mount_export,
    parse_ganesha_mgr_show_output,
    read_ganesha_log,
    redeploy_and_wait,
    reload_ganesha,
    run_light_io,
    umount_export,
    verify_baseline_no_conditional_debug,
    verify_conditional_verbosity,
    verify_match_policy_in_log,
    verify_matching_client_conditional_debug,
    verify_no_elevated_debug,
)
from nfs_delegation_operations import (
    backup_ganesha_template,
    redeploy_nfs_clusters,
    restore_ganesha_template,
)
from nfs_operations import (
    cleanup_cluster,
    setup_nfs_cluster,
    verify_nfs_ganesha_service,
)

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

OP_TC_CL_CONFIG_01 = "tc_cl_config_01"
OP_TC_CL_CONFIG_02 = "tc_cl_config_02"
OP_TC_CL_CONFIG_03 = "tc_cl_config_03"
OP_TC_CL_DYNAMIC_01 = "tc_cl_dynamic_01"
OP_TC_CL_DYNAMIC_02 = "tc_cl_dynamic_02"
OP_CONDITIONAL_LOGGING_ALL = "conditional_logging_all"

_ALL_OPERATIONS = [
    OP_TC_CL_CONFIG_01,
    OP_TC_CL_CONFIG_02,
    OP_TC_CL_CONFIG_03,
    OP_TC_CL_DYNAMIC_01,
    OP_TC_CL_DYNAMIC_02,
]


def _normalize_operation(name):
    if name is None:
        return None
    return str(name).strip().lower().replace("-", "_")


def _operations_to_run(config):
    raw = config.get("operation")
    if raw is None:
        raise OperationFailedError(
            "config.operation is required. Use one of: "
            + ", ".join(_ALL_OPERATIONS + [OP_CONDITIONAL_LOGGING_ALL])
        )
    op = _normalize_operation(raw)
    if op == OP_CONDITIONAL_LOGGING_ALL:
        return list(_ALL_OPERATIONS)
    if op in _ALL_OPERATIONS:
        return [op]
    raise OperationFailedError(f"Unknown operation {raw!r}")


def _ensure_nfs_cluster(ceph_cluster, config, installer, clients, nfs_nodes, cephadm):
    nfs_name = config.get("nfs_name", "cephfs-nfs-cl")
    fs_name = config.get("fs_name", "cephfs")
    nfs_version = config.get("nfs_version", "4.2")
    nfs_port = str(config.get("port", "2049"))
    nfs_mount = config.get("nfs_mount", "/mnt/nfs_cl")
    bootstrap_export = config.get("bootstrap_export", "/export_cl_bootstrap")
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    auto_create = bool(config.get("auto_create_nfs_cluster", True))

    nfs_clusters = cephadm.nfs.cluster.ls()
    created = False
    if nfs_name not in nfs_clusters:
        if not auto_create:
            raise ConfigError(f"NFS cluster {nfs_name!r} not found")
        nfs_servers = [node.hostname for node in nfs_nodes]
        setup_nfs_cluster(
            clients=[clients[0]],
            nfs_server=nfs_servers,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=bootstrap_export,
            fs=fs_name,
            ceph_cluster=ceph_cluster,
            single_export=True,
        )
        verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)
        created = True
    return nfs_name, fs_name, created


def _export_paths(config, count=3):
    base = cl_config_get(
        config, "export_path_prefix", "tc_cl_export", default="/cl_export"
    )
    return [f"{base}_{i}" for i in range(count)]


def _mount_paths(config, count=3):
    base = cl_config_get(
        config, "mount_path_prefix", "tc_cl_mount", default="/mnt/cl_export"
    )
    return [f"{base}_{i}" for i in range(count)]


def _run_tc_cl_config_01(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-01: Static Match_Policy=ANY (two-phase).

    Part A — no Conditional block: elevated FSAL:F_DBG / EXPORT|NFS:M_DBG must be absent.
    Part B — Conditional enabled for client1 + export: matching client elevates;
             non-matching client does not.
    """
    result = TestCaseResult("TC-CL-CONFIG-01", "Static Config – Basic ANY Policy")
    try:
        matched_client = ctx["clients"][0]
        unmatched_client = ctx["clients"][1]
        matched_ip = client_ip(matched_client)

        export_paths = _export_paths(ctx["config"], count=2)
        mount_paths = _mount_paths(ctx["config"], count=2)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        # Same export mounted on both clients; ANY policy filters by Clients IP.
        shared_export = export_paths[0]
        shared_export_id = path_to_id[shared_export]
        mp_matched = mount_paths[0]
        mp_unmatched = mount_paths[1]

        mount_export(
            matched_client,
            ctx["nfs_server"],
            shared_export,
            mp_matched,
            ctx["version"],
            ctx["port"],
        )
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            shared_export,
            mp_unmatched,
            ctx["version"],
            ctx["port"],
        )

        # --- Part A: baseline LOG without Conditional ---
        log.info("=== TC-CL-CONFIG-01 Part A: baseline (no conditional logging) ===")
        baseline_block = build_baseline_log_block(
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
        )
        apply_conditional_log_template(ctx["cmd_host"], baseline_block)
        container_id, _, _ = redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )
        reload_ganesha(ctx["nfs_node"], container_id)

        def _part_a_io():
            run_light_io(matched_client, mp_matched, dd_count=10)
            run_light_io(unmatched_client, mp_unmatched, dd_count=10)

        part_a_log = capture_ganesha_log_window(
            ctx["nfs_node"], container_id, _part_a_io, settle_sec=8
        )
        ok_a, detail_a = verify_baseline_no_conditional_debug(part_a_log)
        if not ok_a:
            umount_export(matched_client, mp_matched)
            umount_export(unmatched_client, mp_unmatched)
            return result.mark(False, f"Part A FAIL: {detail_a}")
        log.info("Part A PASS: %s", detail_a)

        # --- Part B: Conditional Match_Policy=ANY ---
        log.info(
            "=== TC-CL-CONFIG-01 Part B: Conditional ANY "
            "(Clients=%s Exports=%s) ===",
            matched_ip,
            shared_export_id,
        )
        conditional_block = build_conditional_log_block(
            match_policy="ANY",
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            conditional_components={"FSAL": "FULL_DEBUG", "NFS_V4": "MID_DEBUG"},
            exports=[shared_export_id],
            clients=[matched_ip],
        )
        apply_conditional_log_template(ctx["cmd_host"], conditional_block)
        container_id, _, _ = redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )
        reload_ganesha(ctx["nfs_node"], container_id)

        def _matched_io():
            run_light_io(matched_client, mp_matched, dd_count=10)

        def _unmatched_io():
            run_light_io(unmatched_client, mp_unmatched, dd_count=10)

        matched_log = capture_ganesha_log_window(
            ctx["nfs_node"], container_id, _matched_io, settle_sec=8
        )
        unmatched_log = capture_ganesha_log_window(
            ctx["nfs_node"], container_id, _unmatched_io, settle_sec=8
        )

        ok_b1, detail_b1 = verify_matching_client_conditional_debug(matched_log)
        ok_b2, detail_b2 = verify_baseline_no_conditional_debug(unmatched_log)

        umount_export(matched_client, mp_matched)
        umount_export(unmatched_client, mp_unmatched)

        if not ok_b1:
            return result.mark(False, f"Part B matching FAIL: {detail_b1}")
        if not ok_b2:
            return result.mark(False, f"Part B non-matching FAIL: {detail_b2}")

        return result.mark(
            True,
            f"Part A PASS ({detail_a}); Part B PASS "
            f"(matching: {detail_b1}; non-matching: {detail_b2})",
        )
    except Exception as err:
        return result.mark(False, str(err))


def _run_tc_cl_config_02(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-02: Static Match_Policy=ALL (two-phase).

    Part A — baseline without Conditional: no elevated markers.
    Part B — MATCH_ALL Conditional for client1 + export1:
      B1 client+export match → elevate
      B2 client match, export mismatch → no elevate
      B3 client mismatch, export match → no elevate
      B4 neither match → no elevate
    """
    result = TestCaseResult("TC-CL-CONFIG-02", "Static Config – MATCH_ALL Policy")
    try:
        matched_client = ctx["clients"][0]
        unmatched_client = ctx["clients"][1]
        matched_ip = client_ip(matched_client)

        export_paths = _export_paths(ctx["config"], count=2)
        mount_paths = _mount_paths(ctx["config"], count=4)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        matched_export = export_paths[0]
        unmatched_export = export_paths[1]
        matched_export_id = path_to_id[matched_export]

        # --- Part A: baseline LOG without Conditional ---
        log.info("=== TC-CL-CONFIG-02 Part A: baseline (no conditional logging) ===")
        baseline_block = build_baseline_log_block(
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
        )
        apply_conditional_log_template(ctx["cmd_host"], baseline_block)
        container_id, _, _ = redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )
        reload_ganesha(ctx["nfs_node"], container_id)

        # Mount matching export on both clients for baseline I/O.
        mount_export(
            matched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[0],
            ctx["version"],
            ctx["port"],
        )
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[1],
            ctx["version"],
            ctx["port"],
        )

        def _part_a_io():
            run_light_io(matched_client, mount_paths[0], dd_count=5)
            run_light_io(unmatched_client, mount_paths[1], dd_count=5)

        part_a_log = capture_ganesha_log_window(
            ctx["nfs_node"], container_id, _part_a_io, settle_sec=8
        )
        ok_a, detail_a = verify_baseline_no_conditional_debug(part_a_log)
        umount_export(matched_client, mount_paths[0])
        umount_export(unmatched_client, mount_paths[1])
        if not ok_a:
            return result.mark(False, f"Part A FAIL: {detail_a}")
        log.info("Part A PASS: %s", detail_a)

        # --- Part B: Conditional Match_Policy=ALL ---
        log.info(
            "=== TC-CL-CONFIG-02 Part B: Conditional ALL "
            "(Clients=%s Exports=%s) ===",
            matched_ip,
            matched_export_id,
        )
        conditional_block = build_conditional_log_block(
            match_policy="ALL",
            global_level="EVENT",
            components={"FSAL": "INFO", "NFS_V4": "INFO"},
            conditional_components={"FSAL": "FULL_DEBUG", "NFS_V4": "MID_DEBUG"},
            exports=[matched_export_id],
            clients=[matched_ip],
        )
        apply_conditional_log_template(ctx["cmd_host"], conditional_block)
        container_id, _, _ = redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )
        # Capture confirmation of policy acceptance around reload.
        policy_log = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: reload_ganesha(ctx["nfs_node"], container_id),
            settle_sec=5,
        )
        ok_policy, detail_policy = verify_match_policy_in_log(
            policy_log, expected_policy="MATCH_ALL"
        )
        if not ok_policy:
            # Also check a wider log tail in case confirmation was slightly earlier.
            wider = read_ganesha_log(ctx["nfs_node"], container_id, tail_lines=4000)
            ok_policy, detail_policy = verify_match_policy_in_log(
                wider, expected_policy="MATCH_ALL"
            )
        if not ok_policy:
            return result.mark(
                False, f"Part B policy confirmation FAIL: {detail_policy}"
            )
        log.info("Part B policy confirmation PASS: %s", detail_policy)

        # B1 — matching client + matching export
        log.info("=== Part B1: matching client + matching export ===")
        mount_export(
            matched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[0],
            ctx["version"],
            ctx["port"],
        )
        log_b1 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_paths[0], dd_count=5),
            settle_sec=8,
        )
        umount_export(matched_client, mount_paths[0])
        ok_b1, detail_b1 = verify_matching_client_conditional_debug(log_b1)
        if not ok_b1:
            return result.mark(False, f"Part B1 matching FAIL: {detail_b1}")
        log.info("Part B1 PASS: %s", detail_b1)

        # B2 — matching client + non-matching export
        log.info("=== Part B2: matching client + non-matching export ===")
        mount_export(
            matched_client,
            ctx["nfs_server"],
            unmatched_export,
            mount_paths[1],
            ctx["version"],
            ctx["port"],
        )
        log_b2 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_paths[1], dd_count=5),
            settle_sec=8,
        )
        umount_export(matched_client, mount_paths[1])
        ok_b2, detail_b2 = verify_baseline_no_conditional_debug(log_b2)
        if not ok_b2:
            return result.mark(
                False, f"Part B2 client-match/export-mismatch FAIL: {detail_b2}"
            )
        log.info("Part B2 PASS: %s", detail_b2)

        # B3 — non-matching client + matching export
        log.info("=== Part B3: non-matching client + matching export ===")
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            matched_export,
            mount_paths[2],
            ctx["version"],
            ctx["port"],
        )
        log_b3 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(unmatched_client, mount_paths[2], dd_count=5),
            settle_sec=8,
        )
        umount_export(unmatched_client, mount_paths[2])
        ok_b3, detail_b3 = verify_baseline_no_conditional_debug(log_b3)
        if not ok_b3:
            return result.mark(
                False, f"Part B3 client-mismatch/export-match FAIL: {detail_b3}"
            )
        log.info("Part B3 PASS: %s", detail_b3)

        # B4 — neither matches (recommended)
        log.info("=== Part B4: non-matching client + non-matching export ===")
        mount_export(
            unmatched_client,
            ctx["nfs_server"],
            unmatched_export,
            mount_paths[3],
            ctx["version"],
            ctx["port"],
        )
        log_b4 = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(unmatched_client, mount_paths[3], dd_count=5),
            settle_sec=8,
        )
        umount_export(unmatched_client, mount_paths[3])
        ok_b4, detail_b4 = verify_baseline_no_conditional_debug(log_b4)
        if not ok_b4:
            return result.mark(False, f"Part B4 neither-match FAIL: {detail_b4}")
        log.info("Part B4 PASS: %s", detail_b4)

        return result.mark(
            True,
            f"Part A PASS ({detail_a}); policy ({detail_policy}); "
            f"B1 ({detail_b1}); B2 ({detail_b2}); B3 ({detail_b3}); B4 ({detail_b4})",
        )
    except Exception as err:
        return result.mark(False, str(err))


def _run_tc_cl_config_03(ctx) -> TestCaseResult:
    """
    TC-CL-CONFIG-03: graceful handling of malformed Conditional Logging configs.

    Cases (service must stay up; no FATAL):
      1  invalid Match_Policy → WARN about INVALID_POLICY
      2  unknown component → WARN about INVALID_COMPONENT
      3  invalid log level SUPER_DEBUG → WARN about SUPER_DEBUG
      4b empty/trailing commas → starts and stays running
      6  Match_Policy without Conditional → starts; no elevated markers
    """
    result = TestCaseResult(
        "TC-CL-CONFIG-03", "Invalid/Malformed Config & Error Handling"
    )
    failures = []
    passed_cases = []
    try:
        matched_client = ctx["clients"][0]
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        mount_paths = _mount_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = path_to_id[export_paths[0]]
        mount_path = mount_paths[0]

        cases = malformed_log_block_cases(export_id=export_id, client=matched_ip)
        for case in cases:
            case_name = case["name"]
            log.info("=== TC-CL-CONFIG-03 %s ===", case_name)
            try:
                apply_conditional_log_template(ctx["cmd_host"], case["log_block"])
                container_id, _, _ = redeploy_and_wait(
                    ctx["cephadm"],
                    ctx["installer"],
                    ctx["nfs_name"],
                    ctx["redeploy_wait"],
                    ctx["service_wait_timeout"],
                )
                reload_ganesha(ctx["nfs_node"], container_id)

                if not is_ganesha_running(ctx["nfs_node"], container_id):
                    failures.append(f"{case_name}: ganesha not running after reload")
                    continue

                umount_export(matched_client, mount_path)
                mount_export(
                    matched_client,
                    ctx["nfs_server"],
                    export_paths[0],
                    mount_path,
                    ctx["version"],
                    ctx["port"],
                )

                def _io():
                    run_light_io(matched_client, mount_path, dd_count=2)

                case_log = capture_ganesha_log_window(
                    ctx["nfs_node"], container_id, _io, settle_sec=6
                )
                # Also include a wider tail in case WARN was logged at reload time.
                wide_log = read_ganesha_log(
                    ctx["nfs_node"], container_id, tail_lines=8000
                )
                combined_log = case_log + "\n" + wide_log

                if log_contains_fatal(combined_log):
                    failures.append(f"{case_name}: FATAL/abort found in ganesha log")
                    continue
                if not is_ganesha_running(ctx["nfs_node"], container_id):
                    failures.append(f"{case_name}: ganesha exited after I/O")
                    continue

                if case.get("require_warn"):
                    tokens = case.get("expect_warn") or []
                    if not log_contains_any(combined_log, tokens):
                        failures.append(
                            f"{case_name}: expected warn tokens {tokens!r} not found"
                        )
                        continue

                if case.get("verify_no_elevate"):
                    ok_elev, detail_elev = verify_baseline_no_conditional_debug(
                        case_log
                    )
                    if not ok_elev:
                        failures.append(
                            f"{case_name}: unexpected elevated markers ({detail_elev})"
                        )
                        continue

                passed_cases.append(case_name)
                log.info("%s PASS", case_name)
            except Exception as err:
                failures.append(f"{case_name}: {err}")

        umount_export(matched_client, mount_path)

        # Restore a known-good baseline LOG block.
        apply_conditional_log_template(
            ctx["cmd_host"],
            build_baseline_log_block(
                global_level="EVENT",
                components={"FSAL": "INFO", "NFS_V4": "INFO"},
            ),
        )
        redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )

        if failures:
            return result.mark(
                False,
                f"passed={passed_cases}; failures={failures}",
            )
        return result.mark(True, f"validated graceful cases: {', '.join(passed_cases)}")
    except Exception as err:
        return result.mark(False, str(err))


def _run_tc_cl_dynamic_01(ctx) -> TestCaseResult:
    """TC-CL-DYNAMIC-01: ganesha_mgr CRUD lifecycle."""
    result = TestCaseResult(
        "TC-CL-DYNAMIC-01", "ganesha_mgr Full CRUD + Enable/Disable"
    )
    try:
        container_id = ctx["container_id"]
        matched_client = ctx["clients"][0]
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = list(path_to_id.values())[0]
        mount_path = _mount_paths(ctx["config"], 1)[0]

        ganesha_mgr(
            container_id=container_id,
            nfs_node=ctx["nfs_node"],
            args="reset log conditional_config",
        )
        for show_cmd in (
            "show conditional_clients",
            "show conditional_exports",
            "show conditional_match_policy",
        ):
            ganesha_mgr(ctx["nfs_node"], container_id, show_cmd)

        ganesha_mgr(
            ctx["nfs_node"], container_id, f"add conditional_clients {matched_ip}"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "add conditional_clients 192.168.1.0/24"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, f"add conditional_exports {export_id}"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "set log conditional FSAL FULL_DEBUG"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "set log conditional NFS_V4 MID_DEBUG"
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, "update conditional_match_policy ALL"
        )

        show_out, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show log conditional_config"
        )
        parsed = parse_ganesha_mgr_show_output(show_out)
        if str(export_id) not in str(
            parsed.get("exports", [])
        ) and export_id not in parsed.get("exports", []):
            return result.mark(
                False, f"export {export_id} not in show output: {show_out[:300]}"
            )
        if not parsed.get("clients"):
            return result.mark(False, f"no clients in show output: {show_out[:300]}")

        mount_export(
            matched_client,
            ctx["nfs_server"],
            export_paths[0],
            mount_path,
            ctx["version"],
            ctx["port"],
        )
        log_after_add = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
        )
        ok_add, detail_add = verify_conditional_verbosity(log_after_add, "")

        clients_show, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show conditional_clients"
        )
        remove_target = matched_ip
        if "/32" in clients_show:
            for line in clients_show.splitlines():
                if matched_ip in line:
                    remove_target = line.strip()
                    break
        ganesha_mgr(
            ctx["nfs_node"],
            container_id,
            f"remove conditional_clients {remove_target}",
        )
        ganesha_mgr(
            ctx["nfs_node"], container_id, f"remove conditional_exports {export_id}"
        )

        log_after_remove = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
        )
        umount_export(matched_client, mount_path)
        ok_remove, detail_remove = verify_no_elevated_debug(log_after_remove)
        passed = ok_add and ok_remove
        detail = f"after add: {detail_add}; after remove: {detail_remove}"
        return result.mark(passed, detail)
    except Exception as err:
        return result.mark(False, str(err))


def _run_tc_cl_dynamic_02(ctx) -> TestCaseResult:
    """TC-CL-DYNAMIC-02: DBus overrides vs config reload."""
    result = TestCaseResult("TC-CL-DYNAMIC-02", "Persistence, Hot-Reload & Coexistence")
    try:
        container_id = ctx["container_id"]
        matched_client = ctx["clients"][0]
        matched_ip = client_ip(matched_client)
        export_paths = _export_paths(ctx["config"], count=1)
        path_to_id = create_nfs_exports(
            ctx["cmd_host"], ctx["fs_name"], ctx["nfs_name"], export_paths
        )
        export_id = list(path_to_id.values())[0]
        mount_path = _mount_paths(ctx["config"], 1)[0]

        base_block = build_conditional_log_block(
            match_policy="ANY",
            global_level="EVENT",
            conditional_components={"FSAL": "EVENT", "NFS_V4": "EVENT"},
            exports=[export_id],
            clients=[matched_ip],
        )
        apply_conditional_log_template(ctx["cmd_host"], base_block)
        redeploy_and_wait(
            ctx["cephadm"],
            ctx["installer"],
            ctx["nfs_name"],
            ctx["redeploy_wait"],
            ctx["service_wait_timeout"],
        )

        ganesha_mgr(
            ctx["nfs_node"],
            container_id,
            "set log conditional_config "
            f"--components FSAL,NFS_V4 --level FULL_DEBUG "
            f"--clients {matched_ip} --export-ids {export_id} --policy ANY",
        )
        mount_export(
            matched_client,
            ctx["nfs_server"],
            export_paths[0],
            mount_path,
            ctx["version"],
            ctx["port"],
        )
        log_dbus = capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
        )
        ok_dbus, _ = verify_conditional_verbosity(log_dbus, "")

        reload_ganesha(ctx["nfs_node"], container_id)
        sleep(3)
        show_out, _ = ganesha_mgr(
            ctx["nfs_node"], container_id, "show log conditional_config"
        )
        parsed = parse_ganesha_mgr_show_output(show_out)

        capture_ganesha_log_window(
            ctx["nfs_node"],
            container_id,
            lambda: run_light_io(matched_client, mount_path, dd_count=5),
            settle_sec=6,
        )
        umount_export(matched_client, mount_path)

        if not is_ganesha_running(ctx["nfs_node"], container_id):
            return result.mark(False, "ganesha not running after reload")
        passed = ok_dbus and is_ganesha_running(ctx["nfs_node"], container_id)
        detail = (
            f"dbus elevated logs ok={ok_dbus}; post-reload policy={parsed.get('match_policy')}; "
            "ganesha stable after reload"
        )
        return result.mark(passed, detail)
    except Exception as err:
        return result.mark(False, str(err))


_TC_DISPATCH = {
    OP_TC_CL_CONFIG_01: _run_tc_cl_config_01,
    OP_TC_CL_CONFIG_02: _run_tc_cl_config_02,
    OP_TC_CL_CONFIG_03: _run_tc_cl_config_03,
    OP_TC_CL_DYNAMIC_01: _run_tc_cl_dynamic_01,
    OP_TC_CL_DYNAMIC_02: _run_tc_cl_dynamic_02,
}


def run(ceph_cluster, **kw):
    """Entry point for cephci suite execution."""
    config = kw.get("config", {})
    steps = _operations_to_run(config)
    report = TestRunReport()

    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")
    min_clients = int(config.get("min_clients", 2))
    if len(clients) < min_clients:
        raise ConfigError(
            f"conditional logging requires at least {min_clients} clients"
        )
    clients = clients[: max(int(config.get("clients", min_clients)), min_clients)]

    if not nfs_nodes or not installers:
        raise ConfigError("Requires nfs and installer nodes")

    installer = installers[0]
    nfs_node = nfs_nodes[0]
    nfs_cmd_host = nfs_node
    cephadm = CephAdm(installer).ceph
    redeploy_wait = int(config.get("redeploy_wait", 15))
    service_wait_timeout = int(config.get("service_wait_timeout", 300))
    reset_on_exit = bool(config.get("reset_ganesha_template_on_exit", True))

    backup_path = _cl_tmp_path(CONDITIONAL_LOG_BACKUP_SUFFIX)
    template_backup_exists = backup_ganesha_template(nfs_cmd_host, backup_path)
    created_cluster = False
    nfs_name = config.get("nfs_name", "cephfs-nfs-cl")

    try:
        nfs_name, fs_name, created_cluster = _ensure_nfs_cluster(
            ceph_cluster, config, installer, clients, nfs_nodes, cephadm
        )
        container_id, _, _ = redeploy_and_wait(
            cephadm, installer, nfs_name, redeploy_wait, service_wait_timeout
        )
        ctx = {
            "config": config,
            "clients": clients,
            "nfs_node": nfs_node,
            "nfs_server": nfs_node.hostname,
            "cmd_host": nfs_cmd_host,
            "installer": installer,
            "cephadm": cephadm,
            "nfs_name": nfs_name,
            "fs_name": fs_name,
            "version": config.get("nfs_version", "4.2"),
            "port": str(config.get("port", "2049")),
            "redeploy_wait": redeploy_wait,
            "service_wait_timeout": service_wait_timeout,
            "container_id": container_id,
        }

        for step in steps:
            log.info("=== Running conditional logging operation: %s ===", step)
            report.add(_TC_DISPATCH[step](ctx))

        for line in report.summary_lines():
            log.info(line)

        if not report.all_passed():
            failed = [r.tc_id for r in report.results if not r.passed]
            raise OperationFailedError(f"Conditional logging failures: {failed}")
        return 0

    except Exception as err:
        log.error("Conditional logging test failed: %s", err)
        log.error(traceback.format_exc())
        return 1
    finally:
        if reset_on_exit:
            try:
                restore_ganesha_template(
                    nfs_cmd_host, template_backup_exists, backup_path
                )
                redeploy_nfs_clusters(
                    cephadm,
                    [nfs_name],
                    installer,
                    redeploy_wait,
                    service_wait_timeout,
                )
            except Exception as cleanup_err:
                log.error("Template restore failed: %s", cleanup_err)
        if created_cluster and config.get("cleanup_cluster_on_exit", False):
            try:
                cleanup_cluster(
                    clients[0],
                    nfs_name,
                    fs_name=config.get("fs_name", "cephfs"),
                )
            except Exception as cleanup_err:
                log.error("Cluster cleanup failed: %s", cleanup_err)
