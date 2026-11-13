"""NFS-Ganesha xcopy / COPY offload cases (downstream / cephadm only)."""

from nfs_operations import (
    cleanup_cluster,
    cleanup_custom_nfs_cluster_multi_export_client,
    setup_custom_nfs_cluster_multi_export_client,
    setup_nfs_cluster,
)

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.common_lib import get_node_time
from tests.nfs.lib.xcopy import (
    DEFAULT_PCAP_PATH,
    copy_across_exports,
    copy_within_export,
    create_sparse_test_file,
    create_test_file,
    ensure_copy_offload,
    mount_xcopy_nfs,
    start_xcopy_pcap_capture,
    stop_xcopy_pcap_capture,
    verify_copy_offload_logs,
    verify_files_match,
    verify_files_shasum,
    verify_sparse_holes_preserved,
    verify_xcopy_pcap,
)
from tests.nfs.lib.xcopy.helpers import (
    announce,
    case_begin,
    case_pass,
    ganesha_service_active,
    log_case_summary,
)
from utility.log import Log

log = Log(__name__)

CROSS_CASE = "cross_export_copy_offload_on"


def assert_ganesha_up(ctx, label="scenario"):
    """Shared post-case check used by async/resiliency modules."""
    if not ganesha_service_active(
        ctx["ganesha_node"], nfs_name=ctx["nfs_name"], client=ctx["client"]
    ):
        raise OperationFailedError(f"nfs-ganesha not active after {label}")


# Context / setup / cleanup


def _build_ctx(ceph_cluster, config, case_names):
    """Resolve nodes and common paths/params into a shared case context."""
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    installers = ceph_cluster.get_nodes("installer")

    log.info(
        "Cluster nodes: clients=%s nfs=%s installers=%s",
        [c.hostname for c in clients],
        [n.hostname for n in nfs_nodes],
        [i.hostname for i in installers],
    )

    port = config.get("port", "2049")
    version = str(config.get("nfs_version", "4.2"))
    no_clients = int(config.get("clients", 1))
    size_mb = int(config.get("file_size_mb", 64))
    chunk_size_mb = int(config.get("chunk_size_mb", 64))
    copy_mode = config.get("copy_mode", "any")
    need_cross = CROSS_CASE in case_names

    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    pcap_path = config.get("pcap_path", DEFAULT_PCAP_PATH)
    pcap_validate = config.get("pcap_validate", True)
    pcap_offload_enabled = config.get("pcap_offload_enabled", True)

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")
    if chunk_size_mb < 1 or size_mb < 1:
        raise ConfigError("file_size_mb and chunk_size_mb must be >= 1")

    clients = clients[:no_clients]
    client = clients[0]

    if not nfs_nodes:
        raise ConfigError("No NFS nodes found in the cluster")
    if not installers:
        raise ConfigError("No installer nodes found in the cluster")
    ganesha_node = nfs_nodes[0]
    installer = installers[0]
    # Standalone placement is on ganesha_node; scrape that host only.
    nfs_nodes_for_logs = [ganesha_node]
    log.info(
        "Cephadm NFS nodes for logs: %s installer=%s",
        [n.hostname for n in nfs_nodes_for_logs],
        installer.hostname,
    )

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    export_num = 2 if need_cross else 1

    # Dual-export mounts use *_0/*_1 (cephadm multi-export layout).
    if need_cross:
        nfs_mount_a = f"{nfs_mount}_0"
        nfs_mount_b = f"{nfs_mount}_1"
    else:
        nfs_mount_a = nfs_mount
        nfs_mount_b = nfs_mount

    return {
        "ceph_cluster": ceph_cluster,
        "config": config,
        "clients": clients,
        "client": client,
        "installer": installer,
        "nfs_nodes": nfs_nodes,
        "nfs_nodes_for_logs": nfs_nodes_for_logs,
        "ganesha_node": ganesha_node,
        "need_cross": need_cross,
        # Filled by _setup_cluster from setup_* export names ({export}_N).
        "mount_export_pairs": None,
        "port": port,
        "version": version,
        "size_mb": size_mb,
        "chunk_size_mb": chunk_size_mb,
        "copy_mode": copy_mode,
        "nfs_mount": nfs_mount,
        "nfs_mount_a": nfs_mount_a,
        "nfs_mount_b": nfs_mount_b,
        "pcap_path": pcap_path,
        "pcap_validate": pcap_validate,
        "pcap_offload_enabled": pcap_offload_enabled,
        "nfs_name": nfs_name,
        "nfs_export": nfs_export,
        "export_num": export_num,
        "fs_name": "cephfs",
        "fs": "cephfs",
        # Per-case paths / timing set in each case before validations.
        "src": None,
        "dst": None,
        "copy_elapsed_sec": None,
        "short_copy": False,
        "file_size_bytes": size_mb * 1024 * 1024,
        "chunk_size_bytes": chunk_size_mb * 1024 * 1024,
        "log_since": None,
        "server_ip": getattr(ganesha_node, "ip_address", None) or ganesha_node.hostname,
    }


def _setup_cluster(ctx):
    """Deploy cluster → Copy_Offload config → debug logs → mount (in that order)."""
    client = ctx["client"]
    ganesha_node = ctx["ganesha_node"]
    need_cross = ctx["need_cross"]

    announce(
        "SETUP xcopy COPY Offload "
        f"(downstream/cephadm, "
        f"cross={'yes' if need_cross else 'no'}, "
        f"size={ctx['size_mb']}MiB, chunk={ctx['chunk_size_mb']}MiB)"
    )
    log.info(
        "Client=%s Ganesha host=%s mounts=%s,%s nfsvers=%s",
        client.hostname,
        ganesha_node.hostname,
        ctx["nfs_mount_a"],
        ctx["nfs_mount_b"],
        ctx["version"],
    )

    # 1) Cluster + exports only (no client mounts yet).
    if need_cross:
        announce("Step 1/4: create NFS cluster + multi-export (skip_mount=True)")
        setup_custom_nfs_cluster_multi_export_client(
            ctx["clients"],
            ganesha_node.hostname,
            ctx["port"],
            ctx["version"],
            ctx["nfs_name"],
            ctx["fs_name"],
            ctx["fs"],
            ctx["nfs_mount"],
            ctx["nfs_export"],
            export_num=ctx["export_num"],
            ceph_cluster=ctx["ceph_cluster"],
            skip_mount=True,
        )
        ctx["mount_export_pairs"] = [
            (ctx["nfs_mount_a"], f"{ctx['nfs_export']}_0"),
            (ctx["nfs_mount_b"], f"{ctx['nfs_export']}_1"),
        ]
    else:
        announce("Step 1/4: create NFS cluster + export (skip_mount=True)")
        export_list = setup_nfs_cluster(
            ctx["clients"],
            ganesha_node.hostname,
            ctx["port"],
            ctx["version"],
            ctx["nfs_name"],
            ctx["nfs_mount"],
            ctx["fs_name"],
            ctx["nfs_export"],
            ctx["fs"],
            ceph_cluster=ctx["ceph_cluster"],
            single_export=True,
            skip_mount=True,
        )
        # single_export still names the export {nfs_export}_0
        export_path = (export_list or [f"{ctx['nfs_export']}_0"])[0]
        ctx["mount_export_pairs"] = [(ctx["nfs_mount"], export_path)]

    # 2) Template Copy_Offload flags + redeploy, then 3) LOG via cluster config set.
    announce("Step 2–3/4: Ganesha template Copy_Offload + LOG FULL_DEBUG")
    ensure_copy_offload(
        ctx["installer"],
        ctx["nfs_name"],
        enabled=True,
        chunk_size_mb=ctx["chunk_size_mb"],
        short_copy=False,
        max_copy_workers=8,
        client=client,
        remount_ctx=None,
    )

    # 4) Mount only after config + debug logs are applied.
    announce("Step 4/4: mount NFS export(s)")
    mount_xcopy_nfs(ctx, remount=False)


def _cleanup_cluster(ctx):
    """Tear down NFS cluster (cephadm)."""
    if ctx["need_cross"]:
        announce("Cephadm cleanup: cleanup_custom_nfs_cluster_multi_export_client")
        cleanup_custom_nfs_cluster_multi_export_client(
            ctx["clients"],
            ctx["nfs_mount"],
            ctx["nfs_name"],
            ctx["nfs_export"],
            ctx["export_num"],
            nfs_nodes=ctx["nfs_nodes"],
        )
    else:
        announce("Cephadm cleanup: cleanup_cluster")
        cleanup_cluster(
            ctx["clients"],
            ctx["nfs_mount"],
            ctx["nfs_name"],
            ctx["nfs_export"],
            nfs_nodes=ctx["nfs_nodes"],
        )


# Grouped validations (run one after another)


def _mark_logs(ctx):
    """Mark podman log position immediately before the copy window."""
    ctx["log_since"], _ = get_node_time(ctx["nfs_nodes_for_logs"])
    log.info("podman log since marker: %s", ctx["log_since"])


def _require(ok, message):
    if not ok:
        raise OperationFailedError(message)
    return True


def _validate_copy_elapsed(ctx):
    """Report wall-clock seconds captured for the cp operation."""
    elapsed = ctx.get("copy_elapsed_sec")
    announce(f"cp elapsed time: {elapsed}s")
    if elapsed is None:
        raise OperationFailedError("cp elapsed time was not captured")
    log.info(
        "PASS: cp took %.3fs for %s MiB (%s -> %s)",
        float(elapsed),
        ctx["size_mb"],
        ctx["src"],
        ctx["dst"],
    )
    return True


def _validate_pcap(ctx):
    """tcpdump / capinfos / tshark offload checks (skipped if disabled)."""
    if not ctx["pcap_validate"]:
        log.info("pcap_validate=false; skipping pcap validation")
        return True
    return _require(
        verify_xcopy_pcap(
            ctx["client"],
            ctx["pcap_path"],
            file_size_bytes=ctx["file_size_bytes"],
            offload_enabled=ctx["pcap_offload_enabled"],
            chunk_size_bytes=ctx["chunk_size_bytes"],
            short_copy=bool(ctx.get("short_copy")),
        ),
        "xcopy pcap validation failed "
        f"(pcap={ctx['pcap_path']}, size={ctx['size_mb']}MiB, "
        f"chunk={ctx['chunk_size_mb']}MiB, "
        f"short_copy={bool(ctx.get('short_copy'))})",
    )


def _validate_ganesha_logs(ctx):
    """Ganesha log milestones + chunk-size / Short_Copy packaging math."""
    short_copy = bool(ctx.get("short_copy"))
    return _require(
        verify_copy_offload_logs(
            ctx["nfs_nodes_for_logs"],
            ctx["nfs_name"],
            file_size_bytes=ctx["file_size_bytes"],
            chunk_size_bytes=ctx["chunk_size_bytes"],
            since=ctx["log_since"],
            mode=ctx["copy_mode"],
            short_copy=short_copy,
            client=ctx["client"],
        ),
        "xcopy log validation failed "
        f"(mode={ctx['copy_mode']}, short_copy={short_copy})",
    )


def _default_validations():
    """Shared validation sequence for xcopy cases."""
    return [
        (
            "data_integrity_cmp",
            lambda c: _require(
                verify_files_match(c["client"], c["src"], c["dst"]),
                f"mismatched files ({c['src']} vs {c['dst']})",
            ),
        ),
        (
            "data_integrity_shasum",
            lambda c: _require(
                verify_files_shasum(c["client"], c["src"], c["dst"]),
                f"SHA256 mismatch ({c['src']} vs {c['dst']})",
            ),
        ),
        ("copy_elapsed", _validate_copy_elapsed),
        ("pcap_offload", _validate_pcap),
        ("ganesha_logs", _validate_ganesha_logs),
    ]


def _sparse_hole_validations():
    """XCOPY-06: content + hole zero/fill independence probe."""
    return [
        (
            "data_integrity_cmp",
            lambda c: _require(
                verify_files_match(c["client"], c["src"], c["dst"]),
                f"mismatched files ({c['src']} vs {c['dst']})",
            ),
        ),
        (
            "data_integrity_shasum",
            lambda c: _require(
                verify_files_shasum(c["client"], c["src"], c["dst"]),
                f"SHA256 mismatch ({c['src']} vs {c['dst']})",
            ),
        ),
        (
            "sparse_holes_preserved",
            lambda c: _require(
                verify_sparse_holes_preserved(
                    c["client"], c["src"], c["dst"], size_mb=c["size_mb"]
                ),
                f"sparse holes not preserved ({c['src']} vs {c['dst']})",
            ),
        ),
    ]


def _set_short_copy(ctx, enabled):
    """Apply Short_Copy via Ganesha template + redeploy + remount."""
    flag = bool(enabled)
    announce(f"Set Short_Copy={str(flag).lower()} via Ganesha template + redeploy")
    ctx["short_copy"] = flag
    ensure_copy_offload(
        ctx["installer"],
        ctx["nfs_name"],
        enabled=True,
        chunk_size_mb=ctx["chunk_size_mb"],
        short_copy=flag,
        max_copy_workers=8,
        client=ctx["client"],
        remount_ctx=ctx,
    )


def _run_validations(ctx, validations):
    """Run grouped validations sequentially."""
    announce(f"VALIDATIONS ({len(validations)} step(s))")
    for idx, (name, fn) in enumerate(validations, 1):
        announce(f"Validation {idx}/{len(validations)}: {name}")
        log.info("Running validation: %s", name)
        ok = fn(ctx)
        if not ok:
            raise OperationFailedError(f"Validation failed: {name}")
        log.info("PASS validation: %s", name)
    announce("VALIDATIONS complete")


def _apply_case_file_params(ctx, size_mb=None, copy_mode=None):
    """Override per-case file size / required COPY mode for validations."""
    if size_mb is not None:
        ctx["size_mb"] = int(size_mb)
        ctx["file_size_bytes"] = ctx["size_mb"] * 1024 * 1024
        log.info(
            "Case file size override: %s MiB (%s bytes)",
            ctx["size_mb"],
            ctx["file_size_bytes"],
        )
    if copy_mode is not None:
        ctx["copy_mode"] = copy_mode
        log.info("Case copy_mode override: %s", ctx["copy_mode"])


def _run_copy_with_pcap(ctx, copy_fn, create_fn=None, skip_create=False):
    """Create source (optional), mark logs, capture pcap around copy_fn."""
    client = ctx["client"]
    src, dst = ctx["src"], ctx["dst"]
    if not skip_create:
        (create_fn or create_test_file)(client, src, size_mb=ctx["size_mb"])
    _mark_logs(ctx)

    tcpdump_pid = None
    if ctx["pcap_validate"]:
        tcpdump_pid = start_xcopy_pcap_capture(
            client,
            ctx["server_ip"],
            port=ctx["port"],
            pcap_path=ctx["pcap_path"],
        )
    try:
        ctx["copy_elapsed_sec"] = copy_fn(client, src, dst)
        log.info(
            "Recorded cp elapsed: %.3fs (%s -> %s)",
            float(ctx["copy_elapsed_sec"]),
            src,
            dst,
        )
    finally:
        if tcpdump_pid is not None:
            stop_xcopy_pcap_capture(client, ctx["pcap_path"], tcpdump_pid)


# Test cases


def _case_standard_copy(
    ctx,
    name,
    tag,
    src,
    dst,
    copy_fn,
    size_mb=None,
    copy_mode=None,
    short_copy=False,
):
    """Shared XCOPY-01..03 shape: set paths, copy+pcap, default validations."""
    case_begin(name, tag)
    if size_mb is not None or copy_mode is not None:
        _apply_case_file_params(ctx, size_mb=size_mb, copy_mode=copy_mode)
    ctx["short_copy"] = bool(short_copy)
    ctx["src"] = src
    ctx["dst"] = dst
    _run_copy_with_pcap(ctx, copy_fn)
    _run_validations(ctx, _default_validations())
    case_pass(name)


def case_intra_export_copy_offload_on(ctx):
    """XCOPY-01: Copy_Offload enabled — intra-export cp (fs == fs)."""
    _case_standard_copy(
        ctx,
        "intra_export_copy_offload_on",
        "XCOPY-01",
        f"{ctx['nfs_mount_a']}/xcopy_sync_src.bin",
        f"{ctx['nfs_mount_a']}/xcopy_sync_dst.bin",
        copy_within_export,
    )


def case_cross_export_copy_offload_on(ctx):
    """XCOPY-02: Copy_Offload enabled — cross-export cp (fs1 != fs2)."""
    if not ctx["need_cross"]:
        raise ConfigError(
            "cross_export_copy_offload_on selected but dual-export setup was skipped"
        )
    _case_standard_copy(
        ctx,
        "cross_export_copy_offload_on",
        "XCOPY-02",
        f"{ctx['nfs_mount_a']}/xcopy_cross_src.bin",
        f"{ctx['nfs_mount_b']}/xcopy_cross_dst.bin",
        copy_across_exports,
    )


def case_large_file_async_copy_offload_on(ctx):
    """XCOPY-03: Large-file Copy_Offload — expect ASYNC OP_COPY + CB_OFFLOAD."""
    size_mb = int(ctx["config"].get("large_file_size_mb", 2048))
    _case_standard_copy(
        ctx,
        "large_file_async_copy_offload_on",
        "XCOPY-03",
        f"{ctx['nfs_mount_a']}/xcopy_async_src.bin",
        f"{ctx['nfs_mount_a']}/xcopy_async_dst.bin",
        copy_within_export,
        size_mb=size_mb,
        copy_mode="async",
        short_copy=False,
    )


def case_short_copy_offload_on(ctx):
    """XCOPY-04: Short_Copy=ON — per-chunk CB_OFFLOAD packaging."""
    case_begin("short_copy_offload_on", "XCOPY-04")
    # Do not inherit XCOPY-03's large_file_size_mb sticky override.
    size_mb = int(ctx["config"].get("file_size_mb", 128))
    _apply_case_file_params(ctx, size_mb=size_mb, copy_mode="async")
    if ctx["file_size_bytes"] <= ctx["chunk_size_bytes"]:
        raise ConfigError(
            "short_copy_offload_on needs file_size_mb > chunk_size_mb "
            f"(got {ctx['size_mb']} / {ctx['chunk_size_mb']})"
        )
    ctx["src"] = f"{ctx['nfs_mount_a']}/xcopy_short_src.bin"
    ctx["dst"] = f"{ctx['nfs_mount_a']}/xcopy_short_dst.bin"
    create_test_file(ctx["client"], ctx["src"], size_mb=ctx["size_mb"])
    _set_short_copy(ctx, True)
    try:
        _run_copy_with_pcap(ctx, copy_within_export, skip_create=True)
        _run_validations(ctx, _default_validations())
        case_pass("short_copy_offload_on")
    finally:
        # Sparse / other OFF cases must not see sticky Short_Copy=true.
        try:
            _set_short_copy(ctx, False)
        except Exception as exc:
            log.error("Failed to restore Short_Copy=false after XCOPY-04: %s", exc)


def _run_sparse_copy(ctx, src_name, dst_name):
    """Shared sparse create + intra-export cp (Copy_Offload ON, Short_Copy OFF)."""
    size_mb = int(ctx["config"].get("file_size_mb", 128))
    _apply_case_file_params(ctx, size_mb=size_mb, copy_mode=ctx.get("copy_mode", "any"))
    # Config must match validator: Short_Copy OFF (not just ctx flag).
    if ctx.get("short_copy"):
        _set_short_copy(ctx, False)
    else:
        ctx["short_copy"] = False
    ctx["src"] = f"{ctx['nfs_mount_a']}/{src_name}"
    ctx["dst"] = f"{ctx['nfs_mount_a']}/{dst_name}"
    _run_copy_with_pcap(ctx, copy_within_export, create_fn=create_sparse_test_file)


def case_sparse_file_copy_offload(ctx):
    """XCOPY-05: Sparse source + server-side COPY (content / offload)."""
    case_begin("sparse_file_copy_offload", "XCOPY-05")
    _run_sparse_copy(ctx, "xcopy_sparse_src.bin", "xcopy_sparse_dst.bin")
    _run_validations(ctx, _default_validations())
    case_pass("sparse_file_copy_offload")


def case_sparse_hole_preservation(ctx):
    """XCOPY-06: Hole preservation after server-side COPY."""
    case_begin("sparse_hole_preservation", "XCOPY-06")
    _run_sparse_copy(ctx, "xcopy_sparse_hole_src.bin", "xcopy_sparse_hole_dst.bin")
    _run_validations(ctx, _sparse_hole_validations())
    case_pass("sparse_hole_preservation")


CASES = {
    "intra_export_copy_offload_on": case_intra_export_copy_offload_on,
    "cross_export_copy_offload_on": case_cross_export_copy_offload_on,
    "large_file_async_copy_offload_on": case_large_file_async_copy_offload_on,
    "short_copy_offload_on": case_short_copy_offload_on,
    "sparse_file_copy_offload": case_sparse_file_copy_offload,
    "sparse_hole_preservation": case_sparse_hole_preservation,
}


def run_xcopy_module(ctx, cases, case_names, label, extra_finally=None):
    """Shared module runner: setup → per-case try/record → summary → cleanup."""
    results = {}
    try:
        _setup_cluster(ctx)
        for name in case_names:
            announce(f"RUN {label} CASE: {name}")
            try:
                cases[name](ctx)
                results[name] = "passed"
            except Exception as case_exc:
                log.error("%s case %s FAILED: %s", label, name, case_exc)
                results[name] = "failed"
        rc = log_case_summary(results, title=f"{label} CASE SUMMARY")
        if rc == 0:
            announce(f"PASS xcopy {label} module (cases={case_names})")
            log.info("Test result: PASS (return 0)")
        else:
            log.error("xcopy %s module failed", label)
            log.info("Test result: FAIL (return 1)")
        return rc
    except Exception as exc:
        log.error("xcopy %s module failed: %s", label, exc)
        log.info("Test result: FAIL (return 1)")
        return 1
    finally:
        if extra_finally:
            try:
                extra_finally(ctx)
            except Exception as cleanup_exc:
                log.warning("%s extra cleanup failed: %s", label, cleanup_exc)
        _cleanup_cluster(ctx)


def run_selected(
    ceph_cluster,
    kw,
    cases,
    defaults,
    label,
    prepare_ctx=None,
    extra_finally=None,
):
    """Normalize test_cases, build ctx, optionally prepare, then run_xcopy_module."""
    config = kw.get("config") or {}
    case_names = config.get("test_cases") or list(defaults)
    if isinstance(case_names, str):
        case_names = [case_names]
    unknown = [n for n in case_names if n not in cases]
    if unknown:
        raise ConfigError(f"Unknown test_cases {unknown}; valid: {sorted(cases)}")
    if prepare_ctx:
        config = prepare_ctx(config, case_names) or config
    ctx = _build_ctx(ceph_cluster, config, case_names)
    log.info("Running %s test_cases=%s", label, case_names)
    return run_xcopy_module(
        ctx, cases, case_names, label=label, extra_finally=extra_finally
    )


def run(ceph_cluster, **kw):
    """Run one or more xcopy cases from this module."""
    return run_selected(
        ceph_cluster,
        kw,
        CASES,
        defaults=["intra_export_copy_offload_on"],
        label="COPY",
    )
