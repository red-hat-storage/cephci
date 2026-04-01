"""
QoS test module for 3 AZ NFS-Ganesha clusters — multi-cluster,
multi-export, multi-client.

Every scenario in this module operates across multiple NFS clusters,
exports, and clients.  Single-cluster / single-export QoS tests are
covered by ``test_nfs_qos_on_cluster_level_enablement.py``.

Supports multiple test scenarios via the ``test_scenario`` config key.
Each scenario targets a specific QoS feature combination and can be run
independently against one or more pre-deployed NFS clusters.

Multi-cluster support
---------------------
The ``nfs_clusters`` and ``cephfs_volumes`` config lists control how many
NFS clusters participate in a single test run.  Clients are assigned to
exports via round-robin so the test functions even with a single client.

Subvolume-backed exports
------------------------
Every export is backed by a dedicated CephFS subvolume (in the
``ganeshagroup`` subvolume group by default).  Subvolumes are created
at the start of each test run and cleaned up in the finally block.

IO workload
-----------
Uses ``fio`` to generate parallel, high-iodepth IO that
properly stress-tests Ganesha's distributed QoS enforcement.
Configurable via ``fio_bs``, ``fio_iodepth``, ``fio_numjobs``,
``fio_runtime``, ``fio_size`` in the suite YAML.

Implemented scenarios
---------------------
cluster_bandwidth
    Cluster-level bandwidth QoS (separate read/write limits) with
    multiple exports per cluster, each mounted via round-robin clients.
    Validates per-export and per-client aggregate enforcement under
    concurrent IO load.

Placeholder scenarios (for future PRs)
--------------------------------------
cluster_bandwidth_combined
    Cluster-level combined read+write bandwidth QoS.
cluster_iops
    Cluster-level IOPS control with multi-export multi-client.
export_bw_override
    Export-level bandwidth QoS overriding cluster defaults.
export_bw_combined
    Export-level combined bandwidth QoS.
export_iops
    Export-level IOPS control on individual exports.
export_qos_inheritance
    Disable export QoS, verify export re-inherits cluster defaults.
qos_failover
    QoS persistence during NFS site/node failure in 3 AZ.
cross_site_client
    Cross-DC client mounting NFS export from a different datacenter.
qos_post_upgrade
    QoS enforcement survives cluster upgrade.

Test workflow (cluster_bandwidth)
---------------------------------------
Phase 0  – Parse config, install ``fio`` on all client nodes.
Phase 1  – Resolve each NFS cluster's server IP via ``ceph nfs cluster info``.
Phase 2  – Create namespace-isolated CephFS subvolumes (one per export).
Phase 3  – Build the assignment table (export→client round-robin mapping).
Phase 4  – Create NFS exports backed by the subvolume paths.
Phase 5  – Mount each export on its assigned client, verify mounts & daemons.
Phase 6–8 (loop per BW limit) –
           Enable cluster-level QoS with the current bandwidth limit,
           log export QoS details, execute the configured operation (default:
           none), run concurrent ``fio`` across all mounts, collect results,
           disable QoS, and settle before the next iteration.
Phase 9  – Generate consolidated QoS scaling report (per-export, per-client
           aggregate, per-cluster) and determine PASS/FAIL.
Finally  – Snapshot cluster & NFS state, then clean up all mounts, exports,
           and subvolumes.

Cleanup is non-destructive: only exports, subvolumes, and mounts created
by the test are removed; the NFS cluster itself is never deleted.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.nfs_operations import nfs_log_parser
from tests.nfs.qos.test_nfs_qos_on_cluster_level_enablement import (
    _extract_first_float,
    enable_disable_qos_for_cluster,
)
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

_PHASE_SEP = "-" * 70


def _log_phase(number, title):
    """Emit a clear phase banner for easy log searching."""
    log.info(f"\n{_PHASE_SEP}")
    log.info(f"  PHASE {number}: {title}")
    log.info(_PHASE_SEP)


# ---------------------------------------------------------------------------
# Helper functions (shared across scenarios)
# ---------------------------------------------------------------------------


def _get_nfs_server_ip(client, cluster_name):
    """Return the first virtual/backend IP from ``ceph nfs cluster info``.
    The cluster info is a dictionary with the cluster name as the key and
    the value is a dictionary with the following keys:
    - "backend": a list of dictionaries with the following keys:
      - "hostname": the hostname of the backend server
      - "ip": the IP address of the backend server
      - "port": the port of the backend server
    """

    log.info(f"Resolving NFS server IP for cluster '{cluster_name}'...")
    info = Ceph(client).nfs.cluster.info(cluster_name)
    if not info:
        raise OperationFailedError(f"No info returned for NFS cluster {cluster_name}")

    log.info(f"  Raw cluster info: {info}")

    try:
        ip = info[cluster_name]["backend"][0]["hostname"]
    except (KeyError, IndexError, TypeError) as e:
        raise OperationFailedError(
            f"Could not resolve NFS server IP from cluster info: {info} ({e})"
        )
    log.info(f"  -> Resolved {cluster_name} to IP {ip}")
    return ip


def _create_export(client, cephfs_volume, cluster_name, export_path, cephfs_path="/"):
    """Create a CephFS-backed NFS export directly via CLI."""
    cmd = (
        f"ceph nfs export create cephfs {cluster_name} "
        f"{export_path} {cephfs_volume} --path={cephfs_path}"
    )
    log.info(
        f"Creating export: cluster={cluster_name}, pseudo={export_path}, "
        f"cephfs={cephfs_volume}, path={cephfs_path}"
    )
    out, err = client.exec_command(sudo=True, cmd=cmd)
    log.info(f"  stdout={out}")
    if err:
        err = str(err).strip()
        if err:
            log.info(f"  stderr={err}")

    sleep(5)
    export_list = Ceph(client).nfs.export.ls(cluster_name) or []
    if export_path not in export_list:
        raise OperationFailedError(
            f"Export {export_path} not found after creation on "
            f"{cluster_name}. Current exports: {export_list}"
        )
    log.info(f"  Export {export_path} verified on {cluster_name}")


@retry(OperationFailedError, tries=5, delay=10, backoff=2)
def _mount_export(client, nfs_server_ip, export_path, mount_point, port="2049"):
    """Mount an NFS export on a client node."""
    client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    log.info(
        f"Mounting {nfs_server_ip}:{export_path} -> {mount_point} "
        f"on {client.hostname} (port={port})..."
    )

    if Mount(client).nfs(
        mount=mount_point,
        version="4.2",
        port=port,
        server=nfs_server_ip,
        export=export_path,
    ):
        raise OperationFailedError(
            f"mount command returned non-zero for {export_path} on "
            f"{client.hostname}"
        )
    log.info(f"  Mounted on {client.hostname}")


def _create_subvolumes_for_cluster(
    client, cephfs_volume, cluster_name, num_exports, subvol_group
):
    """Create subvolume group and N namespace-isolated subvolumes.

    Returns a list of dicts: [{"name": str, "path": str}, ...].
    """
    ceph_fs = Ceph(client).fs

    log.info(
        f"Creating {num_exports} subvolume(s) on {cephfs_volume} for "
        f"cluster {cluster_name} (group={subvol_group})..."
    )
    try:
        ceph_fs.sub_volume_group.create(volume=cephfs_volume, group=subvol_group)
        log.info(f"  Subvolume group '{subvol_group}' ensured on {cephfs_volume}")
    except Exception:
        log.info(
            f"  Subvolume group '{subvol_group}' already exists on {cephfs_volume}"
        )

    subvolumes = []
    for i in range(num_exports):
        sv_name = f"qos_sv_{cluster_name}_{i}"
        ceph_fs.sub_volume.create(
            volume=cephfs_volume,
            subvolume=sv_name,
            group_name=subvol_group,
            namespace_isolated=True,
        )
        sv_path = str(
            ceph_fs.sub_volume.getpath(
                volume=cephfs_volume,
                subvolume=sv_name,
                group_name=subvol_group,
            )
        ).strip()
        log.info(f"  [{i}] {sv_name} -> {sv_path}")
        subvolumes.append({"name": sv_name, "path": sv_path})

    log.info(
        f"  {len(subvolumes)} subvolume(s) created on {cephfs_volume} "
        f"for {cluster_name}"
    )
    return subvolumes


def _log_nfs_diagnostics(client, cluster_names, ceph_cluster=None):
    """Log NFS-specific diagnostics and cephadm container logs for each cluster."""
    log.info("=" * 60)
    log.info("  NFS DIAGNOSTICS")
    log.info("=" * 60)

    all_nodes = ceph_cluster.get_nodes() if ceph_cluster else []

    for cn in cluster_names:
        nfs_cmds = [
            (f"ceph nfs cluster info {cn}", f"NFS cluster info ({cn})"),
            (f"ceph orch ps --service_name nfs.{cn}", f"NFS daemons ({cn})"),
            (f"ceph nfs export ls {cn}", f"NFS exports ({cn})"),
            (f"ceph nfs cluster qos get {cn}", f"NFS QoS state ({cn})"),
        ]
        for cmd, label in nfs_cmds:
            try:
                out, _ = client.exec_command(sudo=True, cmd=cmd, timeout=60)
                log.info(f"\n**** {label} ****\n{out}")
            except Exception as e:
                log.debug(f"Could not run '{cmd}': {e}")

        if all_nodes:
            try:
                nfs_nodes = _resolve_nfs_nodes(client, cn, all_nodes)
                if nfs_nodes:
                    log.info(f"\n**** NFS cephadm logs ({cn}) ****")
                    nfs_log_parser(client, nfs_nodes, cn)
            except Exception as e:
                log.debug(f"Could not fetch cephadm logs for {cn}: {e}")

    log.info("=" * 60)


def _resolve_nfs_nodes(client, cluster_name, all_nodes):
    """Return node objects hosting NFS daemons for *cluster_name*."""
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ps --service_name nfs.{cluster_name} --format json",
            timeout=30,
        )
        daemons = json.loads(out) if out else []
        daemon_hosts = {d.get("hostname") for d in daemons}
        return [n for n in all_nodes if n.hostname in daemon_hosts]
    except Exception as e:
        log.debug(f"Could not resolve NFS nodes for {cluster_name}: {e}")
        return []


def _verify_mounts(assignments, driver_client, cluster_names):
    """Verify all mounts are live and NFS daemons are healthy before IO.

    Runs ``stat`` on each mount point from the assigned client and logs
    ``ceph orch ps`` for each NFS service.  Any failure is logged as a
    warning but does not abort the test -- the subsequent fio phase will
    surface real problems.
    """
    log.info("  Verifying mounts are accessible...")
    mount_failures = 0
    for i, a in enumerate(assignments):
        try:
            out, _ = a["client"].exec_command(
                sudo=True,
                cmd=f"stat -f -c 'type=%T blocks=%b avail=%a' {a['mount_point']}",
                timeout=30,
            )
            log.info(
                f"    [{i}] {a['client'].hostname}:{a['mount_point']} -> {out.strip()}"
            )
        except Exception as e:
            mount_failures += 1
            log.warning(
                f"    [{i}] {a['client'].hostname}:{a['mount_point']} "
                f"mount check FAILED: {e}"
            )

    if mount_failures:
        log.warning(
            f"  {mount_failures}/{len(assignments)} mount(s) inaccessible, "
            f"checking NFS daemon health..."
        )
        for cn in cluster_names:
            try:
                out, _ = driver_client.exec_command(
                    sudo=True,
                    cmd=f"ceph orch ps --service_name nfs.{cn} --format json",
                    timeout=30,
                )
                daemons = json.loads(out) if out else []
                running = [d for d in daemons if d.get("status_desc") == "running"]
                log.info(
                    f"    nfs.{cn}: {len(running)}/{len(daemons)} daemon(s) running"
                )
                for d in daemons:
                    log.info(
                        f"      {d.get('daemon_name', '?')}: "
                        f"status={d.get('status_desc', '?')}, "
                        f"host={d.get('hostname', '?')}"
                    )
            except Exception as e:
                log.warning(f"    nfs.{cn}: could not check daemons: {e}")
    else:
        log.info(f"  All {len(assignments)} mount(s) accessible")


def _unmount_and_cleanup(client, mount_point):
    """Best-effort unmount and directory removal."""
    try:
        client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}/*", timeout=120)
    except Exception:
        pass
    try:
        Unmount(client).unmount(mount_point, lazy=True)
    except Exception:
        pass
    try:
        client.exec_command(sudo=True, cmd=f"rmdir {mount_point}", timeout=30)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# fio-based IO helpers
# ---------------------------------------------------------------------------


def _install_fio(clients):
    """Ensure fio is installed on all unique client nodes."""
    installed = set()
    for client in clients:
        if client.hostname in installed:
            continue
        log.info(f"  Ensuring fio is installed on {client.hostname}...")
        try:
            client.exec_command(
                sudo=True,
                cmd="rpm -q fio || yum install -y fio",
                timeout=180,
            )
            log.info(f"  fio ready on {client.hostname}")
        except Exception as e:
            log.warning(f"  Could not install fio on {client.hostname}: {e}")
        installed.add(client.hostname)


def _parse_fio_output(fio_stdout, rw_type):
    """Parse fio JSON output and return (bw_mbps, iops).

    Args:
        fio_stdout: Raw stdout from ``fio --output-format=json``
        rw_type: ``"write"`` or ``"read"``

    Returns:
        Tuple of (bandwidth_MiB_per_sec, iops).

    Raises:
        OperationFailedError: If JSON cannot be parsed or has no jobs.
    """
    raw = str(fio_stdout).strip()
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as e:
        raise OperationFailedError(
            f"Could not parse fio JSON output: {e}\n"
            f"Raw output (first 1000 chars): {raw[:1000]}"
        )

    try:
        rw_stats = data["jobs"][0][rw_type]
    except (KeyError, IndexError) as e:
        raise OperationFailedError(
            f"Unexpected fio JSON structure ({e}). "
            f"Raw (first 500 chars): {raw[:500]}"
        )

    bw_mbps = rw_stats["bw"] / 1024.0
    iops = rw_stats["iops"]
    io_mib = rw_stats.get("io_kbytes", 0) / 1024.0
    runtime_ms = rw_stats.get("runtime", 0)

    lat_ns = rw_stats.get("lat_ns", {})
    lat_mean_ms = lat_ns.get("mean", 0) / 1_000_000.0
    lat_p99_ms = lat_ns.get("percentile", {}).get("99.000000", 0) / 1_000_000.0

    log.info(
        f"    fio {rw_type} parsed: bw={bw_mbps:.3f} MiB/s, iops={iops:.1f}, "
        f"io_total={io_mib:.1f} MiB, runtime={runtime_ms}ms, "
        f"lat_mean={lat_mean_ms:.2f}ms, lat_p99={lat_p99_ms:.2f}ms"
    )
    return bw_mbps, iops


def _fio_client_io(client, mount_point, fio_params):
    """Run fio sequential write then read on a single mount.

    Returns dict with write_mbps, read_mbps, write_iops, read_iops.
    """
    fio_timeout = max(int(fio_params["runtime"]) + 120, 600)

    base_args = (
        f"--directory={mount_point} "
        f"--bs={fio_params['bs']} "
        f"--iodepth={fio_params['iodepth']} "
        f"--numjobs={fio_params['numjobs']} "
        f"--size={fio_params['size']} "
        f"--runtime={fio_params['runtime']} "
        f"--time_based --direct=1 --ioengine=libaio "
        f"--group_reporting --output-format=json "
        f"--fallocate=none"
    )

    # --- Write phase ---
    write_cmd = f"fio --name=qos_test --rw=write {base_args}"
    log.info(f"[{client.hostname}] fio WRITE starting on {mount_point}")
    log.info(f"[{client.hostname}]   cmd: {write_cmd}")
    write_out, write_err = client.exec_command(
        sudo=True,
        cmd=write_cmd,
        timeout=fio_timeout,
    )
    if write_err:
        log.info(f"[{client.hostname}]   fio write stderr: {write_err}")
    write_mbps, write_iops = _parse_fio_output(write_out, "write")

    client.exec_command(sudo=True, cmd="echo 3 > /proc/sys/vm/drop_caches")

    # --- Read phase ---
    read_cmd = f"fio --name=qos_test --rw=read {base_args}"
    log.info(f"[{client.hostname}] fio READ starting on {mount_point}")
    log.info(f"[{client.hostname}]   cmd: {read_cmd}")
    read_out, read_err = client.exec_command(
        sudo=True,
        cmd=read_cmd,
        timeout=fio_timeout,
    )
    if read_err:
        log.info(f"[{client.hostname}]   fio read stderr: {read_err}")
    read_mbps, read_iops = _parse_fio_output(read_out, "read")

    # Clean up fio data files
    try:
        client.exec_command(
            sudo=True,
            cmd=f"rm -f {mount_point}/qos_test.*",
            timeout=60,
        )
    except Exception:
        pass

    log.info(
        f"[{client.hostname}] fio RESULTS on {mount_point}: "
        f"write={write_mbps:.3f} MiB/s ({write_iops:.1f} IOPS), "
        f"read={read_mbps:.3f} MiB/s ({read_iops:.1f} IOPS)"
    )
    return {
        "write_mbps": write_mbps,
        "read_mbps": read_mbps,
        "write_iops": write_iops,
        "read_iops": read_iops,
    }


# ---------------------------------------------------------------------------
# QoS validation
# ---------------------------------------------------------------------------


def _within_qos_limit(limit_mb, actual_mbps, slack_ratio=0.10, slack_abs=0.5):
    """Return True if *actual_mbps* is within *limit_mb* (string like '15MB').

    ``slack_ratio`` defaults to 0.10 (10%) and can be overridden via the
    ``fault_tolerance`` suite config key.
    """
    limit = _extract_first_float(limit_mb)
    if limit is None:
        return False
    allowed = max(limit * (1.0 + slack_ratio), limit + slack_abs)
    return float(actual_mbps) <= allowed


def _check_single_result(qos_type, bw_limit, w, r, slack_ratio=0.10):
    """Return (write_ok, read_ok) for one export result.

    Only enforced for PerShare / PerShare_PerClient (per-export cap).
    For PerClient, individual exports are uncapped.
    """
    if qos_type not in ("PerShare", "PerShare_PerClient"):
        return True, True
    return (
        _within_qos_limit(bw_limit, w, slack_ratio=slack_ratio),
        _within_qos_limit(bw_limit, r, slack_ratio=slack_ratio),
    )


def _group_results(assignments, results, key_fn):
    """Aggregate write/read bandwidth by a grouping key.

    Returns dict ``{key: {"w": float, "r": float, "n": int}}``.
    """
    groups = {}
    for i, res in enumerate(results):
        k = key_fn(assignments[i])
        if k not in groups:
            groups[k] = {"w": 0.0, "r": 0.0, "n": 0}
        groups[k]["w"] += res["write_mbps"]
        groups[k]["r"] += res["read_mbps"]
        groups[k]["n"] += 1
    return groups


def _check_aggregate(groups, keys, bw_limit, slack_ratio=0.10):
    """Validate grouped aggregate bandwidth against N × bw_limit.

    Each group of *N* exports has an effective cap of N × ``bw_limit``.

    Returns:
        (details, violations) where *details* is a list of dicts with
        ``key``, ``n``, ``limit``, ``w``, ``r``, ``w_ok``, ``r_ok``
        and *violations* is the total count of limit breaches.
    """
    limit_val = _extract_first_float(bw_limit)
    if limit_val is None:
        return [], 0
    violations = 0
    details = []
    for k in keys:
        agg = groups.get(k)
        if not agg:
            continue
        n = agg["n"]
        cap = limit_val * n
        allowed = max(cap * (1.0 + slack_ratio), cap + 0.5 * n)
        w_ok = agg["w"] <= allowed
        r_ok = agg["r"] <= allowed
        if not w_ok:
            violations += 1
        if not r_ok:
            violations += 1
        details.append(
            {
                "key": k,
                "n": n,
                "limit": cap,
                "w": agg["w"],
                "r": agg["r"],
                "w_ok": w_ok,
                "r_ok": r_ok,
            }
        )
    return details, violations


def _log_export_qos_details(driver_client, cluster_names, assignments):
    """Log per-export details (including effective QoS) after cluster QoS is set."""
    log.info("  Per-export details after QoS enablement:")
    for cn in cluster_names:
        cn_exports = [a for a in assignments if a["cluster_name"] == cn]
        for a in cn_exports:
            try:
                out, _ = driver_client.exec_command(
                    sudo=True,
                    cmd=f"ceph nfs export get {cn} {a['export_path']}",
                    timeout=30,
                )
                log.info(f"    {cn} {a['export_path']}:\n{out.strip()}")
            except Exception as e:
                log.warning(f"    {cn} {a['export_path']}: could not get details: {e}")


def _build_bw_iterations(config):
    """Build a list of bandwidth iterations from config.

    Supports:
      - ``bw_limits: [5MB, 10MB, 15MB]``  (convenience shorthand)
      - Scalar ``max_export_write_bw: 15MB`` etc. (single iteration, backward compat)

    Only populates the parameters relevant to the ``qos_type``:
      - PerShare:            max_export_write_bw, max_export_read_bw
      - PerClient:           max_client_write_bw, max_client_read_bw
      - PerShare_PerClient:  all four
    """
    qos_type = config.get("qos_type", "PerShare")
    bw_limits = config.get("bw_limits")
    if bw_limits:
        if isinstance(bw_limits, str):
            bw_limits = [bw_limits]
        iterations = []
        for bw in bw_limits:
            params = {}
            if qos_type in ("PerShare", "PerShare_PerClient"):
                params["max_export_write_bw"] = bw
                params["max_export_read_bw"] = bw
            if qos_type in ("PerClient", "PerShare_PerClient"):
                params["max_client_write_bw"] = bw
                params["max_client_read_bw"] = bw
            iterations.append(params)
        return iterations

    return [_collect_qos_bw_params(config)]


def _print_qos_report(all_iteration_results, assignments, cfg):
    """Print a consolidated QoS scaling report across all bandwidth iterations.

    ``all_iteration_results`` is a list of:
        {"bw_limit": str, "bw_params": dict, "results": list[dict]}
    """
    qos_type = cfg["qos_type"]
    cluster_names = [c["name"] for c in cfg["clusters"]]
    cephfs_map = {c["name"]: c["cephfs_volume"] for c in cfg["clusters"]}
    slack = cfg.get("fault_tolerance", 0.10)
    sep = "=" * 78
    div = f"  {'-' * 74}"

    log.info(f"\n\n{sep}")
    log.info("  QoS BANDWIDTH SCALING REPORT")
    log.info(sep)
    cluster_cephfs_str = ", ".join(f"{cn} -> {cephfs_map[cn]}" for cn in cluster_names)
    log.info(
        f"  QoS Type:         {qos_type}\n"
        f"  Clusters (CephFS): {cluster_cephfs_str}\n"
        f"  Exports/cluster:  {cfg['num_exports_per_cluster']}\n"
        f"  Fault tolerance:  {slack * 100:.0f}%\n"
        f"  BW levels tested: {len(all_iteration_results)}\n"
        f"  fio: bs={cfg['fio']['bs']}  iodepth={cfg['fio']['iodepth']}  "
        f"numjobs={cfg['fio']['numjobs']}  runtime={cfg['fio']['runtime']}s"
    )
    log.info(sep)

    per_export_enforced = qos_type in ("PerShare", "PerShare_PerClient")
    per_client_enforced = qos_type in ("PerClient", "PerShare_PerClient")
    per_cluster_enforced = qos_type in ("PerShare", "PerShare_PerClient")

    # -- Section 1: Per-export detail table ----------------------------------
    hdr = (
        f"  {'Limit':>8}  {'Cluster':<10} {'Client':<15} "
        f"{'Export':<28} {'W MiB/s':>9} {'R MiB/s':>9} "
        f"{'W-IOPS':>8} {'R-IOPS':>8}"
    )
    if per_export_enforced:
        hdr += f"  {'W-OK':>4} {'R-OK':>4}"
    log.info(hdr)
    log.info(div)

    total_violations = 0
    per_export_violations = 0
    per_iter_violations = []
    for iteration in all_iteration_results:
        bw_label = iteration["bw_limit"]
        params = iteration.get("bw_params", {})
        export_limit = params.get("max_export_write_bw") or bw_label
        iter_v = 0
        for i, res in enumerate(iteration["results"]):
            a = assignments[i]
            line = (
                f"  {bw_label:>8}  {a['cluster_name']:<10} "
                f"{a['client'].hostname:<15} "
                f"{a['export_path']:<28} "
                f"{res['write_mbps']:>9.3f} {res['read_mbps']:>9.3f} "
                f"{res['write_iops']:>8.1f} {res['read_iops']:>8.1f}"
            )
            if per_export_enforced:
                w_ok, r_ok = _check_single_result(
                    qos_type,
                    export_limit,
                    res["write_mbps"],
                    res["read_mbps"],
                    slack_ratio=slack,
                )
                if not w_ok:
                    iter_v += 1
                if not r_ok:
                    iter_v += 1
                line += (
                    f"  {'PASS' if w_ok else 'FAIL':>4} "
                    f"{'PASS' if r_ok else 'FAIL':>4}"
                )
            log.info(line)
        log.info(div)
        per_iter_violations.append(iter_v)
        per_export_violations += iter_v
    total_violations += per_export_violations

    # Per-limit summary
    tag = "enforcement" if per_export_enforced else "informational"
    log.info(f"\n  Per-limit summary ({tag}):")
    hdr2 = f"  {'Limit':>8}  {'Avg W':>9} {'Avg R':>9}  " f"{'Max W':>9} {'Max R':>9}"
    if per_export_enforced:
        hdr2 += f"  {'Violations':>10}"
    log.info(hdr2)
    for idx, iteration in enumerate(all_iteration_results):
        writes = [r["write_mbps"] for r in iteration["results"]]
        reads = [r["read_mbps"] for r in iteration["results"]]
        line = (
            f"  {iteration['bw_limit']:>8}  "
            f"{sum(writes)/len(writes):>9.3f} {sum(reads)/len(reads):>9.3f}  "
            f"{max(writes):>9.3f} {max(reads):>9.3f}"
        )
        if per_export_enforced:
            line += f"  {per_iter_violations[idx]:>10}"
        log.info(line)

    # -- Section 2: Per-client aggregate -------------------------------------
    per_client_violations = 0
    per_client_checks = 0
    tag = "PerClient enforcement" if per_client_enforced else "informational"
    log.info(f"\n  Per-client aggregate ({tag}):")
    if per_client_enforced:
        log.info(
            f"  {'Limit':>8}  {'Client':<15} {'Exports':>7}  "
            f"{'Cli Lim':>9}  {'Total W':>9} {'Total R':>9}  "
            f"{'W-OK':>4} {'R-OK':>4}"
        )
    else:
        log.info(
            f"  {'Limit':>8}  {'Client':<15} {'Exports':>7}  "
            f"{'Total W':>9} {'Total R':>9}  {'Avg W':>9} {'Avg R':>9}"
        )
    key_fn = lambda a: a["client"].hostname  # noqa: E731
    for iteration in all_iteration_results:
        bw_label = iteration["bw_limit"]
        params = iteration.get("bw_params", {})
        client_limit = params.get("max_client_write_bw") or bw_label
        groups = _group_results(assignments, iteration["results"], key_fn)
        if per_client_enforced:
            details, v = _check_aggregate(
                groups, sorted(groups), client_limit, slack_ratio=slack
            )
            per_client_violations += v
            per_client_checks += len(details) * 2
            for d in details:
                log.info(
                    f"  {bw_label:>8}  {d['key']:<15} {d['n']:>7}  "
                    f"{d['limit']:>9.1f}  "
                    f"{d['w']:>9.3f} {d['r']:>9.3f}  "
                    f"{'PASS' if d['w_ok'] else 'FAIL':>4} "
                    f"{'PASS' if d['r_ok'] else 'FAIL':>4}"
                )
        else:
            for hn in sorted(groups):
                g = groups[hn]
                log.info(
                    f"  {bw_label:>8}  {hn:<15} {g['n']:>7}  "
                    f"{g['w']:>9.3f} {g['r']:>9.3f}  "
                    f"{g['w']/g['n']:>9.3f} {g['r']/g['n']:>9.3f}"
                )
        log.info(div)
    total_violations += per_client_violations

    # -- Section 3: Per-cluster aggregate (derived consistency check) ---------
    # No Ganesha "per-cluster" QoS param exists.  This check validates that
    # the sum of per-export bandwidth within a cluster stays ≤ N × export_limit.
    per_cluster_violations = 0
    per_cluster_checks = 0
    tag = "consistency check" if per_cluster_enforced else "informational"
    log.info(f"\n  Per-cluster aggregate ({tag}):")
    if per_cluster_enforced:
        log.info(
            f"  {'Limit':>8}  {'Cluster':<10} {'CephFS':<12} {'Exports':>7}  "
            f"{'Clust Lim':>9}  {'Total W':>9} {'Total R':>9}  "
            f"{'W-OK':>4} {'R-OK':>4}"
        )
    else:
        log.info(
            f"  {'Limit':>8}  {'Cluster':<10} {'CephFS':<12} {'Exports':>7}  "
            f"{'Total W':>9} {'Total R':>9}  {'Avg W':>9} {'Avg R':>9}"
        )
    key_fn = lambda a: a["cluster_name"]  # noqa: E731
    for iteration in all_iteration_results:
        bw_label = iteration["bw_limit"]
        params = iteration.get("bw_params", {})
        export_limit = params.get("max_export_write_bw") or bw_label
        groups = _group_results(assignments, iteration["results"], key_fn)
        if per_cluster_enforced:
            details, v = _check_aggregate(
                groups, cluster_names, export_limit, slack_ratio=slack
            )
            per_cluster_violations += v
            per_cluster_checks += len(details) * 2
            for d in details:
                cn = d["key"]
                log.info(
                    f"  {bw_label:>8}  {cn:<10} "
                    f"{cephfs_map[cn]:<12} {d['n']:>7}  "
                    f"{d['limit']:>9.1f}  "
                    f"{d['w']:>9.3f} {d['r']:>9.3f}  "
                    f"{'PASS' if d['w_ok'] else 'FAIL':>4} "
                    f"{'PASS' if d['r_ok'] else 'FAIL':>4}"
                )
        else:
            for cn in cluster_names:
                g = groups.get(cn, {"w": 0, "r": 0, "n": 1})
                log.info(
                    f"  {bw_label:>8}  {cn:<10} {cephfs_map[cn]:<12} {g['n']:>7}  "
                    f"{g['w']:>9.3f} {g['r']:>9.3f}  "
                    f"{g['w']/g['n']:>9.3f} {g['r']/g['n']:>9.3f}"
                )
        log.info(div)
    total_violations += per_cluster_violations

    # -- Verdict -------------------------------------------------------------
    per_export_checks = (
        sum(len(it["results"]) * 2 for it in all_iteration_results)
        if per_export_enforced
        else 0
    )
    total_checks = per_export_checks + per_client_checks + per_cluster_checks
    verdict = "PASS" if total_violations == 0 else "FAIL"
    log.info(f"\n  Overall: {verdict} ({total_violations}/{total_checks} violations)")

    breakdown = []
    if per_export_enforced:
        breakdown.append(f"Per-export: {per_export_violations}/{per_export_checks}")
    if per_client_enforced:
        breakdown.append(
            f"Per-client aggregate: {per_client_violations}/{per_client_checks}"
        )
    if per_cluster_enforced:
        breakdown.append(
            f"Per-cluster consistency: {per_cluster_violations}/{per_cluster_checks}"
        )
    if breakdown:
        log.info("    " + "  |  ".join(breakdown))
    log.info(sep)
    return total_violations


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


def _parse_multi_cluster_config(config):
    """Normalize config into multi-cluster format.

    CephFS volumes are assigned to clusters via round-robin, so passing
    a single volume with multiple clusters means all clusters share it.
    """
    nfs_clusters = config.get("nfs_clusters")
    if not nfs_clusters:
        raise ConfigError("nfs_clusters is required")

    cephfs_volumes = config.get("cephfs_volumes")
    if not cephfs_volumes:
        raise ConfigError("cephfs_volumes is required")

    qos_type = config.get("qos_type")
    if not qos_type:
        raise ConfigError("qos_type is required")

    clusters = []
    for i, cn in enumerate(nfs_clusters):
        clusters.append(
            {
                "name": cn,
                "cephfs_volume": cephfs_volumes[i % len(cephfs_volumes)],
            }
        )

    return {
        "clusters": clusters,
        "num_exports_per_cluster": int(
            config.get("num_exports_per_cluster", config.get("num_exports", 3))
        ),
        "subvol_group": config.get("subvol_group", "ganeshagroup"),
        "export_prefix": config.get("nfs_export_prefix", "/export_qos"),
        "mount_prefix": config.get("nfs_mount_prefix", "/mnt/nfs_qos"),
        "qos_type": qos_type,
        "control": config.get("control", "bandwidth_control"),
        "operation": config.get("operation", "none"),
        "port": str(config.get("port", "2049")),
        "fault_tolerance": float(config.get("fault_tolerance", 0.10)),
        "fio": {
            "bs": str(config.get("fio_bs", "1M")),
            "iodepth": str(config.get("fio_iodepth", 32)),
            "numjobs": str(config.get("fio_numjobs", 4)),
            "size": str(config.get("fio_size", "256M")),
            "runtime": str(config.get("fio_runtime", 30)),
        },
    }


def _collect_qos_bw_params(config):
    """Return a dict of bandwidth QoS parameters present in config."""
    return {
        k: config[k]
        for k in (
            "max_export_write_bw",
            "max_export_read_bw",
            "max_client_write_bw",
            "max_client_read_bw",
            "max_export_combined_bw",
            "max_client_combined_bw",
        )
        if k in config
    }


def _build_assignments(clients, cluster_specs, subvol_map, nfs_ips, cfg):
    """Build a flat list of mount assignments with round-robin client mapping.

    Each entry contains all info needed for mount, IO, validation,
    and cleanup.
    """
    assignments = []
    idx = 0
    for cluster in cluster_specs:
        cn = cluster["name"]
        for i, sv in enumerate(subvol_map[cn]):
            assignments.append(
                {
                    "client": clients[idx % len(clients)],
                    "cluster_name": cn,
                    "cephfs_volume": cluster["cephfs_volume"],
                    "export_path": f"{cfg['export_prefix']}_{cn}_{i}",
                    "mount_point": f"{cfg['mount_prefix']}_{cn}_{i}",
                    "nfs_server_ip": nfs_ips[cn],
                    "subvol_name": sv["name"],
                    "subvol_path": sv["path"],
                }
            )
            idx += 1
    return assignments


def _log_assignment_table(assignments):
    """Log the assignment table for debugging."""
    log.info(f"Assignment table ({len(assignments)} mount(s)):")
    log.info(
        f"  {'#':>3}  {'Client':<15} {'Cluster':<12} "
        f"{'Export':<30} {'Mount':<30} {'NFS IP':<16}"
    )
    for i, a in enumerate(assignments):
        log.info(
            f"  {i:>3}  {a['client'].hostname:<15} "
            f"{a['cluster_name']:<12} {a['export_path']:<30} "
            f"{a['mount_point']:<30} {a['nfs_server_ip']:<16}"
        )


# ---------------------------------------------------------------------------
# Operation dispatch (Phase 7 actions between QoS enable and IO)
# All operations are placeholders for now -- will be implemented in next PR.
# ---------------------------------------------------------------------------


def _op_not_implemented(name, description):
    """Factory for placeholder operations (future PRs)."""

    def _handler(*args, **kwargs):
        raise OperationFailedError(
            f"Operation '{name}' not yet implemented. {description}"
        )

    _handler.__doc__ = f"[PLACEHOLDER] {description}"
    return _handler


_OPERATIONS = {
    "none": lambda *a, **kw: log.info("  Operation: none (skipped)"),
    "restart": _op_not_implemented(
        "restart",
        "Restart NFS services on all clusters and verify QoS persists. "
        "Use rados_obj.restart_daemon_services or ceph orch restart.",
    ),
    "daemon_kill": _op_not_implemented(
        "daemon_kill",
        "Kill NFS daemon via rados_obj.crash_ceph_daemon or "
        "rados_obj.change_daemon_systemctl_state, verify QoS survives.",
    ),
    "failover": _op_not_implemented(
        "failover",
        "Simulate node failure via reboot, verify VIP migration. "
        "Use perform_failover() from tests/nfs/nfs_operations.py.",
    ),
    "redeploy": _op_not_implemented(
        "redeploy",
        "Redeploy NFS service via Ceph(client).orch.redeploy, verify QoS persists.",
    ),
    "config_update": _op_not_implemented(
        "config_update",
        "Change QoS params during active IO.",
    ),
    "disable_reenable": _op_not_implemented(
        "disable_reenable",
        "Disable then re-enable QoS, verify enforcement resumes.",
    ),
    "export_add_remove": _op_not_implemented(
        "export_add_remove",
        "Add/remove exports during active QoS enforcement.",
    ),
}


# ---------------------------------------------------------------------------
# Scenario: cluster_bandwidth  (IMPLEMENTED)
# ---------------------------------------------------------------------------


def _scenario_cluster_bandwidth(config, ceph_cluster):
    """Cluster-level bandwidth QoS across one or more NFS clusters.

    For each configured NFS cluster the test creates subvolume-backed
    exports, mounts them on clients via round-robin, then iterates
    through one or more bandwidth limits (``bw_limits`` config key).
    Each iteration enables QoS, runs concurrent fio IO, and collects
    results.  A consolidated scaling report is printed at the end.
    """
    cfg = _parse_multi_cluster_config(config)
    cluster_specs = cfg["clusters"]
    cluster_names = [c["name"] for c in cluster_specs]

    clients = ceph_cluster.get_nodes("client")
    if not clients:
        raise ConfigError("No client nodes found")

    bw_iterations = _build_bw_iterations(config)
    bw_labels = []
    for it in bw_iterations:
        label = (
            it.get("max_export_write_bw")
            or it.get("max_client_write_bw")
            or it.get("max_export_combined_bw")
            or it.get("max_client_combined_bw")
            or "?"
        )
        bw_labels.append(label)

    total_exports = len(cluster_specs) * cfg["num_exports_per_cluster"]
    fio = cfg["fio"]
    log.info(
        f"\n{'=' * 70}\n"
        f"  QoS 3AZ Test: cluster_bandwidth\n"
        f"  Clusters:        {cluster_names}\n"
        f"  CephFS:          {[c['cephfs_volume'] for c in cluster_specs]}\n"
        f"  Exports/cluster: {cfg['num_exports_per_cluster']} "
        f"(total: {total_exports})\n"
        f"  Clients:         {[c.hostname for c in clients]} "
        f"(count: {len(clients)})\n"
        f"  QoS type:        {cfg['qos_type']}\n"
        f"  Control:         {cfg['control']}\n"
        f"  Operation:       {cfg['operation']}\n"
        f"  Fault tolerance: {cfg['fault_tolerance'] * 100:.0f}%\n"
        f"  Subvol group:    {cfg['subvol_group']}\n"
        f"  BW limits:       {bw_labels} ({len(bw_iterations)} iteration(s))\n"
        f"  fio params:\n"
        f"    bs={fio['bs']}  iodepth={fio['iodepth']}  "
        f"numjobs={fio['numjobs']}  size={fio['size']}  "
        f"runtime={fio['runtime']}s\n"
        f"{'=' * 70}"
    )

    driver_client = clients[0]
    log.info(f"Driver client (runs Ceph CLI commands): {driver_client.hostname}")

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    nfs_obj = Ceph(driver_client).nfs
    subvol_map = {}
    nfs_ips = {}
    assignments = []

    try:
        # Phase 0: Install fio on all client nodes
        _log_phase(0, "Install fio on client nodes")
        _install_fio(clients)

        # Phase 1: Resolve NFS server IPs
        _log_phase(1, "Resolve NFS server IPs")
        for cn in cluster_names:
            nfs_ips[cn] = _get_nfs_server_ip(driver_client, cn)

        # Phase 2: Create subvolumes
        _log_phase(2, "Create CephFS subvolumes")
        for cluster in cluster_specs:
            subvol_map[cluster["name"]] = _create_subvolumes_for_cluster(
                driver_client,
                cluster["cephfs_volume"],
                cluster["name"],
                cfg["num_exports_per_cluster"],
                cfg["subvol_group"],
            )

        # Phase 3: Build assignments (round-robin client mapping)
        _log_phase(3, "Build round-robin client assignments")
        assignments = _build_assignments(
            clients, cluster_specs, subvol_map, nfs_ips, cfg
        )
        _log_assignment_table(assignments)

        # Phase 4: Create exports using subvolume paths
        _log_phase(4, f"Create NFS exports ({len(assignments)} total)")
        for a in assignments:
            _create_export(
                driver_client,
                a["cephfs_volume"],
                a["cluster_name"],
                a["export_path"],
                cephfs_path=a["subvol_path"],
            )
        log.info(f"  All {len(assignments)} export(s) created")

        log.info("  Export details per cluster:")
        for cn in cluster_names:
            for suffix in ("--detailed", ""):
                try:
                    out, _ = driver_client.exec_command(
                        sudo=True,
                        cmd=f"ceph nfs export ls {cn} {suffix}".strip(),
                        timeout=30,
                    )
                    log.info(f"    {cn}: {out.strip()}")
                    break
                except Exception as e:
                    if not suffix:
                        log.warning(f"    {cn}: could not list exports: {e}")

        # Phase 5: Mount all assignments
        _log_phase(5, f"Mount exports on clients ({len(assignments)} mounts)")
        for a in assignments:
            _mount_export(
                a["client"],
                a["nfs_server_ip"],
                a["export_path"],
                a["mount_point"],
                cfg["port"],
            )
            sleep(1)
        log.info(f"  All {len(assignments)} mount(s) complete")

        _log_phase("5b", "Verify mounts and NFS daemon health")
        _verify_mounts(assignments, driver_client, cluster_names)

        # Validate operation handler once before iteration loop
        op_handler = _OPERATIONS.get(cfg["operation"])
        if not op_handler:
            valid = ", ".join(sorted(_OPERATIONS.keys()))
            raise ConfigError(f"Unknown operation '{cfg['operation']}'. Valid: {valid}")

        # Phase 6-8: Iterate through bandwidth limits
        all_iteration_results = []
        for iter_idx, bw_params in enumerate(bw_iterations):
            bw_label = bw_labels[iter_idx]
            iter_hdr = f"BW iteration {iter_idx + 1}/{len(bw_iterations)}: {bw_label}"
            _log_phase(
                f"6.{iter_idx + 1}",
                f"Enable QoS ({cfg['qos_type']}) — {iter_hdr}",
            )

            log.info(f"  QoS params: {bw_params}")
            for cn in cluster_names:
                enable_disable_qos_for_cluster(
                    enable_flag=True,
                    ceph_cluster_nfs_obj=nfs_obj.cluster,
                    cluster_name=cn,
                    qos_type=cfg["qos_type"],
                    operation=cfg["control"],
                    **bw_params,
                )
                qos_state = nfs_obj.cluster.qos.get(cluster_id=cn, format="json")
                log.info(f"  QoS enabled on {cn}: {qos_state}")

            _log_export_qos_details(driver_client, cluster_names, assignments)

            log.info("  Settling 10s for QoS enforcement to activate...")
            sleep(10)

            # Operation (none by default)
            _log_phase(
                f"7.{iter_idx + 1}",
                f"Operation: {cfg['operation']} — {iter_hdr}",
            )
            op_handler(driver_client, nfs_obj, cluster_names, cfg["qos_type"])

            # Pre-IO QoS re-verification
            log.info("  Re-verifying QoS is active before IO...")
            for cn in cluster_names:
                try:
                    qos_check = nfs_obj.cluster.qos.get(cluster_id=cn, format="json")
                    log.info(f"    {cn} QoS state: {qos_check}")
                except Exception as e:
                    log.warning(f"    {cn} QoS re-check failed: {e}")

            # Concurrent fio IO
            _log_phase(
                f"8.{iter_idx + 1}",
                f"Concurrent fio IO — {iter_hdr} "
                f"({len(assignments)} threads, "
                f"bs={fio['bs']} iodepth={fio['iodepth']} "
                f"numjobs={fio['numjobs']} runtime={fio['runtime']}s)",
            )
            with ThreadPoolExecutor(max_workers=len(assignments)) as executor:
                futures = [
                    executor.submit(
                        _fio_client_io,
                        a["client"],
                        a["mount_point"],
                        fio,
                    )
                    for a in assignments
                ]
                results = [f.result() for f in futures]

            log.info(f"\n  IO Results ({bw_label}):")
            log.info(
                f"  {'#':>3}  {'Client':<15} {'Cluster':<12} "
                f"{'Write MiB/s':>12} {'Read MiB/s':>12} "
                f"{'W-IOPS':>10} {'R-IOPS':>10}"
            )
            for i, res in enumerate(results):
                a = assignments[i]
                log.info(
                    f"  {i:>3}  {a['client'].hostname:<15} "
                    f"{a['cluster_name']:<12} "
                    f"{res['write_mbps']:>12.3f} {res['read_mbps']:>12.3f} "
                    f"{res['write_iops']:>10.1f} {res['read_iops']:>10.1f}"
                )

            all_iteration_results.append(
                {
                    "bw_limit": bw_label,
                    "bw_params": bw_params,
                    "results": results,
                }
            )

            # Disable QoS before next iteration (or before report)
            log.info(f"  Disabling QoS after iteration {iter_idx + 1}...")
            for cn in cluster_names:
                try:
                    enable_disable_qos_for_cluster(
                        enable_flag=False,
                        ceph_cluster_nfs_obj=nfs_obj.cluster,
                        cluster_name=cn,
                        operation=cfg["control"],
                    )
                except Exception as e:
                    log.warning(f"    QoS disable on {cn}: {e}")

            if iter_idx < len(bw_iterations) - 1:
                log.info("  Settling 5s before next iteration...")
                sleep(5)

        # Phase 9: Consolidated report
        _log_phase(9, "QoS Scaling Report")
        violations = _print_qos_report(all_iteration_results, assignments, cfg)

        if violations > 0:
            raise OperationFailedError(
                f"QoS validation failed: {violations} violation(s) detected. "
                f"See report above."
            )

        log.info("\n  *** TEST PASSED ***")
        return 0

    except Exception as e:
        log.error(f"\n  *** TEST FAILED: {e} ***")
        return 1

    finally:
        _log_cluster_state(driver_client, rados_obj, cluster_specs, ceph_cluster)
        _cleanup(
            driver_client,
            assignments,
            subvol_map,
            cluster_specs,
            nfs_obj,
            cfg,
        )


def _log_cluster_state(driver_client, rados_obj, cluster_specs, ceph_cluster=None):
    """Print cluster and NFS state before cleanup.

    This runs first in the finally block so that the full cluster state
    is captured while exports and mounts are still active.
    """
    cluster_names = [c["name"] for c in cluster_specs]
    log.info(
        "\n\n" + "=" * 70 + "\n"
        "  CLUSTER & NFS STATE (pre-cleanup snapshot)\n" + "=" * 70
    )

    log.info("\n  [1/3] Cluster health...")
    try:
        rados_obj.log_cluster_health()
    except Exception as e:
        log.debug(f"    Could not log cluster health: {e}")

    log.info("\n  [2/3] NFS diagnostics...")
    _log_nfs_diagnostics(driver_client, cluster_names, ceph_cluster)

    log.info("\n  [3/3] Checking for crashes...")
    try:
        rados_obj.check_crash_status(check_logs=False)
    except Exception as e:
        log.debug(f"    Crash status check failed: {e}")

    log.info("=" * 70)


def _cleanup(
    driver_client,
    assignments,
    subvol_map,
    cluster_specs,
    nfs_obj,
    cfg,
):
    """Non-destructive cleanup: unmount, delete exports, remove subvolumes,
    disable QoS."""
    log.info("\n\n" + "=" * 70 + "\n" "  RESOURCE CLEANUP\n" + "=" * 70)

    # 1. Unmount all assignments
    log.info(f"\n  [1/5] Unmounting {len(assignments)} mount(s)...")
    for a in assignments:
        log.info(f"    Unmounting {a['mount_point']} on {a['client'].hostname}")
        _unmount_and_cleanup(a["client"], a["mount_point"])

    # 2. Delete exports
    log.info(f"\n  [2/5] Deleting {len(assignments)} export(s)...")
    for a in assignments:
        try:
            nfs_obj.export.delete(a["cluster_name"], a["export_path"])
            log.info(f"    Deleted {a['export_path']} on {a['cluster_name']}")
        except Exception as e:
            log.warning(
                f"    Failed to delete export {a['export_path']} on "
                f"{a['cluster_name']}: {e}"
            )

    # 3. Delete subvolumes
    sv_count = sum(len(svs) for svs in subvol_map.values())
    log.info(f"\n  [3/5] Deleting {sv_count} subvolume(s)...")
    ceph_fs = Ceph(driver_client).fs
    for cluster in cluster_specs:
        cn = cluster["name"]
        for sv in subvol_map.get(cn, []):
            try:
                ceph_fs.sub_volume.rm(
                    cluster["cephfs_volume"],
                    sv["name"],
                    group=cfg["subvol_group"],
                    force=True,
                )
                log.info(
                    f"    Deleted subvolume {sv['name']} from "
                    f"{cluster['cephfs_volume']}"
                )
            except Exception as e:
                log.warning(f"    Failed to delete subvolume {sv['name']}: {e}")

    # 4. Verify remaining exports per cluster
    log.info("\n  [4/5] Verifying export cleanup...")
    for cluster in cluster_specs:
        try:
            remaining = nfs_obj.export.ls(cluster["name"])
            log.info(f"    Exports on {cluster['name']}: {remaining}")
        except Exception:
            pass

    # 5. Disable QoS on each cluster (safety)
    log.info("\n  [5/5] Disabling QoS (safety)...")
    for cluster in cluster_specs:
        cn = cluster["name"]
        try:
            enable_disable_qos_for_cluster(
                enable_flag=False,
                ceph_cluster_nfs_obj=nfs_obj.cluster,
                cluster_name=cn,
                operation=cfg["control"],
            )
            log.info(f"    QoS disabled on {cn}")
        except Exception as e:
            log.debug(f"    QoS disable on {cn} (may already be off): {e}")

    log.info("\n  Cleanup complete\n" + "=" * 70)


# ---------------------------------------------------------------------------
# Placeholder scenarios (NOT YET IMPLEMENTED -- planned for future PRs)
# ---------------------------------------------------------------------------


def _placeholder(scenario_name, description):
    """Factory to create a placeholder handler for an unimplemented scenario."""

    def _handler(config, ceph_cluster):
        log.error(f"[{scenario_name}] Not yet implemented. {description}")
        return 1

    _handler.__doc__ = f"[PLACEHOLDER] {description}"
    _handler.scenario_name = scenario_name
    return _handler


# ---------------------------------------------------------------------------
# Scenario dispatch table
# ---------------------------------------------------------------------------

_SCENARIOS = {
    # ---- Implemented ----
    # Cluster-level separate read/write bandwidth QoS across multiple NFS
    # clusters, exports, and clients.  Supports PerShare, PerClient, and
    # PerShare_PerClient enforcement with configurable bandwidth iterations.
    "cluster_bandwidth": _scenario_cluster_bandwidth,
    # ---- Cluster-level placeholders ----
    "cluster_bandwidth_combined": _placeholder(
        "cluster_bandwidth_combined",
        "Cluster-level combined read+write bandwidth QoS across multiple "
        "NFS clusters, exports, and clients. Uses max_export_combined_bw / "
        "max_client_combined_bw instead of separate read/write limits.",
    ),
    "cluster_iops": _placeholder(
        "cluster_iops",
        "Cluster-level IOPS control across multiple NFS clusters, exports, "
        "and clients. Uses ops_control instead of bandwidth_control. "
        "Validates per-export and per-client IOPS ceilings under concurrent IO.",
    ),
    # ---- Export-level placeholders ----
    "export_bw_override": _placeholder(
        "export_bw_override",
        "Export-level bandwidth QoS overriding cluster defaults across "
        "multiple NFS clusters and exports. Enable cluster-level QoS, then "
        "set different limits on individual exports per cluster. Verify "
        "export-level limits take precedence while other exports inherit.",
    ),
    "export_bw_combined": _placeholder(
        "export_bw_combined",
        "Export-level combined bandwidth QoS across multiple NFS clusters "
        "and exports. Uses max_export_combined_bw / max_client_combined_bw "
        "at the export level with concurrent multi-client IO.",
    ),
    "export_iops": _placeholder(
        "export_iops",
        "Export-level IOPS control across multiple NFS clusters and exports. "
        "Enable per-export IOPS limits and validate enforcement under "
        "concurrent multi-client IO load.",
    ),
    "export_qos_inheritance": _placeholder(
        "export_qos_inheritance",
        "QoS inheritance lifecycle across multiple NFS clusters and exports. "
        "Override cluster defaults on specific exports, disable the overrides, "
        "verify re-inheritance of cluster defaults while other exports and "
        "clients remain active.",
    ),
    # ---- 3 AZ / operational placeholders ----
    "qos_failover": _placeholder(
        "qos_failover",
        "QoS persistence during NFS site/node failure across multiple "
        "NFS clusters in a 3 AZ setup. Enable QoS on all clusters, "
        "simulate node failure, verify QoS enforcement survives on "
        "remaining exports and clients.",
    ),
    "cross_site_client": _placeholder(
        "cross_site_client",
        "Cross-datacenter client access with QoS enforcement across "
        "multiple NFS clusters. Clients in DC1 mount exports from nfs-DC2 "
        "and nfs-DC3, verify bandwidth/IOPS limits are enforced despite "
        "cross-site latency on all exports.",
    ),
    "qos_post_upgrade": _placeholder(
        "qos_post_upgrade",
        "QoS enforcement after cluster upgrade across multiple NFS clusters, "
        "exports, and clients. Enable QoS before upgrade, upgrade the cluster, "
        "verify QoS settings and enforcement survive on all clusters.",
    ),
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster, **kw):
    """Dispatch to the appropriate QoS test scenario.

    The ``test_scenario`` config key selects which scenario to run.
    Defaults to ``cluster_bandwidth`` if not specified.

    See module docstring for the full list of available scenarios.
    """
    config = kw.get("config", {})
    if not config:
        raise ConfigError("No config provided")

    scenario = config.get("test_scenario", "cluster_bandwidth")
    handler = _SCENARIOS.get(scenario)

    if not handler:
        valid = ", ".join(sorted(_SCENARIOS.keys()))
        log.error(f"Unknown test_scenario '{scenario}'. Valid options: {valid}")
        return 1

    log.info(f"=== QoS 3AZ Test Scenario: {scenario} ===")
    return handler(config, ceph_cluster)
