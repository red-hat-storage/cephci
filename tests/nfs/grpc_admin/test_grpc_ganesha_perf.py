"""
gRPC API performance tests for NFS-Ganesha on cephadm NFS.

Measures latency of gRPC admin/export RPCs under light client load.
"""

import statistics
import time
from shlex import quote

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    ensure_subvolume_group,
    first_export_id,
    get_client_ids,
    get_grace_period,
    grpc_invoke_kwargs,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    remove_export,
    resolve_grpc_target,
    setup_grpc_transport,
    show_clients,
    show_exports,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def _timed_call(fn, *args, **kwargs):
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, result


def _percentile(samples, pct):
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = int(len(ordered) * pct / 100)
    idx = min(idx, len(ordered) - 1)
    return ordered[idx]


def _mount_extra_clients(
    clients, nfs_server, nfs_mount, nfs_export, nfs_version, nfs_port
):
    """Mount NFS on additional clients to increase ShowClients cardinality."""
    # setup_nfs_cluster creates exports as ``{export}_0`` (e.g. /export_0).
    export_path = nfs_export if nfs_export.endswith("_0") else f"{nfs_export}_0"
    for client in clients[1:]:
        client.exec_command(sudo=True, cmd=f"mkdir -p {quote(nfs_mount)}")
        mount_cmd = (
            f"mount -t nfs -o vers={nfs_version},proto=tcp,port={nfs_port} "
            f"{nfs_server}:{export_path} {quote(nfs_mount)}"
        )
        client.exec_command(sudo=True, cmd=mount_cmd)
        client.exec_command(
            sudo=True,
            cmd=f"touch {quote(nfs_mount)}/perf_client_{client.hostname}",
        )


def run(ceph_cluster, **kw):
    """
    Performance operations (``operation`` config key):

        admin_perf_under_load — list_services / GetGracePeriod latency
        stats_collection_vs_io — poll GetClientIds while light I/O runs
        show_clients_at_scale — ClientMgr.ShowClients latency with mounts
        export_ops_perf — ShowExports / RemoveExport loop latency
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = int(config.get("port", 2049))
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    grpc_port = int(config.get("grpc_port", DEFAULT_GRPC_PORT))
    samples = int(config.get("samples", 20))
    p99_max_ms = float(config.get("p99_max_ms", 5000))

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    target = resolve_grpc_target(config, nfs_ip)
    installers = ceph_cluster.get_nodes("installer")
    installer = installers[0] if installers else None

    try:
        install_grpcurl(client)
        open_grpc_firewall(nfs_node, grpc_port)
        ensure_subvolume_group(client, fs_name, ceph_cluster=ceph_cluster)
        setup_nfs_cluster(
            clients=clients[:1],
            nfs_server=nfs_server,
            port=nfs_port,
            version=nfs_version,
            nfs_name=nfs_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )
        transport = setup_grpc_transport(config, [client], installer, nfs_name)
        kw_tls = grpc_invoke_kwargs(transport)

        if operation == "admin_perf_under_load":
            latencies = []
            for _ in range(samples):
                ms, (ok, _) = _timed_call(list_grpc_services, client, target, **kw_tls)
                if ok:
                    latencies.append(ms)
                ms, (ok, _, _) = _timed_call(get_grace_period, client, target, **kw_tls)
                if ok:
                    latencies.append(ms)
            if len(latencies) < samples:
                raise OperationFailedError("Insufficient successful RPC samples")
            p50 = statistics.median(latencies)
            p99 = _percentile(latencies, 99)
            log.info(
                "Admin perf: p50=%.1fms p99=%.1fms (n=%s)", p50, p99, len(latencies)
            )
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"p99 latency {p99:.1f}ms exceeds threshold {p99_max_ms}ms"
                )

        elif operation == "stats_collection_vs_io":
            _mount_extra_clients(
                clients, nfs_server, nfs_mount, nfs_export, nfs_version, nfs_port
            )
            latencies = []
            for i in range(samples):
                client.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/zero of={nfs_mount}/perf_io_{i} bs=4k count=16 "
                    "oflag=dsync 2>/dev/null",
                    check_ec=False,
                )
                ms, (ok, _, _) = _timed_call(get_client_ids, client, target, **kw_tls)
                if ok:
                    latencies.append(ms)
            if not latencies:
                raise OperationFailedError("No successful GetClientIds during I/O")
            p99 = _percentile(latencies, 99)
            log.info("Stats vs I/O: p99 GetClientIds=%.1fms", p99)
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"GetClientIds p99 {p99:.1f}ms too high during I/O"
                )

        elif operation == "show_clients_at_scale":
            num_clients = min(len(clients), int(config.get("clients", 3)))
            _mount_extra_clients(
                clients[:num_clients],
                nfs_server,
                nfs_mount,
                nfs_export,
                nfs_version,
                nfs_port,
            )
            latencies = []
            for _ in range(samples):
                ms, (ok, mgr_clients, _) = _timed_call(
                    show_clients, client, target, **kw_tls
                )
                if ok:
                    latencies.append(ms)
                    log.info(
                        "ShowClients returned %s clients in %.1fms",
                        len(mgr_clients),
                        ms,
                    )
            if not latencies:
                raise OperationFailedError("ClientMgr.ShowClients failed at scale")
            p99 = _percentile(latencies, 99)
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"ShowClients-scale p99 {p99:.1f}ms exceeds {p99_max_ms}ms"
                )

        elif operation == "export_ops_perf":
            # Measure ShowExports + RemoveExport using temporary ceph-created exports.
            latencies = []
            loops = int(config.get("export_loops", min(samples, 5)))
            for i in range(loops):
                exp_path = f"{nfs_export}_grpc_perf_{i}"
                Ceph(client).nfs.export.create(
                    fs_name=fs_name,
                    nfs_name=nfs_name,
                    nfs_export=exp_path,
                    fs=fs,
                )
                time.sleep(2)
                ms_show, (ok, exports, _) = _timed_call(
                    show_exports, client, target, **kw_tls
                )
                if not ok:
                    raise OperationFailedError(
                        "ShowExports failed during export_ops_perf"
                    )
                latencies.append(ms_show)
                # Match the temporary export — never remove an unmatched primary export.
                export_id = None
                for entry in exports:
                    pseudo = entry.get("pseudoPath") or entry.get("pseudo_path") or ""
                    if exp_path in pseudo:
                        export_id = first_export_id([entry])
                        break
                if export_id is None:
                    log.warning(
                        "Could not match export %s in ShowExports; skipping RemoveExport",
                        exp_path,
                    )
                    continue
                ms_rm, (ok, _) = _timed_call(
                    remove_export, client, target, export_id, **kw_tls
                )
                if ok:
                    latencies.append(ms_rm)
            if not latencies:
                raise OperationFailedError("No export op latency samples collected")
            p99 = _percentile(latencies, 99)
            log.info("Export ops perf p99=%.1fms (n=%s)", p99, len(latencies))
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"Export ops p99 {p99:.1f}ms exceeds {p99_max_ms}ms"
                )

        else:
            raise ConfigError(f"Unknown perf operation: {operation}")

        log.info("TEST PASSED - gRPC perf operation=%s target=%s", operation, target)
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Perf test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Perf test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
