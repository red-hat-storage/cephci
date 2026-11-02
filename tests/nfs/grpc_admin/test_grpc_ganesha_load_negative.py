"""
gRPC load and resource-exhaustion negative tests.

Loops concurrent grpcurl calls to verify the daemon stays responsive.
"""

import concurrent.futures
import time

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    ensure_subvolume_group,
    grpc_invoke_kwargs,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    setup_grpc_transport,
    verify_grpc_port_listening,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def _spam_list_services(client, target, iterations, kw_tls):
    """Fire *iterations* list calls; return count of successes."""
    successes = 0
    for _ in range(iterations):
        ok, _ = list_grpc_services(client, target, **kw_tls)
        if ok:
            successes += 1
    return successes


def run(ceph_cluster, **kw):
    """
    Load operations (``operation`` config key):

        verify_resource_exhaustion — concurrent grpcurl list; daemon must stay up
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    grpc_port = int(config.get("grpc_port", DEFAULT_GRPC_PORT))
    iterations = int(config.get("iterations", 50))
    workers = int(config.get("workers", 5))

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
            clients=[client],
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

        if operation == "verify_resource_exhaustion":
            per_worker = max(1, iterations // workers)
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(_spam_list_services, client, target, per_worker, kw_tls)
                    for _ in range(workers)
                ]
                results = [f.result(timeout=600) for f in futures]
            total_ok = sum(results)
            log.info(
                "Load test: %s/%s list calls succeeded across %s workers",
                total_ok,
                per_worker * workers,
                workers,
            )
            if not verify_grpc_port_listening(nfs_node, grpc_port):
                raise OperationFailedError(
                    "gRPC port stopped listening after load test"
                )
            min_ok = max(1, int(per_worker * workers * 0.9))
            if total_ok < min_ok:
                raise OperationFailedError(
                    "grpcurl failures under load exceed threshold (%s/%s)"
                    % (total_ok, per_worker * workers)
                )
            time.sleep(2)
            ok, services = list_grpc_services(client, target, **kw_tls)
            if not ok or not services:
                raise OperationFailedError(
                    "gRPC discovery failed after load test recovery"
                )
        else:
            raise ConfigError(f"Unknown load operation: {operation}")

        log.info("TEST PASSED - gRPC load operation=%s target=%s", operation, target)
        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Load test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Load test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
