import json
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from tests.nfs.test_nfs_qos_on_cluster_level_enablement import (
    enable_cluster_ops_control,
    enable_export_ops_control,
    validate_ops_control,
    validate_ops_limit,
    verify_ops_control_settings,
)
from utility.log import Log

log = Log(__name__)


def _measured_iops_from_perf(perf, dd_params):
    """
    Compute measured write/read IOPS from perf dict returned by validate_ops_control.
    IOPS = count / time (seconds).
    Returns (write_iops, read_iops) as floats.
    """
    try:
        count = int(dd_params.get("count", 64))
    except Exception:
        count = 64
    wt = float(perf.get("write_time", 0) or 0.0)
    rt = float(perf.get("read_time", 0) or 0.0)
    write_iops = count / wt if wt > 0 else 0.0
    read_iops = count / rt if rt > 0 else 0.0
    return write_iops, read_iops


def _require_baseline_higher(baseline_perf, measured_perf, dd_params, level_name):
    """Require baseline IOPS strictly greater than measured IOPS for both write and read."""
    b_w_iops, b_r_iops = _measured_iops_from_perf(baseline_perf, dd_params)
    m_w_iops, m_r_iops = _measured_iops_from_perf(measured_perf, dd_params)
    log.info(
        "%s baseline IOPS write=%.2f read=%.2f | measured IOPS write=%.2f read=%.2f",
        level_name,
        b_w_iops,
        b_r_iops,
        m_w_iops,
        m_r_iops,
    )
    if not (b_w_iops > m_w_iops and b_r_iops > m_r_iops):
        raise OperationFailedError(
            f"{level_name} validation failed: baseline IOPS not greater than measured under ops control"
        )


def run(ceph_cluster, **kw):
    """Test NFS ops control at cluster and export levels (ops-only checks)"""
    config = kw.get("config")
    if not config:
        raise ConfigError("Config not found")

    client = None
    nfs_export_path = None
    nfs_mount = None
    try:
        # Nodes and clients
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        clients = ceph_cluster.get_nodes("client")
        if not nfs_nodes:
            raise OperationFailedError("No NFS nodes found in cluster")
        nfs_node = nfs_nodes[0]

        no_clients = int(config.get("clients", "1"))
        if no_clients > len(clients):
            raise ConfigError("The test requires more clients than available")
        clients = clients[:no_clients]
        client = clients[0]

        # Params
        cluster_name = config.get("cluster_name", "cephfs-nfs")
        fs_name = config.get("cephfs_volume", "cephfs")
        nfs_export = "/export_0"
        nfs_mount = "/mnt/nfs"
        fs = "cephfs"
        qos_type = config.get("qos_type")
        if not qos_type:
            raise ConfigError("qos_type missing in config")
        dd_params = config.get("dd_parameters") or {"block_size": "4K", "count": 16384}

        # Setup NFS cluster
        setup_nfs_cluster(
            clients=clients,
            nfs_server=nfs_node.hostname,
            port=config.get("port", "2049"),
            version=config.get("nfs_version", "4.2"),
            nfs_name=cluster_name,
            nfs_mount=nfs_mount,
            fs_name=fs_name,
            export=nfs_export,
            fs=fs,
            ceph_cluster=ceph_cluster,
        )

        log.info("=" * 80)
        log.info("STEP 1: Measuring baseline performance (no ops control)...")
        baseline = validate_ops_control(
            client=client,
            nfs_mount=nfs_mount,
            file_name="baseline.txt",
            dd_params=dd_params,
        )
        log.info("Baseline Results: %s", baseline)

        # Step 2: cluster-level ops control
        log.info("=" * 80)
        log.info("STEP 2: Enabling cluster-level ops control...")
        cluster_ops = config.get("cluster_ops", {}) or {}
        log.info("Cluster ops config: %s", cluster_ops)
        if cluster_ops:
            enable_cluster_ops_control(client, cluster_name, qos_type, cluster_ops)
            cluster_settings, _ = verify_ops_control_settings(client, cluster_name)
            log.info("Cluster settings verified: %s", cluster_settings)

            cluster_test = validate_ops_control(
                client=client,
                nfs_mount=nfs_mount,
                file_name="cluster_ops.txt",
                dd_params=dd_params,
            )
            log.info("Cluster-level Results: %s", cluster_test)

            # validate ops limit calculation (IOPS-based) if applicable
            if (
                qos_type
                and qos_type.lower() in ["pershare", "pershare_perclient"]
                and cluster_ops.get("max_export_iops")
            ):
                matches, calc_limit = validate_ops_limit(
                    cluster_test["write_time"],
                    cluster_ops.get("max_export_iops"),
                )
                if not matches:
                    raise OperationFailedError(
                        f"Cluster ops limit validation failed: calculated={calc_limit} "
                        f"expected={cluster_ops.get('max_export_iops')}"
                    )
                log.info("Cluster ops_limit calculation matches expected")

            # require baseline IOPS higher than cluster-measured IOPS
            _require_baseline_higher(baseline, cluster_test, dd_params, "Cluster-level")

        else:
            log.info(
                "No cluster-level ops config provided; skipping cluster ops checks"
            )

        # Step 3: export-level ops control
        export_ops = config.get("export_ops")
        export_test = None
        if export_ops:
            log.info("=" * 80)
            log.info("STEP 3: Enabling export-level ops control...")
            nfs_export_path = f"{nfs_export}_0"

            enable_export_ops_control(
                client,
                cluster_name,
                nfs_export_path,
                qos_type,
                export_ops,
            )

            cluster_settings, export_settings = verify_ops_control_settings(
                client,
                cluster_name,
                nfs_export_path,
            )
            log.info("Export settings verified: %s", export_settings)

        export_test = validate_ops_control(
            client=client,
            nfs_mount=nfs_mount,
            file_name="export_ops.txt",
            dd_params=dd_params,
        )
        log.info("Export-level Results: %s", export_test)
        _require_baseline_higher(baseline, export_test, dd_params, "Export-level")

        if config.get("operation") == "restart":
            log.info("=" * 80)
            log.info("STEP 4: Restarting service and verifying persistence...")
            data = json.loads(Ceph(client).orch.ls(format="json"))
            service_name = next(
                (
                    x["service_name"]
                    for x in data
                    if x.get("service_id") == cluster_name
                ),
                None,
            )
            if service_name:
                Ceph(client).orch.restart(service_name)
                sleep(10)
                # avoid overly long line by assigning the export argument to a temp variable
                post_verify_export = nfs_export_path if export_ops else None
                post_cluster_settings, post_export_settings = (
                    verify_ops_control_settings(
                        client,
                        cluster_name,
                        post_verify_export,
                    )
                )
                if post_cluster_settings != (
                    cluster_settings if cluster_ops else post_cluster_settings
                ):
                    raise OperationFailedError(
                        "Cluster ops control settings changed after restart"
                    )
                if export_ops and post_export_settings != export_settings:
                    raise OperationFailedError(
                        "Export ops control settings changed after restart"
                    )

                # verify ops still effective
                post_restart_test = validate_ops_control(
                    client=client,
                    nfs_mount=nfs_mount,
                    file_name="post_restart.txt",
                    dd_params=dd_params,
                )
                log.info("Post-restart Results: %s", post_restart_test)
                _require_baseline_higher(
                    baseline, post_restart_test, dd_params, "Post-restart"
                )
            else:
                log.info(
                    "Service for cluster not found; skipping restart persistence checks"
                )

        log.info("=" * 80)
        log.info("NFS ops control ops-only test completed successfully")
        return 0

    except Exception as e:
        log.error("NFS ops control test failed: %s", str(e))
        return 1

    finally:
        log.info("Cleanup in progress")
        if client:
            # attempt to remove generated test files, ignore failures
            files_to_remove = [
                "baseline.txt",
                "cluster_ops.txt",
                "export_ops.txt",
                "post_restart.txt",
            ]
            for fname in files_to_remove:
                try:
                    client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}/{fname}")
                except Exception:
                    log.debug("failed to remove %s", fname)
            # prefer the export-level path if set
            cleanup_cluster(client, nfs_mount, cluster_name, nfs_export)
