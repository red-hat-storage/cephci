from datetime import datetime
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from tests.nfs.lib.multi_active import (
    NfsMultiActiveCleanup,
    NfsMultiActiveClient,
    NfsMultiActiveConfig,
    NfsMultiActiveDeploy,
    NfsMultiActiveFailover,
    NfsMultiActiveHaproxy,
    init_crash_monitoring,
    wait_for_ceph_health,
)
from tests.nfs.lib.multi_active.placement import (
    COLOCATION_PLACEMENT,
    resolve_placement_hosts,
)
from tests.nfs.multi_active.test_nfs_multi_active_failover import (
    _assert_post_failover_backends,
    _log_workflow_timings,
    _resolve_failover_plan,
    _run_failover_steps,
)
from utility.log import Log

log = Log(__name__)

from tests.nfs.lib.multi_active.constants import (
    BG_IO_JOIN_TIMEOUT,
    BG_IO_RUNTIME,
    BG_IO_START_DELAY,
    DEPLOY_TIMEOUT,
    EXPORT_NAME,
    HEALTH_CHECK_INTERVAL,
    MARKER_IO_TIMEOUT,
    MIN_NFS_NODES,
    NFS_MOUNT,
    NFS_VERSION,
    POLL_INTERVAL_SEC,
    SERVICE_ID_PREFIX,
    TC4_NFS_COUNT,
)

NFS_SERVICE_ID_PREFIX = SERVICE_ID_PREFIX["colocation"]
NFS_DAEMON_COUNT = TC4_NFS_COUNT
_VIP1 = 1


def _resolve_workflow_vip(config, workflow_cfg):
    """Return suite VIP (``vip`` is 1-based index into config ``vips``)."""
    vips = NfsMultiActiveConfig.get_vips(config)
    vip_slot = workflow_cfg.get("vip", _VIP1)
    if not isinstance(vip_slot, int) or vip_slot < 1:
        raise ConfigError(
            f"Workflow vip slot must be a positive integer, got {vip_slot!r}"
        )
    if vip_slot > len(vips):
        raise ConfigError(
            f"Workflow requests vip{vip_slot} but suite config has "
            f"{len(vips)} VIP(s): {vips}"
        )
    return vips[vip_slot - 1]


_PLACEMENT = COLOCATION_PLACEMENT

_COLOC_PORTS_1 = {
    "port": 2169,
    "monitoring_port": 9507,
    "colocation_ports": [
        {"data_port": 3169, "monitoring_port": 9517, "cluster_qos_port": 32311},
        {"data_port": 4169, "monitoring_port": 9527, "cluster_qos_port": 33311},
        {"data_port": 5169, "monitoring_port": 9537, "cluster_qos_port": 34311},
    ],
}
_COLOC_PORTS_2 = {
    "port": 2269,
    "monitoring_port": 9607,
    "colocation_ports": [
        {"data_port": 3269, "monitoring_port": 9617, "cluster_qos_port": 32411},
        {"data_port": 4269, "monitoring_port": 9627, "cluster_qos_port": 33411},
        {"data_port": 5269, "monitoring_port": 9637, "cluster_qos_port": 34411},
    ],
}

COLOCATION_WORKFLOWS = {
    "same_host_colocation": {
        "tc": "TC-A1",
        "desc": "Deploy colocated NFS daemons (count=4, 2 per host) and validate VIP IO",
        "deploy_only": True,
        "vip": 2,
        "nfs_count": 4,
        "placement": _PLACEMENT,
        "colocation": _COLOC_PORTS_1,
        "ingress_ports": (2249, 9349),
    },
    "colocation_nfs_failover": {
        "tc": "TC-C1",
        "desc": (
            "Background IO via VIP while stick-table-pinned colocated NFS backend "
            "host is label-drained to spare"
        ),
        "vip": 1,
        "nfs_count": 4,
        "placement": _PLACEMENT,
        "colocation": _COLOC_PORTS_2,
        "ingress_ports": (2259, 9359),
        "failover": {"target": "nfs"},
    },
    "colocation_ingress_failover": {
        "tc": "TC-C2",
        "desc": (
            "Background IO via VIP while keepalived VIP-holder ingress fails over "
            "with colocated NFS backends"
        ),
        "vip": 2,
        "nfs_count": 4,
        "placement": _PLACEMENT,
        "colocation": {
            "port": 2369,
            "monitoring_port": 9707,
            "colocation_ports": [
                {"data_port": 3369, "monitoring_port": 9717, "cluster_qos_port": 32511},
                {"data_port": 4369, "monitoring_port": 9727, "cluster_qos_port": 33511},
                {"data_port": 5369, "monitoring_port": 9737, "cluster_qos_port": 34511},
            ],
        },
        "ingress_ports": (2269, 9369),
        "failover": {"target": "ingress"},
    },
    "colocation_dual_client_failover": {
        "tc": "TC-C4",
        "desc": (
            "Both clients run background IO on distinct colocation backends; primary client's "
            "backend host is label-drained to spare while peer IO continues"
        ),
        "dual_client_io": True,
        "vip": 1,
        "nfs_count": 4,
        "placement": _PLACEMENT,
        "colocation": {
            "port": 2469,
            "monitoring_port": 9807,
            "colocation_ports": [
                {"data_port": 3469, "monitoring_port": 9817, "cluster_qos_port": 32611},
                {"data_port": 4469, "monitoring_port": 9827, "cluster_qos_port": 33611},
                {"data_port": 5469, "monitoring_port": 9837, "cluster_qos_port": 34611},
            ],
        },
        "ingress_ports": (2279, 9379),
        "failover": {"target": "nfs"},
    },
}


def _log_workflow_start(workflow, step, total):
    wf = COLOCATION_WORKFLOWS[workflow]
    log.info(
        "\n" + "=" * 70 + "\n  %s (%s)\n  %s\n" + "=" * 70,
        f"Workflow {step} of {total}: {workflow}",
        wf.get("tc", workflow),
        wf["desc"],
    )


def _poll_interval(config):
    return int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))


def _deploy_kwargs(config, is_first_workflow):
    poll_interval = _poll_interval(config)
    kwargs = {
        "deploy_timeout": DEPLOY_TIMEOUT,
        "ingress_poll_interval": poll_interval,
        "colocation_validation": True,
    }
    if not is_first_workflow:
        kwargs.update(
            {
                "skip_mgr_enable": True,
            }
        )
    return kwargs


def _resolve_hosts(workflow, config, nfs_nodes):
    """Return (nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host)."""
    wf = COLOCATION_WORKFLOWS[workflow]
    placement = wf["placement"]
    nfs_count = int(config.get("nfs_count", wf.get("nfs_count", NFS_DAEMON_COUNT)))
    nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host = (
        resolve_placement_hosts(
            nfs_nodes,
            placement,
            config,
            nfs_count=nfs_count,
            include_spare=not wf.get("deploy_only"),
        )
    )
    if nfs_count > len(nfs_hosts) * 2:
        raise ConfigError(
            f"nfs_count ({nfs_count}) exceeds colocation capacity for workflow "
            f"{workflow!r} (pool size {len(nfs_hosts)})"
        )
    return nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host


def _build_specs(workflow, nfs_hosts, nfs_count, nfs_service_id, vip, labels, config):
    wf = COLOCATION_WORKFLOWS[workflow]
    coloc = wf["colocation"]
    ingress_frontend_port, ingress_monitor_port = wf["ingress_ports"]
    nfs_spec = NfsMultiActiveConfig.build_nfs_colocation_spec(
        nfs_hosts,
        nfs_count,
        nfs_service_id,
        coloc["port"],
        coloc["monitoring_port"],
        coloc["colocation_ports"],
        label=labels["nfs"],
    )
    ingress_spec = NfsMultiActiveConfig.build_ingress_spec(
        vip,
        nfs_service_id,
        ingress_frontend_port,
        ingress_monitor_port,
        config.get("health_check_interval", HEALTH_CHECK_INTERVAL),
        label=labels["ingress"],
    )
    return nfs_spec, ingress_spec, ingress_frontend_port


def _validate_colocation_cluster(
    deploy, nfs_spec, ingress_spec, nfs_service_id, ingress_hosts, timeout=None
):
    cluster_info = deploy.validator.get_cluster_info(nfs_service_id)
    deploy.validator.assert_colocation_cluster_info(
        cluster_info, nfs_spec, ingress_spec
    )
    deploy.validator.assert_colocation_ports_listening(cluster_info, nfs_spec)
    deploy.validator.assert_orch_ps(
        nfs_spec,
        ingress_spec,
        expected_ingress_hosts=ingress_hosts,
        timeout=timeout,
    )
    return cluster_info


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _run_deploy_workflow(ceph_cluster, config, workflow, is_first_workflow=True):
    """Deploy colocation NFS, mount via VIP, validate stick table and basic IO."""
    client1, client2 = ceph_cluster.get_nodes("client")[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    poll_interval = _poll_interval(config)
    wf = COLOCATION_WORKFLOWS[workflow]

    nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, _spare = _resolve_hosts(
        workflow, config, nfs_nodes
    )
    placement = wf["placement"]
    nfs_service_id = (
        f"{NFS_SERVICE_ID_PREFIX}-{workflow}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = _resolve_workflow_vip(config, wf)
    mounted_clients = []
    export_created = False
    cluster_deployed = False
    labels_applied = False

    log.info("NFS service id: %s, VIP: %s", nfs_service_id, vip)
    log.info(
        "Colocation NFS count=%s pool=%s; ingress pool=%s; ports=%s ingress=%s",
        nfs_count,
        nfs_hosts,
        ingress_hosts,
        wf["colocation"],
        wf["ingress_ports"],
    )

    try:
        NfsMultiActiveConfig.apply_placement_labels(
            client1,
            nfs_nodes,
            placement["nfs"],
            placement["ingress"],
            labels,
        )
        labels_applied = True

        nfs_spec, ingress_spec, ingress_frontend_port = _build_specs(
            workflow, nfs_hosts, nfs_count, nfs_service_id, vip, labels, config
        )
        specs = [nfs_spec, ingress_spec]
        haproxy = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_service_id, info_client=client1
        )
        deploy = NfsMultiActiveDeploy(ceph_cluster, nfs_service_id, vip, client=client1)

        deploy.deploy(
            specs,
            deploy_nodes,
            expected_ingress_hosts=ingress_hosts,
            **_deploy_kwargs(config, is_first_workflow),
        )
        cluster_deployed = True
        haproxy.validate_entries(stick_table_interval=poll_interval)

        Ceph(client1).nfs.export.create(
            fs_name="cephfs",
            nfs_name=nfs_service_id,
            nfs_export=EXPORT_NAME,
            fs="cephfs",
        )
        NfsMultiActiveConfig.wait_until_export_visible(
            client1, nfs_service_id, EXPORT_NAME, interval=poll_interval
        )
        export_created = True

        for client in (client1, client2):
            NfsMultiActiveClient.mount_via_vip(
                client,
                vip,
                NFS_MOUNT,
                EXPORT_NAME,
                str(ingress_frontend_port),
                NFS_VERSION,
            )
            mounted_clients.append(client)
            haproxy.validate_entries(
                mounted_clients=mounted_clients,
                stick_table_interval=poll_interval,
            )

        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir=workflow,
        )
        haproxy.validate_entries(
            mounted_clients=mounted_clients,
            stick_table_interval=poll_interval,
        )
        return 0
    except Exception as e:
        log.error("Colocation deploy workflow failed (%s): %s", workflow, e)
        return 1
    finally:
        try:
            if mounted_clients:
                NfsMultiActiveClient.unmount_and_delete_export(
                    mounted_clients, NFS_MOUNT, nfs_service_id, EXPORT_NAME
                )
            elif export_created:
                Ceph(client1).nfs.export.delete(nfs_service_id, EXPORT_NAME)
        except Exception as e:
            log.warning("Mount/export cleanup failed: %s", e)

        if cluster_deployed:
            try:
                NfsMultiActiveCleanup(ceph_cluster).remove_cluster(nfs_service_id)
            except Exception as e:
                log.warning("NFS cluster cleanup failed: %s", e)

        if labels_applied:
            try:
                NfsMultiActiveConfig.remove_placement_labels(
                    client1,
                    nfs_nodes,
                    placement["nfs"],
                    placement["ingress"],
                    labels,
                )
            except Exception as e:
                log.warning("Placement label cleanup failed: %s", e)


def _run_failover_workflow(ceph_cluster, config, workflow):
    """Deploy colocation NFS, run background IO during label failover, validate recovery."""
    client1, client2 = ceph_cluster.get_nodes("client")[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    poll_interval = _poll_interval(config)
    wf = COLOCATION_WORKFLOWS[workflow]
    placement = wf["placement"]
    failover_cfg = wf["failover"]
    dual_client_io = wf.get("dual_client_io", False)

    nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, labels, spare_host = (
        _resolve_hosts(workflow, config, nfs_nodes)
    )
    nfs_service_id = (
        f"{NFS_SERVICE_ID_PREFIX}-{workflow}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = _resolve_workflow_vip(config, wf)
    nfs_label = labels["nfs"]
    ingress_label = labels["ingress"]

    mounted_clients = []
    export_created = False
    cluster_deployed = False
    labels_applied = False
    primary_io_client = None
    io_sessions = None
    io_thread = None
    io_error_box = None
    io_dir = None
    timings = {}

    log.info(
        "NFS service id: %s, VIP: %s, spare: %s, failover: %s",
        nfs_service_id,
        vip,
        spare_host,
        failover_cfg["target"],
    )

    try:
        NfsMultiActiveConfig.apply_placement_labels(
            client1,
            nfs_nodes,
            placement["nfs"],
            placement["ingress"],
            labels,
        )
        labels_applied = True

        nfs_spec, ingress_spec, ingress_frontend_port = _build_specs(
            workflow, nfs_hosts, nfs_count, nfs_service_id, vip, labels, config
        )
        specs = [nfs_spec, ingress_spec]
        deploy = NfsMultiActiveDeploy(ceph_cluster, nfs_service_id, vip, client=client1)
        haproxy = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_service_id, info_client=client1
        )
        failover = NfsMultiActiveFailover()

        deploy.deploy(
            specs,
            deploy_nodes,
            deploy_timeout=DEPLOY_TIMEOUT,
            expected_ingress_hosts=ingress_hosts,
            colocation_validation=True,
        )
        cluster_deployed = True
        haproxy.validate_entries(stick_table_interval=poll_interval)

        Ceph(client1).nfs.export.create(
            fs_name="cephfs",
            nfs_name=nfs_service_id,
            nfs_export=EXPORT_NAME,
            fs="cephfs",
        )
        NfsMultiActiveConfig.wait_until_export_visible(
            client1, nfs_service_id, EXPORT_NAME, interval=poll_interval
        )
        export_created = True

        for client in (client1, client2):
            NfsMultiActiveClient.mount_via_vip(
                client,
                vip,
                NFS_MOUNT,
                EXPORT_NAME,
                str(ingress_frontend_port),
                NFS_VERSION,
            )
            mounted_clients.append(client)
            haproxy.validate_entries(
                mounted_clients=mounted_clients,
                stick_table_interval=poll_interval,
            )

        (
            io_clients,
            primary_io_client,
            _pinned_backend,
            removed_nfs_host,
            removed_ingress_host,
            expected_ingress,
        ) = _resolve_failover_plan(
            haproxy,
            failover,
            ceph_cluster,
            vip,
            mounted_clients,
            spare_host,
            ingress_hosts,
            failover_cfg["target"],
            dual_client_io=dual_client_io,
        )

        io_runtime = NfsMultiActiveConfig.io_runtime(config, default=BG_IO_RUNTIME)
        if dual_client_io:
            io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
                io_clients,
                NFS_MOUNT,
                config,
                io_subdir=workflow,
                runtime=io_runtime,
            )
            io_dir = next(
                session_io_dir
                for client, _, _, session_io_dir in io_sessions
                if client == primary_io_client
            )
        else:
            io_thread, io_error_box, io_dir = NfsMultiActiveClient.run_background_io(
                primary_io_client,
                NFS_MOUNT,
                config,
                io_subdir=workflow,
                runtime=io_runtime,
            )
        timings["io_start"] = _now()
        sleep(BG_IO_START_DELAY)

        target = failover_cfg["target"]
        removed_nfs_host, removed_ingress_host, expected_ingress = _run_failover_steps(
            target,
            failover,
            deploy,
            ceph_cluster,
            vip,
            client1,
            nfs_spec,
            ingress_spec,
            nfs_service_id,
            deploy_nodes,
            nfs_count,
            nfs_label,
            ingress_label,
            ingress_hosts,
            spare_host,
            DEPLOY_TIMEOUT,
            removed_nfs_host,
            removed_ingress_host,
            expected_ingress,
            config,
            timings=timings,
            dual_client_io=dual_client_io,
            io_sessions=io_sessions,
            io_thread=io_thread,
            io_error_box=io_error_box,
            primary_io_client=primary_io_client,
            io_dir=io_dir,
        )

        cluster_info = _validate_colocation_cluster(
            deploy,
            nfs_spec,
            ingress_spec,
            nfs_service_id,
            expected_ingress,
            timeout=DEPLOY_TIMEOUT,
        )
        haproxy.validate_entries(
            mounted_clients=mounted_clients,
            timings=timings,
            stick_table_interval=poll_interval,
        )

        backend_hostnames = {
            backend.get("hostname")
            for backend in cluster_info.get("backend") or []
            if backend.get("hostname")
        }
        _assert_post_failover_backends(
            target, spare_host, removed_nfs_host, backend_hostnames
        )

        if dual_client_io:
            NfsMultiActiveClient.assert_io_completed_on_clients(
                io_sessions,
                nfs_mount=NFS_MOUNT,
                config=config,
                wait_io_resume_timeout=0,
                join_timeout=BG_IO_JOIN_TIMEOUT,
                timings=timings,
            )
        else:
            NfsMultiActiveClient.assert_io_completed(
                io_thread,
                io_error_box,
                config=config,
                stop_client=primary_io_client,
                io_dir=io_dir,
                nfs_mount=NFS_MOUNT,
                wait_io_resume_timeout=0,
                join_timeout=BG_IO_JOIN_TIMEOUT,
                timings=timings,
            )
        timings["io_end"] = _now()

        marker = f"{io_dir}/post-failover-marker"
        marker_client = next(c for c in mounted_clients if c != primary_io_client)
        marker_client.exec_command(
            sudo=True,
            cmd=(
                f"timeout {MARKER_IO_TIMEOUT} sh -c "
                f"'echo failover-ok-{workflow} > {marker}'"
            ),
            timeout=MARKER_IO_TIMEOUT + 10,
        )
        out, _ = primary_io_client.exec_command(
            sudo=True,
            cmd=f"timeout {MARKER_IO_TIMEOUT} cat {marker}",
            timeout=MARKER_IO_TIMEOUT + 10,
        )
        if "failover-ok" not in (out or ""):
            raise ConfigError(
                "Cross-client marker read failed after colocation failover"
            )

        log.info("Colocation failover workflow %s completed successfully", workflow)
        _log_workflow_timings(timings)
        return 0
    except Exception as e:
        log.error("Colocation failover workflow failed (%s): %s", workflow, e)
        return 1
    finally:
        try:
            if dual_client_io and io_sessions:
                NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
                for _, thread, _, _ in io_sessions:
                    thread.join(timeout=30)
            elif io_thread is not None and io_thread.is_alive() and primary_io_client:
                NfsMultiActiveClient.stop_background_io(
                    primary_io_client, io_dir, config, io_error_box
                )
                io_thread.join(timeout=30)
        except Exception as e:
            log.warning("Background IO stop failed: %s", e)

        try:
            if mounted_clients:
                NfsMultiActiveClient.unmount_and_delete_export(
                    mounted_clients, NFS_MOUNT, nfs_service_id, EXPORT_NAME
                )
            elif export_created:
                Ceph(client1).nfs.export.delete(nfs_service_id, EXPORT_NAME)
        except Exception as e:
            log.warning("Mount/export cleanup failed: %s", e)

        if cluster_deployed:
            try:
                NfsMultiActiveCleanup(ceph_cluster).remove_cluster(nfs_service_id)
            except Exception as e:
                log.warning("NFS cluster cleanup failed: %s", e)

        if labels_applied:
            try:
                NfsMultiActiveConfig.remove_placement_labels(
                    client1,
                    nfs_nodes,
                    placement["nfs"],
                    placement["ingress"],
                    labels,
                )
                for spare_label in (labels["nfs"], labels["ingress"]):
                    client1.exec_command(
                        sudo=True,
                        cmd=f"ceph orch host label rm {spare_host} {spare_label}",
                        check_ec=False,
                    )
            except Exception as e:
                log.warning("Placement label cleanup failed: %s", e)


def _run_workflow(ceph_cluster, config, workflow, step, total, is_first_workflow):
    _log_workflow_start(workflow, step, total)
    if COLOCATION_WORKFLOWS[workflow].get("deploy_only"):
        return _run_deploy_workflow(
            ceph_cluster, config, workflow, is_first_workflow=is_first_workflow
        )
    return _run_failover_workflow(ceph_cluster, config, workflow)


def run(ceph_cluster, **kw):
    """Run NFS Multi-Active colocation port deploy and failover workflows."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")

    if len(clients) < 2:
        raise ConfigError("Colocation tests require at least 2 clients")
    if len(nfs_nodes) < MIN_NFS_NODES:
        raise ConfigError(
            f"Need at least {MIN_NFS_NODES} NFS-capable nodes, found {len(nfs_nodes)}"
        )

    workflows = (
        [config["workflow"]] if config.get("workflow") else list(COLOCATION_WORKFLOWS)
    )
    for step, workflow in enumerate(workflows, start=1):
        if workflow not in COLOCATION_WORKFLOWS:
            raise ConfigError(
                f"Unknown workflow {workflow!r}; valid: "
                f"{', '.join(COLOCATION_WORKFLOWS)}"
            )
        rc = _run_workflow(
            ceph_cluster,
            config,
            workflow,
            step,
            len(workflows),
            is_first_workflow=(step == 1),
        )
        if rc != 0:
            return rc
        try:
            tc = COLOCATION_WORKFLOWS[workflow].get("tc", workflow)
            wait_for_ceph_health(ceph_cluster, config, context=f"post-{tc}")
        except ConfigError as e:
            log.error("Post-%s health check failed: %s", tc, e)
            return 1
    try:
        wait_for_ceph_health(ceph_cluster, config, context="post-suite")
    except ConfigError as e:
        log.error("Post-suite stabilization failed: %s", e)
        return 1
    return 0
