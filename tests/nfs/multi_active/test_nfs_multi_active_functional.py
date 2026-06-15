from datetime import datetime

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from tests.nfs.lib.multi_active import (
    NfsMultiActiveCleanup,
    NfsMultiActiveClient,
    NfsMultiActiveConfig,
    NfsMultiActiveDeploy,
    NfsMultiActiveHaproxy,
    init_crash_monitoring,
    wait_for_ceph_health,
)
from utility.log import Log

log = Log(__name__)

from tests.nfs.lib.multi_active.constants import (
    DEPLOY_TIMEOUT,
    EXPORT_NAME,
    HEALTH_CHECK_INTERVAL,
    MIN_NFS_NODES,
    NFS_DAEMON_COUNT,
    NFS_MOUNT,
    NFS_VERSION,
    POLL_INTERVAL_SEC,
    SERVICE_ID_PREFIX,
)

NFS_SERVICE_ID_PREFIX = SERVICE_ID_PREFIX["functional"]

# Bindable user port range (IANA 1024–65535).
GLOBAL_PORT_MIN = 1024
GLOBAL_PORT_MAX = 65535

# Each workflow: desc, ports (backend, frontend, monitor), placement (nfs/ingress slices).
# Ports use the 13xxx–15xxx / 19xxx ranges to avoid well-known services (2049, 3000,
# 4000, 5000, 6000, 8000, 9000, 9090, 10000, etc.) on baremetal lab nodes.
WORKFLOWS = {
    "same_hosts": {
        "desc": "NFS and ingress on the same 2 hosts",
        "ports": (3333, 2049, 9000),
        "placement": {"min_nodes": 2, "nfs": (0, 2), "ingress": (0, 2)},
    },
    "split_hosts": {
        "desc": "NFS and ingress on disjoint host sets",
        "ports": (13650, 13750, 19350),
        "placement": {"min_nodes": 4, "nfs": (0, 2), "ingress": (2, 4)},
    },
    "partial_overlap": {
        "desc": "NFS on A,B; ingress on B,C (one shared host)",
        "ports": (14001, 14101, 19401),
        "placement": {"min_nodes": 3, "nfs": (0, 2), "ingress": (1, 3)},
    },
    "nfs_only_pool": {
        "desc": "NFS count=2 on pool A,B,C; ingress on A,B only",
        "ports": (14201, 14301, 19451),
        "placement": {"min_nodes": 3, "nfs": (0, 3), "ingress": (0, 2)},
    },
    "ingress_wider": {
        "desc": "NFS on A,B; ingress on A,B,C",
        "ports": (14401, 14501, 19501),
        "placement": {"min_nodes": 3, "nfs": (0, 2), "ingress": (0, 3)},
    },
    "single_ingress": {
        "desc": "NFS count=2 on A,B; ingress on A only",
        "ports": (14601, 14701, 19551),
        "placement": {"min_nodes": 2, "nfs": (0, 2), "ingress": (0, 1)},
    },
    "count_lt_hosts": {
        "desc": "NFS count=2 on pool A,B,C; ingress on B,C",
        "ports": (14801, GLOBAL_PORT_MAX, 19601),
        "placement": {"min_nodes": 3, "nfs": (0, 3), "ingress": (1, 3)},
    },
    "label_placement": {
        "desc": "NFS on A,B; ingress on B,C via placement labels",
        "ports": (15001, 15101, 19701),
        "placement": {
            "min_nodes": 3,
            "nfs": (0, 2),
            "ingress": (1, 3),
            "labels": {"nfs": "nfs-ma-nfs", "ingress": "nfs-ma-ingress"},
        },
    },
}


def _log_workflow_start(workflow, step, total):
    log.info(
        "\n" + "=" * 70 + "\n  %s\n" + "=" * 70,
        f"Workflow {step} of {total}: {workflow}\n  {WORKFLOWS[workflow]['desc']}",
    )


def _hostnames(nodes):
    return [node.hostname for node in nodes]


def _unique_nodes(*node_groups):
    seen = set()
    ordered = []
    for group in node_groups:
        for node in group:
            key = node.hostname
            if key not in seen:
                seen.add(key)
                ordered.append(node)
    return ordered


def _poll_interval(config):
    return int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))


def _deploy_kwargs(config, is_first_workflow):
    """Build deploy kwargs; skip one-time setup on subsequent workflows."""
    poll_interval = _poll_interval(config)
    kwargs = {
        "deploy_timeout": DEPLOY_TIMEOUT,
        "ingress_poll_interval": poll_interval,
    }
    if not is_first_workflow:
        kwargs.update(
            {
                "skip_mgr_enable": True,
            }
        )
    return kwargs


def _resolve_hosts(workflow, config, nfs_nodes):
    """Return (nfs_placement_hosts, ingress_hosts, deploy_nodes, nfs_count, labels)."""
    placement = WORKFLOWS[workflow]["placement"]
    nfs_count = int(config.get("nfs_count", NFS_DAEMON_COUNT))
    nfs_start, nfs_end = placement["nfs"]
    ingress_start, ingress_end = placement["ingress"]

    if len(nfs_nodes) < placement["min_nodes"]:
        raise ConfigError(
            f"{workflow} needs {placement['min_nodes']} NFS-capable nodes, "
            f"found {len(nfs_nodes)}"
        )

    nfs_sel = nfs_nodes[nfs_start:nfs_end]
    ingress_sel = nfs_nodes[ingress_start:ingress_end]
    if nfs_count > len(nfs_sel):
        raise ConfigError(
            f"nfs_count ({nfs_count}) exceeds NFS host pool ({len(nfs_sel)}) "
            f"for workflow {workflow!r}"
        )

    return (
        _hostnames(nfs_sel),
        _hostnames(ingress_sel),
        _unique_nodes(nfs_sel, ingress_sel),
        nfs_count,
        placement.get("labels"),
    )


def _run_ha_stick_table(ceph_cluster, config, workflow, is_first_workflow=True):
    """Deploy HA and validate stick table for one placement workflow."""
    client1, client2 = ceph_cluster.get_nodes("client")[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    poll_interval = _poll_interval(config)

    nfs_hosts, ingress_hosts, deploy_nodes, nfs_count, placement_labels = (
        _resolve_hosts(workflow, config, nfs_nodes)
    )
    placement = WORKFLOWS[workflow]["placement"]
    nfs_backend_port, ingress_frontend_port, ingress_monitor_port = WORKFLOWS[workflow][
        "ports"
    ]
    nfs_service_id = (
        f"{NFS_SERVICE_ID_PREFIX}-{workflow}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = NfsMultiActiveConfig.get_vips(config)[0]
    mounted_clients = []
    export_created = False
    cluster_deployed = False
    labels_applied = False

    log.info("NFS service id: %s, VIP: %s", nfs_service_id, vip)
    if placement_labels:
        log.info(
            "NFS count=%s label=%r (pool %s); ingress label=%r (pool %s)",
            nfs_count,
            placement_labels["nfs"],
            nfs_hosts,
            placement_labels["ingress"],
            ingress_hosts,
        )
    else:
        log.info(
            "NFS count=%s placement hosts=%s; ingress hosts=%s",
            nfs_count,
            nfs_hosts,
            ingress_hosts,
        )
    log.info(
        "Ports: nfs_backend=%s ingress_frontend=%s ingress_monitor=%s",
        nfs_backend_port,
        ingress_frontend_port,
        ingress_monitor_port,
    )

    try:
        if placement_labels:
            NfsMultiActiveConfig.apply_placement_labels(
                client1,
                nfs_nodes,
                placement["nfs"],
                placement["ingress"],
                placement_labels,
            )
            labels_applied = True

        nfs_label = placement_labels["nfs"] if placement_labels else None
        ingress_label = placement_labels["ingress"] if placement_labels else None
        specs = [
            NfsMultiActiveConfig.build_nfs_spec(
                nfs_hosts,
                nfs_count,
                nfs_service_id,
                nfs_backend_port,
                label=nfs_label,
            ),
            NfsMultiActiveConfig.build_ingress_spec(
                vip,
                nfs_service_id,
                ingress_frontend_port,
                ingress_monitor_port,
                config.get("health_check_interval", HEALTH_CHECK_INTERVAL),
                ingress_hostnames=None if ingress_label else ingress_hosts,
                label=ingress_label,
            ),
        ]
        haproxy = NfsMultiActiveHaproxy(
            ceph_cluster, nfs_service_id, info_client=client1
        )
        stick_kwargs = {"stick_table_interval": poll_interval}

        deploy_kwargs = _deploy_kwargs(config, is_first_workflow)
        if ingress_label:
            deploy_kwargs["expected_ingress_hosts"] = ingress_hosts

        cluster_deployed = True
        NfsMultiActiveDeploy(ceph_cluster, nfs_service_id, vip, client=client1).deploy(
            specs, deploy_nodes, **deploy_kwargs
        )
        haproxy.validate_entries(**stick_kwargs)

        Ceph(client1).nfs.export.create(
            fs_name="cephfs",
            nfs_name=nfs_service_id,
            nfs_export=EXPORT_NAME,
            fs="cephfs",
        )
        NfsMultiActiveConfig.wait_until_export_visible(
            client1,
            nfs_service_id,
            EXPORT_NAME,
            interval=poll_interval,
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
            haproxy.validate_entries(mounted_clients=mounted_clients, **stick_kwargs)

        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir=workflow,
        )
        haproxy.validate_entries(mounted_clients=mounted_clients, **stick_kwargs)

        return 0
    except Exception as e:
        log.error("NFS Multi-Active test failed (%s): %s", workflow, e)
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
                    placement_labels,
                )
            except Exception as e:
                log.warning("Placement label cleanup failed: %s", e)


def run(ceph_cluster, **kw):
    """Run all multi-active HA placement workflows (see WORKFLOWS)."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")

    if len(clients) < 2:
        raise ConfigError("Stick table mount validation requires at least 2 clients")
    if len(nfs_nodes) < MIN_NFS_NODES:
        raise ConfigError(
            f"Need at least {MIN_NFS_NODES} NFS-capable nodes, found {len(nfs_nodes)}"
        )

    workflows = [config["workflow"]] if config.get("workflow") else list(WORKFLOWS)
    for step, workflow in enumerate(workflows, start=1):
        if workflow not in WORKFLOWS:
            raise ConfigError(
                f"Unknown workflow {workflow!r}; valid: {', '.join(WORKFLOWS)}"
            )
        _log_workflow_start(workflow, step, len(workflows))
        rc = _run_ha_stick_table(
            ceph_cluster,
            config,
            workflow,
            is_first_workflow=(step == 1),
        )
        if rc != 0:
            return rc
        try:
            wait_for_ceph_health(ceph_cluster, config, context=f"post-{workflow}")
        except ConfigError as e:
            log.error("Post-%s health check failed: %s", workflow, e)
            return 1
    try:
        wait_for_ceph_health(ceph_cluster, config, context="post-suite")
    except ConfigError as e:
        log.error("Post-suite stabilization failed: %s", e)
        return 1
    return 0
