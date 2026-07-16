"""Session bootstrap, teardown, and shared deploy helpers."""

from datetime import datetime

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError
from tests.nfs.lib.multi_active import (
    NfsMultiActiveCleanup,
    NfsMultiActiveClient,
    NfsMultiActiveConfig,
    NfsMultiActiveDeploy,
    NfsMultiActiveFailover,
    NfsMultiActiveHaproxy,
)
from tests.nfs.lib.multi_active.constants import (
    BG_IO_RUNTIME,
    DEPLOY_TIMEOUT,
    EXPORT_NAME,
    FS_NAME,
    HEALTH_CHECK_INTERVAL,
    NFS_MOUNT,
    NFS_VERSION,
    POLL_INTERVAL_SEC,
)
from tests.nfs.lib.multi_active.placement import (
    FAILOVER_PLACEMENT,
    resolve_placement_hosts,
)
from utility.log import Log

log = Log(__name__)


def _poll_interval(config):
    return int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))


def io_runtime(config):
    return NfsMultiActiveConfig.io_runtime(config, default=BG_IO_RUNTIME)


def io_tool_label(config):
    return NfsMultiActiveConfig.resolve_io_tool(config)


def _deploy_kwargs(config, is_first_session):
    """Build deploy kwargs; skip one-time setup on subsequent sessions."""
    kwargs = {
        "deploy_timeout": DEPLOY_TIMEOUT,
        "ingress_poll_interval": _poll_interval(config),
    }
    if not is_first_session:
        kwargs.update(
            {
                "skip_mgr_enable": True,
            }
        )
    return kwargs


def client_backend_mappings(haproxy, mounted_clients, spare_host):
    """Return [(client, pinned_backend), ...] for clients not pinned to spare."""
    mappings = []
    for client in mounted_clients:
        backend = haproxy.get_client_backend_hostname(client)
        if backend == spare_host:
            continue
        mappings.append((client, backend))
    if not mappings:
        raise ConfigError(
            f"No mounted client pinned to a non-spare backend (spare={spare_host!r})"
        )
    return mappings


def stop_phase_io(
    config,
    dual_client_io,
    io_sessions,
    io_thread,
    io_error_box,
    primary_io_client,
    io_dir,
):
    """Stop background IO after a failover phase completes."""
    if dual_client_io and io_sessions:
        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _, thread, _, _ in io_sessions:
            thread.join(timeout=30)
    elif (
        io_thread is not None and io_thread.is_alive() and primary_io_client and io_dir
    ):
        NfsMultiActiveClient.stop_background_io(
            primary_io_client, io_dir, config, io_error_box
        )
        io_thread.join(timeout=30)


def session_setup(
    ceph_cluster,
    config,
    session_name,
    vip_slot,
    ports,
    is_first_workflow,
    *,
    service_id_prefix,
    placement=FAILOVER_PLACEMENT,
    spare_slice=None,
):
    """Deploy NFS multi-active once for a failover-style session."""
    client1, client2 = ceph_cluster.get_nodes("client")[:2]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    spare_kw = {} if spare_slice is None else {"spare_slice": spare_slice}
    (
        nfs_hosts,
        ingress_hosts,
        deploy_nodes,
        nfs_count,
        labels,
        spare_host,
    ) = resolve_placement_hosts(nfs_nodes, placement, config, **spare_kw)
    nfs_backend_port, ingress_frontend_port, ingress_monitor_port = ports
    vip = NfsMultiActiveConfig.get_vips(config)[vip_slot - 1]
    nfs_service_id = (
        f"{service_id_prefix}-{session_name}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    NfsMultiActiveConfig.apply_placement_labels(
        client1,
        nfs_nodes,
        placement["nfs"],
        placement["ingress"],
        labels,
    )

    nfs_spec = NfsMultiActiveConfig.build_nfs_spec(
        nfs_hosts,
        nfs_count,
        nfs_service_id,
        nfs_backend_port,
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
    deploy = NfsMultiActiveDeploy(ceph_cluster, nfs_service_id, vip, client=client1)
    haproxy = NfsMultiActiveHaproxy(ceph_cluster, nfs_service_id, info_client=client1)
    failover = NfsMultiActiveFailover()
    stick_kwargs = {"stick_table_interval": _poll_interval(config)}

    deploy.deploy(
        [nfs_spec, ingress_spec],
        deploy_nodes,
        expected_ingress_hosts=ingress_hosts,
        **_deploy_kwargs(config, is_first_workflow),
    )
    haproxy.validate_entries(**stick_kwargs)

    Ceph(client1).nfs.export.create(
        fs_name=FS_NAME,
        nfs_name=nfs_service_id,
        nfs_export=EXPORT_NAME,
        fs=FS_NAME,
    )
    NfsMultiActiveConfig.wait_until_export_visible(
        client1,
        nfs_service_id,
        EXPORT_NAME,
        interval=_poll_interval(config),
    )

    mounted_clients = []
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

    return {
        "ceph_cluster": ceph_cluster,
        "config": config,
        "session_name": session_name,
        "client1": client1,
        "nfs_nodes": nfs_nodes,
        "ingress_hosts": ingress_hosts,
        "deploy_nodes": deploy_nodes,
        "nfs_count": nfs_count,
        "labels": labels,
        "spare_host": spare_host,
        "nfs_service_id": nfs_service_id,
        "vip": vip,
        "ingress_frontend_port": ingress_frontend_port,
        "nfs_spec": nfs_spec,
        "ingress_spec": ingress_spec,
        "deploy": deploy,
        "haproxy": haproxy,
        "failover": failover,
        "stick_kwargs": stick_kwargs,
        "mounted_clients": mounted_clients,
        "labels_applied": True,
        "export_created": True,
        "cluster_deployed": True,
        "placement": placement,
    }


def session_teardown(ctx):
    """Unmount, delete export, remove cluster, and clear placement labels."""
    client1 = ctx["client1"]
    nfs_nodes = ctx["nfs_nodes"]
    nfs_service_id = ctx["nfs_service_id"]
    mounted_clients = ctx.get("mounted_clients") or []
    placement = ctx.get("placement") or FAILOVER_PLACEMENT

    try:
        if mounted_clients:
            NfsMultiActiveClient.unmount_and_delete_export(
                mounted_clients, NFS_MOUNT, nfs_service_id, EXPORT_NAME
            )
        elif ctx.get("export_created"):
            Ceph(client1).nfs.export.delete(nfs_service_id, EXPORT_NAME)
    except Exception as exc:
        log.warning("Mount/export cleanup failed: %s", exc)

    if ctx.get("cluster_deployed"):
        try:
            NfsMultiActiveCleanup(ctx["ceph_cluster"]).remove_cluster(nfs_service_id)
        except Exception as exc:
            log.warning("NFS cluster cleanup failed: %s", exc)

    if ctx.get("labels_applied"):
        try:
            NfsMultiActiveConfig.remove_placement_labels(
                client1,
                nfs_nodes,
                placement["nfs"],
                placement["ingress"],
                ctx["labels"],
            )
            for spare_label in ctx["labels"].values():
                client1.exec_command(
                    sudo=True,
                    cmd=f"ceph orch host label rm {ctx['spare_host']} {spare_label}",
                    check_ec=False,
                )
        except Exception as exc:
            log.warning("Placement label cleanup failed: %s", exc)
