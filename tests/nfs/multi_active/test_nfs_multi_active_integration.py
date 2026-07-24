"""NFS Multi-Active integration tests with CephFS (TC-1..)."""

from datetime import datetime
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.lib.multi_active import (
    NfsMultiActiveCleanup,
    NfsMultiActiveClient,
    NfsMultiActiveConfig,
    NfsMultiActiveDelegation,
    NfsMultiActiveDeploy,
    NfsMultiActiveFailover,
    NfsMultiActiveHaproxy,
    NfsMultiActiveQos,
    init_crash_monitoring,
    wait_for_ceph_health,
)
from tests.nfs.lib.multi_active.constants import (
    DEFAULT_QOS_OPERATION,
    DEFAULT_QOS_TYPE,
    DEFAULT_TC3_DELEGATION_HOLD_READY_SEC,
    DEFAULT_TC3_DELEGATION_HOLD_SEC,
    DEFAULT_TC3_DELEGATION_RELEASE_WAIT_SEC,
    DEFAULT_TC3_REDEPLOY_WAIT,
    DEFAULT_TC3_SERVICE_WAIT_TIMEOUT,
    DEFAULT_TC4_QOS_TYPE,
    DEPLOY_TIMEOUT,
    EXPORT_NAME,
    FS_NAME,
    HEALTH_CHECK_INTERVAL,
    INTEGRATION_PORTS,
    IO_JOIN_TIMEOUT,
    IO_RESUME_TIMEOUT,
    IO_WARMUP_SEC,
    NFS_DAEMON_COUNT,
    NFS_MOUNT,
    NFS_VERSION,
    POLL_INTERVAL_SEC,
    QOS_SETTLE_SEC,
    SERVICE_ID_PREFIX,
    TC4_COLOCATION,
    TC4_INGRESS_PORTS,
    TC4_LABELS,
    TC4_NFS_COUNT,
)
from tests.nfs.lib.multi_active.placement import (
    COLOCATION_PLACEMENT,
    INTEGRATION_PLACEMENT,
    resolve_integration_hosts,
    resolve_placement_hosts,
)
from tests.nfs.nfs_delegation_operations import (
    kill_delegation_holds,
    release_delegation_hold_gracefully,
)
from utility.log import Log

log = Log(__name__)

NFS_SERVICE_ID_PREFIX = SERVICE_ID_PREFIX["integration"]
_INTEGRATION_PLACEMENT = INTEGRATION_PLACEMENT
_PORTS = INTEGRATION_PORTS
_TC4_NFS_COUNT = TC4_NFS_COUNT
_TC4_COLOCATION = TC4_COLOCATION
_TC4_INGRESS_PORTS = TC4_INGRESS_PORTS
_TC4_LABELS = TC4_LABELS

INTEGRATION_WORKFLOWS = {
    "tc1_mds_failover_dual_client_fio": {
        "tc": "TC-1",
        "vip": 1,
        "desc": (
            "Deploy NFS multi-active, mount two clients, run FIO, fail over MDS "
            "via cephfs_common_lib, validate MDS failover and IO resume."
        ),
    },
    "tc2_cqos_pershare_failover_io": {
        "tc": "TC-2",
        "vip": 1,
        "desc": (
            "Configure per-share CQoS on the export, validate bandwidth limits, "
            "run FIO, trigger NFS backend label failover, verify QoS persistence "
            "and IO resume with no limit violations."
        ),
    },
    "tc3_rw_delegation_failover_io": {
        "tc": "TC-3",
        "vip": 1,
        "desc": (
            "Reuse integration HA cluster, set export-level RW delegation, "
            "validate delegation via Ganesha logs on VIP mount, hold delegation "
            "before NFS backend failover, verify post-failover access, then IO resume."
        ),
    },
    "tc4_cqos_perclient_colocation_io": {
        "tc": "TC-4",
        "vip": 2,
        "desc": (
            "Deploy colocated NFS instances on the same node(s), enable per-client "
            "CQoS, run dual-client IO, and validate per-client QoS isolation with "
            "no cross-impact between colocated NFS backends."
        ),
    },
}


def _resolve_integration_vip(config, workflow=None, vip_slot=None):
    """Return VIP for an integration workflow (1-based slot into config vips)."""
    vips = NfsMultiActiveConfig.get_vips(config)
    if vip_slot is None:
        vip_slot = int((workflow or {}).get("vip", config.get("vip", 1)))
    slot = int(vip_slot)
    if slot < 1 or slot > len(vips):
        raise ConfigError(
            f"Integration workflow vip slot must be 1..{len(vips)}, got {slot!r}"
        )
    return vips[slot - 1]


class IntegrationClusterSession:
    """Cluster state shared between TC-1..TC-4 in one integration session."""

    def __init__(self):
        self.ceph_cluster = None
        self.config = None
        self.client1 = None
        self.client2 = None
        self.nfs_nodes = None
        self.nfs_hosts = None
        self.ingress_hosts = None
        self.deploy_nodes = None
        self.spare_host = None
        self.nfs_service_id = None
        self.vip = None
        self.nfs_backend_port = None
        self.ingress_frontend_port = None
        self.ingress_monitor_port = None
        self.labels = None
        self.nfs_count = NFS_DAEMON_COUNT
        self.deploy_timeout = DEPLOY_TIMEOUT
        self.poll_interval = POLL_INTERVAL_SEC
        self.nfs_spec = None
        self.ingress_spec = None
        self.deploy = None
        self.haproxy = None
        self.failover = None
        self.colocation = False
        self.cluster_deployed = False
        self.export_created = False
        self.labels_applied = False
        self.mounted_clients = []
        self.qos_enabled = False
        self.cluster_qos_enabled = False
        self.qos_baseline_get = None
        self.qos_baseline_block = None
        self.io_sessions = []
        self.timings = {}
        self.delegation_logging_template_backup = False
        self.delegation_cephadm = None
        self.delegation_installer = None
        self.nfs_cmd_host = None

    def cleanup(self):
        config = self.config or {}
        if self.client1:
            try:
                kill_delegation_holds(self.client1)
            except Exception as exc:
                log.warning("Delegation hold cleanup failed on client1: %s", exc)
        if self.client2:
            try:
                kill_delegation_holds(self.client2)
            except Exception as exc:
                log.warning("Delegation hold cleanup failed on client2: %s", exc)
        if self.io_sessions:
            try:
                NfsMultiActiveClient.stop_background_io_on_clients(
                    self.io_sessions, config
                )
                for _client, thread, _error_box, _io_dir in self.io_sessions:
                    thread.join(timeout=IO_JOIN_TIMEOUT)
            except Exception as exc:
                log.warning("IO session cleanup failed: %s", exc)
            self.io_sessions = []

        client1 = self.client1
        if client1 and self.nfs_service_id:
            try:
                NfsMultiActiveQos.disable(self)
            except Exception as exc:
                log.warning("QoS cleanup failed: %s", exc)

        try:
            if self.mounted_clients:
                NfsMultiActiveClient.unmount_and_delete_export(
                    self.mounted_clients,
                    NFS_MOUNT,
                    self.nfs_service_id,
                    EXPORT_NAME,
                )
            elif self.export_created and client1 and self.nfs_service_id:
                Ceph(client1).nfs.export.delete(self.nfs_service_id, EXPORT_NAME)
        except Exception as exc:
            log.warning("Mount/export cleanup failed: %s", exc)

        client1 = self.client1
        if (
            self.delegation_logging_template_backup
            and self.nfs_cmd_host
            and self.delegation_cephadm
            and self.delegation_installer
            and self.nfs_service_id
        ):
            cluster_ids = [self.nfs_service_id]
            try:
                redeploy_wait = int(
                    (self.config or {}).get(
                        "tc3_redeploy_wait", DEFAULT_TC3_REDEPLOY_WAIT
                    )
                )
                service_wait = int(
                    (self.config or {}).get(
                        "tc3_service_wait_timeout",
                        DEFAULT_TC3_SERVICE_WAIT_TIMEOUT,
                    )
                )
                NfsMultiActiveDelegation.restore_ganesha_debug_logging(
                    self.nfs_cmd_host,
                    self.delegation_cephadm,
                    self.delegation_installer,
                    cluster_ids,
                    self.delegation_logging_template_backup,
                    redeploy_wait=redeploy_wait,
                    service_wait_timeout=service_wait,
                )
            except Exception as exc:
                log.warning("Ganesha debug logging restore failed: %s", exc)
            self.delegation_logging_template_backup = False

        if self.labels_applied and client1 and self.nfs_nodes and self.labels:
            try:
                placement = _INTEGRATION_PLACEMENT
                NfsMultiActiveConfig.clear_placement_labels(
                    client1,
                    self.nfs_nodes,
                    self.labels,
                )
                NfsMultiActiveConfig.remove_placement_labels(
                    client1,
                    self.nfs_nodes,
                    placement["nfs"],
                    placement["ingress"],
                    self.labels,
                )
            except Exception as exc:
                log.warning("Placement label cleanup failed: %s", exc)
            self.labels_applied = False

        if self.cluster_deployed and self.nfs_service_id:
            try:
                NfsMultiActiveCleanup(self.ceph_cluster).remove_cluster(
                    self.nfs_service_id
                )
            except Exception as exc:
                log.warning("NFS cluster cleanup failed: %s", exc)
            self.cluster_deployed = False
        self.colocation = False


def _ensure_two_active_mdss(cephfs_common, client, fs_name):
    """Raise max_mds when needed so rolling_mds_failover can succeed."""
    active = cephfs_common.get_active_mdss(client, fs_name)
    if len(active) >= 2:
        log.info("CephFS already has %d active MDS: %s", len(active), active)
        return active
    log.info("Setting %s max_mds=2 for MDS failover test", fs_name)
    client.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 2")
    if cephfs_common.wait_for_two_active_mds(client, fs_name):
        raise OperationFailedError(f"Timed out waiting for 2 active MDS on {fs_name!r}")
    active = cephfs_common.get_active_mdss(client, fs_name)
    log.info("Active MDS after max_mds=2: %s", active)
    return active


def _validate_mds_failover(active_before, active_after):
    if not active_after:
        raise OperationFailedError("No active MDS after failover")
    if set(active_before) == set(active_after):
        raise OperationFailedError(
            f"MDS failover did not change active set: before={active_before} "
            f"after={active_after}"
        )
    log.info(
        "MDS failover validated: before=%s after=%s",
        active_before,
        active_after,
    )


def bootstrap_integration_cluster(ceph_cluster, config, session, case_id, vip_slot=1):
    """Deploy label-based NFS multi-active cluster when session is not bootstrapped."""
    if session.cluster_deployed:
        log.info("%s reusing integration cluster %s", case_id, session.nfs_service_id)
        return

    clients = ceph_cluster.get_nodes("client")
    if len(clients) < 2:
        raise ConfigError("Integration tests require at least 2 client nodes")
    client1, client2 = clients[0], clients[1]
    nfs_nodes = NfsMultiActiveConfig.sorted_nfs_nodes(ceph_cluster)
    nfs_hosts, ingress_hosts, deploy_nodes, spare_host = resolve_integration_hosts(
        nfs_nodes
    )
    nfs_backend_port, ingress_frontend_port, ingress_monitor_port = _PORTS
    placement = _INTEGRATION_PLACEMENT
    labels = placement["labels"]
    nfs_count = int(config.get("nfs_count", NFS_DAEMON_COUNT))
    nfs_service_id = (
        f"{NFS_SERVICE_ID_PREFIX}-int-" f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = _resolve_integration_vip(config, vip_slot=vip_slot)
    poll_interval = int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))
    deploy_timeout = int(config.get("deploy_timeout", DEPLOY_TIMEOUT))

    session.ceph_cluster = ceph_cluster
    session.config = config
    session.client1 = client1
    session.client2 = client2
    session.nfs_nodes = nfs_nodes
    session.nfs_hosts = nfs_hosts
    session.ingress_hosts = ingress_hosts
    session.deploy_nodes = deploy_nodes
    session.spare_host = spare_host
    session.nfs_service_id = nfs_service_id
    session.vip = vip
    session.nfs_backend_port = nfs_backend_port
    session.ingress_frontend_port = ingress_frontend_port
    session.ingress_monitor_port = ingress_monitor_port
    session.labels = labels
    session.nfs_count = nfs_count
    session.deploy_timeout = deploy_timeout
    session.poll_interval = poll_interval
    session.colocation = False

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
    session.nfs_spec = nfs_spec
    session.ingress_spec = ingress_spec
    session.deploy = NfsMultiActiveDeploy(
        ceph_cluster, nfs_service_id, vip, client=client1
    )
    session.haproxy = NfsMultiActiveHaproxy(
        ceph_cluster, nfs_service_id, info_client=client1
    )
    session.failover = NfsMultiActiveFailover()

    log.info(
        "%s deploy: nfs=%s ingress=%s spare=%s VIP=%s (slot %s) service_id=%s",
        case_id,
        nfs_hosts,
        ingress_hosts,
        spare_host,
        vip,
        vip_slot,
        nfs_service_id,
    )

    NfsMultiActiveConfig.clear_placement_labels(client1, nfs_nodes, labels)
    NfsMultiActiveConfig.apply_placement_labels(
        client1,
        nfs_nodes,
        placement["nfs"],
        placement["ingress"],
        labels,
    )
    session.labels_applied = True
    session.deploy.deploy(
        [nfs_spec, ingress_spec],
        deploy_nodes,
        deploy_timeout=deploy_timeout,
        expected_ingress_hosts=ingress_hosts,
        ingress_poll_interval=poll_interval,
    )
    session.cluster_deployed = True

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
        interval=poll_interval,
    )
    session.export_created = True

    for client in (client1, client2):
        NfsMultiActiveClient.mount_via_vip(
            client,
            vip,
            NFS_MOUNT,
            EXPORT_NAME,
            str(ingress_frontend_port),
            NFS_VERSION,
        )
        session.mounted_clients.append(client)
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            stick_table_interval=poll_interval,
        )


def _ensure_tc4_colocation_cluster(ceph_cluster, config, session, case_id, vip_slot=2):
    """Deploy or reuse a colocated multi-NFS cluster for TC-4."""
    if session.cluster_deployed and session.colocation:
        log.info("%s reusing colocation cluster %s", case_id, session.nfs_service_id)
        return
    if session.cluster_deployed:
        log.info(
            "%s replacing prior integration cluster with colocation deploy", case_id
        )
        session.cleanup()
    bootstrap_tc4_colocation_cluster(
        ceph_cluster, config, session, case_id, vip_slot=vip_slot
    )


def bootstrap_tc4_colocation_cluster(
    ceph_cluster, config, session, case_id, vip_slot=2
):
    """Deploy colocated NFS instances (multiple daemons per node) for TC-4."""
    clients = ceph_cluster.get_nodes("client")
    if len(clients) < 2:
        raise ConfigError("TC-4 requires at least 2 client nodes")
    client1, client2 = clients[0], clients[1]
    nfs_nodes = NfsMultiActiveConfig.sorted_nfs_nodes(ceph_cluster)
    placement = COLOCATION_PLACEMENT
    nfs_hosts, ingress_hosts, deploy_nodes, _, _, spare_host = resolve_placement_hosts(
        nfs_nodes, placement, config
    )
    nfs_sel_count = len(nfs_hosts)
    nfs_count = int(config.get("tc4_nfs_count", _TC4_NFS_COUNT))
    if nfs_count > nfs_sel_count * 2:
        raise ConfigError(
            f"tc4_nfs_count ({nfs_count}) exceeds colocation capacity on "
            f"{nfs_sel_count} NFS host(s)"
        )

    labels = _TC4_LABELS
    ingress_frontend_port, ingress_monitor_port = _TC4_INGRESS_PORTS
    coloc = _TC4_COLOCATION
    nfs_service_id = (
        f"{NFS_SERVICE_ID_PREFIX}-int-tc4-" f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )
    vip = _resolve_integration_vip(config, vip_slot=vip_slot)
    poll_interval = int(config.get("poll_interval_sec", POLL_INTERVAL_SEC))
    deploy_timeout = int(config.get("deploy_timeout", DEPLOY_TIMEOUT))

    session.ceph_cluster = ceph_cluster
    session.config = config
    session.client1 = client1
    session.client2 = client2
    session.nfs_nodes = nfs_nodes
    session.nfs_hosts = nfs_hosts
    session.ingress_hosts = ingress_hosts
    session.deploy_nodes = deploy_nodes
    session.spare_host = spare_host
    session.nfs_service_id = nfs_service_id
    session.vip = vip
    session.nfs_backend_port = coloc["port"]
    session.ingress_frontend_port = ingress_frontend_port
    session.ingress_monitor_port = ingress_monitor_port
    session.labels = labels
    session.nfs_count = nfs_count
    session.deploy_timeout = deploy_timeout
    session.poll_interval = poll_interval
    session.colocation = True
    session.mounted_clients = []
    session.qos_enabled = False
    session.cluster_qos_enabled = False
    session.qos_baseline_get = None
    session.qos_baseline_block = None

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
    session.nfs_spec = nfs_spec
    session.ingress_spec = ingress_spec
    session.deploy = NfsMultiActiveDeploy(
        ceph_cluster, nfs_service_id, vip, client=client1
    )
    session.haproxy = NfsMultiActiveHaproxy(
        ceph_cluster, nfs_service_id, info_client=client1
    )
    session.failover = NfsMultiActiveFailover()

    log.info(
        "%s colocation deploy: nfs=%s ingress=%s count=%s VIP=%s (slot %s) service_id=%s",
        case_id,
        nfs_hosts,
        ingress_hosts,
        nfs_count,
        vip,
        vip_slot,
        nfs_service_id,
    )

    NfsMultiActiveConfig.clear_placement_labels(client1, nfs_nodes, labels)
    NfsMultiActiveConfig.apply_placement_labels(
        client1,
        nfs_nodes,
        placement["nfs"],
        placement["ingress"],
        labels,
    )
    session.labels_applied = True
    session.deploy.deploy(
        [nfs_spec, ingress_spec],
        deploy_nodes,
        deploy_timeout=deploy_timeout,
        expected_ingress_hosts=ingress_hosts,
        ingress_poll_interval=poll_interval,
        colocation_validation=True,
    )
    session.cluster_deployed = True

    cluster_info = session.deploy.validator.get_cluster_info(nfs_service_id)
    session.deploy.validator.assert_colocation_cluster_info(
        cluster_info, nfs_spec, ingress_spec
    )
    session.deploy.validator.assert_colocation_ports_listening(cluster_info, nfs_spec)

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
        interval=poll_interval,
    )
    session.export_created = True

    stick_kwargs = {"stick_table_interval": poll_interval}
    for client in (client1, client2):
        NfsMultiActiveClient.mount_via_vip(
            client,
            vip,
            NFS_MOUNT,
            EXPORT_NAME,
            str(ingress_frontend_port),
            NFS_VERSION,
        )
        session.mounted_clients.append(client)
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )


def run_tc1_mds_failover_dual_client_fio(
    ceph_cluster, config, case_id, session=None, vip_slot=1
):
    """TC-1: NFS HA deploy, dual-client FIO, CephFS MDS failover, IO resume."""
    session = session or IntegrationClusterSession()
    bootstrap_integration_cluster(
        ceph_cluster, config, session, case_id, vip_slot=vip_slot
    )

    client1 = session.client1
    client2 = session.client2
    poll_interval = session.poll_interval
    io_warmup = int(config.get("mds_failover_io_warmup", IO_WARMUP_SEC))
    io_resume_timeout = int(config.get("io_resume_timeout", IO_RESUME_TIMEOUT))
    stick_kwargs = {"stick_table_interval": poll_interval}
    timings = session.timings
    io_sessions = []

    try:
        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir="tc1_pre_mds_failover",
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )

        io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
            session.mounted_clients,
            NFS_MOUNT,
            config,
            io_subdir="tc1_mds_failover_io",
        )
        if io_warmup > 0:
            log.info("FIO warmup %ss before MDS failover", io_warmup)
            sleep(io_warmup)
        for client, thread, error_box, _io_dir in io_sessions:
            if error_box.get("exc"):
                raise OperationFailedError(
                    f"Background IO failed on {client.hostname}: {error_box['exc']}"
                )
            if not thread.is_alive():
                raise OperationFailedError(
                    f"Background IO stopped during warmup on {client.hostname}"
                )

        cephfs_common = CephFSCommonUtils(ceph_cluster)
        active_before = _ensure_two_active_mdss(cephfs_common, client1, FS_NAME)
        log.info("%s active MDS before failover: %s", case_id, active_before)

        timings["mds_failover_triggered_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if cephfs_common.rolling_mds_failover(client1, FS_NAME, mds_fail_cnt=1):
            raise OperationFailedError("rolling_mds_failover failed")

        active_after = cephfs_common.get_active_mdss(client1, FS_NAME)
        log.info("%s active MDS after failover: %s", case_id, active_after)
        _validate_mds_failover(active_before, active_after)

        NfsMultiActiveClient.wait_for_background_io_resume(
            io_sessions,
            NFS_MOUNT,
            config,
            timeout=io_resume_timeout,
            timings=timings,
            triggered_at=timings.get("mds_failover_triggered_at"),
        )
        log.info("%s background IO resumed after MDS failover", case_id)

        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _client, thread, error_box, _io_dir in io_sessions:
            thread.join(timeout=IO_JOIN_TIMEOUT)
            if thread.is_alive():
                raise OperationFailedError(
                    f"Background IO on {_client.hostname} did not stop within "
                    f"{IO_JOIN_TIMEOUT}s"
                )
            if error_box.get("exc") and not error_box.get("stopped"):
                raise OperationFailedError(
                    f"Background IO failed on {_client.hostname}: {error_box['exc']}"
                )
        io_sessions = []

        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir="tc1_post_mds_failover",
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )

        log.info("%s passed", case_id)
        return 0
    except Exception as exc:
        log.error("%s failed: %s", case_id, exc)
        return 1
    finally:
        if io_sessions:
            try:
                NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
                for _client, thread, _error_box, _io_dir in io_sessions:
                    thread.join(timeout=IO_JOIN_TIMEOUT)
            except Exception as exc:
                log.warning("TC-1 IO session cleanup failed: %s", exc)


def run_tc2_cqos_pershare_failover_io(ceph_cluster, config, case_id, session=None):
    """TC-2: per-share CQoS, IO workload, NFS failover, QoS + IO validation."""
    session = session or IntegrationClusterSession()
    bootstrap_integration_cluster(ceph_cluster, config, session, case_id)

    client1 = session.client1
    client2 = session.client2
    export_bw = NfsMultiActiveQos.export_bw_config(config)
    cluster_bw = NfsMultiActiveQos.cluster_bw_config(config)
    qos_type = config.get("qos_type", DEFAULT_QOS_TYPE)
    qos_operation = config.get("qos_operation") or config.get(
        "control", DEFAULT_QOS_OPERATION
    )
    poll_interval = session.poll_interval
    io_warmup = int(config.get("cqos_failover_io_warmup", IO_WARMUP_SEC))
    io_resume_timeout = int(config.get("io_resume_timeout", IO_RESUME_TIMEOUT))
    qos_settle = int(
        config.get("tc2_qos_settle", config.get("qos_settle", QOS_SETTLE_SEC))
    )
    stick_kwargs = {"stick_table_interval": poll_interval}
    timings = session.timings
    io_sessions = []

    try:
        NfsMultiActiveQos.enable(
            session,
            cluster_bw,
            export_bw,
            qos_type,
            qos_operation,
        )
        qos_get, qos_block = NfsMultiActiveQos.wait_ready(
            client1,
            session.nfs_service_id,
            export_bw,
            qos_type,
            "pre_failover",
            config=config,
        )
        session.qos_baseline_get = qos_get
        session.qos_baseline_block = qos_block
        backend_host, backend_port = session.haproxy.get_client_backend_endpoint(
            client1
        )
        log.info(
            "TC-2 QoS speed via VIP %s (pinned backend %s:%s, pre_failover)",
            NFS_MOUNT,
            backend_host,
            backend_port,
        )
        if qos_settle > 0:
            log.info("TC-2 QoS settle %ss before speed validation", qos_settle)
            sleep(qos_settle)
        NfsMultiActiveQos.validate_speeds(
            client1, NFS_MOUNT, export_bw, qos_type, "pre_failover"
        )

        io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
            session.mounted_clients,
            NFS_MOUNT,
            config,
            io_subdir="tc2_cqos_failover_io",
        )
        session.io_sessions = io_sessions
        if io_warmup > 0:
            log.info("FIO warmup %ss before NFS failover", io_warmup)
            sleep(io_warmup)
        for client, thread, error_box, _io_dir in io_sessions:
            if error_box.get("exc"):
                raise OperationFailedError(
                    f"Background IO failed on {client.hostname}: {error_box['exc']}"
                )
            if not thread.is_alive():
                raise OperationFailedError(
                    f"Background IO stopped during warmup on {client.hostname}"
                )

        pinned_nfs = session.haproxy.get_client_backend_hostname(client1)
        log.info(
            "%s NFS failover: drain pinned backend %s -> spare %s",
            case_id,
            pinned_nfs,
            session.spare_host,
        )
        timings["nfs_failover_triggered_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        def _wait_io_after_nfs_failover():
            NfsMultiActiveClient.wait_for_background_io_resume(
                io_sessions,
                NFS_MOUNT,
                config,
                timeout=io_resume_timeout,
                timings=timings,
                triggered_at=timings.get("nfs_failover_triggered_at"),
            )

        session.failover.failover_nfs_by_label(
            client1,
            session.deploy,
            session.nfs_spec,
            session.deploy_nodes,
            pinned_nfs,
            session.spare_host,
            session.labels["nfs"],
            session.nfs_count,
            deploy_timeout=session.deploy_timeout,
            timings=timings,
            timings_trigger_key="nfs_failover_triggered_at",
            post_apply_callback=_wait_io_after_nfs_failover,
        )
        log.info("%s background IO resumed after NFS failover", case_id)

        NfsMultiActiveQos.validate_persisted(
            client1,
            session.nfs_service_id,
            export_bw,
            qos_type,
            session.qos_baseline_get,
            session.qos_baseline_block,
            "post_failover",
        )

        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _client, thread, error_box, _io_dir in io_sessions:
            thread.join(timeout=IO_JOIN_TIMEOUT)
            if thread.is_alive():
                raise OperationFailedError(
                    f"Background IO on {_client.hostname} did not stop within "
                    f"{IO_JOIN_TIMEOUT}s"
                )
            if error_box.get("exc") and not error_box.get("stopped"):
                raise OperationFailedError(
                    f"Background IO failed on {_client.hostname}: {error_box['exc']}"
                )
        io_sessions = []
        session.io_sessions = []

        backend_host, backend_port = session.haproxy.get_client_backend_endpoint(
            client1
        )
        log.info(
            "TC-2 QoS speed via VIP %s (pinned backend %s:%s, post_failover)",
            NFS_MOUNT,
            backend_host,
            backend_port,
        )
        NfsMultiActiveQos.validate_speeds(
            client1, NFS_MOUNT, export_bw, qos_type, "post_failover"
        )

        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir="tc2_post_failover",
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )

        log.info("%s passed", case_id)
        return 0
    except Exception as exc:
        log.error("%s failed: %s", case_id, exc)
        return 1
    finally:
        if io_sessions:
            try:
                NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
                for _client, thread, _error_box, _io_dir in io_sessions:
                    thread.join(timeout=IO_JOIN_TIMEOUT)
            except Exception as exc:
                log.warning("TC-2 IO session cleanup failed: %s", exc)
            session.io_sessions = []


def run_tc4_cqos_perclient_colocation_io(
    ceph_cluster, config, case_id, session=None, vip_slot=2
):
    """TC-4: per-client CQoS on colocated NFS instances with dual-client isolation."""
    session = session or IntegrationClusterSession()
    _ensure_tc4_colocation_cluster(
        ceph_cluster, config, session, case_id, vip_slot=vip_slot
    )

    client1 = session.client1
    client2 = session.client2
    export_bw = NfsMultiActiveQos.tc4_export_bw_config(config)
    cluster_bw = NfsMultiActiveQos.tc4_cluster_bw_config(config)
    client_bw = export_bw
    qos_type = config.get("tc4_qos_type", config.get("qos_type", DEFAULT_TC4_QOS_TYPE))
    qos_operation = config.get("qos_operation") or config.get(
        "control", DEFAULT_QOS_OPERATION
    )
    qos_settle = int(
        config.get("tc4_qos_settle", config.get("qos_settle", QOS_SETTLE_SEC))
    )
    poll_interval = session.poll_interval
    io_warmup = int(config.get("tc4_io_warmup", IO_WARMUP_SEC))
    stick_kwargs = {"stick_table_interval": poll_interval}
    clients = [client1, client2]
    io_sessions = []

    try:
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )
        NfsMultiActiveQos.assert_colocated_client_isolation(session, case_id)

        NfsMultiActiveQos.enable(
            session,
            cluster_bw,
            export_bw,
            qos_type,
            qos_operation,
        )
        qos_get, qos_block = NfsMultiActiveQos.wait_ready(
            client1,
            session.nfs_service_id,
            export_bw,
            qos_type,
            "pre_io",
            config=config,
        )
        session.qos_enabled = qos_type != "PerClient"
        session.qos_baseline_get = qos_get
        session.qos_baseline_block = qos_block
        if qos_settle > 0:
            log.info("TC-4 QoS settle %ss before speed validation", qos_settle)
            sleep(qos_settle)

        NfsMultiActiveQos.validate_perclient_speeds(
            clients,
            NFS_MOUNT,
            client_bw,
            qos_type,
            "pre_io",
            session=session,
        )

        io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
            session.mounted_clients,
            NFS_MOUNT,
            config,
            io_subdir="tc4_perclient_colocation_io",
        )
        session.io_sessions = io_sessions
        if io_warmup > 0:
            log.info("FIO warmup %ss before post-IO QoS validation", io_warmup)
            sleep(io_warmup)
        for client, thread, error_box, _io_dir in io_sessions:
            if error_box.get("exc"):
                raise OperationFailedError(
                    f"Background IO failed on {client.hostname}: {error_box['exc']}"
                )
            if not thread.is_alive():
                raise OperationFailedError(
                    f"Background IO stopped during warmup on {client.hostname}"
                )

        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _client, thread, error_box, _io_dir in io_sessions:
            thread.join(timeout=IO_JOIN_TIMEOUT)
            if thread.is_alive():
                raise OperationFailedError(
                    f"Background IO on {_client.hostname} did not stop within "
                    f"{IO_JOIN_TIMEOUT}s"
                )
            if error_box.get("exc") and not error_box.get("stopped"):
                raise OperationFailedError(
                    f"Background IO failed on {_client.hostname}: {error_box['exc']}"
                )
        io_sessions = []
        session.io_sessions = []

        NfsMultiActiveQos.assert_colocated_client_isolation(session, case_id)
        NfsMultiActiveQos.validate_persisted(
            client1,
            session.nfs_service_id,
            client_bw,
            qos_type,
            session.qos_baseline_get,
            session.qos_baseline_block,
            "post_io",
        )
        NfsMultiActiveQos.validate_perclient_speeds(
            clients,
            NFS_MOUNT,
            client_bw,
            qos_type,
            "post_io",
            session=session,
        )

        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir="tc4_post_io",
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )

        log.info("%s passed", case_id)
        return 0
    except Exception as exc:
        log.error("%s failed: %s", case_id, exc)
        return 1
    finally:
        if io_sessions:
            try:
                NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
                for _client, thread, _error_box, _io_dir in io_sessions:
                    thread.join(timeout=IO_JOIN_TIMEOUT)
            except Exception as exc:
                log.warning("TC-4 IO session cleanup failed: %s", exc)
            session.io_sessions = []


def _prepare_tc3_session(ceph_cluster, config, session, case_id, vip_slot=1):
    """Reuse the TC-1/TC-2 integration cluster for TC-3 delegation tests."""
    if session.cluster_deployed and session.colocation:
        log.info(
            "%s replacing colocation cluster with integration layout for TC-3",
            case_id,
        )
        session.cleanup()
    bootstrap_integration_cluster(
        ceph_cluster, config, session, case_id, vip_slot=vip_slot
    )
    session.nfs_cmd_host = session.nfs_nodes[0]
    if session.qos_enabled or session.cluster_qos_enabled:
        log.info("%s disabling QoS from prior phase before delegation tests", case_id)
        NfsMultiActiveQos.disable(session)
        session.qos_baseline_get = None
        session.qos_baseline_block = None


def _mount_tc3_clients_on_vip(session):
    """Mount both integration clients on the cluster VIP."""
    if len(session.mounted_clients) >= 2:
        return
    for client in (session.client1, session.client2):
        if client in session.mounted_clients:
            continue
        NfsMultiActiveClient.mount_via_vip(
            client,
            session.vip,
            NFS_MOUNT,
            EXPORT_NAME,
            str(session.ingress_frontend_port),
            NFS_VERSION,
        )
        session.mounted_clients.append(client)
    session.haproxy.validate_entries(
        mounted_clients=session.mounted_clients,
        stick_table_interval=session.poll_interval,
    )


def _tc3_delegation_rel_path(config, tag):
    base = config.get("tc3_delegation_rel_path", "tc3_delegation")
    return "%s/%s" % (base, tag)


def _tc3_delegation_hold_kwargs(config):
    return {
        "hold_seconds": int(
            config.get("tc3_delegation_hold_seconds", DEFAULT_TC3_DELEGATION_HOLD_SEC)
        ),
        "hold_ready_seconds": int(
            config.get(
                "tc3_delegation_hold_ready_seconds",
                DEFAULT_TC3_DELEGATION_HOLD_READY_SEC,
            )
        ),
    }


def _run_tc3_delegation_log_validation(session, client1, config, tag):
    """Validate export-level RW delegation via Ganesha logs on the VIP mount."""
    backend_hostname = session.haproxy.get_client_backend_hostname(client1)
    NfsMultiActiveDelegation.assert_rw_delegation_ganesha_logs(
        client1,
        session.delegation_installer,
        session.nfs_service_id,
        session.nfs_nodes,
        backend_hostname,
        NFS_MOUNT,
        config,
        tag,
    )


def run_tc3_rw_delegation_failover_io(
    ceph_cluster, config, case_id, session=None, vip_slot=1
):
    """TC-3: VIP-only delegation hold across NFS backend failover."""
    session = session or IntegrationClusterSession()
    _prepare_tc3_session(ceph_cluster, config, session, case_id, vip_slot=vip_slot)

    client1 = session.client1
    client2 = session.client2
    nfs_cmd_host = session.nfs_cmd_host
    poll_interval = session.poll_interval
    io_warmup = int(config.get("tc3_failover_io_warmup", IO_WARMUP_SEC))
    io_resume_timeout = int(config.get("io_resume_timeout", IO_RESUME_TIMEOUT))
    redeploy_wait = int(config.get("tc3_redeploy_wait", DEFAULT_TC3_REDEPLOY_WAIT))
    service_wait = int(
        config.get("tc3_service_wait_timeout", DEFAULT_TC3_SERVICE_WAIT_TIMEOUT)
    )
    stick_kwargs = {"stick_table_interval": poll_interval}
    timings = session.timings
    io_sessions = []
    cluster_ids = [session.nfs_service_id]

    try:
        NfsMultiActiveDelegation.prepare_nfs_hosts(ceph_cluster, session.nfs_nodes)
        (
            session.delegation_logging_template_backup,
            session.delegation_cephadm,
            session.delegation_installer,
        ) = NfsMultiActiveDelegation.enable_ganesha_debug_logging_for_clusters(
            nfs_cmd_host,
            ceph_cluster,
            cluster_ids,
            redeploy_wait=redeploy_wait,
            service_wait_timeout=service_wait,
        )
        NfsMultiActiveDelegation.enable_rw_export(
            nfs_cmd_host, session.nfs_service_id, EXPORT_NAME
        )
        NfsMultiActiveDelegation.assert_cluster_services_running(
            client1, session.nfs_service_id, "TC-3 cluster"
        )

        _mount_tc3_clients_on_vip(session)
        _run_tc3_delegation_log_validation(session, client1, config, "pre_failover")
        hold_path, hold_seed = NfsMultiActiveDelegation.hold_rw_delegation_on_mount(
            client1,
            NFS_MOUNT,
            _tc3_delegation_rel_path(config, "failover_hold"),
            **_tc3_delegation_hold_kwargs(config),
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )

        io_sessions = NfsMultiActiveClient.run_background_io_on_clients(
            session.mounted_clients,
            NFS_MOUNT,
            config,
            io_subdir="tc3_delegation_failover_io",
        )
        session.io_sessions = io_sessions
        if io_warmup > 0:
            log.info("FIO warmup %ss before NFS failover", io_warmup)
            sleep(io_warmup)
        for client, thread, error_box, _io_dir in io_sessions:
            if error_box.get("exc"):
                raise OperationFailedError(
                    f"Background IO failed on {client.hostname}: {error_box['exc']}"
                )
            if not thread.is_alive():
                raise OperationFailedError(
                    f"Background IO stopped during warmup on {client.hostname}"
                )

        pinned_nfs = session.haproxy.get_client_backend_hostname(client1)
        log.info(
            "%s NFS failover: drain pinned backend %s -> spare %s",
            case_id,
            pinned_nfs,
            session.spare_host,
        )
        timings["nfs_failover_triggered_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        def _wait_io_after_nfs_failover():
            NfsMultiActiveClient.wait_for_background_io_resume(
                io_sessions,
                NFS_MOUNT,
                config,
                timeout=io_resume_timeout,
                timings=timings,
                triggered_at=timings.get("nfs_failover_triggered_at"),
            )

        session.failover.failover_nfs_by_label(
            client1,
            session.deploy,
            session.nfs_spec,
            session.deploy_nodes,
            pinned_nfs,
            session.spare_host,
            session.labels["nfs"],
            session.nfs_count,
            deploy_timeout=session.deploy_timeout,
            timings=timings,
            timings_trigger_key="nfs_failover_triggered_at",
            post_apply_callback=_wait_io_after_nfs_failover,
        )
        log.info("%s background IO resumed after NFS failover", case_id)

        NfsMultiActiveDelegation.assert_delegation_access_after_failover(
            client1, client2, hold_path, hold_seed
        )
        release_wait = int(
            config.get(
                "tc3_delegation_release_wait",
                DEFAULT_TC3_DELEGATION_RELEASE_WAIT_SEC,
            )
        )
        release_delegation_hold_gracefully(client1, term_wait_seconds=release_wait)
        _run_tc3_delegation_log_validation(session, client1, config, "post_failover")

        NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
        for _client, thread, error_box, _io_dir in io_sessions:
            thread.join(timeout=IO_JOIN_TIMEOUT)
            if thread.is_alive():
                raise OperationFailedError(
                    f"Background IO on {_client.hostname} did not stop within "
                    f"{IO_JOIN_TIMEOUT}s"
                )
            if error_box.get("exc") and not error_box.get("stopped"):
                raise OperationFailedError(
                    f"Background IO failed on {_client.hostname}: {error_box['exc']}"
                )
        io_sessions = []
        session.io_sessions = []

        NfsMultiActiveDelegation.assert_export_delegation(
            nfs_cmd_host, session.nfs_service_id, EXPORT_NAME, "rw"
        )
        NfsMultiActiveClient.run_basic_io(
            client1,
            client2,
            NFS_MOUNT,
            file_count=int(config.get("io_file_count", 2)),
            io_subdir="tc3_post_failover",
        )
        session.haproxy.validate_entries(
            mounted_clients=session.mounted_clients,
            **stick_kwargs,
        )
        NfsMultiActiveDelegation.assert_cluster_services_running(
            client1, session.nfs_service_id, "TC-3 cluster post-failover"
        )

        log.info("%s passed", case_id)
        return 0
    except Exception as exc:
        log.error("%s failed: %s", case_id, exc)
        return 1
    finally:
        if io_sessions:
            try:
                NfsMultiActiveClient.stop_background_io_on_clients(io_sessions, config)
                for _client, thread, _error_box, _io_dir in io_sessions:
                    thread.join(timeout=IO_JOIN_TIMEOUT)
            except Exception as exc:
                log.warning("TC-3 IO session cleanup failed: %s", exc)
            session.io_sessions = []


def run(ceph_cluster, **kw):
    """Run NFS Multi-Active integration workflows (TC-1..)."""
    config = kw.get("config") or {}
    init_crash_monitoring(config)
    phases = config.get("workflows") or list(INTEGRATION_WORKFLOWS)
    for workflow_key in phases:
        if workflow_key not in INTEGRATION_WORKFLOWS:
            raise ConfigError(f"Unknown integration workflow {workflow_key!r}")

    session = IntegrationClusterSession()
    log.info("NFS Multi-Active integration tests | %d phase(s)", len(phases))
    try:
        for phase_idx, workflow_key in enumerate(phases, start=1):
            workflow = INTEGRATION_WORKFLOWS[workflow_key]
            vip_slot = int(workflow.get("vip", 1))
            log.info(
                "\n" + "=" * 70 + "\n"
                "Integration phase %s/%s | %s | %s | VIP slot %s\n"
                "%s\n" + "=" * 70,
                phase_idx,
                len(phases),
                workflow["tc"],
                workflow_key,
                vip_slot,
                workflow["desc"],
            )
            if workflow_key == "tc1_mds_failover_dual_client_fio":
                rc = run_tc1_mds_failover_dual_client_fio(
                    ceph_cluster,
                    config,
                    workflow["tc"],
                    session=session,
                    vip_slot=vip_slot,
                )
            elif workflow_key == "tc2_cqos_pershare_failover_io":
                rc = run_tc2_cqos_pershare_failover_io(
                    ceph_cluster, config, workflow["tc"], session=session
                )
            elif workflow_key == "tc3_rw_delegation_failover_io":
                rc = run_tc3_rw_delegation_failover_io(
                    ceph_cluster,
                    config,
                    workflow["tc"],
                    session=session,
                    vip_slot=vip_slot,
                )
            elif workflow_key == "tc4_cqos_perclient_colocation_io":
                rc = run_tc4_cqos_perclient_colocation_io(
                    ceph_cluster,
                    config,
                    workflow["tc"],
                    session=session,
                    vip_slot=vip_slot,
                )
            else:
                raise ConfigError(
                    f"No runner registered for integration workflow {workflow_key!r}"
                )
            if rc != 0:
                return rc
            try:
                wait_for_ceph_health(
                    ceph_cluster, config, context=f"post-{workflow['tc']}"
                )
            except ConfigError as exc:
                log.error("Post-%s health check failed: %s", workflow["tc"], exc)
                return 1

        try:
            wait_for_ceph_health(ceph_cluster, config, context="post-suite")
        except ConfigError as exc:
            log.error("Post-suite health check failed: %s", exc)
            return 1
        log.info("NFS Multi-Active integration test session completed successfully")
        return 0
    finally:
        session.cleanup()
