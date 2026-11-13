"""
TLS-PSK single-group lifecycle.

Deploys one NVMe-oF service with TLS-PSK and runs the cases that share that
spec, then tears the service down once:

    connect, rotate, keep_connections, alb, dhchap, masking,
    restart, ha, listener_abort, mode_switch

``mode_switch`` is last because it deletes every subsystem and redeploys the
service. ``ha`` stops one gateway daemon; that daemon is started again before
the phase exits, and again before teardown if the phase fails midway.
``listener_abort`` (IBMCEPH-19520) force-deletes a peer listener under FIO.
"""

import threading
import time

from ceph.ceph import Ceph
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from tests.nvmeof.test_ceph_nvmeof_tlspsk_alb import test_ceph_tlspsk_alb
from tests.nvmeof.test_ceph_nvmeof_tlspsk_basic import (
    _setup_tlspsk_subsystem,
    test_ceph_tlspsk_basic_connect,
    test_ceph_tlspsk_plain_connect_refused,
)
from tests.nvmeof.test_ceph_nvmeof_tlspsk_dhchap import test_ceph_tlspsk_with_dhchap
from tests.nvmeof.test_ceph_nvmeof_tlspsk_gw_restart import (
    _nvmeof_daemons,
    _wait_until_gateways_serving,
    test_ceph_tlspsk_gw_restart_full_redeploy,
    test_ceph_tlspsk_gw_restart_single_daemon,
)
from tests.nvmeof.test_ceph_nvmeof_tlspsk_key_rotation import (
    _fio_error_count,
    test_ceph_tlspsk_keep_connections_rotation,
    test_ceph_tlspsk_key_rotation,
)
from tests.nvmeof.test_ceph_nvmeof_tlspsk_listener_delete_abort import (
    test_ceph_tlspsk_listener_delete_abort,
)
from tests.nvmeof.test_ceph_nvmeof_tlspsk_mtls_switch import (
    test_ceph_tlspsk_to_plain_to_tlspsk,
)
from tests.nvmeof.test_ceph_nvmeof_tlspsk_ns_masking import test_ceph_tlspsk_ns_masking
from tests.nvmeof.workflows.gateway_entities import teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    assert_plain_connect_refused,
    connect_with_tls_until,
    disconnect_nqn,
    rotate_psk,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

LIFECYCLE_PHASES = (
    "connect",
    "rotate",
    "keep_connections",
    "alb",
    "dhchap",
    "masking",
    "restart",
    "ha",
    "listener_abort",
    "mode_switch",
)

# rotate and restart reuse the subsystem created by connect.
PHASE_SUBSYSTEM_KEY = {
    "connect": "connect",
    "rotate": "connect",
    "keep_connections": "keep_connections",
    "alb": "alb",
    "dhchap": "dhchap",
    "masking": "masking",
    "restart": "connect",
    "ha": "ha",
    "listener_abort": "listener_abort",
    "mode_switch": "mode_switch",
}

_CREATING_PHASES = (
    "connect",
    "keep_connections",
    "alb",
    "dhchap",
    "masking",
    "ha",
    "listener_abort",
)


def execute_lifecycle(phases, deploy, run_phase, restore_daemons, teardown_service):
    """Deploy once, run *phases* in order, then restore daemons and tear down.

    A phase failure skips the remaining phases. Daemon restore and teardown
    still run. Teardown runs even when daemon restore raises.

    Args:
        phases (tuple): Phase names in execution order.
        deploy (callable): Bring up the NVMe-oF service.
        run_phase (callable): Run one phase. Receives the phase name.
        restore_daemons (callable): Start a daemon left stopped by the HA phase.
        teardown_service (callable): Remove subsystems, pool, and the service.
    """
    deploy()
    try:
        for phase in phases:
            run_phase(phase)
    finally:
        try:
            restore_daemons()
        finally:
            teardown_service()


def _select_subsystem(config, phase):
    """Point the shared service config at the subsystem for *phase*."""
    key = PHASE_SUBSYSTEM_KEY[phase]
    subsystems = config.get("phase_subsystems") or {}
    try:
        subsystem = subsystems[key]
    except KeyError as exc:
        raise KeyError(
            f"phase_subsystems.{key} is required for lifecycle phase {phase}"
        ) from exc
    config["subsystems"] = [subsystem]
    return subsystem


def _remember(created, subsystem):
    nqn = subsystem["nqn"]
    if nqn not in created:
        created.append(nqn)


def _daemon_name_for_node(daemons, node):
    """Return the nvmeof daemon name that belongs to *node*."""
    hostname = node.hostname
    short = hostname.split(".")[0]
    for daemon in daemons:
        name = daemon.get("daemon_name", "")
        if hostname in name or short in name:
            return name
    raise RuntimeError(f"No nvmeof daemon found for {hostname} in {daemons}")


def _start_daemon(ceph_cluster, nvme_service, daemon_name):
    """Start *daemon_name* and wait until every gateway is serving again."""
    orch = Orch(ceph_cluster, **{})
    before = _nvmeof_daemons(orch, nvme_service.service_name)
    LOG.info("Starting NVMe-oF daemon %s", daemon_name)
    Daemon(ceph_cluster, **{}).op("start", {"pos_args": [daemon_name]})
    _wait_until_gateways_serving(orch, nvme_service, before, timeout=180)


def _delete_subsystems(nvme_service, nqns):
    """Force-delete each subsystem. Missing subsystems are ignored."""
    if not nqns or not getattr(nvme_service, "gateways", None):
        return
    gateway = nvme_service.gateways[0]
    for nqn in nqns:
        try:
            gateway.subsystem.delete(**{"args": {"subsystem": nqn, "force": True}})
            LOG.info("Deleted subsystem %s", nqn)
        except Exception as exc:
            LOG.warning("Could not delete subsystem %s: %s", nqn, exc)


def _disconnect_nqns(initiator_node, nqns):
    for nqn in nqns:
        try:
            disconnect_nqn(initiator_node, nqn)
        except Exception as exc:
            LOG.info("Disconnect %s: %s", nqn, exc)


def _run_ha_phase(ceph_cluster, config, nvme_service, psk_info, ctx):
    """Fail a gateway over under FIO, rotate the host PSK, then fail back.

    The stopped daemon name is stored on *ctx*. The caller starts it again
    if this phase raises before its own restart completes.
    """
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(initiator_node)
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    gw1 = nvme_service.gateways[0]
    gw2 = nvme_service.gateways[1]
    gw1_ip = gw1.node.ip_address
    gw2_ip = gw2.node.ip_address
    # host add lands on one gateway immediately. The peer copies the PSK from
    # OMAP on state_update_interval_sec.
    psk_sync_wait = config.get("state_update_interval_sec", 30) + 15

    def _connect_listeners():
        for ip in (gw1_ip, gw2_ip):
            connect_with_tls_until(
                initiator_node,
                ip,
                listener_port,
                nqn,
                timeout=psk_sync_wait,
            )

    _connect_listeners()

    fio_runtime = config.get("ha_fio_runtime", 90)
    fio_box = {"results": None, "error": None}

    def _run_fio():
        try:
            fio_box["results"] = initiator.start_fio(
                runtime=fio_runtime,
                serial=False,
                io_type="write",
                time_based=True,
                test_name="lifecycle-ha",
            )
        except Exception as exc:
            fio_box["error"] = exc

    fio_thread = threading.Thread(target=_run_fio, daemon=True)
    fio_thread.start()
    time.sleep(10)

    orch = Orch(ceph_cluster, **{})
    daemons = _nvmeof_daemons(orch, nvme_service.service_name)
    daemon_name = _daemon_name_for_node(daemons, gw1.node)
    ctx["stopped_daemon"] = daemon_name
    LOG.info("HA: stopping %s", daemon_name)
    Daemon(ceph_cluster, **{}).op("stop", {"pos_args": [daemon_name]})
    try:
        time.sleep(15)
        if not fio_thread.is_alive() and fio_box["error"]:
            raise AssertionError(
                f"FIO failed as soon as the gateway stopped: {fio_box['error']}"
            )
        assert_plain_connect_refused(
            initiator_node, gw2_ip, listener_port, nqn, context="HA surviving gateway"
        )
        fio_thread.join(timeout=fio_runtime + 60)
        if fio_box["error"]:
            raise AssertionError(f"FIO failed during HA failover: {fio_box['error']}")
        errors = _fio_error_count(fio_box["results"])
        if errors:
            raise AssertionError(f"FIO write errors during HA failover: {errors}")
        try:
            disconnect_nqn(initiator_node, nqn)
        except Exception as exc:
            LOG.info("HA disconnect before rotation: %s", exc)
        LOG.info("HA: rotating host PSK while %s is down", daemon_name)
        rotate_psk(
            initiator_node,
            gw2,
            nqn,
            psk_info["hostnqn"],
            [gw2.node],
            new_key_path="/root/encryption_ha_rotate.key",
            trsvcid=listener_port,
        )
    finally:
        if ctx.get("stopped_daemon"):
            _start_daemon(ceph_cluster, nvme_service, ctx["stopped_daemon"])
            ctx["stopped_daemon"] = None

    _connect_listeners()
    initiator.start_fio(
        io_size="128m",
        serial=True,
        io_type="read",
        execute_blkdiscard=False,
        test_name="lifecycle-ha-failback",
    )
    disconnect_nqn(initiator_node, nqn)
    for ip in (gw1_ip, gw2_ip):
        assert_plain_connect_refused(
            initiator_node, ip, listener_port, nqn, context="HA after failback"
        )
    LOG.info("HA phase passed")


def _run_phases(ceph_cluster, config, ctx):
    """Return a phase runner. The service is read from *ctx* after deploy."""

    def run_phase(phase):
        LOG.info("Lifecycle phase start: %s", phase)
        nvme_service = ctx["nvme_service"]
        rbd_obj = ctx["rbd_obj"]
        subsystem = _select_subsystem(config, phase)
        initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])

        if phase == "mode_switch":
            _disconnect_nqns(initiator_node, ctx["created"])
            _delete_subsystems(nvme_service, ctx["created"])
            ctx["created"].clear()
            _remember(ctx["created"], subsystem)
            test_ceph_tlspsk_to_plain_to_tlspsk(
                ceph_cluster, config, nvme_service, rbd_obj
            )
            LOG.info("Lifecycle phase passed: %s", phase)
            return

        if phase in _CREATING_PHASES:
            _remember(ctx["created"], subsystem)

        if phase == "connect":
            ctx["psk"] = _setup_tlspsk_subsystem(
                ceph_cluster, config, nvme_service, initiator_node, rbd_obj
            )
            test_ceph_tlspsk_basic_connect(
                ceph_cluster, config, nvme_service, ctx["psk"]
            )
            test_ceph_tlspsk_plain_connect_refused(
                ceph_cluster, config, nvme_service, ctx["psk"]
            )
        elif phase == "rotate":
            test_ceph_tlspsk_key_rotation(
                ceph_cluster, config, nvme_service, ctx["psk"]
            )
        elif phase == "keep_connections":
            psk = _setup_tlspsk_subsystem(
                ceph_cluster, config, nvme_service, initiator_node, rbd_obj
            )
            test_ceph_tlspsk_keep_connections_rotation(
                ceph_cluster, config, nvme_service, psk
            )
        elif phase == "alb":
            test_ceph_tlspsk_alb(ceph_cluster, config, nvme_service, rbd_obj)
        elif phase == "dhchap":
            test_ceph_tlspsk_with_dhchap(ceph_cluster, config, nvme_service, rbd_obj)
        elif phase == "masking":
            test_ceph_tlspsk_ns_masking(ceph_cluster, config, nvme_service, rbd_obj)
        elif phase == "restart":
            test_ceph_tlspsk_gw_restart_single_daemon(
                ceph_cluster, config, nvme_service, ctx["psk"]
            )
            test_ceph_tlspsk_gw_restart_full_redeploy(
                ceph_cluster, config, nvme_service, ctx["psk"]
            )
        elif phase == "ha":
            if len(nvme_service.gateways) < 2:
                raise RuntimeError("HA phase needs two gateways")
            psk = _setup_tlspsk_subsystem(
                ceph_cluster, config, nvme_service, initiator_node, rbd_obj
            )
            _run_ha_phase(ceph_cluster, config, nvme_service, psk, ctx)
        elif phase == "listener_abort":
            if len(nvme_service.gateways) < 2:
                raise RuntimeError("listener_abort phase needs two gateways")
            psk = _setup_tlspsk_subsystem(
                ceph_cluster, config, nvme_service, initiator_node, rbd_obj
            )
            test_ceph_tlspsk_listener_delete_abort(
                ceph_cluster, config, nvme_service, psk
            )
        else:
            raise RuntimeError(f"Unknown lifecycle phase {phase}")

        LOG.info("Lifecycle phase passed: %s", phase)

    return run_phase


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Deploy TLS-PSK once, run every single-group phase, tear down once.

    Config keys:
        phase_subsystems (dict): Subsystem spec keyed by connect,
            keep_connections, alb, dhchap, masking, ha, listener_abort,
            and mode_switch.
        install (bool): Deploy the NVMe-oF service when True.
        tls_psk (bool): Enable TLS-PSK on the service.
        cleanup (list): Passed through to teardown.
    """
    config = kwargs["config"]
    custom_cfg = kwargs.get("test_data", {}).get("custom-config")
    config.update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )
    ctx = {
        "nvme_service": None,
        "rbd_obj": None,
        "created": [],
        "stopped_daemon": None,
        "psk": None,
    }

    def deploy():
        ctx["rbd_obj"] = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)
        # Subsystems are created per phase. An empty list keeps deploy from
        # walking phase_subsystems, which is a dict rather than a list.
        config["subsystems"] = []
        service = NVMeService(config, ceph_cluster)
        ctx["nvme_service"] = service
        if config.get("install", True):
            service.deploy()
        service.init_gateways()
        service.wait_for_gateways_ready()

    def restore_daemons():
        daemon_name = ctx.get("stopped_daemon")
        service = ctx.get("nvme_service")
        if not daemon_name or service is None:
            return
        LOG.info("Lifecycle restore: starting %s", daemon_name)
        _start_daemon(ceph_cluster, service, daemon_name)
        ctx["stopped_daemon"] = None

    def teardown_service():
        service = ctx.get("nvme_service")
        rbd_obj = ctx.get("rbd_obj")
        if not (config.get("cleanup") and service is not None and rbd_obj is not None):
            return
        config["subsystems"] = [{"nqn": nqn} for nqn in ctx["created"]]
        teardown(service, rbd_obj)

    try:
        execute_lifecycle(
            LIFECYCLE_PHASES,
            deploy,
            _run_phases(ceph_cluster, config, ctx),
            restore_daemons,
            teardown_service,
        )
        return 0
    except Exception as err:
        LOG.exception("TLS-PSK lifecycle FAILED: %s", err)
        return 1
