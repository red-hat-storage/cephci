"""
IBMCEPH-15819 / 16124 / 16125 / 17250 / 16261 / 17007

Host maintenance enter/exit with continuous NVMeoF IO, optionally using
nvme-gw disable/enable around maintenance for faster failover/failback.

Hard assertions:
- FIO / rbd-du IO continues during and after maintenance
- Target GW returns to Availability=AVAILABLE after exit (+ enable)
- nvme-gw disable/enable succeed when gw_disable_enable is requested
- Peer GWs do not crash/restart during the maintenance window
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from ceph.waiter import WaitUntil
from cli.ops.host import host_maintenance_enter, host_maintenance_exit
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    ana_states,
    check_and_set_nvme_cli_image,
    check_gateway,
    check_gateway_availability,
    get_optimized_state,
    validate_io,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _nvme_gw_cmd(installer, *args):
    cmd = "ceph nvme-gw " + " ".join(str(a) for a in args)
    return installer.exec_command(cmd=cmd, sudo=True)


def _snapshot_nvmeof_daemons(installer):
    """Return {daemon_name: {container_id, started, status, host}} for restart detection."""
    out, _ = installer.exec_command(
        cmd="ceph orch ps --daemon_type nvmeof --format json", sudo=True
    )
    rows = json.loads(out or "[]")
    snap = {}
    for r in rows:
        name = r.get("daemon_name") or ""
        if "nvmeof" not in name:
            continue
        snap[name] = {
            "container_id": r.get("container_id") or r.get("container_id_short"),
            "started": r.get("started") or r.get("start_time"),
            "status": r.get("status_desc") or r.get("status"),
            "host": r.get("hostname"),
        }
    return snap


def _assert_no_peer_restarts(before, after, exclude_hosts=None):
    """Fail if peer nvmeof daemons restarted (container_id / started changed)."""
    exclude_hosts = {h.lower() for h in (exclude_hosts or [])}
    restarted = []
    for name, prev in before.items():
        host = (prev.get("host") or "").lower()
        if host in exclude_hosts:
            continue
        cur = after.get(name)
        if not cur:
            restarted.append({"daemon": name, "reason": "missing after maintenance"})
            continue
        status = str(cur.get("status", "")).lower()
        if (
            status
            and status not in ("running", "1", "active")
            and "running" not in status
        ):
            restarted.append(
                {"daemon": name, "reason": f"unhealthy status={cur.get('status')}"}
            )
            continue
        if prev.get("container_id") and cur.get("container_id"):
            if prev["container_id"] != cur["container_id"]:
                restarted.append(
                    {
                        "daemon": name,
                        "reason": "container_id changed",
                        "before": prev["container_id"],
                        "after": cur["container_id"],
                    }
                )
                continue
        if (
            prev.get("started")
            and cur.get("started")
            and prev["started"] != cur["started"]
        ):
            restarted.append(
                {
                    "daemon": name,
                    "reason": "started timestamp changed",
                    "before": prev["started"],
                    "after": cur["started"],
                }
            )
    if restarted:
        raise RuntimeError(f"Peer NVMeoF daemons restarted/unhealthy: {restarted}")


def _ns_for_validate_io(gateway, gateways):
    ana_ids = [gw.ana_group_id for gw in gateways]
    return fetch_namespaces(gateway, ana_ids)


def _wait_gw_available(nvme_service, orch, ana_id, timeout=300, interval=10):
    for w in WaitUntil(timeout=timeout, interval=interval):
        if check_gateway_availability(nvme_service, ana_id, orch, state="AVAILABLE"):
            return True
        LOG.warning("ANA group %s not yet AVAILABLE after maintenance", ana_id)
    if w.expired:
        states = ana_states(nvme_service, orch)
        raise TimeoutError(
            f"Gateway ANA {ana_id} not AVAILABLE within {timeout}s: {states}"
        )
    return False


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    executor = None
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        clients = prepare_io_execution(
            config["initiators"],
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        client = clients[0]
        paths = client.list_spdk_drives() or client.list_devices()
        if not paths:
            raise RuntimeError("No NVMe devices for maintenance IO test")

        installer = ceph_cluster.get_nodes(role="installer")[0]
        orch = Orch(ceph_cluster, **{})
        pool = config.get("rbd_pool") or config.get("nvme_metadata_pool") or "rbd"
        group = config.get("gw_group", "group1")
        use_disable = config.get("gw_disable_enable", True)
        soft_disable = config.get("soft_disable_enable", False)

        maint_node_id = config.get("maintenance_node", config["gw_nodes"][0])
        maint_node = get_node_by_id(ceph_cluster, maint_node_id)
        maint_gw = check_gateway(nvme_service.gateways, maint_node_id)
        ns_for_io = _ns_for_validate_io(nvme_service.gateways[0], nvme_service.gateways)
        if not ns_for_io:
            raise RuntimeError("No namespaces resolved for validate_io")

        before_daemons = _snapshot_nvmeof_daemons(installer)
        LOG.info("NVMeoF daemons before maintenance: %s", before_daemons)

        runtime = str(config.get("fio_runtime", 300))
        executor = ThreadPoolExecutor(max_workers=1)
        fut = executor.submit(
            client.start_fio,
            io_size="1G",
            runtime=runtime,
            paths=paths[:2],
            io_type=config.get("io_type", "randrw"),
            iodepth=16,
            time_based=True,
            execute_blkdiscard=False,
            test_name="bm-maint-under-io",
        )
        time.sleep(int(config.get("fio_settle_seconds", 20)))

        LOG.info("Validating IO before maintenance")
        validate_io(orch, ns_for_io)

        if use_disable:
            LOG.info("Disable NVMe GW on %s before maintenance", maint_node.hostname)
            try:
                _nvme_gw_cmd(installer, "disable", pool, group, maint_node.hostname)
            except Exception as err:
                if soft_disable:
                    LOG.warning("nvme-gw disable failed/unsupported: %s", err)
                else:
                    raise RuntimeError(
                        f"nvme-gw disable failed on {maint_node.hostname}: {err}"
                    ) from err

        LOG.info("Enter maintenance on %s", maint_node.hostname)
        ok = host_maintenance_enter(
            installer,
            maint_node.hostname,
            force=True,
            yes_i_really_mean_it=True,
            timeout=int(config.get("maint_timeout", 600)),
        )
        if not ok:
            raise RuntimeError(f"Failed to enter maintenance on {maint_node.hostname}")

        # During maintenance peer GWs must keep serving; rbd-du should still advance
        time.sleep(int(config.get("maint_hold_seconds", 60)))
        LOG.info("Validating IO during maintenance")
        validate_io(orch, ns_for_io)

        LOG.info("Exit maintenance on %s", maint_node.hostname)
        ok = host_maintenance_exit(
            installer,
            maint_node.hostname,
            timeout=int(config.get("maint_timeout", 600)),
        )
        if not ok:
            raise RuntimeError(f"Failed to exit maintenance on {maint_node.hostname}")

        if use_disable:
            LOG.info("Enable NVMe GW on %s after maintenance", maint_node.hostname)
            try:
                _nvme_gw_cmd(installer, "enable", pool, group, maint_node.hostname)
            except Exception as err:
                if soft_disable:
                    LOG.warning("nvme-gw enable failed/unsupported: %s", err)
                else:
                    raise RuntimeError(
                        f"nvme-gw enable failed on {maint_node.hostname}: {err}"
                    ) from err

        settle = int(config.get("post_maint_settle", 60))
        time.sleep(settle)

        # Hard: recovered GW must be AVAILABLE (IBMCEPH-15819 class of failures)
        _wait_gw_available(
            nvme_service,
            orch,
            maint_gw.ana_group_id,
            timeout=int(config.get("avail_timeout", 300)),
        )
        states = ana_states(nvme_service, orch)
        LOG.info("nvme-gw ANA states after maintenance: %s", states)

        # Soft-ish: prefer optimized path ownership returning to recovered GW
        active = get_optimized_state(nvme_service, orch, maint_gw.ana_group_id)
        if not active:
            raise RuntimeError(
                f"No ACTIVE/optimized path for ANA {maint_gw.ana_group_id} after maintenance"
            )
        LOG.info(
            "Optimized path(s) for recovered ANA %s: %s",
            maint_gw.ana_group_id,
            active,
        )

        LOG.info("Validating IO after maintenance exit")
        validate_io(orch, ns_for_io)

        after_daemons = _snapshot_nvmeof_daemons(installer)
        _assert_no_peer_restarts(
            before_daemons, after_daemons, exclude_hosts=[maint_node.hostname]
        )

        fut.result(timeout=int(runtime) + 120)
        LOG.info("Maintenance-under-IO completed successfully")
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
