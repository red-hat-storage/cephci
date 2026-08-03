"""
IBMCEPH-16374 — after scale-down, removed GW must not remain Availability=DELETING.

Scales down NVMeoF placement, optionally validating IO continuity, then polls
`ceph nvme-gw show` until DELETING is gone. Remaining gateways must be AVAILABLE
and NVMEOF_GATEWAY_DELETING health warning must clear.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from cli.utilities.utils import get_nodes_by_ids
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image, validate_io
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def _gw_rows(show):
    if isinstance(show, dict):
        return show.get("Created Gateways:") or show.get("gateways") or []
    return []


def _deleting_rows(show):
    return [
        g
        for g in _gw_rows(show)
        if str(g.get("Availability", "")).upper() == "DELETING"
        or "DELETING" in str(g.get("Availability", "")).upper()
    ]


def _unavailable_remaining(show, remaining_hostnames):
    remaining = {h.lower() for h in remaining_hostnames}
    bad = []
    for g in _gw_rows(show):
        gw_id = str(g.get("gw-id") or g.get("name") or "").lower()
        host = str(g.get("hostname") or "").lower()
        # gw-id often embeds hostname
        matched = host in remaining or any(h in gw_id for h in remaining)
        if not matched:
            continue
        avail = str(g.get("Availability", "")).upper()
        if avail and avail != "AVAILABLE":
            bad.append(g)
    return bad


def _health_has_deleting(installer):
    out, _ = installer.exec_command(cmd="ceph health detail --format json", sudo=True)
    try:
        health = json.loads(out or "{}")
    except Exception:
        return "NVMEOF_GATEWAY_DELETING" in (out or "")
    checks = health.get("checks") or {}
    return "NVMEOF_GATEWAY_DELETING" in checks


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

        nodes = config.get("scale_down_nodes") or [config["gw_nodes"][-1]]
        if not isinstance(nodes, list):
            nodes = [nodes]

        remaining = list(set(nvme_service.config["gw_nodes"]) - set(nodes))
        if not remaining:
            raise RuntimeError("scale_down_nodes would remove all gateways")

        orch = Orch(ceph_cluster, **{})
        pool = config.get("rbd_pool", "rbd")
        group = config.get("gw_group", "group1")
        ns_for_io = None
        if config.get("initiators"):
            clients = prepare_io_execution(
                config["initiators"],
                gateways=nvme_service.gateways,
                cluster=ceph_cluster,
                return_clients=True,
            )
            client = clients[0]
            paths = client.list_spdk_drives() or client.list_devices()
            if not paths:
                raise RuntimeError("No NVMe devices for scale-down IO")
            ana_ids = [gw.ana_group_id for gw in nvme_service.gateways]
            ns_for_io = fetch_namespaces(nvme_service.gateways[0], ana_ids)
            runtime = str(config.get("fio_runtime", 180))
            executor = ThreadPoolExecutor(max_workers=1)
            executor.submit(
                client.start_fio,
                io_size="1G",
                runtime=runtime,
                paths=paths[:1],
                io_type="randrw",
                iodepth=8,
                time_based=True,
                execute_blkdiscard=False,
                test_name="bm-scale-deleting",
            )
            time.sleep(int(config.get("fio_settle_seconds", 15)))
            if ns_for_io:
                LOG.info("Validating IO before scale-down")
                validate_io(orch, ns_for_io)

        LOG.info("Scaling down gateways %s; remaining %s", nodes, remaining)
        nvme_service.config["gw_nodes"] = remaining
        nvme_service.gw_nodes = get_nodes_by_ids(ceph_cluster, remaining)
        nvme_service.deploy()
        nvme_service.gateways = []
        nvme_service.init_gateways()

        remaining_hostnames = [n.hostname for n in nvme_service.gw_nodes]
        timeout = int(config.get("deleting_timeout", 300))
        end = time.time() + timeout
        last = None
        saw_deleting = False
        while time.time() < end:
            out, _ = orch.shell(args=["ceph", "nvme-gw", "show", pool, repr(group)])
            try:
                last = json.loads(out)
            except Exception:
                last = out

            deleting = _deleting_rows(last) if isinstance(last, dict) else []
            blob = (
                json.dumps(last).upper() if not isinstance(last, str) else last.upper()
            )
            if deleting or "DELETING" in blob:
                saw_deleting = True
                LOG.warning("Still seeing DELETING in nvme-gw show; retrying...")
                time.sleep(15)
                continue

            # DELETING cleared — remaining must be AVAILABLE
            bad = (
                _unavailable_remaining(last, remaining_hostnames)
                if isinstance(last, dict)
                else []
            )
            if bad:
                LOG.warning("Remaining GWs not yet AVAILABLE: %s", bad)
                time.sleep(15)
                continue

            # Health warning should clear after autoload finishes
            installer = ceph_cluster.get_nodes(role="installer")[0]
            if _health_has_deleting(installer):
                LOG.warning("NVMEOF_GATEWAY_DELETING still in ceph health; retrying...")
                time.sleep(15)
                continue

            if ns_for_io:
                # Namespaces may have rebalanced; refresh from a remaining GW
                ana_ids = [gw.ana_group_id for gw in nvme_service.gateways]
                ns_after = (
                    fetch_namespaces(nvme_service.gateways[0], ana_ids) or ns_for_io
                )
                LOG.info("Validating IO after scale-down")
                validate_io(orch, ns_after)

            LOG.info(
                "Scale-down cleared DELETING (observed_transient=%s); remaining AVAILABLE",
                saw_deleting,
            )
            return 0

        raise RuntimeError(
            f"Gateway still DELETING / unhealthy after {timeout}s: {last}"
        )
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
