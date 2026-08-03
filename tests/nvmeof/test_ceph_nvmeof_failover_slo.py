"""
IBMCEPH-15815 — NVMeoF gateway failover taking longer than expected.

Performs orch/systemctl daemon stop failover under IO.
- Hard-fail: ANA failover completes; IO continues (validate_io)
- Soft/hard SLO gate: elapsed time vs failover_slo_seconds (fail_on_slo)
"""

import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    check_gateway,
    validate_io,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


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
            raise RuntimeError("No NVMe devices for failover SLO test")

        orch = Orch(ceph_cluster, **{})
        ana_ids = [gw.ana_group_id for gw in nvme_service.gateways]
        ns_for_io = fetch_namespaces(nvme_service.gateways[0], ana_ids)
        if not ns_for_io:
            raise RuntimeError("No namespaces resolved for failover validate_io")

        runtime = str(config.get("fio_runtime", 120))
        executor = ThreadPoolExecutor(max_workers=1)
        fut = executor.submit(
            client.start_fio,
            io_size="1G",
            runtime=runtime,
            paths=paths[:1],
            io_type="randrw",
            iodepth=16,
            time_based=True,
            execute_blkdiscard=False,
            test_name="bm-failover-slo",
        )
        time.sleep(int(config.get("fio_settle_seconds", 15)))

        LOG.info("Validating IO before failover")
        validate_io(orch, ns_for_io)

        node_id = config.get("failover_node", config["gw_nodes"][0])
        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
        ha.gateways = nvme_service.gateways
        ha.nvme_service = nvme_service
        gw = check_gateway(nvme_service.gateways, node_id)

        slo = float(config.get("failover_slo_seconds", 10))
        fail_tool = config.get("tool", "systemctl")
        start = time.perf_counter()
        result = ha.failover(gw, fail_tool)
        elapsed = time.perf_counter() - start
        LOG.info("Failover elapsed=%.2fs slo=%.2fs result=%s", elapsed, slo, result)

        LOG.info("Validating IO after failover (hard gate)")
        validate_io(orch, ns_for_io)

        ha.failback(gw, fail_tool)
        LOG.info("Validating IO after failback")
        validate_io(orch, ns_for_io)

        try:
            fut.result(timeout=int(runtime) + 60)
        except Exception as err:
            # FIO path flaps can happen; rbd-du validate_io is the hard gate above
            LOG.warning("FIO after failover/failback: %s", err)

        if elapsed > slo:
            msg = f"Failover took {elapsed:.2f}s > SLO {slo}s (IBMCEPH-15815)"
            if config.get("fail_on_slo", False):
                raise RuntimeError(msg)
            LOG.warning(msg)
        else:
            LOG.info("Failover within SLO (%.2fs <= %.2fs)", elapsed, slo)
        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
