"""
RHEL initiator CNC / XCOPY tests for Ceph NVMe-oF.

Config-driven operations:
  - cross_ns_copy_verify   (TC-01)
  - ana_cnc                (TC-07 RHEL)
  - full_volume_integrity  (TC-09)
  - perf_cnc_vs_host_rw    (TC-06)
  - cnc_soak               (TC-10 RHEL)
  - spec_cnc_enable_perf   (orch-spec cnc_enable timing A/B)
  - spec_cnc_params_exercise (orch-spec rate/chunk/parallel)
"""

from ceph.ceph import Ceph
from tests.nvmeof.workflows.cnc import OPERATIONS
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

# Ops that re-apply orch nvmeof spec and need the NVMeService handle
_SPEC_OPS = {"spec_cnc_enable_perf", "spec_cnc_params_exercise"}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Execute a RHEL CNC/XCOPY operation against Ceph NVMe-oF.

    Example::

        config:
            gw_nodes: [node5, node6]
            rbd_pool: rbd
            install: true
            operation: cross_ns_copy_verify
            subsystems:
              - nqn: nqn.2016-06.io.spdk:cnode1
                bdevs: [{count: 2, size: 10G, ns_create_image: true}]
                listeners: [node5, node6]
                listener_port: 4420
                allow_host: "*"
            initiators:
              - nqn: connect-all
                listener_port: 4420
                node: node10
            cleanup: [pool, gateway, initiators, subsystems]
    """
    config = kwargs["config"]
    operation = config.get("operation")
    if operation not in OPERATIONS:
        raise ValueError(
            f"Unknown or missing operation={operation}. "
            f"Supported: {list(OPERATIONS)}"
        )

    rbd_pools = initial_rbd_config(**kwargs)
    if not rbd_pools or not rbd_pools.get("rbd_reppool"):
        raise Exception(
            "RBD pool setup failed (initial_rbd_config returned "
            f"{rbd_pools}). Check earlier 'Pool creation failed' logs — "
            "usually `ceph osd pool create` failed due to capacity/PG limits "
            "or CLI access from the client node."
        )
    rbd_obj = rbd_pools["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    try:
        nvme_service = NVMeService(config, ceph_cluster)

        if config.get("install"):
            nvme_service.deploy()

        nvme_service.init_gateways()

        if config.get("cleanup-only"):
            teardown(nvme_service, rbd_obj)
            return 0

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        clients = prepare_io_execution(
            config.get("initiators", []),
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        if not clients:
            raise Exception("No NVMe initiators connected")

        initiator = clients[0]
        LOG.info(f"Running CNC operation '{operation}' on {initiator.node.hostname}")
        op_kwargs = {}
        if operation in _SPEC_OPS:
            op_kwargs["nvme_service"] = nvme_service
        OPERATIONS[operation](
            initiator, nvme_service.gateways, config, **op_kwargs
        )
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)

    return 1
