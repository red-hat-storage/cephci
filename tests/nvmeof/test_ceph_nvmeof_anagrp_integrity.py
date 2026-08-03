"""
IBMCEPH-15769 — namespaces assigned wrong anagrpid (exceeds max GW count).

After configure (+ optional extra NS churn), assert every namespace
load_balancing_group / anagrp-id is within [1 .. num_gateways].
Missing ANA group ids on existing namespaces are treated as failures.
"""

import json

from ceph.ceph import Ceph
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def _ns_ana_group(ns):
    """Resolve ANA / load-balancing group id from namespace list JSON."""
    for key in (
        "load_balancing_group",
        "anagrp-id",
        "ana_group_id",
        "anagrpid",
        "ana_group",
    ):
        if ns.get(key) is not None:
            return int(ns[key])
    return None


def _assert_anagrp_bounds(gateway, subsystems, max_ana):
    violations = []
    missing = []
    checked = 0
    for sub in subsystems:
        nqn = sub.get("nqn") or sub.get("subnqn")
        out, _ = gateway.namespace.list(
            **{
                "base_cmd_args": {"format": "json"},
                "args": {"subsystem": nqn},
            }
        )
        namespaces = json.loads(out).get("namespaces", []) if out else []
        for ns in namespaces:
            checked += 1
            ana = _ns_ana_group(ns)
            if ana is None:
                missing.append({"subsystem": nqn, "nsid": ns.get("nsid")})
                continue
            if ana < 1 or ana > max_ana:
                violations.append(
                    {"subsystem": nqn, "nsid": ns.get("nsid"), "anagrp-id": ana}
                )
    if missing:
        raise RuntimeError(
            f"namespaces missing load_balancing_group/anagrp-id: {missing}"
        )
    if violations:
        raise RuntimeError(f"anagrpid out of range (max={max_ana}): {violations}")
    if checked == 0:
        raise RuntimeError("No namespaces found to validate anagrpid integrity")
    LOG.info("Checked %s namespaces; all anagrp-ids within 1..%s", checked, max_ana)
    return checked


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        max_ana = len(nvme_service.gateways)
        gateway = nvme_service.gateways[0]
        subsystems = config.get("subsystems", [])
        _assert_anagrp_bounds(gateway, subsystems, max_ana)

        # Optional churn: create extra namespaces and re-assert (catches assign-on-create bugs)
        extra = int(config.get("extra_ns_churn", 0))
        if extra > 0:
            pool = config.get("rbd_pool") or config.get("rep_pool_config", {}).get(
                "pool", "rbd"
            )
            nqn = subsystems[0].get("nqn") or subsystems[0].get("subnqn")
            size = config.get("extra_ns_size", "2G")
            LOG.info(
                "Creating %s extra namespaces on %s for anagrpid churn", extra, nqn
            )
            for _ in range(extra):
                img = f"{generate_unique_id(length=4)}-ana-churn"
                rbd_obj.create_image(pool, img, size)
                gateway.namespace.add(
                    **{
                        "args": {
                            "subsystem": nqn,
                            "rbd-pool": pool,
                            "rbd-image": img,
                        }
                    }
                )
            _assert_anagrp_bounds(gateway, subsystems, max_ana)

        return 0
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup") and nvme_service is not None:
            teardown(nvme_service, rbd_obj)
