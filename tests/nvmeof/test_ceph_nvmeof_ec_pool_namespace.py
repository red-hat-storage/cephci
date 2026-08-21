"""
Test NVMeoF namespace creation on EC pools with --rbd-data-pool (BZ #76777).

Validates create-image, pre-created EC images, initiator IO, negative cases,
and namespace list/get/delete for EC-backed namespaces.
"""

import json
import random
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.parallel import parallel
from tests.nvmeof.test_ceph_nvmeof_gateway import initiators
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    validate_nvme_metadata,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

DEFAULT_EC_META_POOL = "rbd_ec_meta"
DEFAULT_EC_DATA_POOL = "rbd_ec_data"


def setup_ec_pools(rbd_obj, meta_pool, data_pool):
    """Create EC data pool and replicated metadata pool for RBD EC images."""
    LOG.info("Creating EC data pool %s", data_pool)
    rbd_obj.exec_cmd(
        cmd=f"ceph osd pool create {data_pool} 12 12 erasure default",
        check_ec=False,
    )
    rbd_obj.exec_cmd(cmd=f"ceph osd pool set {data_pool} allow_ec_overwrites true")
    rbd_obj.exec_cmd(cmd=f"rbd pool init {data_pool}")

    if not rbd_obj.check_pool_exists(pool_name=meta_pool):
        LOG.info("Creating EC metadata pool %s", meta_pool)
        if not rbd_obj.create_pool(poolname=meta_pool):
            raise RuntimeError(f"Failed to create metadata pool {meta_pool}")


def create_ec_image(rbd_obj, meta_pool, image, data_pool, size):
    """Create an RBD image on an EC layout (metadata + data pool)."""
    cmd = f"rbd create {meta_pool}/{image} --size {size} " f"--data-pool {data_pool}"
    if rbd_obj.exec_cmd(cmd=cmd):
        raise RuntimeError(f"Failed to create EC image {meta_pool}/{image}")


def verify_ec_image_data_pool(rbd_obj, meta_pool, image, data_pool):
    """Verify rbd info reports the expected EC data pool."""
    out = rbd_obj.exec_cmd(cmd=f"rbd info {meta_pool}/{image}", output=True)
    if data_pool not in out:
        raise AssertionError(
            f"Expected data pool {data_pool} in rbd info for "
            f"{meta_pool}/{image}, got: {out}"
        )
    LOG.info("Verified EC data pool %s for image %s/%s", data_pool, meta_pool, image)


def _namespace_from_get_response(ns_get_out):
    """Parse ns get JSON; response wraps namespace details in a list."""
    ns_get = json.loads(ns_get_out)
    if ns_get.get("namespaces"):
        return ns_get["namespaces"][0]
    return ns_get


def expect_namespace_add_failure(gateway, ns_args, expected_in_error=()):
    """Assert namespace add fails (e.g. invalid rbd-data-pool)."""
    hints = list(expected_in_error) or [
        "pool",
        "not found",
        "nonexistent",
        "does not exist",
        "invalid",
        "error",
        "fail",
    ]
    try:
        gateway.namespace.add(**ns_args)
    except Exception as exc:
        message = str(exc).lower()
        if any(hint in message for hint in hints):
            LOG.info("Expected namespace add failure: %s", exc)
            return
        raise
    raise AssertionError(f"Expected namespace add to fail but succeeded: {ns_args}")


def test_ec_pool_namespace_workflow(ceph_cluster, nvme_service, config, rbd_obj):
    """
    Run positive and negative EC pool namespace tests.

    Args:
        ceph_cluster: Ceph cluster object.
        nvme_service: NVMe service object.
        config: Test configuration dictionary.
        rbd_obj: RBD utility object for pool/image operations.

    Returns:
        int: 0 on success.
    """
    meta_pool = config.get("ec_meta_pool", DEFAULT_EC_META_POOL)
    data_pool = config.get("ec_data_pool", DEFAULT_EC_DATA_POOL)
    image_size = config.get("image_size", "1G")
    gateway = nvme_service.gateways[0]
    nvme_metadata_pool = config.get("nvme_metadata_pool", config.get("rbd_pool", "rbd"))
    gw_group = config.get("gw_group", "")

    subsystem_config = config.get("subsystems", [])
    if not subsystem_config:
        raise ValueError("Subsystem configuration is required")

    nqn = subsystem_config[0].get("nqn") or subsystem_config[0].get("subnqn")
    if not nqn:
        raise ValueError("Subsystem NQN is required")

    if config.get("setup_ec_pools", True):
        setup_ec_pools(rbd_obj, meta_pool, data_pool)

    create_image_name = f"ec-create-img-{random.randint(1000, 9999)}"
    precreate_image_name = f"ec-precreate-img-{random.randint(1000, 9999)}"

    # 1. Create namespace + image with rbd-data-pool
    LOG.info("Test: namespace add with rbd-create-image and rbd-data-pool")
    create_ns_args = {
        "args": {
            "subsystem": nqn,
            "rbd-pool": meta_pool,
            "rbd-data-pool": data_pool,
            "rbd-image": create_image_name,
            "size": image_size,
            "rbd-create-image": True,
        }
    }
    gateway.namespace.add(**create_ns_args)
    verify_ec_image_data_pool(rbd_obj, meta_pool, create_image_name, data_pool)

    ns_list_out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(ns_list_out).get("namespaces", [])
    create_ns = next(
        (ns for ns in namespaces if ns.get("rbd_image_name") == create_image_name),
        None,
    )
    if not create_ns:
        raise AssertionError(
            f"Namespace for image {create_image_name} not found in list output"
        )
    create_nsid = create_ns["nsid"]

    validate_nvme_metadata(
        cluster=ceph_cluster,
        config={
            "service": "namespace",
            "command": "add",
            "args": {"subsystem": nqn, "nsid": create_nsid},
        },
        pool=nvme_metadata_pool,
        group=gw_group,
    )

    ns_get_out, _ = gateway.namespace.get(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn, "nsid": create_nsid},
        }
    )
    namespace = _namespace_from_get_response(ns_get_out)
    if namespace.get("rbd_image_name") != create_image_name:
        raise AssertionError(f"namespace get returned wrong image: {namespace}")

    # 2. Pre-created EC image namespace (no create-image)
    LOG.info("Test: namespace add on pre-created EC image")
    create_ec_image(rbd_obj, meta_pool, precreate_image_name, data_pool, image_size)
    precreate_ns_args = {
        "args": {
            "subsystem": nqn,
            "rbd-pool": meta_pool,
            "rbd-image": precreate_image_name,
        }
    }
    gateway.namespace.add(**precreate_ns_args)
    verify_ec_image_data_pool(rbd_obj, meta_pool, precreate_image_name, data_pool)

    # 3. Negative: invalid rbd-data-pool
    LOG.info("Test: invalid rbd-data-pool should fail")
    invalid_image = f"ec-bad-dp-{random.randint(1000, 9999)}"
    expect_namespace_add_failure(
        gateway,
        {
            "args": {
                "subsystem": nqn,
                "rbd-pool": meta_pool,
                "rbd-data-pool": "nonexistent_ec_data_pool",
                "rbd-image": invalid_image,
                "size": image_size,
                "rbd-create-image": True,
            }
        },
    )

    # 4. Initiator IO on EC-backed namespaces
    if config.get("initiators"):
        LOG.info("Test: FIO on EC-backed namespaces")
        with parallel() as p:
            for initiator_cfg in config["initiators"]:
                p.spawn(initiators, ceph_cluster, gateway, initiator_cfg)

    # 5. Delete create-image namespace and verify OMAP cleanup
    LOG.info("Test: namespace delete for EC namespace")
    gateway.namespace.delete(
        **{
            "args": {
                "subsystem": nqn,
                "nsid": create_nsid,
                "force": True,
            }
        }
    )
    validate_nvme_metadata(
        cluster=ceph_cluster,
        config={
            "service": "namespace",
            "command": "delete",
            "args": {"subsystem": nqn, "nsid": create_nsid},
        },
        pool=nvme_metadata_pool,
        group=gw_group,
    )

    LOG.info("All EC pool namespace tests completed successfully")
    return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Execute EC pool NVMe namespace tests (BZ #76777).

    Args:
        ceph_cluster: Ceph cluster object.
        kwargs: Test configuration from suite YAML.

    Returns:
        int: 0 on success, 1 on failure.
    """
    config = deepcopy(kwargs["config"])
    nvme_service = None
    rbd_config = initial_rbd_config(**kwargs)
    if not rbd_config or "rbd_reppool" not in rbd_config:
        raise ValueError("Failed to initialize replicated RBD pool configuration")
    rbd_obj = rbd_config["rbd_reppool"]
    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            LOG.info("Deploy NVMeoF service")
            nvme_service.deploy()

        nvme_service.init_gateways()

        if config.get("cleanup-only"):
            teardown(nvme_service, rbd_obj)
            return 0

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        test_ec_pool_namespace_workflow(
            ceph_cluster=ceph_cluster,
            nvme_service=nvme_service,
            config=config,
            rbd_obj=rbd_obj,
        )
        return 0
    except Exception as err:
        LOG.error(err, exc_info=True)
    finally:
        if config.get("cleanup") and nvme_service:
            teardown(nvme_service, rbd_obj)
    return 1
