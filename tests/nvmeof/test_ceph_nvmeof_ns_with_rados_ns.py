import json
import random
from copy import deepcopy

from ceph.ceph import Ceph
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def test_ceph_nvmeof_ns_with_rados_ns(ceph_cluster, nvme_service, config, rbd_obj):
    """
    Test Ceph NVMeoF with Rados Namespace by:
    1. Creating 2 RADOS namespaces in the pool
    2. Creating NVMe namespaces in each subsystem using both RADOS namespaces
    3. Testing same image name in different RADOS namespaces (should succeed)
    4. Testing duplicate image in same RADOS namespace (should fail)

    Args:
        ceph_cluster: Ceph cluster object.
        nvme_service: NVMe service object.
        config: Configuration dictionary containing test parameters.
        rbd_obj: RBD utility object used to create backing images.

    Returns:
        int: 0 on success, raises exception on failure
    """
    subsystem_config = config.get("subsystems", [])
    if not subsystem_config:
        raise ValueError("Subsystem configuration is required to add a namespace")

    gateway = nvme_service.gateways[0]
    pool = config.get("rbd_pool", "rbd")

    # Get image size from config, default to first subsystem's bdev size
    image_size = config.get("image_size")
    if not image_size and config.get("subsystems"):
        # Try to get size from first subsystem's bdevs
        first_subsys = config["subsystems"][0]
        if first_subsys.get("bdevs") and first_subsys["bdevs"].get("size"):
            image_size = first_subsys["bdevs"]["size"]

    # Step 1: Create 2 RADOS namespaces
    rados_ns1 = config.get("rados_ns1", "rados_ns_1")
    rados_ns2 = config.get("rados_ns2", "rados_ns_2")

    LOG.info(f"Creating RADOS namespace: {rados_ns1}")
    rbd_obj.create_namespace(pool_name=pool, namespace_name=rados_ns1)

    LOG.info(f"Creating RADOS namespace: {rados_ns2}")
    rbd_obj.create_namespace(pool_name=pool, namespace_name=rados_ns2)

    # Step 2: Create NVMe namespaces in each subsystem using both RADOS namespaces
    for idx, sub_cfg in enumerate(subsystem_config):
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        if not nqn:
            LOG.warning(f"Subsystem {idx} missing NQN, skipping")
            continue

        # Add NVMe namespaces in both RADOS namespaces
        for ns_num, rados_ns in enumerate([rados_ns1, rados_ns2], start=1):
            rbd_image = f"image-subsys{idx}-ns{ns_num}-{random.randint(1000, 9999)}"
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rados-namespace": rados_ns,
                    "rbd-image": rbd_image,
                    "size": image_size,
                    "rbd-create-image": True,
                }
            }
            LOG.info(
                f"Adding NVMe namespace for subsystem '{nqn}' with "
                f"image '{rbd_image}' in RADOS namespace '{rados_ns}'"
            )
            out, err = gateway.namespace.add(**ns_args)
            LOG.info(
                f"Successfully added NVMe namespace for subsystem '{nqn}' with "
                f"image '{rbd_image}' from RADOS namespace '{rados_ns}'. "
                f"Output: {out}"
            )

    # Step 3: Test creating same image name in different RADOS namespaces (should succeed)
    LOG.info("Test: Creating same image name in different RADOS namespaces")
    common_image_name = f"common-image-{random.randint(1000, 9999)}"

    # Add NVMe namespaces for these common images
    if subsystem_config:
        nqn = subsystem_config[0].get("nqn") or subsystem_config[0].get("subnqn")
        if nqn:
            # Add from default namespace
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rbd-image": common_image_name,
                    "size": image_size,
                    "rbd-create-image": True,
                }
            }
            LOG.info(
                f"Adding NVMe namespace with image "
                f"'{common_image_name}' in default namespace"
            )
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            else:
                LOG.info(
                    f"Successfully added NVMe namespace for subsystem '{nqn}' "
                    f"with '{common_image_name}' in default namespace. "
                    f"Output: {out}"
                )

            # Add from both RADOS namespaces
            for rados_ns in [rados_ns1, rados_ns2]:
                ns_args = {
                    "args": {
                        "subsystem": nqn,
                        "rbd-pool": pool,
                        "rados-namespace": rados_ns,
                        "rbd-image": common_image_name,
                        "size": image_size,
                        "rbd-create-image": True,
                    }
                }
                LOG.info(
                    f"Adding NVMe namespace with image '{common_image_name}' "
                    f"in RADOS namespace '{rados_ns}'"
                )
                out, err = gateway.namespace.add(**ns_args)
                if err:
                    LOG.warning(f"Namespace add returned error: {err}")
                else:
                    LOG.info(
                        f"Successfully added NVMe namespace for subsystem '{nqn}' "
                        f"with image '{common_image_name}' from RADOS namespace "
                        f"'{rados_ns}'. Output: {out}"
                    )

    # Step 4: Test creating duplicate NVMe namespace with same image in same RADOS namespace (should fail)
    LOG.info(
        "Test: Creating duplicate NVMe namespace with same image in same "
        "RADOS namespace (should fail)"
    )
    duplicate_image_name = f"duplicate-image-{random.randint(1000, 9999)}"

    # Add first NVMe namespace with image
    if subsystem_config:
        nqn = subsystem_config[0].get("nqn") or subsystem_config[0].get("subnqn")
        if nqn:
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rados-namespace": rados_ns1,
                    "rbd-image": duplicate_image_name,
                    "size": image_size,
                    "rbd-create-image": True,
                }
            }
            LOG.info(
                f"Adding first NVMe namespace with image "
                f"'{duplicate_image_name}' in RADOS namespace '{rados_ns1}'"
            )
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            else:
                LOG.info(
                    f"Successfully added NVMe namespace for subsystem '{nqn}' "
                    f"with image '{duplicate_image_name}' from RADOS namespace "
                    f"'{rados_ns1}'. Output: {out}"
                )

            # Try to create duplicate NVMe namespace with same image in same RADOS namespace (should fail)
            try:
                LOG.info(
                    f"Attempting to add duplicate NVMe namespace with same "
                    f"image '{duplicate_image_name}' in same RADOS namespace "
                    f"'{rados_ns1}'"
                )
                gateway.namespace.add(**ns_args)
                # If we reach here, the test failed
                raise AssertionError(
                    f"FAIL: Duplicate NVMe namespace with image "
                    f"'{duplicate_image_name}' was created in same RADOS "
                    f"namespace '{rados_ns1}'. This should have failed!"
                )
            except Exception as e:
                error_msg = str(e)
                # Check if it's the expected error (namespace already exists or similar)
                if any(
                    keyword in error_msg.lower()
                    for keyword in ["exists", "already", "duplicate", "conflict"]
                ):
                    LOG.info(
                        f"SUCCESS: Duplicate NVMe namespace creation failed "
                        f"as expected: {error_msg}"
                    )
                else:
                    # Re-raise if it's a different error
                    LOG.error(
                        f"Unexpected error during duplicate NVMe namespace "
                        f"test: {error_msg}"
                    )
                    raise
    # List all namespaces created
    LOG.info("Listing all NVMe namespaces created during the test")
    try:
        ns_list = gateway.namespace.list()
        LOG.info("All NVMe namespaces:")
        LOG.info(json.dumps(ns_list, indent=2))
    except Exception as e:
        LOG.warning(f"Failed to list namespaces: {e}")

    LOG.info("All RADOS namespace tests completed successfully")
    LOG.info(
        "Verifying HA on all namespaces in RADOS namespaces and " "default namespaces"
    )
    config["nvme_service"] = nvme_service
    ha = HighAvailability(ceph_cluster, config["gw_node"], **config)
    ha.gateways = nvme_service.gateways
    ha.run()
    return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF deployment
                desc: Configure NVMEoF gateways and initiators
                config:
                    gw_node: node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup
                    rep_pool_config:
                      pool: rbd
                    install: true                               # Run SPDK with all pre-requisites
                    subsystems:                                 # Configure subsystems with all sub-entities
                      - nqn: nqn.2016-06.io.spdk:cnode3
                        serial: 3
                        bdevs:
                          count: 1
                          size: 100G
                        listener_port: 5002
                        allow_host: "*"
                    initiators:                                 # Configure Initiators with all pre-req
                      - subnqn: nqn.2016-06.io.spdk:cnode2
                        listener_port: 5002
                        node: node7


        # Cleanup-only
            - test:
                  abort-on-fail: true
                  config:
                    gw_node: node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    rep_pool_config:
                    subsystems:
                      - nqn: nqn.2016-06.io.spdk:cnode1
                    initiators:
                        - subnqn: nqn.2016-06.io.spdk:cnode1
                          node: node7
                    cleanup-only: true                          # Important param for clean up
                    cleanup:
                        - pool
                        - subsystems
                        - initiators
                        - gateway
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
        # Deploy nvmeof service
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            LOG.info("deploy nvme service")
            nvme_service.deploy()

        nvme_service.init_gateways()

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        test_ceph_nvmeof_ns_with_rados_ns(
            ceph_cluster=ceph_cluster,
            nvme_service=nvme_service,
            config=config,
            rbd_obj=rbd_obj,
        )

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup") and nvme_service:
            teardown(nvme_service, rbd_obj)
    return 1
