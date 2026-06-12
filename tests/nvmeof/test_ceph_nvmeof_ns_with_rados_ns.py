import json
import random
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


def run_io(io_args, client):
    """
    Run fio I/O on a given client node using provided fio arguments.

    Args:
        io_args : fio parameters such as device_name, size.
        client: node on which fio should run.

    Returns:
        results from run_fio
    """
    args = {
        **io_args,
        "client_node": client,
        "long_running": True,
        "cmd_timeout": "notimeout",
    }
    results = run_fio(**args)
    return results

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
    image_size = "1G"
    
    # Step 1: Create 2 RADOS namespaces
    rados_ns1 = "rados_ns_1"
    rados_ns2 = "rados_ns_2"
    
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
        
        # Create image in RADOS namespace 1
        rbd_image_ns1 = f"image-subsys{idx}-ns1-{random.randint(1000, 9999)}"
        LOG.info(f"Creating RBD image '{rbd_image_ns1}' in RADOS namespace '{rados_ns1}'")
        rbd_obj.create_image(
            pool_name=pool,
            image_name=rbd_image_ns1,
            size=image_size,
            namespace=rados_ns1,
        )
        
        # Add NVMe namespace backed by image in RADOS namespace 1
        ns_args = {
            "args": {
                "subsystem": nqn,
                "rbd-pool": pool,
                "rados-namespace": rados_ns1,
                "rbd-image": rbd_image_ns1,
            }
        }
        LOG.info(f"Adding NVMe namespace for subsystem '{nqn}' with image from RADOS NS1")
        out, err = gateway.namespace.add(**ns_args)
        if err:
            LOG.warning(f"Namespace add returned error: {err}")
        LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{rbd_image_ns1}' from RADOS namespace '{rados_ns1}'. Output: {out}")
        
        # Create image in RADOS namespace 2
        rbd_image_ns2 = f"image-subsys{idx}-ns2-{random.randint(1000, 9999)}"
        LOG.info(f"Creating RBD image '{rbd_image_ns2}' in RADOS namespace '{rados_ns2}'")
        rbd_obj.create_image(
            pool_name=pool,
            image_name=rbd_image_ns2,
            size=image_size,
            namespace=rados_ns2,
        )
        
        # Add NVMe namespace backed by image in RADOS namespace 2
        ns_args = {
            "args": {
                "subsystem": nqn,
                "rbd-pool": pool,
                "rados-namespace": rados_ns2,
                "rbd-image": rbd_image_ns2,
            }
        }
        LOG.info(f"Adding NVMe namespace for subsystem '{nqn}' with image from RADOS NS2")
        out, err = gateway.namespace.add(**ns_args)
        if err:
            LOG.warning(f"Namespace add returned error: {err}")
        LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{rbd_image_ns1}' from RADOS namespace '{rados_ns1}'. Output: {out}")    
    # Step 3: Test creating same image name in different RADOS namespaces (should succeed)
    LOG.info("Test: Creating same image name in different RADOS namespaces")
    common_image_name = f"common-image-{random.randint(1000, 9999)}"
    
    # Create in default namespace (no namespace specified)
    LOG.info(f"Creating image '{common_image_name}' in default namespace")
    rbd_obj.create_image(
        pool_name=pool,
        image_name=common_image_name,
        size=image_size,
    )
    
    # Create same name in RADOS namespace 1
    LOG.info(f"Creating image '{common_image_name}' in RADOS namespace '{rados_ns1}'")
    rbd_obj.create_image(
        pool_name=pool,
        image_name=common_image_name,
        size=image_size,
        namespace=rados_ns1,
    )
    
    # Create same name in RADOS namespace 2
    LOG.info(f"Creating image '{common_image_name}' in RADOS namespace '{rados_ns2}'")
    rbd_obj.create_image(
        pool_name=pool,
        image_name=common_image_name,
        size=image_size,
        namespace=rados_ns2,
    )
    
    LOG.info("SUCCESS: Same image name created in different RADOS namespaces")
    
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
                }
            }
            LOG.info(f"Adding NVMe namespace with '{common_image_name}' from default namespace")
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{common_image_name}' from RADOS namespace '{rados_ns1}'. Output: {out}")   
            
            # Add from RADOS namespace 1
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rados-namespace": rados_ns1,
                    "rbd-image": common_image_name,
                }
            }
            LOG.info(f"Adding NVMe namespace with '{common_image_name}' from RADOS NS1")
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{common_image_name}' from RADOS namespace '{rados_ns1}'. Output: {out}")               
            # Add from RADOS namespace 2
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rados-namespace": rados_ns2,
                    "rbd-image": common_image_name,
                }
            }
            LOG.info(f"Adding NVMe namespace with '{common_image_name}' from RADOS NS2")
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{common_image_name}' from RADOS namespace '{rados_ns1}'. Output: {out}")   
    
    # Step 4: Test creating duplicate NVMe namespace with same image in same RADOS namespace (should fail)
    LOG.info("Test: Creating duplicate NVMe namespace with same image in same RADOS namespace (should fail)")
    duplicate_image_name = f"duplicate-image-{random.randint(1000, 9999)}"
    
    # Create first image in RADOS namespace 1
    LOG.info(f"Creating first image '{duplicate_image_name}' in RADOS namespace '{rados_ns1}'")
    rbd_obj.create_image(
        pool_name=pool,
        image_name=duplicate_image_name,
        size=image_size,
        namespace=rados_ns1,
    )
    
    # Add first NVMe namespace with this image
    if subsystem_config:
        nqn = subsystem_config[0].get("nqn") or subsystem_config[0].get("subnqn")
        if nqn:
            ns_args = {
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rados-namespace": rados_ns1,
                    "rbd-image": duplicate_image_name,
                }
            }
            LOG.info(f"Adding first NVMe namespace with '{duplicate_image_name}' from RADOS NS1")
            out, err = gateway.namespace.add(**ns_args)
            if err:
                LOG.warning(f"Namespace add returned error: {err}")
            LOG.info(f"Successfully added NVMe namespace for subsystem '{nqn}' with image '{common_image_name}' from RADOS namespace '{rados_ns1}'. Output: {out}")              
            # Try to create duplicate NVMe namespace with same image in same RADOS namespace (should fail)
            try:
                LOG.info(f"Attempting to add duplicate NVMe namespace with same image '{duplicate_image_name}' in same RADOS namespace '{rados_ns1}'")
                gateway.namespace.add(**ns_args)
                # If we reach here, the test failed
                raise AssertionError(
                    f"FAIL: Duplicate NVMe namespace with image '{duplicate_image_name}' was created in same RADOS namespace '{rados_ns1}'. "
                    "This should have failed!"
                )
            except Exception as e:
                error_msg = str(e)
                # Check if it's the expected error (namespace already exists or similar)
                if any(keyword in error_msg.lower() for keyword in ["exists", "already", "duplicate", "conflict"]):
                    LOG.info(f"SUCCESS: Duplicate NVMe namespace creation failed as expected: {error_msg}")
                else:
                    # Re-raise if it's a different error
                    LOG.error(f"Unexpected error during duplicate NVMe namespace test: {error_msg}")
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
    rbd_pool = config.get("rbd_pool", "rbd_pool")
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": rbd_pool},
        }
    )
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
        nvmegwcli = nvme_service.gateways[0]

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)
        
        test_ceph_nvmeof_ns_with_rados_ns(
            ceph_cluster=ceph_cluster,
            nvme_service=nvme_service,
            config=config,
            rbd_obj=rbd_obj,
        )
        if config.get("initiators"):
            for i in config["initiators"]:
                client = get_node_by_id(ceph_cluster, i["node"])
                initiator_obj = NVMeInitiator(client)
                # Use connect-all to connect to all namespaces
                connect_config = {**i, "nqn": "connect-all"}
                initiator_obj.connect_targets(nvmegwcli, connect_config)
        
        # Run IOs on all namespaces in parallel
        if config.get("initiators"):
            LOG.info("Running FIO on all connected namespaces in parallel")
            
            # Collect all namespace devices from all initiators
            all_io_tasks = []
            for i in config["initiators"]:
                client = get_node_by_id(ceph_cluster, i["node"])
                if not client:
                    LOG.warning(f"Could not find client node {i['node']}, skipping IO")
                    continue
                    
                initiator_obj = NVMeInitiator(client)
                
                # Get all connected namespaces
                nsid_ns_pairs = initiator_obj.list_spdk_drives(nsid_device_pair=1)
                client_name = getattr(client, 'hostname', i['node'])
                LOG.info(f"Found {len(nsid_ns_pairs)} namespace(s) on {client_name}")
                
                # Prepare IO tasks for each namespace
                for ns_info in nsid_ns_pairs:
                    device = ns_info.get("Namespace")
                    nsid = ns_info.get("NSID")
                    io_args = {
                        "device_name": device,
                        "run_time": "60",
                    }
                    all_io_tasks.append((client, device, nsid, io_args, client_name))
            
            # Run all FIO operations in parallel
            if all_io_tasks:
                LOG.info(f"Starting parallel FIO on {len(all_io_tasks)} namespace(s)")
                results = []
                with parallel() as p:
                    for client, device, nsid, io_args, client_name in all_io_tasks:
                        LOG.info(f"Spawning FIO on namespace {nsid} (device: {device}) on {client_name}")
                        p.spawn(run_io, io_args, client)
                    
                    # Collect results
                    for idx, result in enumerate(p):
                        client, device, nsid, io_args, client_name = all_io_tasks[idx]
                        results.append(result)
                        if result == 0:
                            LOG.info(f"FIO completed successfully on {device} (NSID: {nsid}) on {client_name}")
                        else:
                            LOG.warning(f"FIO returned non-zero status {result} on {device} (NSID: {nsid}) on {client_name}")
                
                # Check if any FIO failed
                failed_count = sum(1 for r in results if r != 0)
                if failed_count > 0:
                    LOG.error(f"{failed_count} out of {len(results)} FIO operations failed")
                    raise Exception(f"FIO operations failed on {failed_count} namespace(s)")
                else:
                    LOG.info(f"All {len(results)} FIO operations completed successfully")

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup") and nvme_service:
            teardown(nvme_service, rbd_obj)
    return 1
