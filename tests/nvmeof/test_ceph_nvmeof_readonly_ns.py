"""
Test E2E nvmeof readonly namespaces
"""

import json

from ceph.ceph import Ceph
from ceph.parallel import parallel
from tests.nvmeof.test_ceph_nvmeof_high_availability import (
    HighAvailability,
    configure_subsystems,
    deploy_nvme_service,
    get_node_by_id,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_ns(config, rbd_pool, rbd_obj, nvmegwcli, readonly):
    """Confiure namespaces according to given parameters"""
    # Configure namespaces
    LOG.info("Configure Namespaces")
    ns_data = {}
    for nqn in config["subsystems"]:
        for i in range(1, 3):
            # Create 2 namespaces on each subsystem
            nqn_name = nqn["nqn"]
            ns_size = "2G"
            image = f"image-{generate_unique_id(4)}-{i}"
            rbd_obj.create_image(rbd_pool, image, ns_size)
            ns_create_args = {
                "subsystem": nqn_name,
                "rbd-pool": rbd_pool,
                "rbd-image": image,
                "read-only": readonly,
            }
            nvmegwcli.namespace.add(**{"args": {**ns_create_args}})
            img_args = {"nqn": f"{nqn_name}"}
            namespaces, _ = nvmegwcli.namespace.list(
                **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
            )
            namespaces = json.loads(namespaces)
            ns = next(
                (
                    item
                    for item in namespaces.get("namespaces", [])
                    if item.get("rbd_image_name") == image
                ),
                None,
            )
            if ns:
                ns_data.setdefault(image, {}).update(ns)

    return ns_data


def create_images_and_add_ns(config, rbd_obj, rbd_pool, nvmegwcli):
    """Create images and then add namespaces"""
    ns_data = {}
    for nqn in config["subsystems"]:
        for i in range(1, 3):
            # Create 2 namespaces on each subsystem
            nqn_name = nqn["nqn"]
            image = f"image-{generate_unique_id(4)}-{i}"
            sub1_args = {"subsystem": nqn_name}
            rbd_obj.create_image(rbd_pool, image, "2G")
            img_args = {"rbd-pool": rbd_pool, "rbd-image": image}
            nvmegwcli.namespace.add(**{"args": {**sub1_args, **img_args}})
            ns_list_args = {"nqn": f"{nqn_name}"}
            namespaces, _ = nvmegwcli.namespace.list(
                **{"args": {**ns_list_args}, "base_cmd_args": {"format": "json"}}
            )
            namespaces = json.loads(namespaces)
            ns = next(
                (
                    item
                    for item in namespaces.get("namespaces", [])
                    if item.get("rbd_image_name") == image
                ),
                None,
            )
            if ns:
                ns_data.setdefault(image, {}).update(ns)
    return ns_data


def execute_io(
    config, ha, ceph_cluster, ns_data, io_type="write", verify_io_execution=0
):
    """Connect to the initiators and write or read IO"""
    initiators = config.get("initiators")
    results = []
    for io_client in initiators:
        nqn = io_client.get("nqn")
        if io_client.get("subnqn"):
            nqn = io_client.get("subnqn")
        node_id = io_client["node"]
        node = get_node_by_id(ceph_cluster, node_id)
        client = NVMeInitiator(node, nqn)
        # Before connect disconnect
        client.disconnect_all()
        client.connect_targets(ha.gateways[0], io_client)
        paths = client.list_devices()
        LOG.info(f"Paths found are {paths}")
        if len(ns_data.keys()) != len(paths):
            images = ns_data.keys()
            raise Exception(
                f"Namespaces are missing !!! paths found are {paths} and actual images are {images}"
            )
        io_args = {"size": "100%"}
        with parallel() as p:
            for path in paths:
                _io_args = {}
                if io_args.get("test_name"):
                    test_name = f"{io_args['test_name']}-" f"{path.replace('/', '_')}"
                    _io_args.update({"test_name": test_name})
                _io_args.update(
                    {
                        "device_name": path,
                        "client_node": client.node,
                        "long_running": True,
                        "cmd_timeout": "notimeout",
                        "io_type": io_type,
                    }
                )
                _io_args = {**io_args, **_io_args}
                p.spawn(run_fio, **_io_args)
            for op in p:
                if verify_io_execution == 0 and op != 0:
                    raise RuntimeError(f"FIO failed with exit code : {op}")
                elif (
                    verify_io_execution == 1 and op == 1
                ):  # For write on read only namespaces
                    LOG.info("IO write is failed as expected")
                results.append(op)
        return results


def disconnect_all_initiator(ceph_cluster, config):
    """Disconnect all connections on Initiator."""
    initiators = config.get("initiators")
    for io_client in initiators:
        nqn = io_client.get("nqn")
        if io_client.get("subnqn"):
            nqn = io_client.get("subnqn")
        node_id = io_client["node"]
        node = get_node_by_id(ceph_cluster, node_id)
        client = NVMeInitiator(node, nqn)
        # disconnect all the initiators
        client.disconnect_all()


def delete_namespaces(nvmegwcli, ns_data):
    """Delete namespaces"""
    for image in ns_data.keys():
        nqn = ns_data[image]["ns_subsystem_nqn"]
        nsid = ns_data[image]["nsid"]
        sub_args = {"subsystem": nqn}
        nvmegwcli.namespace.delete(**{"args": {**sub_args, **{"nsid": nsid}}})


def test_ceph_83624617(ceph_cluster, config):
    rbd_obj = config["rbd_obj"]
    rbd_pool = config["rbd_pool"]
    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    print(deploy_nvme_service(ceph_cluster, config))
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.initialize_gateways()
    nvmegwcli = ha.gateways[0]

    # Configure subsystems
    LOG.info("Configure subsystems, listeners")
    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

    # Configure readonly namespaces
    ns_data = configure_ns(config, rbd_pool, rbd_obj, nvmegwcli, readonly=True)

    # Verify "ceph nvmeof ns list" should be display "Read Only" as True for all the namespaces
    for image in ns_data.keys():
        if not ns_data[image]["read_only"]:
            raise Exception(
                f"Image {image} is created with read_only flag but in ns list it shows different. \
                Expected is True and Actual is {ns_data[image]['read_only']}"
            )

    # Run IO here and it should fail
    LOG.info("RUN IO on readonly namespaces and it should fail")
    execute_io(
        config, ha, ceph_cluster, ns_data, io_type="write", verify_io_execution=1
    )

    # Disconnect all the namespaces and Delete the namespaces
    LOG.info("Disconect the namespaces from initiator")
    disconnect_all_initiator(ceph_cluster, config)
    LOG.info("Delete namespaces")
    delete_namespaces(nvmegwcli, ns_data)

    # Configure write namespaces
    LOG.info("Create images on pool and add those images as namespaces")
    ns_data = create_images_and_add_ns(config, rbd_obj, rbd_pool, nvmegwcli)

    # Verify "ceph nvmeof ns list" should be display "Read Only" as false for all the namespaces
    for image in ns_data.keys():
        if ns_data[image]["read_only"] is not False:
            raise Exception(
                f"Image {image} is created with read_only flag as False but in ns list it shows different. \
                  Expected is False and Actual is {ns_data[image]['read_only']}"
            )
    # Run IO here and it should fill 100%
    LOG.info("RUN IO nvme namespaces")
    execute_io(
        config, ha, ceph_cluster, ns_data, io_type="write", verify_io_execution=0
    )

    # Perform FIO read on namespaces
    LOG.info("Perform FIO read on namespaces")
    execute_io(config, ha, ceph_cluster, ns_data, io_type="read")

    # Adding read-only namespaces on existing rbd images
    # To add namespaces into readonly mode, we have to delete namespaces
    # and add existing images with --readonly option
    LOG.info("Delete the namespaces for adding same images as read-only namespaces")
    delete_namespaces(nvmegwcli, ns_data)

    LOG.info("Add the deleted namespaces with read-only flag as true")
    for image in ns_data.keys():
        nqn_name = ns_data[image]["ns_subsystem_nqn"]
        image = ns_data[image]["rbd_image_name"]
        rbd_pool = ns_data[image]["rbd_pool_name"]
        sub1_args = {"subsystem": nqn_name}
        img_args = {"rbd-pool": rbd_pool, "rbd-image": image, "read-only": True}
        nvmegwcli.namespace.add(**{"args": {**sub1_args, **img_args}})

    # Perform FIO write on converted namespaces and it should fail
    LOG.info("Perform FIO write on converted namespaces and it should fail")
    execute_io(
        config, ha, ceph_cluster, ns_data, io_type="write", verify_io_execution=1
    )

    # Perform FIO read on namespaces and it should be successful
    LOG.info("Perform FIO read on namespaces and it should be successful")
    execute_io(config, ha, ceph_cluster, ns_data, io_type="read")

    LOG.info("Execution of CEPH-83624617 test case is successful")


testcases = {
    "CEPH-83624617": test_ceph_83624617,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof HA test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform resize of ns and refresh of ns
    - Validate the IO

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    config = kwargs["config"]
    kwargs["config"].update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    try:
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            config.update({"rbd_obj": rbd_obj})
            test_case_run(ceph_cluster, config)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
