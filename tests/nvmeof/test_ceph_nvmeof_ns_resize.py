"""
Test E2E nvmeof namespace resize
"""

import json

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config, verify_image_size
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_ns(config, rbd_pool, rbd_obj, nvmegwcli):
    # Configure namespaces
    LOG.info("Configure Namespaces")
    ns_data = {}
    for nqn in config["subsystems"]:
        for i in range(1, 3):
            # Create 2 namespaces on each subsystem
            nqn_name = nqn["nqn"]
            ns_size = "2G"
            image = "image-" + generate_unique_id(4) + "-" + str(i)
            rbd_obj.create_image(rbd_pool, image, ns_size)
            ns_create_args = {
                "subsystem": nqn_name,
                "rbd-pool": rbd_pool,
                "rbd-image": image,
            }
            nvmegwcli.namespace.add(**{"args": {**ns_create_args}})
            img_args = {"nqn": nqn_name}
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


def set_auto_resize(ns_data, enable, nvmegwcli):
    for image in ns_data.keys():
        ns_auto_resize_args = {
            "subsystem": ns_data[image]["ns_subsystem_nqn"],
            "nsid": ns_data[image]["nsid"],
        }
        # TODO : Fix this once BZ is resolved https://bugzilla.redhat.com/show_bug.cgi?id=2402045
        if enable:
            ns_auto_resize_args.update({"auto-resize-enabled-true": True})
        else:
            ns_auto_resize_args.update({"auto-resize-enabled-false": True})
        nvmegwcli.namespace.set_auto_resize(**{"args": {**ns_auto_resize_args}})


def verfiy_auto_resize(ns_data, nvmegwcli, autoresize_flag):
    for image in ns_data.keys():
        img_args = {"subsystem": ns_data[image]["ns_subsystem_nqn"]}
        namespace_list = nvmegwcli.namespace.list(
            **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
        )
        namespaces = json.loads(namespace_list[0])
        ns = next(
            (
                item
                for item in namespaces.get("namespaces", [])
                if item.get("rbd_image_name") == image
            ),
            None,
        )
        if ns:
            resize_flag = ns["disable_auto_resize"]
            if str(autoresize_flag) == str(resize_flag):
                LOG.info(
                    "%s disable_auto_resize is set to %s as expected",
                    ns.get("rbd_image_name"),
                    resize_flag,
                )
            else:
                raise Exception(
                    "%s disable_auto_resize is set \
                    to %s but expected is ",
                    ns.get("rbd_image_name"),
                    resize_flag,
                    autoresize_flag,
                )


def resize_rbd_image(ns_data, rbd_obj, rbd_pool, size_increase, flag="increase"):
    for image in ns_data.keys():
        # Resize the RBD image
        LOG.info("Resize the RBD image - %s", ns_data[image]["rbd_image_name"])
        rbd_obj.image_resize(
            pool_name=rbd_pool,
            image_name=ns_data[image]["rbd_image_name"],
            size=size_increase,
            flag=flag,
        )
        # Verify RBD image size after increase
        LOG.info(
            "Verify RBD image size is increased for RBD image - %s",
            ns_data[image]["rbd_image_name"],
        )
        verify_image_size(rbd_obj, rbd_pool, image, size_increase)


def verfiy_namespace_size(ns_data, nvmegwcli, size_to_verify):
    for image in ns_data.keys():
        img_args = {"subsystem": ns_data[image]["ns_subsystem_nqn"]}
        namespace_list = nvmegwcli.namespace.list(
            **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
        )
        namespaces = json.loads(namespace_list[0])
        ns = next(
            (
                item
                for item in namespaces.get("namespaces", [])
                if item.get("rbd_image_name") == image
            ),
            None,
        )
        if ns:
            ns_size = int(ns["rbd_image_size"])
            image_size = int(ns_size / 1073741824)  # 1GB = 1073741824 bytes
            image_size_to_verify = str(image_size) + "G"
            if size_to_verify == image_size_to_verify:
                LOG.info(
                    "%s size is equal to expected size as %s",
                    ns.get("rbd_image_name"),
                    size_to_verify,
                )
            else:
                raise Exception(
                    " size is not equal to expected size \
                    as  after image resize",
                    ns.get("rbd_image_name"),
                    size_to_verify,
                )


def execute_io(config, nvmegwcli, ceph_cluster, ns_data, io_type="write"):
    """Connect to the initiators and write or read IO"""
    initiators = config.get("initiators")
    results = []
    # Prepare IO execution
    LOG.info("Prepare IO execution")
    clients = prepare_io_execution(
        initiators, gateways=[nvmegwcli], cluster=ceph_cluster, return_clients=True
    )
    if not clients:
        raise Exception("Failed to prepare IO execution")

    # Get paths from initiators
    LOG.info("Get paths from initiators")
    paths = clients[0].list_spdk_drives()
    if not paths:
        raise Exception("Failed to get paths from initiators")
    LOG.info(f"Paths found are {paths}")

    # Check if the number of paths is equal to the number of namespaces
    if len(ns_data.keys()) != len(paths):
        images = ns_data.keys()
        raise Exception(
            f"Namespaces are missing !!! paths found are {paths} and actual images are {images}"
        )
    LOG.info("All namespaces are listed at Client(s)")
    # Run FIO
    LOG.info("Run FIO")
    results = clients[0].start_fio(io_size="100%", paths=paths, io_type=io_type)
    for op in results:
        if int(op[2]) != 0:
            raise RuntimeError(f"FIO failed with exit code : {op}")
        else:
            print("IO write is successful")
    return results


def check_io_percent(ns_data, ceph, io_size):
    for image in ns_data.keys():
        pool = ns_data[image]["rbd_pool_name"]

        out, _ = ceph.shell(
            args=["rbd --format json du " + str(pool) + "/" + str(image)], timeout=600
        )
        out = json.loads(out)["images"][0]
        size_to_check = int(int(io_size) * 1073741824)
        if int(size_to_check) == int(out["used_size"]):
            LOG.info("In Image %s used_size is %s G", image, io_size)
        else:
            raise Exception(
                "In Image %s used_size is %s, \
                actual provisioned_size is %s",
                image,
                out["used_size"],
                out["provisioned_size"],
            )


def test_ceph_83627178(ceph_cluster, config, nvme_service):
    rbd_obj = config["rbd_obj"]
    rbd_pool = config["rbd_pool"]
    nvmegwcli = nvme_service.gateways[0]
    ceph = Orch(ceph_cluster, **{})

    # Configure subsystems and listeners
    LOG.info("Configure subsystems, listeners")
    configure_subsystems(nvme_service)
    configure_listeners(nvme_service.gateways, nvme_service.config)
    configure_hosts(nvmegwcli, nvme_service.config)

    # Configure namespaces
    ns_data = configure_ns(config, rbd_pool, rbd_obj, nvmegwcli)

    # Run IO here and check size is reached to 100%
    LOG.info("RUN IO and check 100% IO is filled")
    execute_io(config, nvmegwcli, ceph_cluster, ns_data)
    check_io_percent(ns_data, ceph, "2")

    # Turn OFF auto-resize for the namespaces
    LOG.info("Turn OFF auto-resize for the namespaces")
    set_auto_resize(ns_data, False, nvmegwcli)

    # verify Disable Auto Resize is set to true in ns list output
    LOG.info("verify Disable Auto Resize is set to true in ns list output")
    verfiy_auto_resize(ns_data, nvmegwcli, "True")

    # Resize the RBD image
    LOG.info("Resize the RBD images to 3G")
    resize_rbd_image(ns_data, rbd_obj, rbd_pool, "3G")

    # Verify namespace size is not increased
    LOG.info(
        "Verify namespace size is not increased because Disable Auto Resize is set to NO"
    )
    verfiy_namespace_size(ns_data, nvmegwcli, "2G")

    # Run IO here and check IO is filled with 2G only
    execute_io(config, nvmegwcli, ceph_cluster, ns_data)
    check_io_percent(ns_data, ceph, "2")

    #  Manually refresh namespace size
    LOG.info("Manually refresh namespace size")
    for image in ns_data.keys():
        ns_auto_resize_args = {
            "subsystem": ns_data[image]["ns_subsystem_nqn"],
            "nsid": ns_data[image]["nsid"],
        }
        nvmegwcli.namespace.refresh_size(**{"args": {**ns_auto_resize_args}})

    # Verify Namespace size should be 3G
    LOG.info("Verify namespace size is increased to 3G after refresh")
    verfiy_namespace_size(ns_data, nvmegwcli, "3G")
    # RUN IO here and make sure IO is reached to 3G
    LOG.info("RUN IO and check 100% IO is filled")
    execute_io(config, nvmegwcli, ceph_cluster, ns_data)
    check_io_percent(ns_data, ceph, "3")

    # Turn ON auto-resize for the namespace
    LOG.info("Turn ON auto-resize for the namespaces")
    set_auto_resize(ns_data, True, nvmegwcli)

    # verify Disable Auto Resize is set to true in ns list output
    LOG.info("verify Disable Auto Resize is set to no in ns list output")
    verfiy_auto_resize(ns_data, nvmegwcli, "False")

    # Resize the RBD image again and perform IO (with auto-resize)
    LOG.info("Resize the RBD images to 5G")
    resize_rbd_image(ns_data, rbd_obj, rbd_pool, "5G")

    # Verify Namespace size should be 5G
    LOG.info(
        "Verify namespace size is increased to 5G after auto resize is enabled to yes and increased size to 5G"
    )
    verfiy_namespace_size(ns_data, nvmegwcli, "5G")

    # RUN IO here and make sure IO is reached to 5G
    execute_io(config, nvmegwcli, ceph_cluster, ns_data)
    check_io_percent(ns_data, ceph, "5")

    LOG.info("Execution of CEPH-83627178 test case is successful")


testcases = {
    "CEPH-83627178": test_ceph_83627178,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

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
    LOG.info("Check and set NVMe CLI image")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    nvme_service = NVMeService(config, ceph_cluster)
    LOG.info("Deploy NVMe service")
    nvme_service.deploy()
    LOG.info("Initialize gateways")
    nvme_service.init_gateways()

    try:
        if config.get("test_case"):
            test_case_run = testcases[config["test_case"]]
            config.update({"rbd_obj": rbd_obj})
            test_case_run(ceph_cluster, config, nvme_service)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
