"""
Test E2E nvmeof namespace resize
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
from tests.rbd.rbd_utils import initial_rbd_config, verify_image_size
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

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


def execute_io(config, ha, ceph_cluster, ns_data):
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
        client.connect_targets(ha.gateways[0], io_client)
        paths = client.list_devices()
        for path in paths:
            client.node.exec_command(cmd="blkdiscard " + path, sudo=True)
        LOG.info("Paths found are %s", paths)
        if len(ns_data.keys()) != len(paths):
            images = ns_data.keys()
            raise Exception(
                "Namespaces are missing !!! paths found are %s and actual images are %s",
                paths,
                images,
            )
        io_args = {"size": "100%"}
        with parallel() as p:
            for path in paths:
                _io_args = {}
                if io_args.get("test_name"):
                    test_name = io_args["test_name"] + "-" + path.replace("/", "_")
                    _io_args.update({"test_name": test_name})
                _io_args.update(
                    {
                        "device_name": path,
                        "client_node": client.node,
                        "long_running": True,
                        "cmd_timeout": "notimeout",
                        "io_type": "write",
                    }
                )
                _io_args = {**io_args, **_io_args}
                p.spawn(run_fio, **_io_args)
            for op in p:
                if op != 0:
                    raise RuntimeError("FIO failed with exit code : %s", str(op))
                results.append(op)
        return results


def check_io_percent(ns_data, ha, io_size):
    for image in ns_data.keys():
        pool = ns_data[image]["rbd_pool_name"]

        out, _ = ha.orch.shell(
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


def test_ceph_83627178(ceph_cluster, config):
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

    # Configure namespaces
    ns_data = configure_ns(config, rbd_pool, rbd_obj, nvmegwcli)

    # Run IO here and check size is reached to 100%
    LOG.info("RUN IO and check 100% IO is filled")
    execute_io(config, ha, ceph_cluster, ns_data)
    check_io_percent(ns_data, ha, "2")

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
    execute_io(config, ha, ceph_cluster, ns_data)
    check_io_percent(ns_data, ha, "2")

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
    execute_io(config, ha, ceph_cluster, ns_data)
    check_io_percent(ns_data, ha, "3")

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
    execute_io(config, ha, ceph_cluster, ns_data)
    check_io_percent(ns_data, ha, "5")

    LOG.info("Execution of CEPH-83627178 test case is successful")


testcases = {
    "CEPH-83627178": test_ceph_83627178,
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
