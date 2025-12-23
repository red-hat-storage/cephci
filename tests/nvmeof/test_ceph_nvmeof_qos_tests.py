"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    validate_qos,
    verify_qos,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def configure_qos(ceph_cluster, nvmegwcli, subsys_config, config):
    """Configure QOS on namespace"""

    # Set and verify QoS for namespaces
    if subsys_config.get("bdevs").get("qos"):
        for qos_args in subsys_config.get("bdevs").get("qos"):
            prev_key = None
            for key in [
                "r-megabytes-per-second",
                "w-megabytes-per-second",
                "rw-megabytes-per-second",
            ]:
                if key in qos_args:
                    qos_arg = {
                        "nsid": qos_args["nsid"],
                        "subsystem": qos_args["subsystem"],
                        key: qos_args[key],
                    }

                    if prev_key:
                        qos_arg[prev_key] = 0

                    nvmegwcli.namespace.set_qos(
                        **{
                            "args": qos_arg,
                        }
                    )
                    verify_qos(qos_arg, nvmegwcli)
                    prev_key = key

                    run_io_and_validate_qos(
                        ceph_cluster, config, key, qos_args, nvmegwcli
                    )

            # Update QoS values
            qos_arg = {
                "nsid": qos_args["nsid"],
                "subsystem": qos_args["subsystem"],
                "r-megabytes-per-second": 10,
                "w-megabytes-per-second": 20,
                "rw-megabytes-per-second": 30,
                "rw-ios-per-second": 2000,
            }
            nvmegwcli.namespace.set_qos(
                **{
                    "args": qos_arg,
                }
            )
            verify_qos(qos_arg, nvmegwcli)


def trigger_fio(initiator, io_mode, key):
    results = []

    try:
        # List NVMe targets
        paths = initiator.list_spdk_drives()
        if not paths:
            raise Exception(f"NVMe Targets not found on {initiator.hostname}")
        LOG.debug(paths)

        # Utilize NVMeInitiator to start FIO
        io_args = {
            "io_type": io_mode,
            "run_time": "50",
            "size": "100%",
            "iodepth": 4,
            "time_based": True,
        }
        if key == "rw-ios-per-second":
            io_args.update({"rwmixread": "50"})

        results = initiator.start_fio(
            io_size="100%", runtime="50", paths=paths, **io_args
        )

    except Exception as err:
        LOG.error(f"Error in trigger_fio: {err}")
        raise

    return results


def run_io_and_validate_qos(ceph_cluster, config, key, qos_args, nvmegwcli):
    executor = ThreadPoolExecutor()
    lsblk_devs = {}

    initiators = config["initiators"]

    clients = prepare_io_execution(
        initiators, gateways=[nvmegwcli], cluster=ceph_cluster, return_clients=True
    )
    devices_dict = clients[0].fetch_lsblk_nvme_devices_dict()
    lsblk_devs = [device["name"] for device in devices_dict]
    io_mode = {
        "r-megabytes-per-second": "read",
        "w-megabytes-per-second": "write",
        "rw-megabytes-per-second": "randrw",
        "rw-ios-per-second": "randrw",
    }[key]

    # Start IO Execution
    io_tasks = []
    try:
        paths = clients[0].list_spdk_drives()
        if not paths:
            raise Exception(f"NVMe Targets not found on {clients[0].hostname}")
        LOG.debug(paths)

        io_tasks.append(executor.submit(trigger_fio, clients[0], io_mode, key))
        time.sleep(20)

        for initiator in initiators:
            client = get_node_by_id(ceph_cluster, initiator["node"])

            # Validate each QoS parameter one at a time
            validate_qos(client, lsblk_devs[0], **{key: qos_args[key]})

            # Update QoS and verify while IO is in progress
            qos_arg = {
                "nsid": qos_args["nsid"],
                "subsystem": qos_args["subsystem"],
                key: str(int(qos_args[key]) + 100),
            }

            nvmegwcli.namespace.set_qos(
                **{
                    "args": qos_arg,
                }
            )
            LOG.info(f"Updating QoS for namespace {qos_args['nsid']} is successful.")

            verify_qos(qos_arg, nvmegwcli)

    except BaseException as err:  # noqa
        raise Exception(err)
    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=True, cancel_futures=True)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configures Initiators, run FIO on NVMe targets, verify QoS.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    custom_config = kwargs.get("test_data", {}).get("custom-config")

    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    nvmegwcli = None

    try:
        if config.get("install"):
            nvme_service = NVMeService(config, ceph_cluster)
            nvme_service.deploy()
            nvme_service.init_gateways()
            nvmegwcli = nvme_service.gateways[0]

        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj)
            configure_qos(ceph_cluster, nvmegwcli, config["subsystems"][0], config)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
