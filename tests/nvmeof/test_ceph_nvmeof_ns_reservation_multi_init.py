"""
Module for the test
CEPH-83627298: Verify NVMe namespace reservation types across multiple initiators
"""

import random

from ceph.ceph import Ceph, CommandFailed
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)
RESERVATION_NAMES = {
    1: "Write Exclusive Reservation",
    2: "Exclusive Access Reservation",
    3: "Write Exclusive - Registrants Only Reservation",
    4: "Exclusive Access - Registrants Only Reservation",
    5: "Write Exclusive - All Registrants Reservation",
    6: "Exclusive Access - All Registrants Reservation",
}
RESERVATION_EXPECTATIONS = {
    1: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": True,
    },  # Write Exclusive
    2: {
        "c1_reg_write": True,
        "c2_reg_write": False,
        "c2_reg_read": False,
        "c2_write": False,
        "c2_read": False,
    },  # Exclusive Access
    3: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": True,
    },  # WE - Registrants Only
    4: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": False,
    },  # EA - Registrants Only
    5: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": True,
    },  # WE - All Registrants
    6: {
        "c1_reg_write": True,
        "c2_reg_write": True,
        "c2_reg_read": True,
        "c2_write": False,
        "c2_read": False,
    },  # EA - All Registrants
}


def test_reservation(
    rtype,
    initiator_1,
    initiator_2,
    client_node_1,
    client_node_2,
    dev_to_be_reserved,
    dev_nsid_to_be_reserved,
    rkey,
):
    """Cleanup the ceph-nvme gw entities.

    Args:
        rtype: reservation type
        initiator_1 : initiator object of client 1
        initiator_2 : initiator object of client 2
        client_node_1 : Client 1 node object
        client_node_2 : Client 2 node object
        dev_to_be_reserved : Device to be reserved
        dev_nsid_to_be_reserved : Namespace id of the dev to ber eserved
        rkey : reservation key
    """

    expectations = RESERVATION_EXPECTATIONS[rtype]
    rtype_name = RESERVATION_NAMES[rtype]

    LOG.info("\n\n=== Testing %s (rtype=%s) ===\n\n", rtype_name, rtype)

    # Step 1: Register + Acquire reservation on Client1
    LOG.info("Registering Client 1 for reservation")

    resv_out, rep = initiator_1.register(
        {"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved},
        {"rrega": 0},
        rkey,
        client_node_1,
    )
    LOG.debug(f"Register out={resv_out}, report={rep}")
    LOG.info("Client 1 acquiring lock..")

    resv_out, rep = initiator_1.acquire(
        {"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved},
        {"racqa": 0, "rtype": rtype},
        rkey,
        client_node_1,
    )
    LOG.debug(f"Acquire out={resv_out}, report={rep}")

    # Step 2: If registrants-only or all-registrants, Client2 also registers
    LOG.info("Registering Client 2 for reservation")

    resv_out, rep = initiator_2.register(
        {"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved},
        {"rrega": 0},
        rkey,
        client_node_2,
    )
    LOG.debug(f"Register out={resv_out}, report={rep}")

    # Step 3: Client1 Write
    LOG.info(" rtype-%s : Reservation holder Client1 Write test", rtype)
    try:
        out = run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_1,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c1_reg_write"
        ], f"Reservation holder Client1 write succeeded but should have failed: {out}"
        LOG.info(
            "rtype-%s : Reservation holder Client1 write succeeded: \n %s", rtype, out
        )
    except CommandFailed:
        assert not expectations[
            "c1_reg_write"
        ], "Reservation holder Client1 write failed unexpectedly"
        LOG.info(
            "rtype-%s : Reservation holder Client1 write failed as expected", rtype
        )

    # Step 4: Registered Client2 Write
    LOG.info("rtype-%s : Registered [Client2]  Write test", rtype)
    try:
        out = run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c2_reg_write"
        ], f"Registered Client2 write succeeded but should have failed : {out}"
        LOG.info("rtype-%s : Registered Client2 write succeeded :\n %s}", rtype, out)
    except CommandFailed:
        assert not expectations[
            "c2_reg_write"
        ], f"Registered Client2 write failed unexpectedly: {out}"
        LOG.info(
            "rtype-%s : Registered Client2 write failed as expected :\n %s", rtype, out
        )

    # Step 5: Registered Client2 Read
    LOG.info("rtype-%s : Registered [Client2] Read test", rtype)
    try:
        out = run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="read",
            size="100M",
        )
        assert expectations[
            "c2_reg_read"
        ], f"Registered Client2 read succeeded but should have failed: {out}"
        LOG.info("rtype-%s : Registered Client2 read succeeded: \n %s", rtype, out)
    except CommandFailed:
        assert not expectations[
            "c2_reg_read"
        ], f"Registered Client2 read failed unexpectedly: {out}"
        LOG.info(
            "rtype-%s : Registered Client2 read failed as expected : \n %s", rtype, out
        )

    LOG.info("Unregistering Client 2 reservation")
    initiator_2.register_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "crkey": rkey,
            "rrega": 1,
        }
    )

    # Step 6: Unregistered Client2 Write
    LOG.info("rtype - %s : Unregistered Client2 Write test", rtype)
    try:
        out = run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="write",
            size="100M",
        )
        assert expectations[
            "c2_reg_read"
        ], f"Unregistered Client2 write succeeded but should have failed: {out}"
        LOG.info(
            "rtype - %s : Unregistered Client2 write succeeded : \n %s", rtype, out
        )
    except CommandFailed:
        assert not expectations[
            "c2_write"
        ], f"Unregistered Client2 write failed unexpectedly: {out}"
        LOG.info(
            "rtype - %s : Unregistered Client2 write failed as expected: \n %s",
            rtype,
            out,
        )

    # Step 7: Unregistered Client2 Read
    LOG.info("rtype-%s : Unregistered Client2 Read test", rtype)
    try:
        out = run_fio(
            device_name=dev_to_be_reserved,
            client_node=client_node_2,
            io_type="read",
            size="100M",
        )
        assert expectations[
            "c2_read"
        ], f"Unregistered Client2 read succeeded but should have failed: {out}"
        LOG.info("rtype-%s : Unregistered Client2 read succeeded : \n %s", rtype, out)
    except CommandFailed:
        assert not expectations[
            "c2_read"
        ], f"Unregistered Client2 read failed unexpectedly: {out}"
        LOG.info(
            "rtype-%s : Unregistered Client2 read failed as expected: \n %s", rtype, out
        )
    LOG.info("Releasing lock by Client1")
    initiator_1.release_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "rtype": rtype,
            "crkey": rkey,
            "rrela": 1,
        }
    )
    report = initiator_1.report_reservation(
        **{"device": dev_to_be_reserved, "namespace-id": dev_nsid_to_be_reserved}
    )
    LOG.info("Report:\n %s", report)

    LOG.info("Unregistering Client 1 reservation")
    initiator_1.register_reservation(
        **{
            "device": dev_to_be_reserved,
            "namespace-id": dev_nsid_to_be_reserved,
            "crkey": rkey,
            "rrega": 1,
        }
    )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).
    """
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    nvmegwcli = None

    try:
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
        nvme_service = NVMeService(config, ceph_cluster)
        LOG.info("Deploy NVMe service")
        nvme_service.deploy()
        LOG.info("Initialize gateways")
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]
        configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

        inits = []
        devs = {}
        client_nodes = []
        for node in config["initiators"]["node"]:
            client_node = get_node_by_id(ceph_cluster, node)
            init = NVMeInitiator(client_node)
            init.connect_targets(nvmegwcli, config["initiators"])
            devs[node] = init.list_spdk_drives(nsid_device_pair=1)
            inits.append(init)
            client_nodes.append(client_node)

        initiator_1 = inits[0]
        client_node_1 = client_nodes[0]
        initiator_2 = inits[1]
        client_node_2 = client_nodes[1]
        LOG.debug("Namespace info:  %s", devs)
        dev_to_be_reserved = devs[config["initiators"]["node"][0]][0]["Namespace"]
        dev_nsid_to_be_reserved = devs[config["initiators"]["node"][0]][0]["NSID"]
        # Generate a random reservation key
        rkey = random.randint(1000, 9999)
        LOG.debug("Using randomly generated 4 digit key %s", rkey)
        for rtype in RESERVATION_NAMES.keys():
            test_reservation(
                rtype,
                initiator_1,
                initiator_2,
                client_node_1,
                client_node_2,
                dev_to_be_reserved,
                dev_nsid_to_be_reserved,
                rkey,
            )
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
