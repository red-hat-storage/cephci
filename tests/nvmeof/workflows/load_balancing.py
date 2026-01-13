import json
import time

from ceph.utils import get_node_by_id
from ceph.waiter import WaitUntil
from cli.utilities.utils import get_nodes_by_ids
from tests.nvmeof.workflows.gateway_entities import fetch_namespaces
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    catogorize,
    check_gateway,
    check_gateway_availability,
    get_optimized_state,
    nvme_gw_cli_version_adapter,
    validate_io,
)
from utility.log import Log
from utility.utils import log_json_dump

LOG = Log(__name__)


def get_current_timestamp():
    return time.perf_counter(), time.asctime()


def scale_down(nvme_service, orch, gateway_nodes_to_be_scaleddown):
    """Scaling down of the NVMeoF Gateways.

    Initiate scale-down
    - List the gateways which has to be scaled down.
    - Validate the ANA states of scaled down GWs are optimized in one of the other working GWs.

    Post scale-down Validation
    - List out namespaces associated with the scaled down Gateways using ANA group ids.
    - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.
    """
    start_counter = float()
    start_time = str()
    end_counter = float()
    end_time = str()
    LOG.info(f"{gateway_nodes_to_be_scaleddown}: Scaling down NVMe Service")

    if not isinstance(gateway_nodes_to_be_scaleddown, list):
        gateway_nodes_to_be_scaleddown = [gateway_nodes_to_be_scaleddown]

    to_be_scaledown_gws, operational_gws = catogorize(
        nvme_service, gateway_nodes_to_be_scaleddown
    )
    ana_ids = [gw.ana_group_id for gw in to_be_scaledown_gws]
    gateway = operational_gws[0]

    # Validate IO and scale operation
    old_namespaces = fetch_namespaces(gateway, ana_ids)
    validate_io(orch, old_namespaces)

    # Scale down
    gwnodes_to_be_deployed = list(
        set(nvme_service.config["gw_nodes"]) - set(gateway_nodes_to_be_scaleddown)
    )
    nvme_service.gw_nodes = get_nodes_by_ids(
        nvme_service.ceph_cluster, gwnodes_to_be_deployed
    )
    nvme_service.deploy()

    nvme_service.gateways = []
    nvme_service.init_gateways()

    start_counter, start_time = get_current_timestamp()
    for gateway in to_be_scaledown_gws:
        hostname = gateway.hostname

        if nvme_service.ceph_cluster.rhcs_version >= "8":
            # Wait until 600 seconds
            for w in WaitUntil(timeout=600, interval=5):
                # Check for gateway unavailability
                if check_gateway_availability(
                    nvme_service, gateway.ana_group_id, orch, state="DELETING"
                ):
                    LOG.info(f"[ {gateway} ] NVMeofGW service is UNAVAILABLE.")
                    active = get_optimized_state(
                        nvme_service, orch, gateway.ana_group_id
                    )

                    # Find optimized path
                    if active:
                        LOG.info(
                            f"{list(active[0])} is new and only Active GW for failed {hostname}"
                        )
                        break

                LOG.warning(f"[ {hostname} ] is still in AVAILABLE state..")

            if w.expired:
                raise TimeoutError(
                    f"[ {hostname} ] Scale down of NVMeofGW service failed after 600s timeout.."
                )

        end_counter, end_time = get_current_timestamp()
        LOG.info(
            f"[ {hostname} ] Total time taken to scale down - {end_counter - start_counter} seconds"
        )

        result = {
            "scale-down-start-time": start_time,
            "scale-down-end-time": end_time,
            "scale-down-start-counter-time": start_counter,
            "scale-down-end-counter-time": end_counter,
        }
        LOG.info(log_json_dump(result))

        # Validate auto load balance if rhcs version is 8.1
        if nvme_service.ceph_cluster.rhcs_version >= "8.1":
            time.sleep(60)
            validate_ns_balance = validate_auto_loadbalance(
                orch, nvme_service.nvme_metadata_pool, nvme_service.group
            )
            LOG.info(f"Validated namespaces in each GW:{validate_ns_balance}")
        # Validate IO post scale down
        validate_io(orch, set(list(old_namespaces)))
        return result


def validate_scaleup(nvme_service, orch, scaleup_nodes, namespaces):
    """
    - List out namespaces associated with the new Gateways using ANA group ids.
    - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.

    Args:
        scaleup_nodes (list): A list of gateway nodes to be scaled up.
    """
    start_counter = float()
    start_time = str()
    end_counter = float()
    end_time = str()
    new_gws = []

    for gateway_node in scaleup_nodes:
        gw = get_node_by_id(nvme_service.ceph_cluster, gateway_node)
        new_gws.append(
            create_gateway(
                nvme_gw_cli_version_adapter(nvme_service.ceph_cluster),
                gw,
                mtls=nvme_service.mtls,
                shell=getattr(orch, "shell"),
                gw_group=nvme_service.group,
            )
        )

    start_counter, start_time = get_current_timestamp()
    for gateway in new_gws:
        hostname = gateway.hostname

        # Wait until 600 seconds
        for w in WaitUntil(timeout=600, interval=5):
            # Check for gateway availability
            if check_gateway_availability(nvme_service, gateway.ana_group_id, orch):
                LOG.info(f"[ {gateway} ] NVMeofGW service is AVAILABLE.")
                state = get_optimized_state(nvme_service, orch, gateway.ana_group_id)

                # check gateway for its own original path.
                if gateway.ana_group["name"] in state[0]:
                    end_counter, end_time = get_current_timestamp()
                    LOG.info(
                        f"{hostname} restored to original path - {log_json_dump(state)}"
                    )
                    break

            LOG.warning(f"[ {hostname} ] is still not in AVAILABLE state..")

        if w.expired:
            raise TimeoutError(
                f"[ {hostname} ] Scale up of NVMeofGW service failed after 600s timeout.."
            )

        LOG.info(
            f"[ {hostname} ] Total time taken to scale up - {end_counter - start_counter} seconds"
        )
        result = {
            "scale-up-start-time": start_time,
            "scale-up-end-time": end_time,
            "scale-up-start-counter-time": start_counter,
            "scale-up-end-counter-time": end_counter,
        }
        LOG.info(log_json_dump(result))
        # Validate auto load balance if rhcs version is 8.1
        if nvme_service.ceph_cluster.rhcs_version >= "8.1":
            time.sleep(60)
            validate_ns_balance = validate_auto_loadbalance(
                orch, nvme_service.nvme_metadata_pool, nvme_service.group
            )
            LOG.info(f"Validated namespaces in each GW:{validate_ns_balance}")
        # Validate IO post scale up
        validate_io(orch, set(list(namespaces)))
        return result


def scale_up(nvme_service, orch, scaleup_nodes, gw_nodes, existing_namespaces):
    """Scaling up of the NVMeoF Gateways.

    Initiate scale-up
    - Spin up the new gateways.
    - Validate the ANA states of new GWs are optimized.

    Pre scale-up Validation
    - List out namespaces associated with the new Gateways using ANA group ids.
    - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.

    Post scale-up Validation
    - Check if Ana group ids of replaced GWs took over the original ANA group ids
    """
    existing_namespaces = []
    LOG.info(f"{scaleup_nodes}: Scaling up NVMe Service")

    if not isinstance(scaleup_nodes, list):
        scaleup_nodes = [scaleup_nodes]

    # Validate IO before scale up operation
    validate_io(orch, set(list(existing_namespaces)))

    # Scale up
    gwnodes_to_be_deployed = list(set(nvme_service.config["gw_nodes"] + scaleup_nodes))
    nvme_service.gw_nodes = get_nodes_by_ids(
        nvme_service.ceph_cluster, gwnodes_to_be_deployed
    )
    nvme_service.deploy()

    nvme_service.gateways = []
    nvme_service.init_gateways()

    # Validate ana_grp_ids post scale up
    for scaleup_node in scaleup_nodes:
        for gw_node in gw_nodes:
            if gw_node.node.id == scaleup_node:
                gw = check_gateway(nvme_service.gateways, gw_node.node.id)
                scaleup_gw = check_gateway(nvme_service.gateways, scaleup_node)
                if gw.ana_group_id == scaleup_gw.ana_group_id:
                    LOG.info("Scaleup nodes took over the previous anagrpids")
                else:
                    raise Exception("anagrpids are not matching after scaleup")


def validate_auto_loadbalance(orch, nvme_pool, gateway_group=""):
    """
    Fetch the namespace count on each Gateway and compare them.
    Ensure that the number of namespaces for each GW is within the range [num_namespaces_per_gw + or - len(GWs)].
    """
    out, _ = orch.shell(
        args=["ceph", "nvme-gw", "show", nvme_pool, repr(gateway_group)]
    )
    out = json.loads(out)
    total_num_namespaces = out.get("num-namespaces")
    gateways = out.get("Created Gateways:", [])
    total_gateways = len(gateways)
    if total_gateways == 0:
        raise Exception("No gateways found in the output.")

    num_namespaces_per_gw = total_num_namespaces / total_gateways
    namespaces = {}
    LOG.info(f"Total namespace in GW group : {total_num_namespaces}")
    LOG.info(f"Total GWs: {total_gateways}")
    LOG.info(f"Namespaces per GW : {num_namespaces_per_gw}")

    for gateway in gateways:
        gw_id = gateway["gw-id"]
        num_namespaces = gateway["num-namespaces"]
        lower_range = num_namespaces_per_gw - total_gateways
        upper_range = num_namespaces_per_gw + total_gateways
        LOG.info(
            f"namespace per GW must be in range between {lower_range} and {upper_range}"
        )

        if not (lower_range <= num_namespaces <= upper_range):
            raise Exception(
                f"Gateway '{gw_id}' has an invalid num-namespaces: {num_namespaces}. "
                f"It must be between {lower_range} and {upper_range}."
            )

        namespaces[gw_id] = gateway
        namespaces[gw_id]["num-namespaces"] = num_namespaces

    return namespaces
