import json
import random
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
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


def validate_rbd_image_metadata(init_config, crkey, pool_name=None):
    """
    Validate metadata for the RBD images from client_1 node.
    This runs `rbd ls` on client_1, picks the first image listed,
    then runs `rbd image-meta list <image>`.

    Args:
        init_config: client nodes
        pool_name: optional — name of the rbd pool

    Returns:
        metadata key, value from `rbd image-meta list` output.
    """
    client1 = init_config.get("client_1")
    if pool_name:
        cmd_ls = f"rbd ls {pool_name}"
    else:
        cmd_ls = "rbd ls"
    LOG.debug(f"list RBD images: {cmd_ls}")
    image_out, err = client1.exec_command(cmd=cmd_ls, sudo=True)
    image_out = image_out.strip()
    LOG.debug(f"rbd ls output: '{image_out}',")

    if not image_out:
        raise RuntimeError("No RBD images found")

    image_lines = [line.strip() for line in image_out.splitlines() if line.strip()]
    image_name = image_lines[0]

    if pool_name:
        image_spec = f"{pool_name}/{image_name}"
    else:
        image_spec = image_name
    cmd_meta = f"rbd image-meta list {image_spec}"
    metadata_out, err = client1.exec_command(cmd=cmd_meta, sudo=True)
    metadata_out = metadata_out.strip()

    metadata = {}
    for line in metadata_out.splitlines():
        if not line.strip():
            continue
        if line.lower().startswith("there") or line.lower().startswith("key"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            key, val = parts
        else:
            key = parts[0]
            val = ""
        metadata[key] = val
        LOG.info(f"Metadata: {key} = {val}")

    reserve_key = metadata.get("reservation_key")
    if not reserve_key:
        raise RuntimeError("Metadata missing 'reservation_key'; cannot check crkey")
    try:
        obj = json.loads(reserve_key)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Failed to parse reservation_key JSON: {reserve_key}"
        ) from e
    registrants = obj.get("registrants", [])
    for reg in registrants:
        rkey_value = reg.get("rkey")
        if str(rkey_value) != str(crkey):
            raise RuntimeError(
                f"crkey does not match - Expected crkey {crkey} and metadata has {rkey_value}"
            )

        LOG.info(f"Metadata crkey validation succeeded: {rkey_value}")
    return metadata


def reservation_lifecycle(base_args_1, base_args_2, config, init_config):
    """
    Execute reservation steps (register, acquire, release, unregister) from config,
    and validate both I/O behavior and RBD metadata at each stage.
    """
    reservation_steps = config.get("reservation", [])
    LOG.info("reservation_lifecycle_start")
    crkey = nrkey = random.randint(0, 9999)
    client1 = init_config.get("client_1")
    client2 = init_config.get("client_2")
    initiator_1 = NVMeInitiator(client1)
    for step in reservation_steps:
        try:
            if "register_args" in step:
                register_args = step["register_args"]
                LOG.info(f"REGISTER: nrkey={nrkey} on client_1")
                out, rep = initiator_1.register(
                    base_args_1, register_args, nrkey, init_config.get("client_1")
                )
                LOG.debug(f"Register out={out}, report={rep}")

            elif "acquire_args" in step:
                acquire_args = step["acquire_args"]
                LOG.info(f"ACQUIRE: crkey={crkey} on client_1")
                out, rep = initiator_1.acquire(
                    base_args_1, acquire_args, crkey, init_config.get("client_1")
                )
                LOG.debug(f"Acquire out={out}, report={rep}")
                metadata_acquire = validate_rbd_image_metadata(
                    init_config, crkey, pool_name=config.get("rbd_pool")
                )
                LOG.info(f"Metadata after acquire: {metadata_acquire}")
                io_args_1 = {"device_name": base_args_1["device"], "run_time": "30"}
                io_args_2 = {"device_name": base_args_2["device"], "run_time": "30"}
                client_1_fio = run_io(io_args_1, init_config.get("client_1"))
                client_2_fio = run_io(io_args_2, init_config.get("client_2"))
                # Validate IO: client_1 should succeed, client_2 should fail
                if client_1_fio != 0 or client_2_fio == 0:
                    raise RuntimeError(
                        f"FIO write operation validation failed: "
                        f"{client1.hostname} returned {client_1_fio}, {client2.hostname} returned {client_2_fio}"
                    )
                LOG.info(
                    f"Validated — FIO write operation ran successfully on {client1.hostname}"
                    f" and failed on {client2.hostname}"
                )

            elif "release_args" in step:
                release_args = step["release_args"]
                LOG.info(f"RELEASE: crkey={crkey} on client_1")
                out, rep = initiator_1.release(
                    base_args_1, release_args, crkey, init_config.get("client_1")
                )
                LOG.debug(f"Release out={out}, report={rep}")
                metadata_release = validate_rbd_image_metadata(
                    init_config, crkey, pool_name=config.get("rbd_pool")
                )
                LOG.info(f"Metadata after release: {metadata_release}")
                io_args_1 = {"device_name": base_args_1["device"], "run_time": "30"}
                io_args_2 = {"device_name": base_args_2["device"], "run_time": "30"}
                client_1_fio = run_io(io_args_1, init_config.get("client_1"))
                client_2_fio = run_io(io_args_2, init_config.get("client_2"))
                # Validate IO: Both client_1 and client_2 should succeed
                if client_1_fio == 0 and client_2_fio == 0:
                    LOG.info(
                        f"Validated — FIO write operation ran successfully on both "
                        f"{client1.hostname} and {client2.hostname}"
                    )
                else:
                    raise RuntimeError(
                        f"FIO write operation after release failed: "
                        f"{client1.hostname}={client_1_fio}, {client2.hostname}={client_2_fio}"
                    )

            elif "unregister_args" in step:
                unregister_args = step["unregister_args"]
                LOG.info(f"UNREGISTER: key={crkey} on client_1")
                out, rep = initiator_1.unregister(
                    base_args_1, unregister_args, crkey, init_config.get("client_1")
                )
                LOG.debug(f"Unregister out={out}, report={rep}")
            else:
                LOG.warning(f"Skipping unknown step : {step}")

        except Exception as exc:
            LOG.error(f"Error executing step {step}: {exc}")
    LOG.info("Reservation lifecycle done.")


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
                    reservation:
                      - register_args:
                          rrega: 0
                      - acquire_args:
                          racqa: 0
                          rtype: 1
                      - release_args:
                          rrela: 0
                          rtype: 1
                      - unregister_aargs:
                          rrega: 1

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
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
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
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj)

        if config.get("initiators"):
            for i in config["initiators"]:
                client = get_node_by_id(ceph_cluster, i["node"])
                initiator_obj = NVMeInitiator(client)
                initiator_obj.connect_targets(nvmegwcli, i)

        if config.get("reservation"):
            initiators = config.get("initiators", [])
            init_cfg = {
                "client_1": get_node_by_id(ceph_cluster, initiators[0].get("node")),
                "client_2": get_node_by_id(ceph_cluster, initiators[1].get("node")),
            }
            nsid_ns_pairs = {}
            for key, client in init_cfg.items():
                initiator = NVMeInitiator(client)
                nsid_ns_pair = initiator.list_spdk_drives(nsid_device_pair=1)
                nsid_ns_pairs[key] = nsid_ns_pair

            base_args_1 = {
                "device": nsid_ns_pairs["client_1"][0].get("Namespace"),
                "namespace-id": nsid_ns_pairs["client_1"][0].get("NSID"),
            }
            base_args_2 = {
                "device": nsid_ns_pairs["client_2"][0].get("Namespace"),
                "namespace-id": nsid_ns_pairs["client_2"][0].get("NSID"),
            }
            reservation_lifecycle(base_args_1, base_args_2, config, init_cfg)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)
    return 1
