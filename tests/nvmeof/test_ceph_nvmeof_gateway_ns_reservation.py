import json
import random
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, run_fio

LOG = Log(__name__)


def configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    **(
                        {"no-group-append": config.get("no-group-append", True)}
                        if ceph_cluster.rhcs_version >= "8.0"
                        else {}
                    ),
                },
            }
        }
    )
    listener_cfg = {
        "host-name": nvmegwcli.fetch_gateway_hostname(),
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    nvmegwcli.listener.add(**{"args": {**listener_cfg, **sub_args}})
    if config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(config["allow_host"])}}}
        )

    if config.get("hosts"):
        for host in config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = NVMeInitiator(initiator_node)
            host_nqn = initiator.initiator_nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        with parallel() as p:
            count = config["bdevs"].get("count", 1)
            size = config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)
        namespace_args = {**sub_args, **{"rbd-pool": pool}}
        with parallel() as p:
            # Create namespace in gateway
            for num in range(count):
                ns_args = deepcopy(namespace_args)
                ns_args.update({"rbd-image": f"{name}-image{num}"})
                ns_args = {"args": ns_args}
                p.spawn(nvmegwcli.namespace.add, **ns_args)


def configure_initiators(ceph_cluster, gateway, config):
    """Run IOs from NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem

    Args:
        ceph_cluster: Ceph cluster
        gateway: Ceph-NVMeoF Gateway.
        config: Initiator config

    Example::
        config:
            subnqn: nqn.2016-06.io.spdk:cnode2
            listener_port: 5002
            node: node7
    """
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = NVMeInitiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    discovery_port = {"trsvcid": 8009}
    _disc_cmd = {**cmd_args, **discovery_port, **json_format}
    sub_nqns, _ = initiator.discover(**_disc_cmd)
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    conn_port = {"trsvcid": config["listener_port"]}
    _conn_cmd = {**cmd_args, **conn_port}
    LOG.debug(initiator.connect(**_conn_cmd))


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = NVMeInitiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def disconnect_all_initiator(ceph_cluster, nodes):
    """Disconnect all connections on Initiator."""
    for node in nodes:
        node = get_node_by_id(ceph_cluster, node)
        initiator = NVMeInitiator(node)
        initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, nvmegwcli, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(
                ceph_cluster, initiator_cfg["node"], initiator_cfg["subnqn"]
            )

    if "disconnect_all" in config["cleanup"]:
        nodes = config.get("disconnect_all")
        if not nodes:
            nodes = [i["node"] for i in config["initiators"]]

        disconnect_all_initiator(ceph_cluster, nodes)

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in config["cleanup"]:
        config_sub_node = config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {node}")
            nvmegwcli.subsystem.delete(
                **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
            )

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


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
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    custom_config = kwargs.get("test_data", {}).get("custom-config")

    ceph = Orch(ceph_cluster, **{})

    nvmegwcli = None
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        nvmegwcli = create_gateway(
            nvme_gw_cli_version_adapter(ceph_cluster),
            gw_node,
            mtls=config.get("mtls"),
            shell=getattr(ceph, "shell"),
            port=config.get("gw_port", 5500),
            gw_group=config.get("gw_group"),
        )

        if config.get("cleanup-only"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)
            return 0

        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    p.spawn(
                        configure_subsystems,
                        ceph_cluster,
                        rbd_obj,
                        rbd_pool,
                        nvmegwcli,
                        subsys_args,
                    )

        if config.get("initiators"):
            with parallel() as p:
                for initiator_cfg in config["initiators"]:
                    p.spawn(
                        configure_initiators, ceph_cluster, nvmegwcli, initiator_cfg
                    )

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
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)
    return 1
