"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_nodes_by_ids
from tests.cephadm import test_nvmeof, test_orch
from tests.nvmeof.workflows.ha import HighAvailability
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def deploy_nvme_services(ceph_cluster, config):
    """Deploy NVMe Service with apply or with spec"""
    rbd_pool = config["rbd_pool"]
    gw_nodes = get_nodes_by_ids(ceph_cluster, config["gw_nodes"])

    cfg = {
        "no_cluster_state": False,
        "config": {
            "command": "apply",
            "service": "nvmeof",
            "args": {"placement": {"nodes": [i.hostname for i in gw_nodes]}},
            "pos_args": [rbd_pool],
        },
    }
    if config.get("mtls"):
        cfg = {
            "no_cluster_state": False,
            "config": {
                "command": "apply_spec",
                "service": "nvmeof",
                "validate-spec-services": True,
                "specs": [
                    {
                        "service_type": "nvmeof",
                        "service_id": rbd_pool,
                        "mtls": True,
                        "placement": {"nodes": [i.hostname for i in gw_nodes]},
                        "spec": {
                            "pool": rbd_pool,
                            "enable_auth": True,
                        },
                    }
                ],
            },
        }

    test_nvmeof.run(ceph_cluster, **cfg)


def configure_listeners(ha_obj, nodes, config):
    """Configure Listeners on subsystem."""
    # listeners = get_nodes_by_ids(obj, nodes)
    lb_group_ids = {}
    for node in nodes:
        nvmegwcli = ha_obj.check_gateway(node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        listener_config = {
            "args": {
                "subsystem": config["nqn"],
                "traddr": nvmegwcli.node.ip_address,
                "trsvcid": config["listener_port"],
                "host-name": hostname,
            }
        }
        nvmegwcli.listener.add(**listener_config)
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def configure_subsystems(pool, ha, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    ceph_cluster = config["ceph_cluster"]

    nvmegwcli = ha.gateways[0]
    # Add Subsystem
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    "enable-ha": config.get("enable_ha", False),
                },
            }
        }
    )

    # Add Listeners
    listeners = [nvmegwcli.node.hostname]
    if config.get("listeners"):
        listeners = config["listeners"]
    lb_groups = configure_listeners(ha, listeners, config)

    # Add Host access
    nvmegwcli.host.add(**{"args": {**sub_args, **{"host": repr(config["allow_host"])}}})

    # Add Namespaces
    if config.get("bdevs"):
        bdev_configs = config["bdevs"]
        if isinstance(config["bdevs"], dict):
            bdev_configs = [config["bdevs"]]
        for bdev_cfg in bdev_configs:
            name = generate_unique_id(length=4)
            namespace_args = {
                **sub_args,
                **{
                    "rbd-pool": pool,
                    "rbd-create-image": True,
                    "size": bdev_cfg["size"],
                },
            }
            with parallel() as p:
                # Create namespace in gateway
                for num in range(bdev_cfg["count"]):
                    ns_args = deepcopy(namespace_args)
                    ns_args["rbd-image"] = f"{name}-image{num}"
                    if bdev_cfg.get("lb_group"):
                        lbgid = lb_groups[
                            get_node_by_id(ceph_cluster, bdev_cfg["lb_group"]).hostname
                        ]
                        ns_args["load-balancing-group"] = lbgid
                    ns_args = {"args": ns_args}
                    p.spawn(nvmegwcli.namespace.add, **ns_args)


def disconnect_initiator(ceph_cluster, node):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(ceph_cluster, initiator_cfg["node"])

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        cfg = {
            "no_cluster_state": False,
            "config": {
                "command": "remove",
                "service": "nvmeof",
                "args": {
                    "service_name": f"nvmeof.{config['rbd_pool']}",
                    "verify": True,
                },
            },
        }
        test_orch.run(ceph_cluster, **cfg)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof HA test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform failover and failback.
    - Validate the IO continuation prior and after to failover and failback

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
                    gw_nodes:
                     - node6
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
                    initiators:                             # Configure Initiators with all pre-req
                      - nqn: connect-all
                        listener_port: 4420
                        node: node10
                    fault-injection-methods:                # fail-tool: systemctl, nodes-to-be-failed: node names
                      - tool: systemctl
                        nodes: node6
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        if config.get("install"):
            deploy_nvme_services(ceph_cluster, config)

        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

        # Configure Subsystem
        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    subsys_args["ceph_cluster"] = ceph_cluster
                    p.spawn(configure_subsystems, rbd_obj, rbd_pool, ha, subsys_args)

        # HA failover and failback
        ha.run()

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
