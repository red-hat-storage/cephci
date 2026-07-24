from ceph.ceph import Ceph
from ceph.nvmeof.initiator import Initiator
from ceph.utils import get_node_by_id
from rest.common.utils.rest import rest
from rest.workflows.nvmeof.nvmeof import (
    add_and_verify_host,
    add_and_verify_listener,
    create_and_verify_subsystem,
    get_info_nvmeof_gateway,
)
from tests.cephadm import test_orch
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def sanity_workflow(config):
    """
    This method is intended for sanity check of all NVMeOF REST endpoints
    Args:
        config: test config dict
        rbd_obj: rbd_obj to create rbd resources
    """
    _rest = rest()
    ceph_cluster = config.get("ceph_cluster")
    # 1. Get nvmeof gateway info
    rc_nvmeof_gw = get_info_nvmeof_gateway(rest=_rest)
    if rc_nvmeof_gw:
        log.error("FAILED: Get nvmeof gateway info")
        return 1

    # 2. Create a subsytem and do validations
    subsystems = config.get("subsystems", {})
    for _subsystem in subsystems:
        _subsystem.update({"rest": _rest})
        rc_ss = create_and_verify_subsystem(**_subsystem)
        if rc_ss:
            log.error("FAILED: create subsystem and validations")
            return 1
        rc_addhost = add_and_verify_host(**_subsystem)
        if rc_addhost:
            log.error("FAILED: Add host and validations")
            return 1
        rc_addlistener = add_and_verify_listener(ceph_cluster, **_subsystem)
        if rc_addlistener:
            log.error("FAILED: Add listener and validations")
            return 1
    return 0


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
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    config["ceph_cluster"] = ceph_cluster
    try:
        rc_s = sanity_workflow(config)
    except Exception as err:
        log.error(str(err))
        rc_s = 1
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)
    return rc_s
