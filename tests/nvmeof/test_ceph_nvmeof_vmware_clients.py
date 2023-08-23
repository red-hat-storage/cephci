"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.nvmeof.gateway import Gateway, configure_spdk, fetch_gateway_log
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_subsystems(rbd, pool, gw, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_nqn = config["nqn"]
    max_ns = config.get("max_ns", 32)
    gw.create_subsystem(sub_nqn, config["serial"], max_ns)
    gw.create_listener(sub_nqn, config["listener_port"])
    gw.add_host(sub_nqn, config["allow_host"])
    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        count = config["bdevs"].get("count", 1)
        size = config["bdevs"].get("size", "1G")
        bdev_size = config["bdevs"].get("bdev_size", "4096")
        # Create image on ceph cluster
        for num in range(count):
            rbd.create_image(pool, f"{name}-image{num}", size)

        # Create block device and add namespace in gateway
        for num in range(count):
            gw.create_block_device(
                f"{name}-bdev{num}", f"{name}-image{num}", pool, bdev_size
            )
            gw.add_namespace(sub_nqn, f"{name}-bdev{num}")

    gw.get_subsystems()


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures NVMeOF GW for VMware clients

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Configure NvmeOF GW for VMware clients
            desc: test to setup NVMeOF GW node for VMware clients
            config:
                gw_node: node6
                rbd_pool: rbd
                do_not_create_image: true
                rep-pool-only: true
                rep_pool_config:
                  pool: rbd
                  install: true                             # Run SPDK with all pre-requisites
                subsystems:                                 # Configure subsystems with all sub-entities
                  - nqn: nqn.2016-06.io.spdk:cnode1
                    serial: 1
                    max_ns: 256
                    bdevs:
                      count: 1
                      size: 500M
                      bdev_size: 512                         #VMware Esx host can only recognize bdev of 512 block size
                    listener_port: 5001
                    allow_host: "*"
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)

        if config.get("install"):
            configure_spdk(gw_node, rbd_pool)

        if config.get("subsystems"):
            for subsystem in config["subsystems"]:
                configure_subsystems(rbd_obj, rbd_pool, gateway, subsystem)

        instance = CephAdmin(cluster=ceph_cluster, **config)
        health, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph -s", sudo=True
        )
        LOG.info(health)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        fetch_gateway_log(gw_node)

    return 1
