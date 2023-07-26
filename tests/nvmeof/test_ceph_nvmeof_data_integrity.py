import json

from ceph.ceph import Ceph
from ceph.nvmeof.gateway import Gateway, configure_spdk, delete_gateway
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
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
        with parallel() as p:
            count = config["bdevs"].get("count", 1)
            size = config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)

        with parallel() as p:
            # Create block device in gateway
            for num in range(count):
                p.spawn(
                    gw.create_block_device,
                    f"{name}-bdev{num}",
                    f"{name}-image{num}",
                    pool,
                )

        with parallel() as p:
            # Add namespace
            for num in range(count):
                p.spawn(gw.add_namespace, sub_nqn, f"{name}-bdev{num}")


def create_filesystem_and_mount(rbd_obj, devicepath, mount_point):
    """Create a filesystem and mount it.

    Args:
        rbd_obj: rbdcfg object
        devicepath: Device path to create filesystem and mount
        mount_point: Mount point for the filesystem
         mount_point: Mount point for the filesystem
    """
    # Create a filesystem
    cmd_mkfs = f"mkfs.ext4 {devicepath}"
    rbd_obj.exec_cmd(cmd=cmd_mkfs)
    # Create a directory to mount the filesystem
    rbd_obj.exec_cmd(cmd=f"mkdir -p {mount_point}")
    # Mount the filesystem
    cmd_mount = f"mount {devicepath} {mount_point}"
    rbd_obj.exec_cmd(cmd=cmd_mount)


def initiators(ceph_cluster, gateway, config):
    """Run IOs from NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem
    - List targets and Run FIO on target devices.

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
    initiator = Initiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    sub_nqns, _ = initiator.discover(**{**cmd_args | json_format})
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    LOG.debug(initiator.connect(**cmd_args))

    # List NVMe targets
    targets, _ = initiator.list(**json_format)
    LOG.debug(targets)
    return targets


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    if "initiators" in config["cleanup"]:
        for initiator_cfg in config["initiators"]:
            node = get_node_by_id(ceph_cluster, initiator_cfg["node"])
            initiator = Initiator(node)
            LOG.info(
                f"Disconnecting initiator {initiator_cfg['subnqn']} on node {initiator_cfg['node']}"
            )
            initiator.disconnect(nqn=initiator_cfg["subnqn"])

    if "subsystems" in config["cleanup"]:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)
        for sub_cfg in config["subsystems"]:
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {gw_node}")
            gateway.delete_subsystem(subnqn=sub_cfg["nqn"])

        if "gateway" in config["cleanup"]:
            delete_gateway(gw_node)

    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO and test data integrity on NVMe targets.
    - Cleanup the ceph-nvme gw entities.

    Args:

        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:


    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    rbd_obj.ceph_client = get_node_by_id(ceph_cluster, config["initiator"]["node"])

    if config.get("cleanup-only"):
        teardown(ceph_cluster, rbd_obj, config)
        return 0

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])

        gateway = Gateway(gw_node)
        if config.get("install"):
            configure_spdk(gw_node, rbd_pool)

        if config.get("subsystems"):
            with parallel() as p:
                for subsystem in config["subsystems"]:
                    p.spawn(configure_subsystems, rbd_obj, rbd_pool, gateway, subsystem)

        if config.get("initiator"):
            targets = json.loads(initiators(ceph_cluster, gateway, config["initiator"]))
            LOG.info(f"Targets discovered: {targets}")

            # verifying data integrity on NVMe targets
            for target in targets["Devices"]:
                nvme = generate_unique_id(length=4)
                mount_point = f"/tmp/{nvme}"  # Change this to the desired mount point
                create_filesystem_and_mount(rbd_obj, target["DevicePath"], mount_point)
                # FIO command to run on NVMe targets
                fio_command = (
                    f"fio --ioengine=sync --refill_buffers=1 "
                    f"--log_avg_msec=1000 --size=1g --bs=128k "
                    f"--filename={mount_point}/test.txt"
                )

                # Run FIO for Writing the files
                LOG.info("Running FIO for write operations.")
                cmd_write = f"{fio_command} --name=writeiops --rw=randwrite"
                rbd_obj.exec_cmd(cmd=cmd_write)
                # Calculate md5sum after FIO write
                md5sum_write = rbd_obj.exec_cmd(cmd=f"md5sum {mount_point}/test.txt")
                # unmount the device
                rbd_obj.exec_cmd(cmd=f"umount {mount_point}")
                # mount the NVMe target
                rbd_obj.exec_cmd(cmd=f"mount {target['DevicePath']} {mount_point}")
                # Run FIO for reading the files
                LOG.info("Running FIO for read operations.")
                cmd_read = f"{fio_command} --name=readiops --rw=randread"
                rbd_obj.exec_cmd(cmd=cmd_read)
                # Calculate md5sum after FIO read
                md5sum_read = rbd_obj.exec_cmd(cmd=f"md5sum {mount_point}/test.txt")

                # Compare md5sum values
                if md5sum_write == md5sum_read:
                    LOG.info(
                        f"Data integrity verified successfully for target {target['DevicePath']}."
                    )
                else:
                    LOG.error(
                        f"Data integrity verification failed for target {target['DevicePath']}."
                    )
                    return 1

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
